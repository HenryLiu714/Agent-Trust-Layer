"""What a Slack write is answered with, and the `ts` sequence every faked
message takes its identity from. `services.slack` is the other half: what such a
write does to Slack's later reads."""

import json
import re
import threading
import time
from dataclasses import dataclass
from typing import Any

from irimi.bodies import reflect
from irimi.echo.generic import reflect_over
from irimi.exchange import Request
from irimi.servicemap import Route
from irimi.services.slack import SERVICE

__all__ = ["SERVICE", "SLACK_ENVELOPES", "SlackEnvelope", "TsClock", "enveloped", "slack_l1_body"]

# Keys whose value is a real Slack `ts`, wherever they sit in a read's body. `conversations.
# history` and `.replies` carry one per message, `conversations.info` carries the channel's
# `latest`, and a thread carries `thread_ts` and `latest_reply`. Anything else is left alone.
SLACK_TS_KEYS = frozenset({"ts", "thread_ts", "latest_reply"})
_SLACK_TS = re.compile(r"\d{1,12}\.\d{6}")

# How many JSON nodes one read's body may be walked for. A Slack page is a hundred messages; this
# is three orders of magnitude above that, and it is here so a body that is enormous or deeply
# nested cannot stall the response hook it is walked from.
OBSERVE_BUDGET = 20_000


class TsClock:
    """The `ts` sequence every faked Slack message takes its identity from, one per engine.

    The policy owns it, so two engines in one process, or two tests, never share a sequence. The
    runs one engine serves do share it, which costs nothing: one increasing sequence across them
    is still distinct and increasing within each.
    """

    def __init__(self) -> None:
        # The last `ts` minted or seen, as (seconds, sub-second counter). Locked, because writes
        # are faked on the engine's decision threads and reads observed on its event loop.
        self.last: tuple[int, int] = (0, 0)
        self._lock = threading.Lock()

    def mint(self) -> str:
        """A Slack `ts`: seconds, a dot, and six digits. Distinct and increasing for this clock.

        Slack uses `ts` as a message's identifier and as `thread_ts`, so two messages sharing one
        is two messages that are the same message - an agent that posts twice in a second and then
        replies in a thread addresses whichever of them it collided with. Drawing the sub-second
        part at random gave 181,346 distinct values in 200,000 draws.

        A counter rather than a wider random draw, because distinctness is only half of it: real
        `ts` values increase with time, and code that sorts a transcript by `ts` or asks "is this
        reply after that message" reads the same answer here as it would from Slack. The counter
        carries into the next second if a run ever posts more than a million messages inside one.
        """
        with self._lock:
            seconds, counter = int(time.time()), 0
            last_seconds, last_counter = self.last
            if seconds <= last_seconds:
                seconds, counter = last_seconds, last_counter + 1
            if counter > 999_999:
                seconds, counter = seconds + 1, 0
            self.last = (seconds, counter)
        return f"{seconds}.{counter:06d}"

    def observe(self, ts: str) -> None:
        """Raise the watermark to `ts` when it is newer than anything minted or seen so far.

        A minted `ts` has to sort after every real message already read, or an agent that orders
        a transcript by `ts` - which is how a Slack transcript is ordered - finds its own faked
        message somewhere in the middle of the real ones. `mint` already mints strictly above
        `last`, so feeding the real values into that same watermark is the whole mechanism;
        nothing else has to change and nothing has to be remembered per message.

        The watermark is one per clock rather than one per channel. That is coarser than Slack's
        own ordering and deliberately so: it is strictly stronger, since a `ts` above the newest
        message seen in *any* channel is above the newest in each; it needs no record of which
        reads happened, which irimi does not keep; and `mint`'s promise of one increasing sequence
        survives it.

        Never raises. It is reached from a mitmproxy hook, where a raise forwards the flow.
        """
        if not _SLACK_TS.fullmatch(ts):
            return
        whole, _, fraction = ts.partition(".")
        seen = (int(whole), int(fraction))
        with self._lock:
            if seen > self.last:
                self.last = seen

    def observe_history(self, body: bytes) -> None:
        """Feed every real `ts` in a Slack read's body to the watermark. Never raises.

        The read's body is the only place those values appear - irimi keeps no record of the
        reads, only of the writes - so the watermark is raised as each body goes past rather than
        looked up later. A body that carries no `ts` at all, or that is not JSON, is a no-op,
        which is what lets the engine call this for every Slack read without knowing which ones
        have messages in them.
        """
        try:
            parsed = json.loads(body)
        except Exception:  # not JSON, or no body at all: there is nothing to observe
            return
        stack: list[Any] = [parsed]
        seen = 0
        while stack and seen < OBSERVE_BUDGET:
            node = stack.pop()
            seen += 1
            if isinstance(node, dict):
                for key, value in node.items():
                    if key in SLACK_TS_KEYS and isinstance(value, str):
                        self.observe(value)
                    elif isinstance(value, dict | list):
                        stack.append(value)
            elif isinstance(node, list):
                stack.extend(item for item in node if isinstance(item, dict | list))


@dataclass(frozen=True)
class SlackEnvelope:
    """How one Slack write's answer is assembled.

    A Slack response is an envelope, and what sits inside it differs per method: chat.postMessage
    answers `{ok, channel, ts, message}`, reactions.add answers `{ok}` and nothing else. So the
    envelope is described per operation rather than built one way for the whole service: one
    shape would put a top-level `ts` and `channel` on reactions.add, which are chat.postMessage's
    fields and ones the real API never returns there. An operation with no entry here gets the
    default `SlackEnvelope()`, `{ok, channel, ts}`.

    `ok`, `channel` and `ts` stay envelope-owned. `ts` always comes from `TsClock` and never from
    the fixture: it is the message's identity for the rest of the run, and a fixture's frozen one
    would make every faked message the same message.

    `optional` names payload fields the fixture holds as `null` ONLY so a posted value can be
    reflected onto them, and which are dropped again when the caller posted nothing.
    `reflect_over` writes over a `null` placeholder for any value - that is what a `null` in a
    fixture means - so without this a field the fixture names ships as `null` for every
    caller that did not fill it in, and real Slack sends no key at all. It is per envelope and
    not a rule of `reflect_over`'s, because Stripe's own fixtures hold `reason`, `description`
    and `customer` as `null` and real Stripe really does send those as `null`: dropping them
    would take fields off a refund that production sends.
    """

    payload_key: str | None = None  # where the route's `fixture:` object nests, if it nests
    channel: bool = True  # echo the posted `channel` at the top level
    ts: bool = True  # mint a top-level `ts`
    # Payload fields present in the fixture only to receive a posted value; dropped when the
    # caller posted none, because real Slack omits the key rather than sending `null`.
    optional: frozenset[str] = frozenset()


# Keyed on operation: every Slack write the shipped map names, except `incoming_webhook`, which
# `echo.LITERAL_BODIES` answers before any of this runs.
#
# `files.upload` is here with no payload for the same reason reactions.add is: `{ok}` is what it
# answers, and the `ts` the default envelope adds is a field that method never returned. It has no
# `fixture:` because Slack retired it in March 2025 and slack_sdk uploads through
# `files.getUploadURLExternal` instead, so a `file` object here would fake a method nothing
# calls - but "no fixture" is not "no known shape".
SLACK_ENVELOPES: dict[str, SlackEnvelope] = {
    # `thread_ts` is optional rather than always-present: real Slack puts it on the returned
    # `message` for a threaded reply and sends no such key for a top-level post, and irimi knows
    # which this is - the caller posted it.
    "chat.postMessage": SlackEnvelope(payload_key="message", optional=frozenset({"thread_ts"})),
    "reactions.add": SlackEnvelope(payload_key=None, channel=False, ts=False),
    "files.upload": SlackEnvelope(payload_key=None, channel=False, ts=False),
}
DEFAULT_ENVELOPE = SlackEnvelope()


def slack_l1_body(
    request: Request,
    route: Route,
    envelope: SlackEnvelope,
    obj: dict[str, Any] | None,
    clock: TsClock,
) -> dict[str, Any]:
    """A Slack write's answer: the envelope, with the fixture object nested inside it.

    `obj` is None for an operation that has no payload and for a fixture this install cannot
    read. Both answer the envelope alone, and that is what keeps an unreadable fixture out of
    slack_sdk's error branch: `ok: true` is what the SDK reads to decide the call succeeded, and
    it is the envelope's to say rather than the fixture's.

    `envelope.optional` is applied after the reflection: a field the fixture names only so a
    posted value can land on it is dropped again when none did, so a top-level post's answer has
    no `thread_ts` key rather than a `null` one.
    """
    fields = reflect(request)
    body: dict[str, Any] = {"ok": True}
    if envelope.channel and "channel" in fields:
        body["channel"] = fields["channel"]
    if envelope.ts:
        body["ts"] = clock.mint()
    if envelope.payload_key is not None and obj is not None:
        reflect_over(obj, fields, route)
        for name in envelope.optional:
            # Still at its `null` placeholder, so the caller posted nothing for it - and real
            # Slack sends no key at all in that case, never `null`. `get` because "absent" and
            # "present and None" are the same answer here.
            if obj.get(name) is None:
                obj.pop(name, None)
        if "ts" in obj and "ts" in body:
            obj["ts"] = body["ts"]
        body[envelope.payload_key] = obj
    return body


def enveloped(
    request: Request, route: Route, obj: dict[str, Any] | None, clock: TsClock
) -> dict[str, Any]:
    """A mapped Slack write's answer, in its operation's envelope (`SLACK_ENVELOPES`, else
    `DEFAULT_ENVELOPE`). `obj` is the route's fixture object, or None when it names none or this
    install cannot read it: the envelope still answers, because an SDK that sees no `ok` raises
    instead of reading the fields it just sent."""
    envelope = SLACK_ENVELOPES.get(route.operation, DEFAULT_ENVELOPE)
    return slack_l1_body(request, route, envelope, obj, clock)
