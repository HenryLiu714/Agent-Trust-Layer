"""Slack faked-write bodies, and the `ts` sequence every faked message takes its identity from."""

import re
import threading
import time
from dataclasses import dataclass
from typing import Any

from irimi.core.bodies import json_object, reflect
from irimi.core.exchange import Request
from irimi.servicemaps import Route
from irimi.services import generic
from irimi.services.generic import reflect_over
from irimi.services.model import AnswerClock

__all__ = [
    "LITERAL_BODIES",
    "SLACK_ENVELOPES",
    "SlackEnvelope",
    "TsClock",
    "body",
    "enveloped",
    "observe_read",
    "slack_l1_body",
]

SLACK_TS_KEYS = frozenset({"ts", "thread_ts", "latest_reply"})
_SLACK_TS = re.compile(r"\d{1,12}\.\d{6}")

# Caps the JSON nodes walked per read body so it cannot stall the response hook (a page is ~100).
OBSERVE_BUDGET = 20_000


class TsClock:
    """One engine's `ts` sequence for faked messages; thread-safe (decision threads, event loop)."""

    def __init__(self) -> None:
        self.last: tuple[int, int] = (0, 0)  # the last `ts` minted or seen: (seconds, counter)
        self._lock = threading.Lock()

    def mint(self) -> str:
        """Return a new Slack `ts`, distinct and increasing, as agents order transcripts by it."""
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
        """Raise the watermark to `ts` if newer, so a minted `ts` sorts after every real one read.

        One watermark for all channels: above the newest anywhere is above each. Never raises.
        """
        if not _SLACK_TS.fullmatch(ts):
            return
        whole, _, fraction = ts.partition(".")
        seen = (int(whole), int(fraction))
        with self._lock:
            if seen > self.last:
                self.last = seen

    def observe_history(self, body: bytes) -> None:
        """Feed each real `ts` in a read's body to the watermark; any body is fine. Never raises."""
        parsed = json_object(body, max_bytes=None)
        if parsed is None:
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


def observe_read(clock: AnswerClock, body: bytes) -> None:
    """Move the policy's `ts` sequence past every real `ts` in a Slack read's body. Never raises."""
    clock.observe_history(body)


@dataclass(frozen=True)
class SlackEnvelope:
    """How one Slack write's answer is assembled; `ts` comes from `TsClock`, never the fixture."""

    payload_key: str | None = None  # where the route's `fixture:` object nests, if it nests
    channel: bool = True  # echo the posted `channel` at the top level
    ts: bool = True  # mint a top-level `ts`
    optional: frozenset[str] = frozenset()  # dropped when still null: Slack omits the key


# Other operations get `{ok, channel, ts}`. `files.upload` has no fixture: Slack retired it and
# slack_sdk uploads through `files.getUploadURLExternal`.
SLACK_ENVELOPES: dict[str, SlackEnvelope] = {
    # A top-level post's `message` has no `thread_ts`; a threaded reply's has.
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
    clock: AnswerClock,
) -> dict[str, Any]:
    """Return the envelope with the fixture object, if any, nested; slack_sdk needs `ok: true`."""
    fields = reflect(request)
    body: dict[str, Any] = {"ok": True}
    if envelope.channel and "channel" in fields:
        body["channel"] = fields["channel"]
    if envelope.ts:
        body["ts"] = clock.mint()
    if envelope.payload_key is not None and obj is not None:
        reflect_over(obj, fields, route)
        for name in envelope.optional:
            if obj.get(name) is None:
                obj.pop(name, None)
        if "ts" in obj and "ts" in body:
            obj["ts"] = body["ts"]
        body[envelope.payload_key] = obj
    return body


def enveloped(
    request: Request, route: Route, obj: dict[str, Any] | None, clock: AnswerClock
) -> dict[str, Any]:
    """Return a mapped Slack write's answer in its operation's envelope."""
    envelope = SLACK_ENVELOPES.get(route.operation, DEFAULT_ENVELOPE)
    return slack_l1_body(request, route, envelope, obj, clock)


def body(
    request: Request, route: Route | None, obj: dict[str, Any] | None, clock: AnswerClock
) -> dict[str, Any]:
    """Return a faked Slack write's body; an unmapped write gets the echo, without `ok: true`."""
    if route is None:
        return generic.l0_body(request, route)
    return enveloped(request, route, obj, clock)


TEXT_CT = "text/plain"

# Answers that are not JSON: an incoming webhook answers `ok` as text/plain.
LITERAL_BODIES: dict[str, tuple[bytes, str]] = {"incoming_webhook": (b"ok", TEXT_CT)}
