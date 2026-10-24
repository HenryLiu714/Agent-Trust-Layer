"""W4 `slack_ops_bot`: a Slack Events API bot that answers each mention in one run.

It delivers its own signed events over loopback, through no proxy, and dedupes retries."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import threading
import time
from typing import Any

from tests.e2e.workflows.agents import agentkit, observe

OPS = "C0OPS"
ARCHIVED = "C0OLDOPS"
ALICE = "U0ALICE"
THREAD_TS = "1790000000.000100"  # the parent of the thread the bot is mentioned in
MENTION_TS = "1790000000.000200"

_seen_events: set[str] = set()
_seen_lock = threading.Lock()
_failures: list[str] = []


@observe.trigger
def on_mention(event: dict[str, Any]) -> None:
    channel = event["channel"]
    thread = event.get("thread_ts") or event["ts"]
    agentkit.obs("mention", channel=channel, text=event["text"])
    before = agentkit.slack("conversations.replies", channel=channel, ts=thread)
    who = agentkit.slack("users.info", user=event["user"])
    name = (who.get("user") or {}).get("name", "someone")
    answer = _ask_llm(event["text"], name, len(before.get("messages") or []))
    # An operator may pin the bot to a channel by name (`#ops`), which Slack accepts on a post.
    post_to = os.environ.get("SLACK_POST_CHANNEL") or channel

    if "incident" in event["text"]:
        opened = agentkit.slack("chat.postMessage", channel=post_to, text=f"Incident: {answer}")
        _check("post_top", opened)
        if not opened.get("ok"):
            return
        thread = opened["ts"]

    posted = agentkit.slack("chat.postMessage", channel=post_to, text=answer, thread_ts=thread)
    _check("post_reply", posted)
    reacted = agentkit.slack("reactions.add", channel=channel, name="eyes", timestamp=event["ts"])
    agentkit.obs("reacted", ok=reacted.get("ok"), keys=sorted(reacted))
    agentkit.slack(
        "files.upload", channels=channel, filename="status.csv", content="svc,ok\napi,1\n"
    )
    hook = agentkit.base("slack_hooks") + agentkit.key("SLACK_WEBHOOK_PATH")
    agentkit.http("POST", hook, json_body={"text": f"ops bot answered {name}"})
    if not posted.get("ok"):
        return
    # Read back where Slack said the reply went, and whether the reply is there.
    back = agentkit.slack("conversations.replies", channel=posted["channel"], ts=thread)
    texts = [m.get("text") for m in back.get("messages") or []]
    agentkit.obs(
        "readback",
        ok=back.get("ok"),
        error=back.get("error"),
        channel=posted["channel"],
        sees_reply=answer in texts,
        messages=len(texts),
    )


def _ask_llm(text: str, author: str, thread_len: int) -> str:
    message = agentkit.anthropic(
        "You are the ops bot. Answer in one sentence.",
        [{"role": "user", "content": f"{author} ({thread_len} in thread): {text}"}],
        max_tokens=256,
    )
    return agentkit.text_of(message) or "(no answer)"


def _check(what: str, doc: dict[str, Any]) -> None:
    agentkit.obs(what, ok=doc.get("ok"), error=doc.get("error"), channel=doc.get("channel"))
    if not doc.get("ok"):
        _failures.append(f"{what}: {doc.get('error')}")


class _Events(agentkit.Handler):
    def do_POST(self) -> None:
        raw = self.rfile.read(int(self.headers.get("content-length") or 0))
        if self.path != "/slack/events" or not _verified(raw, self.headers):
            self.reply(401, {"error": "bad signature"})
            return
        payload = json.loads(raw)
        if payload.get("type") == "url_verification":
            self.reply(200, {"challenge": payload["challenge"]})
            return
        with _seen_lock:
            duplicate = payload["event_id"] in _seen_events
            _seen_events.add(payload["event_id"])
        agentkit.obs(
            "delivery",
            event_id=payload["event_id"],
            retry=self.headers.get("X-Slack-Retry-Num"),
            duplicate=duplicate,
        )
        if not duplicate and payload["event"]["type"] == "app_mention":
            try:
                on_mention(payload["event"])
            except Exception as exc:
                # Answer 500, as a web framework would, and fail the exit code; an escaping
                # exception would drop the connection with no answer.
                _failures.append(f"on_mention: {type(exc).__name__}: {exc}")
                self.reply(500, {"ok": False})
                return
        self.reply(200, {"ok": True})


def _signature(raw: bytes, ts: str) -> str:
    secret = agentkit.key("SLACK_SIGNING_SECRET").encode()
    return "v0=" + hmac.new(secret, f"v0:{ts}:".encode() + raw, hashlib.sha256).hexdigest()


def _verified(raw: bytes, headers: Any) -> bool:
    ts = headers.get("X-Slack-Request-Timestamp") or ""
    if not ts.isdigit() or abs(time.time() - int(ts)) > 300:
        return False
    return hmac.compare_digest(_signature(raw, ts), headers.get("X-Slack-Signature") or "")


def _deliver(port: int, payload: dict[str, Any], retry: int | None = None) -> dict[str, Any]:
    """Send one event to our own server, signed the way Slack signs it."""
    raw = json.dumps(payload).encode()
    ts = str(int(time.time()))
    headers = {"X-Slack-Request-Timestamp": ts, "X-Slack-Signature": _signature(raw, ts)}
    if retry is not None:
        headers["X-Slack-Retry-Num"] = str(retry)
    resp = agentkit.deliver(port, "/slack/events", raw, headers)
    return {"status": resp.status, **resp.json()} if resp.ok else {"status": resp.status}


def mention(event_id: str, channel: str, text: str) -> dict[str, Any]:
    event = {
        "type": "app_mention",
        "channel": channel,
        "user": ALICE,
        "text": f"<@U0BOT> {text}",
        "ts": MENTION_TS,
        "thread_ts": THREAD_TS,
    }
    return {"type": "event_callback", "event_id": event_id, "event": event}


SCENARIOS: dict[str, list[tuple[dict[str, Any], int | None]]] = {
    "url_verification": [({"type": "url_verification", "challenge": "chal-4711"}, None)],
    "mention_in_thread": [(mention("Ev01", OPS, "are payouts on schedule?"), None)],
    "own_thread": [(mention("Ev02", OPS, "open an incident for payouts"), None)],
    "channel_by_name": [(mention("Ev03", OPS, "are payouts on schedule?"), None)],
    "missing_scope": [(mention("Ev04", OPS, "are payouts on schedule?"), None)],
    "archived_channel": [(mention("Ev05", ARCHIVED, "are payouts on schedule?"), None)],
    "duplicate_delivery": [
        (mention("Ev06", OPS, "are payouts on schedule?"), None),
        (mention("Ev06", OPS, "are payouts on schedule?"), 1),
    ],
}


def main(argv: list[str]) -> int:
    agentkit.start()
    if len(argv) != 1 or argv[0] not in SCENARIOS:
        print(f"usage: agent.py {{{','.join(SCENARIOS)}}}", file=sys.stderr)
        return 2
    with agentkit.serve(_Events) as port:
        answers = [_deliver(port, payload, retry) for payload, retry in SCENARIOS[argv[0]]]
    agentkit.obs("result", scenario=argv[0], answers=answers, failures=_failures)
    return 1 if _failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
