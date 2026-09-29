"""W4 `slack_ops_bot`: an ops bot driven by the Slack Events API.

Slack calls the bot at `POST /slack/events`. The bot answers `url_verification` with its challenge,
and handles each `app_mention` as one run (`@sdk.trigger on_mention`):

1. `conversations.replies` on the thread it was mentioned in, and `users.info` on the author;
2. one LLM call for the answer;
3. a threaded `chat.postMessage` reply, `reactions.add` on the mention, `files.upload` of a CSV,
   and a status line to the incoming webhook (whose URL path is the credential);
4. a read-back of its own thread, logging whether it sees its reply.

A mention that says "incident" first opens a top-level incident message and replies under THAT
message's `ts`, so the read-back asks about a thread this run minted (#52).

The server delivers its own events over loopback with a proxy-less opener: an inbound webhook is not
the agent's egress, and irimi must never see it. Each delivery is signed as Slack signs it and the
handler verifies it. Slack retries a slow delivery with `X-Slack-Retry-Num`; the bot dedupes on
`event_id`, so a retry is acknowledged and not handled twice.

    python -m examples.workflows.w04_slack_ops_bot.agent <scenario>
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from examples.workflows import agentkit, sdk

OPS = "C0OPS"
ARCHIVED = "C0OLDOPS"
ALICE = "U0ALICE"
THREAD_TS = "1790000000.000100"  # the parent of the thread the bot is mentioned in
MENTION_TS = "1790000000.000200"

_seen_events: set[str] = set()
_seen_lock = threading.Lock()
_failures: list[str] = []


@sdk.trigger
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
    agentkit.http("POST", hook, json_body={"text": f"ops bot answered {name}"}, label="webhook")
    if not posted.get("ok"):
        return
    # Read back where Slack said the reply went, as an agent would.
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
    body = {
        "model": "claude-sonnet-5",
        "max_tokens": 256,
        "system": "You are the ops bot. Answer in one sentence.",
        "messages": [{"role": "user", "content": f"{author} ({thread_len} in thread): {text}"}],
    }
    headers = {"x-api-key": agentkit.key("ANTHROPIC_API_KEY"), "anthropic-version": "2023-06-01"}
    resp = agentkit.http(
        "POST",
        agentkit.base("anthropic") + "/v1/messages",
        json_body=body,
        headers=headers,
        label="llm",
    )
    return agentkit.text_of(resp.json() or {}) or "(no answer)"


def _check(what: str, doc: dict[str, Any]) -> None:
    agentkit.obs(what, ok=doc.get("ok"), error=doc.get("error"), channel=doc.get("channel"))
    if not doc.get("ok"):
        _failures.append(f"{what}: {doc.get('error')}")


class Events(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        raw = self.rfile.read(int(self.headers.get("content-length") or 0))
        if self.path != "/slack/events" or not _verified(raw, self.headers):
            self._answer(401, {"error": "bad signature"})
            return
        payload = json.loads(raw)
        if payload.get("type") == "url_verification":
            self._answer(200, {"challenge": payload["challenge"]})
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
            on_mention(payload["event"])
        self._answer(200, {"ok": True})

    def _answer(self, status: int, doc: dict[str, Any]) -> None:
        body = json.dumps(doc).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


def _signature(raw: bytes, ts: str) -> str:
    secret = agentkit.key("SLACK_SIGNING_SECRET").encode()
    return "v0=" + hmac.new(secret, f"v0:{ts}:".encode() + raw, hashlib.sha256).hexdigest()


def _verified(raw: bytes, headers: Any) -> bool:
    ts = headers.get("X-Slack-Request-Timestamp") or ""
    if not ts.isdigit() or abs(time.time() - int(ts)) > 300:
        return False
    return hmac.compare_digest(_signature(raw, ts), headers.get("X-Slack-Signature") or "")


def _deliver(port: int, payload: dict[str, Any], retry: int | None = None) -> dict[str, Any]:
    """Send one event to our own server, signed, the way Slack would: not through any proxy."""
    raw = json.dumps(payload).encode()
    ts = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "X-Slack-Request-Timestamp": ts,
        "X-Slack-Signature": _signature(raw, ts),
    }
    if retry is not None:
        headers["X-Slack-Retry-Num"] = str(retry)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(f"http://127.0.0.1:{port}/slack/events", raw, headers)
    try:
        with opener.open(req, timeout=30) as resp:
            return {"status": resp.status, **json.loads(resp.read())}
    except urllib.error.HTTPError as err:
        return {"status": err.code}


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
    server = ThreadingHTTPServer(("127.0.0.1", 0), Events)
    threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True).start()
    answers = [_deliver(server.server_address[1], p, retry) for p, retry in SCENARIOS[argv[0]]]
    server.shutdown()
    agentkit.obs("result", scenario=argv[0], answers=answers, failures=_failures)
    return 1 if _failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
