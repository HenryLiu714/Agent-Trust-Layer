"""W7 `streaming_assistant`: a chat endpoint that streams LLM output back to its own caller.

`POST /chat` answers with `text/event-stream`. The trigger, `chat`, makes three model calls:

1. OpenAI embeddings for the question (a plain JSON call);
2. Anthropic Messages with `stream: true`, whose text deltas it forwards to the caller as they
   arrive;
3. OpenAI chat completions with `stream: true`, which may answer with a `post_summary` tool call,
   assembled from its deltas. That tool call becomes a Slack post: the one write.

Upstream streams are read a few bytes at a time on purpose, so every SSE event is split across
reads and the parser has to reassemble it (`sse_events`).

    python -m examples.workflows.launch \\
        examples.workflows.w07_streaming_assistant.agent <question> {read_all,hang_up}

The agent delivers the question to itself over loopback. `hang_up` makes that caller reset the
connection after the first event, the way a closed browser tab does. Exit codes: 0 answered,
3 the caller went away, 4 an upstream stream broke.
"""

from __future__ import annotations

import http.client
import json
import socket
import struct
import sys
import threading
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from typing import Any

from examples.workflows import agentkit, sdk

READ_SIZE = 7  # deliberately small: every event straddles several reads
ANSWERED_BY = "Irimi-Answered-By"
EXIT = {"ok": 0, "client_gone": 3, "upstream_broke": 4}
POST_SUMMARY = {
    "type": "function",
    "function": {"name": "post_summary", "parameters": {"type": "object"}},
}


class ClientGone(Exception):
    """Our own caller hung up mid-answer."""


class UpstreamBroke(Exception):
    """A model's stream ended before its terminator."""


def sse_events(chunks: Iterator[bytes]) -> Iterator[tuple[str | None, str]]:
    """`(event, data)` per server-sent event, however the bytes were split across chunks."""
    buf = b""
    for chunk in chunks:
        buf += chunk.replace(b"\r\n", b"\n")
        while b"\n\n" in buf:
            block, buf = buf.split(b"\n\n", 1)
            name, data = None, []
            for line in block.decode().split("\n"):
                if line.startswith("event:"):
                    name = line[6:].strip()
                elif line.startswith("data:"):
                    data.append(line[5:].lstrip())
            if data or name:
                yield name, "\n".join(data)


def stream_post(
    url: str, body: dict[str, Any], headers: dict[str, str], label: str
) -> Iterator[bytes]:
    """POST and yield the response body as it arrives. A stream that never opens (an error status,
    or no answer at all) or fails mid-body is logged and raised as `UpstreamBroke`, so the caller
    is told and the run ends as a broken upstream rather than a crash in the handler thread."""
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        method="POST",
        headers={**headers, "Content-Type": "application/json"},
    )
    try:
        raw = urllib.request.urlopen(request, timeout=agentkit.timeout())
    except urllib.error.HTTPError as err:
        err.close()
        answered_by = err.headers.get(ANSWERED_BY)
        agentkit.obs_http("POST", url, label, status=err.code, answered_by=answered_by)
        raise UpstreamBroke(f"{label}: HTTP {err.code}") from err
    except (OSError, http.client.HTTPException) as exc:
        agentkit.obs_http("POST", url, label, error=type(exc).__name__)
        raise UpstreamBroke(f"{label}: {type(exc).__name__}") from exc
    answered_by = raw.headers.get(ANSWERED_BY)
    agentkit.obs_http("POST", url, label, status=raw.status, answered_by=answered_by)
    with raw:
        while True:
            try:
                chunk = raw.read(READ_SIZE)
            except (OSError, http.client.HTTPException) as exc:
                agentkit.obs("stream_error", label=label, error=type(exc).__name__)
                raise UpstreamBroke(f"{label}: {type(exc).__name__}") from exc
            if not chunk:
                return
            yield chunk


def anthropic_stream(question: str, send: Callable[[str, dict[str, Any]], None]) -> str:
    body = {
        "model": "claude-sonnet-5",
        "max_tokens": 512,
        "stream": True,
        "system": "You are a helpful payments assistant.",
        "messages": [{"role": "user", "content": question}],
    }
    headers = {"x-api-key": agentkit.key("ANTHROPIC_API_KEY"), "anthropic-version": "2023-06-01"}
    url = agentkit.base("anthropic") + "/v1/messages"
    text, stopped = "", False
    for name, data in sse_events(stream_post(url, body, headers, "anthropic_stream")):
        event = json.loads(data)
        if name == "content_block_delta" and event["delta"]["type"] == "text_delta":
            text += event["delta"]["text"]
            send("delta", {"text": event["delta"]["text"]})
        elif name == "message_stop":
            stopped = True
    if not stopped:
        agentkit.obs("stream_error", label="anthropic_stream", error="truncated")
        raise UpstreamBroke("anthropic_stream: ended without message_stop")
    return text


def openai_stream(answer: str) -> tuple[str, list[dict[str, Any]]]:
    body = {
        "model": "gpt-5",
        "stream": True,
        "messages": [
            {"role": "system", "content": "Decide whether the team needs a summary posted."},
            {"role": "user", "content": answer},
        ],
        "tools": [POST_SUMMARY],
    }
    headers = {"Authorization": f"Bearer {agentkit.key('OPENAI_API_KEY')}"}
    url = agentkit.base("openai") + "/v1/chat/completions"
    content, calls, done = "", {}, False
    for _, data in sse_events(stream_post(url, body, headers, "openai_stream")):
        if data == "[DONE]":
            done = True
            continue
        delta = json.loads(data)["choices"][0]["delta"]
        content += delta.get("content") or ""
        for piece in delta.get("tool_calls") or []:
            call = calls.setdefault(piece["index"], {"id": None, "name": "", "arguments": ""})
            call["id"] = piece.get("id") or call["id"]
            function = piece.get("function") or {}
            call["name"] += function.get("name") or ""
            call["arguments"] += function.get("arguments") or ""
    if not done:
        raise UpstreamBroke("openai_stream: ended without [DONE]")
    return content, [calls[i] for i in sorted(calls)]


# The trigger takes the caller's writer as an argument, so its captured args cannot be replayed:
# a streaming handler is the ordinary case of a trigger with a non-data argument (#74, #84).
@sdk.trigger
def chat(question: str, send: Callable[[str, dict[str, Any]], None]) -> dict[str, Any]:
    embed = agentkit.http(
        "POST",
        agentkit.base("openai") + "/v1/embeddings",
        json_body={"model": "text-embedding-3-small", "input": question},
        headers={"Authorization": f"Bearer {agentkit.key('OPENAI_API_KEY')}"},
        label="embed",
    )
    dims = len((embed.json() or {"data": [{"embedding": []}]})["data"][0]["embedding"])
    answer = anthropic_stream(question, send)
    decision, tool_calls = openai_stream(answer)
    posted = None
    for call in tool_calls:
        if call["name"] == "post_summary":
            args = json.loads(call["arguments"])
            doc = agentkit.slack("chat.postMessage", channel=args["channel"], text=args["text"])
            posted = {"ok": doc.get("ok"), "channel": doc.get("channel"), "text": args["text"]}
    send("done", {"posted": posted is not None})
    return {"answer": answer, "dims": dims, "decision": decision, "posted": posted}


class _Chat(agentkit.Handler):
    protocol_version = "HTTP/1.0"
    outcome: dict[str, Any] = {}
    finished = threading.Event()

    def do_POST(self) -> None:
        question = json.loads(self.rfile.read(int(self.headers["content-length"])))["question"]
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.end_headers()

        def send(event: str, data: dict[str, Any]) -> None:
            try:
                self.wfile.write(f"event: {event}\ndata: {json.dumps(data)}\n\n".encode())
                self.wfile.flush()
            except OSError as exc:
                raise ClientGone(type(exc).__name__) from exc

        try:
            send("start", {})
            _Chat.outcome = {"outcome": "ok", **chat(question, send)}
        except ClientGone as exc:
            _Chat.outcome = {"outcome": "client_gone", "error": str(exc)}
        except UpstreamBroke as exc:
            _Chat.outcome = {"outcome": "upstream_broke", "error": str(exc)}
            try:
                send("error", {"error": str(exc)})
            except ClientGone:
                pass
        finally:
            _Chat.finished.set()


def call_self(port: int, question: str, hang_up: bool) -> list[tuple[str | None, str]]:
    """The inbound caller, on loopback and never through a proxy."""
    if hang_up:
        return _hang_up_after_first_event(port, question)
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
    body = json.dumps({"question": question})
    conn.request("POST", "/chat", body, {"Content-Type": "application/json"})
    resp = conn.getresponse()
    events = list(sse_events(iter(lambda: resp.read(READ_SIZE), b"")))
    conn.close()
    return events


def _hang_up_after_first_event(port: int, question: str) -> list[tuple[str | None, str]]:
    """Read the headers and the first event, then reset: a closed browser tab."""
    body = json.dumps({"question": question}).encode()
    sock = socket.create_connection(("127.0.0.1", port), timeout=60)
    sock.sendall(
        b"POST /chat HTTP/1.0\r\nContent-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n\r\n".encode()
        + body
    )
    buf = b""
    while b"\r\n\r\n" not in buf or b"\n\n" not in buf.split(b"\r\n\r\n", 1)[1]:
        chunk = sock.recv(1)
        if not chunk:
            break
        buf += chunk
    # RST, not FIN: the handler's next write fails at once instead of filling a buffer.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    sock.close()
    return list(sse_events(iter([buf.split(b"\r\n\r\n", 1)[-1]])))


def main(argv: list[str]) -> int:
    agentkit.start()
    if len(argv) != 2 or argv[1] not in ("read_all", "hang_up"):
        print("usage: agent.py <question> {read_all,hang_up}", file=sys.stderr)
        return 2
    question, how = argv
    with agentkit.serve(_Chat) as port:
        try:
            events = call_self(port, question, how == "hang_up")
            _Chat.finished.wait(60)
        except OSError as exc:  # URLError is an OSError
            events = [("caller_error", type(exc).__name__)]
    received = "".join(json.loads(d)["text"] for n, d in events if n == "delta")
    outcome = _Chat.outcome
    agentkit.obs(
        "result",
        caller_events=[n for n, _ in events],
        caller_text=received,
        **outcome,
    )
    return EXIT.get(outcome.get("outcome", ""), 1)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
