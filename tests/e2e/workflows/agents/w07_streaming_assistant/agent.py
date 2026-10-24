"""W7 `streaming_assistant`: a chat endpoint that streams LLM output back to its own caller.

`chat` embeds, streams Anthropic and OpenAI answers, and may post one Slack summary."""

from __future__ import annotations

import contextlib
import hashlib
import http.client
import itertools
import json
import os
import socket
import struct
import sys
import threading
import urllib.error
import urllib.request
import zlib
from collections.abc import Callable, Iterator
from typing import Any, ClassVar

import httpx

from tests.e2e.workflows.agents import agentkit, observe

READ_SIZE = 7  # deliberately small: every event straddles several reads
# A compressed stream is read this much at a time and decoded only up to DECODED_LIMIT, as a
# careful client does, so a few MiB that decode to a GiB cost the agent nothing.
ENCODED_READ_SIZE = 64 * 1024
DECODED_LIMIT = 1024 * 1024
ANSWERED_BY = "Irimi-Answered-By"
EXIT = {"ok": 0, "client_gone": 3, "upstream_broke": 4}
MODES = ("read_all", "hang_up", "two_callers", "hand_off")
# `httpx` streams the model calls with httpx instead of urllib.
STREAM_CLIENT_ENV = "W7_STREAM_CLIENT"
POST_SUMMARY = {
    "type": "function",
    "function": {"name": "post_summary", "parameters": {"type": "object"}},
}


class ClientGone(Exception):
    """Our own caller hung up mid-answer; the OS error, which differs by OS, is its `__cause__`."""


HUNG_UP = "caller hung up"


class UpstreamBroke(Exception):
    """A model's stream ended before its terminator."""


def sse_events(chunks: Iterator[bytes]) -> Iterator[tuple[str | None, str]]:
    """Yield `(event, data)` per server-sent event, however the bytes were split across chunks."""
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
    """POST and yield the response body as it arrives; any failure raises `UpstreamBroke`."""
    if os.environ.get(STREAM_CLIENT_ENV) == "httpx":
        yield from _stream_post_httpx(url, body, headers, label)
        return
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
    encoding = raw.headers.get("Content-Encoding")
    size = ENCODED_READ_SIZE if encoding else READ_SIZE
    with raw:

        def reads() -> Iterator[bytes]:
            while True:
                try:
                    chunk = raw.read(size)
                except (OSError, http.client.HTTPException) as exc:
                    agentkit.obs("stream_error", label=label, error=type(exc).__name__)
                    raise UpstreamBroke(f"{label}: {type(exc).__name__}") from exc
                if not chunk:
                    return
                yield chunk

        yield from (decoded(reads(), encoding, label) if encoding else reads())


def decoded(chunks: Iterator[bytes], encoding: str, label: str) -> Iterator[bytes]:
    """Yield a gzip stream's chunks decoded up to DECODED_LIMIT, still reading every chunk.

    At the end it logs `stream_encoded` with the bytes' count, SHA-256 and how many it decoded."""
    if encoding.lower() != "gzip":
        agentkit.obs("stream_error", label=label, error=f"encoding {encoding}")
        raise UpstreamBroke(f"{label}: cannot decode {encoding}")
    inflater = zlib.decompressobj(16 + zlib.MAX_WBITS)
    digest, size, room = hashlib.sha256(), 0, DECODED_LIMIT
    for chunk in chunks:
        digest.update(chunk)
        size += len(chunk)
        if room > 0:
            try:
                out = inflater.decompress(inflater.unconsumed_tail + chunk, room)
            except zlib.error as exc:
                agentkit.obs("stream_error", label=label, error="zlib.error")
                raise UpstreamBroke(f"{label}: {exc}") from exc
            room -= len(out)
            if out:
                yield out
    agentkit.obs(
        "stream_encoded",
        label=label,
        encoding=encoding,
        bytes=size,
        sha256=digest.hexdigest(),
        decoded=DECODED_LIMIT - room,
    )


def _stream_post_httpx(
    url: str, body: dict[str, Any], headers: dict[str, str], label: str
) -> Iterator[bytes]:
    """Stream as `stream_post` does, with `httpx.Client.stream`."""
    opened = False
    try:
        with (
            httpx.Client(timeout=agentkit.timeout()) as client,
            client.stream("POST", url, json=body, headers=headers) as resp,
        ):
            opened = True
            answered_by = resp.headers.get(ANSWERED_BY)
            agentkit.obs_http("POST", url, label, status=resp.status_code, answered_by=answered_by)
            if resp.status_code >= 400:
                raise UpstreamBroke(f"{label}: HTTP {resp.status_code}")
            encoding = resp.headers.get("Content-Encoding")
            if encoding:
                yield from decoded(resp.iter_raw(ENCODED_READ_SIZE), encoding, label)
            else:
                yield from resp.iter_raw(READ_SIZE)
    except httpx.TransportError as exc:
        if opened:
            agentkit.obs("stream_error", label=label, error=type(exc).__name__)
        else:
            agentkit.obs_http("POST", url, label, error=type(exc).__name__)
        raise UpstreamBroke(f"{label}: {type(exc).__name__}") from exc


def anthropic_events(question: str) -> Iterator[tuple[str | None, str]]:
    """Return Anthropic's streamed answer to `question` as server-sent events."""
    body = {
        "model": "claude-sonnet-5",
        "max_tokens": 512,
        "stream": True,
        "system": "You are a helpful payments assistant.",
        "messages": [{"role": "user", "content": question}],
    }
    url = agentkit.base("anthropic") + "/v1/messages"
    return sse_events(stream_post(url, body, anthropic_headers(), "anthropic_stream"))


def anthropic_headers() -> dict[str, str]:
    return {"x-api-key": agentkit.key("ANTHROPIC_API_KEY"), "anthropic-version": "2023-06-01"}


def anthropic_stream(
    question: str,
    send: Callable[[str, dict[str, Any]], None],
    events: Iterator[tuple[str | None, str]] | None = None,
) -> str:
    """Return Anthropic's streamed answer, forwarding each delta to the caller as it arrives.

    It reads `events` when the stream is already open (`hand_off`), or opens one."""
    text, stopped = "", False
    for name, data in events if events is not None else anthropic_events(question):
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
    calls: dict[int, dict[str, Any]] = {}
    content, done = "", False
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
# a streaming handler is the ordinary case of a trigger with a non-data argument.
@observe.trigger
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
    if os.environ.get("LANGSMITH_TRACING") == "true":
        agentkit.http(
            "POST",
            agentkit.base("langsmith") + "/runs",
            json_body={"name": "chat", "run_type": "chain", "inputs": {"question": question}},
            label="trace",
        )
    send("done", {"posted": posted is not None})
    return {"answer": answer, "dims": dims, "decision": decision, "posted": posted}


# `hand_off`: the trigger opens the stream inside the run and returns it, so the run ends with
# the stream still open.
@observe.trigger
def open_chat(question: str) -> Iterator[tuple[str | None, str]]:
    events = anthropic_events(question)
    first = next(events)  # the request goes out, labelled with this run, and its first event back
    return itertools.chain([first], events)


def handed_off(question: str, send: Callable[[str, dict[str, Any]], None]) -> dict[str, Any]:
    """Read the stream `open_chat` returned after its run has ended.

    It lists the models first, in no run, which is what lets the fake service end the stream."""
    events = open_chat(question)
    agentkit.http(
        "GET",
        agentkit.base("anthropic") + "/v1/models",
        headers=anthropic_headers(),
        label="models",
    )
    answer = anthropic_stream(question, send, events)
    send("done", {"posted": False})
    return {"answer": answer, "posted": None}


class _Chat(agentkit.Handler):
    protocol_version = "HTTP/1.0"
    outcomes: ClassVar[dict[str, dict[str, Any]]] = {}  # by question
    finished = threading.Semaphore(0)  # released once per chat answered
    hand_off = False  # answer with `handed_off`, not the `chat` trigger

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
                raise ClientGone(HUNG_UP) from exc

        try:
            send("start", {})
            answer = handed_off if _Chat.hand_off else chat
            _Chat.outcomes[question] = {"outcome": "ok", **answer(question, send)}
        except ClientGone as exc:
            _Chat.outcomes[question] = {
                "outcome": "client_gone",
                "error": str(exc),
                # Whichever OS error this OS raised, it is kept as the cause.
                "caused_by_os_error": isinstance(exc.__cause__, OSError),
            }
        except UpstreamBroke as exc:
            _Chat.outcomes[question] = {"outcome": "upstream_broke", "error": str(exc)}
            with contextlib.suppress(ClientGone):
                send("error", {"error": str(exc)})
        finally:
            _Chat.finished.release()


def call_self(port: int, question: str, hang_up: bool) -> list[tuple[str | None, str]]:
    """Ask our own server as the inbound caller, on loopback and never through a proxy."""
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


def _ask(port: int, question: str, hang_up: bool) -> dict[str, Any]:
    """Return what one caller received and how the agent's handler ended the chat."""
    try:
        events = call_self(port, question, hang_up)
        _Chat.finished.acquire(timeout=60)
    except OSError as exc:  # URLError is an OSError
        events = [("caller_error", type(exc).__name__)]
    received = "".join(json.loads(d)["text"] for n, d in events if n == "delta")
    return {
        "caller_events": [n for n, _ in events],
        "caller_text": received,
        **_Chat.outcomes.get(question, {}),
    }


def main(argv: list[str]) -> int:
    agentkit.start()
    if len(argv) != 2 or argv[1] not in MODES:
        print(f"usage: agent.py <question> {{{','.join(MODES)}}}", file=sys.stderr)
        return 2
    question, how = argv
    _Chat.hand_off = how == "hand_off"
    with agentkit.serve(_Chat) as port:
        if how != "two_callers":
            outcome = _ask(port, question, how == "hang_up")
            agentkit.obs("result", **outcome)
            return EXIT.get(outcome.get("outcome", ""), 1)
        questions = [f"{question} (caller {i})" for i in (1, 2)]
        callers: dict[str, dict[str, Any]] = {}
        threads = [
            threading.Thread(target=lambda q=q: callers.update({q: _ask(port, q, False)}))
            for q in questions
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(90)
    answered = [{"question": q, **callers.get(q, {})} for q in questions]
    ends = [c.get("outcome", "") for c in answered]
    end = "ok" if ends == ["ok", "ok"] else next(e for e in ends if e != "ok")
    agentkit.obs("result", outcome=end, callers=answered)
    return EXIT.get(end, 1)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
