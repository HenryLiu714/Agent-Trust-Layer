"""What every sample agent shares: a urllib HTTP client, its observation log and configuration.

The observation log (`obs()`) lets a test assert on what the agent saw, not only irimi's output."""

from __future__ import annotations

import contextlib
import functools
import json
import logging
import os
import signal
import sqlite3
import struct
import threading
import time
import urllib.error
import urllib.request
import zlib
from collections.abc import Iterator
from dataclasses import dataclass
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

import irimi
from irimi import sdk
from irimi.protocol.control import RECORDED_ACTION
from irimi.protocol.env import CONTROL_ENV, REPLAY_RUN_ENV

OBS_ENV = "WORKFLOW_OBS"
STATE_ENV = "WORKFLOW_STATE"
TIMEOUT_ENV = "WORKFLOW_HTTP_TIMEOUT"
# The port the harness serves every host name on; unset outside the harness.
PORT_ENV = "WORKFLOW_INTERNET_PORT"
# An agent that hangs would hang the test that runs it, because `irimi shadow` waits for its
# child. SIGALRM's default action ends the process, so a stuck agent dies instead.
WATCHDOG_S = 90
# The parent of every irimi logger, the SDK's included: a WARNING there means a run record was lost.
IRIMI_LOGGER = irimi.__name__
# Set for an agent that reaches irimi's control endpoint through a gateway that forwards the routes
# it knows and refuses the one a replay's read tools ask (`lookup_gateway`).
LOOKUP_REFUSED_ENV = "WORKFLOW_LOOKUP_REFUSED"
# That route's tail.
RECORDED_ROUTE = "/" + RECORDED_ACTION

_obs_lock = threading.Lock()


def start() -> None:
    """Start an agent: log the proxy, control endpoint and replay run it was given.

    From here on, each record an irimi logger emits is a `log` event too."""
    if hasattr(signal, "SIGALRM"):
        signal.alarm(WATCHDOG_S)
    logging.getLogger(IRIMI_LOGGER).addHandler(_LogToObs())
    obs(
        "start",
        proxy=proxy(),
        control=os.environ.get(CONTROL_ENV),
        replay=os.environ.get(REPLAY_RUN_ENV),
    )


class _LogToObs(logging.Handler):
    """Log each record an irimi logger emits in the agent as a `log` event.

    The SDK never raises, so a WARNING is how the harness sees an unreachable control endpoint."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            obs("log", logger=record.name, level=record.levelname, message=record.getMessage())
        except Exception:
            self.handleError(record)


def proxy() -> str | None:
    """Return the HTTP proxy the agent was given, as urllib reads it, or None."""
    return os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")


def obs(event: str, **data: Any) -> None:
    """Append one event to the observation log. A no-op when the log is not configured."""
    path = os.environ.get(OBS_ENV)
    if not path:
        return
    line = json.dumps({"event": event, "t": time.time(), **data}, default=repr)
    with _obs_lock, open(path, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def base(service: str) -> str:
    """Return `STRIPE_API_BASE` and friends, with the real service as the default."""
    defaults = {
        "stripe": "https://api.stripe.com",
        "slack": "https://slack.com",
        "slack_hooks": "https://hooks.slack.com",
        "anthropic": "https://api.anthropic.com",
        "openai": "https://api.openai.com",
        "langsmith": "https://api.smith.langchain.com",
    }
    return os.environ.get(f"{service.upper()}_API_BASE") or defaults[service]


def internal(host: str) -> str:
    """Return an internal service's base URL, on the harness's port when there is one."""
    port = os.environ.get(PORT_ENV)
    return f"http://{host}:{port}" if port else f"http://{host}"


def key(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"error: set {name}")
    return value


def state_dir() -> Path:
    """Return where an agent keeps its local state (SQLite files, exports)."""
    path = Path(os.environ.get(STATE_ENV) or ".")
    path.mkdir(parents=True, exist_ok=True)
    return path


def timeout() -> float:
    return float(os.environ.get(TIMEOUT_ENV) or 10)


@contextlib.contextmanager
def sqlite(name: str) -> Iterator[sqlite3.Connection]:
    """Run one transaction on the state directory's SQLite file `name`, and always close it."""
    with contextlib.closing(sqlite3.connect(state_dir() / name)) as conn, conn:
        yield conn


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: bytes

    @property
    def answered_by(self) -> str | None:
        return self.headers.get("irimi-answered-by")

    def json(self) -> Any:
        try:
            return json.loads(self.body)
        except ValueError:
            return None

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


class NetworkError(Exception):
    """The request got no whole HTTP answer: refused, reset, timed out, or cut off mid-body."""


def http(
    method: str,
    url: str,
    *,
    json_body: Any = None,
    form: dict[str, Any] | None = None,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout_s: float | None = None,
    label: str | None = None,
) -> Response:
    """Send one request with urllib and log it as an `http` event.

    An error status is returned, not raised; a request with no answer raises `NetworkError`."""
    sent = dict(headers or {})
    data = body
    if json_body is not None:
        data = json.dumps(json_body).encode()
        sent.setdefault("Content-Type", "application/json")
    elif form is not None:
        data = urlencode(form).encode()
        sent.setdefault("Content-Type", "application/x-www-form-urlencoded")
    request = urllib.request.Request(url, data=data, method=method, headers=sent)
    try:
        resp = _open(urllib.request.urlopen, request, timeout_s or timeout())
    except (OSError, HTTPException) as err:  # URLError is an OSError
        obs_http(method, url, label, error=type(err).__name__)
        parts = urlsplit(url)
        raise NetworkError(f"{method} {parts.hostname}{parts.path}: {err}") from err
    obs_http(method, url, label, status=resp.status, answered_by=resp.answered_by)
    return resp


def _open(opener: Any, request: urllib.request.Request, timeout_s: float) -> Response:
    """Return `opener(request)` read whole, an HTTP error status included."""
    try:
        with opener(request, timeout=timeout_s) as raw:
            return Response(raw.status, {k.lower(): v for k, v in raw.headers.items()}, raw.read())
    except urllib.error.HTTPError as err:
        with err:  # it holds the connection open until closed
            return Response(err.code, {k.lower(): v for k, v in err.headers.items()}, err.read())


def obs_http(
    method: str,
    url: str,
    label: str | None,
    *,
    status: int | None = None,
    answered_by: str | None = None,
    error: str | None = None,
    **extra: Any,
) -> None:
    """Log a call made some other way as `http()` logs one; a call with no answer logs `error`."""
    parts = urlsplit(url)
    record: dict[str, Any] = {"method": method, "url": f"{parts.hostname}{parts.path}"}
    record.update(label=label, run=sdk.current_run_id(), **extra)
    if error is not None:
        record["error"] = error
    else:
        record.update(status=status, answered_by=answered_by)
    obs("http", **record)


# -- the services every workflow talks to --------------------------------------------------------


def stripe(method: str, path: str, form: dict[str, Any] | None = None, **kw: Any) -> Response:
    headers = {"Authorization": f"Bearer {key('STRIPE_API_KEY')}", **kw.pop("headers", {})}
    return http(method, base("stripe") + path, form=form, headers=headers, **kw)


def slack(method: str, *, label: str | None = None, **params: Any) -> dict[str, Any]:
    """Call the Slack Web API and return the document, leaving `ok: false` to the caller."""
    headers = {"Authorization": f"Bearer {key('SLACK_BOT_TOKEN')}"}
    url = f"{base('slack')}/api/{method}"
    resp = http("POST", url, json_body=params, headers=headers, label=label)
    doc = resp.json()
    return doc if isinstance(doc, dict) else {"ok": False, "error": f"http_{resp.status}"}


def anthropic(
    system: str,
    messages: list[dict[str, Any]],
    *,
    tools: list[dict[str, Any]] | None = None,
    max_tokens: int = 1024,
    label: str | None = None,
    metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Make one Anthropic Messages call; an error status raises."""
    body: dict[str, Any] = {
        "model": "claude-sonnet-5",
        "max_tokens": max_tokens,
        "system": system,
        "messages": messages,
    }
    if tools:
        body["tools"] = tools
    if metadata:
        body["metadata"] = metadata
    headers = {"x-api-key": key("ANTHROPIC_API_KEY"), "anthropic-version": "2023-06-01"}
    url = base("anthropic") + "/v1/messages"
    resp = http("POST", url, json_body=body, headers=headers, label=label)
    doc = resp.json()
    if not resp.ok or not isinstance(doc, dict):
        raise RuntimeError(f"anthropic answered {resp.status}")
    return doc


def text_of(message: dict[str, Any]) -> str:
    return "".join(
        b.get("text", "") for b in message.get("content") or [] if b.get("type") == "text"
    )


# -- a service that delivers its own inbound calls -------------------------------------------------
#
# A webhook consumer serves on loopback and delivers each inbound call to itself, in place of its
# caller. An inbound call is not the agent's egress, so it goes through no proxy.


class Handler(BaseHTTPRequestHandler):
    """A request handler that answers JSON and keeps the stdlib's per-request log off stderr."""

    def reply(self, status: int, doc: dict[str, Any]) -> None:
        body = json.dumps(doc).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


@contextlib.contextmanager
def serve(handler: type[BaseHTTPRequestHandler]) -> Iterator[int]:
    """Serve `handler` on a free loopback port for the block, and yield the port."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True).start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()


@contextlib.contextmanager
def lookup_gateway() -> Iterator[None]:
    """Reach the control endpoint through a gateway that refuses recorded lookups, if asked to.

    With `LOOKUP_REFUSED_ENV` set, every post is forwarded but a read tool's lookup gets a 502, so
    a replay misses it and runs the tool for real while the run is still recorded."""
    control = os.environ.get(CONTROL_ENV)
    if not os.environ.get(LOOKUP_REFUSED_ENV) or control is None:
        yield
        return
    irimi_control: str = control
    prefix = urlsplit(control).path
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    class Gateway(Handler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers.get("content-length", 0)))
            if self.path.endswith(RECORDED_ROUTE):
                self.reply(502, {"error": "the gateway has no such route"})
                return
            sent = urllib.request.Request(
                irimi_control + self.path.removeprefix(prefix),
                body,
                {"Content-Type": self.headers.get("content-type", "application/json")},
                method="POST",
            )
            try:
                with opener.open(sent, timeout=5) as answer:
                    status, said = answer.status, answer.read()
            except urllib.error.HTTPError as refused:
                with refused:
                    status, said = refused.code, refused.read()
            self.send_response(status)
            self.send_header("content-length", str(len(said)))
            self.end_headers()
            self.wfile.write(said)

    with serve(Gateway) as port:
        os.environ[CONTROL_ENV] = f"http://127.0.0.1:{port}{prefix}"
        try:
            yield
        finally:
            os.environ[CONTROL_ENV] = control


def gzip_bomb(head: bytes, line: bytes, size: int, tail: bytes = b"") -> bytes:
    """Return gzip of `head`, `size` bytes of repeated `line`, then `tail`, in a few hundred KB.

    The padding is compressed once per `(line, size)` and spliced in as byte-aligned raw deflate
    blocks, so a second bomb costs only a CRC. `size` is a whole number of `_bomb_block(line)`s."""
    block = _bomb_block(line)
    if size % len(block):
        raise ValueError(f"size must be a multiple of {len(block)}")
    crc = zlib.crc32(head)
    for _ in range(size // len(block)):
        crc = zlib.crc32(block, crc)
    crc = zlib.crc32(tail, crc)
    first = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
    last = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
    # The gzip member header: no name, no mtime, no flags (RFC 1952), and its trailer.
    header = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff"
    trailer = struct.pack("<II", crc, (len(head) + size + len(tail)) & 0xFFFFFFFF)
    return b"".join(
        [
            header,
            first.compress(head) + first.flush(zlib.Z_SYNC_FLUSH),
            _bomb_padding(line, size),
            last.compress(tail) + last.flush(),
            trailer,
        ]
    )


def _bomb_block(line: bytes) -> bytes:
    """Return about a MiB of whole `line`s, the unit a bomb's padding is built of."""
    return line * max(1, (1024 * 1024) // len(line))


@functools.cache
def _bomb_padding(line: bytes, size: int) -> bytes:
    """Return `size` bytes of `line` as raw deflate blocks, none final, ending byte-aligned."""
    block = _bomb_block(line)
    deflater = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
    parts = [deflater.compress(block) for _ in range(size // len(block))]
    return b"".join([*parts, deflater.flush(zlib.Z_SYNC_FLUSH)])


def deliver(port: int, path: str, body: bytes, headers: dict[str, str] | None = None) -> Response:
    """POST one inbound call to our own server on `port`, through no proxy and unlogged."""
    sent = {"Content-Type": "application/json", **(headers or {})}
    request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", body, sent, method="POST")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return _open(opener.open, request, 60)
