"""The fake internet: one loopback server that answers for every host a sample workflow talks to.

A workflow's agent calls the real host names (`api.stripe.com`, `slack.com`, `api.anthropic.com`),
so irimi classifies them with its real, shipped maps. Only the address those names resolve to is
fake, and it is fake in exactly one place: `FakeInternet.resolving()` patches `socket.getaddrinfo`
in the process that runs irimi, so mitmproxy's upstream connection and irimi's own L3 read both
land here. Any name the fake internet does not serve fails to resolve, so a harness run can never
reach the real internet.

The agent itself never resolves these names:

- under `irimi shadow` it sends every request to irimi's proxy (the names are not in `NO_PROXY`);
- in a bare run the harness points `HTTP_PROXY` at this server, which accepts the absolute-form
  request a client sends to a proxy and answers it as the host it names.

Every request is recorded in `log` before it is answered. A write that reaches `log` under shadow
is a write that escaped, and that is the first universal invariant (`run.check_invariants`).
"""

from __future__ import annotations

import contextlib
import gzip
import ipaddress
import json
import socket
import struct
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol
from urllib.parse import parse_qsl, urlsplit

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
# How long a `stall` fault holds a request before answering. Agents under test use a shorter
# client timeout, so a stall reads to them as a timeout.
STALL_S = 2.0


@dataclass(frozen=True)
class Req:
    """One request as the fake internet received it."""

    method: str
    host: str
    path: str
    query: dict[str, str]
    headers: dict[str, str]  # names lower-cased
    body: bytes  # as sent, before any content-encoding is undone
    via_proxy: bool  # arrived in absolute form, i.e. from a bare run through HTTP_PROXY

    def text(self) -> str:
        raw = self.body
        if self.headers.get("content-encoding") == "gzip":
            try:
                raw = gzip.decompress(raw)
            except OSError:
                return ""
        return raw.decode("utf-8", "replace")

    def json(self) -> Any:
        try:
            return json.loads(self.text())
        except ValueError:
            return None

    def params(self) -> dict[str, Any]:
        """The query plus the body's fields, JSON or form, flat. Slack takes either body."""
        out: dict[str, Any] = dict(self.query)
        doc = self.json()
        if isinstance(doc, dict):
            out.update(doc)
        elif self.body:
            out.update(parse_qsl(self.text(), keep_blank_values=True))
        return out


@dataclass
class Resp:
    """What a service answers. `body` is JSON for a dict or list, text for a str."""

    status: int = 200
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    # A streamed answer: each chunk is written and flushed on its own, `chunk_delay` apart.
    chunks: list[bytes] | None = None
    chunk_delay: float = 0.0


class Service(Protocol):
    """A fake service: the hosts it answers for, its answers, and which of its requests are
    writes. `is_write` is the service's own truth, not irimi's classification: a Slack read is a
    POST, and an LLM call is a POST that changes nothing."""

    hosts: tuple[str, ...]

    def handle(self, req: Req) -> Resp: ...

    def is_write(self, req: Req) -> bool: ...


@dataclass
class Fault:
    """Answer the next `times` matching requests with `action` instead of the service.

    `429`, `500` and `garbage` answer at once; `stall` answers late; `reset` resets the connection
    before any response; `reset-stream` lets the service answer, sends the headers and the first
    chunk, and then resets. A faulted request is still recorded in `log`.
    """

    host: str
    action: str
    match: Callable[[Req], bool] = lambda req: True
    times: int = 1


def json_error(status: int, **fields: Any) -> Resp:
    return Resp(status, fields)


class FakeInternet:
    def __init__(self, services: list[Service]) -> None:
        self.services: dict[str, Service] = {}
        for service in services:
            for host in service.hosts:
                self.services[host.lower()] = service
        self.log: list[Req] = []
        self.faults: list[Fault] = []
        self._lock = threading.Lock()
        self._server: ThreadingHTTPServer | None = None

    # -- lifecycle -------------------------------------------------------------------------

    def start(self) -> int:
        internet = self

        class Handler(_Handler):
            owner = internet

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        server = self._server
        threading.Thread(
            target=lambda: server.serve_forever(poll_interval=0.01), daemon=True
        ).start()
        return self.port

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

    @property
    def port(self) -> int:
        assert self._server is not None, "the fake internet is not started"
        return int(self._server.server_address[1])

    def base(self, host: str) -> str:
        """The base URL an agent is given for `host`: the real name, this server's port."""
        return f"http://{host}:{self.port}"

    @contextlib.contextmanager
    def resolving(self) -> Iterator[None]:
        """Resolve every host this fake internet serves to loopback, and refuse every other name,
        for the duration of the block and in this process only."""
        real = socket.getaddrinfo
        served = frozenset(self.services)

        def fake(host: Any, port: Any, *args: Any, **kwargs: Any) -> Any:
            name = host.decode() if isinstance(host, bytes) else host
            if name is None or _is_local(name):
                return real(host, port, *args, **kwargs)
            if name.lower().rstrip(".") in served:
                return real("127.0.0.1", port, *args, **kwargs)
            raise socket.gaierror(socket.EAI_NONAME, f"{name} is not on the fake internet")

        socket.getaddrinfo = fake
        try:
            yield
        finally:
            socket.getaddrinfo = real

    # -- what happened ---------------------------------------------------------------------

    def inject(
        self,
        host: str,
        action: str,
        match: Callable[[Req], bool] = lambda req: True,
        times: int = 1,
    ) -> None:
        self.faults.append(Fault(host.lower(), action, match, times))

    def requests(self, host: str | None = None) -> list[Req]:
        with self._lock:
            return [r for r in self.log if host is None or r.host == host]

    def writes(self) -> list[Req]:
        """Every request the service it reached counts as a write."""
        with self._lock:
            log = list(self.log)
        return [r for r in log if r.host in self.services and self.services[r.host].is_write(r)]

    # -- serving ---------------------------------------------------------------------------

    def _take_fault(self, req: Req) -> Fault | None:
        with self._lock:
            self.log.append(req)
            for fault in self.faults:
                if fault.host == req.host and fault.times > 0 and fault.match(req):
                    fault.times -= 1
                    return fault
        return None

    def answer(self, req: Req) -> tuple[Resp | None, str | None]:
        fault = self._take_fault(req)
        action = fault.action if fault is not None else None
        if action == "429":
            return json_error(429, error={"type": "rate_limit_error"}), None
        if action == "500":
            return json_error(500, error={"type": "api_error"}), None
        if action == "garbage":
            return Resp(
                200, "<html>upstream proxy error</html>", {"content-type": "text/html"}
            ), None
        if action == "reset":
            return None, "reset"
        if action == "stall":
            time.sleep(STALL_S)
        service = self.services.get(req.host)
        if service is None:
            return json_error(502, error=f"{req.host} is not on the fake internet"), None
        try:
            resp = service.handle(req)
        except Exception as exc:  # a bug in a fake must read as a failure, not hang the agent
            return json_error(500, error=f"fake {req.host} raised {exc!r}"), None
        return resp, action


def _is_local(name: str) -> bool:
    if name.lower() == "localhost":
        return True
    try:
        ipaddress.ip_address(name.split("%", 1)[0])
    except ValueError:
        return False
    return True


class _Handler(BaseHTTPRequestHandler):
    owner: FakeInternet
    protocol_version = "HTTP/1.0"  # a streamed body ends when the connection closes

    def _read_body(self) -> bytes:
        if "chunked" in (self.headers.get("transfer-encoding") or "").lower():
            out = b""
            while True:
                size = int(self.rfile.readline().split(b";", 1)[0].strip() or b"0", 16)
                if size == 0:
                    self.rfile.readline()
                    return out
                out += self.rfile.read(size)
                self.rfile.readline()
        length = int(self.headers.get("content-length") or 0)
        return self.rfile.read(length) if length else b""

    def _serve(self) -> None:
        body = self._read_body()
        target = self.path
        via_proxy = target.startswith(("http://", "https://"))
        if via_proxy:
            parts = urlsplit(target)
            host, path, query = parts.hostname or "", parts.path or "/", parts.query
        else:
            host = _hostname(self.headers.get("host") or "")
            path, _, query = target.partition("?")
        req = Req(
            method=self.command,
            host=host.lower(),
            path=path,
            query=dict(parse_qsl(query, keep_blank_values=True)),
            headers={k.lower(): v for k, v in self.headers.items()},
            body=body,
            via_proxy=via_proxy,
        )
        resp, action = self.owner.answer(req)
        if resp is None:
            self._reset()
            return
        try:
            self._write(resp, reset_after_first_chunk=action == "reset-stream")
        except (BrokenPipeError, ConnectionResetError):
            pass  # the client hung up mid-answer, which some scenarios do on purpose

    def _write(self, resp: Resp, reset_after_first_chunk: bool) -> None:
        headers = dict(resp.headers)
        if resp.chunks is not None:
            headers.setdefault("content-type", "text/event-stream")
            self.send_response(resp.status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            for i, chunk in enumerate(resp.chunks):
                self.wfile.write(chunk)
                self.wfile.flush()
                if reset_after_first_chunk and i == 0:
                    self._reset()
                    return
                if resp.chunk_delay:
                    time.sleep(resp.chunk_delay)
            return
        if isinstance(resp.body, (dict, list)):
            payload = json.dumps(resp.body).encode()
            headers.setdefault("content-type", "application/json")
        elif isinstance(resp.body, str):
            payload = resp.body.encode()
            headers.setdefault("content-type", "text/plain")
        elif isinstance(resp.body, bytes):
            payload = resp.body
        else:
            payload = b""
        self.send_response(resp.status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def _reset(self) -> None:
        """Close with RST rather than FIN: what a dropped upstream looks like to a client."""
        with contextlib.suppress(OSError):
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            self.connection.close()
        self.close_connection = True

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = do_OPTIONS = _serve

    def log_message(self, format: str, *args: Any) -> None:
        pass


def _hostname(host_header: str) -> str:
    if host_header.startswith("["):
        return host_header[1 : host_header.find("]")]
    name, sep, port = host_header.rpartition(":")
    return name if sep and port.isdigit() else host_header
