"""Loopback servers that stand where a real service would.

`serve(Handler, **state)` starts one on a free port and returns its `Port`: the port number, which
also carries the server's own request log and state, so no two servers, and no two tests, share
either. `FakeStripe`, `FakeSlack` and `FakeLlm` are configured per server through `state`.
"""

import gzip
import json
import re
import socket
import ssl
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from socketserver import BaseServer
from typing import Any, NamedTuple

from tests.support.servers import stop_server


class Port(int):
    """A loopback server's port. It is the port wherever an int is (`f"http://127.0.0.1:{port}"`),
    and its attributes are its handler's per-server state: `port.seen` is what this server heard.
    """

    handler: type[BaseHTTPRequestHandler]
    server: BaseServer

    def __getattr__(self, name: str) -> Any:
        return getattr(self.handler, name)

    def stop(self) -> None:
        stop_server(self.server)


class Logged(BaseHTTPRequestHandler):
    """A handler that writes each request into its own server's `seen`, which `serve` sets."""

    seen: list

    @classmethod
    def per_server(cls) -> dict[str, Any]:
        """The state each server gets afresh, before `serve`'s own."""
        return {"seen": []}

    def body(self) -> bytes:
        length = int(self.headers.get("content-length") or 0)
        return self.rfile.read(length) if length else b""

    def send_json(
        self,
        status: int,
        document: object,
        content_type: str = "application/json",
        headers: tuple[tuple[str, str], ...] = (),
    ) -> None:
        payload = json.dumps(document).encode()
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def send_empty(self, status: int) -> None:
        self.send_response(status)
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, *args):  # keep pytest output clean
        pass


def serve(
    handler: type[BaseHTTPRequestHandler],
    *,
    threaded: bool = False,
    ssl_context: ssl.SSLContext | None = None,
    **state: Any,
) -> Port:
    """Serve `handler` on a free loopback port until `port.stop()`. The server gets a handler class
    of its own, holding `handler.per_server()` and then `state`. Threaded when a test needs a
    request answered while another is still being handled, as irimi's check read is."""
    fresh = handler.per_server() if issubclass(handler, Logged) else {}
    own = type(handler.__name__, (handler,), {**fresh, **state})
    server = (ThreadingHTTPServer if threaded else HTTPServer)(("127.0.0.1", 0), own)
    if ssl_context is not None:
        server.socket = ssl_context.wrap_socket(server.socket, server_side=True)
    # A short poll interval keeps shutdown() from blocking for the default 0.5 s.
    threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True).start()
    port = Port(server.server_address[1])
    port.handler = own
    port.server = server
    return port


def tls_context(leaf_pem: Path) -> ssl.SSLContext:
    """A server-side TLS context presenting `leaf_pem`'s certificate and key."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(leaf_pem)
    return context


def refusing_port() -> int:
    """A loopback port nothing listens on: bound by this process to claim a free one, then closed.

    A port held bound without listening would be surer, but macOS lets a connection to one time
    out instead of refusing it.
    """
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# A plain upstream, whose answers a few paths shape.


class Upstream(Logged):
    """Answers every GET with plain text, and logs `(path, header pairs)`. Pairs, not a dict: a
    dict keeps one of a repeated name, and "exactly once on the wire" is a claim. A POST is
    answered 500, since under shadow mode none may arrive."""

    def do_GET(self):
        self.seen.append((self.path, self.headers.items()))
        body = b"hello from upstream"
        self.send_response(200)
        if self.path == "/badgzip":  # claims gzip, is not: an undecodable body
            body = b"not-gzip"
            self.send_header("content-encoding", "gzip")
        if self.path.startswith("/encoded/"):  # claims the encoding its path names, is plain
            content_encoding = self.path.removeprefix("/encoded/")
            body = b"not-" + content_encoding.encode()
            self.send_header("content-encoding", content_encoding)
        if self.path == "/slack-history":  # a Slack read whose real `ts` the run has to see
            body = b'{"ok": true, "messages": [{"ts": "1999999999.000500"}]}'
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        self.send_response(500)
        self.end_headers()


SSE_FIRST = b'data: {"delta": "one"}\n\n'
SSE_SECOND = b'data: {"delta": "two"}\n\n'


class StreamUpstream(Logged):
    """Answers with two server-sent events, holding the second until the test sets `port.gate`,
    so a buffered response cannot reach the client at all inside the client's socket timeout."""

    protocol_version = "HTTP/1.1"  # chunked transfer encoding needs HTTP/1.1
    gate: threading.Event

    @classmethod
    def per_server(cls) -> dict[str, Any]:
        return {**super().per_server(), "gate": threading.Event()}

    def do_POST(self):
        self.body()
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("transfer-encoding", "chunked")
        self.end_headers()
        self._chunk(SSE_FIRST)
        self.gate.wait(timeout=20)
        self._chunk(SSE_SECOND)
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()
        # Close rather than keep the connection alive: mitmproxy pools its upstream connections,
        # and a single-threaded HTTPServer whose handler is still waiting for a second request on
        # that socket never returns from serve_forever, so shutdown() would block forever.
        self.close_connection = True

    do_GET = do_POST  # the same stream for a `kind: read` route

    def _chunk(self, payload: bytes) -> None:
        self.wfile.write(b"%x\r\n" % len(payload) + payload + b"\r\n")
        self.wfile.flush()


NOT_GZIP = b"not-actually-gzip"
REAL_GZIP = gzip.compress(b'{"from": "target"}')


class TargetStub(Logged):
    """A developer's own stub, the kind an answer target names: it logs `(method, path, body,
    headers)` and answers its own JSON."""

    def _handle(self):
        self.seen.append((self.command, self.path, self.body(), dict(self.headers)))
        body = json.dumps({"from": "target", "path": self.path}).encode()
        self.send_response(200)
        if self.path.endswith("/badgzip"):  # claims gzip, is not: an undecodable body
            body = NOT_GZIP
            self.send_header("content-encoding", "gzip")
        elif self.path.endswith("/gzip"):
            body = REAL_GZIP
            self.send_header("content-encoding", "gzip")
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = _handle


# Stripe.


class Answer(NamedTuple):
    """One answer a fake gives in place of what its tables say."""

    status: int
    document: object
    headers: tuple[tuple[str, str], ...] = ()


CHARGE_PATH = re.compile(r"/v1/charges/([A-Za-z0-9_]+)")
RESOURCE_MISSING = Answer(
    404, {"error": {"type": "invalid_request_error", "code": "resource_missing"}}
)


def stripe_list(*items: object, has_more: bool | None = False) -> dict[str, object]:
    """A Stripe list object holding `items`; `has_more=None` leaves the key out."""
    document: dict[str, object] = {"object": "list"}
    if has_more is not None:
        document["has_more"] = has_more
    document["data"] = list(items)
    return document


class FakeStripe(Logged):
    """A loopback Stripe, configured per server.

    A GET of a path is answered from, in order:

    - `answers`: {path: Answer}, a key ending in `/` covering every path under it;
    - `charges`: {charge id: document} at `/v1/charges/{id}`;
    - `documents`: {path: document};
    - `any_charge(handler, id)`: a document for any other charge id, or None;
    - `not_found`.

    `slow` holds a path's answer back that many seconds, `held` until `port.release` is set
    (`port.arrived` is set as it arrives), and `hang_up` closes the connection unanswered. Every
    POST is answered 500: under shadow mode none may arrive. `seen` logs `(method, path)`,
    `heard` each request's header pairs, `bodies` each request's body.
    """

    charges: dict[str, object] = {}
    documents: dict[str, object] = {}
    answers: dict[str, Answer] = {}
    any_charge: Callable[[str], object] | None = None
    not_found: Answer = RESOURCE_MISSING
    slow: dict[str, float] = {}
    held: frozenset[str] = frozenset()
    hang_up: frozenset[str] = frozenset()
    heard: list
    bodies: list
    arrived: threading.Event
    release: threading.Event

    @classmethod
    def per_server(cls) -> dict[str, Any]:
        return {
            **super().per_server(),
            "heard": [],
            "bodies": [],
            "arrived": threading.Event(),
            "release": threading.Event(),
        }

    def _log(self) -> None:
        self.seen.append((self.command, self.path))
        self.heard.append(self.headers.items())
        self.bodies.append(self.body())

    def do_GET(self):
        self._log()
        route = self.path.split("?", 1)[0]
        if route in self.hang_up:  # an upstream error: no answer at all
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            return
        if route in self.slow:
            time.sleep(self.slow[route])
        if route in self.held:
            self.arrived.set()
            self.release.wait(10)
        status, document, headers = self._answer(route)
        self.send_json(status, document, headers=headers)

    def _answer(self, route: str) -> Answer:
        if route in self.answers:
            return self.answers[route]
        for prefix, answer in self.answers.items():
            if prefix.endswith("/") and route.startswith(prefix):
                return answer
        charge = CHARGE_PATH.fullmatch(route)
        if charge and charge.group(1) in self.charges:
            return Answer(200, self.charges[charge.group(1)])
        if route in self.documents:
            return Answer(200, self.documents[route])
        if charge and self.any_charge is not None:
            document = self.any_charge(charge.group(1))  # bound: called as (handler, id)
            if document is not None:
                return Answer(200, document)
        return self.not_found

    def do_POST(self):
        self._log()
        self.send_empty(500)


# Slack.


class FakeSlack(Logged):
    """A loopback Slack. `methods` maps each Web API path it answers to a function of the posted
    JSON that returns the document, sent at 200 as Slack sends it. Any other POST is answered 500:
    a `chat.postMessage` arriving here is a faked post that escaped. `seen` logs `(method, path)`.
    """

    methods: dict[str, Callable[[dict[str, Any]], object]] = {}

    def do_POST(self):
        self.seen.append(("POST", self.path))
        method = self.methods.get(self.path)
        if method is None:
            self.send_empty(500)
            return
        posted = json.loads(self.body() or b"{}")
        self.send_json(200, method(posted), content_type="application/json; charset=utf-8")


# A model.


class FakeLlm(Logged):
    """A loopback model. `reply(handler, document)` returns each answer's `(status, document)`; a
    document asking to stream gets `stream(handler, document)`'s parts as server-sent events
    instead, when `stream` is set. `seen` logs every posted document."""

    reply: Callable[..., tuple[int, object]]
    stream: Callable[..., list[object]] | None = None

    def do_POST(self):
        document = json.loads(self.body())
        self.seen.append(document)
        if self.stream is not None and document.get("stream") is True:
            self.send_response(200)
            self.send_header("content-type", "text/event-stream")
            self.end_headers()
            for part in self.stream(document):
                self.wfile.write(f"data: {json.dumps(part)}\n\n".encode())
                self.wfile.flush()
            return
        status, answer = self.reply(document)
        self.send_json(status, answer)


# The fakes as the tests configure them.

SLOW_S = 1.0  # how long `ch_SLOW1`'s precondition read takes to answer


def stripe_charge(charge_id, amount_refunded=0):
    """A succeeded, paid 4900-cent `usd` charge, refunded by `amount_refunded`."""
    return {
        "id": charge_id,
        "object": "charge",
        "amount": 4900,
        "amount_refunded": amount_refunded,
        "refunded": amount_refunded == 4900,
        "currency": "usd",
        "status": "succeeded",
        "paid": True,
    }


def precondition_stripe() -> dict[str, Any]:
    """The real Stripe as irimi's precondition check sees it: one refundable charge, one fully
    refunded, one slow to answer, one rate-limited and one that redirects to the fully refunded
    one. `serve(FakeStripe, threaded=True, **precondition_stripe())`."""
    return {
        "charges": {
            "ch_REAL1": stripe_charge("ch_REAL1"),
            "ch_FULL1": stripe_charge("ch_FULL1", amount_refunded=4900),
            "ch_SLOW1": stripe_charge("ch_SLOW1"),
        },
        "slow": {"/v1/charges/ch_SLOW1": SLOW_S},
        "answers": {
            "/v1/charges/ch_BUSY1": Answer(429, {"error": {"type": "rate_limit_error"}}),
            "/v1/charges/ch_MOVED1": Answer(
                302, {"moved_to": "ch_FULL1"}, (("location", "/v1/charges/ch_FULL1"),)
            ),
        },
        "documents": {
            "/v1/charges": stripe_list(stripe_charge("ch_REAL1")),
            "/v1/refunds": stripe_list(),
        },
        "not_found": Answer(404, {"error": {"type": "invalid_request_error"}}),
    }


REPLAY_CHARGE = {
    "id": "ch_REAL1",
    "object": "charge",
    "amount": 4900,
    "amount_refunded": 0,
    "refunded": False,
    "currency": "usd",
}


def replay_stripe() -> dict[str, Any]:
    """A Stripe that answers a charge, hangs up on `ch_RESET`, and refuses every id it never
    minted. `serve(FakeStripe, threaded=True, **replay_stripe())`."""
    return {
        "charges": {"ch_REAL1": REPLAY_CHARGE},
        "documents": {"/v1/refunds": stripe_list()},
        "hang_up": frozenset({"/v1/charges/ch_RESET"}),
        "answers": {},
        "not_found": Answer(404, {"error": {"code": "resource_missing"}}),
    }


def _replay_reply(handler, document):
    """The system prompt and a counter, so two answers to two calls are never the same bytes."""
    text = f"{document.get('system', '')} #{len(handler.seen)}"
    return 200, {"id": "msg_1", "content": [{"type": "text", "text": text}]}


def _replay_stream(handler, document):
    text = f"{document.get('system', '')} #{len(handler.seen)}"
    return [{"part": part} for part in ("message_start", text, "message_stop")]


def replay_llm() -> dict[str, Any]:
    """A model answering with its system prompt and a counter, three server-sent events when asked
    to stream. `serve(FakeLlm, threaded=True, **replay_llm())`."""
    return {"reply": _replay_reply, "stream": _replay_stream}


class ReplayStubs(NamedTuple):
    """The Stripe and the model a replay test records against and replays over."""

    stripe: Port
    llm: Port

    def forget(self) -> None:
        """Forget every request either has heard, so a test sees only what comes next."""
        self.stripe.seen.clear()
        self.llm.seen.clear()
