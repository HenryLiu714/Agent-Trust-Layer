"""The engine's own answers to HEAD carry no content, as the control endpoint's do; and the edges
of what it reads off a request: header names, a base-URL path, a posted tool call."""

import asyncio
import json
import socket
import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from irimi.config import ca, paths
from irimi.core.exchange import Exchange, Request, Response, header_value, without_header
from irimi.decide.overlay import NoOverlay
from irimi.decide.policy import ShadowPolicy
from irimi.engine import EngineConfig
from irimi.engine.control import ControlEndpoint
from irimi.engine.ingress import ReverseDoorRefused, rewrite_reverse
from irimi.engine.mitm import MitmEngine
from irimi.protocol import trace
from irimi.protocol.trace import ErrorInfo, RunRecord, RunStatus, ToolCall
from irimi.record.store import StoreStats


class _Kept:
    """A RunStore that keeps the exchanges it is handed."""

    def __init__(self) -> None:
        self.exchanges: list[Exchange] = []
        self.calls: list[ToolCall] = []

    def start_run(self, record: RunRecord) -> None:
        return None

    def record(self, exchange: Exchange) -> None:
        self.exchanges.append(exchange)

    def record_tool_call(self, call: ToolCall) -> None:
        self.calls.append(call)

    def end_run(
        self,
        run_id: str,
        ended_at: float,
        outcome: RunStatus,
        error: ErrorInfo | None = None,
        exit_code: int | None = None,
    ) -> None:
        return None

    def stats(self) -> StoreStats:
        return StoreStats(queued=0, written=0, dropped=0)

    def close(self) -> None:
        return None


@pytest.fixture
def engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[int, _Kept]]:
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    p = ca.ca_paths()
    ca.generate_ca(p)
    config = EngineConfig(
        run_id="t3st", ca=p, confdir=paths.mitm_dir(), listen_host="127.0.0.1", listen_port=0
    )
    store = _Kept()
    eng = MitmEngine(config, ShadowPolicy(), store, NoOverlay())
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=lambda: loop.run_until_complete(eng.run()), daemon=True)
    thread.start()
    asyncio.run_coroutine_threadsafe(eng.wait_ready(), loop).result(timeout=15)
    port = eng.listen_port()
    assert port is not None
    yield port, store
    eng.shutdown()
    thread.join(timeout=15)
    loop.close()


def _two_on_one_connection(port: int, first: str, then: Callable[[str], str]) -> bytes:
    """`first`, then `then(host)`, written at once on one connection, and everything read back
    until the health answer's JSON ends."""
    host = f"127.0.0.1:{port}"
    sent = first.replace("{host}", host) + then(host)
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.sendall(sent.encode())
        received = b""
        while not (b"engine_version" in received and received.endswith(b"}")):
            chunk = sock.recv(65536)
            assert chunk, f"the connection closed after {received!r}"
            received += chunk
    return received


def _health(host: str) -> str:
    return f"GET /_irimi/health HTTP/1.1\r\nhost: {host}\r\n\r\n"


def test_a_refused_head_gets_no_body_and_the_connection_stays_usable(
    engine: tuple[int, _Kept],
) -> None:
    """A base-URL HEAD for a host irimi does not relay is refused 403. Its JSON would be read as
    the start of the connection's next response, so it goes without content or a Content-Length,
    and is recorded as sent."""
    port, store = engine
    received = _two_on_one_connection(
        port, "HEAD /evil.example.com/x HTTP/1.1\r\nhost: {host}\r\n\r\n", _health
    )
    head, rest = received.split(b"\r\n\r\n", 1)
    assert head.startswith(b"HTTP/1.1 403 ")
    assert b"\r\nirimi-answered-by: irimi" in head
    assert b"content-length" not in head.lower()
    assert rest.startswith(b"HTTP/1.1 200 ")
    (refused,) = store.exchanges
    assert refused.request.method == "HEAD"
    assert refused.answered_by == "irimi"
    assert refused.response is not None and refused.response.body == b""


def test_a_refused_get_still_carries_its_json(engine: tuple[int, _Kept]) -> None:
    """Only HEAD loses its content: a GET's refusal says why."""
    port, store = engine
    received = _two_on_one_connection(
        port, "GET /evil.example.com/x HTTP/1.1\r\nhost: {host}\r\n\r\n", _health
    )
    head, rest = received.split(b"\r\n\r\n", 1)
    assert head.startswith(b"HTTP/1.1 403 ")
    body, _, after = rest.partition(b"HTTP/1.1 200 ")
    assert json.loads(body)["error"]["type"] == "irimi_refused"
    assert after
    (refused,) = store.exchanges
    assert refused.response is not None and json.loads(refused.response.body)["error"]


@pytest.mark.parametrize(
    ("cause", "said"),
    [
        (
            OSError(13, "http(s) proxy failed to listen on 127.0.0.1:1 with [Errno 13] denied"),
            "could not listen on 127.0.0.1:1: permission denied",
        ),
        (
            OSError("http(s) proxy failed to listen: [Errno None] no route"),
            "could not listen on 127.0.0.1:1",
        ),
        (None, "could not listen on 127.0.0.1:1"),
        (RuntimeError("loop closed"), "could not listen on 127.0.0.1:1: loop closed"),
    ],
)
def test_a_listener_that_did_not_bind_is_told_in_irimi_s_words(
    cause: BaseException | None, said: str
) -> None:
    """mitmproxy's strerror is its own sentence; only the OS's reason for the errno is irimi's."""
    from irimi.engine.mitm import _start_failure

    config = EngineConfig(
        run_id="t3st", ca=ca.ca_paths(), confdir=Path("."), listen_host="127.0.0.1", listen_port=1
    )
    assert _start_failure(config, cause) == said


@pytest.mark.parametrize("name", ["irimi-answered-by", "Irimi-Answered-By", "IRIMI-ANSWERED-BY"])
def test_a_header_is_found_and_removed_however_its_name_is_spelled(name: str) -> None:
    headers = (("Irimi-Answered-By", "irimi"), ("content-type", "application/json"))
    assert header_value(headers, name) == "irimi"
    assert without_header(headers, name) == (("content-type", "application/json"),)
    response = Response(200, headers, b"{}")
    assert response.without_header(name).headers == (("content-type", "application/json"),)


@pytest.mark.parametrize("path", ["/", "//v1/charges", "///api.stripe.com/v1/charges"])
def test_a_base_url_path_whose_first_segment_is_empty_names_no_host(path: str) -> None:
    """Only the one leading slash is taken off: `//v1/charges` is not the host `v1`."""
    request = Request("POST", "http", "127.0.0.1", 4000, path, "", (), b"")
    with pytest.raises(ReverseDoorRefused, match="path must be /<upstream-host>/<path>"):
        rewrite_reverse(request, frozenset({"v1", "api.stripe.com"}))


def test_a_tool_call_posted_without_awaited_is_refused_and_the_sdk_always_sends_it() -> None:
    """`awaited` is required: 0.1.0's SDK posted it on every call and its store wrote it on every
    line, so only a post made by hand can lack it, and it is told which field."""
    call = ToolCall("tc1", "run1", "m:f", "write", "shadow", {}, None, None, 1.0, 2.0)
    posted = trace.tool_call_to_json(call)
    assert posted["awaited"] is False
    del posted["run_id"], posted["awaited"]
    request = Request(
        "POST",
        "http",
        "127.0.0.1",
        4000,
        "/_irimi/runs/run1/tool-calls",
        "",
        (),
        json.dumps(posted).encode(),
    )
    store = _Kept()
    answer = ControlEndpoint(store, serve=False).answer(request)
    assert answer.status == 400 and store.calls == []
    assert json.loads(answer.body)["error"]["message"] == "missing required field 'awaited'"
