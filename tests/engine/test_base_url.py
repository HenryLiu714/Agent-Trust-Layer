"""Base-URL mode, for SDKs that ignore proxy settings: requests addressed to the listener."""

import asyncio
import json
import socket
import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from irimi.config import ca, paths
from irimi.core import exchange
from irimi.core.exchange import Flag, Request
from irimi.decide.overlay import NoOverlay
from irimi.decide.policy import ShadowPolicy
from irimi.engine import EngineConfig
from irimi.engine.ingress import BaseUrlRefused, rewrite_base_url
from irimi.engine.mitm import MitmEngine
from irimi.protocol.trace import RunRecord
from irimi.report import model, summary
from irimi.report.model import RunView
from tests.support.doubles import KeptStore
from tests.support.engine import (
    engine_config,
    leaf_cert_for_loopback,
    start_engine,
    via_base_url,
    via_proxy,
)
from tests.support.upstreams import Upstream, serve, tls_context


def test_a_base_url_read_reaches_its_tls_upstream(tmp_path, monkeypatch):
    cfg = engine_config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    up = serve(
        Upstream, ssl_context=tls_context(leaf_cert_for_loopback(cfg.ca, tmp_path / "leaf.pem"))
    )
    eng, seen, stop = start_engine(cfg, trust_upstream_ca=cfg.ca.cert)
    try:
        status, data = via_base_url(eng.listen_port(), "GET", f"/127.0.0.1:{up}/hello?x=1")
    finally:
        stop()
        up.stop()
    assert (status, data) == (200, b"hello from upstream")
    assert len(seen) == 1
    ex = seen[0]
    assert ex.door == "reverse"
    assert ex.request.scheme == "https"
    assert ex.request.host == "127.0.0.1"
    assert ex.request.port == up
    assert ex.request.path == "/hello"
    assert ex.request.query == "x=1"
    assert ex.kind == "read"
    assert ex.answered_by == "live"
    assert ex.flags == ()
    assert ex.response.status == 200
    assert ("host", f"127.0.0.1:{up}") in ex.request.headers
    assert ex.service == "127.0.0.1"
    assert ex.operation == "GET /hello"


def test_a_base_url_request_to_an_unlisted_host_is_403(engine):
    """A base-URL request to an unlisted host gets irimi's JSON 403 and is recorded `refused`.

    A refused GET stays a read."""
    eng, seen = engine
    status, data = via_base_url(eng.listen_port(), "GET", "/api.stripe.com/v1")
    assert status == 403
    assert json.loads(data) == {
        "error": {
            "type": "irimi_refused",
            "message": "irimi refused this request: base-URL host 'api.stripe.com' is not in a "
            "loaded map or --base-url-host",
        }
    }
    (ex,) = seen
    assert (ex.answered_by, ex.kind, ex.door) == ("irimi", "read", "reverse")
    assert ex.flags == (Flag.REFUSED,)
    assert ex.request.path == "/api.stripe.com/v1"
    assert ex.response.status == 403
    assert not exchange.is_summary_write(ex)


def test_a_refused_post_is_still_listed_among_the_writes(engine):
    """A refused base-URL POST is listed in the summary as a write irimi refused."""
    eng, seen = engine
    status, _ = via_base_url(eng.listen_port(), "POST", "/api.stripe.com/v1/refunds", body=b"{}")
    assert status == 403
    (ex,) = seen
    assert (ex.answered_by, ex.kind) == ("irimi", "unknown")
    assert exchange.is_summary_write(ex)
    (entry,) = model.build(RunView(RunRecord.header("t3st"), [ex]), None).writes
    assert summary.write_sentence(entry).endswith("  refused by irimi")


def test_a_base_url_request_naming_no_host_is_refused(engine):
    eng, seen = engine
    status, data = via_base_url(eng.listen_port(), "GET", "/")
    assert status == 403
    assert "/<upstream-host>/<path>" in json.loads(data)["error"]["message"]
    (ex,) = seen
    assert (ex.answered_by, ex.flags) == ("irimi", (Flag.REFUSED,))


def test_a_base_url_write_is_faked(tmp_path, monkeypatch):
    cfg = engine_config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    eng, seen, stop = start_engine(cfg)
    try:
        status, data = via_base_url(
            eng.listen_port(), "POST", "/127.0.0.1:1/things", body=b'{"a":1}'
        )
    finally:
        stop()
    assert status == 200
    assert json.loads(data)["a"] == 1
    ex = seen[0]
    assert ex.door == "reverse"
    assert ex.answered_by == "fake-L0"
    assert ex.kind == "unknown"
    assert ex.request.host == "127.0.0.1"
    assert ex.request.port == 1
    assert ex.request.path == "/things"
    assert ex.request.body == b'{"a":1}'


def test_a_base_url_upstream_error_is_flagged(tmp_path, monkeypatch):
    cfg = engine_config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    eng, seen, stop = start_engine(cfg)
    try:
        status, _ = via_base_url(eng.listen_port(), "GET", "/127.0.0.1:1/x", host_name="localhost")
    finally:
        stop()
    assert status == 502
    ex = seen[0]
    assert ex.door == "reverse"
    assert ex.response is None
    assert "upstream-error" in ex.flags
    assert ex.request.host == "127.0.0.1"
    assert ex.request.port == 1
    assert ex.request.scheme == "https"


def test_an_absolute_url_naming_the_listener_is_base_url_mode(tmp_path, monkeypatch):
    cfg = engine_config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    eng, seen, stop = start_engine(cfg)
    port = eng.listen_port()
    try:
        status, _ = via_proxy(port, "GET", f"http://127.0.0.1:{port}/127.0.0.1:1/x")
    finally:
        stop()
    assert status == 502
    assert len(seen) == 1
    assert seen[0].door == "reverse"
    assert seen[0].request.port == 1
    assert "upstream-error" in seen[0].flags


@pytest.mark.parametrize("host_name", ["0.0.0.0", "127.1", "[::ffff:127.0.0.1]"])
def test_a_self_addressed_alias_is_base_url_mode_not_a_loop(tmp_path, monkeypatch, host_name):
    # Any spelling of "this listener" is base-URL mode; forwarding it would make the proxy
    # connect to itself and re-receive the same request until the ports run out.
    cfg = engine_config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    eng, seen, stop = start_engine(cfg)
    try:
        status, _ = via_base_url(eng.listen_port(), "GET", "/127.0.0.1:1/x", host_name=host_name)
    finally:
        stop()
    assert status == 502
    assert len(seen) == 1
    assert seen[0].door == "reverse"
    assert seen[0].request.port == 1


def test_a_base_url_request_without_a_host_header_gets_one(tmp_path, monkeypatch):
    # An HTTP/1.0 absolute-form request carries no Host header; the upstream still needs one.
    cfg = engine_config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    eng, seen, stop = start_engine(cfg)
    port = eng.listen_port()
    try:
        sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        sock.sendall(f"GET http://127.0.0.1:{port}/127.0.0.1:1/x HTTP/1.0\r\n\r\n".encode())
        raw = b""
        while chunk := sock.recv(65536):
            raw += chunk
        sock.close()
    finally:
        stop()
    assert b" 502 " in raw.split(b"\r\n", 1)[0]
    assert len(seen) == 1
    assert seen[0].door == "reverse"
    assert ("host", "127.0.0.1:1") in seen[0].request.headers


# ------------------------------------------- a refusal on a connection that is kept alive


@pytest.fixture
def kept_engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[int, KeptStore]]:
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    p = ca.ca_paths()
    ca.generate_ca(p)
    config = EngineConfig(
        run_id="t3st", ca=p, confdir=paths.mitm_dir(), listen_host="127.0.0.1", listen_port=0
    )
    store = KeptStore()
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
    """Send `first`, then `then(host)`, on one connection; return all read until the health JSON."""
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
    kept_engine: tuple[int, KeptStore],
) -> None:
    """A refused base-URL HEAD gets a 403 with no content or Content-Length, recorded as sent.

    Its JSON would be read as the start of the connection's next response."""
    port, store = kept_engine
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


def test_a_refused_get_still_carries_its_json(kept_engine: tuple[int, KeptStore]) -> None:
    """Only HEAD loses its content: a GET's refusal says why."""
    port, store = kept_engine
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


@pytest.mark.parametrize("path", ["/", "//v1/charges", "///api.stripe.com/v1/charges"])
def test_a_base_url_path_whose_first_segment_is_empty_names_no_host(path: str) -> None:
    """Only the one leading slash is taken off: `//v1/charges` is not the host `v1`."""
    request = Request("POST", "http", "127.0.0.1", 4000, path, "", (), b"")
    with pytest.raises(BaseUrlRefused, match="path must be /<upstream-host>/<path>"):
        rewrite_base_url(request, frozenset({"v1", "api.stripe.com"}))
