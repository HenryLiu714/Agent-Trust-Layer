import asyncio
import http.client
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from irimi import ca, paths
from irimi.engine import EngineConfig, EngineStartError
from irimi.engine.mitm import MitmEngine
from irimi.exchange import Response
from irimi.overlay import NoOverlay
from irimi.policy import ShadowPolicy
from irimi.store import NullStore


class _Upstream(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"hello from upstream"
        self.send_response(200)
        if self.path == "/badgzip":  # claims gzip, is not: an undecodable body
            body = b"not-gzip"
            self.send_header("content-encoding", "gzip")
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):  # must never be reached under ShadowPolicy
        self.send_response(500)
        self.end_headers()

    def log_message(self, *args):  # keep pytest output clean
        pass


@pytest.fixture
def upstream():
    srv = HTTPServer(("127.0.0.1", 0), _Upstream)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield srv.server_address[1]
    srv.shutdown()


def _config(tmp_path, monkeypatch, port=0):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    p = ca.ca_paths()
    ca.generate_ca(p)
    return EngineConfig(
        run_id="t3st", ca=p, confdir=paths.mitm_dir(), listen_host="127.0.0.1", listen_port=port
    )


def _start(cfg, overlay=None):
    """Serve `cfg` on a background loop until the returned stop() is called."""
    seen = []
    eng = MitmEngine(
        cfg,
        policy=ShadowPolicy(),
        store=NullStore(),
        overlay=overlay or NoOverlay(),
        on_exchange=seen.append,
    )
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=lambda: loop.run_until_complete(eng.run()), daemon=True)
    t.start()
    asyncio.run_coroutine_threadsafe(eng.wait_ready(), loop).result(timeout=15)

    def stop():
        eng.shutdown()
        t.join(timeout=15)
        loop.close()

    return eng, seen, stop


@pytest.fixture
def engine(tmp_path, monkeypatch):
    eng, seen, stop = _start(_config(tmp_path, monkeypatch))
    yield eng, seen
    stop()


def _via_proxy(proxy_port, method, url, body=None, extra_headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", proxy_port, timeout=10)
    headers = {"host": url.split("/")[2]}
    if body is not None:
        headers["content-type"] = "application/json"
    headers.update(extra_headers or {})
    conn.request(method, url, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def test_get_is_forwarded_live(engine, upstream):
    eng, seen = engine
    status, data = _via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    assert (status, data) == (200, b"hello from upstream")
    assert len(seen) == 1
    ex = seen[0]
    assert ex.answered_by == "live"
    assert ex.kind == "read"
    assert ex.service == "127.0.0.1"
    assert ex.operation == "GET /hello"
    assert ex.run_id == "t3st"
    assert ex.validation == "unvalidated"
    assert ex.flags == ()
    assert ex.response.status == 200


def test_post_is_faked_l0(engine, upstream):
    eng, seen = engine
    status, data = _via_proxy(
        eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b'{"a":1}'
    )
    assert status == 200
    assert json.loads(data) == {}
    ex = seen[0]
    assert ex.answered_by == "fake-L0"
    assert ex.kind == "unknown"
    assert "unclassified" in ex.flags
    assert ex.validation == "unvalidated"


def test_run_header_attributes_run(engine, upstream):
    eng, seen = engine
    _via_proxy(
        eng.listen_port(),
        "GET",
        f"http://127.0.0.1:{upstream}/hello",
        extra_headers={"irimi-run": "abcd"},
    )
    assert seen[0].run_id == "abcd"


def test_upstream_error_is_flagged(engine, upstream):
    eng, seen = engine
    status, _ = _via_proxy(eng.listen_port(), "GET", "http://127.0.0.1:1/x")
    assert status == 502
    assert seen[0].response is None
    assert "upstream-error" in seen[0].flags


def test_bundle_written_from_irimi_ca(engine, upstream):
    bundle = paths.mitm_dir() / "mitmproxy-ca.pem"
    assert bundle.is_file()
    assert ca.ca_paths().cert.read_bytes() in bundle.read_bytes()


def test_undecodable_request_body_is_still_faked(engine, upstream):
    # A strict body decode raising inside the hook would make mitmproxy forward the write.
    eng, seen = engine
    status, data = _via_proxy(
        eng.listen_port(),
        "POST",
        f"http://127.0.0.1:{upstream}/things",
        body=b'{"not":"gzip"}',
        extra_headers={"content-encoding": "gzip"},
    )
    assert status == 200  # the upstream answers every POST with 500, so it was not reached
    assert json.loads(data) == {}
    assert [ex.answered_by for ex in seen] == ["fake-L0"]
    assert seen[0].request.body == b'{"not":"gzip"}'


def test_undecodable_upstream_body_is_still_recorded(engine, upstream):
    eng, seen = engine
    status, data = _via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/badgzip")
    assert (status, data) == (200, b"not-gzip")
    assert len(seen) == 1
    assert seen[0].answered_by == "live"
    assert seen[0].response.body == b"not-gzip"


def test_overlay_output_reaches_the_client(tmp_path, monkeypatch, upstream):
    def overlay(write_log, read_request, upstream_response):
        return Response(200, (("content-type", "text/plain"),), b"OVERLAID")

    eng, seen, stop = _start(_config(tmp_path, monkeypatch), overlay=overlay)
    try:
        status, data = _via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    finally:
        stop()
    assert (status, data) == (200, b"OVERLAID")
    assert seen[0].response.body == b"OVERLAID"


def test_repeated_headers_survive_a_local_answer(tmp_path, monkeypatch, upstream):
    def overlay(write_log, read_request, upstream_response):
        return Response(200, (("set-cookie", "a=1"), ("set-cookie", "b=2")), b"")

    eng, seen, stop = _start(_config(tmp_path, monkeypatch), overlay=overlay)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=10)
        conn.request("GET", f"http://127.0.0.1:{upstream}/hello", headers={"host": "127.0.0.1"})
        resp = conn.getresponse()
        resp.read()
        cookies = resp.headers.get_all("set-cookie")
        conn.close()
    finally:
        stop()
    assert cookies == ["a=1", "b=2"]


def test_port_in_use_raises_engine_start_error(tmp_path, monkeypatch):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    cfg = _config(tmp_path, monkeypatch, port=sock.getsockname()[1])
    eng = MitmEngine(cfg, ShadowPolicy(), NullStore(), NoOverlay())

    async def go():
        task = asyncio.ensure_future(eng.run())
        with pytest.raises(EngineStartError, match="did not start"):
            await asyncio.wait_for(eng.wait_ready(), 15)
        with pytest.raises(EngineStartError):
            await asyncio.wait_for(task, 15)

    try:
        asyncio.run(go())
    finally:
        sock.close()
    assert eng.listen_port() is None


def test_shutdown_before_run_makes_run_return(tmp_path, monkeypatch):
    eng = MitmEngine(_config(tmp_path, monkeypatch), ShadowPolicy(), NullStore(), NoOverlay())

    async def go():
        task = asyncio.ensure_future(eng.run())
        eng.shutdown()
        await asyncio.wait_for(task, 15)
        with pytest.raises(EngineStartError, match="stopped before"):
            await eng.wait_ready()

    asyncio.run(go())


def test_store_is_closed_when_setup_fails(tmp_path, monkeypatch):
    closed = []

    class _Store:
        def record(self, ex):
            pass

        def close(self):
            closed.append(True)

    cfg = _config(tmp_path, monkeypatch)
    cfg.ca.key.unlink()  # bundle write fails before mitmproxy starts
    eng = MitmEngine(cfg, ShadowPolicy(), _Store(), NoOverlay())
    with pytest.raises(FileNotFoundError):
        asyncio.run(eng.run())
    assert closed == [True]
