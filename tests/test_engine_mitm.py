import asyncio
import http.client
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from irimi import ca, paths
from irimi.engine import EngineConfig
from irimi.engine.mitm import MitmEngine
from irimi.overlay import NoOverlay
from irimi.policy import ShadowPolicy
from irimi.store import NullStore


class _Upstream(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"hello from upstream"
        self.send_response(200)
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


@pytest.fixture
def engine(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    p = ca.ca_paths()
    ca.generate_ca(p)
    seen = []
    eng = MitmEngine(
        EngineConfig(
            run_id="t3st",
            ca=p,
            confdir=paths.mitm_dir(),
            listen_host="127.0.0.1",
            listen_port=0,
        ),
        policy=ShadowPolicy(),
        store=NullStore(),
        overlay=NoOverlay(),
        on_exchange=seen.append,
    )
    loop = asyncio.new_event_loop()

    async def _serve():
        await eng.run()

    t = threading.Thread(target=lambda: loop.run_until_complete(_serve()), daemon=True)
    t.start()
    fut = asyncio.run_coroutine_threadsafe(eng.wait_ready(), loop)
    fut.result(timeout=15)
    yield eng, seen
    eng.shutdown()
    t.join(timeout=15)
    loop.close()


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
