import asyncio
import datetime as dt
import http.client
import ipaddress
import json
import socket
import ssl
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

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


def _config(tmp_path, monkeypatch, port=0, reverse_hosts=frozenset()):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    p = ca.ca_paths()
    ca.generate_ca(p)
    return EngineConfig(
        run_id="t3st",
        ca=p,
        confdir=paths.mitm_dir(),
        listen_host="127.0.0.1",
        listen_port=port,
        reverse_hosts=reverse_hosts,
    )


def _start(cfg, overlay=None, trust_upstream_ca=None):
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
    if trust_upstream_ca is not None:
        # Test-only. mitmproxy verifies upstream TLS against certifi's bundle; the TLS upstream in
        # these tests presents a leaf signed by the irimi CA, so trust that instead. This reaches
        # into the engine on purpose; there is no product option for it.
        eng._master.options.update(ssl_verify_upstream_trusted_ca=str(trust_upstream_ca))

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


def _reverse(proxy_port, method, path, body=None, host_name="127.0.0.1"):
    """Talk to the reverse door: an origin-form request addressed to the listener itself."""
    conn = http.client.HTTPConnection("127.0.0.1", proxy_port, timeout=10)
    headers = {"host": f"{host_name}:{proxy_port}"}
    if body is not None:
        headers["content-type"] = "application/json"
    conn.request(method, path, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def _leaf_cert_for_loopback(p: ca.CAPaths, out: Path) -> Path:
    """Mint a leaf for 127.0.0.1 signed by the irimi CA; writes cert+key PEM to `out`."""
    ca_key = serialization.load_pem_private_key(p.key.read_bytes(), None)
    ca_cert = x509.load_pem_x509_certificate(p.cert.read_bytes())
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = dt.datetime.now(dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")]))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    out.write_bytes(
        cert.public_bytes(serialization.Encoding.PEM)
        + key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return out


def _serve_tls(leaf_pem: Path) -> HTTPServer:
    """The same _Upstream handler, behind TLS. Caller shuts it down."""
    srv = HTTPServer(("127.0.0.1", 0), _Upstream)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(leaf_pem)
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


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
    assert ex.door == "forward"
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


@pytest.mark.parametrize("host_name", ["127.0.0.1", "localhost"])
def test_reverse_door_forwards_to_tls_upstream(tmp_path, monkeypatch, host_name):
    cfg = _config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    srv = _serve_tls(_leaf_cert_for_loopback(cfg.ca, tmp_path / "leaf.pem"))
    up = srv.server_address[1]
    eng, seen, stop = _start(cfg, trust_upstream_ca=cfg.ca.cert)
    try:
        status, data = _reverse(
            eng.listen_port(), "GET", f"/127.0.0.1:{up}/hello?x=1", host_name=host_name
        )
    finally:
        stop()
        srv.shutdown()
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


def test_reverse_door_refuses_unlisted_host(engine):
    eng, seen = engine
    status, data = _reverse(eng.listen_port(), "GET", "/api.stripe.com/v1")
    assert status == 403
    assert b"not in a loaded map or --allow-host" in data
    assert b"api.stripe.com" in data
    assert seen == []


def test_reverse_door_refuses_missing_host(engine):
    eng, seen = engine
    status, data = _reverse(eng.listen_port(), "GET", "/")
    assert status == 403
    assert b"/<upstream-host>/<path>" in data
    assert seen == []


def test_reverse_door_write_is_faked(tmp_path, monkeypatch):
    cfg = _config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    eng, seen, stop = _start(cfg)
    try:
        status, data = _reverse(eng.listen_port(), "POST", "/127.0.0.1:1/things", body=b'{"a":1}')
    finally:
        stop()
    assert status == 200
    assert json.loads(data) == {}
    ex = seen[0]
    assert ex.door == "reverse"
    assert ex.answered_by == "fake-L0"
    assert ex.kind == "unknown"
    assert ex.request.host == "127.0.0.1"
    assert ex.request.port == 1
    assert ex.request.path == "/things"
    assert ex.request.body == b'{"a":1}'


def test_reverse_door_upstream_error_is_flagged(tmp_path, monkeypatch):
    cfg = _config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    eng, seen, stop = _start(cfg)
    try:
        status, _ = _reverse(eng.listen_port(), "GET", "/127.0.0.1:1/x")
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


def test_absolute_url_to_listener_is_reverse_door(tmp_path, monkeypatch):
    cfg = _config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    eng, seen, stop = _start(cfg)
    port = eng.listen_port()
    try:
        status, _ = _via_proxy(port, "GET", f"http://127.0.0.1:{port}/127.0.0.1:1/x")
    finally:
        stop()
    assert status == 502
    assert len(seen) == 1
    assert seen[0].door == "reverse"
    assert seen[0].request.port == 1
    assert "upstream-error" in seen[0].flags


def test_forward_door_to_loopback_upstream_stays_forward(engine, upstream):
    eng, seen = engine
    status, _ = _via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    assert status == 200
    assert seen[0].door == "forward"
    assert seen[0].request.port == upstream
