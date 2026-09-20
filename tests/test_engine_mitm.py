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

from irimi import ca, paths, pipeline, servicemap
from irimi import policy as policy_module
from irimi.engine import EngineConfig, EngineStartError
from irimi.engine.mitm import IrimiAddon, MitmEngine
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


def _serve(ssl_context=None):
    """An _Upstream server on a free loopback port, plain or TLS. Caller shuts it down."""
    srv = HTTPServer(("127.0.0.1", 0), _Upstream)
    if ssl_context is not None:
        srv.socket = ssl_context.wrap_socket(srv.socket, server_side=True)
    # A short poll interval keeps shutdown() from blocking for the default 0.5 s.
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    return srv


@pytest.fixture
def upstream():
    srv = _serve()
    yield srv.server_address[1]
    srv.shutdown()


def _config(tmp_path, monkeypatch, port=0, reverse_hosts=frozenset(), maps=None):
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
        maps=maps if maps is not None else servicemap.MapIndex(),
    )


# The upstream in these tests lives on 127.0.0.1, so a map claiming that host makes its two routes
# classified ones. Hosts carry no port, so the upstream's random port does not matter.
DEMO_MAP = """
version: 1
service: demo
hosts:
  - 127.0.0.1
routes:
  - match:
      method: GET
      path: /hello
    operation: things.list
    kind: read
    human: list things
  - match:
      method: POST
      path: /things
    operation: things.create
    kind: write
    human: create a thing
"""


def _maps(tmp_path, monkeypatch, doc=DEMO_MAP):
    """A MapIndex holding `doc` alone. $IRIMI_HOME is pointed somewhere empty first: the loader
    merges `$IRIMI_HOME/maps.yaml` when it exists, and a developer may have a real one."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "maps-home"))
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    (maps_dir / "demo.yaml").write_text(doc)
    return servicemap.load(cwd=tmp_path, maps_dir=maps_dir)


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
        async def trust() -> None:  # on the engine's loop, like every other options change
            eng._master.options.update(ssl_verify_upstream_trusted_ca=str(trust_upstream_ca))

        asyncio.run_coroutine_threadsafe(trust(), loop).result(timeout=15)

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
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(leaf_pem)
    return _serve(ctx)


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
    assert ex.request.port == upstream  # a loopback upstream on another port is not the door
    assert ex.response.status == 200


def test_post_is_faked_l0(engine, upstream):
    eng, seen = engine
    status, data = _via_proxy(
        eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b'{"a":1}'
    )
    assert status == 200
    body = json.loads(data)
    assert body["a"] == 1  # the L0 echo reflects the request's own fields
    assert sorted(body) == ["a", "created"]  # unmapped: nothing minted
    ex = seen[0]
    assert ex.answered_by == "fake-L0"
    assert ex.kind == "unknown"
    assert "unclassified" in ex.flags
    assert ex.validation == "unvalidated"


def test_mapped_write_is_named_and_faked(tmp_path, monkeypatch, upstream):
    """The maps have to reach the classifier through EngineConfig, or every mapped route would be
    classified by its verb alone."""
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch))
    eng, seen, stop = _start(cfg)
    try:
        status, data = _via_proxy(
            eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b'{"a":1}'
        )
    finally:
        stop()
    assert status == 200
    assert json.loads(data)["a"] == 1  # the upstream's do_POST would have been a 500
    ex = seen[0]
    assert (ex.service, ex.operation, ex.kind) == ("demo", "things.create", "write")
    assert ex.answered_by == "fake-L0"
    assert ex.flags == ("fidelity:L0",)


def test_mapped_read_is_named_and_forwarded(tmp_path, monkeypatch, upstream):
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch))
    eng, seen, stop = _start(cfg)
    try:
        status, data = _via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    finally:
        stop()
    assert (status, data) == (200, b"hello from upstream")
    ex = seen[0]
    assert (ex.service, ex.operation, ex.kind) == ("demo", "things.list", "read")
    assert ex.answered_by == "live"
    assert ex.flags == ()


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
    assert json.loads(data)["not"] == "gzip"  # reflected from the body we could not decode
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


def test_reverse_door_forwards_to_tls_upstream(tmp_path, monkeypatch):
    cfg = _config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    srv = _serve_tls(_leaf_cert_for_loopback(cfg.ca, tmp_path / "leaf.pem"))
    up = srv.server_address[1]
    eng, seen, stop = _start(cfg, trust_upstream_ca=cfg.ca.cert)
    try:
        status, data = _reverse(eng.listen_port(), "GET", f"/127.0.0.1:{up}/hello?x=1")
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
    assert json.loads(data)["a"] == 1
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
        status, _ = _reverse(eng.listen_port(), "GET", "/127.0.0.1:1/x", host_name="localhost")
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


@pytest.mark.parametrize("host_name", ["0.0.0.0", "127.1", "[::ffff:127.0.0.1]"])
def test_self_addressed_alias_is_reverse_door_not_a_loop(tmp_path, monkeypatch, host_name):
    # Any spelling of "this listener" must take the door; forwarding it would make the proxy
    # connect to itself and re-receive the same request until the ports run out.
    cfg = _config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    eng, seen, stop = _start(cfg)
    try:
        status, _ = _reverse(eng.listen_port(), "GET", "/127.0.0.1:1/x", host_name=host_name)
    finally:
        stop()
    assert status == 502
    assert len(seen) == 1
    assert seen[0].door == "reverse"
    assert seen[0].request.port == 1


def test_reverse_door_request_without_host_header_gets_one(tmp_path, monkeypatch):
    # An HTTP/1.0 absolute-form request carries no Host header; the upstream still needs one.
    cfg = _config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    eng, seen, stop = _start(cfg)
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


def test_faked_write_reflects_a_form_body_and_is_flagged(engine, upstream):
    """The L0 echo reaches the client through the proxy, form-encoded as stripe-python posts."""
    eng, seen = engine
    status, data = _via_proxy(
        eng.listen_port(),
        "POST",
        f"http://127.0.0.1:{upstream}/things",
        body=b"amount=4900&charge=ch_test",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    body = json.loads(data)
    assert body["amount"] == 4900
    assert body["charge"] == "ch_test"
    assert isinstance(body["created"], int)
    ex = seen[0]
    assert ex.answered_by == "fake-L0"
    assert "fidelity:L0" in ex.flags


def test_live_read_carries_no_fidelity_flag(engine, upstream):
    eng, seen = engine
    _via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    assert seen[0].answered_by == "live"
    assert seen[0].flags == ()


# ------------------------------------------- telemetry is never stored, and SSE streams (#8/#9)


class _RecordingStore:
    """A TraceStore that remembers what it was handed. NullStore cannot answer this question."""

    def __init__(self):
        self.recorded = []

    def record(self, exchange):
        self.recorded.append(exchange)

    def close(self):
        return None


def _addon(tmp_path, monkeypatch, store):
    """An IrimiAddon with no proxy under it. `_finish` is a plain method; driving a whole engine
    would only add a thread between the assertion and the thing asserted."""
    from irimi.engine.mitm import IrimiAddon

    seen = []
    addon = IrimiAddon(
        _config(tmp_path, monkeypatch),
        ShadowPolicy(),
        store,
        NoOverlay(),
        seen.append,
        lambda port, error: None,
    )
    return addon, seen


def _exchange(kind, answered_by="live"):
    from irimi.exchange import Exchange, Request

    request = Request(
        method="POST",
        scheme="https",
        host="o0.ingest.sentry.io",
        port=443,
        path="/api/7/envelope/",
        query="",
        headers=(),
        body=b"",
    )
    return Exchange(
        request=request,
        response=Response(status=200, headers=(), body=b""),
        service="sentry",
        operation="envelope.send",
        kind=kind,
        answered_by=answered_by,
        validation="unvalidated",
        run_id="t3st",
    )


def test_telemetry_is_reported_but_never_recorded(tmp_path, monkeypatch):
    """Forwarded in every mode, counted in its own bucket, and kept out of the trace store: a
    recording of the agent's own observability traffic would re-emit someone else's events."""
    store = _RecordingStore()
    addon, seen = _addon(tmp_path, monkeypatch, store)
    addon._finish(_exchange("telemetry"))
    assert store.recorded == []
    assert [ex.kind for ex in seen] == ["telemetry"]


@pytest.mark.parametrize("kind", ["read", "write", "llm", "unknown"])
def test_every_other_kind_is_still_recorded(tmp_path, monkeypatch, kind):
    store = _RecordingStore()
    addon, seen = _addon(tmp_path, monkeypatch, store)
    addon._finish(_exchange(kind))
    assert [ex.kind for ex in store.recorded] == [kind]
    assert [ex.kind for ex in seen] == [kind]


@pytest.mark.parametrize(
    ("content_type", "expected"),
    [
        ("text/event-stream", True),
        ("text/event-stream; charset=utf-8", True),
        ("Text/Event-Stream", True),
        ("application/json", False),
        ("", False),
    ],
)
def test_is_event_stream(content_type, expected):
    from irimi.engine.mitm import _is_event_stream

    assert _is_event_stream(content_type) is expected


STREAM_MAP = """
version: 1
service: llmhost
hosts:
  - 127.0.0.1
routes:
  - match:
      method: POST
      path: /v1/chat/completions
    operation: chat.completions.create
    kind: llm
    human: chat completion
"""

SSE_FIRST = b'data: {"delta": "one"}\n\n'
SSE_SECOND = b'data: {"delta": "two"}\n\n'

# Set by the test once it has the first chunk; the upstream holds the second one until then, so a
# buffered response cannot reach the client at all inside the client's socket timeout.
_STREAM_GATE = threading.Event()


class _StreamUpstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # chunked transfer encoding needs HTTP/1.1

    def do_POST(self):
        length = int(self.headers.get("content-length") or 0)
        if length:
            self.rfile.read(length)
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("transfer-encoding", "chunked")
        self.end_headers()
        self._chunk(SSE_FIRST)
        _STREAM_GATE.wait(timeout=20)
        self._chunk(SSE_SECOND)
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()
        # Close rather than keep the connection alive: mitmproxy pools its upstream connections,
        # and a single-threaded HTTPServer whose handler is still waiting for a second request on
        # that socket never returns from serve_forever, so shutdown() would block forever.
        self.close_connection = True

    def _chunk(self, payload: bytes) -> None:
        self.wfile.write(b"%x\r\n" % len(payload) + payload + b"\r\n")
        self.wfile.flush()

    def log_message(self, *args):
        pass


def test_a_server_sent_event_response_reaches_the_client_in_chunks(tmp_path, monkeypatch):
    """The whole point of the `responseheaders` hook: without `flow.response.stream = True`
    mitmproxy buffers the body, so the first chunk would not arrive until the upstream finished.

    The upstream holds the second chunk until this test has read the first, and the client's
    socket timeout is far shorter than the upstream's wait, so buffering fails the test rather
    than slowing it down.
    """
    _STREAM_GATE.clear()
    srv = HTTPServer(("127.0.0.1", 0), _StreamUpstream)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch, STREAM_MAP))
    eng, seen, stop = _start(cfg)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=5)
        url = f"http://127.0.0.1:{srv.server_address[1]}/v1/chat/completions"
        conn.request(
            "POST",
            url,
            body=b"{}",
            headers={
                "host": f"127.0.0.1:{srv.server_address[1]}",
                "content-type": "application/json",
            },
        )
        resp = conn.getresponse()
        assert resp.status == 200
        assert resp.getheader("content-type") == "text/event-stream"
        first = resp.read(len(SSE_FIRST))  # times out at 5 s if the proxy buffered the body
        assert first == SSE_FIRST
        _STREAM_GATE.set()
        assert resp.read() == SSE_SECOND
        conn.close()
    finally:
        _STREAM_GATE.set()
        stop()
        srv.shutdown()
    assert len(seen) == 1
    ex = seen[0]
    assert (ex.service, ex.operation, ex.kind) == ("llmhost", "chat.completions.create", "llm")
    assert ex.answered_by == "live"
    assert ex.flags == ()
    # A streamed body is never assembled, so the recorded exchange carries an empty one. That is
    # the trade for the agent seeing tokens as they arrive.
    assert ex.response.status == 200
    assert ex.response.body == b""


def test_a_json_response_is_still_buffered_and_recorded(tmp_path, monkeypatch, upstream):
    """The hook keys on the content type, so an ordinary live read is unaffected."""
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch))
    eng, seen, stop = _start(cfg)
    try:
        status, data = _via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    finally:
        stop()
    assert (status, data) == (200, b"hello from upstream")
    assert seen[0].response.body == b"hello from upstream"


# -------------------------------------------------------- answer targets through the engine (#16)


class _Target(BaseHTTPRequestHandler):
    """A developer's own stub: it records what it was asked and answers its own JSON."""

    seen: list = []

    def _handle(self):
        length = int(self.headers.get("content-length") or 0)
        _Target.seen.append((self.command, self.path, self.rfile.read(length), dict(self.headers)))
        body = json.dumps({"from": "target", "path": self.path}).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = _handle

    def log_message(self, *args):
        pass


def _host_header(headers: dict) -> str:
    """The Host header as sent, whatever case the client spelled it in."""
    return next(v for k, v in headers.items() if k.lower() == "host")


@pytest.fixture
def target():
    _Target.seen = []
    srv = HTTPServer(("127.0.0.1", 0), _Target)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    yield srv.server_address[1]
    srv.shutdown()


def _targeted(tmp_path, monkeypatch, targets=(), target_reads=()):
    """A MapIndex over DEMO_MAP with `--target` style flags already applied."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "maps-home"))
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    (maps_dir / "demo.yaml").write_text(DEMO_MAP)
    return servicemap.load(
        cwd=tmp_path, maps_dir=maps_dir, targets=targets, target_reads=target_reads
    )


def test_a_targeted_write_is_answered_by_the_target_not_by_the_fake(tmp_path, monkeypatch, target):
    """The headline of #16: the write lands on an address the developer controls, that stub's
    body is what the agent parses, and the fake never runs."""
    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = _via_proxy(
            eng.listen_port(), "POST", "http://127.0.0.1/things", body=b'{"a": 1}'
        )
    finally:
        stop()
    assert status == 200
    assert json.loads(data) == {"from": "target", "path": "/w"}
    # Method, path and body pass through unchanged. Sliced to three elements rather than
    # comparing the headers against themselves, and built as a list comprehension so an empty
    # `seen` - the regression this test exists to catch - fails readably instead of IndexError.
    assert [row[:3] for row in _Target.seen] == [("POST", "/w", b'{"a": 1}')]
    (ex,) = seen
    assert ex.answered_by == "delegated"
    assert ex.target == f"http://127.0.0.1:{target}/w"
    assert ex.kind == "write"


def test_a_delegated_exchange_records_what_the_agent_asked_for(tmp_path, monkeypatch, target):
    """The Exchange keeps the agent's own request; where the answer came from is `target`. The
    summary has to be able to say `create a thing -> 127.0.0.1:NNNN/w`, which needs both."""
    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    (ex,) = seen
    assert (ex.request.host, ex.request.path) == ("127.0.0.1", "/things")
    assert ex.operation == "things.create"
    assert ex.target.endswith("/w")


def test_the_authorization_header_is_stripped_before_it_reaches_a_target(
    tmp_path, monkeypatch, target
):
    """A local stub does not need the real key, and forwarding it makes the target an
    exfiltration path for a credential the agent never meant it to have (design §7)."""
    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        _via_proxy(
            eng.listen_port(),
            "POST",
            "http://127.0.0.1/things",
            body=b"{}",
            extra_headers={"authorization": "Bearer sk_live_SECRET"},
        )
    finally:
        stop()
    headers = {k.lower(): v for k, v in _Target.seen[0][3].items()}
    assert "authorization" not in headers
    assert headers["content-type"] == "application/json"  # everything else passes through


def test_forward_auth_keeps_the_authorization_header(tmp_path, monkeypatch, target):
    """The route opting in: a sandbox tenant or an internal simulator does need the key."""
    from dataclasses import replace as _replace

    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    sm = maps.services[0]
    routes = tuple(
        _replace(r, forward_auth=True) if r.target != servicemap.SELF_TARGET else r
        for r in sm.routes
    )
    maps = servicemap.MapIndex((_replace(sm, routes=routes),))
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        _via_proxy(
            eng.listen_port(),
            "POST",
            "http://127.0.0.1/things",
            body=b"{}",
            extra_headers={"authorization": "Bearer sk_live_SECRET"},
        )
    finally:
        stop()
    headers = {k.lower(): v for k, v in _Target.seen[0][3].items()}
    assert headers["authorization"] == "Bearer sk_live_SECRET"


def test_target_reads_sends_a_read_to_the_target_instead_of_the_real_service(
    tmp_path, monkeypatch, target, upstream
):
    """A delegated service: its target, not production, is the world the agent sees. The read
    must not reach the upstream, which would answer `hello from upstream`."""
    maps = _targeted(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "", f"http://127.0.0.1:{target}")],
        target_reads=["127.0.0.1"],
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = _via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    finally:
        stop()
    assert status == 200
    assert json.loads(data)["from"] == "target"
    assert _Target.seen[0][1] == "/hello"  # a bare origin keeps the original path
    (ex,) = seen
    assert (ex.answered_by, ex.kind) == ("delegated", "read")


def test_a_service_target_also_covers_a_route_its_map_does_not_list(tmp_path, monkeypatch, target):
    """Answering the unlisted routes with our own fake would give the agent a world that is half
    the stub's and half ours, which is what `service_map` on the Classification is for."""
    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "", f"http://127.0.0.1:{target}")]
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = _via_proxy(
            eng.listen_port(), "POST", "http://127.0.0.1/unlisted", body=b"{}"
        )
    finally:
        stop()
    assert status == 200 and json.loads(data)["from"] == "target"
    (ex,) = seen
    assert (ex.answered_by, ex.kind) == ("delegated", "unknown")
    assert _Target.seen[0][1] == "/unlisted"


def test_an_unreachable_target_is_a_502_flagged_target_failed(tmp_path, monkeypatch):
    """Never a silent fall back to the local fake: that would hide a broken setup and look
    exactly like a working shadow run (design D20)."""
    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    dead = closed.getsockname()[1]
    closed.close()
    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{dead}/w")]
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, _ = _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    assert status == 502
    (ex,) = seen
    assert ex.answered_by == "delegated"
    assert "target-failed" in ex.flags
    assert ex.response is None


def test_a_target_naming_our_own_listener_is_refused_with_a_json_502(tmp_path, monkeypatch):
    """Forwarding to ourselves is the self-connection loop of #4 wearing a different hat.

    Driven through the addon rather than a live engine on purpose: the check compares the target
    against the port the listener actually bound, and arranging for a real engine to bind the one
    port a pre-built map already names is a race against TIME_WAIT, not a test.
    """
    from mitmproxy.test import tflow, tutils

    from irimi.engine.mitm import META_KEY, IrimiAddon

    listen_port = 4000
    maps = _targeted(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{listen_port}/w")],
    )
    seen = []
    addon = IrimiAddon(
        _config(tmp_path, monkeypatch, maps=maps),
        ShadowPolicy(),
        NullStore(),
        NoOverlay(),
        seen.append,
        lambda port, error: None,
    )
    flow = tflow.tflow(req=tutils.treq(method=b"POST", host="127.0.0.1", port=80, path=b"/things"))
    flow.client_conn.sockname = ("127.0.0.1", listen_port)
    addon.request(flow)

    assert flow.response.status_code == 502
    assert json.loads(flow.response.content)["error"]["type"] == "irimi_target_failed"
    assert "own listener" in json.loads(flow.response.content)["error"]["message"]
    # The flow was not pointed anywhere: refusing must not leave it aimed at the real service.
    assert (flow.request.host, flow.request.path) == ("127.0.0.1", "/things")
    pending = flow.metadata[META_KEY]
    assert pending.answered_by == "delegated"
    assert "target-failed" in pending.flags


def test_a_locally_answered_flow_that_errors_keeps_its_own_flags(tmp_path, monkeypatch):
    """#29 item 5: the `error` hook used to assert `upstream-error` on every flow it saw. For a
    write irimi answered itself that is untrue twice over - nothing was ever sent upstream, and
    the fidelity flag the answer really carried was dropped. Driven through the addon because
    the case is a client disappearing between two hooks, which a live engine cannot stage."""
    from mitmproxy.test import tflow, tutils

    from irimi.engine.mitm import IrimiAddon

    maps = _targeted(tmp_path, monkeypatch)
    seen = []
    addon = IrimiAddon(
        _config(tmp_path, monkeypatch, maps=maps),
        ShadowPolicy(),
        NullStore(),
        NoOverlay(),
        seen.append,
        lambda port, error: None,
    )
    flow = tflow.tflow(req=tutils.treq(method=b"POST", host="127.0.0.1", port=80, path=b"/things"))
    flow.client_conn.sockname = ("127.0.0.1", 4000)
    addon.request(flow)
    addon.error(flow)

    (ex,) = seen
    assert ex.answered_by == "fake-L0"
    assert "fidelity:L0" in ex.flags
    assert "upstream-error" not in ex.flags


def test_an_untargeted_write_is_still_faked(tmp_path, monkeypatch, target):
    """A route target must not leak onto the rest of the service."""
    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = _via_proxy(
            eng.listen_port(), "POST", "http://127.0.0.1/elsewhere", body=b"{}"
        )
    finally:
        stop()
    assert status == 200
    assert json.loads(data).keys() == {"created"}  # the L0 echo, not the target
    assert _Target.seen == []
    (ex,) = seen
    assert ex.answered_by == "fake-L0" and ex.target == ""


def test_a_raise_in_the_decision_answers_locally_instead_of_forwarding(
    tmp_path, monkeypatch, upstream
):
    """The never-raise rule, as a whole rather than per call site (COMPAT 4.10, #16 review D-3).

    A hook that raises makes mitmproxy forward the flow untouched, so an exception anywhere in
    the classify/answer decision sends the agent's write to the real service - and `_Pending` is
    never set, so `response()` returns early and no Exchange is recorded either. The write is
    performed for real and is invisible in the trace.
    """
    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", "http://127.0.0.1:9/w")]
    )

    def boom(*args, **kwargs):
        raise RuntimeError("the decision exploded")

    monkeypatch.setattr(policy_module, "target_url", boom)
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = _via_proxy(
            eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}"
        )
    finally:
        stop()
    # The upstream's do_POST answers 500 and exists to prove it was never reached.
    assert status == 502, "the write reached the real upstream"
    assert json.loads(data)["error"]["type"] == "irimi_decision_failed"
    assert len(seen) == 1, "the exchange must still be recorded"
    assert seen[0].flags == (pipeline.UNCLASSIFIED_FLAG, pipeline.DECISION_FAILED_FLAG)
    assert (seen[0].kind, seen[0].answered_by) == ("unknown", "fake-L0")


def test_a_delegated_read_is_not_a_write(tmp_path, monkeypatch, target):
    """`answered_by != "live"` was an exhaustive spelling of "is a write" only while every read
    was live. `target_reads` makes a delegated read the first non-live read, and the write log is
    what the overlay replays onto later reads (#20, #28)."""
    maps = _targeted(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "", f"http://127.0.0.1:{target}")],
        target_reads=["127.0.0.1"],
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        addon = next(a for a in eng._master.addons.chain if isinstance(a, IrimiAddon))
        status, _ = _via_proxy(eng.listen_port(), "GET", "http://127.0.0.1:1/hello")
        assert status == 200
        assert [ex.kind for ex in addon.write_log] == [], "a delegated read is in the write log"
        # The delegated *write* on the same service still is one.
        _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1:1/things", body=b"{}")
        assert [(ex.kind, ex.answered_by) for ex in addon.write_log] == [("write", "delegated")]
    finally:
        stop()


def test_a_refused_target_is_not_a_write_either(tmp_path, monkeypatch, target):
    """A target irimi refuses is answered `502` in `request()`, which DOES set `_Pending`, so
    unlike a target that could not be dialled it reaches `response()` and was being appended to
    the write log - the log the Phase 2 overlay replays onto live reads.

    Nothing was performed: not at the target, not at the real service, not by the fake. The
    overlay would have handed the agent back a write irimi refused to send anywhere.
    """

    def refuse(url, port):
        raise pipeline.TargetRefused("refusing this target")

    monkeypatch.setattr(pipeline, "refuse_self_target", refuse)
    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        addon = next(a for a in eng._master.addons.chain if isinstance(a, IrimiAddon))
        status, data = _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    assert status == 502 and json.loads(data)["error"]["type"] == "irimi_target_failed"
    # Still recorded - a write irimi could not answer is exactly what the trace has to show.
    assert [ex.flags for ex in seen] == [("fidelity:delegated", "target-failed")]
    assert addon.write_log == [], "a write performed nowhere is in the overlay's write log"


def test_an_ipv6_target_gets_a_bracketed_host_header(tmp_path, monkeypatch):
    """`parts.hostname` strips an IPv6 literal's brackets, so the authority has to put them back:
    RFC 3986 spells it `[::1]:3000`, and `::1:3000` is a different and unparseable thing. The
    README lists `::1` as a supported target and the loader accepts it."""

    class _V6Server(HTTPServer):
        address_family = socket.AF_INET6

    srv = _V6Server(("::1", 0), _Target)
    port = srv.server_address[1]
    _Target.seen = []
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    try:
        maps = _targeted(
            tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://[::1]:{port}/w")]
        )
        eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
        try:
            status, _ = _via_proxy(
                eng.listen_port(), "POST", "http://127.0.0.1:1/things", body=b"{}"
            )
        finally:
            stop()
        assert status == 200
        assert _Target.seen, "the target was never reached"
        host_header = _host_header(_Target.seen[0][3])
        assert host_header == f"[::1]:{port}", host_header
    finally:
        srv.shutdown()


def test_the_host_header_is_set_to_the_target_not_left_as_the_service(
    tmp_path, monkeypatch, target
):
    """Deleting `flow.request.host_header = authority` passed the whole suite. A stub that routes
    on Host - any vhost, any framework's host check - would see the real service's name."""
    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1:1/things", body=b"{}")
    finally:
        stop()
    assert _host_header(_Target.seen[0][3]) == f"127.0.0.1:{target}"


def test_a_streamed_target_response_is_not_buffered(tmp_path, monkeypatch):
    """The streaming guard names `delegated` as well as `live`; reverting it to `("live",)` passed
    the whole suite. A delegated SSE route is the case #28 is about."""
    _STREAM_GATE.clear()
    srv = HTTPServer(("127.0.0.1", 0), _StreamUpstream)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    stream_port = srv.server_address[1]
    try:
        maps = _targeted(
            tmp_path,
            monkeypatch,
            targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{stream_port}/v1/stream")],
        )
        eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
        try:
            conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=5)
            conn.request(
                "POST",
                "http://127.0.0.1:1/things",
                body=b"{}",
                headers={"host": "127.0.0.1:1", "content-type": "application/json"},
            )
            resp = conn.getresponse()
            assert resp.status == 200
            first = resp.read(len(SSE_FIRST))  # times out at 5 s if the proxy buffered the body
            assert first == SSE_FIRST
            _STREAM_GATE.set()
            assert resp.read() == SSE_SECOND
            conn.close()
        finally:
            _STREAM_GATE.set()
            stop()
    finally:
        srv.shutdown()
