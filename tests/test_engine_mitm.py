import asyncio
import datetime as dt
import gzip
import http.client
import ipaddress
import json
import socket
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from irimi import ca, delegation, idempotency, paths, pipeline, report, servicemap
from irimi.engine import EngineConfig, EngineStartError
from irimi.engine.mitm import IrimiAddon, MitmEngine
from irimi.exchange import (
    DECISION_FAILED_FLAG,
    IDEMPOTENCY_CONFLICT_FLAG,
    IDEMPOTENT_REPLAY_FLAG,
    UNCLASSIFIED_FLAG,
    Request,
    Response,
)
from irimi.overlay import NoOverlay, Overlaid
from irimi.policy import Answer, ShadowPolicy
from irimi.store import NullStore


class _Upstream(BaseHTTPRequestHandler):
    # (path, header pairs) of every GET, so a test can see what was forwarded. Pairs, not a dict:
    # a dict keeps one of a repeated name, and "exactly once on the wire" is a claim (#53).
    seen: list = []

    def do_GET(self):
        _Upstream.seen.append((self.path, self.headers.items()))
        body = b"hello from upstream"
        self.send_response(200)
        if self.path == "/badgzip":  # claims gzip, is not: an undecodable body
            body = b"not-gzip"
            self.send_header("content-encoding", "gzip")
        if self.path == "/slack-history":  # a Slack read whose real `ts` the run has to see
            body = b'{"ok": true, "messages": [{"ts": "1999999999.000500"}]}'
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
    _Upstream.seen = []
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
    fires:
      - thing.created
"""

# A map claiming the loopback upstream as `slack`, so a read through it is a Slack read. The
# service name is what the `ts` watermark keys on, not the host or the port (#42).
SLACK_MAP = """
version: 1
service: slack
hosts:
  - 127.0.0.1
routes:
  - match:
      method: GET
      path: /slack-history
    operation: conversations.history
    kind: read
    human: read the history
"""


def _maps(tmp_path, monkeypatch, doc=DEMO_MAP):
    """A MapIndex holding `doc` alone. $IRIMI_HOME is pointed somewhere empty first: the loader
    merges `$IRIMI_HOME/maps.yaml` when it exists, and a developer may have a real one."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "maps-home"))
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    (maps_dir / "demo.yaml").write_text(doc)
    return servicemap.load(cwd=tmp_path, maps_dir=maps_dir)


def _start(cfg, overlay=None, trust_upstream_ca=None, policy=None):
    """Serve `cfg` on a background loop until the returned stop() is called."""
    seen = []
    eng = MitmEngine(
        cfg,
        policy=policy or ShadowPolicy(),
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


def test_a_slack_read_raises_the_ts_watermark(tmp_path, monkeypatch, upstream):
    """#42: a minted `ts` has to sort after the real messages the run already read, and this hook
    is the only place those values go past - the write log holds writes, not reads."""
    from irimi import echo

    monkeypatch.setattr(echo, "_last_slack_ts", (0, 0))
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch, doc=SLACK_MAP))
    eng, seen, stop = _start(cfg)
    try:
        status, _ = _via_proxy(
            eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/slack-history"
        )
    finally:
        stop()
    assert status == 200
    assert (seen[0].service, seen[0].kind) == ("slack", "read")
    whole, _, fraction = echo.slack_ts().partition(".")
    assert (int(whole), int(fraction)) > (1_999_999_999, 500)


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
    class _Overlay:
        def __call__(self, write_log, read_request, upstream_response):
            return Overlaid(Response(200, (("content-type", "text/plain"),), b"OVERLAID"))

        def rewrite(self, write_log, read_request):
            return read_request

    eng, seen, stop = _start(_config(tmp_path, monkeypatch), overlay=_Overlay())
    try:
        # The overlay is asked only once there is a write to show.
        _via_proxy(eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")
        status, data = _via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    finally:
        stop()
    assert (status, data) == (200, b"OVERLAID")
    assert seen[-1].response.body == b"OVERLAID"


def test_repeated_headers_survive_a_local_answer(tmp_path, monkeypatch, upstream):
    class _Overlay:
        def __call__(self, write_log, read_request, upstream_response):
            return Overlaid(Response(200, (("set-cookie", "a=1"), ("set-cookie", "b=2")), b""))

        def rewrite(self, write_log, read_request):
            return read_request

    eng, seen, stop = _start(_config(tmp_path, monkeypatch), overlay=_Overlay())
    try:
        # The overlay is asked only once there is a write to show.
        _via_proxy(eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")
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

    do_GET = do_POST  # the same stream for a `kind: read` route (#28)

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


READ_STREAM_MAP = STREAM_MAP.replace("kind: llm", "kind: read").replace(
    "method: POST", "method: GET"
)


class _RewritingOverlay:
    """The first real overlay's shape: it returns a NEW Response rather than the one it was
    handed. `NoOverlay` returns the same object, which is what hid #28 for a whole phase."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, write_log, read_request, upstream_response):
        self.calls.append(read_request.path)
        return Overlaid(
            Response(
                status=200,
                headers=(("content-type", "application/json"),),
                body=b'{"overlaid": true}',
            )
        )

    def rewrite(self, write_log, read_request):
        return read_request


def test_an_overlay_is_not_handed_a_streamed_read_and_cannot_rewrite_one(tmp_path, monkeypatch):
    """#28: `responseheaders` streams any live `text/event-stream` response, `kind: read`
    included, and mitmproxy never assembles a streamed body. An overlay called there would see
    `body == b""` and return a new Response, which the addon would then assign to the flow -
    turning a streamed read into a buffered, empty-bodied one. It presents as "reads through the
    proxy mysteriously return nothing", and only for streaming endpoints.

    Both halves are asserted, because either one alone would let the bug back in: the overlay is
    never called, and the chunks reach the client unchanged.
    """
    _STREAM_GATE.clear()
    srv = HTTPServer(("127.0.0.1", 0), _StreamUpstream)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch, READ_STREAM_MAP))
    overlay = _RewritingOverlay()
    eng, seen, stop = _start(cfg, overlay=overlay)
    try:
        # A faked write first, so the overlay WOULD be asked about this read if it were not
        # streamed: with an empty write log it is never asked at all, and this test would pass
        # without the streaming guard.
        _via_proxy(
            eng.listen_port(), "POST", f"http://127.0.0.1:{srv.server_address[1]}/w", body=b"{}"
        )
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=5)
        url = f"http://127.0.0.1:{srv.server_address[1]}/v1/chat/completions"
        conn.request("GET", url, headers={"host": f"127.0.0.1:{srv.server_address[1]}"})
        resp = conn.getresponse()
        assert resp.status == 200
        assert resp.getheader("content-type") == "text/event-stream"
        assert resp.read(len(SSE_FIRST)) == SSE_FIRST
        _STREAM_GATE.set()
        assert resp.read() == SSE_SECOND
        conn.close()
    finally:
        _STREAM_GATE.set()
        stop()
        srv.shutdown()
    assert overlay.calls == [], "the overlay was handed a body mitmproxy never assembled"
    _, ex = seen
    assert (ex.kind, ex.answered_by) == ("read", "live")


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


NOT_GZIP = b"not-actually-gzip"
REAL_GZIP = gzip.compress(b'{"from": "target"}')


class _Target(BaseHTTPRequestHandler):
    """A developer's own stub: it records what it was asked and answers its own JSON."""

    seen: list = []

    def _handle(self):
        length = int(self.headers.get("content-length") or 0)
        _Target.seen.append((self.command, self.path, self.rfile.read(length), dict(self.headers)))
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

    def log_message(self, *args):
        pass


def _host_header(headers: dict) -> str:
    """The Host header as sent, whatever case the client spelled it in."""
    return next(v for k, v in headers.items() if k.lower() == "host")


_RUNNING_TARGETS: list[HTTPServer] = []


@pytest.fixture
def target():
    _Target.seen = []
    srv = HTTPServer(("127.0.0.1", 0), _Target)
    _RUNNING_TARGETS.append(srv)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    yield srv.server_address[1]
    _kill_target()


def _kill_target() -> None:
    """Stop the `target` fixture's stub and free its port, so the next connect is refused."""
    while _RUNNING_TARGETS:
        srv = _RUNNING_TARGETS.pop()
        srv.shutdown()
        srv.server_close()


def _targeted(tmp_path, monkeypatch, targets=(), target_reads=()):
    """A MapIndex over DEMO_MAP with `--target` style flags already applied."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "maps-home"))
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    (maps_dir / "demo.yaml").write_text(DEMO_MAP)
    return servicemap.load(
        cwd=tmp_path, maps_dir=maps_dir, targets=targets, target_reads=target_reads
    )


def _via_tls_proxy(cfg, proxy_port, host, port, method, path, body=None):
    """A CONNECT tunnel through the proxy, trusting the irimi CA, then one request inside it."""
    ctx = ssl.create_default_context(cafile=str(cfg.ca.cert))
    conn = http.client.HTTPSConnection("127.0.0.1", proxy_port, context=ctx, timeout=10)
    conn.set_tunnel(host, port)
    try:
        conn.request(method, path, body=body, headers={"content-type": "application/json"})
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def test_an_https_write_is_answered_when_the_real_host_is_not_listening(tmp_path, monkeypatch):
    """mitmproxy's default `connection_strategy` is `eager`: it dials the real host - sending a
    ClientHello carrying real SNI - before it has seen the request it would have answered. An
    HTTPS route could then not be answered at all when the real service was unreachable, which is
    exactly the case shadow mode and delegation are for: an offline, decommissioned or
    not-yet-built API (#32). `lazy` connects only when there is something to send.

    The dead port stands in for the unreachable service; nothing is ever listening on it, so a
    pass here means no connection to it was attempted.
    """
    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    dead = closed.getsockname()[1]
    closed.close()
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch))
    eng, seen, stop = _start(cfg)
    try:
        status, data = _via_tls_proxy(
            cfg, eng.listen_port(), "127.0.0.1", dead, "POST", "/things", body=b'{"a": 1}'
        )
    finally:
        stop()
    assert status == 200
    assert json.loads(data)["a"] == 1
    (ex,) = seen
    assert (ex.answered_by, ex.kind, ex.request.scheme) == ("fake-L0", "write", "https")


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


@pytest.mark.parametrize(
    ("path", "body"), [("/badgzip", NOT_GZIP), ("/gzip", REAL_GZIP)], ids=["undecodable", "gzip"]
)
def test_a_delegated_body_reaches_the_agent_as_the_target_sent_it(
    tmp_path, monkeypatch, target, path, body
):
    """#39: the answer is stamped with a header, and stamping it used to rebuild the response.
    `http.Response.make` assigns `.content`, which mitmproxy re-encodes per the surviving
    `content-encoding` - so an already-compressed body was compressed again, and a body that was
    never valid gzip came back as a valid gzip stream of itself. Either way the agent got
    different bytes than the target sent, in a tool whose thesis is that it sees what the service
    would have sent.

    Since #12 `respond` returns a new Response for every non-live answer, so this ran for every
    delegated exchange; the live path stayed safe only because `respond` hands back the object it
    was given there.
    """
    maps = _targeted(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}{path}")],
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=10)
        conn.request("POST", "http://127.0.0.1/things", body=b"{}", headers={"host": "127.0.0.1"})
        resp = conn.getresponse()
        received, encoding, stamp = (
            resp.read(),
            resp.getheader("content-encoding"),
            resp.getheader("irimi-answered-by"),
        )
        conn.close()
    finally:
        stop()
    assert received == body  # byte for byte, not merely "decodes to the same thing"
    assert encoding == "gzip"
    assert stamp == "delegated"  # the header the rebuild existed to add is still there
    (ex,) = seen
    assert ex.answered_by == "delegated"


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


CREDENTIAL_HEADERS_SENT = {
    "authorization": "Bearer sk_live_SECRET",
    "cookie": "session=SECRET",
    "x-api-key": "SECRET",
    "dd-api-key": "SECRET",
    "x-honeycomb-team": "SECRET",
}


def test_every_credential_header_is_stripped_before_it_reaches_a_target(
    tmp_path, monkeypatch, target
):
    """A local stub does not need the real key, and forwarding it makes the target an
    exfiltration path for a credential the agent never meant it to have (design §7).

    Every credential header, not only `Authorization`: the rule is about keys, and `Cookie`,
    `x-api-key` and `DD-API-KEY` are keys (#32).
    """
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
            extra_headers=dict(CREDENTIAL_HEADERS_SENT),
        )
    finally:
        stop()
    headers = {k.lower(): v for k, v in _Target.seen[0][3].items()}
    assert [name for name in CREDENTIAL_HEADERS_SENT if name in headers] == []
    assert "SECRET" not in "".join(headers.values())
    assert headers["content-type"] == "application/json"  # everything else passes through


def test_forward_auth_keeps_the_credential_headers(tmp_path, monkeypatch, target):
    """The route opting in: a sandbox tenant or an internal simulator does need the key. It is
    one switch for all of them - a route that wants its `Authorization` forwarded is a route
    whose target is trusted with credentials."""
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
            extra_headers=dict(CREDENTIAL_HEADERS_SENT),
        )
    finally:
        stop()
    headers = {k.lower(): v for k, v in _Target.seen[0][3].items()}
    assert {name: headers.get(name) for name in CREDENTIAL_HEADERS_SENT} == CREDENTIAL_HEADERS_SENT


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


def test_an_unreachable_target_is_a_502_with_the_json_body_the_issue_specifies(
    tmp_path, monkeypatch
):
    """Never a silent fall back to the local fake: that would hide a broken setup and look
    exactly like a working shadow run (design D20).

    And the body is JSON naming irimi and the target, not mitmproxy's HTML error page. The flag
    was right all along; the page was not. An SDK parses the body, and stripe-python, openai and
    slack_sdk all raise on an HTML blob that names neither irimi nor the answer target, so "my
    shadow run started failing" gave no hint that the developer's own stub was down (#38).
    """
    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    dead = closed.getsockname()[1]
    closed.close()
    target_url = f"http://127.0.0.1:{dead}/w"
    maps = _targeted(tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", target_url)])
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    assert status == 502
    body = json.loads(data)
    assert body["error"]["type"] == "irimi_target_failed"
    assert target_url in body["error"]["message"]
    (ex,) = seen
    assert ex.answered_by == "delegated"
    assert "target-failed" in ex.flags
    assert ex.target == target_url
    assert ex.response is not None and ex.response.status == 502


def test_a_target_that_stops_listening_mid_run_is_still_flagged(tmp_path, monkeypatch, target):
    """The probe is best-effort by construction - a stub can die between the probe and the dial -
    so the properties that must not depend on it are pinned separately: no fall back to the fake,
    `target-failed` on the exchange, and a 502 to the agent. Here the stub answers the first write
    and is gone for the second."""
    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        first, _ = _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
        _kill_target()
        second, _ = _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    assert (first, second) == (200, 502)
    assert [ex.answered_by for ex in seen] == ["delegated", "delegated"]
    assert "target-failed" not in seen[0].flags
    assert "target-failed" in seen[1].flags


def test_a_faked_write_on_a_route_with_no_fixture_still_lists_its_webhooks(tmp_path, monkeypatch):
    """`fires:` is about the write, not the answer's fidelity: DEMO_MAP's route names no
    `fixture:`, so the fake is L0, and it was still accepted (#47)."""
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch)))
    try:
        status, _ = _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    assert status == 200
    (ex,) = seen
    assert ex.answered_by == "fake-L0"
    assert ex.would_fire == ("thing.created",)


def test_a_delegated_write_lists_no_webhooks_whether_or_not_its_target_answers(
    tmp_path, monkeypatch, target
):
    """The target performed the write, or did something else with it, or was not there; irimi
    built nothing and claims nothing about what the real service would have sent (#47). The
    route names `fires:`, so the empty lists are the policy withholding it, not a map without
    one - the test above is the same route faked."""
    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        first, _ = _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
        _kill_target()
        second, _ = _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    assert (first, second) == (200, 502)
    assert [ex.answered_by for ex in seen] == ["delegated", "delegated"]
    assert "target-failed" in seen[1].flags
    assert [ex.would_fire for ex in seen] == [(), ()]


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
    asyncio.run(addon.request(flow))

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
    asyncio.run(addon.request(flow))
    addon.error(flow)

    (ex,) = seen
    assert ex.answered_by == "fake-L0"
    assert "fidelity:L0" in ex.flags
    assert "upstream-error" not in ex.flags


def test_a_refused_target_is_flagged_once_even_if_the_client_then_vanishes(tmp_path, monkeypatch):
    """`request()` already answered and flagged this one; the client going away afterwards does
    not make the target fail a second time. The flag is a fact about the exchange, not a counter
    (#32). Driven through the addon, like every other client-disappears case."""
    from mitmproxy.test import tflow, tutils

    from irimi.engine.mitm import IrimiAddon

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
    asyncio.run(addon.request(flow))  # the target is our own listener, so refused and flagged here
    addon.error(flow)

    (ex,) = seen
    assert ex.answered_by == "delegated"
    assert ex.flags.count("target-failed") == 1


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

    monkeypatch.setattr(delegation, "target_url", boom)
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
    assert seen[0].flags == (UNCLASSIFIED_FLAG, DECISION_FAILED_FLAG)
    assert (seen[0].kind, seen[0].answered_by) == ("unknown", "fake-L0")


def _bare_addon(tmp_path, monkeypatch, policy=None, overlay=None, on_exchange=None):
    """An IrimiAddon over DEMO_MAP, driven hook by hook with no proxy around it."""
    from irimi.engine.mitm import IrimiAddon

    return IrimiAddon(
        _config(tmp_path, monkeypatch, maps=_targeted(tmp_path, monkeypatch)),
        policy or ShadowPolicy(),
        NullStore(),
        overlay or NoOverlay(),
        on_exchange or (lambda ex: None),
        lambda port, error: None,
    )


def _bare_flow(method, path):
    from mitmproxy.test import tflow, tutils

    flow = tflow.tflow(req=tutils.treq(method=method, host="127.0.0.1", port=80, path=path))
    flow.client_conn.sockname = ("127.0.0.1", 4000)
    return flow


def test_a_raise_handing_the_decision_to_a_worker_answers_locally(tmp_path, monkeypatch):
    """The hook awaits `asyncio.to_thread` as of #45, and the hand-off can fail on its own - an
    executor already shut down under a stopping proxy - before `_decide`'s guard is ever reached.
    It takes the same 502 as a decision that raised: the request may be a write."""
    from irimi.engine import mitm

    seen = []
    addon = _bare_addon(tmp_path, monkeypatch, on_exchange=seen.append)

    def no_threads(*args, **kwargs):
        raise RuntimeError("cannot schedule new futures after shutdown")

    monkeypatch.setattr(mitm.asyncio, "to_thread", no_threads)
    flow = _bare_flow(b"POST", b"/things")
    asyncio.run(addon.request(flow))

    assert flow.response.status_code == 502
    assert json.loads(flow.response.content)["error"]["type"] == "irimi_decision_failed"
    (ex,) = seen
    assert ex.flags == (UNCLASSIFIED_FLAG, DECISION_FAILED_FLAG)


def test_a_raise_carrying_a_rewrite_onto_the_flow_answers_locally(tmp_path, monkeypatch):
    """`_apply_rewrite` runs back on the loop, outside `_decide`'s guard (#45). Before the split
    its lines sat under the decision's backstop; they still do, so a raise there cannot escape
    the hook and forward a half-edited read with nothing recorded."""
    from dataclasses import replace

    class _Translating:
        def __call__(self, write_log, read_request, upstream_response):
            return Overlaid(upstream_response)

        def rewrite(self, write_log, read_request):
            return replace(read_request, query="translated=1")

    seen = []
    addon = _bare_addon(tmp_path, monkeypatch, overlay=_Translating(), on_exchange=seen.append)
    write = _bare_flow(b"POST", b"/things")
    asyncio.run(addon.request(write))
    addon.response(write)  # a faked write joins the log here, so the read below is rewritten

    def boom(flow, rewritten):
        raise RuntimeError("the flow edit exploded")

    monkeypatch.setattr(addon, "_apply_rewrite", boom)
    read = _bare_flow(b"GET", b"/hello")
    asyncio.run(addon.request(read))

    assert read.response.status_code == 502
    assert json.loads(read.response.content)["error"]["type"] == "irimi_decision_failed"
    assert read.request.path == "/hello"
    assert seen[-1].flags == (UNCLASSIFIED_FLAG, DECISION_FAILED_FLAG)


def test_a_raise_recording_an_engine_read_leaves_the_write_answered(tmp_path, monkeypatch):
    """`Answer.issued` is recorded last, after the write's own answer is on the flow (#45). A store
    or `on_exchange` that raises there escapes the hook, and mitmproxy forwards a flow only when
    it has no response - so the write is still answered locally and still pending its record."""
    from irimi.engine.mitm import META_KEY

    probe = Request(
        method="GET",
        scheme="http",
        host="127.0.0.1",
        port=80,
        path="/hello",
        query="",
        headers=(),
        body=b"",
    )
    issued = pipeline.annotate(
        probe,
        Response(status=200, headers=(), body=b"{}"),
        pipeline.Classification(service="demo", operation="things.list", kind="read", flags=()),
        "live",
        "",
        issued_by="engine",
    )

    class _Checking:
        name = "checking"

        def answer(self, request, classification, write_log=(), run_id=""):
            return Answer(
                answered_by="fake-L0",
                response=Response(status=200, headers=(), body=b"{}"),
                precondition="passed",
                issued=(issued,),
            )

    def refuse_engine_reads(ex):
        if ex.issued_by == "engine":
            raise RuntimeError("the store exploded")

    addon = _bare_addon(tmp_path, monkeypatch, _Checking(), on_exchange=refuse_engine_reads)
    flow = _bare_flow(b"POST", b"/things")
    with pytest.raises(RuntimeError, match="the store exploded"):
        asyncio.run(addon.request(flow))

    assert flow.response is not None and flow.response.status_code == 200
    assert flow.metadata[META_KEY].precondition == "passed"


def test_neither_half_of_a_delegated_exchange_is_a_write(tmp_path, monkeypatch, target):
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
        # Nor is the delegated *write*, since #43: the target performed it, or did not, and the
        # overlay cannot replay a write irimi did not author.
        _via_proxy(eng.listen_port(), "POST", "http://127.0.0.1:1/things", body=b"{}")
        assert addon.write_log == [], "a delegated write is in the overlay's write log"
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
        raise delegation.TargetRefused("refusing this target")

    monkeypatch.setattr(delegation, "refuse_self_target", refuse)
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


def test_the_response_hook_writes_nothing_back_to_a_streamed_flow(tmp_path, monkeypatch):
    """The other half of #28, at the seam rather than end to end.

    A streamed response's headers were sent before this hook ran, and its body is never assembled,
    so anything `response()` writes back is either ignored or destructive: rebuilding the flow
    replaces a live stream with a buffered, empty-bodied response. `responseheaders` is the hook
    that owns a streamed answer - it is where the `Irimi-Answered-By` stamp goes for exactly this
    reason - and it is deliberately not called here, so that a stamp appearing on the flow is
    proof that `response()` wrote to it.

    Driven through the addon because no client can observe the difference: a write that lands
    after the headers are on the wire is invisible until the day it carries a body.
    """
    from mitmproxy.test import tflow, tutils

    from irimi.engine.mitm import META_KEY, IrimiAddon

    maps = _targeted(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", "http://127.0.0.1:3999/w")]
    )
    addon = IrimiAddon(
        _config(tmp_path, monkeypatch, maps=maps),
        ShadowPolicy(),
        NullStore(),
        NoOverlay(),
        lambda ex: None,
        lambda port, error: None,
    )
    flow = tflow.tflow(
        req=tutils.treq(method=b"POST", host="127.0.0.1", port=80, path=b"/things"),
        resp=tutils.tresp(content=None, headers=((b"content-type", b"text/event-stream"),)),
    )
    flow.client_conn.sockname = ("127.0.0.1", 4000)
    asyncio.run(addon.request(flow))
    assert flow.metadata[META_KEY].answered_by == "delegated"
    flow.response.stream = True  # what `responseheaders` does for an event stream
    before = (flow.response.status_code, tuple(flow.response.headers.fields), flow.response.content)

    addon.response(flow)

    after = (flow.response.status_code, tuple(flow.response.headers.fields), flow.response.content)
    assert after == before
    assert pipeline.ANSWERED_BY_HEADER not in flow.response.headers


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


# ------------------------------------------------------ the overlay through the engine (#43)


class _OverlayDouble:
    """An overlay whose answers a test chooses, counting how often the engine asked it."""

    def __init__(self, answer=None, rewrite=None):
        self._answer = answer or (lambda upstream: Overlaid(upstream))
        self._rewrite = rewrite or (lambda request: request)
        self.calls = 0
        self.rewrites = 0

    def __call__(self, write_log, read_request, upstream_response):
        self.calls += 1
        return self._answer(upstream_response)

    def rewrite(self, write_log, read_request):
        self.rewrites += 1
        return self._rewrite(read_request)


def _read_via_proxy(proxy_port, url, extra_headers=None, method="GET", body=None):
    """A read through the proxy that returns the response headers too: the stamp is on the wire
    only, never on the recorded exchange. A GET unless told otherwise: Slack reads are POSTs."""
    conn = http.client.HTTPConnection("127.0.0.1", proxy_port, timeout=10)
    headers = {"host": url.split("/")[2]}
    headers.update(extra_headers or {})
    conn.request(method, url, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    received = {k.lower(): v for k, v in resp.getheaders()}
    conn.close()
    return resp.status, received, data


def _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay):
    """An engine over DEMO_MAP whose write log already holds one faked write."""
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch))
    eng, seen, stop = _start(cfg, overlay=overlay)
    try:
        addon = next(a for a in eng._master.addons.chain if isinstance(a, IrimiAddon))
        _via_proxy(eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")
        assert addon.write_log, "the engine only asks the overlay once the log holds a write"
    except BaseException:
        stop()
        raise
    return eng, seen, stop


def _overlaid_read(tmp_path, monkeypatch, upstream, overlay):
    """One faked write, then one live read, through an engine using `overlay`."""
    eng, seen, stop = _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay)
    try:
        status, headers, data = _read_via_proxy(
            eng.listen_port(), f"http://127.0.0.1:{upstream}/hello"
        )
    finally:
        stop()
    return status, headers, data, seen[-1]


def test_an_overlay_that_changes_a_read_stamps_it_overlay(tmp_path, monkeypatch, upstream):
    changed = Response(200, (("content-type", "application/json"),), b'{"overlaid": true}')
    overlay = _OverlayDouble(answer=lambda upstream: Overlaid(changed, "full"))
    status, headers, data, ex = _overlaid_read(tmp_path, monkeypatch, upstream, overlay)
    assert (status, data) == (200, b'{"overlaid": true}')
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"
    assert ex.answered_by == "overlay"
    assert "fidelity:overlay" in ex.flags
    assert ex.overlay == "full"


def test_an_overlay_that_changes_nothing_leaves_the_read_live_and_unstamped(
    tmp_path, monkeypatch, upstream
):
    overlay = _OverlayDouble(answer=lambda upstream: Overlaid(upstream))
    status, headers, data, ex = _overlaid_read(tmp_path, monkeypatch, upstream, overlay)
    assert overlay.calls == 1
    assert (status, data) == (200, b"hello from upstream")
    assert pipeline.ANSWERED_BY_HEADER not in headers
    assert ex.answered_by == "live"
    assert ex.overlay is None
    assert ex.response.body == b"hello from upstream"


def test_an_overlay_may_flag_a_read_partial_without_touching_it(tmp_path, monkeypatch, upstream):
    overlay = _OverlayDouble(answer=lambda upstream: Overlaid(upstream, "partial"))
    status, headers, data, ex = _overlaid_read(tmp_path, monkeypatch, upstream, overlay)
    assert (status, data) == (200, b"hello from upstream")
    assert pipeline.ANSWERED_BY_HEADER not in headers
    assert ex.answered_by == "live"
    assert ex.overlay == "partial"


def test_an_overlay_that_raises_does_not_break_the_read(tmp_path, monkeypatch, upstream):
    def boom(upstream):
        raise RuntimeError("the overlay exploded")

    overlay = _OverlayDouble(answer=boom)
    status, headers, data, ex = _overlaid_read(tmp_path, monkeypatch, upstream, overlay)
    assert overlay.calls == 1
    assert (status, data) == (200, b"hello from upstream")
    assert pipeline.ANSWERED_BY_HEADER not in headers
    assert ex.answered_by == "live"
    assert ex.overlay == "partial"


def test_a_rewritten_read_reaches_the_upstream_translated_and_is_recorded_that_way(
    tmp_path, monkeypatch, upstream
):
    from dataclasses import replace

    stamp = (pipeline.REWROTE_HEADER, "starting_after=re_MINTED1")

    def translate(request):
        return replace(request, query="limit=1", headers=request.headers + (stamp,))

    overlay = _OverlayDouble(rewrite=translate)
    eng, seen, stop = _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay)
    try:
        status, _, _ = _read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello?limit=1&starting_after=re_MINTED1",
        )
    finally:
        stop()
    assert status == 200
    ((path, received),) = _Upstream.seen
    assert path == "/hello?limit=1"
    assert {k.lower(): v for k, v in received}[stamp[0]] == stamp[1]
    ex = seen[-1]
    assert ex.request.query == "limit=1"
    assert stamp in ex.request.headers


def test_a_rewrite_that_raises_forwards_the_read_unchanged(tmp_path, monkeypatch, upstream):
    def boom(request):
        raise RuntimeError("the rewrite exploded")

    overlay = _OverlayDouble(rewrite=boom)
    eng, seen, stop = _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay)
    try:
        status, _, data = _read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello?limit=1&starting_after=re_MINTED1",
        )
    finally:
        stop()
    assert overlay.rewrites == 1
    assert (status, data) == (200, b"hello from upstream")
    assert [path for path, _ in _Upstream.seen] == ["/hello?limit=1&starting_after=re_MINTED1"]
    ex = seen[-1]
    assert ex.answered_by == "live"
    assert DECISION_FAILED_FLAG not in ex.flags


def test_the_overlay_is_not_asked_about_a_read_before_any_write(tmp_path, monkeypatch, upstream):
    overlay = _OverlayDouble()
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch))
    eng, seen, stop = _start(cfg, overlay=overlay)
    try:
        status, _ = _via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    finally:
        stop()
    assert status == 200
    # The gate #53 deliberately did NOT widen: the header is stripped by the engine instead, so
    # the overlay still sees no read until the write log holds a write (Notion, § Architecture
    # changes).
    assert (overlay.calls, overlay.rewrites) == (0, 0)
    assert seen[-1].overlay is None


def test_a_rewrote_header_the_agent_sent_is_not_forwarded_upstream(tmp_path, monkeypatch, upstream):
    """Only irimi may tell the response side that a page follows a minted refund. The overlay
    strips an agent's own `irimi-rewrote`, and the engine has to carry that onto the flow too, or
    the header reaches the real service anyway (#43).

    Since #53 the engine strips it in `request()` before the overlay is asked, so this now holds
    even for an overlay whose `rewrite` strips nothing. It stays as the non-empty-log twin of
    `test_a_rewrote_header_the_agent_sent_is_stripped_before_the_runs_first_write`.
    """
    from dataclasses import replace

    def strip(request):
        kept = tuple((k, v) for k, v in request.headers if k != pipeline.REWROTE_HEADER)
        return replace(request, headers=kept)

    overlay = _OverlayDouble(rewrite=strip)
    eng, seen, stop = _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay)
    try:
        status, _, _ = _read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello",
            extra_headers={pipeline.REWROTE_HEADER: "starting_after=re_FORGED"},
        )
    finally:
        stop()
    assert status == 200
    ((path, received),) = _Upstream.seen
    assert path == "/hello"
    assert pipeline.REWROTE_HEADER not in {k.lower() for k, _ in received}


def test_a_rewrote_header_the_agent_sent_is_stripped_before_the_runs_first_write(
    tmp_path, monkeypatch, upstream
):
    """#53. The engine's rewrite path is closed until the write log holds a write, so the overlay's
    own strip could not run - and irimi's vocabulary reached the real service on a read irimi did
    not touch. The engine strips it in `request()` now, whatever the log holds.

    `_start` with no overlay gives `NoOverlay`, whose `rewrite` returns the request it was handed,
    so nothing but the engine could have removed the header here. Sent twice, in two spellings:
    a forged repeat has to lose every instance, not the first.
    """
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch))
    eng, seen, stop = _start(cfg)
    try:
        addon = next(a for a in eng._master.addons.chain if isinstance(a, IrimiAddon))
        status, _, _ = _read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello",
            extra_headers={
                pipeline.REWROTE_HEADER: "starting_after=re_FORGED",
                "Irimi-Rewrote": "starting_after=re_FORGED2",
            },
        )
        assert addon.write_log == [], "this test is about the empty-log path"
    finally:
        stop()
    assert status == 200
    ((path, received),) = _Upstream.seen
    assert path == "/hello"
    assert pipeline.REWROTE_HEADER not in {k.lower() for k, _ in received}


def test_a_rewrote_header_the_agent_sent_is_kept_out_of_the_recorded_request(
    tmp_path, monkeypatch, upstream
):
    """The Exchange records what irimi asked the service. It did not ask with this header, and the
    response side reads the recorded request - `ServiceOverlay._apply` looks for `Irimi-Rewrote`
    there - so leaving it on would let a forgery be believed by a later hook (#53)."""
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch))
    eng, seen, stop = _start(cfg)
    try:
        _read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello",
            extra_headers={pipeline.REWROTE_HEADER: "starting_after=re_FORGED"},
        )
    finally:
        stop()
    assert [k for k, _ in seen[-1].request.headers if k == pipeline.REWROTE_HEADER] == []


def test_only_irimis_own_rewrote_header_reaches_the_upstream(tmp_path, monkeypatch, upstream):
    """The forgery is gone and irimi's own is there, exactly once. Two headers of one name would
    make `stripe._refunds_list` read whichever mitmproxy happened to hand it first (#53)."""
    from dataclasses import replace

    stamp = (pipeline.REWROTE_HEADER, "starting_after=re_MINTED1")

    def translate(request):
        assert all(k != pipeline.REWROTE_HEADER for k, _ in request.headers), (
            "the engine strips the agent's before the overlay is asked"
        )
        return replace(request, query="limit=1", headers=request.headers + (stamp,))

    overlay = _OverlayDouble(rewrite=translate)
    eng, seen, stop = _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay)
    try:
        status, _, _ = _read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello?limit=1&starting_after=re_MINTED1",
            extra_headers={pipeline.REWROTE_HEADER: "starting_after=re_FORGED"},
        )
    finally:
        stop()
    assert status == 200
    ((path, received),) = _Upstream.seen
    assert path == "/hello?limit=1"
    present = [v for k, v in received if k.lower() == pipeline.REWROTE_HEADER]
    assert present == [stamp[1]]
    assert [v for k, v in seen[-1].request.headers if k == pipeline.REWROTE_HEADER] == [stamp[1]]


def test_a_write_carrying_a_rewrote_header_is_still_faked_and_records_none(
    tmp_path, monkeypatch, upstream
):
    """A write is answered locally, so the header never leaves the machine either way - but it must
    not survive on the recorded request, where a trace reader would read it as irimi's (#53)."""
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch))
    eng, seen, stop = _start(cfg)
    try:
        status, headers, _ = _read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/things",
            extra_headers={
                pipeline.REWROTE_HEADER: "starting_after=re_FORGED",
                "content-type": "application/json",
            },
            method="POST",
            body=b"{}",
        )
    finally:
        stop()
    assert status == 200
    assert headers[pipeline.ANSWERED_BY_HEADER] == "fake-L0"
    ex = seen[-1]
    assert ex.kind == "write"
    assert [k for k, _ in ex.request.headers if k == pipeline.REWROTE_HEADER] == []


# ------------------------------------------------ the Stripe overlay, end to end (#43)

# A map claiming the loopback upstream as `stripe`, so the real `ServiceOverlay` applies Stripe's
# effects table to reads through it. `fixture: refund` resolves against the shipped Stripe
# fixtures, which are keyed by the service name and not by the host.
STRIPE_MAP = """
version: 1
service: stripe
hosts:
  - 127.0.0.1
routes:
  - match:
      method: POST
      path: /v1/refunds
    operation: refunds.create
    kind: write
    human: refund {amount} on {charge}
    fixture: refund
    ids:
      id: re_
      balance_transaction: txn_
    fires:
      - refund.created
      - charge.refunded
  - match:
      method: GET
      path: /v1/refunds
    operation: refunds.list
    kind: read
    human: list refunds
  - match:
      method: GET
      path: /v1/charges
    operation: charges.list
    kind: read
    human: list charges
  - match:
      method: GET
      path: /v1/charges/{charge}
    operation: charges.retrieve
    kind: read
    human: get charge {charge}
  - match:
      method: POST
      path: /v1/customers/{customer}
    operation: customers.update
    kind: write
    human: update customer {customer}
    fixture: customer
    ids:
      id: cus_
  - match:
      method: GET
      path: /v1/customers/{customer}
    operation: customers.retrieve
    kind: read
    human: get customer {customer}
  - match:
      method: POST
      path: /v1/payment_intents/{payment_intent}/cancel
    operation: payment_intents.cancel
    kind: write
    human: cancel {payment_intent}
    fixture: payment_intent
    ids:
      id: pi_
  - match:
      method: GET
      path: /v1/payment_intents/{payment_intent}
    operation: payment_intents.retrieve
    kind: read
    human: get payment intent {payment_intent}
"""


class _StripeStub(BaseHTTPRequestHandler):
    """A Stripe-shaped upstream: real state for every read the effects table models.

    `/v1/refunds` answers a FULL page - `REFUNDS_PAGE_SIZE` real refunds, the same number a
    `limit` of that size asks for - so a test can prove the minted refund pushes the oldest real
    one off the page rather than making it longer than Stripe would.
    """

    seen: list = []  # (method, path) of every request, so a test can prove no write reached it
    REFUNDS_PAGE_SIZE = 2

    def do_GET(self):
        _StripeStub.seen.append(("GET", self.path))
        route = self.path.split("?", 1)[0]
        if route == "/v1/charges/ch_REAL1":
            status, document = (
                200,
                {
                    "id": "ch_REAL1",
                    "object": "charge",
                    "amount": 4900,
                    "amount_refunded": 0,
                    "refunded": False,
                    "currency": "usd",
                },
            )
        elif route == "/v1/refunds":
            real = [
                {"id": f"re_REAL{n}", "object": "refund", "charge": "ch_REAL1"}
                for n in range(1, _StripeStub.REFUNDS_PAGE_SIZE + 1)
            ]
            status, document = 200, {"object": "list", "has_more": False, "data": real}
        elif route == "/v1/customers/cus_REAL1":
            status, document = (
                200,
                {
                    "id": "cus_REAL1",
                    "object": "customer",
                    "email": "old@example.test",
                    "metadata": {"tier": "free"},
                },
            )
        elif route == "/v1/payment_intents/pi_REAL1":
            status, document = (
                200,
                {
                    "id": "pi_REAL1",
                    "object": "payment_intent",
                    "status": "requires_capture",
                    "canceled_at": None,
                    "cancellation_reason": None,
                },
            )
        else:
            status, document = 404, {"error": {"type": "invalid_request_error"}}
        body = json.dumps(document).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):  # must never be reached under ShadowPolicy
        _StripeStub.seen.append(("POST", self.path))
        self.send_response(500)
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, *args):  # keep pytest output clean
        pass


@pytest.fixture
def stripe_stub(tmp_path, monkeypatch):
    """The stub on a free loopback port, and an engine over STRIPE_MAP with the real overlay,
    built the way the CLI builds it. Yields (proxy port, stub port, recorded exchanges)."""
    from irimi.overlay import ServiceOverlay

    _StripeStub.seen = []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _StripeStub)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    maps = _maps(tmp_path, monkeypatch, doc=STRIPE_MAP)
    eng, seen, stop = _start(
        _config(tmp_path, monkeypatch, maps=maps), overlay=ServiceOverlay(maps)
    )
    yield eng.listen_port(), srv.server_address[1], seen
    stop()
    srv.shutdown()


def test_a_charge_reread_after_a_faked_refund_shows_the_refund(stripe_stub):
    proxy, stub, _ = stripe_stub
    status, _ = _via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/refunds",
        body=b"charge=ch_REAL1&amount=100",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    status, headers, data = _read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert status == 200
    charge = json.loads(data)
    assert charge["amount_refunded"] == 100
    assert charge["refunded"] is False
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"
    assert [method for method, _ in _StripeStub.seen] == ["GET"], "a faked write reached Stripe"


def test_a_faked_refund_records_the_webhooks_it_would_have_fired(stripe_stub):
    """#47's done-when through the proxy, where `_Pending` and the response hook's `annotate`
    carry it: a faked refund lists `refund.created` and `charge.refunded`, and the read after it
    lists nothing."""
    proxy, stub, seen = stripe_stub
    status, _ = _via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/refunds",
        body=b"charge=ch_REAL1&amount=100",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    status, _, _ = _read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert status == 200
    (write,) = [ex for ex in seen if ex.request.method == "POST"]
    (read,) = [ex for ex in seen if ex.request.method == "GET"]
    assert write.would_fire == ("refund.created", "charge.refunded")
    assert read.would_fire == ()
    assert "POST" not in [method for method, _ in _StripeStub.seen], "a faked write reached Stripe"


def test_the_refunds_list_shows_the_minted_refund_first(stripe_stub):
    proxy, stub, _ = stripe_stub
    status, data = _via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/refunds",
        body=b"charge=ch_REAL1&amount=100",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    refund_id = json.loads(data)["id"]
    assert refund_id.startswith("re_")
    status, headers, data = _read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/refunds")
    assert status == 200
    assert json.loads(data)["data"][0]["id"] == refund_id
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"


class _HeldRefundsStub(_StripeStub):
    """`_StripeStub`, except a refunds list is held at the upstream until the test lets it go, so
    a write can land while the read is in flight."""

    arrived = threading.Event()
    release = threading.Event()

    def do_GET(self):
        if self.path.split("?", 1)[0] == "/v1/refunds":
            _HeldRefundsStub.arrived.set()
            _HeldRefundsStub.release.wait(10)
        super().do_GET()


def test_a_forged_rewrote_header_cannot_hide_a_refund_that_lands_mid_read(tmp_path, monkeypatch):
    """The race #53 closed, which was a real forgery and not only noise. `request()` snapshots
    the write log, empty here, so the read is never rewritten; the refund then lands before the
    read's `response()`, which tests the live log and overlays the page. The overlay read the
    agent's forged `Irimi-Rewrote: starting_after=...` off the recorded request, took the page
    for the one after a minted refund, and left the refund off it."""
    from irimi.overlay import ServiceOverlay

    _StripeStub.seen = []
    _HeldRefundsStub.arrived.clear()
    _HeldRefundsStub.release.clear()
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _HeldRefundsStub)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    stub = srv.server_address[1]
    maps = _maps(tmp_path, monkeypatch, doc=STRIPE_MAP)
    eng, _, stop = _start(_config(tmp_path, monkeypatch, maps=maps), overlay=ServiceOverlay(maps))
    read: dict = {}

    def list_refunds():
        read["result"] = _read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{stub}/v1/refunds",
            extra_headers={pipeline.REWROTE_HEADER: "starting_after=re_FORGED"},
        )

    reader = threading.Thread(target=list_refunds)
    try:
        reader.start()
        assert _HeldRefundsStub.arrived.wait(10), "the read never reached the upstream"
        status, data = _via_proxy(
            eng.listen_port(),
            "POST",
            f"http://127.0.0.1:{stub}/v1/refunds",
            body=b"charge=ch_REAL1&amount=100",
            extra_headers={"content-type": "application/x-www-form-urlencoded"},
        )
        assert status == 200
        _HeldRefundsStub.release.set()
        reader.join(10)
    finally:
        _HeldRefundsStub.release.set()
        stop()
        srv.shutdown()
    status, headers, page = read["result"]
    assert status == 200
    assert json.loads(page)["data"][0]["id"] == json.loads(data)["id"]
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"


def _keyed_refund(proxy, stub, key, amount=100):
    """One refund through the proxy carrying `Idempotency-Key`, as stripe-python sends every POST.
    Returns (status, response headers, body)."""
    return _read_via_proxy(
        proxy,
        f"http://127.0.0.1:{stub}/v1/refunds",
        extra_headers={
            "content-type": "application/x-www-form-urlencoded",
            "idempotency-key": key,
        },
        method="POST",
        body=f"charge=ch_REAL1&amount={amount}".encode(),
    )


def test_a_retry_with_the_same_key_is_one_refund_not_two(stripe_stub):
    """The bug #46 exists for: a retry with the key it already sent got a second minted id, the
    engine logged it as a second write, and the overlay applied one 100 refund as 200. The retry
    now gets the first answer's own bytes and never reaches the write log."""
    proxy, stub, seen = stripe_stub
    status, first_headers, first = _keyed_refund(proxy, stub, "k-1")
    assert status == 200
    status, second_headers, second = _keyed_refund(proxy, stub, "k-1")
    assert status == 200
    assert second == first
    stamp = pipeline.ANSWERED_BY_HEADER
    assert second_headers[stamp] == first_headers[stamp]
    assert idempotency.REPLAYED_HEADER not in first_headers
    assert second_headers[idempotency.REPLAYED_HEADER] == idempotency.REPLAYED_VALUE
    refunds = [ex for ex in seen if ex.request.method == "POST"]
    assert [IDEMPOTENT_REPLAY_FLAG in ex.flags for ex in refunds] == [False, True]

    _, _, data = _read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert json.loads(data)["amount_refunded"] == 100
    _, _, data = _read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/refunds")
    minted = [r["id"] for r in json.loads(data)["data"] if not r["id"].startswith("re_REAL")]
    assert minted == [json.loads(first)["id"]]
    assert "POST" not in [method for method, _ in _StripeStub.seen], "a faked write reached Stripe"


def test_a_replayed_refund_records_no_webhooks(stripe_stub):
    """The retry's events are already on the first write's own exchange; listing them again
    would promise one refund's `refund.created` twice (#46, #47)."""
    proxy, stub, seen = stripe_stub
    _keyed_refund(proxy, stub, "k-1")
    _keyed_refund(proxy, stub, "k-1")
    refunds = [ex for ex in seen if ex.request.method == "POST"]
    assert [ex.would_fire for ex in refunds] == [("refund.created", "charge.refunded"), ()]
    assert [IDEMPOTENT_REPLAY_FLAG in ex.flags for ex in refunds] == [False, True]


def test_the_same_key_with_a_different_amount_is_refused(stripe_stub):
    """A key reused for a different write is Stripe's own `idempotency_error`, and it is not a
    write: the charge still shows the first refund only (#46)."""
    proxy, stub, seen = stripe_stub
    status, _, _ = _keyed_refund(proxy, stub, "k-2")
    assert status == 200
    status, _, data = _keyed_refund(proxy, stub, "k-2", amount=250)
    assert status == 400
    assert json.loads(data)["error"]["type"] == "idempotency_error"
    refused = [ex for ex in seen if ex.request.method == "POST"][-1]
    assert IDEMPOTENCY_CONFLICT_FLAG in refused.flags

    _, _, data = _read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert json.loads(data)["amount_refunded"] == 100


def test_a_reused_key_on_a_different_write_records_no_webhooks(stripe_stub):
    """The real service would have refused the reused key, so nothing would have fired. A
    conflict is not `precondition: rejected`, so it is pinned on its own (#46, #47)."""
    proxy, stub, seen = stripe_stub
    _keyed_refund(proxy, stub, "k-2")
    _keyed_refund(proxy, stub, "k-2", amount=250)
    first, refused = [ex for ex in seen if ex.request.method == "POST"]
    assert first.would_fire == ("refund.created", "charge.refunded")
    assert IDEMPOTENCY_CONFLICT_FLAG in refused.flags
    assert refused.would_fire == ()


def test_a_read_before_the_refund_is_untouched_and_unstamped(stripe_stub):
    proxy, stub, seen = stripe_stub
    status, headers, data = _read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert status == 200
    assert json.loads(data)["amount_refunded"] == 0
    assert pipeline.ANSWERED_BY_HEADER not in headers
    assert seen[-1].answered_by == "live"
    assert seen[-1].overlay is None
    # The same read after the refund IS overlaid, so the untouched one above was untouched
    # because it came first, not because this engine has no overlay.
    _via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/refunds",
        body=b"charge=ch_REAL1&amount=100",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    _, headers, data = _read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
    assert json.loads(data)["amount_refunded"] == 100
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"


def _fake_refund(proxy, stub, charge="ch_REAL1", amount=100):
    """One faked refund through the proxy. Returns the minted refund id."""
    status, data = _via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/refunds",
        body=f"charge={charge}&amount={amount}".encode(),
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    return json.loads(data)["id"]


def test_a_full_refunds_page_drops_the_oldest_real_refund_and_says_there_is_more(stripe_stub):
    """A page Stripe filled to the limit cannot also hold the minted refund. The page stays the
    length the agent asked for and `has_more` becomes true, because a longer page is one no real
    Stripe read could return (#43)."""
    proxy, stub, seen = stripe_stub
    refund_id = _fake_refund(proxy, stub)
    limit = _StripeStub.REFUNDS_PAGE_SIZE
    status, headers, data = _read_via_proxy(
        proxy, f"http://127.0.0.1:{stub}/v1/refunds?limit={limit}"
    )
    assert status == 200
    page = json.loads(data)
    assert [item["id"] for item in page["data"]] == [refund_id, "re_REAL1"]
    assert page["has_more"] is True
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"
    assert seen[-1].overlay == "full"


def test_a_cursor_naming_a_minted_refund_is_dropped_before_the_read_reaches_stripe(stripe_stub):
    """Stripe has never heard of the refund irimi minted and would answer the page with an error,
    so the cursor is dropped and `Irimi-Rewrote` carries what was removed. The page that comes back
    is the real list from its top, and the minted refund is NOT prepended again: it belongs on the
    page before this one (#43)."""
    proxy, stub, seen = stripe_stub
    refund_id = _fake_refund(proxy, stub)
    _StripeStub.seen.clear()
    status, headers, data = _read_via_proxy(
        proxy, f"http://127.0.0.1:{stub}/v1/refunds?starting_after={refund_id}"
    )
    assert status == 200
    assert _StripeStub.seen == [("GET", "/v1/refunds")], "the minted cursor reached Stripe"
    assert [item["id"] for item in json.loads(data)["data"]] == ["re_REAL1", "re_REAL2"]
    # The body really is the one Stripe sent, so it stays unstamped; the exchange is what records
    # that irimi translated the request, and shows the upstream what it asked.
    assert pipeline.ANSWERED_BY_HEADER not in headers
    ex = seen[-1]
    assert ex.answered_by == "live"
    assert ex.overlay == "full"
    assert ex.request.query == ""
    assert (pipeline.REWROTE_HEADER, f"starting_after={refund_id}") in ex.request.headers


def test_a_customer_reread_after_a_faked_update_shows_the_posted_fields(stripe_stub):
    proxy, stub, seen = stripe_stub
    status, _ = _via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/customers/cus_REAL1",
        body=b"email=new%40example.test&metadata[tier]=pro",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    status, headers, data = _read_via_proxy(
        proxy, f"http://127.0.0.1:{stub}/v1/customers/cus_REAL1"
    )
    assert status == 200
    customer = json.loads(data)
    assert customer["email"] == "new@example.test"
    assert customer["metadata"] == {"tier": "pro"}, "Stripe merges metadata, it does not replace it"
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"
    assert seen[-1].overlay == "full"
    assert [method for method, _ in _StripeStub.seen] == ["GET"], "a faked write reached Stripe"


def test_a_payment_intent_reread_after_a_faked_cancel_shows_it_canceled(stripe_stub):
    proxy, stub, seen = stripe_stub
    status, _ = _via_proxy(
        proxy,
        "POST",
        f"http://127.0.0.1:{stub}/v1/payment_intents/pi_REAL1/cancel",
        body=b"cancellation_reason=requested_by_customer",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    status, headers, data = _read_via_proxy(
        proxy, f"http://127.0.0.1:{stub}/v1/payment_intents/pi_REAL1"
    )
    assert status == 200
    intent = json.loads(data)
    assert intent["status"] == "canceled"
    assert intent["cancellation_reason"] == "requested_by_customer"
    assert isinstance(intent["canceled_at"], int)
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"
    assert seen[-1].overlay == "full"
    assert [method for method, _ in _StripeStub.seen] == ["GET"], "a faked write reached Stripe"


# ------------------------------------------------- the Slack overlay, end to end (#44)

# The loopback upstream claimed as `slack`, with the shipped map's two routes that matter here.
# `verbs: post-only` because a Slack read is a POST, as it is in `maps/slack.yaml`; `fixture:
# message` resolves against the shipped Slack fixtures, keyed by the service name.
SLACK_OVERLAY_MAP = """
version: 1
service: slack
verbs: post-only
hosts:
  - 127.0.0.1
routes:
  - match:
      method: POST
      path: /api/chat.postMessage
    operation: chat.postMessage
    kind: write
    human: post to {channel}
    fixture: message
  - match:
      method: POST
      path: /api/conversations.history
    operation: conversations.history
    kind: read
    human: read the {channel} history

  - match:
      method: POST
      path: /api/conversations.replies
    operation: conversations.replies
    kind: read
    human: read a thread in {channel}
"""
SLACK_JSON = "application/json;charset=utf-8"  # what slack_sdk sends (#44)
REAL_SLACK_TS = "1700000000.000100"


REAL_SLACK_MESSAGE = {"type": "message", "ts": REAL_SLACK_TS, "text": "real", "user": "U1"}


class _SlackStub(BaseHTTPRequestHandler):
    """A Slack-shaped upstream: one real message, in whatever channel or thread is asked about and
    with no replies of its own. Only the two reads may reach it; a `chat.postMessage` here is a
    faked write that escaped."""

    seen: list = []  # (method, path) of every request

    def do_POST(self):
        _SlackStub.seen.append(("POST", self.path))
        if self.path not in ("/api/conversations.history", "/api/conversations.replies"):
            self.send_response(500)
            self.send_header("content-length", "0")
            self.end_headers()
            return
        self.rfile.read(int(self.headers.get("content-length", 0)))
        document = {"ok": True, "messages": [dict(REAL_SLACK_MESSAGE)], "has_more": False}
        body = json.dumps(document).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # keep pytest output clean
        pass


@pytest.fixture
def slack_stub(tmp_path, monkeypatch):
    """`stripe_stub`'s twin over SLACK_OVERLAY_MAP. Yields (proxy port, stub port, exchanges)."""
    from irimi.overlay import ServiceOverlay

    _SlackStub.seen = []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _SlackStub)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    maps = _maps(tmp_path, monkeypatch, doc=SLACK_OVERLAY_MAP)
    eng, seen, stop = _start(
        _config(tmp_path, monkeypatch, maps=maps), overlay=ServiceOverlay(maps)
    )
    yield eng.listen_port(), srv.server_address[1], seen
    stop()
    srv.shutdown()


def _slack_call(proxy, stub, method, params):
    """One Slack Web API call through the proxy, as slack_sdk sends it: a JSON POST."""
    return _read_via_proxy(
        proxy,
        f"http://127.0.0.1:{stub}/api/{method}",
        extra_headers={"content-type": SLACK_JSON},
        method="POST",
        body=json.dumps(params).encode(),
    )


def test_a_slack_history_read_after_a_faked_post_shows_the_post_first(slack_stub):
    proxy, stub, seen = slack_stub
    status, _, data = _slack_call(
        proxy, stub, "chat.postMessage", {"channel": "C0123", "text": "hi"}
    )
    assert status == 200
    minted = json.loads(data)["ts"]
    status, headers, data = _slack_call(proxy, stub, "conversations.history", {"channel": "C0123"})
    assert status == 200
    assert [m["ts"] for m in json.loads(data)["messages"]] == [minted, REAL_SLACK_TS]
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"
    assert seen[-1].answered_by == "overlay"
    assert seen[-1].overlay == "full"
    assert _SlackStub.seen == [("POST", "/api/conversations.history")], "a faked post reached Slack"


def test_a_slack_history_read_of_another_channel_is_forwarded_unstamped(slack_stub):
    """Two different channel ids are a definite no, not a "cannot tell": the read is left as
    Slack sent it and is not `partial` (#44)."""
    proxy, stub, seen = slack_stub
    status, _, _ = _slack_call(proxy, stub, "chat.postMessage", {"channel": "C0123", "text": "hi"})
    assert status == 200
    # The log holds a faked write, so the overlay IS asked about the read below.
    assert seen[-1].kind == "write" and seen[-1].answered_by.startswith("fake-")
    status, headers, data = _slack_call(proxy, stub, "conversations.history", {"channel": "C0999"})
    assert status == 200
    assert [m["ts"] for m in json.loads(data)["messages"]] == [REAL_SLACK_TS]
    assert pipeline.ANSWERED_BY_HEADER not in headers
    assert seen[-1].answered_by == "live"
    assert seen[-1].overlay is None


def test_a_slack_replies_read_after_a_faked_reply_shows_it_at_the_tail(slack_stub):
    """#44's done-when for the second read: the reply last, the parent's counts moved, the whole
    page stamped `overlay` - and the post itself never reached Slack."""
    proxy, stub, seen = slack_stub
    status, _, data = _slack_call(
        proxy,
        stub,
        "chat.postMessage",
        {"channel": "C0123", "thread_ts": REAL_SLACK_TS, "text": "on it"},
    )
    assert status == 200
    minted = json.loads(data)["ts"]
    status, headers, data = _slack_call(
        proxy, stub, "conversations.replies", {"channel": "C0123", "ts": REAL_SLACK_TS}
    )
    assert status == 200
    messages = json.loads(data)["messages"]
    assert [m["ts"] for m in messages] == [REAL_SLACK_TS, minted]
    assert messages[0]["reply_count"] == 1
    assert messages[0]["latest_reply"] == minted
    assert messages[-1]["thread_ts"] == REAL_SLACK_TS
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"
    assert seen[-1].answered_by == "overlay"
    assert seen[-1].overlay == "full"
    assert _SlackStub.seen == [("POST", "/api/conversations.replies")], "a faked post reached Slack"


def test_a_slack_read_the_effects_cannot_place_is_recorded_partial_and_left_alone(slack_stub):
    """`chat.postMessage` takes `#general`, `conversations.history` requires the id, and irimi's own
    echo carries the posted spelling - so the two sides cannot be matched. The body is left exactly
    as Slack sent it and the exchange says the world irimi showed is incomplete (#44)."""
    proxy, stub, seen = slack_stub
    status, _, _ = _slack_call(
        proxy, stub, "chat.postMessage", {"channel": "#general", "text": "hi"}
    )
    assert status == 200
    status, headers, data = _slack_call(proxy, stub, "conversations.history", {"channel": "C0123"})
    assert status == 200
    assert [m["ts"] for m in json.loads(data)["messages"]] == [REAL_SLACK_TS]
    assert pipeline.ANSWERED_BY_HEADER not in headers, "nothing of irimi's is in this body"
    assert seen[-1].answered_by == "live"
    assert seen[-1].overlay == "partial"


# ------------------------------------------------ L3 preconditions through the engine (#45)

# STRIPE_MAP with the shipped map's `precondition:` on `refunds.create`. Its own copy, so the
# `stripe_stub` tests above keep meaning exactly what they meant and issue no probes.
PRECONDITION_STRIPE_MAP = STRIPE_MAP.replace(
    "    fixture: refund\n", "    fixture: refund\n    precondition: charge_refundable\n"
)
SLOW_S = 1.0  # how long `ch_SLOW1`'s precondition read takes to answer


def _charge(charge_id, amount_refunded=0):
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


class _PreconditionStub(BaseHTTPRequestHandler):
    """The real Stripe as L3 sees it: one refundable charge, one fully refunded, one slow to
    answer and one rate-limited. A POST reaching it is a faked write that escaped."""

    seen: list = []  # (method, path) of every request

    def do_GET(self):
        _PreconditionStub.seen.append(("GET", self.path))
        route = self.path.split("?", 1)[0]
        if route == "/v1/charges/ch_REAL1":
            status, document = 200, _charge("ch_REAL1")
        elif route == "/v1/charges/ch_FULL1":
            status, document = 200, _charge("ch_FULL1", amount_refunded=4900)
        elif route == "/v1/charges/ch_SLOW1":
            time.sleep(SLOW_S)
            status, document = 200, _charge("ch_SLOW1")
        elif route == "/v1/charges/ch_BUSY1":
            status, document = 429, {"error": {"type": "rate_limit_error"}}
        elif route == "/v1/charges":
            status, document = (
                200,
                {"object": "list", "has_more": False, "data": [_charge("ch_REAL1")]},
            )
        elif route == "/v1/refunds":
            status, document = 200, {"object": "list", "has_more": False, "data": []}
        else:
            status, document = 404, {"error": {"type": "invalid_request_error"}}
        body = json.dumps(document).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):  # must never be reached under ShadowPolicy
        _PreconditionStub.seen.append(("POST", self.path))
        self.send_response(500)
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, *args):  # keep pytest output clean
        pass


@pytest.fixture
def precondition_stub(tmp_path, monkeypatch):
    """The stub, and an engine over PRECONDITION_STRIPE_MAP built the way the CLI builds one: the
    real overlay and a policy holding the real reader. Yields (proxy port, stub port, exchanges)."""
    from irimi.overlay import ServiceOverlay
    from irimi.policy import UpstreamReader

    _PreconditionStub.seen = []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _PreconditionStub)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    maps = _maps(tmp_path, monkeypatch, doc=PRECONDITION_STRIPE_MAP)
    eng, seen, stop = _start(
        _config(tmp_path, monkeypatch, maps=maps),
        overlay=ServiceOverlay(maps),
        policy=ShadowPolicy(reader=UpstreamReader(), maps=maps),
    )
    yield eng.listen_port(), srv.server_address[1], seen
    stop()
    srv.shutdown()


def _refund(proxy, stub, charge, amount=100):
    """One refund through the proxy, form-encoded as stripe-python posts it."""
    return _read_via_proxy(
        proxy,
        f"http://127.0.0.1:{stub}/v1/refunds",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
        method="POST",
        body=f"charge={charge}&amount={amount}".encode(),
    )


def _writes(seen):
    return [ex for ex in seen if ex.kind == "write"]


def test_a_refund_of_a_fully_refunded_charge_is_rejected_with_stripes_own_body(
    precondition_stub,
):
    proxy, stub, seen = precondition_stub
    status, headers, data = _refund(proxy, stub, "ch_FULL1")
    assert status == 400
    assert json.loads(data)["error"]["code"] == "charge_already_refunded"
    assert headers[pipeline.ANSWERED_BY_HEADER] == "fake-L1"
    (write,) = _writes(seen)
    assert write.precondition == "rejected"
    assert write.rejection_code == "charge_already_refunded"


def test_a_rejected_refund_records_no_webhooks(precondition_stub):
    """L3 said the real service would have refused this refund: nothing was performed, so
    nothing would have fired (#45, #47). A refund L3 passed goes first, so the empty list is
    this map's `fires:` withheld and not a map that never had one."""
    proxy, stub, seen = precondition_stub
    status, _, _ = _refund(proxy, stub, "ch_REAL1")
    assert status == 200
    status, _, _ = _refund(proxy, stub, "ch_FULL1")
    assert status == 400
    passed, write = _writes(seen)
    assert passed.would_fire == ("refund.created", "charge.refunded")
    assert write.precondition == "rejected"
    assert write.rejection_code == "charge_already_refunded"
    assert write.would_fire == ()


def test_a_second_refund_in_one_run_is_rejected_because_the_overlay_applied_the_first(
    precondition_stub,
):
    """#45's done-when. The real charge is untouched and refundable both times; what makes the
    second refund one Stripe would refuse is the first, which only irimi's write log knows about."""
    proxy, stub, seen = precondition_stub
    first, _, data = _refund(proxy, stub, "ch_REAL1", amount=4900)
    assert first == 200
    assert json.loads(data)["object"] == "refund"
    second, headers, data = _refund(proxy, stub, "ch_REAL1", amount=4900)
    assert second == 400
    assert json.loads(data)["error"]["code"] == "charge_already_refunded"
    assert headers[pipeline.ANSWERED_BY_HEADER] == "fake-L1"
    assert [ex.precondition for ex in _writes(seen)] == ["passed", "rejected"]


def test_a_rejected_write_never_enters_the_write_log(precondition_stub):
    """Had the rejected refund been logged, the overlay would add it to the charge re-read below
    and stamp the read `overlay`."""
    proxy, stub, seen = precondition_stub
    status, _, _ = _refund(proxy, stub, "ch_FULL1")
    assert status == 400
    status, headers, data = _read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_FULL1")
    assert status == 200
    assert pipeline.ANSWERED_BY_HEADER not in headers
    assert json.loads(data)["amount_refunded"] == 4900  # the stub's own
    assert seen[-1].answered_by == "live"
    assert seen[-1].overlay is None


def test_the_precondition_read_is_recorded_as_issued_by_the_engine(precondition_stub):
    proxy, stub, seen = precondition_stub
    status, _, _ = _refund(proxy, stub, "ch_REAL1")
    assert status == 200
    (issued,) = [ex for ex in seen if ex.issued_by == "engine"]
    assert (issued.kind, issued.answered_by) == ("read", "live")
    assert (issued.request.method, issued.request.path) == ("GET", "/v1/charges/ch_REAL1")
    assert [ex.issued_by for ex in seen if ex is not issued] == ["agent"]


def test_a_precondition_read_that_429s_leaves_the_write_faked_at_l2(precondition_stub):
    proxy, stub, seen = precondition_stub
    status, headers, data = _refund(proxy, stub, "ch_BUSY1")
    assert status == 200
    assert json.loads(data)["object"] == "refund"
    assert headers[pipeline.ANSWERED_BY_HEADER] == "fake-L1"
    (write,) = _writes(seen)
    assert (write.answered_by, write.precondition) == ("fake-L1", "not_evaluable")
    (issued,) = [ex for ex in seen if ex.issued_by == "engine"]
    assert issued.response is not None and issued.response.status == 429


def test_a_refund_l3_could_not_check_still_lists_its_webhooks(precondition_stub):
    """`not_evaluable` is not `rejected`: irimi could not find out, faked the write anyway, and
    so accepted it - its events are listed like a passed refund's. Only a refusal lists nothing
    (#45, #47)."""
    proxy, stub, seen = precondition_stub
    status, _, _ = _refund(proxy, stub, "ch_BUSY1")
    assert status == 200
    (write,) = _writes(seen)
    assert write.precondition == "not_evaluable"
    assert write.would_fire == ("refund.created", "charge.refunded")


def test_a_concurrent_request_is_not_delayed_by_another_requests_precondition_read(
    precondition_stub,
):
    """The request hook is `async def` and the decision runs on a worker thread, so a read that
    arrives while a write's precondition read is still in the air is served straight away. With
    the old synchronous hook B would have waited out the whole of A's probe."""
    proxy, stub, _ = precondition_stub
    took: dict[str, float] = {}

    def a():
        start = time.monotonic()
        _refund(proxy, stub, "ch_SLOW1")
        took["a"] = time.monotonic() - start

    def b():
        start = time.monotonic()
        _read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
        took["b"] = time.monotonic() - start

    thread_a = threading.Thread(target=a)
    thread_b = threading.Thread(target=b)
    thread_a.start()
    time.sleep(0.1)
    thread_b.start()
    thread_a.join(timeout=15)
    thread_b.join(timeout=15)
    assert took["a"] >= SLOW_S
    assert took["b"] < SLOW_S * 0.6


def test_the_stub_never_saw_a_post(precondition_stub):
    proxy, stub, _ = precondition_stub
    assert _refund(proxy, stub, "ch_REAL1")[0] == 200
    assert _refund(proxy, stub, "ch_FULL1")[0] == 400
    assert _PreconditionStub.seen == [
        ("GET", "/v1/charges/ch_REAL1"),
        ("GET", "/v1/charges/ch_FULL1"),
    ]


def _refund_then_list_summary(proxy, stub, seen, tmp_path, monkeypatch):
    """A full refund of `ch_REAL1` and then the agent's own refunds list, through the proxy, and
    the summary that run prints.

    The index is built again by the same `_maps` call `precondition_stub` made, so the write line
    is the map's `human:` sentence and not the bare request. The fixture does not yield its index
    because nine tests unpack it and only these two print a summary (#48)."""
    status, _, _ = _refund(proxy, stub, "ch_REAL1", amount=4900)
    assert status == 200
    status, headers, _ = _read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/refunds")
    assert status == 200
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"
    maps = _maps(tmp_path, monkeypatch, doc=PRECONDITION_STRIPE_MAP)
    return report.summary_lines("t3st", seen, 1.0, maps)


def test_an_overlaid_read_is_listed_under_the_write_it_saw_through_the_proxy(
    precondition_stub, tmp_path, monkeypatch
):
    """The `↳` line off a real run (#48): the refunds list the overlay edited to show the refund
    sits directly under that refund, and is the only `↳` line. The precondition read L3 issued
    between them is counted as an engine read and never in `N reads` (#45); it is live and
    unedited, so it owes no line either way."""
    proxy, stub, seen = precondition_stub
    lines = _refund_then_list_summary(proxy, stub, seen, tmp_path, monkeypatch)
    # `$49.00` and not `4900` since #60: the write names no currency, and irimi's own precondition
    # read of `ch_REAL1` found `usd` on the charge.
    write = lines.index("  ○ refund $49.00 on ch_REAL1  unvalidated (L3 preconditions passed)")
    assert lines[write + 1] == "    ↳ GET /v1/refunds saw it  overlay"
    assert [line for line in lines if "↳" in line] == [lines[write + 1]]
    assert lines[2] == (
        "  127.0.0.1  1 read (1 showing this run's writes)  1 engine read  1 write intercepted"
    )


def test_the_closing_line_names_the_webhooks_a_real_refund_would_have_sent(
    precondition_stub, tmp_path, monkeypatch
):
    """The closing clause off a real run (#48): the events are the ones the engine put on the
    accepted refund's `Exchange.would_fire`, named once each in the map's order (#47)."""
    proxy, stub, seen = precondition_stub
    lines = _refund_then_list_summary(proxy, stub, seen, tmp_path, monkeypatch)
    assert [ex.would_fire for ex in _writes(seen)] == [("refund.created", "charge.refunded")]
    assert lines[-1] == (
        "  These writes did not happen. Would have fired: refund.created, charge.refunded."
    )


def test_a_faked_refund_carries_the_currency_its_precondition_read_found(precondition_stub):
    """#60 end to end through the real proxy: the charge the stub answers is in `usd`, the refund
    posts no currency, and the exchange carries the code across so the summary can format it."""
    proxy, stub, seen = precondition_stub
    status, _, _ = _refund(proxy, stub, "ch_REAL1")
    assert status == 200
    (write,) = _writes(seen)
    assert write.precondition == "passed"
    assert write.currency == "usd"


def test_a_write_whose_precondition_read_never_answered_carries_no_currency(precondition_stub):
    """`ch_BUSY1` 429s, so no document was read and there is nothing to denominate the amount
    with. The write is still faked at L2 and its line still prints raw minor units (#60)."""
    proxy, stub, seen = precondition_stub
    status, _, _ = _refund(proxy, stub, "ch_BUSY1")
    assert status == 200
    (write,) = _writes(seen)
    assert write.precondition == "not_evaluable"
    assert write.currency == ""


# SLACK_OVERLAY_MAP with the shipped map's `precondition:` on `chat.postMessage`, and the
# `conversations.info` read the probe has to classify as - the policy refuses to issue anything
# the maps do not call a read.
SLACK_PRECONDITION_MAP = SLACK_OVERLAY_MAP.replace(
    "    fixture: message\n", "    fixture: message\n    precondition: channel_postable\n"
) + (
    "  - match:\n"
    "      method: POST\n"
    "      path: /api/conversations.info\n"
    "    operation: conversations.info\n"
    "    kind: read\n"
    "    human: look up {channel}\n"
)


class _SlackPreconditionStub(BaseHTTPRequestHandler):
    """Slack as L3 sees it: `C0OK` is a healthy channel the bot is in, `C0ARCHIVED` is archived.
    A `chat.postMessage` here is a faked post that escaped."""

    seen: list = []  # (method, path) of every request

    def do_POST(self):
        _SlackPreconditionStub.seen.append(("POST", self.path))
        posted = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))) or b"{}")
        if self.path != "/api/conversations.info":
            self.send_response(500)
            self.send_header("content-length", "0")
            self.end_headers()
            return
        channel = posted.get("channel")
        if channel in ("C0OK", "C0ARCHIVED"):
            document = {
                "ok": True,
                "channel": {
                    "id": channel,
                    "is_channel": True,
                    "is_member": True,
                    "is_archived": channel == "C0ARCHIVED",
                },
            }
        else:
            document = {"ok": False, "error": "channel_not_found"}
        body = json.dumps(document).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # keep pytest output clean
        pass


@pytest.fixture
def slack_precondition_stub(tmp_path, monkeypatch):
    """`precondition_stub`'s Slack twin. Yields (proxy port, stub port, exchanges)."""
    from irimi.overlay import ServiceOverlay
    from irimi.policy import UpstreamReader

    _SlackPreconditionStub.seen = []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _SlackPreconditionStub)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    maps = _maps(tmp_path, monkeypatch, doc=SLACK_PRECONDITION_MAP)
    eng, seen, stop = _start(
        _config(tmp_path, monkeypatch, maps=maps),
        overlay=ServiceOverlay(maps),
        policy=ShadowPolicy(reader=UpstreamReader(), maps=maps),
    )
    yield eng.listen_port(), srv.server_address[1], seen
    stop()
    srv.shutdown()


def test_a_post_to_an_archived_channel_is_rejected(slack_precondition_stub):
    """Slack's own envelope, `ok: false` at 200, which is what makes slack_sdk raise
    `SlackApiError` - asserted on the body, since the SDK is not a test dependency."""
    proxy, stub, seen = slack_precondition_stub
    status, headers, data = _slack_call(
        proxy, stub, "chat.postMessage", {"channel": "C0ARCHIVED", "text": "hi"}
    )
    assert status == 200
    assert json.loads(data) == {"ok": False, "error": "is_archived"}
    assert headers[pipeline.ANSWERED_BY_HEADER] == "fake-L1"
    (write,) = _writes(seen)
    assert (write.precondition, write.rejection_code) == ("rejected", "is_archived")


def test_a_post_to_a_healthy_channel_passes_and_the_probe_was_a_conversations_info(
    slack_precondition_stub,
):
    proxy, stub, seen = slack_precondition_stub
    status, headers, data = _slack_call(
        proxy, stub, "chat.postMessage", {"channel": "C0OK", "text": "hi"}
    )
    assert status == 200
    assert json.loads(data)["ok"] is True
    assert headers[pipeline.ANSWERED_BY_HEADER] == "fake-L1"
    (write,) = _writes(seen)
    assert write.precondition == "passed"
    assert _SlackPreconditionStub.seen == [("POST", "/api/conversations.info")]
