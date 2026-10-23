"""Running irimi for a test, and talking to it: a real `MitmEngine` on a loopback port, `irimi`
as a process of its own, and this process set up as an agent the SDK reports from."""

import asyncio
import contextlib
import dataclasses
import datetime as dt
import http.client
import io
import ipaddress
import json
import socket
import ssl
import struct
import sys
import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from irimi import servicemaps
from irimi.config import ca, paths
from irimi.core.exchange import Exchange
from irimi.decide.overlay import NoOverlay
from irimi.decide.policy import ShadowPolicy
from irimi.engine import EngineConfig, Serve
from irimi.engine.capture import recorded_stream
from irimi.engine.control import ControlEndpoint
from irimi.engine.mitm import MitmEngine
from irimi.protocol.env import AGENT_VERSION_ENV, CONTROL_ENV, ENGINE_ACTIVE_ENV
from irimi.protocol.headers import ANSWERED_BY_HEADER, RUN_HEADER
from irimi.protocol.trace import RunRecord
from irimi.record import redact
from irimi.record.store.reader import StoreReader
from irimi.record.store.writer import DirectoryStore
from irimi.report import model, summary
from irimi.report.model import RunView
from irimi.sdk import _control_client as client
from irimi.sdk import _http_labels as instrumentation
from irimi.sdk._state import state
from tests.support.doubles import NullStore
from tests.support.maps import targeted_index
from tests.support.waiting import eventually


def engine_config(tmp_path, monkeypatch, port=0, reverse_hosts=frozenset(), maps=None):
    """A shadow-mode config for run `t3st` on loopback, with a fresh CA under `tmp_path`."""
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
        maps=maps if maps is not None else servicemaps.MapIndex(),
    )


def live_summary(seen, maps):
    """The summary `irimi shadow` prints for a run whose engine reported `seen`."""
    view = RunView(RunRecord.header("t3st"), [*map(model.for_summary, seen)])
    return summary.summary_lines(view, maps, 1.0, summary.Live())


def start_engine(
    cfg,
    overlay=None,
    trust_upstream_ca=None,
    policy=None,
    store=None,
    on_tool_call=None,
    on_tls_failure=None,
):
    """Serve `cfg` on a background loop until the returned stop() is called, which closes
    `store` once the engine has stopped."""
    seen: list[Exchange] = []
    policy = policy or ShadowPolicy()
    store = store or NullStore()
    control = ControlEndpoint(
        store,
        serve=isinstance(cfg.mode, Serve),
        process_run=cfg.run_id,
        on_tool_call=on_tool_call,
        policy=policy,
    )
    eng = MitmEngine(
        cfg,
        policy=policy,
        store=store,
        overlay=overlay or NoOverlay(),
        on_exchange=seen.append,
        control=control,
        on_tls_failure=on_tls_failure,
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
            assert eng._master is not None
            eng._master.options.update(ssl_verify_upstream_trusted_ca=str(trust_upstream_ca))

        asyncio.run_coroutine_threadsafe(trust(), loop).result(timeout=15)

    def stop():
        eng.shutdown()
        t.join(timeout=15)
        loop.close()
        store.close()

    return eng, seen, stop


def _send(proxy_port, method, target, host, body, extra_headers):
    """One request to the listener, a JSON body when there is one: `(status, data, response)`."""
    conn = http.client.HTTPConnection("127.0.0.1", proxy_port, timeout=10)
    headers = {"host": host}
    if body is not None:
        headers["content-type"] = "application/json"
    headers.update(extra_headers or {})
    conn.request(method, target, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data, resp


def via_proxy(proxy_port, method, url, body=None, extra_headers=None):
    """One request through the forward proxy to the absolute `url`: `(status, data)`."""
    status, data, _ = _send(proxy_port, method, url, url.split("/")[2], body, extra_headers)
    return status, data


def via_proxy_stamped(proxy_port, method, url, body=None):
    """`via_proxy`, and the `Irimi-Answered-By` the answer carried, or None."""
    status, data, resp = _send(proxy_port, method, url, url.split("/")[2], body, None)
    return status, data, resp.getheader(ANSWERED_BY_HEADER)


def via_base_url(proxy_port, method, path, body=None, host_name="127.0.0.1", extra_headers=None):
    """One base-URL request: origin-form, addressed to the listener itself. `(status, data)`."""
    host = f"{host_name}:{proxy_port}"
    status, data, _ = _send(proxy_port, method, path, host, body, extra_headers)
    return status, data


def leaf_cert_for_loopback(p: ca.CAPaths, out: Path) -> Path:
    """Mint a leaf for 127.0.0.1 signed by the irimi CA; writes cert+key PEM to `out`."""
    ca_key = serialization.load_pem_private_key(p.key.read_bytes(), None)
    assert isinstance(ca_key, rsa.RSAPrivateKey)
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


def raw_tunnel(proxy_port: int, port: int) -> socket.socket:
    """A CONNECT tunnel through the proxy to 127.0.0.1:`port`, open and ready for bytes."""
    s = socket.create_connection(("127.0.0.1", proxy_port), timeout=2)
    s.sendall(f"CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n".encode())
    assert s.recv(4096).startswith(b"HTTP/1.1 200")
    return s


def via_tls_proxy(cfg, proxy_port, host, port, method, path, body=None):
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


def open_tunnel(proxy_port, host, port):
    """A CONNECT tunnel through the proxy to `host`:`port`, before any TLS."""
    s = socket.create_connection(("127.0.0.1", proxy_port), timeout=10)
    s.sendall(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n".encode())
    answer = b""
    while b"\r\n\r\n" not in answer:
        answer += s.recv(1024)
    assert answer.startswith(b"HTTP/1.1 200"), answer
    return s


def bare_addon(tmp_path, monkeypatch, policy=None, overlay=None, on_exchange=None, store=None):
    """An IrimiAddon over DEMO_MAP, driven hook by hook with no proxy around it."""
    from irimi.engine.mitm import IrimiAddon

    return IrimiAddon(
        engine_config(tmp_path, monkeypatch, maps=targeted_index(tmp_path, monkeypatch)),
        policy or ShadowPolicy(),
        store or NullStore(),
        overlay or NoOverlay(),
        on_exchange or (lambda ex: None),
        lambda port, error: None,
    )


def bare_flow(method, path):
    """A mitmproxy flow for `method path` on 127.0.0.1, arrived on a listener at port 4000."""
    from mitmproxy.test import tflow, tutils

    flow = tflow.tflow(req=tutils.treq(method=method, host="127.0.0.1", port=80, path=path))
    flow.client_conn.sockname = ("127.0.0.1", 4000)
    return flow


def read_via_proxy(proxy_port, url, extra_headers=None, method="GET", body=None):
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


SLACK_JSON = "application/json;charset=utf-8"  # what slack_sdk sends


def slack_call(proxy, stub, method, params):
    """One Slack Web API call through the proxy, as slack_sdk sends it: a JSON POST."""
    return read_via_proxy(
        proxy,
        f"http://127.0.0.1:{stub}/api/{method}",
        extra_headers={"content-type": SLACK_JSON},
        method="POST",
        body=json.dumps(params).encode(),
    )


def post_refund(proxy, stub, charge, amount=100):
    """One refund through the proxy, form-encoded as stripe-python posts it."""
    return read_via_proxy(
        proxy,
        f"http://127.0.0.1:{stub}/v1/refunds",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
        method="POST",
        body=f"charge={charge}&amount={amount}".encode(),
    )


def writes_in(seen):
    """The writes among the exchanges an engine reported."""
    return [ex for ex in seen if ex.kind == "write"]


def stream_server(chunks, gates=(), reset=False, extra_headers=(), tls_leaf=None):
    """A loopback SSE upstream that sends `chunks` with chunked transfer encoding, as OpenAI and
    Anthropic do, then ends the body, or with `reset` resets the connection instead. It waits for
    `gates[i]` before whatever follows chunk `i`, and serves TLS with `tls_leaf`, a PEM of a leaf
    and its key. The caller shuts it down."""

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            length = int(self.headers.get("content-length") or 0)
            if length:
                self.rfile.read(length)
            self.send_response(200)
            self.send_header("content-type", "text/event-stream")
            self.send_header("transfer-encoding", "chunked")
            for name, value in extra_headers:
                self.send_header(name, value)
            self.end_headers()
            for i, chunk in enumerate(chunks):
                if i and i <= len(gates):
                    gates[i - 1].wait(timeout=20)
                self.wfile.write(b"%x\r\n" % len(chunk) + chunk + b"\r\n")
                self.wfile.flush()
            if len(gates) >= len(chunks):
                gates[len(chunks) - 1].wait(timeout=20)
            if reset:
                # RST rather than FIN, and no terminating chunk: what a dropped upstream sends.
                self.connection.setsockopt(
                    socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                )
                self.connection.close()
            else:
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
            self.close_connection = True  # see `StreamUpstream.do_POST`

        def log_message(self, *args):
            pass

    srv = HTTPServer(("127.0.0.1", 0), Handler)
    if tls_leaf is not None:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(tls_leaf)
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    return srv


def open_stream(eng, srv):
    """POST to `srv`'s chat route through the proxy; the response, headers read, body not."""
    conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=5)
    authority = f"127.0.0.1:{srv.server_address[1]}"
    conn.request(
        "POST",
        f"http://{authority}/v1/chat/completions",
        body=b"{}",
        headers={"host": authority, "content-type": "application/json"},
    )
    return conn, conn.getresponse()


THREE_CHUNKS = [
    b'data: {"delta": "one"}\n\n',
    b'data: {"delta": "and two"}\n\n',
    b'data: {"delta": "three, the longest"}\n\n',
]


def streamed_flow(*headers):
    """A mitmproxy flow whose response has streamed, for `recorded_stream` alone."""
    from mitmproxy.test import tflow, tutils

    fields = ((b"content-type", b"text/event-stream"), *headers)
    return tflow.tflow(resp=tutils.tresp(content=None, headers=fields))


def recorded(flow, capture, *, whole):
    """`recorded_stream` of the response `flow` streamed."""
    headers = tuple(
        (k.decode("latin-1"), v.decode("latin-1")) for k, v in flow.response.headers.fields
    )
    return recorded_stream(flow.response.status_code, headers, capture, whole=whole)


# Sent twice, in two spellings: a repeat has to lose every instance, not the first.
RUN_HEADERS = {"Irimi-Run": "abc", "irimi-run": "abc"}


def carries_run_header(headers) -> bool:
    """Whether header pairs hold `Irimi-Run`, in any spelling."""
    return RUN_HEADER in {k.lower() for k, _ in headers}


class Served:
    """`irimi <argv>` as its own process, on a free port, with this test's `$IRIMI_HOME`. Its
    stdout is read line by line on a thread, so a test waits for a line with a deadline rather
    than blocking on a pipe, and its stderr goes to a file. `launcher` is what Python runs irimi
    as: its own module, or a `-c` bootstrap that calls `irimi.cli.main`."""

    def __init__(self, home, *argv: str, launcher: tuple[str, ...] = ("-m", "irimi")) -> None:
        import os
        import subprocess
        import sys
        import threading

        env = {**os.environ, paths.IRIMI_HOME_ENV: str(home)}
        env.pop("STRIPE_API_KEY", None)
        self.err = home / "irimi.err"
        with open(self.err, "w") as err:
            self.proc = subprocess.Popen(
                [sys.executable, *launcher, *argv, "--port", "0"],
                cwd=home,
                env=env,
                stdout=subprocess.PIPE,
                stderr=err,
                text=True,
            )
        self.lines: list[str] = []
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self) -> None:
        assert self.proc.stdout is not None
        with self.proc.stdout:
            for line in self.proc.stdout:
                self.lines.append(line.rstrip("\n"))

    def wait_for(self, condition) -> None:
        eventually(lambda: condition(self.lines), timeout=30)

    def banner(self) -> list[str]:
        self.wait_for(lambda lines: len(lines) >= 2)
        return self.lines[:2]

    def stop(self, signame: str = "SIGTERM") -> tuple[int, list[str], str]:
        """Send `signame`, and return the exit code, every line printed and stderr."""
        import signal

        self.proc.send_signal(getattr(signal, signame))
        code = self.proc.wait(timeout=60)
        self._reader.join(timeout=10)
        return code, self.lines, self.err.read_text()

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)


def port_of(banner: list[str]) -> int:
    """The port a banner says irimi is listening on."""
    import re

    match = re.search(r" · listening on 127\.0\.0\.1:(\d+) · ", banner[0])
    assert match, banner
    return int(match.group(1))


def serve_blocks(lines: list[str], reader) -> list[str]:
    """The run ids of the `--- run <id> ---` blocks in `lines`, each checked to be followed at once
    by the summary of that run as the store holds it now: printed under one lock, a
    block is never split by another line."""
    from irimi.report.model import RunView
    from irimi.report.summary import summary_lines

    maps = servicemaps.load()
    ids = []
    for at, line in enumerate(lines):
        if not line.startswith("--- run "):
            continue
        run_id = line.removeprefix("--- run ").removesuffix(" ---")
        stored = reader.load_run(run_id)
        expected = summary_lines(RunView(stored.record, stored.events), maps)
        assert lines[at + 1 : at + 1 + len(expected)] == expected, run_id
        ids.append(run_id)
    return ids


@dataclasses.dataclass
class RunningIrimi:
    """A running engine with a real store, which the SDK in this process reports to."""

    port: int
    seen: list[Exchange]
    store: DirectoryStore
    stop: Callable[[], None]

    @property
    def proxy(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def finish(self) -> StoreReader:
        """Stop the engine, which closes the store, and read the store back."""
        self.stop()
        return StoreReader(self.store.layout.root)

    def sdk_runs(self) -> list[RunRecord]:
        return [r for r in self.finish().list_runs() if r.attribution == "sdk"]


@contextlib.contextmanager
def running_irimi(cfg, tmp_path, monkeypatch, **start) -> Iterator[RunningIrimi]:
    """`cfg` served by a real engine with a real store, and this process set up as `irimi shadow`
    sets up its child: active, and reporting to the engine's control endpoint through a fresh
    client, so no warning a test expects was already given by another."""
    store = DirectoryStore(tmp_path / "store", redact.load_key(tmp_path))
    eng, seen, stop = start_engine(cfg, store=store, **start)
    stopped = False

    def stop_once() -> None:
        nonlocal stopped
        if not stopped:
            stopped = True
            stop()

    port = eng.listen_port()
    for name in instrumentation.LISTENER_ENVS:  # a developer's own proxy is not irimi
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
    monkeypatch.delenv(AGENT_VERSION_ENV, raising=False)
    monkeypatch.setattr(state, "reporter", client.ControlClient())
    try:
        yield RunningIrimi(port, seen, store, stop_once)
    finally:
        stop_once()


def run_engine(tmp_path, monkeypatch, maps, calls, policy=None, overlay=None):
    """Serve `maps`, let `calls(port)` reach the proxy, and hand back what the run produced."""
    cfg = engine_config(tmp_path, monkeypatch, maps=maps)
    eng, seen, stop = start_engine(cfg, overlay=overlay, policy=policy)
    try:
        replies = calls(eng.listen_port())
    finally:
        stop()
    return seen, replies


def python_child(source: str = "pass") -> list[str]:
    """The arguments that make `irimi shadow` run `source` in this Python, on a free port."""
    return ["--port", "0", "--", sys.executable, "-c", source]


def spy_on_the_store(monkeypatch, close=None):
    """Swap the class `compose._open_store` builds for a real `DirectoryStore` that remembers each
    `close()` and the thread it came from, and may do `close(store)` after the real one."""
    import threading

    from irimi.record.store.writer import DirectoryStore

    class _Spy(DirectoryStore):
        closed: list[str] = []

        def close(self):
            super().close()
            _Spy.closed.append(threading.current_thread().name)
            if close is not None:
                close(self)

    monkeypatch.setattr("irimi.record.store.writer.DirectoryStore", _Spy)
    return _Spy


class ReaderGoneWhen(io.StringIO):
    """stdout, whose reader goes away once `gone` is set: every later write fails as a closed
    pipe's does."""

    gone = False

    def write(self, text: str) -> int:
        if self.gone:
            raise BrokenPipeError(32, "Broken pipe")
        return super().write(text)


class RealWriteRan(AssertionError):
    """A write tool's real function ran where only its fake may."""
