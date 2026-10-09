"""`Irimi-Run` on every request a run makes through irimi (#75), against a real engine.

Each test runs the engine over a map that claims the loopback upstream as a read, with a real
`DirectoryStore`, and this process set up as `irimi shadow` sets up its child. A request is
labelled when the exchange irimi recorded for it carries the trigger's run id; the upstream must
never see the header, which irimi strips (#67). A request that does not go through irimi is never
labelled at all, since nothing would strip it.
"""

import asyncio
import contextlib
import dataclasses
import http.client
import http.server
import socket
import ssl
import threading
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import requests

from irimi import paths, redact, sdk
from irimi.exchange import RUN_HEADER, Exchange
from irimi.sdk import client, instrumentation, runs
from irimi.store import DirectoryStore, StoreReader
from tests import test_engine_mitm
from tests.test_engine_mitm import (
    _config,
    _leaf_cert_for_loopback,
    _maps,
    _serve_tls,
    _start,
    _Upstream,
)

upstream = test_engine_mitm.upstream  # bound here so pytest finds the fixture

ENGINE_RUN = "t3st"  # the engine's own run, `_config`'s run id: the process run's place
CLIENTS = ("urllib", "http.client", "requests", "httpx", "httpx-async")


@dataclasses.dataclass
class _Irimi:
    port: int
    seen: list[Exchange]
    store: DirectoryStore
    stop: Callable[[], None]

    @property
    def proxy(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def finish(self) -> StoreReader:
        self.stop()
        return StoreReader(self.store.layout.root)


@contextlib.contextmanager
def _running(cfg: Any, tmp_path: Path, monkeypatch, **start: Any) -> Iterator[_Irimi]:
    """`cfg` served by a real engine with a real store, and this process set up as `irimi shadow`
    sets up its child: active, and reporting to the engine's control endpoint."""
    store = DirectoryStore(tmp_path / "store", redact.load_key(tmp_path))
    eng, seen, stop = _start(cfg, store=store, **start)
    stopped = False

    def stop_once() -> None:
        nonlocal stopped
        if not stopped:
            stopped = True
            stop()

    port = eng.listen_port()
    for name in instrumentation.LISTENER_ENVS:  # a developer's own proxy is not irimi
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
    monkeypatch.setattr(runs, "reporter", client.ControlClient())
    try:
        yield _Irimi(port, seen, store, stop_once)
    finally:
        stop_once()


@pytest.fixture
def irimi(tmp_path, monkeypatch, upstream) -> Iterator[_Irimi]:
    """A running engine whose map claims the upstream, and the SDK set up to report to it. No
    proxy variable is set: each client below is given the proxy explicitly, and ignores the
    environment's, so a developer's own `NO_PROXY` cannot route around irimi."""
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch))
    with _running(cfg, tmp_path, monkeypatch) as running:
        yield running


@pytest.fixture
def tls(tmp_path, monkeypatch) -> Iterator[tuple[_Irimi, int, Path]]:
    """`(irimi, upstream port, CA cert)`: the upstream behind TLS, with a leaf the irimi CA
    signed, which the engine trusts; its reverse door open to 127.0.0.1, whose upstream is always
    https; and the CA the agent trusts for irimi's own leaves."""
    cfg = _config(
        tmp_path,
        monkeypatch,
        reverse_hosts=frozenset({"127.0.0.1"}),
        maps=_maps(tmp_path, monkeypatch),
    )
    _Upstream.seen = []
    srv = _serve_tls(_leaf_cert_for_loopback(cfg.ca, tmp_path / "leaf.pem"))
    try:
        with _running(cfg, tmp_path, monkeypatch, trust_upstream_ca=cfg.ca.cert) as running:
            yield running, srv.server_address[1], cfg.ca.cert
    finally:
        srv.shutdown()


def _get(name: str, proxy: str, url: str, **kw: Any) -> int:
    """One GET of `url` through `proxy` with the client `name`, sync. Its status."""
    if name == "urllib":
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({"http": proxy}))
        with opener.open(url, timeout=10) as resp:
            resp.read()
            return int(resp.status)
    if name == "http.client":
        host, port = proxy.removeprefix("http://").split(":")
        conn = http.client.HTTPConnection(host, int(port), timeout=10)
        try:
            conn.request("GET", url, headers=kw.get("headers", {}))
            resp = conn.getresponse()
            resp.read()
            return resp.status
        finally:
            conn.close()
    if name == "requests":
        session = requests.Session()
        session.trust_env = False
        return session.get(url, proxies={"http": proxy}, timeout=10).status_code
    if name == "httpx":
        with httpx.Client(proxy=proxy, trust_env=False) as c:
            return c.get(url, headers=kw.get("headers", {})).status_code
    raise ValueError(name)


async def _get_async(proxy: str, url: str) -> int:
    async with httpx.AsyncClient(proxy=proxy, trust_env=False) as c:
        return (await c.get(url)).status_code


@sdk.trigger
def fetch(name: str, proxy: str, url: str) -> str | None:
    assert _get(name, proxy, url) == 200
    return sdk.current_run_id()


@sdk.trigger
async def fetch_async(proxy: str, url: str) -> str | None:
    assert await _get_async(proxy, url) == 200
    return sdk.current_run_id()


def _in_a_run(name: str, proxy: str, url: str) -> str | None:
    if name == "httpx-async":
        return asyncio.run(fetch_async(proxy, url))
    return fetch(name, proxy, url)


def _outside_any_run(name: str, proxy: str, url: str) -> None:
    if name == "httpx-async":
        assert asyncio.run(_get_async(proxy, url)) == 200
    else:
        assert _get(name, proxy, url) == 200


def _upstream_saw_the_header() -> bool:
    return any(RUN_HEADER in {k.lower() for k, _ in headers} for _, headers in _Upstream.seen)


@pytest.mark.parametrize("name", CLIENTS)
def test_a_request_in_a_run_is_attributed_to_it_and_the_upstream_never_sees_the_header(
    irimi, upstream, name
):
    url = f"http://127.0.0.1:{upstream}/hello"
    run_id = _in_a_run(name, irimi.proxy, url)
    assert run_id is not None
    [ex] = irimi.seen
    assert (ex.request.path, ex.run_id) == ("/hello", run_id)
    assert [path for path, _ in _Upstream.seen] == ["/hello"]
    assert not _upstream_saw_the_header()
    # Stored in that run, which the SDK started and ended around it (#74).
    reader = irimi.finish()
    [read] = reader.load_run(run_id).events
    assert isinstance(read, Exchange) and read.request.path == "/hello"


@pytest.mark.parametrize("name", CLIENTS)
def test_the_same_request_outside_any_run_is_the_engines_own(irimi, upstream, name):
    """No run is current, so nothing is added: the exchange falls back to the engine's own run,
    the process run under `irimi shadow -- <cmd>`."""
    sdk.instrument()  # as a run earlier in the process would have
    _outside_any_run(name, irimi.proxy, f"http://127.0.0.1:{upstream}/hello")
    [ex] = irimi.seen
    assert ex.run_id == ENGINE_RUN
    assert not _upstream_saw_the_header()


def test_a_client_made_before_the_first_run_is_covered(irimi, upstream):
    """The patches are on classes, made on the first run's entry: a session built at startup,
    before any run and before `instrument()`, still labels what a run sends through it."""
    assert not hasattr(http.client.HTTPConnection.putrequest, "__wrapped__")
    session = requests.Session()
    session.trust_env = False

    @sdk.trigger
    def first(url: str) -> str | None:
        assert session.get(url, proxies={"http": irimi.proxy}, timeout=10).status_code == 200
        return sdk.current_run_id()

    run_id = first(f"http://127.0.0.1:{upstream}/hello")
    assert [ex.run_id for ex in irimi.seen] == [run_id]


def test_two_threads_sharing_one_session_each_keep_their_own_run(irimi, upstream):
    """One `requests.Session`, one connection pool, two runs at once: a connection opened by one
    run's request carries the other's next. Attribution is per request, so neither run gets a
    request of the other's, and each holds exactly its own 20."""
    url = f"http://127.0.0.1:{upstream}/hello"
    session = requests.Session()
    session.trust_env = False
    barrier = threading.Barrier(2)
    ids: dict[int, str | None] = {}

    @sdk.trigger
    def worker(n: int) -> None:
        ids[n] = sdk.current_run_id()
        for _ in range(20):
            barrier.wait(timeout=10)
            assert session.get(url, proxies={"http": irimi.proxy}, timeout=10).status_code == 200

    threads = [threading.Thread(target=worker, args=(n,)) for n in (0, 1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert None not in ids.values() and len(set(ids.values())) == 2
    assert sorted(ex.run_id for ex in irimi.seen) == sorted([ids[0]] * 20 + [ids[1]] * 20)
    assert not _upstream_saw_the_header()
    reader = irimi.finish()
    for run_id in ids.values():
        assert run_id is not None
        assert len(reader.load_run(run_id).events) == 20


# -- only through irimi -------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["urllib", "httpx"])
def test_a_request_that_bypasses_irimi_is_never_labelled(irimi, upstream, name):
    """A client with no proxy goes straight to the upstream. Nothing would strip a run's id on
    that path, so the SDK never puts one there: the upstream sees no header, and irimi sees no
    exchange (#75)."""
    url = f"http://127.0.0.1:{upstream}/hello"

    @sdk.trigger
    def direct() -> str | None:
        if name == "urllib":
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(url, timeout=10) as resp:
                resp.read()
        else:
            with httpx.Client(trust_env=False) as c:
                c.get(url)
        return sdk.current_run_id()

    assert direct() is not None
    assert [path for path, _ in _Upstream.seen] == ["/hello"]
    assert not _upstream_saw_the_header()
    assert irimi.seen == []


def test_the_proxy_variables_name_irimi_when_the_control_endpoint_is_unset(
    irimi, upstream, monkeypatch
):
    """A launcher that passes on only the variables it knows can drop `IRIMI_CONTROL` (W10). irimi
    then never hears of the run, but the proxy variables still name its listener, so the run's
    requests are labelled as before and land together (#74, #75)."""
    monkeypatch.delenv(paths.CONTROL_ENV)
    monkeypatch.setenv("HTTP_PROXY", irimi.proxy)
    run_id = fetch("urllib", irimi.proxy, f"http://127.0.0.1:{upstream}/hello")
    assert [ex.run_id for ex in irimi.seen] == [run_id]


def test_a_request_through_the_reverse_door_is_labelled(tls):
    """The reverse door is irimi's own listener, addressed as an origin: a connection to irimi's
    host and port is labelled whatever its request line says (#75)."""
    running, up, _ = tls

    @sdk.trigger
    def door() -> str | None:
        conn = http.client.HTTPConnection("127.0.0.1", running.port, timeout=10)
        try:
            conn.request("GET", f"/127.0.0.1:{up}/hello")
            assert conn.getresponse().read() == b"hello from upstream"
        finally:
            conn.close()
        return sdk.current_run_id()

    run_id = door()
    assert [(ex.door, ex.run_id) for ex in running.seen] == [("reverse", run_id)]
    assert not _upstream_saw_the_header()


def test_a_connect_tunnel_through_irimi_is_labelled_inside_the_tunnel(tls):
    """HTTPS through the proxy: `http.client` opens a CONNECT tunnel on the proxy connection and
    runs `putrequest` on it, so the request inside TLS carries the header, irimi reads it there,
    and the TLS upstream never sees it."""
    running, up, ca_cert = tls
    context = ssl.create_default_context(cafile=str(ca_cert))

    @sdk.trigger
    def tunnelled() -> str | None:
        conn = http.client.HTTPSConnection("127.0.0.1", running.port, timeout=10, context=context)
        conn.set_tunnel("127.0.0.1", up)
        try:
            conn.request("GET", "/hello")
            assert conn.getresponse().read() == b"hello from upstream"
        finally:
            conn.close()
        return sdk.current_run_id()

    run_id = tunnelled()
    assert [ex.run_id for ex in running.seen] == [run_id]
    assert [path for path, _ in _Upstream.seen] == ["/hello"]
    assert not _upstream_saw_the_header()


@pytest.mark.parametrize("method", ["GET", "M-SEARCH"])
def test_a_plain_tunnel_through_irimi_is_labelled_and_never_relayed_raw(irimi, upstream, method):
    """Plain HTTP in a CONNECT tunnel is labelled like any request on irimi's connection, and
    irimi reads it whatever its method: one mitmproxy would not call HTTP (`M-SEARCH`) would
    otherwise be relayed as raw bytes, its `Irimi-Run` and all, straight to the upstream (#75)."""

    @sdk.trigger
    def tunnelled() -> str | None:
        conn = http.client.HTTPConnection("127.0.0.1", irimi.port, timeout=10)
        conn.set_tunnel("127.0.0.1", upstream)
        try:
            conn.request(method, "/hello")
            conn.getresponse().read()
        finally:
            conn.close()
        return sdk.current_run_id()

    run_id = tunnelled()
    assert [(ex.request.method, ex.run_id) for ex in irimi.seen] == [(method, run_id)]
    assert not _upstream_saw_the_header()


@pytest.mark.parametrize("name", ["httpx", "httpx-async"])
def test_httpx_takes_the_header_off_once_sent(irimi, upstream, name):
    """httpx builds a redirect from the request it sent, headers and all, and the next hop may
    not go through irimi. So the label is off the request again once its transport has sent it:
    each hop is decided by the transport that sends it."""
    url = f"http://127.0.0.1:{upstream}/hello"

    @sdk.trigger
    def sent() -> httpx.Request:
        with httpx.Client(proxy=irimi.proxy, trust_env=False) as c:
            return c.get(url).request

    @sdk.trigger
    async def sent_async() -> httpx.Request:
        async with httpx.AsyncClient(proxy=irimi.proxy, trust_env=False) as c:
            return (await c.get(url)).request

    request = sent() if name == "httpx" else asyncio.run(sent_async())
    assert RUN_HEADER not in request.headers
    assert len(irimi.seen) == 1 and irimi.seen[0].run_id != ENGINE_RUN


def test_an_httpx_request_that_names_its_own_run_keeps_it(irimi, upstream):
    @sdk.trigger
    def own() -> None:
        _get(
            "httpx",
            irimi.proxy,
            f"http://127.0.0.1:{upstream}/hello",
            headers={"Irimi-Run": "agents-own"},
        )

    own()
    assert [ex.run_id for ex in irimi.seen] == ["agents-own"]


# -- instrument() itself ------------------------------------------------------------------------


def _patched_methods() -> list[Any]:
    return [
        http.client.HTTPConnection.putrequest,
        httpx.HTTPTransport.handle_request,
        httpx.AsyncHTTPTransport.handle_async_request,
    ]


def test_instrument_patches_once_however_often_and_from_however_many_threads(monkeypatch):
    """Eight threads all get past the unlocked check before any of them patches: `active()` is
    where each waits for the others. Then they queue on the lock, and only the first patches."""
    originals = _patched_methods()
    assert not any(hasattr(m, "__wrapped__") for m in originals)
    barrier = threading.Barrier(8)

    def active_once_all_are_here() -> bool:
        barrier.wait(timeout=10)
        return True

    monkeypatch.setattr(instrumentation, "active", active_once_all_are_here)

    def race() -> None:
        sdk.instrument()

    threads = [threading.Thread(target=race) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    monkeypatch.setattr(instrumentation, "active", lambda: True)
    sdk.instrument()
    # Each method wrapped exactly once: its wrapper wraps the original, which wraps nothing.
    assert [m.__wrapped__ for m in _patched_methods()] == originals
    assert len(instrumentation._patched) == 3


def test_an_inactive_sdk_patches_nothing(monkeypatch):
    monkeypatch.delenv(paths.ENGINE_ACTIVE_ENV, raising=False)
    originals = _patched_methods()
    sdk.instrument()
    assert _patched_methods() == originals
    assert not hasattr(http.client.HTTPConnection.putrequest, "__wrapped__")
    assert instrumentation._patched == []


class _ControlStandIn(http.server.BaseHTTPRequestHandler):
    heard: list[tuple[str, list[str]]] = []

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("content-length", 0)))
        _ControlStandIn.heard.append((self.path, [k.lower() for k in self.headers]))
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args: Any) -> None:
        pass


def test_the_sdks_own_control_posts_carry_no_run_header(monkeypatch):
    """The control endpoint is on irimi's own listener, which `instrument()` labels, and the
    route already names the run (#73). So every start and end the SDK posts, the ones after the
    patches are in place included, goes out unlabelled."""
    _ControlStandIn.heard = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _ControlStandIn)
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}).start()
    try:
        monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
        monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{server.server_address[1]}/_irimi")
        monkeypatch.setattr(runs, "reporter", client.ControlClient())

        @sdk.trigger
        def twice() -> None:
            pass

        twice()
        twice()
    finally:
        server.shutdown()
        server.server_close()
    assert [path.rsplit("/", 1)[1] for path, _ in _ControlStandIn.heard] == [
        "start",
        "end",
        "start",
        "end",
    ]
    assert hasattr(http.client.HTTPConnection.putrequest, "__wrapped__")
    assert all(RUN_HEADER not in names for _, names in _ControlStandIn.heard)


# -- edges: racing first calls, proxy spellings, HTTPS, an own header, redirects ----------------


def test_instrument_returns_only_once_every_patch_is_in_place(monkeypatch):
    """A run that starts while another run's `instrument()` is still patching must not go on with
    the patches half made: its own call waits for the first, rather than returning as soon as the
    first has begun (#75). Here the first call is held between its first patch and the rest, as
    importing httpx would hold it."""
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    holding, release = threading.Event(), threading.Event()
    patch = instrumentation._patch

    def held_patch(owner: type, name: str, wrap: Callable[[Any], Any]) -> None:
        if owner is httpx.HTTPTransport:
            holding.set()
            release.wait(timeout=10)
        patch(owner, name, wrap)

    monkeypatch.setattr(instrumentation, "_patch", held_patch)
    seen_by_second: list[bool] = []

    def second() -> None:
        sdk.instrument()
        seen_by_second.append(all(hasattr(m, "__wrapped__") for m in _patched_methods()))

    first_call = threading.Thread(target=sdk.instrument)
    first_call.start()
    assert holding.wait(timeout=10)
    second_call = threading.Thread(target=second)
    second_call.start()
    second_call.join(timeout=0.2)
    release.set()
    first_call.join(timeout=10)
    second_call.join(timeout=10)
    assert seen_by_second == [True]


def test_instrument_called_while_it_patches_returns_rather_than_waiting_on_itself(monkeypatch):
    """Patching imports httpx, and an import hook may start a run on the same thread, which calls
    `instrument()` again while the first call holds its lock. That call returns at once: it neither
    deadlocks the agent nor patches a second time (#75)."""
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    patch = instrumentation._patch

    def reentering_patch(owner: type, name: str, wrap: Callable[[Any], Any]) -> None:
        if owner is httpx.HTTPTransport:
            sdk.instrument()  # as an import hook that starts a run would
        patch(owner, name, wrap)

    monkeypatch.setattr(instrumentation, "_patch", reentering_patch)
    first_call = threading.Thread(target=sdk.instrument, daemon=True)
    first_call.start()
    first_call.join(timeout=10)
    deadlocked = first_call.is_alive()
    if deadlocked:  # free it, so the suite's own `_uninstall` can take the lock
        instrumentation._lock.release()
        first_call.join(timeout=10)
    assert not deadlocked
    for method in _patched_methods():
        assert hasattr(method, "__wrapped__")
        assert not hasattr(method.__wrapped__, "__wrapped__")


def test_a_proxy_variable_spelled_without_a_scheme_names_irimi(irimi, upstream, monkeypatch):
    """urllib, requests and httpx all read `HTTP_PROXY=127.0.0.1:4000` as an http proxy, so a
    deployment that sets it by hand that way still sends through irimi, and is labelled (#75)."""
    monkeypatch.delenv(paths.CONTROL_ENV)
    monkeypatch.setenv("HTTP_PROXY", irimi.proxy.removeprefix("http://"))
    run_id = fetch("requests", irimi.proxy, f"http://127.0.0.1:{upstream}/hello")
    assert [ex.run_id for ex in irimi.seen] == [run_id]


def _get_https(name: str, proxy: str, url: str, ca_cert: Path) -> bytes:
    """One GET of an https `url` through `proxy` with the client `name`: a CONNECT tunnel through
    irimi, with TLS inside it to irimi's leaf, which `ca_cert` signed. Its body."""
    context = ssl.create_default_context(cafile=str(ca_cert))
    if name == "urllib":
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({"https": proxy}),
            urllib.request.HTTPSHandler(context=context),
        )
        with opener.open(url, timeout=10) as resp:
            return bytes(resp.read())
    if name == "requests":
        session = requests.Session()
        session.trust_env = False
        return session.get(url, proxies={"https": proxy}, verify=str(ca_cert), timeout=10).content
    if name == "httpx":
        with httpx.Client(proxy=proxy, verify=context, trust_env=False) as c:
            return c.get(url).content
    if name == "httpx-async":

        async def get() -> bytes:
            async with httpx.AsyncClient(proxy=proxy, verify=context, trust_env=False) as c:
                return (await c.get(url)).content

        return asyncio.run(get())
    raise ValueError(name)


@pytest.mark.parametrize("name", ["urllib", "requests", "httpx", "httpx-async"])
def test_https_through_irimi_is_labelled_inside_the_tunnel(tls, name):
    """Every covered client sends HTTPS through the proxy as a CONNECT tunnel, and each decides by
    the connection it tunnels through, which is irimi's: `requests`' urllib3 connects to the proxy
    and tunnels from there, and httpx's transport has the proxy's address (#75)."""
    running, up, ca_cert = tls

    @sdk.trigger
    def tunnelled() -> str | None:
        assert _get_https(name, running.proxy, f"https://127.0.0.1:{up}/hello", ca_cert) == (
            b"hello from upstream"
        )
        return sdk.current_run_id()

    run_id = tunnelled()
    assert [ex.run_id for ex in running.seen] == [run_id]
    assert [path for path, _ in _Upstream.seen] == ["/hello"]
    assert not _upstream_saw_the_header()


def test_an_http_client_request_that_names_its_own_run_is_the_sdks_first(irimi, upstream):
    """`http.client` gets the SDK's header right after the request line, ahead of any the agent
    set, and irimi takes the first: the run is the trigger's, as the README says (#75)."""

    @sdk.trigger
    def own() -> str | None:
        status = _get(
            "http.client",
            irimi.proxy,
            f"http://127.0.0.1:{upstream}/hello",
            headers={"Irimi-Run": "agents-own"},
        )
        assert status == 200
        return sdk.current_run_id()

    run_id = own()
    assert [ex.run_id for ex in irimi.seen] == [run_id]
    assert not _upstream_saw_the_header()


REDIRECT_MAP = """
version: 1
service: demo
hosts:
  - 127.0.0.1
routes:
  - match:
      method: GET
      path: /redirect
    operation: things.moved
    kind: read
    human: a thing that moved
"""


class _Redirecting(http.server.BaseHTTPRequestHandler):
    """`/redirect` answers 302 to `to`; anything else is a landing, whose headers it records."""

    to = ""
    landed: list[list[str]] = []

    def do_GET(self) -> None:
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("location", _Redirecting.to)
            self.send_header("content-length", "0")
        else:
            _Redirecting.landed.append([k.lower() for k in self.headers])
            self.send_response(200)
            self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, *args: Any) -> None:
        pass


@pytest.mark.parametrize("name", ["urllib", "requests", "httpx", "httpx-async"])
def test_a_redirect_away_from_irimi_is_not_labelled(tmp_path, monkeypatch, name):
    """A read through irimi answers 302 to a host the client reaches directly. The first hop went
    through irimi and was labelled; the second does not, and is not: each hop is decided by the
    connection that sends it, so the run's id never follows a redirect out of irimi (#75)."""
    _Redirecting.landed = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Redirecting)
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}).start()
    port = server.server_address[1]
    # `localhost` is the same server under a name the clients below do not proxy.
    _Redirecting.to = f"http://localhost:{port}/landed"
    url = f"http://127.0.0.1:{port}/redirect"
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch, doc=REDIRECT_MAP))
    try:
        with _running(cfg, tmp_path, monkeypatch) as running:
            proxy = running.proxy

            @sdk.trigger
            def follow() -> str | None:
                if name == "urllib":
                    monkeypatch.setenv("no_proxy", "localhost")
                    opener = urllib.request.build_opener(
                        urllib.request.ProxyHandler({"http": proxy})
                    )
                    with opener.open(url, timeout=10) as resp:
                        resp.read()
                elif name == "requests":
                    session = requests.Session()
                    session.trust_env = False
                    session.get(url, proxies={"http://127.0.0.1": proxy}, timeout=10)
                elif name == "httpx":
                    mounts = {"http://127.0.0.1": httpx.HTTPTransport(proxy=proxy)}
                    with httpx.Client(mounts=mounts, trust_env=False) as c:
                        c.get(url, follow_redirects=True)
                else:

                    async def get() -> None:
                        mounts = {"http://127.0.0.1": httpx.AsyncHTTPTransport(proxy=proxy)}
                        async with httpx.AsyncClient(mounts=mounts, trust_env=False) as c:
                            await c.get(url, follow_redirects=True)

                    asyncio.run(get())
                return sdk.current_run_id()

            run_id = follow()
            assert [(ex.request.path, ex.run_id) for ex in running.seen] == [("/redirect", run_id)]
    finally:
        server.shutdown()
        server.server_close()
    assert len(_Redirecting.landed) == 1
    assert RUN_HEADER.lower() not in _Redirecting.landed[0]


@pytest.mark.parametrize("name", ["httpx", "httpx-async"])
def test_httpx_takes_the_header_off_when_sending_fails_and_the_failure_is_the_agents(
    irimi, monkeypatch, name
):
    """A send to irimi's listener that fails raises to the agent exactly what httpx raised, and
    the request it may retry or reuse is left with no header (#75). The proxy here is a port
    nothing listens on, which the proxy variable names as irimi's."""
    dead = socket.socket()
    dead.bind(("127.0.0.1", 0))
    proxy = f"http://127.0.0.1:{dead.getsockname()[1]}"
    dead.close()
    monkeypatch.setenv("HTTP_PROXY", proxy)
    request = httpx.Request("GET", "http://127.0.0.1:9/hello")

    @sdk.trigger
    def send() -> None:
        with httpx.Client(proxy=proxy, trust_env=False) as c:
            c.send(request)

    @sdk.trigger
    async def send_async() -> None:
        async with httpx.AsyncClient(proxy=proxy, trust_env=False) as c:
            await c.send(request)

    with pytest.raises(httpx.ConnectError):
        send() if name == "httpx" else asyncio.run(send_async())
    assert RUN_HEADER not in request.headers
