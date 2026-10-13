"""Serve mode (#77): `irimi shadow --serve`, a sidecar with no child process.

The engine half first: a listener bound wider than loopback is this machine at every address it
was reached at, so a request naming that address, or a name that resolves to it, is never
forwarded to irimi itself; and the reverse door stays loopback-only whatever the bind. Then the
store's end-of-run callback, and then the CLI, driven as a subprocess where a signal is the point.
"""

import asyncio
import dataclasses
import socket

import pytest

from irimi import ca, paths, servicemap
from irimi.engine import EngineConfig
from irimi.engine.mitm import IrimiAddon, MitmEngine
from irimi.overlay import NoOverlay
from irimi.policy import ShadowPolicy
from irimi.store import NullStore


def _config(tmp_path, monkeypatch, listen_host="127.0.0.1"):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    p = ca.ca_paths()
    ca.generate_ca(p)
    return EngineConfig(
        run_id="s3rv",
        ca=p,
        confdir=paths.mitm_dir(),
        listen_host=listen_host,
        listen_port=0,
        reverse_hosts=frozenset({"api.stripe.com"}),
        maps=servicemap.MapIndex(),
    )


def _own_address() -> str | None:
    """This machine's non-loopback IPv4 address: the one a connected UDP socket would send from.
    Connecting a UDP socket sends nothing; it only asks the routing table. None with no route."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            sock.connect(("192.0.2.1", 9))  # TEST-NET-1: routed like any address, never answered
        except OSError:
            return None
        address = sock.getsockname()[0]
    return None if address.startswith("127.") or address == "0.0.0.0" else address


# -- the reverse door stays loopback-only ---------------------------------------------------------


def test_the_reverse_door_refuses_a_peer_that_is_not_loopback(tmp_path, monkeypatch):
    """A wider bind lets other machines reach the listener, but never through the reverse door,
    which relays plain HTTP with the agent's credentials to the real service: the peer check in
    `IrimiAddon._through_reverse_door` is unchanged (#77). Driven through the addon, because a
    connection from another machine cannot be staged here."""
    from mitmproxy.test import tflow, tutils

    seen = []
    addon = IrimiAddon(
        _config(tmp_path, monkeypatch, listen_host="0.0.0.0"),
        ShadowPolicy(),
        NullStore(),
        NoOverlay(),
        seen.append,
        lambda port, error: None,
    )
    flow = tflow.tflow(
        req=tutils.treq(method=b"GET", host="10.0.0.5", port=4000, path=b"/api.stripe.com/v1/x")
    )
    flow.client_conn.sockname = ("10.0.0.5", 4000)
    flow.client_conn.peername = ("10.0.0.9", 51515)
    asyncio.run(addon.request(flow))

    assert flow.response.status_code == 403
    assert b"loopback only, refusing '10.0.0.9'" in flow.response.content
    assert (flow.request.host, flow.request.path) == ("10.0.0.5", "/api.stripe.com/v1/x")
    assert seen == []


# -- the self-loop guard, widened ------------------------------------------------------------------


def _counting_requests(monkeypatch) -> list[tuple[str, int]]:
    """Every request the addon's hook is called with, as the (address, port) its connection
    arrived on. A request irimi forwarded to itself arrives a second time."""
    calls: list[tuple[str, int]] = []
    hook = IrimiAddon.request

    async def counted(self, flow):
        calls.append(tuple(flow.client_conn.sockname[:2]))
        await hook(self, flow)

    monkeypatch.setattr(IrimiAddon, "request", counted)
    return calls


def _absolute_get(address: str, port: int, url: str) -> tuple[int, bytes]:
    """A forward-proxy request for `url`, sent to irimi at `address:port`."""
    import http.client

    conn = http.client.HTTPConnection(address, port, timeout=10)
    try:
        conn.request("GET", url, headers={"host": url.split("/")[2]})
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


@pytest.mark.parametrize("named", [False, True], ids=["address", "name"])
def test_a_request_naming_the_address_irimi_was_reached_at_is_never_forwarded_to_it(
    tmp_path, monkeypatch, named
):
    """Bound to 0.0.0.0, irimi is this machine at its non-loopback address too, which is not a
    loopback literal and does not resolve to one. A forward-proxy request for that address on
    irimi's port - or for a name that resolves to it, as `irimi` does in compose - is irimi's own
    listener: answered by irimi, here the reverse door's refusal of a peer that is not loopback,
    and never forwarded, which would open a connection to the listener and arrive a second time
    (#4's loop, #77). The name is resolved by a `getaddrinfo` patch, in the process that runs
    irimi."""
    import os

    address = _own_address()
    if address is None:
        # The one proof of the done-when's non-loopback case: CI must run it, never skip it.
        assert not os.environ.get("CI"), "a CI runner with no route has no address to test"
        pytest.skip("this machine has no route, so no non-loopback address")
    if named:
        real = socket.getaddrinfo

        def resolving(host, *args, **kwargs):
            return real(address if host == "irimi.test" else host, *args, **kwargs)

        monkeypatch.setattr(socket, "getaddrinfo", resolving)
    calls = _counting_requests(monkeypatch)
    cfg = _config(tmp_path, monkeypatch, listen_host="0.0.0.0")
    engine = MitmEngine(cfg, ShadowPolicy(), NullStore(), NoOverlay())
    from irimi import runner

    handle = runner.start_engine(engine)
    try:
        port = handle.port()
        host = "irimi.test" if named else address
        status, body = _absolute_get(address, port, f"http://{host}:{port}/api.stripe.com/v1/x")
    finally:
        handle.stop()
    assert status == 403
    assert f"loopback only, refusing '{address}'".encode() in body
    assert calls == [(address, port)]


def test_a_door_that_cannot_be_told_is_refused_never_forwarded(tmp_path, monkeypatch):
    """Telling the doors apart now parses the address a connection arrived on and may resolve a
    name (#77). If that raises, the hook would forward the flow, and the request it was asking
    about may be one addressed to irimi itself: it is answered 400 by irimi instead."""
    from mitmproxy.test import tflow, tutils

    from irimi import reverse_door

    def broken(*args):
        raise ValueError("no address")

    monkeypatch.setattr(reverse_door, "detect_door", broken)
    seen = []
    addon = IrimiAddon(
        _config(tmp_path, monkeypatch, listen_host="0.0.0.0"),
        ShadowPolicy(),
        NullStore(),
        NoOverlay(),
        seen.append,
        lambda port, error: None,
    )
    flow = tflow.tflow(req=tutils.treq(method=b"POST", host="irimi", port=4000, path=b"/x"))
    flow.client_conn.sockname = ("10.0.0.5", 4000)
    asyncio.run(addon.request(flow))
    assert flow.response.status_code == 400
    assert flow.response.content == b"irimi: could not route request\n"
    assert seen == []


def test_a_failed_decision_under_serve_mode_is_unattributed(tmp_path, monkeypatch):
    """Serve mode's engine run is `trace.UNATTRIBUTED` (#77), and the failed-decision path reads
    the same fallback as every other (#67's note on #77): irimi's own 502, filed under no run, not
    under a process run that does not exist."""
    from mitmproxy.test import tflow, tutils

    from irimi.trace import UNATTRIBUTED

    class _Broken(ShadowPolicy):
        def answer(self, *args, **kwargs):
            raise RuntimeError("the decision failed")

    class _Kept(NullStore):
        def __init__(self):
            self.recorded = []

        def record(self, exchange):
            self.recorded.append(exchange)

    store = _Kept()
    cfg = dataclasses.replace(_config(tmp_path, monkeypatch), run_id=UNATTRIBUTED, serve=True)
    addon = IrimiAddon(cfg, _Broken(), store, NoOverlay(), None, lambda port, error: None)
    flow = tflow.tflow(
        req=tutils.treq(method=b"POST", host="api.example.com", port=443, path=b"/things")
    )
    flow.client_conn.sockname = ("127.0.0.1", 4000)
    asyncio.run(addon.request(flow))
    assert flow.response.status_code == 502
    [ex] = store.recorded
    assert ex.run_id == UNATTRIBUTED
    assert "decision-failed" in ex.flags


def test_a_loopback_bind_still_finds_its_own_loopback_spellings(tmp_path, monkeypatch):
    """The default bind is unchanged: `127.1` on irimi's port is irimi, through the forward door
    as through the reverse one, and goes no further than the reverse door's allow-list."""
    calls = _counting_requests(monkeypatch)
    cfg = dataclasses.replace(_config(tmp_path, monkeypatch), reverse_hosts=frozenset())
    engine = MitmEngine(cfg, ShadowPolicy(), NullStore(), NoOverlay())
    from irimi import runner

    handle = runner.start_engine(engine)
    try:
        port = handle.port()
        status, body = _absolute_get("127.0.0.1", port, f"http://127.1:{port}/nope.example/x")
    finally:
        handle.stop()
    assert status == 403
    assert b"not in a loaded map" in body
    assert calls == [("127.0.0.1", port)]
