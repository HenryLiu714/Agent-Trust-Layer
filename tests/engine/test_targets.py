"""Answer targets through the engine: a user's URL answers in place of the service."""

import asyncio
import http.client
import json
import socket
import threading
from http.server import HTTPServer
from typing import cast

import pytest

from irimi import servicemaps
from irimi.core import netaddr
from irimi.core.exchange import Exchange, Flag
from irimi.decide import targets
from irimi.decide.overlay import NoOverlay
from irimi.decide.policy import ShadowPolicy
from irimi.engine import session, wire
from irimi.engine.capture import StreamCapture
from irimi.engine.mitm import IrimiAddon
from irimi.protocol.headers import ANSWERED_BY_HEADER
from tests.support.credentials import BEARER_SECRET
from tests.support.doubles import NullStore
from tests.support.engine import engine_config, start_engine, via_proxy, via_tls_proxy
from tests.support.maps import map_index, targeted_index
from tests.support.servers import stop_server
from tests.support.upstreams import (
    NOT_GZIP,
    REAL_GZIP,
    SSE_FIRST,
    SSE_SECOND,
    StreamUpstream,
    TargetStub,
    serve,
)


def _host_header(headers: dict) -> str:
    """The Host header as sent, whatever case the client spelled it in."""
    return next(v for k, v in headers.items() if k.lower() == "host")


def test_an_https_write_is_answered_when_the_real_host_is_not_listening(tmp_path, monkeypatch):
    """mitmproxy's default `connection_strategy` is `eager`: it dials the real host - sending a
    ClientHello carrying real SNI - before it has seen the request it would have answered. An
    HTTPS route could then not be answered at all when the real service was unreachable, which is
    exactly the case shadow mode and delegation are for: an offline, decommissioned or
    not-yet-built API. `lazy` connects only when there is something to send.

    The dead port stands in for the unreachable service; nothing is ever listening on it, so a
    pass here means no connection to it was attempted.
    """
    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    dead = closed.getsockname()[1]
    closed.close()
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch))
    eng, seen, stop = start_engine(cfg)
    try:
        status, data = via_tls_proxy(
            cfg, eng.listen_port(), "127.0.0.1", dead, "POST", "/things", body=b'{"a": 1}'
        )
    finally:
        stop()
    assert status == 200
    assert json.loads(data)["a"] == 1
    (ex,) = seen
    assert (ex.answered_by, ex.kind, ex.request.scheme) == ("fake-L0", "write", "https")


def test_a_targeted_write_is_answered_by_the_target_not_by_the_fake(tmp_path, monkeypatch, target):
    """What a target is for: the write lands on an address the developer controls, that stub's
    body is what the agent parses, and the fake never runs."""
    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = via_proxy(
            eng.listen_port(), "POST", "http://127.0.0.1/things", body=b'{"a": 1}'
        )
    finally:
        stop()
    assert status == 200
    assert json.loads(data) == {"from": "target", "path": "/w"}
    # Method, path and body pass through unchanged. Sliced to three elements rather than
    # comparing the headers against themselves, and built as a list comprehension so an empty
    # `seen` - the regression this test exists to catch - fails readably instead of IndexError.
    assert [row[:3] for row in target.seen] == [("POST", "/w", b'{"a": 1}')]
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
    """Stamping the answer's header does not rebuild the response. `http.Response.make` assigns
    `.content`, which mitmproxy re-encodes per the surviving `content-encoding` - so a rebuilt
    response compresses an already-compressed body again, and turns a body that was never valid
    gzip into a valid gzip stream of itself. Either way the agent would get different bytes than
    the target sent, in a tool whose thesis is that it sees what the service would have sent.

    `respond` returns a new Response for every non-live answer, so this covers every delegated
    exchange; the live path is safe because `respond` hands back the object it was given there.
    """
    maps = targeted_index(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}{path}")],
    )
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
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
    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    (ex,) = seen
    assert (ex.request.host, ex.request.path) == ("127.0.0.1", "/things")
    assert ex.operation == "things.create"
    assert ex.target.endswith("/w")


CREDENTIAL_HEADERS_SENT = {
    "authorization": f"Bearer {BEARER_SECRET}",
    "cookie": "session=SECRET",
    "x-api-key": "SECRET",
    "dd-api-key": "SECRET",
    "x-honeycomb-team": "SECRET",
}


def test_every_credential_header_is_stripped_before_it_reaches_a_target(
    tmp_path, monkeypatch, target
):
    """A local stub does not need the real key, and forwarding it makes the target an
    exfiltration path for a credential the agent never meant it to have.

    Every credential header, not only `Authorization`: the rule is about keys, and `Cookie`,
    `x-api-key` and `DD-API-KEY` are keys.
    """
    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, _seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        via_proxy(
            eng.listen_port(),
            "POST",
            "http://127.0.0.1/things",
            body=b"{}",
            extra_headers=dict(CREDENTIAL_HEADERS_SENT),
        )
    finally:
        stop()
    headers = {k.lower(): v for k, v in target.seen[0][3].items()}
    assert [name for name in CREDENTIAL_HEADERS_SENT if name in headers] == []
    assert "SECRET" not in "".join(headers.values())
    assert headers["content-type"] == "application/json"  # everything else passes through


def test_forward_auth_keeps_the_credential_headers(tmp_path, monkeypatch, target):
    """The route opting in: a sandbox tenant or an internal simulator does need the key. It is
    one switch for all of them - a route that wants its `Authorization` forwarded is a route
    whose target is trusted with credentials."""
    from dataclasses import replace as _replace

    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    sm = maps.services[0]
    routes = tuple(
        _replace(r, forward_auth=True) if r.target != servicemaps.SELF_TARGET else r
        for r in sm.routes
    )
    maps = servicemaps.MapIndex((_replace(sm, routes=routes),))
    eng, _seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        via_proxy(
            eng.listen_port(),
            "POST",
            "http://127.0.0.1/things",
            body=b"{}",
            extra_headers=dict(CREDENTIAL_HEADERS_SENT),
        )
    finally:
        stop()
    headers = {k.lower(): v for k, v in target.seen[0][3].items()}
    assert {name: headers.get(name) for name in CREDENTIAL_HEADERS_SENT} == CREDENTIAL_HEADERS_SENT


def test_target_reads_sends_a_read_to_the_target_instead_of_the_real_service(
    tmp_path, monkeypatch, target, upstream
):
    """A delegated service: its target, not production, is the world the agent sees. The read
    must not reach the upstream, which would answer `hello from upstream`."""
    maps = targeted_index(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "", f"http://127.0.0.1:{target}")],
        target_reads=["127.0.0.1"],
    )
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    finally:
        stop()
    assert status == 200
    assert json.loads(data)["from"] == "target"
    assert target.seen[0][1] == "/hello"  # a bare origin keeps the original path
    (ex,) = seen
    assert (ex.answered_by, ex.kind) == ("delegated", "read")


def test_a_service_target_also_covers_a_route_its_map_does_not_list(tmp_path, monkeypatch, target):
    """Answering the unlisted routes with our own fake would give the agent a world that is half
    the stub's and half ours, which is what `service_map` on the Classification is for."""
    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "", f"http://127.0.0.1:{target}")]
    )
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/unlisted", body=b"{}")
    finally:
        stop()
    assert status == 200 and json.loads(data)["from"] == "target"
    (ex,) = seen
    assert (ex.answered_by, ex.kind) == ("delegated", "unknown")
    assert target.seen[0][1] == "/unlisted"


def test_an_unreachable_target_is_a_502_with_the_json_body_the_issue_specifies(
    tmp_path, monkeypatch
):
    """Never a silent fall back to the local fake: that would hide a broken setup and look
    exactly like a working shadow run.

    And the body is JSON naming irimi and the target, not mitmproxy's HTML error page. The flag
    was right all along; the page was not. An SDK parses the body, and stripe-python, openai and
    slack_sdk all raise on an HTML blob that names neither irimi nor the answer target, so "my
    shadow run started failing" gave no hint that the developer's own stub was down.
    """
    closed = socket.socket()
    closed.bind(("127.0.0.1", 0))
    dead = closed.getsockname()[1]
    closed.close()
    target_url = f"http://127.0.0.1:{dead}/w"
    maps = targeted_index(tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", target_url)])
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    assert status == 502
    body = json.loads(data)
    assert body["error"]["type"] == "irimi_target_failed"
    assert target_url in body["error"]["message"]
    # irimi answered it, with an error, and the flag says why. The write keeps its classification.
    (ex,) = seen
    assert (ex.answered_by, ex.kind) == ("irimi", "write")
    assert ex.flags == (Flag.TARGET_FAILED,)
    assert ex.target == target_url
    assert ex.response is not None and ex.response.status == 502


def test_a_target_that_stops_listening_mid_run_is_still_flagged(tmp_path, monkeypatch, target):
    """The probe is best-effort by construction - a stub can die between the probe and the dial -
    so the properties that must not depend on it are pinned separately: no fall back to the fake,
    `target-failed` on the exchange, and a 502 to the agent. Here the stub answers the first write
    and is gone for the second."""
    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        first, _ = via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
        target.stop()
        second, _ = via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    assert (first, second) == (200, 502)
    assert [ex.answered_by for ex in seen] == ["delegated", "irimi"]
    assert "target-failed" not in seen[0].flags
    assert "target-failed" in seen[1].flags


def test_a_faked_write_on_a_route_with_no_fixture_still_lists_its_webhooks(tmp_path, monkeypatch):
    """`fires:` is about the write, not the answer's fidelity: DEMO_MAP's route names no
    `fixture:`, so the fake is L0, and it was still accepted."""
    eng, seen, stop = start_engine(
        engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch))
    )
    try:
        status, _ = via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
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
    built nothing and claims nothing about what the real service would have sent. The
    route names `fires:`, so the empty lists are the policy withholding it, not a map without
    one - the test above is the same route faked."""
    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        first, _ = via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
        target.stop()
        second, _ = via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    assert (first, second) == (200, 502)
    assert [ex.answered_by for ex in seen] == ["delegated", "irimi"]
    assert "target-failed" in seen[1].flags
    assert [ex.would_fire for ex in seen] == [(), ()]


def test_a_remote_target_is_probed_too_and_an_unreachable_one_is_irimis_json_502(monkeypatch):
    """Every target is probed, a remote one as much as a loopback one, on the decision's worker
    thread, so one nothing answers at gets irimi's JSON 502 rather than mitmproxy's HTML page."""
    dialled = []

    def refuse(address, timeout):
        dialled.append(address)
        raise ConnectionRefusedError("refused")

    monkeypatch.setattr(socket, "create_connection", refuse)
    forward = targets.ForwardTo(netaddr.Target.parse("https://sandbox.example.com/v1"))
    refused = session._target_refusal(forward, ("127.0.0.1", 4000))
    assert isinstance(refused, targets.TargetUnreachable)
    assert "'https://sandbox.example.com/v1' could not be reached" in str(refused)
    assert dialled == [("sandbox.example.com", 443)]


def test_a_target_naming_our_own_listener_is_refused_with_a_json_502(tmp_path, monkeypatch):
    """Forwarding to ourselves would make irimi connect to its own listener in a loop.

    Driven through the addon rather than a live engine on purpose: the check compares the target
    against the port the listener actually bound, and arranging for a real engine to bind the one
    port a pre-built map already names is a race against TIME_WAIT, not a test.
    """
    from mitmproxy.test import tflow, tutils

    from irimi.engine.mitm import META_KEY, IrimiAddon

    listen_port = 4000
    maps = targeted_index(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{listen_port}/w")],
    )
    seen: list[Exchange] = []
    addon = IrimiAddon(
        engine_config(tmp_path, monkeypatch, maps=maps),
        ShadowPolicy(),
        NullStore(),
        NoOverlay(),
        seen.append,
        lambda port, error: None,
    )
    flow = tflow.tflow(req=tutils.treq(method=b"POST", host="127.0.0.1", port=80, path=b"/things"))
    flow.client_conn.sockname = ("127.0.0.1", listen_port)
    asyncio.run(addon.request(flow))

    assert flow.response is not None and flow.response.content is not None
    assert flow.response.status_code == 502
    assert json.loads(flow.response.content)["error"]["type"] == "irimi_target_failed"
    assert "own listener" in json.loads(flow.response.content)["error"]["message"]
    assert flow.response.headers[ANSWERED_BY_HEADER] == "irimi"
    # The flow was not pointed anywhere: refusing must not leave it aimed at the real service.
    assert (flow.request.host, flow.request.path) == ("127.0.0.1", "/things")
    # Answered and recorded in the request hook, so no later hook has anything left to finish.
    assert META_KEY not in flow.metadata
    (ex,) = seen
    assert (ex.answered_by, ex.flags) == ("irimi", (Flag.TARGET_FAILED,))


def test_a_locally_answered_flow_that_errors_keeps_its_own_flags(tmp_path, monkeypatch):
    """The `error` hook does not assert `upstream-error` on a write irimi answered itself: nothing
    was ever sent upstream, and the fidelity flag the answer really carried must survive. Driven
    through the addon because the case is a client disappearing between two hooks, which a live
    engine cannot stage."""
    from mitmproxy.test import tflow, tutils

    from irimi.engine.mitm import IrimiAddon

    maps = targeted_index(tmp_path, monkeypatch)
    seen: list[Exchange] = []
    addon = IrimiAddon(
        engine_config(tmp_path, monkeypatch, maps=maps),
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
    assert "upstream-error" not in ex.flags


def test_a_refused_target_is_flagged_once_even_if_the_client_then_vanishes(tmp_path, monkeypatch):
    """`request()` already answered and recorded this one; the client going away afterwards does
    not record it twice or make the target fail a second time. Driven through the addon, like
    every other client-disappears case."""
    from mitmproxy.test import tflow, tutils

    from irimi.engine.mitm import IrimiAddon

    listen_port = 4000
    maps = targeted_index(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{listen_port}/w")],
    )
    seen: list[Exchange] = []
    addon = IrimiAddon(
        engine_config(tmp_path, monkeypatch, maps=maps),
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
    assert ex.answered_by == "irimi"
    # The answer's fidelity flag goes with it: nothing answered the request faithfully.
    assert ex.flags == ("target-failed",)


def test_an_untargeted_write_is_still_faked(tmp_path, monkeypatch, target):
    """A route target must not leak onto the rest of the service."""
    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = via_proxy(
            eng.listen_port(), "POST", "http://127.0.0.1/elsewhere", body=b"{}"
        )
    finally:
        stop()
    assert status == 200
    assert json.loads(data) == {}  # the L0 echo, not the target
    assert target.seen == []
    (ex,) = seen
    assert ex.answered_by == "fake-L0" and ex.target == ""


def test_neither_half_of_a_delegated_exchange_is_a_write(tmp_path, monkeypatch, target):
    """`answered_by != "live"` was an exhaustive spelling of "is a write" only while every read
    was live. `target_reads` makes a delegated read the first non-live read, and the write log is
    what the overlay replays onto later reads."""
    maps = targeted_index(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "", f"http://127.0.0.1:{target}")],
        target_reads=["127.0.0.1"],
    )
    eng, _seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        addon = next(a for a in eng._master.addons.chain if isinstance(a, IrimiAddon))
        status, _ = via_proxy(eng.listen_port(), "GET", "http://127.0.0.1:1/hello")
        assert status == 200
        assert addon.session.write_log.for_run("t3st") == (), "a delegated read is in the write log"
        # Nor is the delegated *write*: the target performed it, or did not, and the overlay
        # cannot apply a write irimi did not author.
        via_proxy(eng.listen_port(), "POST", "http://127.0.0.1:1/things", body=b"{}")
        assert addon.session.write_log.for_run("t3st") == (), (
            "a delegated write is in the write log"
        )
    finally:
        stop()


def test_a_refused_target_is_not_a_write_either(tmp_path, monkeypatch, target):
    """A target irimi refuses is answered `502` and recorded in `request()`, and it must not reach
    the write log, the log the overlay applies to live reads.

    Nothing was performed: not at the target, not at the real service, not by the fake. The
    overlay would hand the agent back a write irimi refused to send anywhere.

    It is asked with the port and the address the connection reached irimi at, which is irimi's
    own listener too.
    """
    asked = []

    def refuse(target, port, local_address=""):
        asked.append((target.url, port, local_address))
        raise targets.TargetRefused("refusing this target")

    monkeypatch.setattr(targets, "refuse_self_target", refuse)
    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        addon = next(a for a in eng._master.addons.chain if isinstance(a, IrimiAddon))
        status, data = via_proxy(eng.listen_port(), "POST", "http://127.0.0.1/things", body=b"{}")
    finally:
        stop()
    assert status == 502 and json.loads(data)["error"]["type"] == "irimi_target_failed"
    assert asked == [(f"http://127.0.0.1:{target}/w", eng.listen_port(), "127.0.0.1")]
    # Still recorded - a write irimi could not answer is exactly what the trace has to show.
    assert [ex.flags for ex in seen] == [("target-failed",)]
    assert addon.session.write_log.for_run("t3st") == (), (
        "a write performed nowhere is in the write log"
    )


def test_an_ipv6_target_gets_a_bracketed_host_header(tmp_path, monkeypatch):
    """`parts.hostname` strips an IPv6 literal's brackets, so the authority has to put them back:
    RFC 3986 spells it `[::1]:3000`, and `::1:3000` is a different and unparseable thing. The
    README lists `::1` as a supported target and the loader accepts it."""

    class _V6Server(HTTPServer):
        address_family = socket.AF_INET6

    handler = cast(type[TargetStub], type("TargetStub", (TargetStub,), TargetStub.per_server()))
    srv = _V6Server(("::1", 0), handler)
    port = srv.server_address[1]
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    try:
        maps = targeted_index(
            tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://[::1]:{port}/w")]
        )
        eng, _seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
        try:
            status, _ = via_proxy(
                eng.listen_port(), "POST", "http://127.0.0.1:1/things", body=b"{}"
            )
        finally:
            stop()
        assert status == 200
        assert handler.seen, "the target was never reached"
        host_header = _host_header(handler.seen[0][3])
        assert host_header == f"[::1]:{port}", host_header
    finally:
        stop_server(srv)


def test_the_host_header_is_set_to_the_target_not_left_as_the_service(
    tmp_path, monkeypatch, target
):
    """Deleting `flow.request.host_header = authority` passed the whole suite. A stub that routes
    on Host - any vhost, any framework's host check - would see the real service's name."""
    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, _seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        via_proxy(eng.listen_port(), "POST", "http://127.0.0.1:1/things", body=b"{}")
    finally:
        stop()
    assert _host_header(target.seen[0][3]) == f"127.0.0.1:{target}"


def test_the_response_hook_writes_nothing_back_to_a_streamed_flow(tmp_path, monkeypatch):
    """The streamed-flow rule at the seam rather than end to end.

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

    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", "http://127.0.0.1:3999/w")]
    )
    addon = IrimiAddon(
        engine_config(tmp_path, monkeypatch, maps=maps),
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
    monkeypatch.setattr(session, "_probe_target", lambda url: None)  # nothing listens at 3999
    asyncio.run(addon.request(flow))
    assert flow.metadata[META_KEY].answered_by == "delegated"
    assert flow.response is not None
    flow.response.stream = True  # what `responseheaders` does for an event stream
    before = (flow.response.status_code, tuple(flow.response.headers.fields), flow.response.content)

    addon.response(flow)

    after = (flow.response.status_code, tuple(flow.response.headers.fields), flow.response.content)
    assert after == before
    assert ANSWERED_BY_HEADER not in flow.response.headers


def test_the_response_hook_records_a_streamed_flow_from_its_tee_and_writes_nothing_back(
    tmp_path, monkeypatch
):
    """The seam test above, with the stream going through a real `StreamCapture` as
    `responseheaders` sets one up: the flow is still left exactly as it was, and the exchange is
    recorded from the tee's copy."""
    from mitmproxy.test import tflow, tutils

    from irimi.engine.mitm import META_KEY, STREAM_KEY, IrimiAddon

    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", "http://127.0.0.1:3999/w")]
    )
    seen: list[Exchange] = []
    addon = IrimiAddon(
        engine_config(tmp_path, monkeypatch, maps=maps),
        ShadowPolicy(),
        NullStore(),
        NoOverlay(),
        seen.append,
        lambda port, error: None,
    )
    flow = tflow.tflow(
        req=tutils.treq(method=b"POST", host="127.0.0.1", port=80, path=b"/things"),
        resp=tutils.tresp(content=None, headers=((b"content-type", b"text/event-stream"),)),
    )
    flow.client_conn.sockname = ("127.0.0.1", 4000)
    monkeypatch.setattr(session, "_probe_target", lambda url: None)  # nothing listens at 3999
    asyncio.run(addon.request(flow))
    assert flow.metadata[META_KEY].answered_by == "delegated"
    # What `responseheaders` does for an event stream, and a chunk through it.
    assert flow.response is not None
    tee = StreamCapture()
    flow.metadata[STREAM_KEY] = flow.response.stream = tee
    tee(SSE_FIRST)
    before = (flow.response.status_code, tuple(flow.response.headers.fields), flow.response.content)

    addon.response(flow)

    after = (flow.response.status_code, tuple(flow.response.headers.fields), flow.response.content)
    assert after == before
    assert ANSWERED_BY_HEADER not in flow.response.headers
    assert flow.response.stream is tee and STREAM_KEY not in flow.metadata
    (ex,) = seen
    assert ex.response is not None and ex.response.body == SSE_FIRST


def test_a_streamed_target_response_is_not_buffered(tmp_path, monkeypatch):
    """The streaming guard names `delegated` as well as `live`, and only a delegated SSE route
    tells the two apart: a guard of `("live",)` alone would buffer it."""
    srv = serve(StreamUpstream)
    stream_port = srv
    try:
        maps = targeted_index(
            tmp_path,
            monkeypatch,
            targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{stream_port}/v1/stream")],
        )
        eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
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
            stamp = wire.answered_by_header("delegated")
            assert resp.getheader(ANSWERED_BY_HEADER) == stamp
            first = resp.read(len(SSE_FIRST))  # times out at 5 s if the proxy buffered the body
            assert first == SSE_FIRST
            srv.gate.set()
            assert resp.read() == SSE_SECOND
            conn.close()
        finally:
            srv.gate.set()
            stop()
    finally:
        srv.stop()
    # A delegated stream is recorded as a live one is: the target's chunks, joined.
    (ex,) = seen
    assert (ex.answered_by, ex.target) == ("delegated", f"http://127.0.0.1:{stream_port}/v1/stream")
    assert ex.response is not None and ex.response.body == SSE_FIRST + SSE_SECOND
    assert ex.stream_chunks == (len(SSE_FIRST), len(SSE_SECOND))
