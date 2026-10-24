"""Targets through the engine: a user's URL answers in place of the service."""

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
    """An HTTPS write is faked without dialling the real host, which may be unreachable.

    irimi sets mitmproxy's `connection_strategy` to `lazy`; `eager` would dial before the request
    arrives. Nothing listens on the dead port, so a pass means no connection was attempted."""
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
    """A targeted write lands on the developer's address and the agent gets its body; no fake."""
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
    # Method, path and body pass through unchanged; an empty `seen` fails readably here.
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
    """A target's body reaches the agent byte for byte; stamping does not rebuild the response.

    A rebuilt response would be re-encoded per its `content-encoding`, compressing gzip twice or
    turning invalid gzip into valid gzip of itself."""
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
    assert stamp == "delegated"  # the answered-by header is still added
    (ex,) = seen
    assert ex.answered_by == "delegated"


def test_a_delegated_exchange_records_what_the_agent_asked_for(tmp_path, monkeypatch, target):
    """A targeted exchange keeps the agent's own request, and the target in `target`."""
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
    """Every credential header, `Cookie` and `x-api-key` included, is stripped before a target.

    Forwarded, the target would hold a key the agent never meant it to have."""
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
    """A route with `forward_auth` passes every credential header to its trusted target."""
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
    """With `target_reads`, a read goes to the target and never reaches the real service."""
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
    """A service-wide target answers unlisted routes too, so no answer is half fake, half target."""
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
    """An unreachable target is irimi's JSON 502 naming the target, never a fall back to the fake.

    A fall back would look like a working run; an HTML page would name neither irimi nor the
    target to the SDK that parses it."""
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
    """A target that dies after the probe still gives a 502 and `target-failed`, never a fake.

    The probe is best effort: a stub can die between the probe and the dial."""
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
    """An echo-answered write still lists its `fires:` webhooks: they describe the write."""
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
    """A write sent to a target lists no webhooks: irimi claims nothing about what it did.

    The route names `fires:`, so the empty lists are the policy withholding them."""
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
    """A remote target is probed like a loopback one; an unreachable one gets irimi's JSON 502."""
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
    """A target naming irimi's own listener gets a JSON 502, so irimi never loops to itself.

    Driven through the addon: binding a real engine to the port a map names races TIME_WAIT."""
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
    """The `error` hook adds no `upstream-error` to a write irimi answered itself.

    Driven through the addon: a client vanishing between two hooks cannot be staged live."""
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
    assert ex.flags == ()


def test_a_refused_target_is_flagged_once_even_if_the_client_then_vanishes(tmp_path, monkeypatch):
    """A refused target is recorded once, even if the client vanishes after `request()`."""
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
    assert ex.flags == ("target-failed",)


def test_an_untargeted_write_is_still_faked(tmp_path, monkeypatch, target):
    """A route's target does not apply to the service's other routes."""
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
    assert json.loads(data) == {}  # the echo, not the target
    assert target.seen == []
    (ex,) = seen
    assert ex.answered_by == "fake-L0" and ex.target == ""


def test_neither_half_of_a_delegated_exchange_is_a_write(tmp_path, monkeypatch, target):
    """Neither a read nor a write sent to a target enters the write log the overlay applies."""
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
        # The overlay cannot apply a write irimi did not author.
        via_proxy(eng.listen_port(), "POST", "http://127.0.0.1:1/things", body=b"{}")
        assert addon.session.write_log.for_run("t3st") == (), (
            "a delegated write is in the write log"
        )
    finally:
        stop()


def test_a_refused_target_is_not_a_write_either(tmp_path, monkeypatch, target):
    """A write to a refused target is recorded with a 502 but never enters the write log.

    The refusal is asked with the port and address the connection reached irimi at."""
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
    # Still recorded: the trace has to show a write irimi could not answer.
    assert [ex.flags for ex in seen] == [("target-failed",)]
    assert addon.session.write_log.for_run("t3st") == (), (
        "a write performed nowhere is in the write log"
    )


def test_an_ipv6_target_gets_a_bracketed_host_header(tmp_path, monkeypatch):
    """An IPv6 target gets a `[::1]:PORT` Host header (RFC 3986), not an unparseable `::1:PORT`."""

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
    """A target gets its own authority as Host, so a stub that routes on Host works."""
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
    """The `response` hook writes nothing back to a streamed flow, whose headers are already sent.

    `responseheaders` owns a streamed answer and its stamp, and is not called here, so a stamp
    on the flow proves `response()` wrote to it. No client could observe the difference."""
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
    """With a real `StreamCapture`, the flow is left as it was and recorded from the tee's copy."""
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
    """A target's event stream reaches the agent chunk by chunk, as a live one does."""
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
    # A target's stream is recorded as a live one is: its chunks, joined.
    (ex,) = seen
    assert (ex.answered_by, ex.target) == ("delegated", f"http://127.0.0.1:{stream_port}/v1/stream")
    assert ex.response is not None and ex.response.body == SSE_FIRST + SSE_SECOND
    assert ex.stream_chunks == (len(SSE_FIRST), len(SSE_SECOND))
