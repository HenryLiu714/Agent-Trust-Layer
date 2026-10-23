"""No addon hook raises: a raise forwards the flow, so a write would escape shadow mode."""

import asyncio
import itertools
import json
import types

import pytest

from irimi.core.exchange import Exchange, Flag, Precondition, Request, Response, WriteOutcome
from irimi.decide import targets
from irimi.decide.overlay import Overlaid
from irimi.decide.policy import Decided, Respond
from irimi.decide.writelog import Translation
from irimi.engine import mitm, session
from irimi.protocol.headers import ANSWERED_BY_HEADER
from tests.support.doubles import NullStore
from tests.support.engine import bare_addon, bare_flow, engine_config, start_engine, via_proxy
from tests.support.maps import targeted_index


def test_a_raise_in_the_decision_answers_locally_instead_of_forwarding(
    tmp_path, monkeypatch, upstream, refused_port
):
    """The never-raise rule, as a whole rather than per call site.

    A hook that raises makes mitmproxy forward the flow untouched, so an exception anywhere in
    the classify/answer decision sends the agent's write to the real service - and no `Plan` is
    set, so `response()` returns early and no Exchange is recorded either. The write is
    performed for real and is invisible in the trace.
    """
    maps = targeted_index(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{refused_port}/w")],
    )

    def boom(*args, **kwargs):
        raise RuntimeError("the decision exploded")

    monkeypatch.setattr(targets, "target_url", boom)
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = via_proxy(
            eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}"
        )
    finally:
        stop()
    # The upstream's do_POST answers 500 and exists to prove it was never reached.
    assert status == 502, "the write reached the real upstream"
    assert json.loads(data)["error"]["type"] == "irimi_decision_failed"
    assert len(seen) == 1, "the exchange must still be recorded"
    assert seen[0].flags == (Flag.UNCLASSIFIED, Flag.DECISION_FAILED)
    assert (seen[0].kind, seen[0].answered_by) == ("unknown", "irimi")


def test_a_raise_handing_the_decision_to_a_worker_answers_locally(tmp_path, monkeypatch):
    """The hook awaits `asyncio.to_thread`, and the hand-off can fail on its own - an
    executor already shut down under a stopping proxy - before `plan`'s guard is ever reached.
    It takes the same 502 as a decision that raised: the request may be a write. It is timed like
    any other exchange, from the request hook's first line to the answer: the engine's clock
    is a counter here, so an `ended_at` the 502 path forgot would read 100.0, not 101.0."""
    seen: list[Exchange] = []
    addon = bare_addon(tmp_path, monkeypatch, on_exchange=seen.append)

    def no_threads(*args, **kwargs):
        raise RuntimeError("cannot schedule new futures after shutdown")

    monkeypatch.setattr(asyncio, "to_thread", no_threads)
    clock = itertools.count(100)
    counter = types.SimpleNamespace(time=lambda: float(next(clock)))
    monkeypatch.setattr(mitm, "time", counter)
    monkeypatch.setattr(session, "time", counter)
    flow = bare_flow(b"POST", b"/things")
    asyncio.run(addon.request(flow))

    assert flow.response.status_code == 502
    assert json.loads(flow.response.content)["error"]["type"] == "irimi_decision_failed"
    (ex,) = seen
    assert ex.flags == (Flag.UNCLASSIFIED, Flag.DECISION_FAILED)
    assert (ex.started_at, ex.ended_at) == (100.0, 101.0)


def test_a_raise_carrying_a_rewrite_onto_the_flow_answers_locally(tmp_path, monkeypatch):
    """`_carry_translation` runs back on the loop, outside `plan`'s guard, but under the
    decision's backstop all the same, so a raise there cannot escape the hook and forward a
    half-edited read with nothing recorded."""
    from dataclasses import replace

    class _Translating:
        def __call__(
            self, write_log, classification, read_request, upstream_response, translated_from=None
        ):
            return Overlaid(upstream_response)

        def rewrite(self, write_log, classification, read_request):
            return Translation(replace(read_request, query="translated=1"), "translated=0")

    seen: list[Exchange] = []
    addon = bare_addon(tmp_path, monkeypatch, overlay=_Translating(), on_exchange=seen.append)
    write = bare_flow(b"POST", b"/things")
    asyncio.run(addon.request(write))
    addon.response(write)  # a faked write joins the log here, so the read below is rewritten

    def boom(flow, rewritten):
        raise RuntimeError("the flow edit exploded")

    monkeypatch.setattr(addon, "_carry_translation", boom)
    read = bare_flow(b"GET", b"/hello")
    asyncio.run(addon.request(read))

    assert read.response.status_code == 502
    assert json.loads(read.response.content)["error"]["type"] == "irimi_decision_failed"
    assert read.request.path == "/hello"
    assert seen[-1].flags == (Flag.UNCLASSIFIED, Flag.DECISION_FAILED)


def test_a_raise_recording_an_engine_read_leaves_the_write_answered(tmp_path, monkeypatch):
    """`Decided.issued` is recorded last, after the write's own answer is on the flow. A store
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
    issued = Exchange(
        request=probe,
        response=Response(status=200, headers=(), body=b"{}"),
        service="demo",
        operation="things.list",
        kind="read",
        answered_by="live",
        run_id="",
        issued_by="engine",
    )

    class _Checking:
        name = "checking"

        def decide(self, request, classification, write_log=(), run_id="", target=None):
            return Decided(
                Respond(
                    Response(status=200, headers=(), body=b"{}"),
                    "fake-L0",
                    WriteOutcome(Precondition("passed")),
                ),
                issued=(issued,),
            )

    def refuse_engine_reads(ex):
        if ex.issued_by == "engine":
            raise RuntimeError("the store exploded")

    addon = bare_addon(tmp_path, monkeypatch, _Checking(), on_exchange=refuse_engine_reads)
    flow = bare_flow(b"POST", b"/things")
    with pytest.raises(RuntimeError, match="the store exploded"):
        asyncio.run(addon.request(flow))

    assert flow.response is not None and flow.response.status_code == 200
    assert flow.metadata[META_KEY].decision.outcome.precondition.outcome == "passed"


class _Raising(RuntimeError):
    """What every raising collaborator below raises."""


class _RaisingPolicy:
    name = "raising"

    def decide(self, request, classification, write_log=(), run_id="", target=None):
        raise _Raising("the policy raised")


class _RaisingStore(NullStore):
    def record(self, exchange):
        raise _Raising("the store raised")


class _RaisingOverlay:
    def __call__(
        self, write_log, classification, read_request, upstream_response, translated_from=None
    ):
        raise _Raising("the overlay raised")

    def rewrite(self, write_log, classification, read_request):
        raise _Raising("the overlay raised")


def _faked_write(addon):
    """A write the addon answered itself, with its plan still on the flow."""
    flow = bare_flow(b"POST", b"/things")
    asyncio.run(addon.request(flow))
    assert flow.response is not None
    return flow


def _live_read_after_a_write(addon):
    """A read forwarded live once the run has a write, answered by the upstream."""
    from mitmproxy.test import tutils

    write = _faked_write(addon)
    addon.response(write)
    flow = bare_flow(b"GET", b"/hello")
    asyncio.run(addon.request(flow))
    flow.response = tutils.tresp(content=b"hello from upstream")
    return flow


def _request_with_raising_policy(tmp_path, monkeypatch, seen):
    addon = bare_addon(tmp_path, monkeypatch, _RaisingPolicy(), on_exchange=seen.append)
    flow = bare_flow(b"POST", b"/things")
    asyncio.run(addon.request(flow))
    assert flow.response is not None and flow.response.status_code == 502
    assert flow.response.headers[ANSWERED_BY_HEADER] == "irimi"
    assert Flag.DECISION_FAILED in seen[-1].flags


def _request_with_raising_store(tmp_path, monkeypatch, seen):
    addon = bare_addon(tmp_path, monkeypatch, store=_RaisingStore(), on_exchange=seen.append)
    flow = _faked_write(addon)
    assert flow.response.headers[ANSWERED_BY_HEADER] == "fake-L0"


def _request_with_raising_overlay(tmp_path, monkeypatch, seen):
    addon = bare_addon(tmp_path, monkeypatch, overlay=_RaisingOverlay(), on_exchange=seen.append)
    write = _faked_write(addon)
    addon.response(write)
    read = bare_flow(b"GET", b"/hello")
    asyncio.run(addon.request(read))
    # A read is forwarded live, and unchanged: the rewrite that raised is not applied.
    assert read.response is None and read.request.path == "/hello"


def _response_with_raising_store(tmp_path, monkeypatch, seen):
    addon = bare_addon(tmp_path, monkeypatch, store=_RaisingStore(), on_exchange=seen.append)
    addon.response(_faked_write(addon))
    assert seen[-1].answered_by == "fake-L0"


def _response_with_raising_overlay(tmp_path, monkeypatch, seen):
    addon = bare_addon(tmp_path, monkeypatch, overlay=_RaisingOverlay(), on_exchange=seen.append)
    flow = _live_read_after_a_write(addon)
    addon.response(flow)
    assert flow.response.content == b"hello from upstream"
    assert ANSWERED_BY_HEADER not in flow.response.headers


def _error_with_raising_store(tmp_path, monkeypatch, seen):
    addon = bare_addon(tmp_path, monkeypatch, store=_RaisingStore(), on_exchange=seen.append)
    flow = bare_flow(b"GET", b"/hello")
    asyncio.run(addon.request(flow))
    addon.error(flow)
    assert Flag.UPSTREAM_ERROR in seen[-1].flags


def _tls_failure_with_raising_callback(tmp_path, monkeypatch, seen):
    addon = bare_addon(tmp_path, monkeypatch, on_exchange=seen.append)

    def refuse(host):
        raise _Raising("the callback raised")

    addon.on_tls_failure = refuse
    data = types.SimpleNamespace(
        conn=types.SimpleNamespace(sni="api.example.test"),
        context=types.SimpleNamespace(server=types.SimpleNamespace(address=None)),
    )
    addon.tls_failed_client(data)


@pytest.mark.parametrize(
    "case",
    [
        _request_with_raising_policy,
        _request_with_raising_store,
        _request_with_raising_overlay,
        _response_with_raising_store,
        _response_with_raising_overlay,
        _error_with_raising_store,
        _tls_failure_with_raising_callback,
    ],
    ids=lambda case: case.__name__.lstrip("_"),
)
def test_no_hook_raises_when_a_collaborator_does(case, tmp_path, monkeypatch):
    """Each hook the addon has a collaborator in, with that collaborator raising: the hook
    returns, a write is answered by irimi and never forwarded, and what happened is recorded."""
    seen: list[Exchange] = []
    case(tmp_path, monkeypatch, seen)
