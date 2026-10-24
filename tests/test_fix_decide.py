"""The overlay never edits an upstream error about the caller or the service into a success, and
a run that has ended keeps no write and no idempotency key, whatever it sends after its end."""

import dataclasses
import json
from typing import Any

import pytest

from irimi.core.exchange import Exchange, Precondition, Request, Response
from irimi.decide import idempotency, writelog
from irimi.decide.overlay import ServiceOverlay
from irimi.decide.writelog import WriteLog
from irimi.servicemaps import IdSpec, MapIndex, Route, ServiceMap
from irimi.servicemaps.classify import classify

STRIPE = ServiceMap(
    service="stripe",
    hosts=frozenset({"api.stripe.com"}),
    routes=(
        Route("GET", "/v1/charges/{charge}", "charges.retrieve", "read"),
        Route("GET", "/v1/refunds/{refund}", "refunds.retrieve", "read"),
        Route("POST", "/v1/refunds", "refunds.create", "write", ids={"id": IdSpec(prefix="re_")}),
    ),
)
SLACK = ServiceMap(
    service="slack",
    hosts=frozenset({"slack.com"}),
    routes=(
        Route("POST", "/api/chat.postMessage", "chat.postMessage", "write"),
        Route("POST", "/api/conversations.replies", "conversations.replies", "read"),
    ),
    style="rpc",
)
MAPS = MapIndex(services=(STRIPE, SLACK))
MINTED = "re_MINTEDfix1"
TS = "1800000000.000001"
JSON = (("content-type", "application/json"),)


def _request(method: str, host: str, path: str, body: bytes = b"") -> Request:
    headers = (("content-type", "application/json"),) if body else ()
    return Request(method, "https", host, 443, path, "", headers, body)


def _faked(request: Request, service: str, operation: str, answer: dict[str, Any]) -> Exchange:
    return Exchange(
        request=request,
        response=Response(200, JSON, json.dumps(answer).encode()),
        service=service,
        operation=operation,
        kind="write",
        answered_by="fake-L1",
        run_id="r1",
    )


REFUND = _faked(
    _request("POST", "api.stripe.com", "/v1/refunds", b'{"charge": "ch_1", "amount": 100}'),
    "stripe",
    "refunds.create",
    {"id": MINTED, "object": "refund", "charge": "ch_1", "amount": 100, "status": "succeeded"},
)
POST = _faked(
    _request("POST", "slack.com", "/api/chat.postMessage", b'{"channel": "C1", "text": "hi"}'),
    "slack",
    "chat.postMessage",
    {
        "ok": True,
        "channel": "C1",
        "ts": TS,
        "message": {"type": "message", "ts": TS, "text": "hi", "user": "U0BOT"},
    },
)


def _overlaid(write: Exchange, read: Request, status: int, document: Any) -> tuple[Response, Any]:
    upstream = Response(status, JSON, json.dumps(document).encode())
    out = ServiceOverlay()([write], classify(read, MAPS), read, upstream)
    return out.response, out.fidelity


@pytest.mark.parametrize(
    ("status", "document"),
    [
        (429, {"message": "slow down"}),
        (503, {}),
        (401, {"error": {"code": "resource_missing"}}),
        (500, {"error": {"type": "api_error"}}),
    ],
)
def test_a_minted_refunds_read_keeps_an_error_that_is_not_not_found(
    status: int, document: Any
) -> None:
    """Only a 404 is Stripe saying it never heard of the id irimi minted. A rate limit, a bad key
    or an outage would have reached the agent for a real refund too: left as sent, `partial`."""
    read = _request("GET", "api.stripe.com", f"/v1/refunds/{MINTED}")
    response, fidelity = _overlaid(REFUND, read, status, document)
    assert response.status == status
    assert json.loads(response.body) == document
    assert fidelity == "partial"


def test_a_minted_refunds_not_found_is_still_answered_with_the_refund() -> None:
    read = _request("GET", "api.stripe.com", f"/v1/refunds/{MINTED}")
    response, fidelity = _overlaid(REFUND, read, 404, {"error": {"code": "resource_missing"}})
    assert response.status == 200
    assert json.loads(response.body)["id"] == MINTED
    assert fidelity == "full"


@pytest.mark.parametrize("status", [429, 500])
def test_a_minted_threads_read_is_not_made_ok_under_an_error_status(status: int) -> None:
    """Slack answers `thread_not_found` at 200; the same body at a 429 or a 5xx is not that."""
    read = _request(
        "POST",
        "slack.com",
        "/api/conversations.replies",
        json.dumps({"channel": "C1", "ts": TS}).encode(),
    )
    error = {"ok": False, "error": "thread_not_found"}
    response, fidelity = _overlaid(POST, read, status, error)
    assert response.status == status
    assert json.loads(response.body) == error
    assert fidelity == "partial"


def test_a_minted_threads_not_found_at_200_is_still_the_thread() -> None:
    read = _request(
        "POST",
        "slack.com",
        "/api/conversations.replies",
        json.dumps({"channel": "C1", "ts": TS}).encode(),
    )
    response, fidelity = _overlaid(POST, read, 200, {"ok": False, "error": "thread_not_found"})
    assert response.status == 200
    assert json.loads(response.body)["ok"] is True
    assert fidelity == "full"


def test_a_write_sent_after_its_run_ended_is_filed_under_no_run() -> None:
    log = WriteLog()
    log.add(REFUND)
    log.end_run("r1")
    log.add(REFUND)
    assert log.for_run("r1") == ()
    log.add(dataclasses.replace(POST, run_id="r2"))
    assert len(log.for_run("r2")) == 1


def test_a_key_sent_after_its_run_ended_is_kept_by_no_run() -> None:
    keys = idempotency.Store()
    first = idempotency.FirstAnswer(200, b"{}", "application/json", "fake-L1", (), Precondition())
    slot = ("r1", "stripe", "k1")
    params: idempotency.Canonical = ("POST", "/v1/refunds", "", {})
    keys.end_run("r1")
    keys.put(slot, params, first)
    assert keys.get(slot, params) == (None, False)
    keys.put(("r2", "stripe", "k1"), params, first)
    assert keys.get(("r2", "stripe", "k1"), params) == (first, False)


def test_the_ended_runs_remembered_are_bounded_newest_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(writelog, "MAX_ENDED_RUNS", 3)
    ended = writelog.EndedRuns()
    for run_id in ("a", "b", "c", "a", "d"):
        ended.add(run_id)
    assert [run_id in ended for run_id in "abcd"] == [True, False, True, True]
