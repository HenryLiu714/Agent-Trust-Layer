"""`writelog`: the run's faked writes, decoded for the overlay and the policy alike."""

import json
from dataclasses import replace

from irimi.core import bodies
from irimi.core.exchange import Exchange, Request, Response
from irimi.core.limits import MAX_BODY_BYTES
from irimi.decide import writelog
from irimi.protocol.trace import UNATTRIBUTED

ANSWER = {
    "id": "re_MINTED1",
    "object": "refund",
    "charge": "ch_REAL1",
    "amount": 100,
    "status": "succeeded",
}


def _request(host="api.stripe.com", path="/v1/refunds", method="POST", headers=(), body=b""):
    return Request(
        method=method,
        scheme="https",
        host=host,
        port=443,
        path=path,
        query="",
        headers=tuple(headers),
        body=body,
    )


def _read(headers=()):
    return _request(method="GET", path="/v1/charges/ch_REAL1", headers=headers)


def _write(service="stripe", headers=(), body=None, response=True):
    request = _request(
        headers=(("content-type", "application/x-www-form-urlencoded"), *headers),
        body=b"charge=ch_REAL1&amount=100",
    )
    raw = body if body is not None else json.dumps(ANSWER).encode()
    return Exchange(
        request=request,
        response=Response(status=200, headers=(), body=raw) if response else None,
        service=service,
        operation="refunds.create",
        kind="write",
        answered_by="fake-L1",
        run_id="7f3a",
    )


def test_a_write_in_scope_is_decoded_with_its_posted_fields_and_answer():
    [write] = writelog.decode("stripe", _read(), [_write()])
    assert write.operation == "refunds.create"
    assert write.posted["charge"] == "ch_REAL1"
    assert write.answer == ANSWER


def test_a_write_of_another_service_is_not_decoded():
    assert writelog.decode("stripe", _read(), [_write(service="slack")]) == []


def test_a_write_from_another_connected_account_is_not_decoded():
    log = [_write(headers=(("stripe-account", "acct_A"),))]
    assert writelog.decode("stripe", _read(headers=(("stripe-account", "acct_B"),)), log) == []
    assert len(writelog.decode("stripe", _read(headers=(("stripe-account", "acct_A"),)), log)) == 1


def test_a_write_with_no_response_or_a_body_that_is_not_a_json_object_is_skipped():
    log = [_write(response=False), _write(body=b"not json"), _write(body=b"[1, 2]")]
    assert writelog.decode("stripe", _read(), log) == []


def test_scoped_writes_is_none_without_a_table_entry_or_a_write():
    assert writelog.scoped_writes(None, "slack", _read(), [_write()]) is None
    assert writelog.scoped_writes("entry", "stripe", _read(), []) is None


def test_scoped_writes_returns_the_entry_and_the_decoded_writes():
    ex = _write()
    writes = writelog.decode("stripe", _read(), [ex])
    assert writelog.scoped_writes("entry", "stripe", _read(), [ex]) == (
        "entry",
        writes,
    )


def test_a_run_sees_its_own_writes_and_no_other_run_s():
    """Two runs faking writes into one log: each reads back its own, in order, and a run that has
    made none gets an empty tuple."""
    log = writelog.WriteLog()
    a1, b1, a2 = (replace(_write(), run_id=run) for run in ("run-a", "run-b", "run-a"))
    for ex in (a1, b1, a2):
        log.add(ex)
    assert log.for_run("run-a") == (a1, a2)
    assert log.for_run("run-b") == (b1,)
    assert log.for_run("run-c") == ()


def test_requests_no_run_names_form_a_log_of_their_own():
    log = writelog.WriteLog()
    lost, named = replace(_write(), run_id=UNATTRIBUTED), _write()
    log.add(lost)
    log.add(named)
    assert log.for_run(UNATTRIBUTED) == (lost,)
    assert log.for_run(named.run_id) == (named,)


def test_ending_a_run_drops_its_writes_and_no_other_run_s():
    log = writelog.WriteLog()
    a, b = (replace(_write(), run_id=run) for run in ("run-a", "run-b"))
    log.add(a)
    log.add(b)
    log.end_run("run-a")
    log.end_run("never-started")
    assert log.for_run("run-a") == ()
    assert log.for_run("run-b") == (b,)


def test_a_run_s_writes_already_read_do_not_change_when_it_adds_more():
    """The engine reads a run's writes on its event loop and hands them to a worker thread, so the
    tuple it got is the log's own and never changes under the worker."""
    log = writelog.WriteLog()
    first = _write()
    log.add(first)
    seen = log.for_run(first.run_id)
    log.add(_write())
    assert seen == (first,)
    assert len(log.for_run(first.run_id)) == 2


def test_a_body_over_the_size_cap_is_not_a_json_object():
    padded = json.dumps({"description": "x" * MAX_BODY_BYTES}).encode()
    assert bodies.json_object(padded) is None
    assert bodies.json_object(b'{"ok": true}') == {"ok": True}
