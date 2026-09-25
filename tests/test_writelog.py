"""`writelog`: the run's faked writes, decoded for the overlay and the policy alike (#45)"""

import json

from irimi import writelog
from irimi.exchange import Exchange, Request, Response

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
        validation="unvalidated",
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


def test_a_body_over_the_size_cap_is_not_a_json_object():
    padded = json.dumps({"description": "x" * writelog.MAX_BODY_BYTES}).encode()
    assert writelog.json_object(padded) is None
    assert writelog.json_object(b'{"ok": true}') == {"ok": True}
