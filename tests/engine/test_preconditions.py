"""Precondition checks through the engine: irimi's check read, and the verdict it gives."""

import json
import threading
import time

import pytest

from irimi.decide.policy import ShadowPolicy
from irimi.protocol.headers import ANSWERED_BY_HEADER
from tests.support.engine import (
    engine_config,
    live_summary,
    post_refund,
    read_via_proxy,
    slack_call,
    start_engine,
    writes_in,
)
from tests.support.maps import PRECONDITION_STRIPE_MAP, SLACK_PRECONDITION_MAP, map_index
from tests.support.upstreams import SLOW_S, FakeSlack, serve
from tests.support.waiting import eventually


def test_a_refund_of_a_fully_refunded_charge_is_rejected_with_stripes_own_body(
    precondition_stub,
):
    proxy, stub, seen = precondition_stub
    status, headers, data = post_refund(proxy, stub, "ch_FULL1")
    assert status == 400
    assert json.loads(data)["error"]["code"] == "charge_already_refunded"
    assert headers[ANSWERED_BY_HEADER] == "fake-L1"
    (write,) = writes_in(seen)
    assert write.precondition.outcome == "rejected"
    assert write.precondition.rejection_code == "charge_already_refunded"


def test_a_rejected_refund_records_no_webhooks(precondition_stub):
    """A refund the precondition check rejects would fire nothing; a passed one lists its events.

    The passed refund goes first, so the empty list is the map's `fires:` withheld."""
    proxy, stub, seen = precondition_stub
    status, _, _ = post_refund(proxy, stub, "ch_REAL1")
    assert status == 200
    status, _, _ = post_refund(proxy, stub, "ch_FULL1")
    assert status == 400
    passed, write = writes_in(seen)
    assert passed.would_fire == ("refund.created", "charge.refunded")
    assert write.precondition.outcome == "rejected"
    assert write.precondition.rejection_code == "charge_already_refunded"
    assert write.would_fire == ()


def test_a_second_refund_in_one_run_is_rejected_because_the_overlay_applied_the_first(
    precondition_stub,
):
    """The precondition check sees the run's own faked writes, so a second full refund fails.

    The real charge stays refundable; only irimi's write log knows about the first refund."""
    proxy, stub, seen = precondition_stub
    first, _, data = post_refund(proxy, stub, "ch_REAL1", amount=4900)
    assert first == 200
    assert json.loads(data)["object"] == "refund"
    second, headers, data = post_refund(proxy, stub, "ch_REAL1", amount=4900)
    assert second == 400
    assert json.loads(data)["error"]["code"] == "charge_already_refunded"
    assert headers[ANSWERED_BY_HEADER] == "fake-L1"
    assert [ex.precondition.outcome for ex in writes_in(seen)] == ["passed", "rejected"]


def test_a_rejected_write_never_enters_the_write_log(precondition_stub):
    """A rejected refund is not logged, so the overlay leaves a re-read of its charge live."""
    proxy, stub, seen = precondition_stub
    status, _, _ = post_refund(proxy, stub, "ch_FULL1")
    assert status == 400
    status, headers, data = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_FULL1")
    assert status == 200
    assert ANSWERED_BY_HEADER not in headers
    assert json.loads(data)["amount_refunded"] == 4900  # the stub's own
    assert seen[-1].answered_by == "live"
    assert seen[-1].overlay is None


def test_the_precondition_read_is_recorded_as_issued_by_the_engine(precondition_stub):
    proxy, stub, seen = precondition_stub
    status, _, _ = post_refund(proxy, stub, "ch_REAL1")
    assert status == 200
    (issued,) = [ex for ex in seen if ex.issued_by == "engine"]
    assert (issued.kind, issued.answered_by) == ("read", "live")
    assert (issued.request.method, issued.request.path) == ("GET", "/v1/charges/ch_REAL1")
    assert [ex.issued_by for ex in seen if ex is not issued] == ["agent"]


def test_a_precondition_read_that_429s_leaves_the_write_faked_at_l2(precondition_stub):
    proxy, stub, seen = precondition_stub
    status, headers, data = post_refund(proxy, stub, "ch_BUSY1")
    assert status == 200
    assert json.loads(data)["object"] == "refund"
    assert headers[ANSWERED_BY_HEADER] == "fake-L1"
    (write,) = writes_in(seen)
    assert (write.answered_by, write.precondition.outcome) == ("fake-L1", "not_evaluable")
    (issued,) = [ex for ex in seen if ex.issued_by == "engine"]
    assert issued.response is not None and issued.response.status == 429


def test_a_refund_l3_could_not_check_still_lists_its_webhooks(precondition_stub):
    """A refund irimi could not check is faked as accepted, and lists its webhooks."""
    proxy, stub, seen = precondition_stub
    status, _, _ = post_refund(proxy, stub, "ch_BUSY1")
    assert status == 200
    (write,) = writes_in(seen)
    assert write.precondition.outcome == "not_evaluable"
    assert write.would_fire == ("refund.created", "charge.refunded")


def test_a_precondition_read_that_redirects_is_not_followed_and_decides_nothing(precondition_stub):
    """irimi's check read never follows a redirect: a 302 leaves the check `not_evaluable`.

    Following `ch_MOVED1`'s 302 to `ch_FULL1` would judge the refund by another charge's state."""
    proxy, stub, seen = precondition_stub
    status, headers, data = post_refund(proxy, stub, "ch_MOVED1")
    assert status == 200
    assert json.loads(data)["object"] == "refund"
    assert headers[ANSWERED_BY_HEADER] == "fake-L1"
    (write,) = writes_in(seen)
    assert (write.answered_by, write.precondition.outcome) == ("fake-L1", "not_evaluable")
    (issued,) = [ex for ex in seen if ex.issued_by == "engine"]
    assert issued.response is not None and issued.response.status == 302
    assert stub.seen == [("GET", "/v1/charges/ch_MOVED1")]


def test_a_concurrent_request_is_not_delayed_by_another_requests_precondition_read(
    precondition_stub,
):
    """A read is served while another write's check read is in flight, on a worker thread."""
    proxy, stub, _ = precondition_stub
    took: dict[str, float] = {}
    finished: list[str] = []

    def a():
        start = time.monotonic()
        post_refund(proxy, stub, "ch_SLOW1")
        took["a"] = time.monotonic() - start
        finished.append("a")

    def b():
        read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/charges/ch_REAL1")
        finished.append("b")

    thread_a = threading.Thread(target=a)
    thread_b = threading.Thread(target=b)
    thread_a.start()
    # B is sent once A's precondition read is at the stub, which holds it for SLOW_S.
    eventually(lambda: ("GET", "/v1/charges/ch_SLOW1") in stub.seen)
    thread_b.start()
    thread_a.join(timeout=15)
    thread_b.join(timeout=15)
    assert took["a"] >= SLOW_S
    assert finished == ["b", "a"], "the read waited out the write's precondition read"


def test_the_stub_never_saw_a_post(precondition_stub):
    proxy, stub, _ = precondition_stub
    assert post_refund(proxy, stub, "ch_REAL1")[0] == 200
    assert post_refund(proxy, stub, "ch_FULL1")[0] == 400
    assert stub.seen == [
        ("GET", "/v1/charges/ch_REAL1"),
        ("GET", "/v1/charges/ch_FULL1"),
    ]


def _refund_then_list_summary(proxy, stub, seen, tmp_path, monkeypatch):
    """Refund `ch_REAL1` in full, list refunds, and return the summary the run prints.

    The map index is rebuilt so the write line is the map's `human:` sentence."""
    status, _, _ = post_refund(proxy, stub, "ch_REAL1", amount=4900)
    assert status == 200
    status, headers, _ = read_via_proxy(proxy, f"http://127.0.0.1:{stub}/v1/refunds")
    assert status == 200
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    maps = map_index(tmp_path, monkeypatch, doc=PRECONDITION_STRIPE_MAP)
    return live_summary(seen, maps)


def test_an_overlaid_read_is_listed_under_the_write_it_saw_through_the_proxy(
    precondition_stub, tmp_path, monkeypatch
):
    """The overlaid refunds list is the only `↳` line, directly under the refund it shows.

    irimi's check read is counted apart from the agent's reads and owes no `↳` line."""
    proxy, stub, seen = precondition_stub
    lines = _refund_then_list_summary(proxy, stub, seen, tmp_path, monkeypatch)
    # `$49.00` and not `4900`: the write names no currency, and irimi's own precondition read of
    # `ch_REAL1` found `usd` on the charge.
    write = lines.index(
        "  ○ stripe  refund $49.00 on ch_REAL1  faked; checked against live state: would succeed"
    )
    assert lines[write + 1] == "    ↳ GET /v1/refunds saw it  overlay"
    assert [line for line in lines if "↳" in line] == [lines[write + 1]]
    assert (
        "  127.0.0.1  1 read (1 showing this run's writes)  1 irimi check read  1 faked write"
    ) in lines


def test_the_closing_line_names_the_webhooks_a_real_refund_would_have_sent(
    precondition_stub, tmp_path, monkeypatch
):
    """The summary names the accepted refund's `would_fire` events once each, in the map's order."""
    proxy, stub, seen = precondition_stub
    lines = _refund_then_list_summary(proxy, stub, seen, tmp_path, monkeypatch)
    assert [ex.would_fire for ex in writes_in(seen)] == [("refund.created", "charge.refunded")]
    assert "  would have fired: refund.created, charge.refunded" in lines


def test_a_faked_refund_carries_the_currency_its_precondition_read_found(precondition_stub):
    """A faked refund that posts no currency carries the `usd` irimi's check read found."""
    proxy, stub, seen = precondition_stub
    status, _, _ = post_refund(proxy, stub, "ch_REAL1")
    assert status == 200
    (write,) = writes_in(seen)
    assert write.precondition.outcome == "passed"
    assert write.precondition.currency == "usd"


def test_a_write_whose_precondition_read_never_answered_carries_no_currency(
    precondition_stub, tmp_path, monkeypatch
):
    """A write whose check read got 429 prints minor units of an unknown currency, and the 429."""
    proxy, stub, seen = precondition_stub
    status, _, _ = post_refund(proxy, stub, "ch_BUSY1")
    assert status == 200
    (write,) = writes_in(seen)
    assert write.precondition.outcome == "not_evaluable"
    assert write.precondition.currency == ""
    maps = map_index(tmp_path, monkeypatch, doc=PRECONDITION_STRIPE_MAP)
    lines = live_summary(seen, maps)
    assert (
        "  ○ stripe  refund 100 (minor units, currency unknown) on ch_BUSY1  faked; could not "
        "check: stripe answered irimi's check read with 429"
    ) in lines


def _conversations_info(posted):
    """Answer `conversations.info`: `C0OK` healthy, `C0ARCHIVED` archived, others not found."""
    channel = posted.get("channel")
    if channel not in ("C0OK", "C0ARCHIVED"):
        return {"ok": False, "error": "channel_not_found"}
    return {
        "ok": True,
        "channel": {
            "id": channel,
            "is_channel": True,
            "is_member": True,
            "is_archived": channel == "C0ARCHIVED",
        },
    }


@pytest.fixture
def slack_precondition_stub(tmp_path, monkeypatch):
    """Yield (proxy port, stub port, exchanges) for precondition checks against a fake Slack."""
    from irimi.decide.overlay import ServiceOverlay
    from irimi.decide.reader import UpstreamReader

    slack = serve(
        FakeSlack, threaded=True, methods={"/api/conversations.info": _conversations_info}
    )
    maps = map_index(tmp_path, monkeypatch, doc=SLACK_PRECONDITION_MAP)
    eng, seen, stop = start_engine(
        engine_config(tmp_path, monkeypatch, maps=maps),
        overlay=ServiceOverlay(),
        policy=ShadowPolicy(reader=UpstreamReader(), maps=maps),
    )
    yield eng.listen_port(), slack, seen
    stop()
    slack.stop()


def test_a_post_to_an_archived_channel_is_rejected(slack_precondition_stub):
    """A post to an archived channel gets Slack's own `ok: false` at 200, which slack_sdk raises."""
    proxy, stub, seen = slack_precondition_stub
    status, headers, data = slack_call(
        proxy, stub, "chat.postMessage", {"channel": "C0ARCHIVED", "text": "hi"}
    )
    assert status == 200
    assert json.loads(data) == {"ok": False, "error": "is_archived"}
    assert headers[ANSWERED_BY_HEADER] == "fake-L1"
    (write,) = writes_in(seen)
    assert (write.precondition.outcome, write.precondition.rejection_code) == (
        "rejected",
        "is_archived",
    )


def test_a_post_to_a_healthy_channel_passes_and_the_probe_was_a_conversations_info(
    slack_precondition_stub,
):
    proxy, stub, seen = slack_precondition_stub
    status, headers, data = slack_call(
        proxy, stub, "chat.postMessage", {"channel": "C0OK", "text": "hi"}
    )
    assert status == 200
    assert json.loads(data)["ok"] is True
    assert headers[ANSWERED_BY_HEADER] == "fake-L1"
    (write,) = writes_in(seen)
    assert write.precondition.outcome == "passed"
    assert stub.seen == [("POST", "/api/conversations.info")]
