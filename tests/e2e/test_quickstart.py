"""The quickstart refund agent end to end, through the proxy and base-URL mode.

Every refund is answered by irimi and none reaches Stripe."""

import json
import sys
import uuid

import pytest

from irimi.cli import main
from irimi.core.money import format_minor_units
from irimi.decide import policy, reader
from irimi.decide.overlay import ServiceOverlay
from irimi.protocol.headers import ANSWERED_BY_HEADER
from irimi.protocol.trace import RunRecord
from irimi.report.model import RunView, for_summary
from irimi.report.summary import Live, summary_lines
from tests.support.engine import engine_config, read_via_proxy, start_engine
from tests.support.maps import PRECONDITION_STRIPE_MAP, map_index
from tests.support.scenarios import (
    BASE_URL_CHILD,
    CHARGE_ID,
    REFUND_AMOUNT,
    REFUND_AMOUNT_MINOR,
    REFUND_CHARGE,
    REFUND_SUMMARY,
    StandInReader,
    demo,
    run_refund_scenario,
)
from tests.support.upstreams import FakeStripe, precondition_stripe, serve

pytestmark = pytest.mark.e2e


def test_the_quickstart_agent_refunds_reads_back_and_is_refused_and_stripe_hears_no_write(
    home, tmp_path, capfd
):
    """The agent under `irimi shadow` sees its refund on both later reads and its retry refused.

    The agent runs with stripe-python against the fake Stripe, and no write reaches it."""
    code, internet = demo().run("quickstart", tmp_path)
    out = capfd.readouterr().out
    assert code == 0, out
    assert internet.writes() == []

    # What the agent saw: its refund, in the list and on the charge, and the retry refused.
    charge = demo().CHARGE
    said = [line for line in out.splitlines() if line.startswith(("charge ", "refund", "retry "))]
    assert len(said) == 5, out
    refund_id = said[1].split()[1].rstrip(":")
    assert refund_id.startswith("re_")
    assert said == [
        f"charge {charge}: 49.00 USD left to refund",
        f"refund {refund_id}: 49.00 USD",
        f"refunds on {charge}: {refund_id}",
        f"charge {charge}: 49.00 USD refunded",
        "retry refused: charge_already_refunded",
    ]

    # And what irimi says of it: both refunds faked, the second refused, the reads overlaid.
    lines = out.splitlines()
    write = lines.index(
        f"  ○ stripe  refund $49.00 on {charge}  faked; checked against live state: would succeed"
    )
    assert lines[write + 1 : write + 4] == [
        "    ↳ GET /v1/refunds saw it  overlay",
        f"    ↳ GET /v1/charges/{charge} saw it  overlay",
        f"  ✗ stripe  refund $49.00 on {charge}  faked; would fail: charge_already_refunded",
    ]
    # The run is the agent's `@sdk.trigger`, ready to replay.
    assert any(
        line.startswith("  run ") and "examples.quickstart.agent:refund_one_charge()" in line
        for line in lines
    ), out


def test_a_refund_through_the_door_is_answered_by_irimi_and_never_leaves_the_machine(
    home, tmp_path, stripe_stand_in, capfd, monkeypatch
):
    """In base-URL mode the read is live and the refund is a parseable fake that never leaves."""
    # `cli.compose.build_engine` looks the reader up when it runs, so it builds this one.
    monkeypatch.setattr("irimi.decide.reader.UpstreamReader", StandInReader)
    StandInReader.port = stripe_stand_in
    child = tmp_path / "child.py"
    child.write_text(
        BASE_URL_CHILD.replace("__CHARGES_PORT__", str(stripe_stand_in))
        .replace("__CHARGE__", CHARGE_ID)
        .replace("__AMOUNT__", str(REFUND_AMOUNT_MINOR))
    )
    argv = ["shadow", "--port", "0", "--", sys.executable, str(child)]
    assert main(argv) == 0
    out = capfd.readouterr().out

    # 1. The read is live: it reached the stub and carries no `Irimi-Answered-By`. The second
    #    GET is irimi's check read of the charge, made before it faked the refund.
    assert stripe_stand_in.seen == [
        ("GET", "/v1/charges"),
        ("GET", f"/v1/charges/{CHARGE_ID}"),
    ]
    read = next(line for line in out.splitlines() if line.startswith("READ "))
    assert read.split(" ", 3)[:3] == ["READ", "200", "None"]
    assert CHARGE_ID in read

    # 2. The write is answered by irimi, and says so on the wire.
    write = next(line for line in out.splitlines() if line.startswith("WRITE "))
    _, status, stamp, payload = write.split(" ", 3)
    assert status == "200"
    assert stamp == "fake-L1"

    # 3. The answer is a whole refund object an SDK can parse, including fields the caller never
    #    sent, which an agent may branch on.
    body = json.loads(payload)
    assert body["object"] == "refund"
    assert body["id"].startswith("re_")
    assert body["balance_transaction"].startswith("txn_")
    assert body["charge"] == CHARGE_ID
    assert body["amount"] == REFUND_AMOUNT_MINOR
    assert body["status"] == "succeeded"
    assert body["currency"] == "usd"
    assert "livemode" not in body  # the real refund object has no such field

    # 4. Nothing on the other side was asked to do anything.
    assert [(m, p) for m, p in stripe_stand_in.seen if m != "GET"] == []

    # 5. The summary says so in the map's words. The amount prints as `$1.00` because irimi's
    #    check read of the charge carries its `usd` onto the write; the child posts no currency.
    assert (
        f"  ○ stripe  refund {format_minor_units(REFUND_AMOUNT_MINOR, 'usd')} on {CHARGE_ID}  "
        "faked; checked against live state: would succeed" in out
    )
    assert "  None of these writes reached the real service:" in out
    assert "1 read" in out

    # 6. irimi's check read is counted apart from the agent's reads, but in the `real` total.
    #    The agent's own read went to the stub's address, so Stripe's line holds only the check.
    host = next(line for line in out.splitlines() if line.startswith("  api.stripe.com  "))
    assert host == "  api.stripe.com  1 irimi check read  1 faked write"
    assert "  3 exchanges · 2 real · 1 answered by irimi (1 faked write) · 0 sent to targets" in out


@pytest.fixture
def refund_engine(tmp_path, monkeypatch):
    """Yield (proxy port, stub, exchanges, maps) for an engine built as `build_engine` builds one.

    It has the real overlay and the real reader: through the proxy, the check read goes where the
    agent's reads go."""
    stripe = serve(FakeStripe, threaded=True, **precondition_stripe())
    maps = map_index(tmp_path, monkeypatch, doc=PRECONDITION_STRIPE_MAP)
    eng, seen, stop = start_engine(
        engine_config(tmp_path, monkeypatch, maps=maps),
        overlay=ServiceOverlay(),
        policy=policy.ShadowPolicy(reader=reader.UpstreamReader(), maps=maps),
    )
    yield eng.listen_port(), stripe, seen, maps
    stop()
    stripe.stop()


def _stripe_refund(proxy, stub):
    """Send `stripe.Refund.create(charge=, amount=)` as stripe-python does, with a fresh key.

    With a new `Idempotency-Key` on each call, the precondition check must refuse a second refund;
    the idempotency store cannot answer it."""
    return read_via_proxy(
        proxy,
        f"http://127.0.0.1:{stub}/v1/refunds",
        extra_headers={
            "content-type": "application/x-www-form-urlencoded",
            "idempotency-key": str(uuid.uuid4()),
        },
        method="POST",
        body=f"charge={REFUND_CHARGE}&amount={REFUND_AMOUNT}".encode(),
    )


def test_the_refund_agents_four_calls_through_the_forward_proxy_are_answered_shadow_mode_correctly(
    refund_engine,
):
    """Through the proxy, the agent's read, refund, list, re-read and retry each get their answer.

    The calls come from this process in stripe-python's wire shape; the next test runs them under
    `irimi shadow`. The refund is for the charge's full amount, so the retry is refused."""
    proxy, stub, seen, maps = refund_engine
    base = f"http://127.0.0.1:{stub}"

    # 1. The agent's first read is live: it reached the stub and carries no stamp.
    status, headers, _ = read_via_proxy(proxy, f"{base}/v1/charges")
    assert status == 200
    assert ANSWERED_BY_HEADER not in headers

    # 2. The refund is answered by irimi with a whole refund object an SDK can parse.
    status, headers, data = _stripe_refund(proxy, stub)
    assert status == 200
    assert headers[ANSWERED_BY_HEADER] == "fake-L1"
    refund = json.loads(data)
    assert refund["object"] == "refund"
    assert refund["id"].startswith("re_")
    assert refund["balance_transaction"].startswith("txn_")
    assert (refund["charge"], refund["amount"]) == (REFUND_CHARGE, REFUND_AMOUNT)

    # 3. The refunds list, filtered by `charge` and `limit`, shows the refund: the overlay
    #    applies both parameters.
    status, headers, data = read_via_proxy(
        proxy, f"{base}/v1/refunds?charge={REFUND_CHARGE}&limit=10"
    )
    assert status == 200
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    assert [r["id"] for r in json.loads(data)["data"]] == [refund["id"]]

    # 4. The charge re-read is shown refunded, though the real charge is untouched.
    status, headers, data = read_via_proxy(proxy, f"{base}/v1/charges/{REFUND_CHARGE}")
    assert status == 200
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    charge = json.loads(data)
    assert charge["amount_refunded"] == REFUND_AMOUNT
    assert charge["refunded"] is True

    # 5. The precondition check refuses the retry in Stripe's words: it carries its own key, and
    #    only the run's write log knows the first refund.
    status, headers, data = _stripe_refund(proxy, stub)
    assert status == 400
    assert headers[ANSWERED_BY_HEADER] == "fake-L1"
    assert json.loads(data)["error"]["code"] == "charge_already_refunded"

    # Each refund was checked against the service first, by an irimi check read.
    issued = [ex for ex in seen if ex.issued_by == "engine"]
    assert [(ex.kind, ex.request.method, ex.request.path) for ex in issued] == [
        ("read", "GET", f"/v1/charges/{REFUND_CHARGE}"),
    ] * 2
    writes = [ex for ex in seen if ex.kind == "write"]
    assert [ex.precondition.outcome for ex in writes] == ["passed", "rejected"]
    assert writes[1].precondition.rejection_code == "charge_already_refunded"
    # The refused refund would fire nothing, and the summary's closing clause is built from this.
    assert [ex.would_fire for ex in writes] == [("refund.created", "charge.refunded"), ()]

    # Nothing on the other side was ever asked to do anything.
    assert [m for m, _ in stub.seen if m != "GET"] == []

    # The summary, line by line.
    run_id = seen[0].run_id
    view = RunView(RunRecord.header(run_id), [*map(for_summary, seen)])
    assert summary_lines(view, maps, 1.0, Live()) == [
        f"irimi shadow · run {run_id} · 7 exchanges · 1.0s",
        *REFUND_SUMMARY,
    ]


def test_the_same_five_calls_under_irimi_shadow_print_the_refund_summary(
    home, tmp_path, capfd, monkeypatch
):
    """`irimi shadow` builds the engine the previous test builds and prints the same summary.

    Only the maps directory is swapped, so the loopback stub is the `stripe` service. The child
    names the proxy explicitly, for the `NO_PROXY` reason `tests/support/scenarios.py` gives."""
    run_refund_scenario(tmp_path, monkeypatch)
    lines = capfd.readouterr().out.splitlines()

    assert [line for line in lines if line.startswith("CALL ")] == [
        "CALL GET /v1/charges 200 None",
        "CALL POST /v1/refunds 200 fake-L1",
        f"CALL GET /v1/refunds?charge={REFUND_CHARGE}&limit=10 200 overlay",
        f"CALL GET /v1/charges/{REFUND_CHARGE} 200 overlay",
        "CALL POST /v1/refunds 400 fake-L1",
    ]
    # The banner opens `irimi shadow · run ...` too; the summary's header is the one with a count.
    header = next(i for i, line in enumerate(lines) if " · 7 exchanges · " in line)
    assert lines[header].startswith("irimi shadow · run ")
    assert lines[header + 1 : header + 1 + len(REFUND_SUMMARY)] == REFUND_SUMMARY
