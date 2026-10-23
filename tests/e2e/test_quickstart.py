"""The quickstart refund agent end to end: the agent itself under `irimi shadow`, and its calls
through base-URL mode and through the forward proxy, each refund answered by irimi and none
reaching Stripe."""

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
    """The agent itself, stripe-python and all, under the real `irimi shadow` against the fake
    Stripe: what it prints is what it was shown, a refund that the two reads after it see and a
    retry Stripe would refuse, and no write reached Stripe."""
    code, internet = demo().run("quickstart", tmp_path)
    out = capfd.readouterr().out
    assert code == 0, out
    assert internet.writes() == []

    # What the agent saw, from its own lines: its refund, in the list and on the charge, and the
    # retry refused.
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
    """The base-URL criterion. One run of the real CLI: a live read that reaches the world,
    a refund that does not, an echo the caller can parse, and a summary that says so."""
    # `cli.compose.build_engine` looks the reader up when it runs, so this is what it builds.
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

    # 1. The read was real: it reached the only server there is, came back from it, and carries
    #    no `Irimi-Answered-By` - absence is how a live forward says it was not ours. The
    #    second GET is irimi's own precondition read of the charge, made before it faked the
    #    refund - and it reaching this stand-in is the proof it did not reach Stripe.
    assert stripe_stand_in.seen == [
        ("GET", "/v1/charges"),
        ("GET", f"/v1/charges/{CHARGE_ID}"),
    ]
    read = next(line for line in out.splitlines() if line.startswith("READ "))
    assert read.split(" ", 3)[:3] == ["READ", "200", "None"]
    assert CHARGE_ID in read

    # 2. The write was answered by irimi, and says so on the wire.
    write = next(line for line in out.splitlines() if line.startswith("WRITE "))
    _, status, stamp, payload = write.split(" ", 3)
    assert status == "200"
    assert stamp == "fake-L1"

    # 3. The answer is something an SDK can read: the id it minted, the object, and the fields
    #    the caller sent back unchanged. `stripe.Refund.create(...).id` is this assertion in the
    #    SDK. It is a whole refund object, so the fields the caller never sent - the ones an
    #    agent branches on - are there too.
    body = json.loads(payload)
    assert body["object"] == "refund"
    assert body["id"].startswith("re_")
    assert body["balance_transaction"].startswith("txn_")
    assert body["charge"] == CHARGE_ID
    assert body["amount"] == REFUND_AMOUNT_MINOR
    assert body["status"] == "succeeded"
    assert body["currency"] == "usd"
    assert "livemode" not in body  # the real refund object has no such field

    # 4. And it never left: nothing on the other side was ever asked to do anything.
    assert [(m, p) for m, p in stripe_stand_in.seen if m != "GET"] == []

    # 5. The summary says all of it, in the map's own words.
    # `$1.00`, not `100`: the child posts no currency, and the stand-in's charge is in `usd`, which
    # irimi's own precondition read of it carried onto the write's exchange.
    assert (
        f"  ○ stripe  refund {format_minor_units(REFUND_AMOUNT_MINOR, 'usd')} on {CHARGE_ID}  "
        "faked; checked against live state: would succeed" in out
    )
    assert "  None of these writes reached the real service:" in out
    assert "1 read" in out

    # 6. And irimi checked with Stripe before answering. The charge GET in (1) is a read
    #    irimi issued on its own account, and it is recorded `issued_by: engine` so that it can be
    #    counted apart from the agent's: "reads are real" means the reads the AGENT made. It did
    #    reach the service, so the run's `real` count holds it beside the agent's own read. The
    #    agent's read went to the stand-in's own address, so Stripe's line holds no read of the
    #    agent's at all: irimi's check read is not counted as one.
    host = next(line for line in out.splitlines() if line.startswith("  api.stripe.com  "))
    assert host == "  api.stripe.com  1 irimi check read  1 faked write"
    assert "  3 exchanges · 2 real · 1 answered by irimi (1 faked write) · 0 sent to targets" in out


@pytest.fixture
def refund_engine(tmp_path, monkeypatch):
    """The shared loopback Stripe, and an engine over it built the way
    `cli.compose.build_engine` builds one: the real overlay, and a policy holding the real reader.
    Nothing is swapped for a stand-in - through the forward proxy the precondition read goes where
    the agent's reads go.
    Yields (proxy port, stub port, exchanges, maps)."""
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
    """`stripe.Refund.create(charge=, amount=)` on the wire: a form body, and a fresh
    `Idempotency-Key` on every call, because stripe-python defaults one per POST. A retry is
    therefore a new request to the idempotency store, and it is L3 that must refuse it - a shared
    key would have the store replay the first answer."""
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
    """The forward-proxy criterion. The refund agent's first read and then its four calls -
    refund the charge in full, list its refunds, re-read the charge, retry the refund - through the
    forward proxy, with no key, no network and no SDK.

    The calls are made from this process with `_read_via_proxy`, not from a child under `irimi
    shadow`: the claim is about what the proxy answers each call with, `irimi shadow`'s spawning of
    a child is already the base-URL test's subject, and a second subprocess would buy nothing but a
    layer of stdout to parse. The wire shape is stripe-python's all the same.

    The refund is for the charge's FULL amount, as the agent's is. A partial refund leaves the
    charge refundable, and the retry would be accepted rather than refused.
    """
    proxy, stub, seen, maps = refund_engine
    base = f"http://127.0.0.1:{stub}"

    # 1. The agent's first read is real: it reached the stub and carries no stamp.
    status, headers, _ = read_via_proxy(proxy, f"{base}/v1/charges")
    assert status == 200
    assert ANSWERED_BY_HEADER not in headers

    # 2. The refund is irimi's, and a whole refund object an SDK can parse.
    status, headers, data = _stripe_refund(proxy, stub)
    assert status == 200
    assert headers[ANSWERED_BY_HEADER] == "fake-L1"
    refund = json.loads(data)
    assert refund["object"] == "refund"
    assert refund["id"].startswith("re_")
    assert refund["balance_transaction"].startswith("txn_")
    assert (refund["charge"], refund["amount"]) == (REFUND_CHARGE, REFUND_AMOUNT)

    # 3. The agent's refunds list, filtered as an agent filters it, is shown the refund in full:
    #    `charge` and `limit` are both parameters the overlay knows how to apply.
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

    # 5. The retry is refused in Stripe's own words, by L3 and not by the store: it carries its own
    #    key, and what makes it refusable is the first refund, which only irimi's write log knows.
    status, headers, data = _stripe_refund(proxy, stub)
    assert status == 400
    assert headers[ANSWERED_BY_HEADER] == "fake-L1"
    assert json.loads(data)["error"]["code"] == "charge_already_refunded"

    # Each refund was checked with the real service first, by a read irimi issued on its own
    # account and counted apart from the agent's.
    issued = [ex for ex in seen if ex.issued_by == "engine"]
    assert [(ex.kind, ex.request.method, ex.request.path) for ex in issued] == [
        ("read", "GET", f"/v1/charges/{REFUND_CHARGE}"),
    ] * 2
    writes = [ex for ex in seen if ex.kind == "write"]
    assert [ex.precondition.outcome for ex in writes] == ["passed", "rejected"]
    assert writes[1].precondition.rejection_code == "charge_already_refunded"
    # On the exchange, not only in the summary line: the refused refund would have fired nothing,
    # and the closing clause must be built from exactly this.
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
    """The criterion's twin through the real CLI. The criterion builds its engine the way
    `cli.compose.build_engine` builds one; this proves that is the engine `irimi shadow` really
    builds - the real overlay, a policy holding the real reader - and that the block it prints on
    exit is the one the criterion pins. Nothing is swapped but where the shipped maps are read
    from: a directory holding the loopback Stripe's map, so the stub's host is the `stripe`
    service. The child uses the forward proxy explicitly, because `irimi shadow` exempts loopback
    from it through `NO_PROXY` and the stub is on loopback."""
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
