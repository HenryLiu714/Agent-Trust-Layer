"""The refund agent against Stripe test mode, which proves that no refund reached the real
ledger. Deselected by default; run it with `uv run pytest -m live`."""

import os
import sys
from pathlib import Path

import pytest

from irimi.cli import main
from irimi.core.money import format_minor_units

pytestmark = pytest.mark.e2e


AGENT_PATH = Path(__file__).resolve().parents[2] / "examples" / "refund_agent" / "agent.py"


@pytest.mark.live
@pytest.mark.skipif(
    not os.environ.get("STRIPE_API_KEY"),
    reason="live phase-exit check: set STRIPE_API_KEY to a sk_test_ key and seed a charge",
)
def test_the_refund_agent_leaves_no_refund_on_a_real_test_mode_charge(home, capfd):
    """The live half of both criteria, run by hand (CONTRIBUTING.md). The refund agent itself,
    through the reverse door, against Stripe test mode: it refunds a charge in full, finds its
    refund in the list, re-reads the charge refunded, retries and is refused - every answer
    irimi's - and a live re-read of the charge afterwards shows none of it happened.

        uv sync --group examples && uv run python examples/refund_agent/seed.py
        STRIPE_API_KEY=sk_test_... uv run pytest -m live -q -rs tests/live

    With SLACK_BOT_TOKEN and SLACK_CHANNEL set the agent also posts and reads the channel back,
    and the post must be in what it read. SLACK_CHANNEL has to be a channel id for that: a post
    to `#general` is read back from a channel irimi cannot match to it.
    """
    stripe = pytest.importorskip("stripe", reason="live phase-exit check needs the stripe SDK")
    stripe.api_key = os.environ["STRIPE_API_KEY"]
    assert stripe.api_key.startswith("sk_test_"), "test mode only"

    code = main(["shadow", "--port", "0", "--", sys.executable, str(AGENT_PATH)])
    out = capfd.readouterr().out
    if code == 3:
        pytest.skip("no refundable charge: run `python examples/refund_agent/seed.py` first")
    assert code == 0, out

    # The agent's whole contract is this one line, so nothing here parses its prose.
    line = next((row for row in out.splitlines() if row.startswith("AGENT-RESULT ")), None)
    assert line is not None, out
    result = dict(pair.split("=", 1) for pair in line.split()[1:])
    assert set(result) == {
        "charge",
        "refund",
        "amount",
        "currency",
        "listed",
        "refunded",
        "retry",
    }, line
    charge_id, amount = result["charge"], int(result["amount"])
    # The agent prints the charge's currency, because irimi formats the amount in the summary from
    # the code its own precondition read found on that charge - so the expected line cannot be
    # built from the amount alone.
    money = format_minor_units(amount, result["currency"])
    assert result["refund"].startswith("re_"), "the agent did not get a parseable refund id"
    # What the agent saw: its refund in the list and the charge refunded in full, both overlaid,
    # and the retry refused by L3 because of a refund that exists only in irimi's write log.
    assert result["listed"] == "yes", line
    assert int(result["refunded"]) == amount, line
    assert result["retry"] == "charge_already_refunded", line

    # What Stripe holds - the assertion that matters: none of it happened.
    charge = stripe.Charge.retrieve(charge_id)
    assert charge.amount_refunded == 0, "a refund reached Stripe"
    assert charge.refunded is False
    assert list(stripe.Refund.list(charge=charge_id).data) == []

    # The summary says so. Through the door every Stripe call is on `api.stripe.com`, the agent's
    # reads and irimi's two precondition reads alike.
    lines = out.splitlines()
    assert (
        "  api.stripe.com  3 reads (2 showing this run's writes)  2 irimi check reads  "
        "2 faked writes"
    ) in lines
    write = lines.index(
        f"  ○ stripe  refund {money} on {charge_id}  faked; checked against live state: "
        "would succeed"
    )
    assert lines[write + 1 : write + 4] == [
        "    ↳ GET /v1/refunds saw it  overlay",
        f"    ↳ GET /v1/charges/{charge_id} saw it  overlay",
        f"  ✗ stripe  refund {money} on {charge_id}  faked; would fail: charge_already_refunded",
    ]
    assert "  would have fired: refund.created, charge.refunded" in lines

    # Slack is optional. Its read-back is asserted to show the post - not to be `overlay: full`,
    # which only a channel id makes it.
    if os.environ.get("SLACK_BOT_TOKEN") and os.environ.get("SLACK_CHANNEL"):
        history = next((row for row in lines if row.startswith("slack: history on ")), None)
        assert history is not None, out
        assert " shows " in history, history
