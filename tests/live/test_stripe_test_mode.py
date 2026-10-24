"""The quickstart agent against Stripe test mode: no refund reaches the real ledger.

Deselected by default; run it with `uv run pytest -m live`.
"""

import os
import re
import sys
from pathlib import Path

import pytest

from irimi.cli import main
from irimi.core.money import format_minor_units

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[2]
# The lines the agent prints, in order, and what each one names.
CHARGE_LINE = re.compile(r"charge (ch_\w+): [\d.]+ \w+ left to refund")
REFUND_LINE = re.compile(r"refund (re_\w+): ")
LISTED_LINE = re.compile(r"refunds on (ch_\w+): (.*)")
REREAD_LINE = re.compile(r"charge (ch_\w+): ([\d.]+) \w+ refunded")
RETRY_LINE = re.compile(r"retry (refused: \w+|accepted)")


def _said(pattern: re.Pattern[str], out: str) -> re.Match[str]:
    found = next((m for m in map(pattern.fullmatch, out.splitlines()) if m), None)
    assert found is not None, f"the agent printed no line like {pattern.pattern!r}:\n{out}"
    return found


@pytest.mark.live
@pytest.mark.skipif(
    not os.environ.get("STRIPE_API_KEY"),
    reason="live check: set STRIPE_API_KEY to a sk_test_ key and seed a charge",
)
def test_the_quickstart_agent_leaves_no_refund_on_a_real_test_mode_charge(home, capfd, monkeypatch):
    """The quickstart agent's refund, re-read and refused retry leave the test-mode charge as is.

    Every answer the agent sees is irimi's; a live re-read afterwards shows none of it happened.

        uv sync --group examples && uv run python -m examples.seed_stripe
        STRIPE_API_KEY=sk_test_... uv run pytest -m live -q -rs tests/live
    """
    stripe = pytest.importorskip("stripe", reason="the live check needs the stripe SDK")
    stripe.api_key = os.environ["STRIPE_API_KEY"]
    assert stripe.api_key.startswith("sk_test_"), "test mode only"
    monkeypatch.setenv("PYTHONPATH", str(ROOT))

    code = main(["shadow", "--", sys.executable, "-m", "examples.quickstart.agent"])
    out, err = capfd.readouterr()
    if "no refundable charge" in err:
        pytest.skip("no refundable charge: run `python -m examples.seed_stripe` first")
    assert code == 0, out + err

    # What the agent saw: its refund in the list and the charge refunded in full, both overlaid,
    # and the retry refused by the precondition check, because of a refund only irimi knows of.
    charge_id = _said(CHARGE_LINE, out).group(1)
    refund_id = _said(REFUND_LINE, out).group(1)
    assert _said(LISTED_LINE, out).groups() == (charge_id, refund_id)
    assert _said(RETRY_LINE, out).group(1) == "refused: charge_already_refunded"

    # What Stripe holds - the assertion that matters: none of it happened.
    charge = stripe.Charge.retrieve(charge_id)
    assert charge.amount_refunded == 0, "a refund reached Stripe"
    assert charge.refunded is False
    assert list(stripe.Refund.list(charge=charge_id).data) == []
    # The agent saw it refunded in full, though.
    assert _said(REREAD_LINE, out).group(2) == f"{charge.amount / 100:.2f}"

    # The summary says so, the amount formatted in the currency irimi's check read found.
    money = format_minor_units(charge.amount, charge.currency)
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
