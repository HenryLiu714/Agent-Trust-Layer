"""W2's scenarios: the size and shape of one night's reconciliation."""

from __future__ import annotations

from collections.abc import Callable

from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import World
from examples.workflows.w02_nightly_reconcile.agent import CHANNEL, charge_id, customer_id

PRIOR_REFUND = "re_PRIOR000"


def _seed(charges: int, prior_refund: bool = False) -> Callable[[World], None]:
    def setup(world: World) -> None:
        for i in range(charges):
            # Distinct `created` so Stripe's newest-first order, and so every page, is fixed.
            world.stripe.add_charge(
                charge_id(i), 1000 + i, customer=customer_id(i), created=1_790_000_000 + i
            )
            world.stripe.add_customer(customer_id(i))
        world.slack.add_channel(CHANNEL, "finance-ops")
        if prior_refund:
            # A real refund already on the charge, older than anything this run makes.
            world.stripe.refunds.append(
                {
                    "id": PRIOR_REFUND,
                    "object": "refund",
                    "amount": 50,
                    "charge": charge_id(0),
                    "currency": "usd",
                    "status": "succeeded",
                    "created": 1_789_000_000,
                    "metadata": {},
                }
            )
            world.stripe.charges[charge_id(0)]["amount_refunded"] = 50

    return setup


WORKFLOW = Workflow(
    name="w02_nightly_reconcile",
    summary="A scheduled batch: page every charge, diff a SQLite ledger, tag each mismatch in "
    "Stripe and the ledger, post one Slack summary.",
    scenarios={
        "clean": Scenario(
            ("--charges", "12"), setup=_seed(12), doc="two pages, nothing to reconcile"
        ),
        "forty_mismatches": Scenario(
            ("--charges", "45", "--mismatches", "40"),
            setup=_seed(45),
            doc="five pages, forty customer updates and forty write-tool calls in one run",
        ),
        "datetime_trigger": Scenario(
            ("--charges", "12", "--mismatches", "2", "--datetime-trigger"),
            setup=_seed(12),
            doc="the trigger is a datetime, which #74 captures as non-replayable",
        ),
        "same_date_twice": Scenario(
            ("--charges", "12", "--mismatches", "3", "--twice"),
            setup=_seed(12),
            doc="the same date reconciled twice in one run: duplicate writes",
        ),
        "refund_then_page": Scenario(
            ("--charges", "3", "--refund-then-page"),
            setup=_seed(3, prior_refund=True),
            doc="page refunds with starting_after naming a refund this run minted (#53)",
        ),
    },
)
