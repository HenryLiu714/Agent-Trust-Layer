"""W5's scenarios: two disputes on two charges, one over the refund threshold and one under."""

from __future__ import annotations

from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import LlmCall, LlmTurn, World
from examples.workflows.w05_dispute_responder.agent import BIG, DISPUTES_CHANNEL, SMALL

CUSTOMER = "cus_DISPUTER"


def _evidence(call: LlmCall) -> LlmTurn:
    return LlmTurn(text="The customer received the goods. Delivery was confirmed on file.")


def _world(world: World) -> None:
    world.stripe.add_customer(CUSTOMER)
    world.stripe.add_charge("ch_BIG", 5000, customer=CUSTOMER)
    world.stripe.add_charge("ch_SMALL", 1500, customer=CUSTOMER)
    # Each charge's intent, as Stripe returns it: with its `client_secret` (#70).
    for intent, amount, order in (("pi_BIG", 5000, "1042"), ("pi_SMALL", 1500, "1043")):
        world.stripe.add_payment_intent(
            intent, amount, status="succeeded", description=f"Order #{order}: headphones"
        )
    world.stripe.add_dispute(BIG, "ch_BIG", 5000, "product_not_received", "pi_BIG")
    world.stripe.add_dispute(SMALL, "ch_SMALL", 1500, "fraudulent", "pi_SMALL")
    world.slack.add_channel(DISPUTES_CHANNEL, "disputes")
    world.llm.script = _evidence


WORKFLOW = Workflow(
    name="w05_dispute_responder",
    summary="A signed Stripe webhook consumer: raw bytes as the trigger, reads on an unmapped "
    "route, customer tagging, a Slack alert, a small-dispute refund and its would-be webhook.",
    scenarios={
        name: Scenario((name,), setup=_world, doc=doc)
        for name, doc in {
            "over_threshold": "a $50 dispute: tag, alert, fight; no refund",
            "under_threshold": "a $15 dispute: tag, alert, refund; the refund would have fired "
            "refund.created and charge.refunded",
            "cascade": "the refund's own charge.refunded delivered back: a second run, no writes",
            "bad_signature": "a forged signature: 400 and no outbound calls at all",
            "replayed_event": "the same event id twice: one set of writes",
        }.items()
    },
)
