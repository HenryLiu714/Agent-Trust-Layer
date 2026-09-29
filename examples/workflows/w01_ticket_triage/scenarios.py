"""W1's scenarios: one ticket each, a different system prompt each.

The scripted model reads the system prompt and the tool results it has been given so far, and
nothing else, so the same prompt always walks the same loop and a changed prompt changes it.
"""

from __future__ import annotations

import json
from typing import Any

from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import LlmCall, LlmTurn, World

CHARGE = "ch_TICKET1"
CUSTOMER = "cus_TICKET1"
AMOUNT = 4900
ARGV = ("T-1001", CHARGE, CUSTOMER, "I was charged twice for the same order")


def _results(call: LlmCall) -> list[dict[str, Any]]:
    out = []
    for text in call.tool_results():
        try:
            doc = json.loads(text)
        except ValueError:
            continue
        if isinstance(doc, dict):
            out.append(doc)
    return out


def triage_script(call: LlmCall) -> LlmTurn:
    """The model: look the charge and customer up, refund what the prompt says, reply, stop."""
    ticket = json.loads(call.messages[0]["content"])
    results = _results(call)
    if "Keep checking" in call.system:
        return LlmTurn(tool_calls=(("get_charge", {"charge": ticket["charge"]}),))
    charges = [r["body"] for r in results if r["tool"] == "get_charge"]
    if not charges:
        return LlmTurn(
            text="Let me look into this.",
            tool_calls=(
                ("get_charge", {"charge": ticket["charge"]}),
                ("lookup_customer", {"customer": ticket["customer"]}),
            ),
        )
    charge = charges[-1]
    left = charge["amount"] - charge["amount_refunded"]
    if "half" in call.system:
        amount = left // 2
    elif "double the charge" in call.system:
        amount = charge["amount"] * 2
    else:
        amount = left
    wanted = 2 if "refund again" in call.system else 1
    refunds = [r for r in results if r["tool"] == "issue_refund"]
    if len(refunds) < wanted:
        return LlmTurn(tool_calls=(("issue_refund", {"charge": charge["id"], "amount": amount}),))
    if not any(r["tool"] == "send_reply" for r in results):
        statuses = ", ".join(str(r["status"]) for r in refunds)
        body = f"Refund of {amount} requested (statuses: {statuses})."
        return LlmTurn(tool_calls=(("send_reply", {"body": body}),))
    return LlmTurn(text="Ticket resolved.")


def _world(world: World) -> None:
    world.stripe.add_charge(CHARGE, AMOUNT, customer=CUSTOMER)
    world.stripe.add_customer(CUSTOMER)
    world.llm.script = triage_script


def _scenario(prompt: str, doc: str, **kw: Any) -> Scenario:
    return Scenario(ARGV, env={"PROMPT": prompt}, setup=_world, doc=doc, **kw)


WORKFLOW = Workflow(
    name="w01_ticket_triage",
    summary="A helpdesk webhook answered by a multi-turn Anthropic tool_use loop: a Stripe read "
    "and write, a SQLite read tool and a SQLite write tool. The prompt variant decides the refund.",
    scenarios={
        "full_refund": _scenario("full", "refund the whole charge, reply, stop"),
        "half_refund": _scenario("half", "the prompt edit: refund half instead"),
        "too_large": _scenario("too_large", "the model asks for twice the charge"),
        "double_refund": _scenario("double", "the model refunds twice with different keys"),
        "runaway_loop": _scenario("runaway", "the model never stops; the agent gives up at turn 6"),
    },
)
