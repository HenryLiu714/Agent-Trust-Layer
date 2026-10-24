"""W1's scenarios: one ticket each, with a different system prompt each.

The scripted model reads only the prompt and tool results, so a new prompt changes its loop."""

from __future__ import annotations

import json
from typing import Any

from tests.e2e.workflows.harness.scenario import Scenario, Workflow
from tests.e2e.workflows.harness.services import LlmCall, LlmTurn, World

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
    # "only then look the customer up": not before the refund, but beside the second read.
    lookup = ("lookup_customer", {"customer": ticket["customer"]})
    late = "only then look the customer up" in call.system
    if not charges:
        first = ("get_charge", {"charge": ticket["charge"]})
        return LlmTurn(
            text="Let me look into this.", tool_calls=(first,) if late else (first, lookup)
        )
    # The first read decides the refund; a second, which `verify` asks for, only confirms it.
    charge = charges[0]
    left = charge["amount"] - charge["amount_refunded"]
    if "half" in call.system:
        amount = left // 2
    elif "double the charge" in call.system:
        amount = charge["amount"] * 2
    else:
        amount = left
    wanted = 0 if "Do not refund" in call.system else 2 if "refund again" in call.system else 1
    refunds = [r for r in results if r["tool"] == "issue_refund"]
    if len(refunds) < wanted:
        refund = ("issue_refund", {"charge": charge["id"], "amount": amount})
        # "send the refund twice": the same call twice in one turn, as a model repeats itself.
        return LlmTurn(tool_calls=(refund, refund) if "twice" in call.system else (refund,))
    if "check the charge again" in call.system and len(charges) < 2:
        again = ("get_charge", {"charge": charge["id"]})
        return LlmTurn(tool_calls=(lookup, again) if late else (again,))
    if not any(r["tool"] == "send_reply" for r in results):
        statuses = ", ".join(str(r["status"]) for r in refunds)
        body = f"Refund of {amount} requested (statuses: {statuses})."
        if not wanted:
            body = f"No refund issued; {charges[-1]['amount_refunded']} already refunded."
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
        "double_refund_quiet": _scenario(
            "double",
            "double_refund under --quiet: the banner and the summary, and no line per exchange",
            shadow_flags=("--quiet",),
            serve_flags=("--quiet",),
        ),
        "retention_mid_run": Scenario(
            ARGV,
            env={"PROMPT": "full", "HELPDESK_RETENTION_MID_RUN": "1"},
            setup=_world,
            doc="full_refund, which asks irimi to delete its own run, the newest run and old runs "
            "while it is still in progress: refused, and nothing is deleted",
        ),
        "retried_refund": _scenario(
            "retry", "the model sends one refund twice in a turn, same key: one write"
        ),
        "runaway_loop": _scenario("runaway", "the model never stops; the agent gives up at turn 6"),
        "verified_refund": _scenario(
            "verify",
            "refund the whole charge, then read the charge again, which the overlay answers with "
            "the refund in it; replayed with `PROMPT=verify_only`, which refunds nothing, "
            "that recorded read shows a refund the replay never made",
        ),
        "checked_charge": _scenario(
            "verify_only",
            "refund nothing and read the charge twice; replayed with `PROMPT=verify`, which "
            "refunds, the second read is not answered from the recording: it goes live, overlaid",
        ),
        "late_lookup": _scenario(
            "late_lookup",
            "refund the whole charge, and only then look the customer up and read the charge "
            "again; replayed with `PROMPT=verify`, which looks the customer up before the refund, "
            "that recorded lookup reflects a refund the replay has not made yet: the "
            "tool runs for real, and the replay goes on answering from the recording",
        ),
    },
)
