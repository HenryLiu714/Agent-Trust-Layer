"""W8 `orchestrator`: one run that fans out to sub-agents and to an internal service.

`plan_quarter_close()` is the trigger. It calls two sub-agents in-process, each an `@sdk.trigger`
of its own, which must join the parent run rather than start one:

- `inventory_agent()` asks the internal service `subagent.internal` for stock (`GET /inventory`)
  and for a price (`POST /quote`: a read spelled as a POST, it prices a basket and changes nothing);
- `finance_agent()` reads Stripe's recent charges to check there is budget.

The orchestrator then reserves the stock (`POST /reservations`, a write) unless it could not price
it. The internal service is the point: irimi has no map for it unless the scenario adds one.

With `--record`, the sub-agent reads its reorder policy and the orchestrator records its decision
in the planning file, each a tool (`@sdk.read_tool` or `@sdk.write_tool`): a tool called inside a
nested trigger is a call of the run that trigger joined, the parent's. With `--nested-reserve`, a
third sub-agent, `reservation_agent()`, makes the reservation: a write made inside a nested
trigger, which is the parent run's own write, as `irimi compare` shows it.

    python -m examples.workflows.w08_orchestrator.agent [--runs N] [--parent-header] [--record]
        [--nested-reserve]
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from examples.workflows import agentkit, sdk
from examples.workflows.agentkit import http

SUBAGENT_HOST = "subagent.internal"
BUDGET_MINOR = 10_000
PLAN_FILE = "decisions.jsonl"
# Set by `--record`: the sub-agent and the orchestrator call their tools.
RECORD = False
# Set by `--nested-reserve`: the reservation is made by `reservation_agent`, a nested trigger.
NESTED_RESERVE = False


@sdk.read_tool
def load_reorder_policy() -> dict[str, Any]:
    """What the sub-agent reads before it plans: the decisions recorded so far, which the bare
    run's real `record_decision` adds to and the shadow run's stand-in never does."""
    path = agentkit.state_dir() / PLAN_FILE
    return {"decisions_so_far": len(path.read_text().splitlines()) if path.exists() else 0}


def _record_stand_in(decision: dict[str, Any]) -> dict[str, Any]:
    return {"recorded": False}


@sdk.write_tool(fake=_record_stand_in)
def record_decision(decision: dict[str, Any]) -> dict[str, Any]:
    with (agentkit.state_dir() / PLAN_FILE).open("a") as fh:
        fh.write(json.dumps(decision) + "\n")
    return {"recorded": True}


def _headers(parent_header: bool) -> dict[str, str]:
    """With `parent_header`, the agent carries its run id to the internal service itself, in a
    header of its own. irimi strips `Irimi-Run` from every request that leaves it, so this
    is the only way the service can learn which run called it."""
    run_id = sdk.current_run_id()
    return {"X-Parent-Run": run_id} if parent_header and run_id else {}


@sdk.trigger(name="inventory_agent")
def inventory_agent(parent_header: bool) -> dict[str, Any]:
    service = agentkit.internal(SUBAGENT_HOST)
    headers = _headers(parent_header)
    stock = http("GET", service + "/inventory", headers=headers, label="inventory").json()
    if RECORD:
        agentkit.obs("policy", **load_reorder_policy())
    items = [i for i in (stock or {}).get("items", []) if i.get("qty", 0) > 0]
    quote = http(
        "POST",
        service + "/quote",
        json_body={"skus": [i["sku"] for i in items]},
        headers=headers,
        label="quote",
    ).json()
    agentkit.obs("quote", doc=quote)
    return {"items": items, "quote": quote if isinstance(quote, dict) else {}}


@sdk.trigger(name="finance_agent")
def finance_agent() -> int:
    """Recent revenue, in minor units: the budget the reservation is checked against."""
    doc = agentkit.stripe("GET", "/v1/charges?limit=10", label="charges").json() or {}
    return sum(c.get("amount", 0) - c.get("amount_refunded", 0) for c in doc.get("data", []))


def reserve(items: list[dict[str, Any]], total: int, parent_header: bool) -> agentkit.Response:
    """Hold the priced stock: the one write the orchestrator makes."""
    return http(
        "POST",
        agentkit.internal(SUBAGENT_HOST) + "/reservations",
        json_body={"skus": [i["sku"] for i in items], "total": total},
        headers=_headers(parent_header),
        label="reserve",
    )


# `reserve` as a sub-agent of its own: a nested trigger joins the run it is called in, so its
# write is its parent's, and starts no run that `irimi compare` could name.
reservation_agent = sdk.trigger(name="reservation_agent")(reserve)


@sdk.trigger(name="plan_quarter_close")
def plan_quarter_close(parent_header: bool = False) -> dict[str, Any]:
    stock = inventory_agent(parent_header)
    revenue = finance_agent()
    total = stock["quote"].get("total")
    if not isinstance(total, int):
        # The branch a shadow run takes when the quote was faked: an L0 echo of the request, with
        # no `total`, so the orchestrator cannot price the basket and reserves nothing.
        decision = {"reserved": False, "reason": "no_price"}
    elif total > revenue + BUDGET_MINOR:
        decision = {"reserved": False, "reason": "over_budget", "total": total}
    else:
        resp = (reservation_agent if NESTED_RESERVE else reserve)(
            stock["items"], total, parent_header
        )
        doc = resp.json() or {}
        decision = {
            "reserved": resp.ok,
            "reason": "ok" if resp.ok else f"http_{resp.status}",
            "total": total,
            "reservation": doc.get("id"),
        }
    agentkit.obs("decision", **decision)
    if RECORD:
        agentkit.obs("recorded", **record_decision(decision))
    return decision


def main(argv: list[str]) -> int:
    agentkit.start()
    parser = argparse.ArgumentParser(prog="w08_orchestrator")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--parent-header", action="store_true")
    parser.add_argument("--record", action="store_true")
    parser.add_argument("--nested-reserve", action="store_true")
    args = parser.parse_args(argv)
    global RECORD, NESTED_RESERVE
    RECORD = args.record
    NESTED_RESERVE = args.nested_reserve
    decisions = [plan_quarter_close(args.parent_header) for _ in range(args.runs)]
    agentkit.obs("result", runs=args.runs, reserved=sum(d["reserved"] for d in decisions))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
