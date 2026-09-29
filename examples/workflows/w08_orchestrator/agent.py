"""W8 `orchestrator`: one run that fans out to sub-agents and to an internal service.

`plan_quarter_close()` is the trigger. It calls two sub-agents in-process, each an `@sdk.trigger`
of its own, which must join the parent run rather than start one (#74):

- `inventory_agent()` asks the internal service `subagent.internal` for stock (`GET /inventory`)
  and for a price (`POST /quote`: a read spelled as a POST, it prices a basket and changes nothing);
- `finance_agent()` reads Stripe's recent charges to check there is budget.

The orchestrator then reserves the stock (`POST /reservations`, a write) unless it could not price
it. The internal service is the point: irimi has no map for it unless the scenario adds one.

    python -m examples.workflows.launch \\
        examples.workflows.w08_orchestrator.agent [--runs N] [--parent-header]
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from examples.workflows import agentkit, sdk
from examples.workflows.agentkit import http

SUBAGENT_HOST = "subagent.internal"
BUDGET_MINOR = 10_000


def _headers(parent_header: bool) -> dict[str, str]:
    """With `parent_header`, the agent carries its run id to the internal service itself, in a
    header of its own. irimi strips `Irimi-Run` from every request that leaves it (#67), so this
    is the only way the service can learn which run called it."""
    run_id = sdk.current_run_id()
    return {"X-Parent-Run": run_id} if parent_header and run_id else {}


@sdk.trigger(name="inventory_agent")
def inventory_agent(parent_header: bool) -> dict[str, Any]:
    service = agentkit.internal(SUBAGENT_HOST)
    headers = _headers(parent_header)
    stock = http("GET", service + "/inventory", headers=headers, label="inventory").json()
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
        resp = http(
            "POST",
            agentkit.internal(SUBAGENT_HOST) + "/reservations",
            json_body={"skus": [i["sku"] for i in stock["items"]], "total": total},
            headers=_headers(parent_header),
            label="reserve",
        )
        doc = resp.json() or {}
        decision = {
            "reserved": resp.ok,
            "reason": "ok" if resp.ok else f"http_{resp.status}",
            "total": total,
            "reservation": doc.get("id"),
        }
    agentkit.obs("decision", **decision)
    return decision


def main(argv: list[str]) -> int:
    agentkit.start()
    parser = argparse.ArgumentParser(prog="w08_orchestrator")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--parent-header", action="store_true")
    args = parser.parse_args(argv)
    decisions = [plan_quarter_close(args.parent_header) for _ in range(args.runs)]
    agentkit.obs("result", runs=args.runs, reserved=sum(d["reserved"] for d in decisions))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
