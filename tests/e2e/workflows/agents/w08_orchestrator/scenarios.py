"""W8's scenarios: an internal service irimi has no map for, the same service with one, and a run
that crosses a service hop.

The map is added with `Scenario.extra_maps`, beside the shipped maps, and not with the overrides
file: an override may only retarget a service a shipped map already names (`loader.apply_overrides`
refuses an unknown one), so today a user cannot classify their own internal API without adding a
map file to irimi's shipped directory. That gap is part of what `with_map` documents.
"""

from __future__ import annotations

import re
from typing import Any

from tests.e2e.workflows.agents.w08_orchestrator.agent import SUBAGENT_HOST
from tests.e2e.workflows.harness.internet import Req, Resp
from tests.e2e.workflows.harness.scenario import Scenario, Workflow
from tests.e2e.workflows.harness.services import JsonService, World

PRICES = {"SKU-A": 1200, "SKU-B": 800, "SKU-C": 5000}

SUBAGENT_MAP = f"""\
version: 1
service: subagent
style: rest
hosts:
  - {SUBAGENT_HOST}
routes:
  - match:
      method: GET
      path: /inventory
    operation: inventory.list
    kind: read
  - match:
      method: POST
      path: /quote
    operation: quote.create
    kind: read
    live_because: prices a basket and stores nothing, so it is forwarded like a GET
  - match:
      method: POST
      path: /reservations
    operation: reservations.create
    kind: write
    ids:
      id: rsv_
"""

# A loopback answer target nothing listens on: the discard port, which no test machine serves.
DOWN_TARGET = "http://127.0.0.1:9"
# The same map with the reservation delegated to `DOWN_TARGET`: irimi cannot reach it, answers the
# agent 502 itself and says the write went unanswered, a write `irimi compare` compares as
# its summary prints it.
TARGET_DOWN_MAP = SUBAGENT_MAP.replace(
    "    operation: reservations.create\n",
    f"    operation: reservations.create\n    target: {DOWN_TARGET}\n",
)

# The same map without `live_because:`. The loader refuses a live kind on POST unless the route
# says it persists nothing, so irimi does not start and the agent never runs: fail closed.
UNJUSTIFIED_MAP = re.sub(r"    live_because: .*\n", "", SUBAGENT_MAP)


def _inventory(req: Req, state: dict[str, Any]) -> Resp:
    stock = [("SKU-A", 5), ("SKU-B", 2), ("SKU-C", 0)]
    return Resp(200, {"items": [{"sku": sku, "qty": qty} for sku, qty in stock]})


def _quote(req: Req, state: dict[str, Any]) -> Resp:
    skus = (req.json() or {}).get("skus", [])
    return Resp(200, {"total": sum(PRICES.get(s, 0) for s in skus), "currency": "usd"})


def _reserve(req: Req, state: dict[str, Any]) -> Resp:
    held = state.setdefault("reservations", [])
    held.append(req.json())
    return Resp(200, {"id": f"rsv_{len(held):04d}", "status": "held"})


def _world(world: World) -> None:
    world.stripe.add_charge("ch_O1", 4900)
    world.stripe.add_charge("ch_O2", 2500)
    world.extra.append(
        JsonService(
            (SUBAGENT_HOST,),
            {
                ("GET", "/inventory"): _inventory,
                ("POST", "/quote"): _quote,
                ("POST", "/reservations"): _reserve,
            },
            # The service's own truth: pricing a basket changes nothing, a reservation does.
            is_write=lambda req: req.path == "/reservations",
        )
    )


WORKFLOW = Workflow(
    name="w08_orchestrator",
    summary="An orchestrator run with nested sub-agent triggers and an internal HTTP service: "
    "what irimi does to a service it has no map for, and to one it has.",
    scenarios={
        "no_map": Scenario(
            (),
            setup=_world,
            doc="unmapped internal service: its POST read is faked, so the agent cannot price",
        ),
        "with_map": Scenario(
            (),
            setup=_world,
            extra_maps={"subagent.yaml": SUBAGENT_MAP},
            doc="a map names POST /quote a read: it is forwarded, the reservation is faked",
        ),
        "map_refused": Scenario(
            (),
            setup=_world,
            extra_maps={"subagent.yaml": UNJUSTIFIED_MAP},
            diverges=True,
            doc="a POST read with no `live_because:`: irimi refuses the map and never starts",
        ),
        "run_header_across_hop": Scenario(
            ("--parent-header",),
            setup=_world,
            extra_maps={"subagent.yaml": SUBAGENT_MAP},
            doc="Irimi-Run is stripped at the hop; only the agent's own header carries the run",
        ),
        "nested_runs": Scenario(
            ("--runs", "2"),
            setup=_world,
            extra_maps={"subagent.yaml": SUBAGENT_MAP},
            doc="two orchestrator runs: each sub-agent call joins its own parent's run",
        ),
        "nested_tools": Scenario(
            ("--runs", "2", "--record"),
            setup=_world,
            extra_maps={"subagent.yaml": SUBAGENT_MAP},
            doc="nested_runs with tools: the sub-agent's read tool and the orchestrator's write "
            "tool each land in their own parent's run (#76)",
        ),
        "target_down": Scenario(
            (),
            setup=_world,
            extra_maps={"subagent.yaml": TARGET_DOWN_MAP},
            doc="with_map with the reservation delegated to an answer target nothing listens on: "
            "irimi answers 502 and the write goes unanswered (#16, #79)",
        ),
        "target_down_tools": Scenario(
            ("--record",),
            setup=_world,
            extra_maps={"subagent.yaml": TARGET_DOWN_MAP},
            doc="target_down with the decision recorded through its write tool's stand-in: the "
            "closing lines say the tool write did not happen either (#121)",
        ),
        "nested_write": Scenario(
            ("--runs", "2", "--nested-reserve"),
            setup=_world,
            extra_maps={"subagent.yaml": SUBAGENT_MAP},
            doc="nested_runs with the reservation made by a nested trigger: its write is its "
            "parent run's, as `irimi compare` shows it (#79)",
        ),
    },
)
