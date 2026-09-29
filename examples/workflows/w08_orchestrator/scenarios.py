"""W8's scenarios: an internal service irimi has no map for, the same service with one, and a run
that crosses a service hop.

The map is added with `Scenario.extra_maps`, beside the shipped maps, and not with the overrides
file: an override may only retarget a service a shipped map already names (`loader.apply_overrides`
refuses an unknown one), so today a user cannot classify their own internal API without adding a
map file to irimi's shipped directory. That gap is part of what `with_map` documents.
"""

from __future__ import annotations

from typing import Any

from examples.workflows.harness.internet import Req, Resp
from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import JsonService, World
from examples.workflows.w08_orchestrator.agent import SUBAGENT_HOST

PRICES = {"SKU-A": 1200, "SKU-B": 800, "SKU-C": 5000}

SUBAGENT_MAP = f"""\
version: 1
service: subagent
verbs: honest
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
    persists: false
    comment: prices a basket and stores nothing, so it is forwarded like a GET
  - match:
      method: POST
      path: /reservations
    operation: reservations.create
    kind: write
    ids:
      id: rsv_
"""

# The same map without `persists: false`. The loader refuses a live kind on POST unless the route
# says it persists nothing, so irimi does not start and the agent never runs: fail closed.
UNJUSTIFIED_MAP = SUBAGENT_MAP.replace("    persists: false\n", "")


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
            doc="a POST read with no `persists: false`: irimi refuses the map and never starts",
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
    },
)
