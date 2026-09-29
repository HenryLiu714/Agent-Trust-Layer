"""W9's scenarios: one per group of edges in `agent.py`."""

from __future__ import annotations

from examples.workflows.harness.internet import Req, Resp
from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import JsonService, World
from examples.workflows.w09_scope_gauntlet.agent import CHARGE, CUSTOMER, INTENT


def _stripe(world: World) -> None:
    world.stripe.add_charge(CHARGE, 10_000)
    world.stripe.add_customer(CUSTOMER)
    world.stripe.add_payment_intent(INTENT)


def _internal(world: World) -> None:
    _stripe(world)
    world.slack.add_channel("C0GAUNT", "gauntlet")

    def gql(req: Req, state: dict) -> Resp:
        doc = req.json() or {}
        if str(doc.get("query", "")).lstrip().startswith("mutation"):
            state.setdefault("cancelled", []).append(doc)
            return Resp(200, {"data": {"cancelOrder": {"id": 7}}})
        return Resp(200, {"data": {"orders": [{"id": 7}, {"id": 8}]}})

    world.extra.append(
        JsonService(
            ("graphql.internal",),
            {
                ("POST", "/graphql"): gql,
                ("GET", "/health"): lambda req, state: Resp(200, {"ok": True}),
            },
            # GraphQL is POST for reads and writes alike; only a mutation changes anything.
            is_write=lambda req: (
                str((req.json() or {}).get("query", "")).lstrip().startswith("mutation")
            ),
        )
    )


def _legacy(world: World) -> None:
    world.extra.append(
        JsonService(
            ("legacy.internal",),
            {
                ("GET", "/api/delete_user"): lambda req, state: Resp(
                    200, {"deleted": req.query["id"]}
                )
            },
            is_write=lambda req: req.path == "/api/delete_user",
        )
    )


WORKFLOW = Workflow(
    name="w09_scope_gauntlet",
    summary="Every classification edge from a plain script: THE SCOPE RULE, the L0 floor, "
    "idempotency, unreadable bodies, forged irimi headers, unmapped hosts.",
    scenarios={
        "verbs": Scenario(
            ("verbs",), setup=_stripe, doc="DELETE/PATCH/PUT/unrouted POST are faked"
        ),
        "idempotency": Scenario(("idempotency",), setup=_stripe, doc="one key, three uses"),
        "bodies": Scenario(("bodies",), setup=_stripe, doc="gzip, chunked, and a 3 MB body"),
        "headers": Scenario(("headers",), setup=_stripe, doc="Irimi-Rewrote and Irimi-Run forged"),
        "unmapped_hosts": Scenario(
            ("unmapped_hosts",), setup=_internal, doc="GraphQL and Slack's unrouted method"
        ),
        "get_that_writes": Scenario(
            ("get_that_writes",),
            setup=_legacy,
            leaks=True,
            doc="a GET that deletes: forwarded, because a GET is a read",
        ),
    },
)
