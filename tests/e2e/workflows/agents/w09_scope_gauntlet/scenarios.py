"""W9's scenarios: one per group of edges in `agent.py`."""

from __future__ import annotations

from typing import Any

from tests.e2e.workflows.agents import agentkit
from tests.e2e.workflows.agents.w09_scope_gauntlet.agent import BIG_CHARGE, CHARGE, CUSTOMER, INTENT
from tests.e2e.workflows.harness.internet import PADDED_BYTES, PADDED_LINE, Req, Resp
from tests.e2e.workflows.harness.scenario import Scenario, Workflow
from tests.e2e.workflows.harness.services import JsonService, World

# Past `irimi.core.bodies.MAX_BODY_BYTES` (2 MB), the most irimi parses of a response it reads.
BIG_DESCRIPTION_BYTES = 3_000_000


def _stripe(world: World) -> None:
    world.stripe.add_charge(CHARGE, 10_000)
    world.stripe.add_customer(CUSTOMER)
    world.stripe.add_payment_intent(INTENT)
    world.slack.add_channel("C0GAUNT", "gauntlet")


def _big(world: World) -> None:
    _stripe(world)
    world.stripe.add_charge(BIG_CHARGE, 10_000, description="x" * BIG_DESCRIPTION_BYTES)


def _bombs(world: World) -> None:
    """Set up `_stripe`, building `gzip-padded`'s padding before the agent starts."""
    agentkit.gzip_bomb(b"", PADDED_LINE, PADDED_BYTES)
    _stripe(world)


def _refunds_read(req: Req) -> bool:
    return req.method == "GET" and req.path == "/v1/refunds"


def _internal(world: World) -> None:
    _stripe(world)

    def gql(req: Req, state: dict[str, Any]) -> Resp:
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
    summary="Every classification edge from a plain script: the scope rule, fixture-less writes, "
    "idempotency, unreadable bodies, forged irimi headers, base-URL requests, unmapped hosts, "
    "and the control endpoint.",
    scenarios={
        "verbs": Scenario(
            ("verbs",),
            setup=_stripe,
            doc="DELETE/PATCH/PUT/unrouted POST are faked; a fixture-less write gets an echo",
        ),
        "idempotency": Scenario(("idempotency",), setup=_stripe, doc="one key, three uses"),
        "bodies": Scenario(("bodies",), setup=_stripe, doc="gzip, chunked, and a 3 MB body"),
        "big_reads": Scenario(
            ("big_reads",), setup=_big, doc="a 3 MB charge: the check read and the overlay give up"
        ),
        "headers": Scenario(("headers",), setup=_stripe, doc="Irimi-Rewrote and Irimi-Run forged"),
        "self_addressed": Scenario(
            ("self_addressed",),
            setup=_stripe,
            doc="a refund to the proxy's own address as 127.0.0.1, 127.1 and 0.0.0.0; in serve "
            "mode with irimi bound to 0.0.0.0",
            serve_flags=("--bind", "0.0.0.0"),  # the bind this scenario tests
        ),
        "control_runs": Scenario(
            ("control_runs",),
            setup=_stripe,
            doc="the SDK's job by hand over the control endpoint: runs started, given tool calls "
            "and labelled reads, and ended; two at once; one in error; every refusal",
        ),
        "control_hazards": Scenario(
            ("control_hazards",),
            setup=_stripe,
            doc="a start re-posted after its run ended; a start, a tool call and an end posted "
            "for the process run's own id; a tool call posted for no run",
        ),
        "bombs": Scenario(
            ("bombs",),
            setup=_bombs,
            faults=(("api.stripe.com", "gzip-padded", _refunds_read, 1),),
            doc="bodies that decode past irimi's cap: a refund, a model call forwarded live, "
            "a read-back after a refund, and run starts posted to the control endpoint",
        ),
        "cut_refund": Scenario(
            ("cut_refund",),
            setup=_stripe,
            doc="bombs's two refunds alone, the cut one for another amount: irimi stores no body "
            "for it, so both print alike, and `irimi compare` will not call the two the same, a "
            "`?` row",
        ),
        "cut_update": Scenario(
            ("cut_update",),
            setup=_stripe,
            doc="cut_refund with the cut write a customer update: `irimi compare` will not compare "
            "it with the cut refund, a `?` row",
        ),
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
