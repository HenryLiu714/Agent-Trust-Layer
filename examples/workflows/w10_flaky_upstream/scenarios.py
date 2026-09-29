"""W10's scenarios: one failure each."""

from __future__ import annotations

from collections.abc import Callable

from examples.workflows.harness.internet import Req
from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import LlmTurn, World
from examples.workflows.w10_flaky_upstream.agent import CHANNEL, script

CHARGE = "ch_PAYOUT1"
STRIPE = "api.stripe.com"


def _seed(llm: Callable | None = None) -> Callable[[World], None]:
    def setup(world: World) -> None:
        world.stripe.add_charge(CHARGE, 4900, customer="cus_PAYOUT1")
        world.stripe.add_charge("ch_PAYOUT2", 1500, created=1_789_000_000)
        world.slack.add_channel(CHANNEL, "payouts")
        world.llm.script = llm or script

    return setup


def _prose(call) -> LlmTurn:
    return LlmTurn(text="Sure! Looking at these, I'd refund the first one, probably.")


def _list(req: Req) -> bool:
    return req.method == "GET" and req.path == "/v1/charges"


def _refund(req: Req) -> bool:
    return req.method == "POST" and req.path == "/v1/refunds"


WORKFLOW = Workflow(
    name="w10_flaky_upstream",
    summary="Retries, timeouts, resets, a prose-speaking model, and an agent that dies after its "
    "write: which failures a shadow run shows and which it cannot.",
    scenarios={
        "rate_limited": Scenario(
            setup=_seed(), faults=((STRIPE, "429", _list, 1),), doc="a 429 on the read, retried"
        ),
        "server_error_read": Scenario(
            setup=_seed(), faults=((STRIPE, "500", _list, 1),), doc="a 500 on the read, retried"
        ),
        "read_timeout": Scenario(
            setup=_seed(),
            faults=((STRIPE, "stall", _list, 1),),
            doc="the read stalls past the client timeout and is retried",
        ),
        "reset_on_write": Scenario(
            setup=_seed(),
            faults=((STRIPE, "reset", _refund, 1),),
            doc="the refund's connection resets: bare retries it, shadow never sees it fail",
        ),
        "retry_without_key": Scenario(
            ("--no-key",),
            setup=_seed(),
            faults=((STRIPE, "500", _refund, 1),),
            doc="a 500 on a keyless refund: bare retries it, shadow never sees the 500",
        ),
        "malformed_llm": Scenario(
            setup=_seed(_prose), doc="the model answers prose; the agent does nothing"
        ),
        "raise_after_write": Scenario(
            ("--raise-after-write",), setup=_seed(), doc="the agent raises after its refund"
        ),
        "sigterm_mid_run": Scenario(
            ("--sigterm-after-write",),
            setup=_seed(),
            doc="the agent is killed with SIGTERM after its refund",
        ),
    },
)
