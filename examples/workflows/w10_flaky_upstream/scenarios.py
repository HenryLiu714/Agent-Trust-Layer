"""W10's scenarios: one failure each."""

from __future__ import annotations

import json
import re
from collections.abc import Callable

from examples.workflows.harness.internet import Req
from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import LlmCall, LlmTurn, World
from examples.workflows.w10_flaky_upstream.agent import CHANNEL, CONTROL_FAULTS

CHARGE = "ch_PAYOUT1"
STRIPE = "api.stripe.com"
# The scenarios that take irimi's control endpoint away from the agent, one per fault (#74): the
# SDK's posts fail, so its run is stored only as the `header` run its labelled requests make.
CONTROL_DOWN = tuple(f"control_{how}" for how in CONTROL_FAULTS)


def _script(call: LlmCall) -> LlmTurn:
    """The model for every scenario but `malformed_llm`: refund 5.00 on the first charge the
    prompt names."""
    found = re.search(r"ch_[A-Za-z0-9]+", call.last_user_text())
    decision = {"refund": found.group(0) if found else None, "amount": 500, "note": "duplicate"}
    return LlmTurn(text=json.dumps(decision))


def _seed(llm: Callable[[LlmCall], LlmTurn] = _script) -> Callable[[World], None]:
    def setup(world: World) -> None:
        world.stripe.add_charge(CHARGE, 4900, customer="cus_PAYOUT1")
        world.stripe.add_charge("ch_PAYOUT2", 1500, created=1_789_000_000)
        world.slack.add_channel(CHANNEL, "payouts")
        world.llm.script = llm

    return setup


def _prose(call: LlmCall) -> LlmTurn:
    return LlmTurn(text="Sure! Looking at these, I'd refund the first one, probably.")


def _list(req: Req) -> bool:
    return req.method == "GET" and req.path == "/v1/charges"


def _refund(req: Req) -> bool:
    return req.method == "POST" and req.path == "/v1/refunds"


def _precondition_read(req: Req) -> bool:
    """The charge read irimi itself issues before it fakes the refund (L3). The agent never makes
    it, so in a bare run this fault never fires."""
    return req.method == "GET" and req.path == f"/v1/charges/{CHARGE}"


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
        "reset_on_read": Scenario(
            setup=_seed(),
            faults=((STRIPE, "reset", _list, 1),),
            doc="the read's connection resets before any answer, and is retried",
        ),
        "precondition_read_fails": Scenario(
            setup=_seed(),
            faults=((STRIPE, "500", _precondition_read, 1),),
            doc="irimi's own L3 read of the charge gets a 500; the agent never sees it",
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
        "sigint_mid_run": Scenario(
            ("--sigint-after-write",),
            setup=_seed(),
            doc="Ctrl-C after the refund: a KeyboardInterrupt ends the run in error (#74)",
        ),
        "control_unset": Scenario(
            ("--control", "unset"),
            setup=_seed(),
            doc="IRIMI_CONTROL dropped before the run: the SDK warns once, the agent is unchanged",
        ),
        "control_unreachable": Scenario(
            ("--control", "unreachable"),
            setup=_seed(),
            doc="the control endpoint is gone: the SDK warns once, the agent is unchanged",
        ),
        "control_refused": Scenario(
            ("--control", "refused"),
            setup=_seed(),
            doc="the control endpoint answers 404: the SDK warns once, the agent is unchanged",
        ),
    },
)
