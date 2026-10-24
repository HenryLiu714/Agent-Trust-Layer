"""W10's scenarios: one failure each."""

from __future__ import annotations

import json
import re
from collections.abc import Callable

from tests.e2e.workflows.agents.w10_flaky_upstream.agent import (
    CHANNEL,
    CONTROL_FAULTS,
    OTHER_CHARGE,
)
from tests.e2e.workflows.harness.internet import BOMB_ENCODINGS, Req, bomb
from tests.e2e.workflows.harness.scenario import Scenario, Workflow
from tests.e2e.workflows.harness.services import LlmCall, LlmTurn, World

CHARGE = "ch_PAYOUT1"
STRIPE = "api.stripe.com"
# The scenarios whose read is answered with a bomb, by the encoding it is sent in.
BOMBED = {f"{encoding}_bomb_read": encoding for encoding in BOMB_ENCODINGS.values()}
# The scenarios that take irimi's control endpoint away from the agent: the SDK's posts fail, so
# its run is stored only as the `header` run its labelled requests make.
CONTROL_DOWN = tuple(f"control_{how}" for how in CONTROL_FAULTS)


def _script(call: LlmCall) -> LlmTurn:
    """Answer as the model does outside `malformed_llm`: refund 5.00 on the first charge named."""
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


def _seed_with_a_bomb(encoding: str) -> Callable[[World], None]:
    """Return `_seed()` with the bomb built now, rather than within the agent's read timeout."""

    def setup(world: World) -> None:
        bomb(encoding)
        _seed()(world)

    return setup


def _prose(call: LlmCall) -> LlmTurn:
    return LlmTurn(text="Sure! Looking at these, I'd refund the first one, probably.")


def _list(req: Req) -> bool:
    return req.method == "GET" and req.path == "/v1/charges"


def _refund(req: Req) -> bool:
    return req.method == "POST" and req.path == "/v1/refunds"


def _refund_read(req: Req) -> bool:
    return req.method == "GET" and req.path.startswith("/v1/refunds/")


def _check_read_of(charge: str) -> Callable[[Req], bool]:
    """irimi's own check read of `charge`, which the agent never makes."""
    return lambda req: req.method == "GET" and req.path == f"/v1/charges/{charge}"


def _precondition_read(req: Req) -> bool:
    """Match irimi's check read of the charge, which a bare run never makes."""
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
        "gzip_bomb_read": Scenario(
            setup=_seed_with_a_bomb("gzip"),
            faults=((STRIPE, "gzip-bomb", _list, 1),),
            doc="the read is answered with 2 MiB of gzip that decodes to 1 GiB: the agent gets "
            "the bytes it was sent, and irimi records the read empty, flagged",
        ),
        "br_bomb_read": Scenario(
            setup=_seed_with_a_bomb("br"),
            faults=((STRIPE, "br-bomb", _list, 1),),
            doc="the read is answered with a few KB of br that decodes to 1 GiB",
        ),
        "zstd_bomb_read": Scenario(
            setup=_seed_with_a_bomb("zstd"),
            faults=((STRIPE, "zstd-bomb", _list, 1),),
            doc="the read is answered with 100 KB of zstd that decodes to 1 GiB",
        ),
        "precondition_read_fails": Scenario(
            setup=_seed(),
            faults=((STRIPE, "500", _precondition_read, 1),),
            doc="irimi's check read of the charge gets a 500; the agent never sees it",
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
        "refund_read_unavailable": Scenario(
            ("--confirm-refund",),
            setup=_seed(),
            faults=((STRIPE, "503-json", _refund_read, 1),),
            doc="the refund's read-back gets a 503 with an empty JSON body once, and is retried",
        ),
        "two_checks_refused": Scenario(
            ("--refund-both",),
            setup=_seed(),
            faults=(
                (STRIPE, "429-paired", _check_read_of(CHARGE), 1),
                (STRIPE, "401-paired", _check_read_of(OTHER_CHARGE), 1),
            ),
            doc="two refunds at once in one run, irimi's check reads in the air together and "
            "answered 429 and 401: each refund says why it could not be checked",
        ),
        "malformed_llm": Scenario(
            setup=_seed(_prose), doc="the model answers prose; the agent does nothing"
        ),
        "raise_after_write": Scenario(
            ("--raise-after-write",), setup=_seed(), doc="the agent raises after its refund"
        ),
        "raise_with_key": Scenario(
            ("--raise-with-key",),
            setup=_seed(),
            doc="the agent raises after its refund with an error that is one key, 4000 "
            "characters past any cut irimi makes of a message",
        ),
        "sigterm_mid_run": Scenario(
            ("--sigterm-after-write",),
            setup=_seed(),
            doc="the agent is killed with SIGTERM after its refund",
        ),
        "sigint_mid_run": Scenario(
            ("--sigint-after-write",),
            setup=_seed(),
            doc="Ctrl-C after the refund: a KeyboardInterrupt ends the run in error",
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
        "control_stalled": Scenario(
            ("--control", "stalled"),
            setup=_seed(),
            doc="the control endpoint accepts a connection and never answers: the SDK warns once "
            "and pauses its posts, the agent is unchanged",
        ),
    },
)
