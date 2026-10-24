"""The control endpoint across every scenario of every sample workflow.

SDK workflows post each run's start and end to it, and W9 calls it by hand.
"""

import pytest

from irimi.core.exchange import Exchange
from irimi.protocol.control import CONTROL_PREFIX
from irimi.protocol.env import REPLAY_RUN_ENV
from tests.e2e.workflows.agents.w10_flaky_upstream.scenarios import CONTROL_DOWN
from tests.e2e.workflows.harness.scenario import cases

CASES = cases()
# Every scenario under `irimi shadow -- <cmd>`, and every one that runs under serve mode.
MODE_CASES = [(n, s, mode) for mode in ("shadow", "serve") for n, s in CASES]
MODE_IDS = [f"{n}:{s}:{m}" for n, s, m in MODE_CASES]
# The scenarios whose shadow run starts no agent: irimi refused its maps and did not start. Every
# other one starts its agent as often as the bare run does, so a lost agent fails.
NO_AGENT_UNDER_SHADOW = {
    ("w08_orchestrator", "map_refused"),
    ("w04_slack_ops_bot", "post_fixture_missing"),
}
# How the SDK's one warning for a write tool that ran for real ends.
REAL_WRITE_WARNING = "HTTPS_PROXY is set but IRIMI_ENGINE_ACTIVE is not"


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_no_control_request_is_ever_an_exchange(run_workflow, name, scenario, mode):
    """Not printed, not handed to the store, not stored: W9's control scenarios post dozens."""
    shadow = run_workflow(name, scenario, mode)
    assert not [line for line in shadow.exchange_lines() if CONTROL_PREFIX in line]
    assert not [ex for ex in shadow.handed if ex.request.path.startswith(CONTROL_PREFIX)]
    stored = [e for e in shadow.stored_events() if isinstance(e, Exchange)]
    assert not [ex for ex in stored if ex.request.path.startswith(CONTROL_PREFIX)]


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_irimi_control_reaches_every_shadow_agent_and_no_bare_one(
    run_workflow, name, scenario, mode
):
    """Each shadow or serve agent gets `IRIMI_CONTROL` on its proxy's listener; a bare one none.

    Serve agents are given it as the README's serve-mode guide says. A non-replaying
    `irimi shadow` removes a stale `IRIMI_REPLAY_RUN` (W6 `read_after_write`); bare and serve
    agents keep it. `NO_AGENT_UNDER_SHADOW` scenarios start no agent under shadow."""
    shadow = run_workflow(name, scenario, mode)
    bare = run_workflow(name, scenario, "bare")
    assert bare.events("start")
    if (name, scenario) in NO_AGENT_UNDER_SHADOW:
        assert shadow.obs == []
    else:
        assert len(shadow.events("start")) == len(bare.events("start"))
    exported = shadow.workflow.scenarios[scenario].env.get(REPLAY_RUN_ENV)
    for start in shadow.events("start"):
        assert start["control"] == start["proxy"] + CONTROL_PREFIX.rstrip("/")
        assert start["replay"] == (exported if mode == "serve" else None)
    for start in bare.events("start"):
        assert start["control"] is None
        assert start["replay"] == exported


# W9's scenarios that call a control endpoint by hand. Bare, they call it at the fake internet,
# which answers 502, so that the agent exits as it does under shadow.
BY_HAND = {
    ("w09_scope_gauntlet", "self_addressed"),
    ("w09_scope_gauntlet", "control_runs"),
    ("w09_scope_gauntlet", "control_hazards"),
    ("w09_scope_gauntlet", "bombs"),
}


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=[f"{n}:{s}" for n, s in CASES])
def test_a_bare_agent_starts_no_run_and_calls_no_control_endpoint(run_workflow, name, scenario):
    """Without irimi the SDK is inert: no run starts and nothing is posted anywhere.

    The fake internet is where a post that ignored NO_PROXY or the missing `IRIMI_CONTROL` lands."""
    bare = run_workflow(name, scenario, "bare")
    assert bare.events("run.start") == bare.events("run.end") == []
    if (name, scenario) in BY_HAND:
        return
    assert [r.path for r in bare.internet.requests() if CONTROL_PREFIX in r.path] == []


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_the_sdk_warns_only_where_its_control_endpoint_is_taken_away(
    run_workflow, name, scenario, mode
):
    """Only W10's `CONTROL_DOWN` runs warn; a bare run warns once of a write tool run for real.

    A failed post is one warning on `irimi.sdk` per process per kind of failure, never an
    exception. A bare agent's warning fires when a proxy variable is set, once per process."""
    bare = run_workflow(name, scenario, "bare")
    warned = [(e["logger"], e["level"], e["message"]) for e in bare.events("log")]
    real_writes = [tool for tool, ran in bare.tools("write") if ran == "real"]
    if real_writes:
        # The one warning names whichever write tool ran first, which concurrent runs of two
        # tools (W3 `ledger_two_tools_8`) race for: any one of them, once.
        assert warned in [
            [
                (
                    "irimi.sdk",
                    "WARNING",
                    f"irimi: write tool {tool} ran for real: {REAL_WRITE_WARNING}",
                )
            ]
            for tool in dict.fromkeys(real_writes)
        ]
    else:
        assert warned == []
    shadow = run_workflow(name, scenario, mode)
    logged = [(e["logger"], e["level"]) for e in shadow.events("log")]
    if name == "w10_flaky_upstream" and scenario in CONTROL_DOWN:
        assert logged == [("irimi.sdk", "WARNING")]
    else:
        assert logged == []
