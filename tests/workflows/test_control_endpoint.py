"""The control endpoint across every scenario of every sample workflow.

Every workflow that uses the SDK posts each run's start and end to it, and W9 calls it by
hand. These hold all of them: a control request is never an exchange, `IRIMI_CONTROL` reaches
every agent `irimi shadow` starts and no bare one, and `IRIMI_REPLAY_RUN` none of either but in a
replay, a bare agent, with no irimi, starts no run and calls no control endpoint, and the
SDK warns only where W10 takes the endpoint away.
"""

import pytest

from examples.workflows.harness.run import serve_cases, workflows
from examples.workflows.w10_flaky_upstream.scenarios import CONTROL_DOWN
from irimi.exchange import Exchange
from irimi.protocol.control import CONTROL_PREFIX
from irimi.protocol.env import REPLAY_RUN_ENV

CASES = [(name, scenario) for name, wf in workflows().items() for scenario in wf.scenarios]
# Every scenario under `irimi shadow -- <cmd>`, and every one that runs under serve mode.
MODE_CASES = [(n, s, "shadow") for n, s in CASES] + [(n, s, "serve") for n, s in serve_cases()]
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
    """Through `runner.child_env`: the endpoint on the listener the agent's proxy names. A bare
    agent's environment is `INHERITED_ENV` and the scenario's own, so it has none. Under serve
    mode the agent is not irimi's child, and is given both as the README's serve-mode guide says,
    with the same result.

    `IRIMI_REPLAY_RUN` is irimi's word that it replays, so an `irimi shadow` that does not
    replay removes a stale one its own environment holds, as W6 `read_after_write`'s does; a bare
    agent and a serve agent, which are not irimi's children, keep it, and are told nothing else.
    `test_replays.py` holds every replay's agent to being told.

    `agentkit.start()` logs all three, first thing in every agent. A bare run always starts its
    agent, and a shadow run starts it as often, but for the scenarios where irimi refused to start
    (`NO_AGENT_UNDER_SHADOW`): there the agent logged nothing at all."""
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
    """Without irimi the SDK is inert: `IRIMI_ENGINE_ACTIVE` is unset, so no trigger and no
    `sdk.run` starts a run, and nothing is posted anywhere. Nothing reached the fake internet,
    which is where a post that ignored NO_PROXY or the missing `IRIMI_CONTROL` would land."""
    bare = run_workflow(name, scenario, "bare")
    assert bare.events("run.start") == bare.events("run.end") == []
    if (name, scenario) in BY_HAND:
        return
    assert [r.path for r in bare.internet.requests() if CONTROL_PREFIX in r.path] == []


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_the_sdk_warns_only_where_its_control_endpoint_is_taken_away(
    run_workflow, name, scenario, mode
):
    """A post that fails is the SDK's one WARNING on `irimi.sdk`, once per process per kind of
    failure, and never an exception; `agentkit.start()` logs each irimi log record. Every
    post in the corpus is accepted, so no run warns, but those of W10's `CONTROL_DOWN`, whose
    start and end both fail and warn once between them.

    A bare agent's SDK is inert, and warns of one thing only: a write tool that ran for real while
    a proxy variable was set, as every bare run's is, once per process however many ran.
    Under serve mode the same posts go to the endpoint the agent was given."""
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
