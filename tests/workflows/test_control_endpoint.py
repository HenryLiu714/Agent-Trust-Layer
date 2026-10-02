"""The control endpoint (#73) across every scenario of every sample workflow.

Every workflow that uses the SDK posts each run's start and end to it (#74), and W9 calls it by
hand. These hold all of them: a control request is never an exchange, `IRIMI_CONTROL` reaches
every agent `irimi shadow` starts and no bare one, a bare agent, with no irimi, starts no run
and calls no control endpoint, and the SDK warns only where W10 takes the endpoint away.
"""

import pytest

from examples.workflows.harness.run import workflows
from examples.workflows.w10_flaky_upstream.scenarios import CONTROL_DOWN
from irimi.exchange import CONTROL_PREFIX, Exchange

CASES = [(name, scenario) for name, wf in workflows().items() for scenario in wf.scenarios]
# The scenarios whose shadow run starts no agent: irimi refused to start (W8 `map_refused`). Every
# other one starts its agent as often as the bare run does, so a lost agent fails (#73).
NO_AGENT_UNDER_SHADOW = {("w08_orchestrator", "map_refused")}


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=[f"{n}:{s}" for n, s in CASES])
def test_no_control_request_is_ever_an_exchange(run_workflow, name, scenario):
    """Not printed, not handed to the store, not stored: W9's control scenarios post dozens."""
    shadow = run_workflow(name, scenario, "shadow")
    assert not [line for line in shadow.exchange_lines() if CONTROL_PREFIX in line]
    assert not [ex for ex in shadow.handed if ex.request.path.startswith(CONTROL_PREFIX)]
    stored = [e for e in shadow.stored_events() if isinstance(e, Exchange)]
    assert not [ex for ex in stored if ex.request.path.startswith(CONTROL_PREFIX)]


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=[f"{n}:{s}" for n, s in CASES])
def test_irimi_control_reaches_every_shadow_agent_and_no_bare_one(run_workflow, name, scenario):
    """Through `runner.child_env`: the endpoint on the listener the agent's proxy names. A bare
    agent's environment is `INHERITED_ENV` and the scenario's own, so it has none.

    `agentkit.start()` logs both, first thing in every agent. A bare run always starts its agent,
    and a shadow run starts it as often, but for the scenarios where irimi refused to start
    (`NO_AGENT_UNDER_SHADOW`): there the agent logged nothing at all."""
    shadow = run_workflow(name, scenario, "shadow")
    bare = run_workflow(name, scenario, "bare")
    assert bare.events("start")
    if (name, scenario) in NO_AGENT_UNDER_SHADOW:
        assert shadow.obs == []
    else:
        assert len(shadow.events("start")) == len(bare.events("start"))
    for start in shadow.events("start"):
        assert start["control"] == start["proxy"] + CONTROL_PREFIX.rstrip("/")
    for start in bare.events("start"):
        assert start["control"] is None


# W9's scenarios that call a control endpoint by hand. Bare, they call it at the fake internet,
# which answers 502, so that the agent exits as it does under shadow (#73).
BY_HAND = {
    ("w09_scope_gauntlet", "self_addressed"),
    ("w09_scope_gauntlet", "control_runs"),
    ("w09_scope_gauntlet", "control_hazards"),
}


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=[f"{n}:{s}" for n, s in CASES])
def test_a_bare_agent_starts_no_run_and_calls_no_control_endpoint(run_workflow, name, scenario):
    """Without irimi the SDK is inert (#74): `IRIMI_ENGINE_ACTIVE` is unset, so no trigger and no
    `sdk.run` starts a run, and nothing is posted anywhere. Nothing reached the fake internet,
    which is where a post that ignored NO_PROXY or the missing `IRIMI_CONTROL` would land."""
    bare = run_workflow(name, scenario, "bare")
    assert bare.events("run.start") == bare.events("run.end") == []
    if (name, scenario) in BY_HAND:
        return
    assert [r.path for r in bare.internet.requests() if CONTROL_PREFIX in r.path] == []


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=[f"{n}:{s}" for n, s in CASES])
def test_the_sdk_warns_only_where_its_control_endpoint_is_taken_away(run_workflow, name, scenario):
    """A post that fails is the SDK's one WARNING on `irimi.sdk`, once per process per kind of
    failure, and never an exception (#74); `agentkit.start()` logs each irimi log record. Every
    post in the corpus is accepted, so no run warns, but those of W10's `CONTROL_DOWN`, whose
    start and end both fail and warn once between them. A bare agent's SDK is inert: no warning."""
    assert run_workflow(name, scenario, "bare").events("log") == []
    shadow = run_workflow(name, scenario, "shadow")
    logged = [(e["logger"], e["level"]) for e in shadow.events("log")]
    if name == "w10_flaky_upstream" and scenario in CONTROL_DOWN:
        assert logged == [("irimi.sdk", "WARNING")]
    else:
        assert logged == []
