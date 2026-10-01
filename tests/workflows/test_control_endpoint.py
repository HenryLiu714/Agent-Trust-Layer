"""The control endpoint (#73) across every scenario of every sample workflow.

Today only W9 calls the endpoint (`self_addressed`). Once the SDK (#74) posts a run's start and
end, every SDK workflow does, and these hold all of them: a control request is never an exchange,
and `IRIMI_CONTROL` reaches every agent `irimi shadow` starts and no bare one.
"""

import pytest

from examples.workflows.harness.run import workflows
from irimi.exchange import CONTROL_PREFIX, Exchange

CASES = [(name, scenario) for name, wf in workflows().items() for scenario in wf.scenarios]
# The scenarios whose shadow run starts no agent: irimi refused to start (W8 `map_refused`).
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
