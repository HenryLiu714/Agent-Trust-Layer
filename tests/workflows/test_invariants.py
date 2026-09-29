"""The five universal invariants, over every scenario of every sample workflow.

1. No write reached a fake service under shadow (inverted for a scenario marked `leaks`).
2. No request that reached a fake service carried `Irimi-Run`.
3. No write tool's real function ran under shadow.
4. No canary credential reached disk under irimi's home.
5. The agent exited under shadow as it did bare, unless the scenario is marked `diverges`.

A new workflow is picked up by name (`harness.run.workflows`), so it is held to these without an
edit here.
"""

import pytest

from examples.workflows.harness.run import check_invariants, workflows

CASES = [(name, scenario) for name, wf in workflows().items() for scenario in wf.scenarios]


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=[f"{n}:{s}" for n, s in CASES])
def test_every_shadow_run_keeps_the_universal_invariants(run_workflow, name, scenario):
    bare = run_workflow(name, scenario, "bare")
    shadow = run_workflow(name, scenario, "shadow")
    assert check_invariants(shadow, bare) == []


def test_every_workflow_has_scenarios_and_a_summary():
    catalog = workflows()
    assert catalog, "no workflows found under examples/workflows"
    for name, wf in catalog.items():
        assert wf.name == name
        assert wf.summary and wf.scenarios
        assert all(s.doc for s in wf.scenarios.values()), f"{name}: every scenario needs a doc"
