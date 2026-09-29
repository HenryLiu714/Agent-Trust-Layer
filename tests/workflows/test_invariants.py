"""The five universal invariants, over every scenario of every sample workflow.

1. No write reached a fake service under shadow (inverted for a scenario marked `leaks`), and
   every live answer the agent saw came from a fake service, so that is where a write would land.
2. No request that reached a fake service carried `Irimi-Run`.
3. No write tool's real function ran under shadow.
4. No canary credential reached disk where irimi writes: its home, its working directory, TMPDIR.
   The trace store is under its home (#70), so this is the redaction test across the corpus, of
   what the agent sends (`CANARIES`) and of what a live read hands back (`SERVED_CANARIES`).
5. The agent exited under shadow as it did bare, unless the scenario is marked `diverges`.

A new workflow is picked up by name (`harness.run.workflows`), so it is held to these without an
edit here.
"""

import pytest

from examples.workflows.harness.run import CANARIES, SERVED_CANARIES, check_invariants, workflows

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


def test_the_corpus_gives_every_invariant_something_to_catch(run_workflow):
    """An invariant nothing exercises passes by default. Each needs at least one scenario whose
    shadow run could have broken it: a write irimi held back, an `Irimi-Run` label irimi had to
    strip, a write tool irimi's SDK had to stand in for, a credential irimi handled - sent by the
    agent, and handed back by a live read - and an escape the fake services did see."""
    runs = [
        (run_workflow(name, scenario, "bare"), run_workflow(name, scenario, "shadow"))
        for name, scenario in CASES
    ]
    held_back = [s.scenario for b, s in runs if b.internet.writes() and not s.internet.writes()]
    labelled_live = [
        s.scenario
        for _, s in runs
        if any(c.get("run") and c.get("status") and not c.get("answered_by") for c in s.calls())
    ]
    stood_in = [
        s.scenario
        for b, s in runs
        if any(t["kind"] == "write" and t["ran"] == "real" for t in b.events("tool"))
        and any(t["kind"] == "write" and t["ran"] == "shadow" for t in s.events("tool"))
    ]
    handled_canary = [
        s.scenario
        for _, s in runs
        if any(v in r.headers.values() for r in s.internet.requests() for v in CANARIES.values())
    ]
    # A credential in a live read's response: the response side of redaction (#70).
    served_canary = [
        s.scenario
        for _, s in runs
        if any(
            v.encode() in ex.response.body
            for ex in s.reported
            if ex.answered_by == "live" and ex.response is not None
            for v in SERVED_CANARIES.values()
        )
    ]
    seen_escape = [s.scenario for _, s in runs if s.internet.writes()]
    stored_placeholder = [
        s.scenario
        for _, s in runs
        if any(
            b"<redacted:" in path.read_bytes()
            for path in (s.home / "store").rglob("*")
            if path.is_file()
        )
    ]
    assert held_back, "invariant 1: no shadow run held back a write its bare run made"
    assert labelled_live, "invariant 2: no labelled call was forwarded live under shadow"
    assert stood_in, "invariant 3: no write tool ran for real bare and as a stand-in shadow"
    assert handled_canary, "invariant 4: no canary credential passed through irimi"
    assert served_canary, "invariant 4: no live read handed back a canary credential"
    assert seen_escape, "invariant 1: no `leaks` scenario's escape reached a fake service"
    assert stored_placeholder, "invariant 4: no shadow run stored a credential it had redacted"
