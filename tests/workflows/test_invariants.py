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

A scenario that runs under `irimi shadow --serve` too (`Scenario.serve_flags`, #77) is held to the
same five there, and to what serve mode owes each run it ended: one `--- run <id> ---` block, equal
to that run's stored summary.
"""

import pytest

from examples.workflows.harness.run import (
    CANARIES,
    RUN_BLOCK,
    SERVED_CANARIES,
    check_invariants,
    serve_cases,
    workflows,
)
from irimi import report

CASES = [(name, scenario) for name, wf in workflows().items() for scenario in wf.scenarios]
SERVE_CASES = serve_cases()


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=[f"{n}:{s}" for n, s in CASES])
def test_every_shadow_run_keeps_the_universal_invariants(run_workflow, name, scenario):
    bare = run_workflow(name, scenario, "bare")
    shadow = run_workflow(name, scenario, "shadow")
    assert check_invariants(shadow, bare) == []


@pytest.mark.parametrize(
    ("name", "scenario"), SERVE_CASES, ids=[f"{n}:{s}" for n, s in SERVE_CASES]
)
def test_every_serve_run_keeps_the_universal_invariants(run_workflow, name, scenario):
    bare = run_workflow(name, scenario, "bare")
    serve = run_workflow(name, scenario, "serve")
    assert check_invariants(serve, bare) == []


@pytest.mark.parametrize(
    ("name", "scenario"), SERVE_CASES, ids=[f"{n}:{s}" for n, s in SERVE_CASES]
)
def test_every_run_a_serve_ended_prints_its_stored_summary_once(run_workflow, name, scenario):
    """Serve mode prints a run's summary when its end is on disk, read back from the store (#77):
    one `--- run <id> ---` block per run the agent ended, none for a run it did not, and each
    equal to `stored_summary_lines` for its run. It starts no process run, so none is stored."""
    serve = run_workflow(name, scenario, "serve")
    reader = serve.stored()
    records = reader.list_runs()
    assert "process" not in {r.attribution for r in records}
    ended = sorted(r.run_id for r in records if r.outcome is not None)
    assert ended == sorted(e["run"] for e in serve.events("run.end"))
    assert sorted(m[1] for line in serve.irimi if (m := RUN_BLOCK.fullmatch(line))) == ended
    maps = serve.maps()
    for run_id, block in serve.run_blocks().items():
        assert block == report.stored_summary_lines(reader.load_run(run_id), maps), run_id


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
