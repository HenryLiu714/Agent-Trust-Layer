"""The five universal invariants, over every scenario of every sample workflow.

1. No write reached a fake service under shadow (inverted for a scenario marked `leaks`), and
   every live answer the agent saw came from a fake service, so that is where a write would land.
2. No request that reached a fake service carried `Irimi-Run`.
3. No write tool's real function ran under shadow.
4. No canary credential reached disk where irimi writes: its home, its working directory, TMPDIR,
   and a store it was given outside its home (#79). The trace store is under its home (#70), so
   this is the redaction test across the corpus, of what the agent sends (`CANARIES`) and of what
   a live read hands back (`SERVED_CANARIES`).
5. The agent exited under shadow as it did bare, unless the scenario is marked `diverges`.

A new workflow is picked up by name (`harness.run.workflows`), so it is held to these without an
edit here.

Every scenario runs under `irimi shadow --serve` too (`Scenario.serve_flags`, #77), and is held
to the same five there, and to what serve mode owes each run whose end it took: one
`--- run <id> ---` block, equal to that run's stored summary.
"""

import ast
import dataclasses
import re
from pathlib import Path

import pytest

from examples.workflows.harness.run import (
    CANARIES,
    RUN_BLOCK,
    SERVED_CANARIES,
    Result,
    check_invariants,
    serve_cases,
    workflows,
)
from examples.workflows.w10_flaky_upstream.scenarios import CONTROL_DOWN
from irimi import report, trace
from irimi.store import StoredRun

TESTS = Path(__file__).parent
CASES = [(name, scenario) for name, wf in workflows().items() for scenario in wf.scenarios]
SERVE_CASES = serve_cases()
# The scenarios whose runs hold events recorded after their end (#77). Serve prints a run's block
# as soon as its end is on disk, so an event stored after that is in the run and not in its block.
# LOOKS WRONG: W3 `task_outlives_trigger`'s task makes its calls after its run has ended (#75),
# and W7 `hand_off`'s stream, sent inside its run, ends after it (the issue's "Since #70": a
# streamed exchange is recorded after its last chunk). Each block says less than its run holds.
LATE_EVENTS = {
    ("w03_queue_worker", "task_outlives_trigger"),
    ("w07_streaming_assistant", "hand_off"),
}
# W9's run started again after its end (#97): its block is the run as it was when it ended, and
# the stored run is the reopened one. `test_w09_scope_gauntlet.py` pins that block.
REOPENED = {("w09_scope_gauntlet", "control_hazards")}
# A control post that ends a run, as W9 makes one by hand: `<listener>/_irimi/runs/<id>/end`.
END_POST = re.compile(r"/_irimi/runs/([^/]+)/end$")


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
    one `--- run <id> ---` block per end irimi took, none for a run that did not end (W10
    `sigterm_mid_run`, killed) or whose end never reached it (W10's `CONTROL_DOWN`), and each
    equal to `stored_summary_lines` for its run. It starts no process run, so none is stored."""
    serve = run_workflow(name, scenario, "serve")
    reader = serve.stored()
    records = reader.list_runs()
    assert "process" not in {r.attribution for r in records}
    printed = [m[1] for line in serve.irimi if (m := RUN_BLOCK.fullmatch(line))]
    assert sorted(printed) == sorted(ends_taken(serve))
    assert {r.run_id for r in records if r.outcome is not None} <= set(printed)
    maps = serve.maps()
    for run_id, block in serve.run_blocks().items():
        stored = reader.load_run(run_id)
        if (name, scenario) in REOPENED:
            continue
        if (name, scenario) not in LATE_EVENTS:
            assert block == report.stored_summary_lines(stored, maps), run_id
            continue
        # LOOKS WRONG (`LATE_EVENTS`): the block is the run as it stood at its end, every event
        # that ended by then and none after; the run itself holds more.
        ended = stored.record.ended_at
        assert ended is not None
        before = [e for e in stored.events if _ended_at(e) <= ended]
        assert len(before) < len(stored.events), run_id
        assert block == report.stored_summary_lines(StoredRun(stored.record, before), maps)


def ends_taken(result: Result) -> list[str]:
    """Every run whose end irimi took, once per end: each the SDK ended (`run.end`), but where W10
    takes the control endpoint away, and each W9 ended by hand, its post answered 204."""
    if result.workflow.name == "w10_flaky_upstream" and result.scenario in CONTROL_DOWN:
        return []
    by_hand = [
        match.group(1)
        for call in result.calls()
        if call.get("status") == 204 and (match := END_POST.search(str(call["url"])))
    ]
    return [e["run"] for e in result.events("run.end")] + by_hand


def _ended_at(event: trace.Event) -> float:
    return event.started_at if isinstance(event, trace.TelemetrySeen) else event.ended_at


def test_every_workflow_has_scenarios_and_a_summary():
    catalog = workflows()
    assert catalog, "no workflows found under examples/workflows"
    for name, wf in catalog.items():
        assert wf.name == name
        assert wf.summary and wf.scenarios
        assert all(s.doc for s in wf.scenarios.values()), f"{name}: every scenario needs a doc"


def test_a_scenario_whose_doc_says_looks_wrong_has_a_looks_wrong_pin():
    """A fix that flips a `LOOKS WRONG` pin drops the marker in the same PR (#89), the scenario's
    doc included: W9 `cut_refund`'s still said compare called its cut refund the same, pinned
    `LOOKS WRONG`, after #79 made that pair `?` and no pin said so. Each scenario whose doc says
    `LOOKS WRONG` is named in a test of its workflow whose docstring says it too."""
    stale = []
    for name, wf in workflows().items():
        module = ast.parse((TESTS / f"test_{name}.py").read_text())
        pins = [
            doc
            for node in ast.walk(module)
            if isinstance(node, ast.FunctionDef)
            and "LOOKS WRONG" in (doc := ast.get_docstring(node) or "")
        ]
        for scenario, s in wf.scenarios.items():
            if "LOOKS WRONG" in s.doc and not any(f"`{scenario}`" in doc for doc in pins):
                stale.append(f"{name}:{scenario}")
    assert stale == []


def test_every_scenario_runs_under_serve_too():
    """Serve is how irimi runs beside an agent it did not launch, so every scenario is held to the
    checks above under it as well (#77). A scenario whose `serve_flags` became None would drop out
    of every serve check here silently: none may, until one needs irimi to be its agent's parent
    and says why."""
    assert serve_cases() == CASES


@pytest.mark.parametrize("mode", ["shadow", "serve"])
def test_the_corpus_gives_every_invariant_something_to_catch(run_workflow, mode):
    """An invariant nothing exercises passes by default. Each needs at least one scenario whose
    shadow run, and whose serve run (#77), could have broken it: a write irimi held back, an
    `Irimi-Run` label irimi had to strip, a write tool irimi's SDK had to stand in for, a
    credential irimi handled - sent by the agent, and handed back by a live read - and an escape
    the fake services did see."""
    runs = [
        (run_workflow(name, scenario, "bare"), run_workflow(name, scenario, mode))
        for name, scenario in (CASES if mode == "shadow" else SERVE_CASES)
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


def test_invariant_4_reads_a_store_a_run_was_given_outside_its_home(shared_store_run, tmp_path):
    """A run recorded into a store runs share keeps it outside its own home (#79): a canary on
    disk there is still on disk where irimi writes."""
    shadow = shared_store_run("w01_ticket_triage", "full_refund")
    assert not shadow.store_root().is_relative_to(shadow.home)
    assert check_invariants(shadow) == []
    name, value = next(iter(CANARIES.items()))
    (tmp_path / "events.jsonl").write_text(value)
    leaked = check_invariants(dataclasses.replace(shadow, store=tmp_path))
    assert leaked == [
        f"4. a canary reached disk where irimi writes: ['{name} in {tmp_path / 'events.jsonl'}']"
    ]


# The replays the workflows pin (#82): (workflow, scenario, environment over the scenario's own).
REPLAYS = [
    ("w01_ticket_triage", "full_refund", {}),
    ("w01_ticket_triage", "full_refund", {"PROMPT": "half"}),
    ("w01_ticket_triage", "retried_refund", {}),
    ("w02_nightly_reconcile", "refund_then_page", {}),
    ("w07_streaming_assistant", "normal", {}),
    ("w07_streaming_assistant", "secret_in_stream", {}),
    ("w07_streaming_assistant", "secret_split_across_writes", {}),
    ("w09_scope_gauntlet", "get_that_writes", {}),
    ("w10_flaky_upstream", "precondition_read_fails", {}),
]


@pytest.mark.parametrize(
    ("name", "scenario", "env"),
    REPLAYS,
    ids=[f"{n}:{s}{''.join(f':{k}={v}' for k, v in e.items())}" for n, s, e in REPLAYS],
)
def test_every_replay_holds_the_invariants_and_never_forwards_a_write(
    replayed, name, scenario, env
):
    """Invariants 1 to 4 over each replay, and what a replay owes besides: the agent exits as it
    did when it was recorded, and no write reaches a fake service, whether the recording answered
    it or shadow mode faked it."""
    recorded, replay = replayed(name, scenario, env)
    assert check_invariants(replay) == []
    assert replay.internet.writes() == []
    assert replay.exit_code == recorded.exit_code
