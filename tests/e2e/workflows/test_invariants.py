"""The five invariants the corpus README lists, over every scenario of every sample workflow.

Each scenario is held to them under `irimi shadow`, under `irimi serve` and in its replays.
"""

import ast
import dataclasses
import re
from pathlib import Path

import pytest

from irimi.protocol import trace
from irimi.report.model import RunView
from irimi.report.summary import summary_lines
from tests.e2e.workflows.agents.w10_flaky_upstream.scenarios import CONTROL_DOWN
from tests.e2e.workflows.harness.invariants import (
    CANARIES,
    CANARY_PART,
    SERVED_CANARIES,
    check_invariants,
)
from tests.e2e.workflows.harness.result import RUN_BLOCK, Result
from tests.e2e.workflows.harness.scenario import cases, workflows

TESTS = Path(__file__).parent
CASES = cases()
# Spelled in two parts so this file's own source never reads as a marker.
MARKER = "KNOWN" + " BUG"
MARKER_FORM = re.compile(MARKER + r"(?: \(#\d+(?:, #\d+)*\))?: \S")
RETIRED_MARKER = "LOOKS" + " WRONG"
# Serve prints a run's block once its end is on disk, so an event stored later is not in it.
# KNOWN BUG (#125): each block says less than its run holds: W3 `task_outlives_trigger`'s task
# calls after its run ends, and W7 `hand_off`'s stream is recorded after the run's end.
LATE_EVENTS = {
    ("w03_queue_worker", "task_outlives_trigger"),
    ("w07_streaming_assistant", "hand_off"),
}
# A control post that ends a run, as W9 makes one by hand: `<listener>/_irimi/runs/<id>/end`.
END_POST = re.compile(r"/_irimi/runs/([^/]+)/end$")


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=[f"{n}:{s}" for n, s in CASES])
def test_every_shadow_run_keeps_the_universal_invariants(run_workflow, name, scenario):
    bare = run_workflow(name, scenario, "bare")
    shadow = run_workflow(name, scenario, "shadow")
    assert check_invariants(shadow, bare) == []


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=[f"{n}:{s}" for n, s in CASES])
def test_every_serve_run_keeps_the_universal_invariants(run_workflow, name, scenario):
    bare = run_workflow(name, scenario, "bare")
    serve = run_workflow(name, scenario, "serve")
    assert check_invariants(serve, bare) == []


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=[f"{n}:{s}" for n, s in CASES])
def test_every_run_a_serve_ended_prints_its_stored_summary_once(run_workflow, name, scenario):
    """Serve prints one `--- run <id> ---` block per run end it took, equal to the stored summary.

    A run that did not end (W10 `sigterm_mid_run`) or whose end never arrived (W10's
    `CONTROL_DOWN`) gets no block. Serve stores no process run."""
    serve = run_workflow(name, scenario, "serve")
    reader = serve.stored()
    records = reader.list_runs()
    assert "process" not in {r.attribution for r in records}
    printed = [m[1] for line in serve.irimi if (m := RUN_BLOCK.fullmatch(line))]
    assert sorted(printed) == sorted(ends_taken(serve))
    assert {r.run_id for r in records if r.end is not None} <= set(printed)
    maps = serve.maps()
    for run_id, block in serve.run_blocks().items():
        stored = reader.load_run(run_id)
        if (name, scenario) not in LATE_EVENTS:
            assert block == summary_lines(RunView(stored.record, stored.events), maps), run_id
            continue
        # The bug `LATE_EVENTS` names: the block is the run as it stood at its end.
        ended = stored.record.end.ended_at
        assert ended is not None
        before = [e for e in stored.events if _ended_at(e) <= ended]
        assert len(before) < len(stored.events), run_id
        assert block == summary_lines(RunView(stored.record, before), maps)


def ends_taken(result: Result) -> list[str]:
    """Return each run whose end irimi took: SDK ends, and W9's hand-posted ends answered 204.

    W10's `CONTROL_DOWN` scenarios take the control endpoint away, so irimi takes none."""
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
    assert catalog, "no workflows found under tests/e2e/workflows/agents"
    for name, wf in catalog.items():
        assert wf.name == name
        assert wf.summary and wf.scenarios
        assert all(s.doc for s in wf.scenarios.values()), f"{name}: every scenario needs a doc"


def test_a_scenario_whose_doc_names_a_known_bug_has_a_known_bug_pin():
    """A scenario whose doc carries the marker is named by a test docstring that carries it too.

    So a fix that flips a known-bug pin drops the marker from the scenario's doc as well."""
    stale = []
    for name, wf in workflows().items():
        module = ast.parse((TESTS / f"test_{name}.py").read_text())
        pins = [
            doc
            for node in ast.walk(module)
            if isinstance(node, ast.FunctionDef)
            and MARKER in (doc := ast.get_docstring(node) or "")
        ]
        for scenario, s in wf.scenarios.items():
            if MARKER in s.doc and not any(f"`{scenario}`" in doc for doc in pins):
                stale.append(f"{name}:{scenario}")
    assert stale == []


def test_every_known_bug_marker_takes_one_form():
    """Every pinned-bug marker in the corpus's code and README takes one greppable form.

    The marker, any tracking issues in parentheses, then a colon and the bug in one sentence."""
    found = []
    for path in sorted([*TESTS.rglob("*.py"), TESTS / "README.md"]):
        for number, line in enumerate(path.read_text().splitlines(), 1):
            retired = RETIRED_MARKER in line
            if retired or (MARKER in line and not MARKER_FORM.search(line)):
                found.append(f"{path.relative_to(TESTS)}:{number}: {line.strip()}")
    assert found == []


# It runs every scenario bare and in `mode`: a minute alone, and over 120 s on a loaded worker.
@pytest.mark.timeout(600)
@pytest.mark.parametrize("mode", ["shadow", "serve"])
def test_the_corpus_gives_every_invariant_something_to_catch(run_workflow, mode):
    """Each invariant has at least one scenario, in shadow and in serve, that could break it.

    An invariant nothing exercises passes by default: so the corpus holds a held-back write, a
    stripped `Irimi-Run` label, a write tool's fake, a credential sent and one handed back by a
    live read, and an escape the fake services saw."""
    runs = [
        (run_workflow(name, scenario, "bare"), run_workflow(name, scenario, mode))
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
    # A credential in a live read's response: the response side of redaction.
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
    assert stood_in, "invariant 3: no write tool ran for real bare and its fake under shadow"
    assert handled_canary, "invariant 4: no canary credential passed through irimi"
    assert served_canary, "invariant 4: no live read handed back a canary credential"
    assert seen_escape, "invariant 1: no `leaks` scenario's escape reached a fake service"
    assert stored_placeholder, "invariant 4: no shadow run stored a credential it had redacted"


def test_invariant_4_reads_a_store_a_run_was_given_outside_its_home(shared_store_run, tmp_path):
    """A canary in a shared store outside the run's home is still on disk where irimi writes."""
    shadow = shared_store_run("w01_ticket_triage", "full_refund")
    assert not shadow.store_root().is_relative_to(shadow.home)
    assert check_invariants(shadow) == []
    name, value = next(iter(CANARIES.items()))
    (tmp_path / "events.jsonl").write_text(value)
    leaked = check_invariants(dataclasses.replace(shadow, store=tmp_path))
    assert leaked == [
        f"4. a canary reached disk where irimi writes: ['{name} in {tmp_path / 'events.jsonl'}']"
    ]


@pytest.mark.parametrize("name", [*CANARIES, *SERVED_CANARIES])
def test_invariant_4_finds_the_start_of_a_canary_a_cut_left_on_disk(
    shared_store_run, tmp_path, name
):
    """Invariant 4 finds a canary's first `CANARY_PART` characters, and a whole canary once.

    A key cut before redaction loses its shape, so only the part the cut kept reaches disk."""
    shadow = shared_store_run("w01_ticket_triage", "full_refund")
    value = {**CANARIES, **SERVED_CANARIES}[name]
    stored = tmp_path / "events.jsonl"
    found = "4. a canary reached disk where irimi writes: "
    stored.write_text(f"cut: {value[: CANARY_PART - 1]}")
    assert check_invariants(dataclasses.replace(shadow, store=tmp_path)) == []
    stored.write_text(f"cut: {value[:CANARY_PART]}")
    assert check_invariants(dataclasses.replace(shadow, store=tmp_path)) == [
        f'{found}["{name}\'s first {CANARY_PART} characters in {stored}"]'
    ]
    stored.write_text(f"whole: {value}")
    assert check_invariants(dataclasses.replace(shadow, store=tmp_path)) == [
        f"{found}['{name} in {stored}']"
    ]


# The replays the workflows pin: (workflow, scenario, environment over the scenario's own,
# replayed by the scenario's command rather than by what `irimi replay` runs for the run).
REPLAYS = [
    ("w01_ticket_triage", "full_refund", {}, False),
    ("w01_ticket_triage", "full_refund", {"PROMPT": "half"}, False),
    ("w01_ticket_triage", "full_refund", {"HELPDESK_DB_LOST": "1"}, True),
    ("w01_ticket_triage", "retried_refund", {}, False),
    ("w01_ticket_triage", "verified_refund", {"PROMPT": "verify_only"}, False),
    ("w01_ticket_triage", "checked_charge", {"PROMPT": "verify"}, False),
    ("w01_ticket_triage", "late_lookup", {"PROMPT": "verify"}, True),
    ("w02_nightly_reconcile", "refund_then_page", {}, True),
    ("w06_crm_db_agent", "enrich", {"WORKFLOW_LOOKUP_REFUSED": "1"}, False),
    ("w07_streaming_assistant", "normal", {}, True),
    ("w07_streaming_assistant", "secret_in_stream", {}, True),
    ("w07_streaming_assistant", "secret_split_across_writes", {}, True),
    ("w09_scope_gauntlet", "get_that_writes", {}, False),
    ("w10_flaky_upstream", "precondition_read_fails", {}, False),
]


@pytest.mark.parametrize(
    ("name", "scenario", "env", "command"),
    REPLAYS,
    ids=[
        f"{n}:{s}{''.join(f':{k}={v}' for k, v in e.items())}{':command' if c else ''}"
        for n, s, e, c in REPLAYS
    ],
)
def test_every_replay_holds_the_invariants_and_never_forwards_a_write(
    replayed, name, scenario, env, command
):
    """Each replay keeps invariants 1-4, exits as recorded, and sends no write to a fake service."""
    recorded, replay = replayed(name, scenario, env, command=command)
    assert check_invariants(replay) == []
    assert replay.internet.writes() == []
    assert replay.agent_exit_code() == recorded.exit_code
