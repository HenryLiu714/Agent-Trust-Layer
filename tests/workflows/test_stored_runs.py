"""What every shadow run leaves in its trace store (#70), over every scenario of every workflow.

`irimi shadow` stores under `$IRIMI_HOME/store`, and the harness gives every run a home of its
own, so each scenario's store holds that run and nothing else (`Result.stored()`). The promises:
a run that started irimi stores exactly one process run, with the agent's argv and exit; every
exchange irimi reported on its terminal is on disk exactly once, and is, field by field, the
exchange the engine handed the store with its secrets redacted; the store is private and holds no
half-written line; and a bare run, with no irimi, stores nothing.
"""

import dataclasses
import re
import stat
import sys

import pytest

from examples.workflows.harness.run import CANARIES, Result, workflows
from irimi import paths, redact, report, trace
from irimi.exchange import Exchange
from irimi.trace import TelemetrySeen

CASES = [(name, scenario) for name, wf in workflows().items() for scenario in wf.scenarios]
IDS = [f"{n}:{s}" for n, s in CASES]


def started(result: Result) -> bool:
    """irimi printed its banner: its engine ran, so it had opened its store."""
    return any(
        line.startswith("irimi shadow · run ") and " · listening on " in line
        for line in result.irimi
    )


def reported(result: Result) -> list[str]:
    """irimi's per-exchange lines, with the one secret the corpus puts in one - W4's incoming
    webhook path, which the terminal still prints (#87) - swapped for the placeholder the store
    holds instead (#69)."""
    lines = result.exchange_lines()
    secret = CANARIES["SLACK_WEBHOOK_PATH"]
    if not any(secret in line for line in lines):
        return lines
    key = (result.home / paths.REDACT_KEY_NAME).read_bytes()
    return [line.replace(secret, "/" + redact.placeholder(key, secret)) for line in lines]


def is_telemetry(line: str) -> bool:
    return line.split()[1] == "telemetry"


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_every_shadow_run_stores_one_process_run_with_its_argv_and_exit(
    run_workflow, name, scenario
):
    shadow = run_workflow(name, scenario, "shadow")
    if not started(shadow):
        assert not (shadow.home / paths.STORE_DIR_NAME).exists()
        return
    workflow = workflows()[name]
    records = shadow.stored().list_runs()
    (run,) = [r for r in records if r.attribution == "process"]
    argv = [sys.executable, "-m", "examples.workflows.launch", workflow.module]
    assert run.trigger is not None
    assert run.trigger.args == {"argv": [*argv, *workflow.scenarios[scenario].argv]}
    assert (run.exit_code, run.outcome) == (
        shadow.exit_code,
        "ok" if shadow.exit_code == 0 else "error",
    )
    assert run.started_at is not None and run.ended_at is not None
    assert [r.dropped_events for r in records] == [0] * len(records)


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_every_exchange_irimi_reported_is_stored_exactly_once(run_workflow, name, scenario):
    """Across all of the process's runs: each stored exchange prints as one of irimi's own lines,
    and each telemetry line is one `TelemetrySeen` for its host. Order is compared within nothing,
    because the runs of W3 interleave freely."""
    shadow = run_workflow(name, scenario, "shadow")
    events = shadow.stored_events()
    lines = reported(shadow)
    stored = [report.exchange_line(e) for e in events if isinstance(e, Exchange)]
    assert sorted(stored) == sorted(line for line in lines if not is_telemetry(line))
    hosts = [line.split()[3].split("/", 1)[0] for line in lines if is_telemetry(line)]
    assert sorted(e.host for e in events if isinstance(e, TelemetrySeen)) == sorted(hosts)


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_every_stored_event_is_the_exchange_irimi_handed_its_store_redacted(
    run_workflow, name, scenario
):
    """#70's end-to-end comparison over the whole corpus. The engine handed its store exactly the
    exchanges irimi printed, the same objects in the same order. Run by run, in `seq` order, each
    stored event is one of them: an exchange equal to itself after `redact.redact_exchange`,
    compared over every field of `Exchange`, and telemetry reduced to its host and time. No
    corpus run names an id the store refuses, so nothing is unattributed."""
    shadow = run_workflow(name, scenario, "shadow")
    assert len(shadow.handed) == len(shadow.reported) == len(shadow.exchange_lines())
    assert all(a is b for a, b in zip(shadow.handed, shadow.reported, strict=True))
    if not started(shadow):
        return
    key = redact.load_key(shadow.home)
    reader = shadow.stored()
    runs = [r.run_id for r in reader.list_runs()]
    expected: dict[str, list[trace.Event]] = {run_id: [] for run_id in runs}
    for ex in shadow.reported:
        expected[ex.run_id].append(
            TelemetrySeen(ex.run_id, ex.request.host, ex.started_at)
            if ex.kind == "telemetry"
            else redact.redact_exchange(ex, key)
        )
    assert reader.load_run(trace.UNATTRIBUTED).events == []
    for run_id in runs:
        stored = reader.load_run(run_id).events
        assert len(stored) == len(expected[run_id]), run_id
        for got, want in zip(stored, expected[run_id], strict=True):
            assert type(got) is type(want), run_id
            for f in dataclasses.fields(want):
                assert getattr(got, f.name) == getattr(want, f.name), (run_id, f.name)


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_every_store_is_private_and_holds_no_half_written_line(run_workflow, name, scenario):
    """What the store keeps is redacted, not public: every directory it made is 0700 and every
    file 0600, as the CA key is (#70). Every `events.jsonl` ends in a newline - a killed agent
    (W10 `sigterm_mid_run`) included - so a reader skips no truncated last line, and no temp file
    of an atomic write is left behind."""
    shadow = run_workflow(name, scenario, "shadow")
    root = shadow.home / paths.STORE_DIR_NAME
    if not started(shadow):
        return
    for path in [root, *root.rglob("*")]:
        mode = stat.S_IMODE(path.stat().st_mode)
        assert mode == (0o700 if path.is_dir() else 0o600), (oct(mode), path)
        assert not path.name.endswith(".tmp"), path
    for events in root.rglob("events.jsonl"):
        assert events.read_bytes().endswith(b"\n"), events


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_every_run_but_the_process_run_is_a_header_run_its_first_event_made(
    run_workflow, name, scenario
):
    """Until #74 lands, a run the SDK stand-in labels with `Irimi-Run` is known to irimi only by
    its events: its record is the one the first of them created, `attribution: "header"`, with no
    trigger, no start and no end (#70), and it holds at least that event."""
    shadow = run_workflow(name, scenario, "shadow")
    if not started(shadow):
        return
    reader = shadow.stored()
    for record in reader.list_runs():
        if record.attribution == "process":
            continue
        assert record.attribution == "header"
        assert (record.trigger, record.started_at, record.ended_at) == (None, None, None)
        assert (record.outcome, record.exit_code, record.error) == (None, None, None)
        assert reader.load_run(record.run_id).events != []


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_a_bare_run_stores_nothing(run_workflow, name, scenario):
    """No irimi, no store: the bare baseline leaves its `IRIMI_HOME` without a store or a
    redaction key, whatever the agent's SDK stand-in does."""
    bare = run_workflow(name, scenario, "bare")
    assert bare.handed == bare.reported == []
    assert not (bare.home / paths.STORE_DIR_NAME).exists()
    assert not (bare.home / paths.REDACT_KEY_NAME).exists()


# The workflows whose every exchange lands in the process run: plain scripts that send no
# `Irimi-Run`, so the process run's stored summary is the one `irimi shadow` printed (#72).
UNLABELLED = ("w09_scope_gauntlet", "w11_leaky_agent")
UNLABELLED_CASES = [(n, s) for n, s in CASES if n in UNLABELLED]
ELAPSED = re.compile(r" · \d+\.\ds · ")


def no_elapsed(lines: list[str]) -> list[str]:
    """The live summary times the child and a stored one its run record: the one field that may
    differ (#72)."""
    return [ELAPSED.sub(" · <elapsed> · ", line) for line in lines]


@pytest.mark.parametrize(
    ("name", "scenario"), UNLABELLED_CASES, ids=[f"{n}:{s}" for n, s in UNLABELLED_CASES]
)
def test_an_unlabelled_agent_s_process_run_prints_the_summary_irimi_shadow_printed(
    run_workflow, name, scenario
):
    """#72's invariant on the corpus, through the real CLI: `irimi runs show <process run>` ends in
    the block `irimi shadow` printed on exit, line for line but for the elapsed seconds. W9 covers
    L0 and L1 fakes, idempotent replays and conflicts, unreadable bodies and an engine read with no
    response; W11 a `0 exchanges` run whose every write escaped.

    For an agent that labels its runs the live summary is still the process's, every exchange the
    proxy saw whatever run it named, while each stored run holds its own: the two agree only
    here, where the process run is the only run (docs/trace-format.md, "Reading a run back")."""
    shadow = run_workflow(name, scenario, "shadow")
    live = shadow.summary()
    assert live, "irimi printed no summary"
    assert [r.attribution for r in shadow.stored().list_runs()] == ["process"]
    code, out, err = shadow.runs("show", shadow.process_run_id())
    assert (code, err) == (0, [])
    assert no_elapsed(out[-len(live) :]) == no_elapsed(live)
