"""What every shadow and serve run leaves in its trace store, over every corpus scenario.

Each run has its own home, so each scenario's store holds that run alone (`Result.stored()`).
"""

import dataclasses
import re
import stat
import sys
from collections import Counter

import pytest

import irimi
from irimi.config import paths
from irimi.core import exchange
from irimi.core.exchange import Exchange, Flag
from irimi.core.limits import MAX_ERROR_MESSAGE, MAX_ERROR_TYPE
from irimi.protocol import trace
from irimi.protocol.trace import ErrorInfo, RunRecord, StoredRun, TelemetrySeen, ToolCall
from irimi.record import redact
from irimi.record.store.layout import SERVE_LOCK_FILE
from irimi.report.model import RunView, for_summary
from irimi.report.summary import NO_TRAFFIC, Live, summary_lines
from irimi.report.terminal import exchange_line, tool_call_line
from tests.e2e.workflows.agents.w10_flaky_upstream.scenarios import CONTROL_DOWN
from tests.e2e.workflows.harness.invariants import CANARIES
from tests.e2e.workflows.harness.result import TOOL_WRITE, Result
from tests.e2e.workflows.harness.scenario import cases, workflows

CASES = cases()
IDS = [f"{n}:{s}" for n, s in CASES]
# Every scenario under `irimi shadow -- <cmd>`, and every one that runs under serve mode.
MODE_CASES = [(n, s, mode) for mode in ("shadow", "serve") for n, s in CASES]
MODE_IDS = [f"{n}:{s}:{m}" for n, s, m in MODE_CASES]
# The workflows whose agents are plain scripts with no trigger or `sdk.run`: they send no
# `Irimi-Run`, so every exchange lands in the process run.
UNLABELLED = ("w09_scope_gauntlet",)
# W9's scenarios that do the SDK's job by hand, posting runs to the control endpoint.
BY_HAND_SCENARIOS = {
    ("w09_scope_gauntlet", "control_runs"),
    ("w09_scope_gauntlet", "control_hazards"),
}
# W10's scenarios that take the control endpoint away, so the SDK's posts fail: irimi knows
# each run only by its labelled requests, as the `header` run the first of them made.
RECORD_LOST = {("w10_flaky_upstream", s) for s in CONTROL_DOWN}
# The scenarios whose runs, but the process run, are all ones the SDK started (or W9 posted by
# hand): every SDK workflow's, but those whose SDK cannot reach irimi.
SDK_RUN_SCENARIOS = {
    (n, s) for n, s in CASES if n not in UNLABELLED and (n, s) not in RECORD_LOST
} | BY_HAND_SCENARIOS
# The scenarios of SDK workflows whose agent starts no run at all: nothing triggers one (W4's
# handshake, W6's decoration errors), or the trigger is never called (W5's forged signature).
NO_RUN_SCENARIOS = {
    ("w04_slack_ops_bot", "url_verification"),
    ("w05_dispute_responder", "bad_signature"),
    ("w06_crm_db_agent", "decoration_errors"),
}
# A run id the SDK mints: 16 lower-case hex characters.
RUN_ID = re.compile(r"[0-9a-f]{16}")


def started(result: Result) -> bool:
    """Return whether irimi printed its banner, so its engine ran and opened its store."""
    return any(
        line.startswith(("irimi shadow · run ", "irimi serve · run: "))
        and " · listening on " in line
        for line in result.irimi
    )


def is_quiet(result: Result) -> bool:
    """Return whether the run was given `--quiet`, so irimi printed no exchange or tool lines."""
    scenario = result.workflow.scenarios[result.scenario]
    flags = scenario.serve_flags if result.mode == "serve" else scenario.shadow_flags
    return "--quiet" in (flags or ())


def reported(result: Result) -> list[str]:
    """Return irimi's exchange lines, W4's webhook path swapped for the store's placeholder.

    Under `--quiet`, which prints none, the lines of the exchanges the engine reported."""
    lines = result.exchange_lines()
    if is_quiet(result):
        assert lines == [], lines
        lines = [exchange_line(e) for e in result.reported]
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
    argv = [sys.executable, "-m", workflow.module]
    assert run.trigger is not None
    assert trace.trigger_args(run.trigger) == {"argv": [*argv, *workflow.scenarios[scenario].argv]}
    assert run.end is not None
    assert (run.end.exit_code, run.end.outcome) == (
        shadow.exit_code,
        "ok" if shadow.exit_code == 0 else "error",
    )
    assert run.started_at is not None and run.end.ended_at is not None
    assert [r.dropped_events for r in records] == [0] * len(records)


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_every_exchange_irimi_reported_is_stored_exactly_once(run_workflow, name, scenario):
    """Each stored exchange prints as one of irimi's lines, and each telemetry line is stored once.

    Compared unordered, because W3's runs interleave freely."""
    shadow = run_workflow(name, scenario, "shadow")
    events = shadow.stored_events()
    lines = reported(shadow)
    stored = [exchange_line(e) for e in events if isinstance(e, Exchange)]
    assert sorted(stored) == sorted(line for line in lines if not is_telemetry(line))
    hosts = [line.split()[3].split("/", 1)[0] for line in lines if is_telemetry(line)]
    assert sorted(e.host for e in events if isinstance(e, TelemetrySeen)) == sorted(hosts)


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_every_stored_event_is_the_exchange_irimi_handed_its_store_redacted(
    run_workflow, name, scenario, mode
):
    """Each stored event, run by run in `seq` order, is the event irimi handed its store, redacted.

    The store gets exactly the exchanges irimi printed. Exchanges compare field by field after
    `redact.redact_exchange`, telemetry by host and time, tool calls with args, result and error
    redacted. Under shadow only W9's `control_hazards` posts anything to `unattributed`."""
    shadow = run_workflow(name, scenario, mode)
    assert len(shadow.handed) == len(shadow.reported) == len(reported(shadow))
    assert all(a is b for a, b in zip(shadow.handed, shadow.reported, strict=True))
    exchanges = [e for e in shadow.handed_events if isinstance(e, Exchange)]
    assert len(exchanges) == len(shadow.handed)
    assert all(a is b for a, b in zip(exchanges, shadow.handed, strict=True))
    if not started(shadow):
        return
    key = redact.load_key(shadow.home)
    reader = shadow.stored()
    runs = [r.run_id for r in reader.list_runs()] + [trace.UNATTRIBUTED]
    expected: dict[str, list[trace.Event]] = {run_id: [] for run_id in runs}
    for event in shadow.handed_events:
        if isinstance(event, ToolCall):
            expected[event.run_id].append(redacted_tool_call(event, key))
        elif event.kind == "telemetry":
            expected[event.run_id].append(
                TelemetrySeen(event.run_id, event.request.host, event.started_at)
            )
        else:
            expected[event.run_id].append(redact.redact_exchange(event, key))
    if mode == "shadow" and (name, scenario) != ("w09_scope_gauntlet", "control_hazards"):
        assert expected[trace.UNATTRIBUTED] == []
    for run_id in runs:
        stored = reader.load_run(run_id).events
        assert len(stored) == len(expected[run_id]), run_id
        for got, want in zip(stored, expected[run_id], strict=True):
            assert type(got) is type(want), run_id
            for f in dataclasses.fields(want):
                assert getattr(got, f.name) == getattr(want, f.name), (run_id, f.name)


def redacted_tool_call(call: ToolCall, key: bytes) -> ToolCall:
    """Return `call` as the store keeps it: args, result and error redacted whole, then cut."""
    error = call.error
    if error is not None:
        kind, message = (redact.redact_json(text, key) for text in (error.type, error.message))
        assert isinstance(kind, str) and isinstance(message, str)
        error = ErrorInfo(kind[:MAX_ERROR_TYPE], message[:MAX_ERROR_MESSAGE])
    return dataclasses.replace(
        call,
        args=redact.redact_json(call.args, key),
        result=redact.redact_json(call.result, key),
        error=error,
    )


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_every_store_is_private_and_holds_no_half_written_line(run_workflow, name, scenario, mode):
    """Every store directory is 0700 and file 0600, with no half-written line or temp file left.

    Every `events.jsonl` ends in a newline, a killed agent's (W10 `sigterm_mid_run`) included."""
    shadow = run_workflow(name, scenario, mode)
    root = shadow.home / paths.STORE_DIR_NAME
    if not started(shadow):
        return
    for path in [root, *root.rglob("*")]:
        mode = stat.S_IMODE(path.stat().st_mode)
        assert mode == (0o700 if path.is_dir() else 0o600), (oct(mode), path)
        assert not path.name.endswith(".tmp"), path
    for events in root.rglob("events.jsonl"):
        assert events.read_bytes().endswith(b"\n"), events


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_every_run_but_the_process_run_is_one_the_sdk_started(run_workflow, name, scenario, mode):
    """Every SDK workflow's runs are `sdk` runs with the posted trigger and versions, no exit code.

    A run id known only by its events (W10's `CONTROL_DOWN`) is a `header` run with no trigger or
    end, started when its first event did."""
    shadow = run_workflow(name, scenario, mode)
    if not started(shadow):
        return
    reader = shadow.stored()
    for record in reader.list_runs():
        if mode == "shadow" and record.run_id == shadow.process_run_id():
            continue
        if (name, scenario) in SDK_RUN_SCENARIOS:
            assert (record.attribution, record.mode) == ("sdk", "shadow")
            assert record.end is None or record.end.exit_code is None
            assert record.engine_version == irimi.__version__
            assert None not in (record.trigger, record.sdk_version, record.started_at)
            continue
        assert record.attribution == "header"
        assert (record.trigger, record.end) == (None, None)
        events = reader.load_run(record.run_id).events
        assert events != []
        assert record.started_at == events[0].started_at > 0


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_every_run_the_agent_logged_is_stored_with_the_outcome_it_logged(
    run_workflow, name, scenario, mode
):
    """The runs the agent logged are the `sdk` runs irimi stored, each with its logged outcome.

    Errors are stored by `module.qualname`; a run with no logged end (W10 `sigterm_mid_run`) has
    none, and `RECORD_LOST` runs are `header` runs. Every SDK scenario logs a run, but
    `NO_RUN_SCENARIOS`, so a scenario that stops starting runs fails."""
    shadow = run_workflow(name, scenario, mode)
    if not started(shadow):
        assert shadow.events("run.start") == []
        return
    stored = {r.run_id: r for r in shadow.sdk_runs()}
    logged = {e["run"]: e["name"] for e in shadow.events("run.start")}
    if (name, scenario) in BY_HAND_SCENARIOS:
        assert logged == {}
        return
    if name not in UNLABELLED:
        assert bool(logged) is ((name, scenario) not in NO_RUN_SCENARIOS), logged
    # Minted lower-case: on a case-insensitive filesystem, ids differing only in case would clash.
    assert all(RUN_ID.fullmatch(run_id) for run_id in logged), logged
    if (name, scenario) in RECORD_LOST:
        headers = {r.run_id for r in shadow.stored().list_runs() if r.attribution == "header"}
        assert (stored, headers) == ({}, set(logged))
        return
    assert {run_id: r.trigger.name for run_id, r in stored.items() if r.trigger} == logged
    ends = {e["run"]: e for e in shadow.events("run.end")}
    for run_id, record in stored.items():
        end = ends.get(run_id)
        if end is None:
            assert record.end is None
            continue
        assert record.end is not None and record.end.outcome == end["outcome"]
        assert record.end.ended_at is not None and record.started_at is not None
        assert record.end.ended_at >= record.started_at
        if end["outcome"] == "ok":
            assert record.end.error is None
        else:
            assert record.end.error is not None
            assert record.end.error.type.rsplit(".", 1)[-1] == end["error"]


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_every_request_a_run_sent_through_irimi_is_stored_in_that_run(
    run_workflow, name, scenario, mode
):
    """Each call the agent logged inside a run is stored in that run, whatever client sent it.

    Matched by method and host, or by host for telemetry. A run also holds irimi's check reads, so
    this is containment. Calls in no run (W3 `unpropagated_thread`) and `leaks` scenarios are
    skipped."""
    shadow = run_workflow(name, scenario, mode)
    if not started(shadow) or shadow.workflow.scenarios[scenario].leaks:
        return
    reader = shadow.stored()
    stored = {r.run_id for r in reader.list_runs()}
    made: dict[str, Counter] = {}
    for call in shadow.calls():
        if call.get("run") is None:
            continue
        host = str(call["url"]).split("/", 1)[0]
        made.setdefault(call["run"], Counter())[(call["method"], host)] += 1
    for run_id, calls in made.items():
        assert run_id in stored, run_id
        events = reader.load_run(run_id).events
        held = Counter(
            (e.request.method, e.request.host) for e in events if isinstance(e, Exchange)
        )
        # Telemetry is stored only as having happened, by host.
        seen = Counter(e.host for e in events if isinstance(e, TelemetrySeen))
        missing = calls - held
        missing -= Counter({(m, h): seen[h] for m, h in missing})
        assert not missing, (run_id, missing)


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_every_tool_call_the_agent_made_is_stored_in_its_run_and_printed_once(
    run_workflow, name, scenario, mode
):
    """Each run stores exactly the tool calls the agent logged in it, and irimi prints each once.

    `observe.py` logs which body ran in which run; calls in no run go to the process run, or to
    `unattributed` under serve. Compared as counts: a call is logged when it starts and stored when
    it ends (W6 `async_tools`). W9's hand-posted calls check only the lines."""
    bare = run_workflow(name, scenario, "bare")
    assert bare.tool_call_lines() == []
    assert {ran for _, ran in bare.tools()} <= {"real"}
    shadow = run_workflow(name, scenario, mode)
    stored = [e for e in shadow.stored_events() if isinstance(e, ToolCall)]
    printed = [] if is_quiet(shadow) else sorted(tool_call_line(c) for c in stored)
    assert sorted(shadow.tool_call_lines()) == printed
    if (name, scenario) in BY_HAND_SCENARIOS:
        return
    if not started(shadow):
        assert shadow.tools() == []
        return
    process = shadow.engine_run_id()
    logged = Counter(
        (t["run"] or process, t["name"], t["kind"], t["ran"]) for t in shadow.events("tool")
    )
    held = Counter((c.run_id, c.name, c.kind, c.ran) for c in stored)
    assert held == logged


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_a_bare_run_stores_nothing(run_workflow, name, scenario):
    """A bare run leaves its `IRIMI_HOME` with no store and no redaction key."""
    bare = run_workflow(name, scenario, "bare")
    assert bare.handed == bare.reported == []
    assert not (bare.home / paths.STORE_DIR_NAME).exists()
    assert not (bare.home / paths.REDACT_KEY_NAME).exists()


# An `UNLABELLED` workflow's exchanges all land in the process run, so its stored summary is the
# live one; W9's control scenarios label their reads with the runs they post.
UNLABELLED_CASES = [(n, s) for n, s in CASES if n in UNLABELLED and (n, s) not in SDK_RUN_SCENARIOS]
ELAPSED = re.compile(r" · \d+\.\ds$")


def no_elapsed(lines: list[str]) -> list[str]:
    """Return `lines` with the elapsed time masked, the one field live and stored may differ in."""
    return [ELAPSED.sub(" · <elapsed>", line) for line in lines]


# The summary's totals line, which a live summary of a process with SDK runs follows with them.
TOTALS = re.compile(r"  \d+ exchanges? · \d+ real · ")
WHOLE_PROCESS = " (whole process) · "


def process_part(live: list[str]) -> list[str]:
    """Return `live` without its `(whole process)` header clause and the SDK run lines after it."""
    end = next(i for i, line in enumerate(live) if TOTALS.match(line)) + 1
    return [live[0].replace(WHOLE_PROCESS, " · ", 1), *live[1:end]]


@pytest.mark.parametrize(
    ("name", "scenario"), UNLABELLED_CASES, ids=[f"{n}:{s}" for n, s in UNLABELLED_CASES]
)
def test_an_unlabelled_agent_s_process_run_prints_the_summary_irimi_shadow_printed(
    run_workflow, name, scenario
):
    """`runs show` of an unlabelled agent's process run ends in the summary `irimi shadow` printed.

    Line for line but for the elapsed time. W9 covers echo and fixture answers, retries and
    conflicts, unreadable bodies and a check read with no response."""
    shadow = run_workflow(name, scenario, "shadow")
    live = shadow.summary()
    assert live, "irimi printed no summary"
    assert [r.attribution for r in shadow.stored().list_runs()] == ["process"]
    code, out, err = shadow.runs("show", shadow.process_run_id())
    assert (code, err) == (0, [])
    assert no_elapsed(out[-len(live) :]) == no_elapsed(live)


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_every_run_the_process_stored_prints_the_summary_irimi_shadow_printed(
    run_workflow, name, scenario
):
    """A summary over all of a process's stored runs is the summary `irimi shadow` printed live.

    Exchanges are put back in the order they ended, since a read hangs under the write before it;
    tool calls stay as stored, since the summary orders them by end. W4's webhook post checks a
    route found by its stored operation."""
    shadow = run_workflow(name, scenario, "shadow")
    live = shadow.summary()
    if not live:
        return
    reader = shadow.stored()
    ids = [record.run_id for record in reader.list_runs()] + [trace.UNATTRIBUTED]
    events = [event for run_id in ids for event in reader.load_run(run_id).events]
    exchanges = sorted((e for e in events if isinstance(e, Exchange)), key=lambda e: e.ended_at)
    telemetry = [e for e in events if isinstance(e, TelemetrySeen)]
    tool_calls = [e for e in events if isinstance(e, ToolCall)]
    process = reader.load_run(shadow.process_run_id()).record
    union = StoredRun(process, [*exchanges, *telemetry, *tool_calls])
    stored = summary_lines(RunView(union.record, union.events), shadow.maps())
    assert no_elapsed(stored) == no_elapsed(process_part(live))


# -- serve mode -----------------------------------------------------------------------------------


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_every_exchange_and_tool_call_a_serve_reported_is_stored_once_in_its_run(
    run_workflow, name, scenario
):
    """Serve stores each line it printed once, in the run that named it or in `unattributed/`."""
    serve = run_workflow(name, scenario, "serve")
    reader = serve.stored()
    ids = [record.run_id for record in reader.list_runs()] + [trace.UNATTRIBUTED]
    events = {run_id: reader.load_run(run_id).events for run_id in ids}
    flat = [event for run_events in events.values() for event in run_events]
    stored = [exchange_line(e) for e in flat if isinstance(e, Exchange)]
    assert sorted(stored) == sorted(line for line in reported(serve) if not is_telemetry(line))
    assert all(e.run_id == run_id for run_id, run_events in events.items() for e in run_events)
    calls = [e for e in flat if isinstance(e, ToolCall)]
    printed = [] if is_quiet(serve) else sorted(tool_call_line(c) for c in calls)
    assert sorted(serve.tool_call_lines()) == printed


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_a_serve_stores_no_process_run_and_drops_nothing(run_workflow, name, scenario):
    """Serve stores only `sdk` and `header` shadow runs, none dropping an event, and no process run.

    A serve that refused to start (W8 `map_refused`) opens no store."""
    serve = run_workflow(name, scenario, "serve")
    if not started(serve):
        assert not (serve.home / paths.STORE_DIR_NAME).exists()
        return
    records = serve.stored().list_runs()
    assert {r.attribution for r in records} <= {"sdk", "header"}
    assert {r.mode for r in records} <= {"shadow"}
    assert [r.dropped_events for r in records] == [0] * len(records)
    assert (serve.home / paths.STORE_DIR_NAME / SERVE_LOCK_FILE).is_file()


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_all_of_a_serves_runs_together_are_what_it_reported_live(run_workflow, name, scenario):
    """A summary over all of a serve's stored runs equals the summary of what it reported live.

    So redacting and storing changes nothing a summary shows, and no event is lost between runs."""
    serve = run_workflow(name, scenario, "serve")
    if not started(serve):
        return
    reader = serve.stored()
    ids = [record.run_id for record in reader.list_runs()] + [trace.UNATTRIBUTED]
    events = [event for run_id in ids for event in reader.load_run(run_id).events]
    exchanges = sorted((e for e in events if isinstance(e, Exchange)), key=lambda e: e.ended_at)
    telemetry = [e for e in events if isinstance(e, TelemetrySeen)]
    tool_calls = [e for e in events if isinstance(e, ToolCall)]
    union = StoredRun(RunRecord.header(trace.UNATTRIBUTED), [*exchanges, *telemetry, *tool_calls])
    maps = serve.maps()
    handed_calls = [e for e in serve.handed_events if isinstance(e, ToolCall)]
    events = [*map(for_summary, serve.reported), *handed_calls]
    view = RunView(RunRecord.header(trace.UNATTRIBUTED), events)
    live = summary_lines(view, maps, 0.0, Live())
    # The one line only a live block prints, for a run that saw no traffic.
    live = [line for line in live if line != f"  {NO_TRAFFIC}"]
    assert summary_lines(RunView(union.record, union.events), maps) == live


# -- irimi compare --------------------------------------------------------------------------------


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_irimi_compare_of_every_stored_run_with_itself_is_its_summary_s_writes(
    run_workflow, name, scenario, mode
):
    """`irimi compare r r` prints one `=` row per write line `runs show r`'s summary printed.

    A write whose body was cut when stored (W9 `bombs`) is a `?` pair counted as not compared,
    and the run exits 1. Compare interleaves HTTP and tool writes in `seq` order, so each kind is
    checked in order separately."""
    shadow = run_workflow(name, scenario, mode)
    if not started(shadow):
        return
    reader = shadow.stored()
    ids = [record.run_id for record in reader.list_runs()] + [trace.UNATTRIBUTED]
    for run_id in ids:
        label, writes = shadow.shown_writes(run_id)
        if run_id == trace.UNATTRIBUTED and not reader.load_run(run_id, bodies=False).events:
            # `runs show` invents no run for an empty pool, so there is no label to compare.
            assert label == ""
            code, out, err = shadow.compare_runs(run_id, run_id)
            assert (code, err, out[-1]) == (
                0,
                [],
                "  0 same · 0 changed · 0 only in A · 0 only in B",
            ), run_id
            continue
        # Whether each HTTP write the summary printed, in `seq` order, was stored cut.
        cut = [
            Flag.BODY_TRUNCATED in e.flags
            for e in reader.load_run(run_id, bodies=False).events
            if isinstance(e, Exchange) and exchange.is_intercepted(e)
        ]
        code, out, err = shadow.compare_runs(run_id, run_id)
        assert (code, err) == (1 if any(cut) else 0, []), run_id
        assert out[:4] == [
            f"irimi compare · {run_id} vs {run_id}",
            f"  A  {label}".rstrip(),
            f"  B  {label}".rstrip(),
            "",
        ]
        totals = f"  {len(writes) - sum(cut)} same · 0 changed · 0 only in A · 0 only in B"
        if any(cut):
            totals += f" · {sum(cut)} not compared (a body was cut when stored)"
        if not writes:
            assert out[4:] == [totals], run_id
            continue
        assert out[-2:] == ["", totals], run_id
        rows = out[4:-2]
        # Each row as (its mark, its line): a `?` pair's B line must be its A line.
        compared: list[tuple[str, str]] = []
        while rows:
            if rows[0].startswith("  ? A "):
                assert rows[1] == f"    B {rows[0][6:]}", rows[:2]
                compared.append(("?", rows[0][6:]))
                rows = rows[2:]
            else:
                assert rows[0].startswith("  = "), rows[0]
                compared.append(("=", rows[0][4:]))
                rows = rows[1:]
        http = [w for w in writes if not TOOL_WRITE.match(w)]
        assert len(http) == len(cut), (run_id, http, cut)
        assert [row for row in compared if not TOOL_WRITE.match(row[1])] == [
            ("?" if c else "=", w) for w, c in zip(http, cut, strict=True)
        ], run_id
        assert [row for row in compared if TOOL_WRITE.match(row[1])] == [
            ("=", w) for w in writes if TOOL_WRITE.match(w)
        ], run_id


# It reads every scenario's stored runs in `mode`: over 120 s on a loaded worker.
@pytest.mark.timeout(600)
@pytest.mark.parametrize("mode", ["shadow", "serve"])
def test_the_corpus_gives_irimi_compare_s_universal_check_something_to_catch(run_workflow, mode):
    """The corpus gives the compare check above every kind of write to meet, in each mode.

    A faked write, one that would fail, a write tool's fake, a cut body, interleaved HTTP and tool
    writes, and under serve a write in `unattributed`."""
    seen: Counter[str] = Counter()
    for name, scenario in CASES:
        result = run_workflow(name, scenario, mode)
        if not started(result):
            continue
        reader = result.stored()
        ids = [record.run_id for record in reader.list_runs()] + [trace.UNATTRIBUTED]
        for run_id in ids:
            events = reader.load_run(run_id, bodies=False).events
            actions = [
                e
                for e in events
                if (isinstance(e, Exchange) and exchange.is_intercepted(e))
                or (isinstance(e, ToolCall) and exchange.fake_ran(e))
            ]
            writes = [e for e in actions if isinstance(e, Exchange)]
            kinds = [isinstance(e, ToolCall) for e in actions]
            seen["answered"] += any(e.precondition.outcome != "rejected" for e in writes)
            seen["would fail"] += any(e.precondition.outcome == "rejected" for e in writes)
            seen["stood in"] += any(kinds)
            seen["cut"] += any(Flag.BODY_TRUNCATED in e.flags for e in writes)
            seen["interleaved"] += kinds != sorted(kinds)
            seen["unattributed"] += run_id == trace.UNATTRIBUTED and bool(actions)
    expected = ["answered", "would fail", "stood in", "cut", "interleaved"]
    expected += ["unattributed"] if mode == "serve" else []
    assert [kind for kind in expected if not seen[kind]] == []
