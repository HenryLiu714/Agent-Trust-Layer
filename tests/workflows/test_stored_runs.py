"""What every shadow run leaves in its trace store (#70), over every scenario of every workflow.

`irimi shadow` stores under `$IRIMI_HOME/store`, and the harness gives every run a home of its
own, so each scenario's store holds that run and nothing else (`Result.stored()`). The promises:
a run that started irimi stores exactly one process run, with the agent's argv and exit; every
exchange irimi reported on its terminal is on disk exactly once, and is, field by field, the
exchange the engine handed the store with its secrets redacted; the store is private and holds no
half-written line; and a bare run, with no irimi, stores nothing.

Every scenario runs under `irimi shadow --serve` too (#77), and the promises that do not depend on
irimi having launched the agent are held there as well (`MODE_CASES`): no process run is stored,
and a request or tool call no run names lands in `unattributed/` instead of in it.
"""

import dataclasses
import re
import stat
import sys
from collections import Counter

import pytest

import irimi
from examples.workflows.harness.run import CANARIES, TOOL_WRITE, Result, serve_cases, workflows
from examples.workflows.w10_flaky_upstream.scenarios import CONTROL_DOWN
from irimi import paths, redact, report, trace
from irimi.exchange import BODY_TRUNCATED_FLAG, Exchange
from irimi.store import SERVE_LOCK_FILE, StoredRun, header_record
from irimi.trace import MAX_ERROR_MESSAGE, MAX_ERROR_TYPE, ErrorInfo, TelemetrySeen, ToolCall

CASES = [(name, scenario) for name, wf in workflows().items() for scenario in wf.scenarios]
IDS = [f"{n}:{s}" for n, s in CASES]
# Every scenario under `irimi shadow -- <cmd>`, and every one that runs under serve mode (#77).
MODE_CASES = [(n, s, "shadow") for n, s in CASES] + [(n, s, "serve") for n, s in serve_cases()]
MODE_IDS = [f"{n}:{s}:{m}" for n, s, m in MODE_CASES]
# The workflows whose agents are plain scripts, with no `@sdk.trigger` and no `sdk.run`: they send
# no `Irimi-Run`, so every exchange they make lands in the process run. W11 was one until each of
# its escapes became an `sdk.run` (#75).
UNLABELLED = ("w09_scope_gauntlet",)
# W9's scenarios that do the SDK's job by hand, posting runs to the control endpoint (#73).
BY_HAND_SCENARIOS = {
    ("w09_scope_gauntlet", "control_runs"),
    ("w09_scope_gauntlet", "control_hazards"),
}
# W10's scenarios that take the control endpoint away, so the SDK's posts fail (#74): irimi knows
# each run only by its labelled requests, as the `header` run the first of them made.
RECORD_LOST = {("w10_flaky_upstream", s) for s in CONTROL_DOWN}
# The scenarios whose runs, but the process run, are all ones the SDK started (#74): every
# scenario of every workflow that uses the SDK, but those whose SDK cannot reach irimi, and W9's
# by hand.
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
# A run id the SDK mints: 16 lower-case hex characters (#74).
RUN_ID = re.compile(r"[0-9a-f]{16}")


def started(result: Result) -> bool:
    """irimi printed its banner, `irimi shadow`'s or serve's (#77): its engine ran, so it had
    opened its store."""
    return any(
        line.startswith(("irimi shadow · run ", "irimi shadow --serve · run: "))
        and " · listening on " in line
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


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_every_stored_event_is_the_exchange_irimi_handed_its_store_redacted(
    run_workflow, name, scenario, mode
):
    """#70's end-to-end comparison over the whole corpus. The engine handed its store exactly the
    exchanges irimi printed, the same objects in the same order. Run by run, in `seq` order, each
    stored event is one of them: an exchange equal to itself after `redact.redact_exchange`,
    compared over every field of `Exchange`, and telemetry reduced to its host and time. No
    corpus run names an id the store refuses, so nothing is unattributed but the tool call W9's
    `control_hazards` posts there by hand (#76).

    A tool call the control endpoint handed the store (#73) is in its run's order among them,
    stored with its args, result and error redacted (#70, #179). Under serve mode (#77) what no
    run names is `unattributed`, so there it is no exception."""
    shadow = run_workflow(name, scenario, mode)
    assert len(shadow.handed) == len(shadow.reported) == len(shadow.exchange_lines())
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
    """`call` as the store keeps it: its args, its result and its error redacted (#70), the error's
    type and message each whole, and only then cut to what a trace keeps (#179)."""
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
    """What the store keeps is redacted, not public: every directory it made is 0700 and every
    file 0600, as the CA key is (#70), serve's `serve.lock` included (#77). Every `events.jsonl`
    ends in a newline - a killed agent (W10 `sigterm_mid_run`) included - so a reader skips no
    truncated last line, and no temp file of an atomic write is left behind."""
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
    """Every SDK workflow's runs are ones the SDK started (#74), and so are W9's by hand (#73):
    `attribution: "sdk"`, with the trigger, SDK version and start it posted, the engine's version,
    and no exit code, which is the process run's alone. None is a `header` run, which would mean
    an exchange named a run whose start never reached irimi.

    A `header` run is still what a run id known only by its events becomes: its record is the one
    the first of them created, with no trigger and no end (#70), and it started when that event
    did, so `irimi runs list` sorts it among the runs that started (#72). W10's `CONTROL_DOWN`
    makes one: its SDK cannot post the run's start, but its requests still name the run. Under serve
    mode (#77) there is no process run, and every run is one of these."""
    shadow = run_workflow(name, scenario, mode)
    if not started(shadow):
        return
    reader = shadow.stored()
    for record in reader.list_runs():
        if mode == "shadow" and record.run_id == shadow.process_run_id():
            continue
        if (name, scenario) in SDK_RUN_SCENARIOS:
            assert (record.attribution, record.mode, record.exit_code) == ("sdk", "shadow", None)
            assert record.engine_version == irimi.__version__
            assert None not in (record.trigger, record.sdk_version, record.started_at)
            continue
        assert record.attribution == "header"
        assert (record.trigger, record.ended_at) == (None, None)
        assert (record.outcome, record.exit_code, record.error) == (None, None, None)
        events = reader.load_run(record.run_id).events
        assert events != []
        assert record.started_at == events[0].started_at > 0


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_every_run_the_agent_logged_is_stored_with_the_outcome_it_logged(
    run_workflow, name, scenario, mode
):
    """The SDK end to end (#74): the runs the agent saw are the runs irimi stored. Each `run.start`
    the corpus logged, with the real run id, is one `sdk` run with that trigger name, and no `sdk`
    run is stored that the agent did not log, but W9's by hand. Each `run.end` is that run's
    outcome, its error stored under the exception's `module.qualname` where the log has the class
    name alone. A run the agent logged no end for - W10 `sigterm_mid_run`, killed - is stored with
    no outcome: incomplete. A run whose SDK could not reach irimi (`RECORD_LOST`) is the `header`
    run its requests made. Every scenario of an SDK workflow logs a run, but `NO_RUN_SCENARIOS`, so
    this check is never met by a scenario that silently stopped starting them. The same holds under
    serve mode (#77), whose agent irimi did not start."""
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
    # Minted lower-case: an id that differed from another only in case would be filed in
    # `unattributed/` on a case-insensitive filesystem (#73).
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
            assert (record.outcome, record.ended_at, record.error) == (None, None, None)
            continue
        assert record.outcome == end["outcome"]
        assert record.ended_at is not None and record.started_at is not None
        assert record.ended_at >= record.started_at
        if end["outcome"] == "ok":
            assert record.error is None
        else:
            assert record.error is not None
            assert record.error.type.rsplit(".", 1)[-1] == end["error"]


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_every_request_a_run_sent_through_irimi_is_stored_in_that_run(
    run_workflow, name, scenario, mode
):
    """#75 end to end: the SDK labels each request a run sends to irimi with the run's id, read
    as the request is sent, so irimi stores it in that run, whichever client, thread, task or
    pooled connection sent it. Each call the agent logged inside a run (its `run`, the context's
    id when it was made) is one of that run's stored exchanges, by method and host, or its
    telemetry, by host: a run also holds irimi's own L3 reads, so this is containment, not
    equality. A call that belonged to no run is not checked here (W3 `unpropagated_thread` pins
    where it lands), nor a `leaks` scenario, whose call went around irimi on purpose (W11). The SDK
    labels the same way under serve mode (#77)."""
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
        # Telemetry is stored only as having happened, by host (#70).
        seen = Counter(e.host for e in events if isinstance(e, TelemetrySeen))
        missing = calls - held
        missing -= Counter({(m, h): seen[h] for m, h in missing})
        assert not missing, (run_id, missing)


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_every_tool_call_the_agent_made_is_stored_in_its_run_and_printed_once(
    run_workflow, name, scenario, mode
):
    """`@sdk.tool` end to end (#76): what irimi recorded is what the agent did. The corpus's `sdk`
    wraps each tool's real function and stand-in to log which ran, in which run (`tool` events),
    and that log is the witness the store is checked against: under shadow, each run holds exactly
    the calls the agent logged in it, by name, kind and which body ran, a call made in no run held
    by the process run (`IRIMI_RUN`), or under serve mode by `unattributed` (#77); none is lost,
    duplicated, mixed between runs or left unattributed under shadow, whatever thread or task made
    it. Compared as counts per run, not in order: a
    call is stored when it ends and logged when its body starts, so a tool awaited inside another's
    coroutine (W6 `async_tools`) is logged second and stored first. irimi printed one line per
    stored call, and none for anything else.

    W9's control scenarios post tool calls by hand, not through a tool, so only the lines are
    checked there. Bare, the SDK is inert: every tool ran its real function, irimi printed nothing
    and stored nothing (`test_a_bare_run_stores_nothing`)."""
    bare = run_workflow(name, scenario, "bare")
    assert bare.tool_call_lines() == []
    assert {ran for _, ran in bare.tools()} <= {"real"}
    shadow = run_workflow(name, scenario, mode)
    stored = [e for e in shadow.stored_events() if isinstance(e, ToolCall)]
    assert sorted(shadow.tool_call_lines()) == sorted(report.tool_call_line(c) for c in stored)
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
    """No irimi, no store: the bare baseline leaves its `IRIMI_HOME` without a store or a
    redaction key, whatever the agent's SDK stand-in does."""
    bare = run_workflow(name, scenario, "bare")
    assert bare.handed == bare.reported == []
    assert not (bare.home / paths.STORE_DIR_NAME).exists()
    assert not (bare.home / paths.REDACT_KEY_NAME).exists()


# The `UNLABELLED` workflows' every exchange lands in the process run, so the process run's stored
# summary is the one `irimi shadow` printed (#72). W9's control scenarios are the exception: they
# label their reads with the runs they post (#73).
UNLABELLED_CASES = [(n, s) for n, s in CASES if n in UNLABELLED and (n, s) not in SDK_RUN_SCENARIOS]
ELAPSED = re.compile(r" · \d+\.\ds$")


def no_elapsed(lines: list[str]) -> list[str]:
    """The live summary times the child and a stored one its run record: the one field that may
    differ (#72)."""
    return [ELAPSED.sub(" · <elapsed>", line) for line in lines]


@pytest.mark.parametrize(
    ("name", "scenario"), UNLABELLED_CASES, ids=[f"{n}:{s}" for n, s in UNLABELLED_CASES]
)
def test_an_unlabelled_agent_s_process_run_prints_the_summary_irimi_shadow_printed(
    run_workflow, name, scenario
):
    """#72's invariant on the corpus, through the real CLI: `irimi runs show <process run>` ends in
    the block `irimi shadow` printed on exit, line for line but for the elapsed seconds. W9 covers
    L0 and L1 fakes, idempotent replays and conflicts, unreadable bodies and an engine read with no
    response.

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


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=IDS)
def test_every_run_the_process_stored_prints_the_summary_irimi_shadow_printed(
    run_workflow, name, scenario
):
    """#72's invariant for an agent that labels its runs. The live summary is the process's: every
    exchange the proxy saw, whatever run each named. Its runs together hold the same events, so a
    summary over all of them, the unattributed ones included, is the block `irimi shadow` printed,
    line for line but for the elapsed seconds.

    The events are put back in the order the engine reported them, the order they ended in, since
    a read hangs under the write before it. W4's webhook post is the case this caught: its stored
    path is a placeholder that no route matches, and its line lost its `human:` sentence until the
    route was found by its stored operation instead.

    The tool calls too, each a line under the writes when its stand-in ran (#76): W1's reply, W2's
    forty ledger marks and W6's six database and file writes. They are left as stored, run by run
    and each run's in `seq` order: the summary prints them in the order they ended, as the live
    one did, whatever order their posts reached irimi in (#120)."""
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
    assert no_elapsed(report.stored_summary_lines(union, shadow.maps())) == no_elapsed(live)


# -- serve mode (#77) ------------------------------------------------------------------------------

SERVE_CASES = serve_cases()
SERVE_IDS = [f"{n}:{s}" for n, s in SERVE_CASES]


@pytest.mark.parametrize(("name", "scenario"), SERVE_CASES, ids=SERVE_IDS)
def test_every_exchange_and_tool_call_a_serve_reported_is_stored_once_in_its_run(
    run_workflow, name, scenario
):
    """Under `irimi shadow --serve` (#77) what irimi printed as it happened is what it stored:
    each exchange line and each tool-call line once, and each event in the run that named it, or
    in `unattributed/` when none did. No process run holds the rest. Which run holds each tool
    call and each request is checked against the agent's own log by the checks above, run in
    both modes."""
    serve = run_workflow(name, scenario, "serve")
    reader = serve.stored()
    ids = [record.run_id for record in reader.list_runs()] + [trace.UNATTRIBUTED]
    events = {run_id: reader.load_run(run_id).events for run_id in ids}
    flat = [event for run_events in events.values() for event in run_events]
    stored = [report.exchange_line(e) for e in flat if isinstance(e, Exchange)]
    assert sorted(stored) == sorted(line for line in reported(serve) if not is_telemetry(line))
    assert all(e.run_id == run_id for run_id, run_events in events.items() for e in run_events)
    calls = [e for e in flat if isinstance(e, ToolCall)]
    assert sorted(serve.tool_call_lines()) == sorted(report.tool_call_line(c) for c in calls)


@pytest.mark.parametrize(("name", "scenario"), SERVE_CASES, ids=SERVE_IDS)
def test_a_serve_stores_no_process_run_and_drops_nothing(run_workflow, name, scenario):
    """Serve mode starts no process run (#77): every run it stored is one a request or the SDK
    named, decided in shadow mode, and none dropped an event. A serve that refused to start (W8
    `map_refused`) opened no store."""
    serve = run_workflow(name, scenario, "serve")
    if not started(serve):
        assert not (serve.home / paths.STORE_DIR_NAME).exists()
        return
    records = serve.stored().list_runs()
    assert {r.attribution for r in records} <= {"sdk", "header"}
    assert {r.mode for r in records} <= {"shadow"}
    assert [r.dropped_events for r in records] == [0] * len(records)
    assert (serve.home / paths.STORE_DIR_NAME / SERVE_LOCK_FILE).is_file()


@pytest.mark.parametrize(("name", "scenario"), SERVE_CASES, ids=SERVE_IDS)
def test_all_of_a_serves_runs_together_are_what_it_reported_live(run_workflow, name, scenario):
    """#72's invariant under serve mode (#77): "all of a process's runs together (unattributed
    included, in the order the exchanges ended) are the live block". Serve prints no closing block,
    so the live block is the summary of what it reported as it happened: every exchange it printed
    a line for, in the order they came, and every tool call the control endpoint passed on, which
    the summary prints in the order they ended (#120), so the stored ones are left run by run.
    That is the summary over every stored run and `unattributed/` together: redacting and storing
    them changed nothing a summary shows, and no event was lost between runs."""
    serve = run_workflow(name, scenario, "serve")
    if not started(serve):
        return
    reader = serve.stored()
    ids = [record.run_id for record in reader.list_runs()] + [trace.UNATTRIBUTED]
    events = [event for run_id in ids for event in reader.load_run(run_id).events]
    exchanges = sorted((e for e in events if isinstance(e, Exchange)), key=lambda e: e.ended_at)
    telemetry = [e for e in events if isinstance(e, TelemetrySeen)]
    tool_calls = [e for e in events if isinstance(e, ToolCall)]
    union = StoredRun(header_record(trace.UNATTRIBUTED), [*exchanges, *telemetry, *tool_calls])
    maps = serve.maps()
    handed_calls = [e for e in serve.handed_events if isinstance(e, ToolCall)]
    live = report.summary_lines(trace.UNATTRIBUTED, serve.reported, 0.0, maps, handed_calls)
    assert report.stored_summary_lines(union, maps) == live


# -- irimi compare (#79) ---------------------------------------------------------------------------

NOT_A_VERDICT = "a comparison, not a verdict: nothing was replayed"


@pytest.mark.parametrize(("name", "scenario", "mode"), MODE_CASES, ids=MODE_IDS)
def test_irimi_compare_of_every_stored_run_with_itself_is_its_summary_s_writes(
    run_workflow, name, scenario, mode
):
    """#79 over the whole corpus, through the real CLI: `irimi compare <r> <r>`, for every run the
    store holds and `unattributed`, prints its header, one row for each write line `irimi runs
    show <r>`'s summary printed, and the totals, or the totals alone for a run that wrote nothing.
    Each side is named as `runs show` names the run.

    Each row is `=`, but for a write whose body was cut when stored (W9 `bombs`, #94): irimi never
    calls a cut write the same, not even as itself, so it is a `?` pair whose two lines are its
    summary line, the totals count it as not compared, and the run exits 1. Every other run exits
    0. The summary prints a run's HTTP writes and then its tool writes, each in `seq` order;
    compare prints the two interleaved in `seq` order. So each kind is compared in order, and the
    totals count both. Under serve mode (#77) `unattributed` holds what no run named."""
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
            BODY_TRUNCATED_FLAG in e.flags
            for e in reader.load_run(run_id, bodies=False).events
            if isinstance(e, Exchange) and report.is_intercepted(e)
        ]
        code, out, err = shadow.compare_runs(run_id, run_id)
        assert (code, err) == (1 if any(cut) else 0, []), run_id
        assert out[:4] == [
            f"irimi compare · {run_id} vs {run_id} · {NOT_A_VERDICT}",
            f"  A  {label}",
            f"  B  {label}",
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


@pytest.mark.parametrize("mode", ["shadow", "serve"])
def test_the_corpus_gives_irimi_compare_s_universal_check_something_to_catch(run_workflow, mode):
    """The check above passes by default over runs that wrote nothing. Across the corpus it must
    meet, in each mode: a write irimi answered, one it says would fail, a write tool stood in, a
    write whose body was cut when stored, a run whose HTTP and tool writes interleave in `seq`
    order (so compare's order is not the summary's), and under serve mode (#77), where what no run
    names is `unattributed`, `unattributed` holding a write."""
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
                if (isinstance(e, Exchange) and report.is_intercepted(e))
                or (isinstance(e, ToolCall) and report.is_stood_in(e))
            ]
            writes = [e for e in actions if isinstance(e, Exchange)]
            kinds = [isinstance(e, ToolCall) for e in actions]
            seen["answered"] += any(e.precondition != "rejected" for e in writes)
            seen["would fail"] += any(e.precondition == "rejected" for e in writes)
            seen["stood in"] += any(kinds)
            seen["cut"] += any(BODY_TRUNCATED_FLAG in e.flags for e in writes)
            seen["interleaved"] += kinds != sorted(kinds)
            seen["unattributed"] += run_id == trace.UNATTRIBUTED and bool(actions)
    expected = ["answered", "would fail", "stood in", "cut", "interleaved"]
    expected += ["unattributed"] if mode == "serve" else []
    assert [kind for kind in expected if not seen[kind]] == []
