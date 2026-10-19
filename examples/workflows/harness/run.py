"""Run a sample workflow's scenario, bare or under `irimi shadow`, and check what every run owes.

A workflow is a package `examples/workflows/wNN_<name>/` holding an agent (`agent.py`, started
through `examples.workflows.launch` so it keeps its real module name) and a `scenarios.py` that
defines `WORKFLOW`. A scenario seeds the fake services, names the agent's argv and environment,
and says whether anything about it is expected to differ from a bare run.

`run()` is the one place a scenario is executed:

- **bare**: the agent runs as a plain subprocess with `HTTP_PROXY` pointing at the fake internet.
  Its writes really land there. This is the baseline.
- **shadow**: `irimi shadow -- <agent>` runs in THIS process through `irimi.cli.main`, with the
  fake internet resolving its hosts, so irimi's real shipped maps, policy and overlay decide every
  call. Its writes must not land anywhere.
- **serve**: `irimi shadow --serve` runs in THIS process too, on a thread of its own, because the
  fake internet resolves names only here; the agent runs beside it as its own process, pointed at
  it by the variables the README's "Serve mode" names, and irimi is stopped, by its `stop` event
  rather than a signal, when the agent exits (#77). Every scenario runs this way, with its own
  `Scenario.serve_flags`. A serve that refuses to start (a map it will not load) starts no agent,
  as `irimi shadow` starts none.

- **replay**: one run a scenario stored under `irimi shadow` is replayed by `irimi replay <run id>`
  (#84), in THIS process too, in a copy of the recording's `IRIMI_HOME` and store, so the replay
  never adds its run to a recording another test reads, with a fresh `World` from the same `setup`,
  so anything not answered from the recording reaches the fake internet and shows up there. The
  replay runs what `irimi replay` runs for that run: its trigger, called by `irimi.sdk.replay_child`
  from its entrypoint and args, or a process run's argv; or, with `command`, the scenario's own
  agent command (`-- <cmd>`). Every run the SDK starts in the replay's child adopts the replay's
  one run. `replay()` runs it; `run()` never does, since there is nothing to replay before a
  recording exists. `sdk_replay()` calls `sdk.replay()` the same way, from this process.

A later mode is a new branch here and not a new harness.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import os
import pkgutil
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from examples.workflows import sdk as agent_sdk
from examples.workflows.harness.internet import BOMB_SECRET, FakeInternet, Req
from examples.workflows.harness.services import CLIENT_SECRET_CANARY, World
from irimi import ca, cli, paths, report, runner, sdk, servicemap, trace
from irimi import store as irimi_store
from irimi.cli import build_parser
from irimi.cli import main as irimi_main
from irimi.exchange import Exchange
from irimi.sdk import replays as sdk_replays
from irimi.servicemap import MapIndex, loader
from irimi.store import StoreReader
from irimi.trace import RunRecord, ToolCall

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS_DIR = REPO_ROOT / "examples" / "workflows"

# Every credential a workflow is given is a canary: fake, recognisable, and greppable. The fake
# services accept them by prefix. A canary found on disk under irimi's home is a redaction leak.
CANARIES = {
    "STRIPE_API_KEY": "sk_test_CANARYstripe0000000000000000",
    "SLACK_BOT_TOKEN": "xoxb-CANARY-slack-0000000000",
    "SLACK_SIGNING_SECRET": "CANARYslacksigning00000000000000",
    "STRIPE_WEBHOOK_SECRET": "whsec_CANARYwebhook000000000000000",
    "ANTHROPIC_API_KEY": "sk-ant-CANARYanthropic0000000000000",
    "OPENAI_API_KEY": "sk-CANARYopenai000000000000000000000",
    "SLACK_WEBHOOK_PATH": "/services/T0CANARY/B0CANARY/CANARYwebhookpath000000",
}
# And every credential a fake service hands back in a response body. A live read's response is
# stored, so one found on disk is a leak on the response side, which the canaries above, all sent
# by the agent, never reach (#70). Not given to the agent: it is the service's to hand out. A
# bomb's first line holds one, so the start of a body cut at irimi's decoding cap is held to it
# too (#94).
SERVED_CANARIES = {"STRIPE_CLIENT_SECRET": CLIENT_SECRET_CANARY, "BOMB_SECRET": BOMB_SECRET}
MODES = ("bare", "shadow", "serve")
# The line irimi's serve mode prints above each run's summary when the SDK ends the run (#77).
RUN_BLOCK = re.compile(r"--- run (\S+) ---")
# The run `irimi shadow`'s banner names, its process run (#70).
BANNER_RUN = re.compile(r"irimi (?:shadow|replay) · run (\S+) · ")
# How long a serve-mode run waits for irimi's banner, and then for irimi to stop.
SERVE_TIMEOUT_S = 60
# What the name of a thread the fake internet answers a request on ends in: the one kind of
# thread a serve run may leave behind it.
FAKE_HANDLER = "(process_request_thread)"
# How long the threads a serve started have to end once it has returned.
THREAD_EXIT_S = 5.0
# A tool call's line in irimi's stream (`report.tool_call_line`, #76). Its first column is `ran`,
# and its second the tool's kind followed by ` tool `, which no exchange line has: `recording` is
# also an `answered_by` an exchange line starts with (#82).
TOOL_LINE = re.compile(r"(real|shadow|recording) +(read|write) +tool .* -> ")
# A write tool's line in a summary, without its indent (`report.tool_write_line`, #76). The summary
# prints these after the HTTP writes, and `irimi compare` among them, in `seq` order (#79).
TOOL_WRITE = re.compile(rf"{report.WRITE_MARKER} tool \S+  shadow stand-in ")
# How a summary's write line starts, a write irimi answered or one it says would fail (#45).
WRITE_LINE_PREFIXES = (f"  {report.WRITE_MARKER} ", f"  {report.REJECTED_MARKER} ")
PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy")
# The only variables an agent inherits from the harness's own environment. Everything else it is
# given by name, so a developer's shell cannot change a run: an exported `IRIMI_ENGINE_ACTIVE=1`
# would turn a bare run's write tools into stand-ins, and an exported `PROMPT` or
# `SLACK_POST_CHANNEL` would change what an agent sends.
INHERITED_ENV = ("PATH", "HOME", "USER", "LOGNAME", "LANG", "TZ", "SYSTEMROOT")


@dataclass(frozen=True)
class Scenario:
    argv: tuple[str, ...] = ()
    env: dict[str, str] = field(default_factory=dict)
    setup: Callable[[World], None] | None = None
    # YAML documents added beside the shipped maps for this scenario, by file name.
    extra_maps: dict[str, str] = field(default_factory=dict)
    # Faults to inject once the fake internet is up: (host, action, match, times).
    faults: tuple[tuple[str, str, Callable[[Req], bool], int], ...] = ()
    # The agent's exit code under shadow is expected to differ from the bare run's.
    diverges: bool = False
    # The scenario exists to show a write irimi cannot stop; universal invariant 1 is inverted.
    leaks: bool = False
    doc: str = ""
    # The irimi flags it runs with under `irimi shadow --serve` (#77). Every scenario runs there
    # too, since serve is how irimi runs beside an agent it did not launch; None would keep one to
    # bare and shadow, for a scenario that needs irimi to be the agent's parent.
    serve_flags: tuple[str, ...] | None = ()


@dataclass(frozen=True)
class Workflow:
    name: str  # the package name, e.g. "w09_scope_gauntlet"
    summary: str
    scenarios: dict[str, Scenario]

    @property
    def module(self) -> str:
        return f"examples.workflows.{self.name}.agent"


@dataclass
class Result:
    workflow: Workflow
    scenario: str
    mode: str
    exit_code: int
    obs: list[dict[str, Any]]
    irimi: list[str]  # every line irimi printed (shadow and serve)
    world: World
    internet: FakeInternet
    home: Path  # IRIMI_HOME
    state: Path  # the agent's own files (WORKFLOW_STATE)
    cwd: Path  # the working directory of irimi (shadow) and of the agent
    tmp: Path  # TMPDIR, and `tempfile`'s directory in this process during a shadow run
    # Every exchange irimi printed a line for, as it printed them (shadow only): the live
    # exchanges its `on_exchange` saw, which what the store kept is compared against (#70).
    reported: list[Exchange] = field(default_factory=list)
    # Every exchange the engine handed its trace store, in the order it did (shadow only).
    handed: list[Exchange] = field(default_factory=list)
    # Every exchange and every tool call the engine handed its trace store, in the order it did
    # (shadow only): `handed` with the tool calls the control endpoint passed on (#73) among them.
    handed_events: list[Exchange | ToolCall] = field(default_factory=list)
    # The store `run` was told to record into, which other runs may share (#79); None for irimi's
    # default, `$IRIMI_HOME/store`, the run's own.
    store: Path | None = None
    # A replay's result, as `irimi replay --result-file` wrote it or `sdk.replay` returned it (#84).
    replay: sdk.ReplayResult | None = None

    def events(self, event: str, *, with_time: bool = True) -> list[dict[str, Any]]:
        """Every `event` the agent logged, in order. `with_time=False` drops each one's
        timestamp, so events from two runs compare equal when only their time differs."""
        found = [o for o in self.obs if o["event"] == event]
        return found if with_time else [{k: v for k, v in o.items() if k != "t"} for o in found]

    def one(self, event: str) -> dict[str, Any]:
        """The one `event` the agent logged. Raises if it logged none, or more than one."""
        found = self.events(event)
        if len(found) != 1:
            raise AssertionError(f"expected one {event!r} event, got {len(found)}: {found}")
        return found[0]

    def result(self, *, with_time: bool = True) -> dict[str, Any] | None:
        """The agent's final `result` event, if it logged one."""
        found = self.events("result", with_time=with_time)
        return found[-1] if found else None

    def returned(self) -> Any:
        """What the one trigger the agent's run started with returned, as its `run.end` logged it
        (`examples/workflows/sdk.py`, #84): a replay that calls the trigger alone logs no `result`.
        Raises unless exactly one run ended, and ended `ok`, and its end says what it returned."""
        end = self.one("run.end")
        assert end["outcome"] == "ok", end
        return end["returned"]

    def calls(self, label: str | None = None) -> list[dict[str, Any]]:
        """The agent's HTTP calls (`http` events), in order: all of them, or those with `label`."""
        return [o for o in self.events("http") if label is None or o.get("label") == label]

    def answered(self) -> list[tuple[str, Any, Any]]:
        """`(label, status, Irimi-Answered-By)` per call, in order. An unlabelled call is named by
        its host and path; a call with no answer has status None."""
        return [
            (c.get("label") or c["url"], c.get("status"), c.get("answered_by"))
            for c in self.calls()
        ]

    def by_label(self) -> dict[str, tuple[Any, Any]]:
        """`{label: (status, Irimi-Answered-By)}`, one entry per call. Raises if a call has no
        label or shares one, because then the dict could not hold every call."""
        found: dict[str, tuple[Any, Any]] = {}
        for c in self.calls():
            if not c.get("label") or c["label"] in found:
                raise AssertionError(f"{c['method']} {c['url']}: no label of its own")
            found[c["label"]] = (c.get("status"), c.get("answered_by"))
        return found

    def by_run(self) -> dict[str | None, list[str | None]]:
        """`{run id: [label, ...]}` over the calls, each run's in order; None holds the calls
        that belonged to no run."""
        found: dict[str | None, list[str | None]] = {}
        for c in self.calls():
            found.setdefault(c.get("run"), []).append(c.get("label"))
        return found

    def tools(self, kind: str | None = None) -> list[tuple[str, str]]:
        """`(tool name, which body ran)` per `@sdk.tool` call, in order: all, or one `kind`'s."""
        return [
            (t["name"], t["ran"]) for t in self.events("tool") if kind is None or t["kind"] == kind
        ]

    def query(self, db: str, sql: str) -> list[tuple[Any, ...]]:
        """Rows from the agent's own SQLite file `db`, in its state directory."""
        with contextlib.closing(sqlite3.connect(self.state / db)) as conn:
            return conn.execute(sql).fetchall()

    def exchange_lines(self) -> list[str]:
        """irimi's per-exchange lines: `<answered_by> <kind> <METHOD> <host><path> -> <status>`.
        A tool call's line is in the same stream and the same columns, and is not one (#76)."""
        return [
            line
            for line in self.irimi
            if " -> " in line and not line.startswith(" ") and not TOOL_LINE.match(line)
        ]

    def tool_call_lines(self) -> list[str]:
        """irimi's per-tool-call lines, in the stream with the exchanges' (#76):
        `<ran> <kind> tool <name> -> ok` or `-> raised <type>`."""
        return [line for line in self.irimi if TOOL_LINE.match(line)]

    def stored(self) -> StoreReader:
        """The trace store this run wrote: the `store` it was given (#79), or else `irimi
        shadow`'s default, `$IRIMI_HOME/store` (#70), which is `home / "store"` because the
        harness gives every run a home of its own. A store runs share holds all of their runs."""
        return StoreReader(self.store_root())

    def store_root(self) -> Path:
        return self.store if self.store is not None else self.home / paths.STORE_DIR_NAME

    def stored_events(self) -> list[trace.Event]:
        """Every event the store holds: run by run, each in `seq` order, then the unattributed
        ones. Empty when irimi stopped before it opened a store."""
        reader = self.stored()
        ids = [record.run_id for record in reader.list_runs()] + [trace.UNATTRIBUTED]
        return [event for run_id in ids for event in reader.load_run(run_id).events]

    def run_blocks(self) -> dict[str, list[str]]:
        """Serve mode's summaries, by run id: the lines under each `--- run <id> ---`, which serve
        prints under one lock, so a block ends at the first line that is no summary's: a stream
        line, or the next block (#77)."""
        blocks: dict[str, list[str]] = {}
        current: list[str] | None = None
        for line in self.irimi:
            match = RUN_BLOCK.fullmatch(line)
            if match:
                current = blocks.setdefault(match.group(1), [])
                continue
            in_summary = line == "" or line.startswith("  ") or line.startswith("irimi shadow · ")
            if current is not None and in_summary:
                current.append(line)
            else:
                current = None
        return blocks

    def engine_run_id(self) -> str:
        """The run a request no `Irimi-Run` names is stored in: the process run under
        `irimi shadow -- <cmd>`, and `unattributed` under serve mode, which has none (#75, #77)."""
        return trace.UNATTRIBUTED if self.mode == "serve" else self.process_run_id()

    def summaries(self) -> list[str]:
        """Every summary line irimi printed: `irimi shadow`'s closing block, or under serve mode
        each run's block in the order the runs ended, separators left out (#77)."""
        if self.mode != "serve":
            return self.summary()
        return [line for block in self.run_blocks().values() for line in block]

    def summary(self) -> list[str]:
        """irimi's closing block, from its `N exchanges` header on. `irimi replay` prints its result
        instead (#84), so a replay's is its run's summary as `irimi runs show` prints it, which
        opens `irimi replay · run` and never `irimi shadow · run` (#154)."""
        if self.mode == "replay":
            run_id = self.process_run_id()
            _, out, _ = self.runs("show", run_id, "--store", str(self.store_root()))
            start = next(
                i for i, line in enumerate(out) if line.startswith(_summary_headers(run_id)[1])
            )
            return out[start:]
        for i, line in enumerate(self.irimi):
            if line.startswith("irimi shadow · run ") and re.search(r" · \d+ exchanges? · ", line):
                return self.irimi[i:]
        return []

    def replay_result(self) -> list[str]:
        """What `irimi replay` said of the replay (#84): its result's lines, the block it printed
        last, or why it could not run."""
        assert self.replay is not None, self.mode
        return list(self.replay.lines)

    def agent_exit_code(self) -> int:
        """How the agent exited: the process's exit code, which is irimi's under `irimi shadow`.
        Under `irimi replay`, whose exit code is the comparison's (#84), its child's, as the
        replay's run recorded it: a called trigger's 0 or 1, or the agent command's."""
        if self.mode != "replay":
            return self.exit_code
        record = self.stored().load_run(self.process_run_id()).record
        assert record.exit_code is not None, record
        return record.exit_code

    def sdk_runs(self) -> list[RunRecord]:
        """The runs the SDK started (#74), as stored: `attribution: "sdk"`, newest first."""
        return [r for r in self.stored().list_runs() if r.attribution == "sdk"]

    def process_run_id(self) -> str:
        """The id of the one run `irimi shadow` stored for the agent's process (#70). A store runs
        share holds one process run per `irimi shadow` (#79), as a replay's store holds the
        recording's and its own (#82): this one's is the run its banner named. A replay's is the
        replay's one run (#84)."""
        runs = [r.run_id for r in self.stored().list_runs() if r.attribution == "process"]
        if self.store is None and self.mode != "replay":
            (run_id,) = runs
            return run_id
        (run_id,) = [m[1] for line in self.irimi if (m := BANNER_RUN.match(line))][:1]
        assert run_id in runs, (run_id, runs)
        return run_id

    def runs(self, *argv: str) -> tuple[int, list[str], list[str]]:
        """`irimi runs <argv>` over this run's store, as its user would type it afterwards: the
        same `$IRIMI_HOME`, working directory and maps (#72). Returns the exit code and what it
        printed to stdout and to stderr, line by line."""
        return self.command("runs", *argv)

    def command(self, *argv: str) -> tuple[int, list[str], list[str]]:
        """`irimi <argv>` as `runs` runs it (#79). A store the run was given is not added: name it
        with `--store`, as its user would."""
        out, err = io.StringIO(), io.StringIO()
        scenario = self.workflow.scenarios[self.scenario]
        with (
            _environ({**os.environ, paths.IRIMI_HOME_ENV: str(self.home)}),
            contextlib.chdir(self.cwd),
            _maps(scenario, self.home.parent),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = irimi_main(list(argv))
        return code, out.getvalue().splitlines(), err.getvalue().splitlines()

    def compare_runs(self, run_a: str, run_b: str) -> tuple[int, list[str], list[str]]:
        """`irimi compare <run_a> <run_b>` over the store this run recorded into, named with
        `--store` as its user would name it (#79): any two runs it holds, `unattributed` too."""
        return self.command("compare", run_a, run_b, "--store", str(self.store_root()))

    def shown_writes(self, run_id: str) -> tuple[str, list[str]]:
        """What `irimi runs show <run_id>` prints of a stored run that `irimi compare` prints too
        (#79): the run in one line, its trigger, entrypoint and agent version as compare's header
        names a side, and its summary's write lines without their indent, in the order printed,
        `↳` lines left out. Raises unless `runs show` exits 0."""
        code, out, err = self.runs("show", run_id, "--store", str(self.store_root()))
        assert (code, err) == (0, []), (run_id, code, err)
        fields = dict(line.split(": ", 1) for line in out[: out.index("")])
        label = f"{fields['trigger']}  {fields['entrypoint']}  agent {fields['agent version']}"
        start = next(i for i, line in enumerate(out) if line.startswith(_summary_headers(run_id)))
        writes = [line[2:] for line in out[start:] if line.startswith(WRITE_LINE_PREFIXES)]
        return label, writes

    def maps(self) -> MapIndex | None:
        """The maps `irimi runs show` renders this run's writes with: shipped plus the scenario's
        own, loaded from the run's working directory and `$IRIMI_HOME`, as `runs()` loads them.
        None when they do not load, as `runs show` then carries on without them (#72)."""
        scenario = self.workflow.scenarios[self.scenario]
        with (
            _environ({**os.environ, paths.IRIMI_HOME_ENV: str(self.home)}),
            contextlib.chdir(self.cwd),
            _maps(scenario, self.home.parent),
        ):
            try:
                return servicemap.load()
            except servicemap.MapError:
                return None


def _summary_headers(run_id: str) -> tuple[str, str]:
    """The line `irimi runs show` opens a stored run's summary with: `irimi replay · run` for a
    replay's run, which printed no summary live, `irimi shadow · run` for any other (#154)."""
    return (f"irimi shadow · run {run_id} · ", f"irimi replay · run {run_id} · ")


def workflows() -> dict[str, Workflow]:
    """Every workflow package under `examples/workflows/`, found by name so a new one needs no
    registry edit."""
    found: dict[str, Workflow] = {}
    for info in pkgutil.iter_modules([str(WORKFLOWS_DIR)]):
        is_workflow = info.ispkg and info.name.startswith("w") and info.name[1:3].isdigit()
        # A package with no scenarios.py yet is half-written, not broken: skip it. One whose
        # scenarios.py fails to import is broken, and that error is not swallowed.
        if is_workflow and (WORKFLOWS_DIR / info.name / "scenarios.py").exists():
            module = importlib.import_module(f"examples.workflows.{info.name}.scenarios")
            found[info.name] = module.WORKFLOW
    return dict(sorted(found.items()))


def serve_cases() -> list[tuple[str, str]]:
    """`(workflow, scenario)` for every scenario that runs under `irimi shadow --serve` too: the
    ones whose `serve_flags` are set (#77). The one statement of that set, for every test that
    holds serve runs to something."""
    return [
        (name, scenario)
        for name, wf in workflows().items()
        for scenario, spec in wf.scenarios.items()
        if spec.serve_flags is not None
    ]


def run(
    workflow: Workflow, scenario_name: str, mode: str, workdir: Path, store: Path | None = None
) -> Result:
    """One scenario in one mode. `store` is passed to irimi as `--store`, so two runs can record
    into one store and be compared (#79); without it irimi records into the run's own home."""
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, not {mode!r}")
    return _execute(workflow, scenario_name, mode, workdir, store)


def replay(
    recorded: Result,
    run_id: str,
    workdir: Path,
    env: dict[str, str] | None = None,
    options: tuple[str, ...] = (),
    *,
    command: bool = False,
) -> Result:
    """`run_id`, a run `recorded` stored, replayed by `irimi replay <run_id>` (#82, #84):
    `recorded`'s scenario, recorded under `irimi shadow` or itself a replay, in a copy of its
    `IRIMI_HOME` and its store under `workdir`, so `recorded` reads its store as it was and the
    replay's holds both, with `env` over the scenario's own environment - `PROMPT=half` is the
    prompt edit - and `options` given to `irimi replay` too (`--call`, `--args`). What runs is what
    `irimi replay` runs for the run, or, with `command`, the scenario's agent command. The result
    file `irimi replay` writes is the result's `replay`.

    Everything else is the run's own: a fresh `World` from the scenario's `setup` and a fresh fake
    internet, so a request the recording does not answer shows up in `internet.requests()`, and its
    own state, working directory and TMPDIR under `workdir`. The result's `mode` is `replay`."""
    return _replayed(recorded, run_id, workdir, env, _Replay(options, command))


def sdk_replay(recorded: Result, run_id: str, workdir: Path, *call: Any) -> Result:
    """`sdk.replay(run_id, *call)` (#84) called as a test calls it, through the agents' own `sdk`
    (`examples/workflows/sdk.py`, which forwards `replay` to `irimi.sdk`), a `replay` of
    `recorded` in every other way. `sdk.replay`'s one seam, `sdk.replays._run_irimi`, runs `irimi`
    in this process instead of a subprocess, so the fake internet resolves its names and the
    scenario's maps load: the agent is still a child process of its own, `replay_child`, as `irimi
    replay` starts it. The result's `replay` is what `sdk.replay` returned."""
    return _replayed(recorded, run_id, workdir, None, _Replay(call=call))


@dataclass(frozen=True)
class _Replay:
    """How `_execute` replays a run (#84): the `options` `irimi replay` is given, whether it runs
    the scenario's `command`, and, for `sdk_replay`, the `sdk.replay` `call` it makes instead."""

    options: tuple[str, ...] = ()
    command: bool = False
    call: tuple[Any, ...] | None = None


def _replayed(
    recorded: Result, run_id: str, workdir: Path, env: dict[str, str] | None, how: _Replay
) -> Result:
    if recorded.mode not in ("shadow", "replay"):
        raise ValueError(
            f"replay takes a run recorded under shadow or replayed, not {recorded.mode}"
        )
    # A replay records into the store it answers from, so it runs in a copy of `recorded`'s home,
    # and of the store it was given: never in `recorded`'s own, which a session-cached recording
    # or replay shares with every test that reads it. A replay of the cached replay wrote its run
    # there, and `test_replays.py`'s rule 9 then failed for W1 `full_refund` whenever W1's
    # replay-of-a-replay tests ran first (found under #85; on main since #84).
    workdir.mkdir(parents=True, exist_ok=True)
    home = workdir / "recorded-home"
    shutil.copytree(recorded.home, home)
    store = None
    if recorded.store is not None:
        store = workdir / "recorded-store"
        shutil.copytree(recorded.store, store)
    return _execute(
        recorded.workflow,
        recorded.scenario,
        "replay",
        workdir,
        store,
        home=home,
        env=env or {},
        replay_of=run_id,
        how=how,
    )


def replayed_run_id(recorded: Result) -> str:
    """The run a replay of `recorded` answers from (#82): the one run besides the process run that
    `recorded`'s irimi put exchanges or tool calls in (#83) - the SDK's, or a `header` run whose
    start never reached irimi (W10's control faults) - or the process run when there is none (W9,
    W11). Read off the events it handed its store, so a store that holds other runs too, as a
    replay's holds its recording's, still names `recorded`'s own. A recording with several (W3,
    W8) has no one run to replay, and raises."""
    process = recorded.process_run_id()
    events = [*recorded.reported, *recorded.handed_events]
    runs = sorted({event.run_id for event in events} - {process})
    if len(runs) > 1:
        raise AssertionError(f"{recorded.scenario} stored {len(runs)} runs; a replay names one")
    return runs[0] if runs else process


def replay_targets(recorded: Result) -> list[str]:
    """Every run a replay of `recorded` can answer from (#82), in the order they started: each run
    its irimi stored besides the process run - an SDK run, a run W9 posted by hand, a `header` run -
    exchanges or none, or the process run alone when there is none (W9's scripts). A process that
    held several runs (W3, W5's cascade, W8) is replayed once per run, each answering that run
    alone, as the issue's "Replay which run?" says. Read before anything replays into its store."""
    process = recorded.process_run_id()
    runs = [r for r in recorded.stored().list_runs() if r.run_id != process]
    runs.sort(key=lambda r: (r.started_at is None, r.started_at or 0.0, r.run_id))
    return [r.run_id for r in runs] or [process]


def counterpart(recorded: Result, replayed: Result, run_id: str) -> str | None:
    """The run of `replayed` that stands for `recorded`'s run `run_id` (#82): the replay's one run,
    which every run its child's SDK started adopted (#84), as its result names it. None when the
    replay stored no run, refused before it ran."""
    assert replayed.replay is not None and replayed.replay.source_run_id == run_id
    return replayed.replay.replay_run_id


def _execute(
    workflow: Workflow,
    scenario_name: str,
    mode: str,
    workdir: Path,
    store: Path | None,
    *,
    home: Path | None = None,
    env: dict[str, str] | None = None,
    replay_of: str | None = None,
    how: _Replay | None = None,
) -> Result:
    """`run` and `replay` alike: `home` is the `IRIMI_HOME` to run in, `workdir`'s own unless a
    replay names the recording's, `env` is laid over the scenario's environment, `replay_of` is the
    stored run a replay answers from, and `how` how it is replayed."""
    scenario = workflow.scenarios[scenario_name]
    workdir.mkdir(parents=True, exist_ok=True)
    state, cwd, tmp = (workdir / d for d in ("state", "cwd", "tmp"))
    home = home if home is not None else workdir / "home"
    for d in (home, state, cwd, tmp):
        d.mkdir(exist_ok=True)
    obs_path = workdir / "obs.jsonl"
    obs_path.write_text("")

    world = World()
    if scenario.setup is not None:
        scenario.setup(world)
    internet = FakeInternet(world.services())
    internet.start()
    irimi_out: list[str] = []
    reported: list[Exchange] = []
    handed: list[Exchange] = []
    handed_events: list[Exchange | ToolCall] = []
    result_file = workdir / "result.json"
    try:
        for host, action, match, times in scenario.faults:
            internet.inject(host, action, match, times)
        agent_env = _agent_env(internet, state, obs_path, home, tmp)
        agent_env.update(scenario.env)
        agent_env.update(env or {})
        cmd = [sys.executable, "-m", "examples.workflows.launch", workflow.module, *scenario.argv]
        flags = ("--store", str(store)) if store is not None else ()
        if mode == "bare":
            code = _run_bare(cmd, agent_env, cwd, internet)
        elif mode == "shadow":
            argv = ["shadow", "--port", "0", *flags, "--", *cmd]
            with _watching_the_store(reported, handed, handed_events):
                code, irimi_out = _run_irimi(argv, agent_env, cwd, tmp, internet, scenario, workdir)
        elif mode == "replay":
            assert replay_of is not None and how is not None
            argv = ["replay", "--port", "0", *flags, "--result-file", str(result_file)]
            # As its user types it: the run id, then `-- <cmd>` for a command (#84).
            argv += [*how.options, replay_of, *(["--", *cmd] if how.command else [])]
            with _watching_the_store(reported, handed, handed_events):
                if how.call is None:
                    run_irimi = lambda: irimi_main(argv)  # noqa: E731
                else:
                    run_irimi = _calling_sdk_replay(replay_of, how.call, store, result_file)
                code, irimi_out = _run_irimi(
                    argv, agent_env, cwd, tmp, internet, scenario, workdir, run_irimi
                )
        else:
            if scenario.serve_flags is None:
                raise ValueError(f"{workflow.name}:{scenario_name} does not run in serve mode")
            with _watching_the_store(reported, handed, handed_events):
                code, irimi_out = _run_serve(
                    cmd, agent_env, cwd, tmp, internet, scenario, workdir, flags
                )
    finally:
        internet.stop()
    obs = [json.loads(line) for line in obs_path.read_text().splitlines() if line.strip()]
    return Result(
        workflow,
        scenario_name,
        mode,
        code,
        obs,
        irimi_out,
        world,
        internet,
        home,
        state,
        cwd,
        tmp,
        reported,
        handed,
        handed_events,
        store,
        _replay_result(result_file) if mode == "replay" else None,
    )


def _replay_result(path: Path) -> sdk.ReplayResult:
    """The result file a replay wrote, as `sdk.replay` reads it back."""
    return sdk.ReplayResult.from_json(json.loads(path.read_text(encoding="utf-8")))


def _calling_sdk_replay(
    run_id: str, call: tuple[Any, ...], store: Path | None, result_file: Path
) -> Callable[[], int]:
    """`sdk.replay(run_id, *call, store=store)`, its `irimi` run in this process: what it
    returned written to `result_file`, as `irimi replay --result-file` writes it, and its exit
    code."""

    def run() -> int:
        real = sdk_replays._run_irimi
        sdk_replays._run_irimi = lambda argv, env: irimi_main(argv)
        try:
            got = agent_sdk.replay(run_id, *call, store=store)
        finally:
            sdk_replays._run_irimi = real
        result_file.write_text(json.dumps(got.to_json()), encoding="utf-8")
        return got.exit_code

    return run


def compare(a: Result, b: Result) -> tuple[int, list[str], list[str]]:
    """`irimi compare` over the SDK run each of `a` and `b` started, as `b`'s user would type it,
    naming the store both recorded into: `run`'s `store`, given to both (#79)."""
    ids = [result.one("run.start")["run"] for result in (a, b)]
    return b.compare_runs(*ids)


# Whose summary printed each kind of `irimi compare` row (#79): A's for a write both made the same
# (B's too), for one only A made, and for a pair's A line; B's for one only B made and for a pair's
# B line, which a `~` or a `?` prints under its A line.
ROW_SIDES = {"  = ": "AB", "  - ": "A", "  ~ A ": "A", "  ? A ": "A", "  + ": "B", "    B ": "B"}


# How a `~` or `?` pair's A row starts, and its B row under it (#79).
PAIR_A_PREFIXES = ("  ~ A ", "  ? A ")
PAIR_B_PREFIX = "    B "


def rows_not_shown(result: Result, run_a: str, run_b: str, out: list[str]) -> list[str]:
    """Every row of `irimi compare <run_a> <run_b>`'s output `out` that is not a write line the
    summary of its run printed (`irimi runs show`, #72), or that no row kind starts (#79). Empty
    when compare showed each run's own writes and nothing else, but for the one exception compare
    makes: a pair it told apart (`_told_apart`)."""
    shown = {"A": result.shown_writes(run_a)[1], "B": result.shown_writes(run_b)[1]}
    rows = out[out.index("") + 1 : -2] if out[-2:-1] == [""] else []
    missing = []
    for i, row in enumerate(rows):
        prefix = next((p for p in ROW_SIDES if row.startswith(p)), None)
        if prefix is not None and all(row[len(prefix) :] in shown[s] for s in ROW_SIDES[prefix]):
            continue
        pair = rows[i : i + 2] if row.startswith(PAIR_A_PREFIXES) else rows[max(i - 1, 0) : i + 1]
        if not (len(pair) == 2 and _told_apart(pair[0], pair[1], shown)):
            missing.append(row)
    return missing


def _told_apart(a_row: str, b_row: str, shown: dict[str, list[str]]) -> bool:
    """Whether `a_row` over `b_row` is a `~` or `?` pair whose writes the two summaries print as
    one line, which `irimi compare` prints with each backslash escaped too so the two differ
    (#79): the rows differ, and with each doubled backslash made one again, both are that line,
    and each run's summary printed it."""
    if not (a_row.startswith(PAIR_A_PREFIXES) and b_row.startswith(PAIR_B_PREFIX)):
        return False
    a, b = a_row[len(PAIR_A_PREFIXES[0]) :], b_row[len(PAIR_B_PREFIX) :]
    line = a.replace("\\\\", "\\")
    return a != b and b.replace("\\\\", "\\") == line and line in shown["A"] and line in shown["B"]


def _agent_env(
    internet: FakeInternet, state: Path, obs_path: Path, home: Path, tmp: Path
) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k in INHERITED_ENV or k.startswith("LC_")}
    env.update(CANARIES)
    env.update(
        {
            "STRIPE_API_BASE": internet.base("api.stripe.com"),
            "SLACK_API_BASE": internet.base("slack.com"),
            "SLACK_HOOKS_API_BASE": internet.base("hooks.slack.com"),
            "ANTHROPIC_API_BASE": internet.base("api.anthropic.com"),
            "OPENAI_API_BASE": internet.base("api.openai.com"),
            "LANGSMITH_API_BASE": internet.base("api.smith.langchain.com"),
            "WORKFLOW_INTERNET_PORT": str(internet.port),
            "WORKFLOW_STATE": str(state),
            "WORKFLOW_OBS": str(obs_path),
            "WORKFLOW_HTTP_TIMEOUT": "5",
            "PYTHONPATH": str(REPO_ROOT),
            "PYTHONUNBUFFERED": "1",
            "IRIMI_HOME": str(home),
            "TMPDIR": str(tmp),
        }
    )
    return env


def _run_bare(cmd: list[str], env: dict[str, str], cwd: Path, internet: FakeInternet) -> int:
    """The agent as a plain subprocess. Every proxy variable names the fake internet, so a client
    that honours any of them never dials a real address: an `https://` URL gets the fake's refusal
    of `CONNECT`, not a real TLS connection."""
    proxy = f"http://127.0.0.1:{internet.port}"
    env = {**env, **dict.fromkeys(PROXY_VARS, proxy)}
    env["NO_PROXY"] = env["no_proxy"] = runner.NO_PROXY_VALUE  # as `irimi shadow` sets it
    returncode = subprocess.run(cmd, env=env, cwd=cwd, timeout=180, check=False).returncode
    return runner.exit_code_for(returncode)


def _run_irimi(
    argv: list[str],
    env: dict[str, str],
    cwd: Path,
    tmp: Path,
    internet: FakeInternet,
    scenario: Scenario,
    workdir: Path,
    run: Callable[[], int] | None = None,
) -> tuple[int, list[str]]:
    """`irimi <argv>` in this process, as a user would run it from `cwd`: `irimi shadow -- <agent>`,
    or `irimi replay <run id>` (#84), or `run`, which runs it another way (`sdk_replay`)."""
    buf = io.StringIO()
    with _as_irimi(env, cwd, tmp, internet, scenario, workdir), contextlib.redirect_stdout(buf):
        code = irimi_main(argv) if run is None else run()
    return code, buf.getvalue().splitlines()


@contextlib.contextmanager
def _as_irimi(
    env: dict[str, str],
    cwd: Path,
    tmp: Path,
    internet: FakeInternet,
    scenario: Scenario,
    workdir: Path,
) -> Iterator[None]:
    """This process made the one irimi runs in, for `irimi shadow` and `irimi shadow --serve`
    alike (#77): the agent's environment, `cwd`, TMPDIR, the scenario's maps, the fake resolver,
    and a CA under the run's `IRIMI_HOME`.

    irimi's L3 read is urllib in this process. With no proxy variable set, urllib on macOS falls
    back to the system's proxy settings, which would carry the read (and its canary credential)
    past the fake names to a real proxy. `no_proxy=*` keeps it direct. irimi overwrites both
    spellings for its child, and serve's agent is given its own, so the agent still sees exactly
    irimi's `NO_PROXY`.
    """
    harness_env = {**env, "NO_PROXY": "*", "no_proxy": "*"}
    with (
        _environ(harness_env),
        contextlib.chdir(cwd),
        _tempdir(tmp),
        _maps(scenario, workdir),
        internet.resolving(),
    ):
        ca.generate_ca(ca.ca_paths())
        yield


def _run_serve(
    cmd: list[str],
    env: dict[str, str],
    cwd: Path,
    tmp: Path,
    internet: FakeInternet,
    scenario: Scenario,
    workdir: Path,
    flags: tuple[str, ...] = (),
) -> tuple[int, list[str]]:
    """`irimi shadow --serve` in this process, as `_run_shadow` runs `irimi shadow`, on a thread of
    its own; the agent beside it as its own process; and irimi stopped when the agent exits (#77).

    The command is parsed by irimi's own parser and run by the function `irimi shadow --serve`
    runs, with the `stop` event in place of SIGTERM, which would reach this whole test process.
    The agent is given exactly the variables an agent container is (README, "Serve mode"): the
    proxy, the CA, `IRIMI_ENGINE_ACTIVE` and `IRIMI_CONTROL`, and never `IRIMI_RUN`, so a
    request no SDK run names is `unattributed`.
    """
    buf = io.StringIO()
    stop = threading.Event()
    codes: list[int] = []
    assert scenario.serve_flags is not None
    argv = ["shadow", "--serve", "--port", "0", *flags, *scenario.serve_flags]
    args = build_parser().parse_args(argv)
    before = set(threading.enumerate())
    with _as_irimi(env, cwd, tmp, internet, scenario, workdir), contextlib.redirect_stdout(buf):
        serving = threading.Thread(
            target=lambda: codes.append(args.func(args, stop)), name="irimi-serve"
        )
        serving.start()
        try:
            port = _served_port(buf, serving)
            if port is None:
                # Refused before it listened, as `irimi shadow` refuses before it starts its
                # child: no agent is started beside a proxy that is not there, and serve's own
                # exit is the run's (W8 `map_refused`).
                serving.join(timeout=SERVE_TIMEOUT_S)
                if serving.is_alive() or codes in ([], [0]):
                    raise AssertionError(f"irimi shadow --serve printed no banner: {codes}")
                return codes[0], buf.getvalue().splitlines()
            agent_env = _serve_env(env, port, ca.ca_paths().cert)
            done = subprocess.run(cmd, env=agent_env, cwd=cwd, timeout=180, check=False)
            code = runner.exit_code_for(done.returncode)
        finally:
            stop.set()
            serving.join(timeout=SERVE_TIMEOUT_S)
    if serving.is_alive() or codes != [0]:
        raise AssertionError(f"irimi shadow --serve did not stop cleanly: {codes}")
    # Stopped means stopped: the engine's thread joined and the store closed, its writer thread
    # with it, by the time serve returned (#77). A thread of the engine's event loop's executor,
    # told to shut down as the loop closed, may take a moment to see it, so each is given that
    # long to end; the fake internet's own handlers may still be finishing an answer.
    started = [t for t in threading.enumerate() if t not in before and FAKE_HANDLER not in t.name]
    deadline = time.monotonic() + THREAD_EXIT_S
    for thread in started:
        thread.join(max(0.0, deadline - time.monotonic()))
    left = [t.name for t in started if t.is_alive()]
    if left:
        raise AssertionError(f"irimi shadow --serve left threads running: {left}")
    return code, buf.getvalue().splitlines()


def _served_port(buf: io.StringIO, serving: threading.Thread) -> int | None:
    """The port in serve's banner, once it has printed it, or None if serve ended without one."""
    deadline = time.monotonic() + SERVE_TIMEOUT_S
    while time.monotonic() < deadline:
        # Alive is read first, so a serve that printed its banner and then ended is not missed.
        alive = serving.is_alive()
        match = re.search(r" · listening on \S+:(\d+) · ", buf.getvalue())
        if match:
            return int(match.group(1))
        if not alive:
            return None
        time.sleep(0.01)
    raise AssertionError(f"irimi shadow --serve printed no banner: {buf.getvalue()!r}")


def _serve_env(env: dict[str, str], port: int, cert: Path) -> dict[str, str]:
    """The agent's environment beside a serve: the README's serve-mode variables, naming irimi as
    the agent connects to it, here loopback (#77)."""
    listener = f"http://127.0.0.1:{port}"
    return {
        **env,
        **dict.fromkeys(paths.PROXY_ENVS, listener),
        "NO_PROXY": runner.NO_PROXY_VALUE,
        "no_proxy": runner.NO_PROXY_VALUE,
        "SSL_CERT_FILE": str(cert),
        "REQUESTS_CA_BUNDLE": str(cert),
        paths.ENGINE_ACTIVE_ENV: "1",
        paths.CONTROL_ENV: f"{listener}/_irimi",
    }


@contextlib.contextmanager
def _environ(env: dict[str, str]) -> Iterator[None]:
    saved = dict(os.environ)
    os.environ.clear()
    os.environ.update(env)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


@contextlib.contextmanager
def _watching_the_store(
    reported: list[Exchange], handed: list[Exchange], handed_events: list[Exchange | ToolCall]
) -> Iterator[None]:
    """`irimi shadow` unchanged, but each exchange it prints a line for is appended to `reported`,
    and each one its engine hands the trace store to `handed`, and to `handed_events` with each
    tool call the control endpoint hands it (#73). `cli` imports `DirectoryStore` when it opens the
    store and builds its engine with `_build_engine`, which it hands the `on_exchange` that prints
    each line, so swapping the two names is enough, as `tests/test_trace_e2e.py` does (#70). Not
    `report.exchange_line` itself, which `irimi replay` also calls for the line of its divergence
    point (#84)."""
    real = irimi_store.DirectoryStore
    build = cli._build_engine

    def building(run: Any, on_exchange: Any, *args: Any) -> Any:
        def printing(ex: Exchange) -> None:
            reported.append(ex)
            on_exchange(ex)

        return build(run, printing, *args)

    class Handed(real):
        def record(self, exchange: Exchange) -> None:
            handed.append(exchange)
            handed_events.append(exchange)
            super().record(exchange)

        def record_tool_call(self, call: ToolCall) -> None:
            handed_events.append(call)
            super().record_tool_call(call)

    irimi_store.DirectoryStore = Handed
    cli._build_engine = building
    try:
        yield
    finally:
        irimi_store.DirectoryStore = real
        cli._build_engine = build


@contextlib.contextmanager
def _tempdir(path: Path) -> Iterator[None]:
    """`tempfile`'s directory in this process: it reads TMPDIR once and caches it."""
    saved = tempfile.tempdir
    tempfile.tempdir = str(path)
    try:
        yield
    finally:
        tempfile.tempdir = saved


@contextlib.contextmanager
def _maps(scenario: Scenario, workdir: Path) -> Iterator[None]:
    """The shipped maps, plus the scenario's own, read from a directory of this run's."""
    if not scenario.extra_maps:
        yield
        return
    maps_dir = workdir / "maps"
    maps_dir.mkdir(exist_ok=True)
    for shipped in loader.shipped_dir().glob("*.yaml"):
        shutil.copy(shipped, maps_dir / shipped.name)
    for name, text in scenario.extra_maps.items():
        (maps_dir / name).write_text(text)
    saved = loader.shipped_dir
    loader.shipped_dir = lambda: maps_dir
    try:
        yield
    finally:
        loader.shipped_dir = saved


# -- the universal invariants ---------------------------------------------------------------------


def check_invariants(shadow: Result, bare: Result | None = None) -> list[str]:
    """What every shadow run owes, whatever the workflow, and every serve run (#77) and replay
    (#82). Returns the broken ones, worded. Invariant 5 compares exit codes with `bare`, when it
    is given."""
    if shadow.mode not in ("shadow", "serve", "replay"):
        raise ValueError(f"check_invariants takes a shadow, serve or replay run, not {shadow.mode}")
    scenario = shadow.workflow.scenarios[shadow.scenario]
    problems = []
    # Invariant 1 watches the fake internet, so it holds only if irimi's upstream IS the fake
    # internet. A live answer the agent saw that no fake served came from somewhere else, and a
    # write sent there would pass invariant 1 unseen.
    unserved = _live_answers_not_served(shadow)
    if unserved:
        problems.append(f"1. the agent saw live answers no fake service gave: {unserved}")
    writes = shadow.internet.writes()
    if not scenario.leaks and writes:
        problems.append(
            "1. a write reached a fake service: "
            + ", ".join(f"{r.method} {r.host}{r.path}" for r in writes)
        )
    # A replay answers the leaking read from the recording, so it may leak nothing (#82).
    if scenario.leaks and not writes and shadow.mode != "replay":
        problems.append("1. a scenario marked `leaks` leaked nothing")
    labelled = [r for r in shadow.internet.requests() if "irimi-run" in r.headers]
    if labelled:
        problems.append(
            "2. Irimi-Run reached a fake service: "
            + ", ".join(f"{r.method} {r.host}{r.path}" for r in labelled)
        )
    real_writes = [
        t["name"] for t in shadow.events("tool") if t["kind"] == "write" and t["ran"] == "real"
    ]
    if real_writes:
        problems.append(f"3. a write tool ran for real: {real_writes}")
    roots = [shadow.home, shadow.cwd, shadow.tmp]
    if not shadow.store_root().is_relative_to(shadow.home):
        roots.append(shadow.store_root())  # a store the run was given, outside its home (#79)
    leaked = [found for root in roots for found in _canaries_under(root)]
    if leaked:
        problems.append(f"4. a canary reached disk where irimi writes: {leaked}")
    if bare is not None and not scenario.diverges and bare.exit_code != shadow.exit_code:
        problems.append(
            f"5. the agent exited {shadow.exit_code} under shadow but {bare.exit_code} bare"
        )
    return problems


def _live_answers_not_served(result: Result) -> list[str]:
    """Each call the agent saw answered live (no `Irimi-Answered-By`) by a host the fake internet
    serves, and that the fake internet has no matching request for."""
    received = Counter((r.method, f"{r.host}{r.path}") for r in result.internet.requests())
    missing = []
    for call in result.calls():
        host = str(call["url"]).split("/", 1)[0]
        live = call.get("status") is not None and call.get("answered_by") is None
        if not live or host not in result.internet.services:
            continue
        key = (call["method"], call["url"])
        if received[key]:
            received[key] -= 1
        else:
            missing.append(f"{call['method']} {call['url']}")
    return missing


def _canaries_under(root: Path) -> list[str]:
    needles = {name: value.encode() for name, value in {**CANARIES, **SERVED_CANARIES}.items()}
    found = []
    for path in root.rglob("*"):
        if path.is_file():
            data = path.read_bytes()
            found += [f"{name} in {path}" for name, v in needles.items() if v in data]
    return found
