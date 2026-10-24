"""What one scenario run left: the agent's log, irimi's output, and the store it recorded into."""

from __future__ import annotations

import contextlib
import io
import os
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from irimi import servicemaps, testing
from irimi.cli import main as irimi_main
from irimi.config import paths
from irimi.core.exchange import Exchange
from irimi.decide.writelog import WriteLog
from irimi.protocol import trace
from irimi.protocol.trace import RunRecord, ToolCall
from irimi.record.store.reader import StoreReader
from irimi.report.runs import NOT_SET, RUN_LIST_HEADER
from irimi.report.summary import REJECTED_MARKER, TOOL_SERVICE, WRITE_MARKER
from irimi.report.terminal import TARGET_PREFIX
from irimi.servicemaps import MapIndex
from tests.e2e.workflows.harness.internet import FakeInternet
from tests.e2e.workflows.harness.scenario import Workflow, environ, scenario_maps
from tests.e2e.workflows.harness.services import World

# The line irimi's serve mode prints above each run's summary when the SDK ends the run.
RUN_BLOCK = re.compile(r"--- run (\S+) ---")
# The run `irimi shadow`'s banner names, its process run.
BANNER_RUN = re.compile(r"irimi (?:shadow|replay) · run (\S+) · ")
# An `irimi runs list` row (`report.runs.run_list_lines`): run id, started (a date and a time, or
# `-`), duration and status (`error (exit N)` or one word), then the attribution column, which
# never holds two spaces, and the trigger. Columns are padded, so any run of spaces separates them.
RUN_LIST_LINE = re.compile(r"(\S+) +(?:\S+ \S+|-) +\S+ +(?:error \(exit -?\d+\)|\S+) +(.+?)  ")
# A tool call's line in irimi's stream (`report.terminal.tool_call_line`): how it ran, its kind.
TOOL_LINE = re.compile(r"(tool-real|tool-fake|tool-recorded) +(read|write) +\S.* -> ")
# How `irimi replay`'s header starts; it names its two runs with an arrow, as exchange lines do.
REPLAY_HEADER = "irimi replay · "
# A write tool's line in a summary, without its indent (`report.summary.tool_write_line`). The
# summary prints these after the HTTP writes, and `irimi compare` among them, in `seq` order.
TOOL_WRITE = re.compile(rf"{WRITE_MARKER} {TOOL_SERVICE}  \S+  fake ")
# How a summary's write line starts, a write irimi answered or one it says would fail.
WRITE_LINE_PREFIXES = (f"  {WRITE_MARKER} ", f"  {REJECTED_MARKER} ")


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
    # Every exchange irimi printed a line for, in order (shadow only), to compare with the store.
    reported: list[Exchange] = field(default_factory=list)
    # Every exchange the engine handed its trace store, in the order it did (shadow only).
    handed: list[Exchange] = field(default_factory=list)
    # `handed` with the tool calls the control endpoint passed on among them (shadow only).
    handed_events: list[Exchange | ToolCall] = field(default_factory=list)
    # The store `run` was told to record into, which other runs may share; None for the default.
    store: Path | None = None
    # A replay's result, as `irimi replay --result-file` wrote it or `testing.replay` returned it.
    replay: testing.ReplayResult | None = None
    # Every line irimi printed to stderr (shadow, serve and replay): its warnings and errors.
    irimi_errors: list[str] = field(default_factory=list)
    # The engine's write log as irimi left it, once every run it decided has ended or not.
    write_log: WriteLog | None = None

    def events(self, event: str, *, with_time: bool = True) -> list[dict[str, Any]]:
        """Return every `event` the agent logged, in order; `with_time=False` drops timestamps."""
        found = [o for o in self.obs if o["event"] == event]
        return found if with_time else [{k: v for k, v in o.items() if k != "t"} for o in found]

    def one(self, event: str) -> dict[str, Any]:
        """Return the one `event` the agent logged; raise if it logged none or several."""
        found = self.events(event)
        if len(found) != 1:
            raise AssertionError(f"expected one {event!r} event, got {len(found)}: {found}")
        return found[0]

    def result(self, *, with_time: bool = True) -> dict[str, Any] | None:
        """Return the agent's final `result` event, if it logged one."""
        found = self.events("result", with_time=with_time)
        return found[-1] if found else None

    def returned(self) -> Any:
        """Return what the run's one trigger returned, as its `run.end` logged it.

        A replay that calls the trigger alone logs no `result`. Raises unless one run ended `ok`."""
        end = self.one("run.end")
        assert end["outcome"] == "ok", end
        return end["returned"]

    def calls(self, label: str | None = None) -> list[dict[str, Any]]:
        """Return the agent's HTTP calls (`http` events), in order: all, or those with `label`."""
        return [o for o in self.events("http") if label is None or o.get("label") == label]

    def answered(self) -> list[tuple[str, Any, Any]]:
        """Return `(label or URL, status, Irimi-Answered-By)` per call, in order."""
        return [
            (c.get("label") or c["url"], c.get("status"), c.get("answered_by"))
            for c in self.calls()
        ]

    def by_label(self) -> dict[str, tuple[Any, Any]]:
        """Return `{label: (status, Irimi-Answered-By)}`; raise if a label is missing or shared."""
        found: dict[str, tuple[Any, Any]] = {}
        for c in self.calls():
            if not c.get("label") or c["label"] in found:
                raise AssertionError(f"{c['method']} {c['url']}: no label of its own")
            found[c["label"]] = (c.get("status"), c.get("answered_by"))
        return found

    def by_run(self) -> dict[str | None, list[str | None]]:
        """Return `{run id: [label, ...]}` over the calls; None holds calls in no run."""
        found: dict[str | None, list[str | None]] = {}
        for c in self.calls():
            found.setdefault(c.get("run"), []).append(c.get("label"))
        return found

    def tools(self, kind: str | None = None) -> list[tuple[str, str]]:
        """Return `(tool name, which body ran)` per tool call, in order: all, or one `kind`'s."""
        return [
            (t["name"], t["ran"]) for t in self.events("tool") if kind is None or t["kind"] == kind
        ]

    def query(self, db: str, sql: str) -> list[tuple[Any, ...]]:
        """Return rows from the agent's own SQLite file `db`, in its state directory."""
        with contextlib.closing(sqlite3.connect(self.state / db)) as conn:
            return conn.execute(sql).fetchall()

    def exchange_lines(self) -> list[str]:
        """Return irimi's exchange lines: `<answered_by> <kind> <METHOD> <host><path> -> <status>`.

        Tool call lines, the banner's target lines and a replay's header are left out."""
        return [
            line
            for line in self.irimi
            if " -> " in line
            and not line.startswith((" ", TARGET_PREFIX, REPLAY_HEADER))
            and not TOOL_LINE.match(line)
        ]

    def tool_call_lines(self) -> list[str]:
        """Return irimi's tool call lines: `<ran> <kind> tool <name> -> ok` or `-> raised <t>`."""
        return [line for line in self.irimi if TOOL_LINE.match(line)]

    def stored(self) -> StoreReader:
        """Return the store this run wrote: the `store` it was given, or `$IRIMI_HOME/store`."""
        return StoreReader(self.store_root())

    def store_root(self) -> Path:
        return self.store if self.store is not None else self.home / paths.STORE_DIR_NAME

    def stored_events(self) -> list[trace.Event]:
        """Return every stored event, run by run in `seq` order, then the unattributed ones."""
        reader = self.stored()
        ids = [record.run_id for record in reader.list_runs()] + [trace.UNATTRIBUTED]
        return [event for run_id in ids for event in reader.load_run(run_id).events]

    def run_blocks(self) -> dict[str, list[str]]:
        """Return serve mode's summaries by run id: the lines under each `--- run <id> ---`.

        Serve prints a block under one lock, so it ends at the first line that is no summary's."""
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
        """Return the run unlabelled requests land in: the process run, or `unattributed`."""
        return trace.UNATTRIBUTED if self.mode == "serve" else self.process_run_id()

    def summaries(self) -> list[str]:
        """Return every summary line: shadow's closing block, or serve's run blocks in end order."""
        if self.mode != "serve":
            return self.summary()
        return [line for block in self.run_blocks().values() for line in block]

    def summary(self) -> list[str]:
        """Return irimi's closing block, from its `N exchanges` header on.

        A replay prints its result instead, so its summary is the one `irimi runs show` prints."""
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
        """Return the lines `irimi replay` printed last: its result, or why it could not run."""
        assert self.replay is not None, self.mode
        return list(self.replay.lines)

    def agent_exit_code(self) -> int:
        """Return the agent's exit code, which is irimi's under shadow.

        A replay's own exit code is the comparison's, so this reads the child's from its run."""
        if self.mode != "replay":
            return self.exit_code
        record = self.stored().load_run(self.process_run_id()).record
        assert record.end is not None and record.end.exit_code is not None, record
        return record.end.exit_code

    def sdk_runs(self) -> list[RunRecord]:
        """Return the stored runs the SDK started, newest first."""
        return [r for r in self.stored().list_runs() if r.attribution == "sdk"]

    def process_run_id(self) -> str:
        """Return the id of the process run stored for this shadow run or replay.

        A shared store holds one process run per shadow; this one's is the run its banner named."""
        runs = [r.run_id for r in self.stored().list_runs() if r.attribution == "process"]
        if self.store is None and self.mode != "replay":
            (run_id,) = runs
            return run_id
        (run_id,) = [m[1] for line in self.irimi if (m := BANNER_RUN.match(line))][:1]
        assert run_id in runs, (run_id, runs)
        return run_id

    def runs(self, *argv: str) -> tuple[int, list[str], list[str]]:
        """Run `irimi runs <argv>` with this run's home, working directory and maps.

        Returns the exit code and the stdout and stderr lines."""
        return self.command("runs", *argv)

    def command(self, *argv: str) -> tuple[int, list[str], list[str]]:
        """Run `irimi <argv>` as `runs` does; a store the run was given needs `--store`."""
        out, err = io.StringIO(), io.StringIO()
        scenario = self.workflow.scenarios[self.scenario]
        with (
            environ({**os.environ, paths.IRIMI_HOME_ENV: str(self.home)}),
            contextlib.chdir(self.cwd),
            scenario_maps(scenario, self.home.parent),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = irimi_main(list(argv))
        return code, out.getvalue().splitlines(), err.getvalue().splitlines()

    def listed_attributions(self) -> dict[str, str]:
        """Return each run `irimi runs list` shows, by id, to its attribution column.

        Raises unless it exits 0, quiet, with every line after the header row a run's."""
        code, listed, err = self.runs("list", "--all", "--store", str(self.store_root()))
        assert (code, err) == (0, []), (code, err)
        assert listed[0].split() == list(RUN_LIST_HEADER), listed
        columns = [RUN_LIST_LINE.match(line) for line in listed[1:]]
        assert None not in columns, listed
        return {m[1]: m[2] for m in columns if m is not None}

    def compare_runs(self, run_a: str, run_b: str) -> tuple[int, list[str], list[str]]:
        """Run `irimi compare <run_a> <run_b>` over the store this run recorded into."""
        return self.command("compare", run_a, run_b, "--store", str(self.store_root()))

    def shown_writes(self, run_id: str) -> tuple[str, list[str]]:
        """Return the parts of `irimi runs show <run_id>` that `irimi compare` prints too.

        That is the run's label as compare's header names a side, and its summary's write lines
        without their indent, `↳` lines left out. An empty unattributed pool gives `("", [])`."""
        code, out, err = self.runs("show", run_id, "--store", str(self.store_root()))
        assert (code, err) == (0, []), (run_id, code, err)
        if out == [f"no unattributed events in {self.store_root()}"]:
            return "", []
        fields = dict(line.split(": ", 1) for line in out[: out.index("")])
        # `runs show` prints `-` for what the run did not record, which compare leaves out, and
        # compare leaves out an entrypoint that is the trigger's name too.
        version = fields["agent version"]
        entrypoint = fields["entrypoint"]
        parts = [
            fields["trigger"],
            NOT_SET if entrypoint == fields["trigger"] else entrypoint,
        ]
        parts.append(NOT_SET if version == NOT_SET else f"agent {version}")
        label = "  ".join(part for part in parts if part != NOT_SET)
        start = next(i for i, line in enumerate(out) if line.startswith(_summary_headers(run_id)))
        writes = [line[2:] for line in out[start:] if line.startswith(WRITE_LINE_PREFIXES)]
        return label, writes

    def maps(self) -> MapIndex | None:
        """Return the maps `irimi runs show` renders this run's writes with, or None."""
        scenario = self.workflow.scenarios[self.scenario]
        with (
            environ({**os.environ, paths.IRIMI_HOME_ENV: str(self.home)}),
            contextlib.chdir(self.cwd),
            scenario_maps(scenario, self.home.parent),
        ):
            try:
                return servicemaps.load()
            except servicemaps.MapError:
                return None


def _summary_headers(run_id: str) -> tuple[str, str]:
    """Return the two headers `irimi runs show` opens a stored run's summary with."""
    return (f"irimi shadow · run {run_id} · ", f"irimi replay · run {run_id} · ")
