"""`irimi shadow -- <command>`, and `run_child`, which `irimi replay` shares."""

import argparse
import contextlib
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field, replace
from typing import Any, Literal

from irimi.cli import CliError, UsageError, compose, runner
from irimi.cli.options import SHADOW_EXIT_CODES, add_proxy_options, epilog, help_formatted
from irimi.cli.runner import ChildCall
from irimi.core import exitcodes
from irimi.core.exchange import Exchange
from irimi.protocol.env import AGENT_VERSION_ENV
from irimi.protocol.replay_files import CallResult, read_call_outcome
from irimi.protocol.trace import UNATTRIBUTED, Event, RunRecord, ToolCall, Trigger
from irimi.report import terminal
from irimi.report.model import RunView, for_summary
from irimi.report.summary import Live, summary_lines

USAGE = "irimi shadow [options] -- COMMAND [ARGS...]"


def register(subparsers: "argparse._SubParsersAction[Any]") -> None:
    shadow = subparsers.add_parser(
        "shadow",
        **help_formatted(
            help="run a command with its HTTP(S) writes faked, then summarize",
            usage=USAGE,
            description="Run COMMAND with its HTTP(S) traffic sent through irimi's proxy: reads "
            "go to the real service,\nwrites are faked. When it exits, print what it would have "
            "written, and store the run.",
            epilog=epilog(
                ["irimi shadow -- python3 agent.py", "irimi shadow --port 4100 -- npm start"],
                SHADOW_EXIT_CODES,
            ),
        ),
    )
    add_proxy_options(shadow, 0)
    # `cmd` is what follows `--`, which `main` splits off before argparse sees it.
    shadow.set_defaults(func=cmd_shadow, cmd=[])


def cmd_shadow(
    args: argparse.Namespace,
    *,
    engine_factory: compose.EngineFactory = compose.build_engine,
) -> int:
    cmd = list(args.cmd)
    if not cmd:
        raise UsageError(f"nothing to run: {USAGE}")
    with runner.passing_sigterm_to(runner.Child()) as child:
        return _run_shadow(args, cmd, child, engine_factory)


def _run_shadow(
    args: argparse.Namespace,
    cmd: list[str],
    child: runner.Child,
    engine_factory: compose.EngineFactory,
) -> int:
    """Run `irimi shadow -- <cmd>` from start to summary."""
    run = compose.prepare_run(args)
    if run is None:
        return exitcodes.ERROR
    record = process_run(run.run_id, cmd)
    with run:
        ran = run_child(args, run, child, ChildSpec("shadow", cmd, record), engine_factory)
    if ran.status in ("not-started", "not-spawned"):
        return ran.code
    sdk_runs = _sdk_runs(args, run.run_id, ran)
    end_the_childs_line()
    live = Live(sdk_runs, ran.tls_failures, run.ca.cert)
    lines = summary_lines(RunView(record, ran.events), run.index, ran.elapsed, live)
    for line in lines:
        print(line, flush=True)
    return ran.code


def process_run(
    run_id: str,
    argv: list[str],
    trigger: Trigger | None = None,
    replay_of: str | None = None,
) -> RunRecord:
    """Return the child's process run record; `started_at` is set when it is stored."""
    agent_version = os.environ.get(AGENT_VERSION_ENV)
    return RunRecord.process(run_id, argv, agent_version, 0.0, trigger, replay_of)


def end_the_childs_line() -> None:
    """Print a newline, since the child may leave its last stdout line open (as `curl` does)."""
    print(flush=True)


def _sdk_runs(
    args: argparse.Namespace, process_run_id: str, ran: "ChildResult"
) -> dict[str, RunRecord | None]:
    """Return the SDK runs seen in the child, by first event, with their stored records or None."""
    seen = [
        *(ev.run_id for ev in ran.events if not isinstance(ev, ToolCall)),
        *(ev.run_id for ev in ran.events if isinstance(ev, ToolCall)),
    ]
    ids = [rid for rid in dict.fromkeys(seen) if rid not in (process_run_id, UNATTRIBUTED)]
    root = compose.store_root(args)
    runs: dict[str, RunRecord | None] = {}
    for run_id in ids:
        try:
            runs[run_id] = compose.load_stored_run(root, run_id, bodies=False).record
        except CliError:
            runs[run_id] = None
    return runs


@dataclass(frozen=True)
class ChildSpec:
    """The child to run: banner command (`shadow`/`replay`), argv, run record, replay call."""

    banner: str
    argv: list[str]
    record: RunRecord
    call: ChildCall | None = None


# How far a child run got:
#   not-started  no run stored; irimi printed why
#   not-spawned  the run was stored but the child could not be spawned
#   interrupted  Ctrl-C after the banner, before the run was stored
#   ran          the run was stored and the child started
ChildStatus = Literal["not-started", "not-spawned", "interrupted", "ran"]


@dataclass
class ChildResult:
    """The result of a child run and the events the engine reported meanwhile.

    Short of "ran", `code` is irimi's own. `stopped` is the exit code of a signal irimi received
    while the child ran; `call` is how a replay's call ended.
    """

    code: int
    status: ChildStatus = "ran"
    elapsed: float = 0.0
    events: list[Event] = field(default_factory=list)
    stopped: int | None = None
    call: CallResult | None = None
    # TLS handshakes clients refused, by host.
    tls_failures: dict[str, int] = field(default_factory=dict)


def _cannot_run(program: str) -> tuple[int, str] | None:
    """Return the shell exit code and reason `program` cannot start, or None if it can."""
    if os.sep not in program and (os.altsep is None or os.altsep not in program):
        found = shutil.which(program)
        return None if found else (exitcodes.NOT_FOUND, f"command not found: {program}")
    if not os.path.exists(program):
        return exitcodes.NOT_FOUND, f"command not found: {program}"
    if os.path.isdir(program):
        return exitcodes.CANNOT_EXEC, f"could not run {program}: it is a directory"
    if not os.access(program, os.X_OK):
        return exitcodes.CANNOT_EXEC, f"could not run {program}: permission denied"
    return None


def run_child(
    args: argparse.Namespace,
    run: compose.Run,
    child: runner.Child,
    spec: ChildSpec,
    engine_factory: compose.EngineFactory,
) -> ChildResult:
    """Run `spec` as `run`'s child under its engine, then end the run and stop the engine.

    The caller closes the store.
    """
    cannot = _cannot_run(spec.argv[0])
    if cannot is not None:
        print(f"error: {cannot[1]}", file=sys.stderr)
        return ChildResult(cannot[0], status="not-started")
    ran = ChildResult(exitcodes.SIGINT, status="interrupted")

    def on_exchange(ex: Exchange) -> None:
        ran.events.append(for_summary(ex))
        if not args.quiet:
            print(terminal.exchange_line(ex), flush=True)

    def on_tool_call(call: ToolCall) -> None:
        # Runs on the proxy's event loop. The line never shows args or results, which would
        # print unredacted.
        ran.events.append(call)
        if not args.quiet:
            print(terminal.tool_call_line(call), flush=True)

    def on_tls_failure(host: str) -> None:
        ran.tls_failures[host] = ran.tls_failures.get(host, 0) + 1

    engine = engine_factory(run, on_exchange, on_tool_call, on_tls_failure)

    try:
        handle = compose.start_engine(engine)
    except KeyboardInterrupt:
        print("error: interrupted before the proxy was ready", file=sys.stderr)
        return ChildResult(exitcodes.SIGINT, status="not-started")
    if handle is None:
        return ChildResult(exitcodes.ERROR, status="not-started")

    cmd = spec.argv
    try:
        host, port = run.config.listen_host, handle.port()
        replaying = compose.replayed_run(run.config.mode)
        compose.print_startup(
            spec.banner, args, run.index, run.run_id, host, port, run.ca.cert, replaying
        )
        env = runner.child_env(
            dict(os.environ),
            host,
            port,
            run.ca.cert,
            run.run_id,
            replaying=replaying is not None,
            call=spec.call,
        )
        if child.terminated:  # SIGTERM while irimi started: the command is never run
            print("error: terminated before the command started", file=sys.stderr)
            ran.code, ran.status = exitcodes.SIGTERM, "not-started"
            return ran
        # Stored before the child exists, so every exchange lands in a run with a record.
        run.store.start_run(replace(spec.record, started_at=time.time()))
        ran.status = "ran"
        try:
            proc = subprocess.Popen(cmd, env=env)
            child.adopt(proc)
        except FileNotFoundError:
            print(f"error: command not found: {cmd[0]}", file=sys.stderr)
            ran.code, ran.status = exitcodes.NOT_FOUND, "not-spawned"
            return ran
        except OSError as exc:
            print(f"error: could not run {cmd[0]}: {exc}", file=sys.stderr)
            ran.code, ran.status = exitcodes.CANNOT_EXEC, "not-spawned"
            return ran
        # The summary reports how long the agent ran; monotonic survives clock changes.
        started = time.monotonic()
        ran.code = runner.exit_code_for(runner.wait_for_child(proc, child))
        ran.elapsed = time.monotonic() - started
        if child.terminated or child.interrupted:
            ran.stopped = exitcodes.SIGTERM if child.terminated else exitcodes.SIGINT
    except KeyboardInterrupt:  # Ctrl-C outside wait_for_child's own handling
        ran.code = ran.stopped = exitcodes.SIGINT
    finally:
        # An exchange that finishes after the run ends is still recorded.
        if ran.status in ("ran", "not-spawned"):
            if spec.call is not None:
                ran.call = read_call_outcome(spec.call.outcome_file)
            error = ran.call.error if ran.call is not None else None
            runner.end_process_run(run.store, run.run_id, ran.code, time.time(), error)
        # A second Ctrl-C can land while the engine thread is joined.
        with contextlib.suppress(KeyboardInterrupt):
            handle.stop()
    return ran
