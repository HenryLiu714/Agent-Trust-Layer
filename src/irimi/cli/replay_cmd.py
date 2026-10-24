"""`irimi replay RUN_ID`: a stored run's trigger run again through irimi, answered from the
run's recording, and what the agent did differently."""

import argparse
import contextlib
import json
import os
import sys
import tempfile
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any, TextIO, cast

from irimi import __version__
from irimi.cli import CliError, UsageError, compose, os_error, runner
from irimi.cli.options import (
    STORED_RUN_HELP,
    add_proxy_options,
    args_arg,
    args_file_arg,
    entrypoint_arg,
    epilog,
    help_formatted,
)
from irimi.cli.shadow import (
    ChildResult,
    ChildSpec,
    end_the_childs_line,
    process_run,
    run_child,
)
from irimi.core import exitcodes
from irimi.protocol.replay_files import CallResult, ReplayResult, replay_result_to_json
from irimi.protocol.trace import UNATTRIBUTED, StoredRun
from irimi.replay import plan as replay_plan
from irimi.replay import report as replay_report
from irimi.report.terminal import printable

EXIT_CODES = exitcodes.help_table(
    [
        exitcodes.OK,
        exitcodes.DIFFERENT,
        exitcodes.ERROR,
        exitcodes.SIGINT,
        exitcodes.BROKEN_PIPE,
        exitcodes.SIGTERM,
    ]
)


def register(subparsers: "argparse._SubParsersAction[Any]") -> None:
    replay = subparsers.add_parser(
        "replay",
        **help_formatted(
            help="re-run a stored run against its recorded responses and show what changed",
            usage="irimi replay RUN [options] [-- COMMAND [ARGS...]]",
            description="Re-run a stored run's entrypoint, or its recorded command, with every "
            "request answered\nfrom the run's recording, and compare the writes it makes with the "
            "recorded ones.",
            epilog=epilog(
                [
                    "irimi replay last",
                    "irimi replay 7729eae4 --entrypoint agent:handle --python .venv/bin/python",
                    "irimi replay 7729eae4 -- python3 agent.py",
                ],
                EXIT_CODES,
            ),
        ),
    )
    replay.add_argument(
        "run_id",
        nargs="?",
        metavar="RUN",
        help=f"the run to replay: {STORED_RUN_HELP}",
    )
    own = replay.add_argument_group("replay options")
    own.add_argument(
        "--run",
        metavar="RUN",
        help="the run to replay, for a run id that starts with -: --run=RUN",
    )
    own.add_argument(
        "--entrypoint",
        type=entrypoint_arg,
        metavar="MODULE:FUNC",
        help="call this function instead of the run's recorded entrypoint",
    )
    own.add_argument(
        "--python",
        metavar="PATH",
        help="the agent's Python interpreter (needs irimi installed)",
    )
    given = own.add_mutually_exclusive_group()
    given.add_argument(
        "--args",
        type=args_arg,
        metavar="JSON",
        help="call it with these arguments, a JSON object of {parameter: value}, instead of the "
        "run's recorded ones",
    )
    given.add_argument(
        "--args-file",
        type=args_file_arg,
        dest="args",
        metavar="PATH",
        help="read --args from a file",
    )
    own.add_argument(
        "--result-file",
        type=Path,
        metavar="PATH",
        help="write the result as JSON to PATH",
    )
    add_proxy_options(replay, 0)
    # What follows `--`, which `main` splits off before argparse sees it: a command to run in
    # place of a call of the run's entrypoint.
    replay.set_defaults(func=cmd_replay, cmd=[])


def cmd_replay(
    args: argparse.Namespace, *, engine_factory: compose.EngineFactory = compose.build_engine
) -> int:
    """`irimi replay RUN`: a stored run's trigger run again through irimi, answered from the
    run's recording, and what the agent did differently. What it runs is `replay.plan`'s
    to say, and what it prints `replay.report.ReplayReport`'s. Exits with `EXIT_CODES`: as
    `irimi compare` does, 2 for a replay that could not run, or whose run could not be read back,
    and 130 or 143 for one SIGINT or SIGTERM stopped.
    `--result-file` writes the result as JSON, the one `testing.replay()` reads
    (`protocol.replay_files.ReplayResult`), whether or not the replay ran, holding what irimi
    said on stderr of a replay that did not."""
    if args.run_id is not None and args.run is not None:
        raise UsageError("give the run once: RUN, or --run=RUN")
    if args.run_id is None and args.run is None:
        raise UsageError("give the run to replay: RUN, or --run=RUN for an id that starts with -")
    args.run_id = args.run_id if args.run is None else args.run
    if args.cmd and (args.entrypoint is not None or args.args is not None):
        raise UsageError(
            "-- CMD runs a command, not a call: drop --entrypoint and --args, or the command"
        )
    if args.cmd and args.python is not None:
        raise UsageError("-- CMD names its own Python: drop --python, or the command")
    with runner.passing_sigterm_to(runner.Child()) as child, _keeping_stderr() as said:
        try:
            result = _run_replay(args, child, said, engine_factory)
        except KeyboardInterrupt:
            # Ctrl-C while irimi read a run or prepared one, before any was stored.
            result = _interrupted(args, said, None)
    if args.result_file is not None:
        try:
            args.result_file.write_text(json.dumps(replay_result_to_json(result)), encoding="utf-8")
        except OSError as exc:
            why = os_error(f"--result-file {args.result_file}", exc)
            print(f"error: {why}", file=sys.stderr)
            return exitcodes.ERROR
    return result.exit_code


def _run_replay(
    args: argparse.Namespace,
    child: runner.Child,
    said: list[str],
    engine_factory: compose.EngineFactory,
) -> ReplayResult:
    """`cmd_replay` inside its SIGTERM handler: refused with what irimi said on stderr, or run,
    start to result."""
    try:
        source = _load_source(args)
        plan = replay_plan.plan(source.record, list(args.cmd), args.entrypoint, args.args)
        python = _replay_python(args, plan)
    except replay_plan.Refused as exc:
        print(f"error: {exc}", file=sys.stderr)
        result = replay_report.refused(args.run_id, said)
        # A SIGTERM that came while `--python` was asked stopped the replay, which exits as one
        # stopped before its command started, whatever the answer.
        return replace(result, exit_code=exitcodes.SIGTERM) if child.terminated else result
    if source.record.end is None:
        print(
            f"warning: run {args.run_id} did not end; it is replayed as far as it was recorded",
            file=sys.stderr,
        )
    # Made before the store is opened, so nothing after it can leave the store unclosed. Only
    # this user can enter it: the args are the run's, and the outcome the child's.
    with tempfile.TemporaryDirectory(prefix="irimi-replay-") as tmp:
        argv, call = list(plan.argv), None
        if plan.call is not None:
            call = runner.ChildCall(
                plan.call.entrypoint, Path(tmp) / "args.json", Path(tmp) / "outcome.json"
            )
            call.args_file.write_text(json.dumps(plan.call.args), encoding="utf-8")
            argv = [python, "-m", replay_plan.CHILD_MODULE]
        run = compose.prepare_run(args, source=source)
        if run is None:
            return replay_report.refused(args.run_id, said)
        record = process_run(run.run_id, argv, plan.trigger, source.record.run_id)
        said_before = len(said)
        with run:
            ran = run_child(
                args, run, child, ChildSpec("replay", argv, record, call), engine_factory
            )
    if args.python is None and _no_such_module(ran.call):
        # irimi's own Python is not the agent's when irimi is installed apart from it, as a tool:
        # say which Python it was, and how to name the agent's.
        print(
            f"hint: the trigger was called under irimi's own Python, {sys.executable}, which may "
            "not have the agent's packages; name the agent's Python with --python PATH",
            file=sys.stderr,
        )
    if ran.status != "ran":
        # The agent never ran: refused, or stopped by a signal before its run was stored, which
        # exits as `irimi shadow` would.
        stored = run.run_id if ran.status == "not-spawned" else None
        result = replay_report.refused(args.run_id, said, stored)
        if ran.code in (exitcodes.SIGINT, exitcodes.SIGTERM):
            return replace(result, exit_code=ran.code)
        return result
    if plan.call is not None and ran.call is not None and not ran.call.called:
        # The child said why on stderr, and the hint is said: the agent never ran, so there is no
        # replay to report and nothing to compare. The result holds both lines.
        why = ran.call.error.message if ran.call.error is not None else "it was not called"
        cannot = f"error: cannot call {plan.call.entrypoint}: {printable(why)}"
        lines = [*said[:said_before], cannot, *said[said_before:]]
        result = replay_report.refused(args.run_id, lines, run.run_id)
        return result if ran.stopped is None else replace(result, exit_code=ran.stopped)
    try:
        rerun = plan.argv if plan.rerun else None
        return _report_replay(args, source, run.run_id, ran, said, rerun)
    except KeyboardInterrupt:
        # Ctrl-C while irimi read the runs back or compared them: the replay's run is stored and
        # ended, and its result names it.
        return _interrupted(args, said, run.run_id)


def _load_source(args: argparse.Namespace) -> StoredRun:
    """The run `irimi replay` answers from, in the store under `--store`, read whole before
    anything starts, or `replay.plan.Refused` saying why it cannot be."""
    root = compose.store_root(args)
    if args.run_id == UNATTRIBUTED:
        # Every serve session's requests no run named, in one pool: not one run to answer from.
        raise replay_plan.Refused(f"{UNATTRIBUTED} is not a run to replay")
    try:
        # The whole id from here on: the replay's messages and its result name the run it read.
        args.run_id = compose.resolve_run_id(root, args.run_id)
    except LookupError as exc:
        raise replay_plan.Refused(str(exc)) from None
    try:
        return compose.load_stored_run(root, args.run_id)
    except CliError as exc:
        raise replay_plan.Refused(exc.message) from None


def _replay_python(args: argparse.Namespace, plan: replay_plan.Plan) -> str:
    """The Python a replay's child calls its trigger under: irimi's own, or `--python`, the
    agent's, once `runner.probe_python` has asked it for its irimi. One that does not run, or
    cannot import irimi's replay child, is Refused before anything starts; one with another irimi
    than this is warned of, since that irimi makes the call. `--python` for a run replayed by its
    recorded command is Refused, as with `-- CMD`: the command names its own Python."""
    if args.python is None:
        return sys.executable
    if plan.call is None:
        raise replay_plan.Refused(
            f"run {args.run_id} is replayed by its recorded command, which names its own Python; "
            "drop --python"
        )
    named = f"--python {printable(args.python)}"
    if os.path.isdir(args.python):
        # `--python .venv`, as uv's own `--python` takes it, is looked up on PATH, or cannot be
        # executed: either error would hide that the directory is the trouble.
        inside = printable(os.path.join(args.python, "bin", "python"))
        raise replay_plan.Refused(
            f"{named} is a directory; name the Python in it, as --python {inside}"
        )
    try:
        printed = runner.probe_python(args.python)
    except runner.PythonProbeFailed as exc:
        if exc.reason == "timeout":
            raise replay_plan.Refused(f"{named} did not answer within {exc.detail}") from None
        if exc.reason == "cannot-run":
            raise replay_plan.Refused(f"{named} could not be run: {exc.detail}") from None
        raise replay_plan.Refused(
            f"{named} cannot import irimi's replay child ({printable(exc.detail)}); install irimi "
            "in that Python's environment, or replay with -- CMD"
        ) from None
    version = printable(printed) if printed is not None else "?"
    if version != __version__:
        print(
            f"warning: {named} has irimi {version}, and this is irimi {__version__}; the "
            "trigger is called by that irimi, so install the same version in both",
            file=sys.stderr,
        )
    python: str = args.python
    return python


def _no_such_module(call: CallResult | None) -> bool:
    """Whether a replay's child could not call its trigger because a module it imports is not
    installed under the child's Python."""
    return call is not None and not call.called and call.cause == "builtins.ModuleNotFoundError"


def _report_replay(
    args: argparse.Namespace,
    source: StoredRun,
    run_id: str,
    ran: ChildResult,
    said: list[str],
    rerun: tuple[str, ...] | None,
) -> ReplayResult:
    """A replay whose run `run_id` was stored and ended: both runs read back, and what the
    replay did printed and returned (`replay.report.outcome`). `rerun` is the recorded
    command line a process run's replay ran again."""
    try:
        replayed = compose.load_stored_run(compose.store_root(args), run_id)
    except CliError as exc:
        why = exc.__cause__
        print(f"error: the replay's run {run_id} could not be read back: {why}", file=sys.stderr)
        return replay_report.refused(args.run_id, said, run_id)
    ended = replay_report.Ended(ran.code, ran.call, ran.stopped)
    outcome = replay_report.outcome(source, replayed, ended, compose.maps_for_stored_runs(), rerun)
    end_the_childs_line()
    for line in outcome.lines():
        print(line, flush=True)
    return outcome.result()


def _interrupted(args: argparse.Namespace, said: list[str], run_id: str | None) -> ReplayResult:
    """A replay Ctrl-C stopped while no agent ran, so none was waited on: exit 130, as `irimi
    shadow` exits, its result naming the replay's run when one was stored."""
    print("error: interrupted", file=sys.stderr)
    return replace(replay_report.refused(args.run_id, said, run_id), exit_code=exitcodes.SIGINT)


class _Tee:
    """`stream`, but each whole line written to it is kept in `lines` too. Everything else - a
    flush, `isatty`, `fileno` - is the stream's own. Not an `io` class, whose finalizer would
    flush a stream the caller may since have closed."""

    def __init__(self, stream: TextIO, lines: list[str]) -> None:
        self._stream = stream
        self._lines = lines
        self._partial = ""

    def write(self, text: str) -> int:
        self._stream.write(text)
        *whole, self._partial = (self._partial + text).split("\n")
        self._lines.extend(whole)
        return len(text)

    def __getattr__(self, name: str) -> object:
        return getattr(self._stream, name)


@contextlib.contextmanager
def _keeping_stderr() -> Iterator[list[str]]:
    """For the block, stderr as it is, and each line written to it kept in the list yielded: what
    irimi said of why a replay could not run, which its result holds. Python's own writes
    to stderr only, not a child's, which writes to the same file descriptor directly."""
    lines: list[str] = []
    with contextlib.redirect_stderr(cast(TextIO, _Tee(sys.stderr, lines))):
        yield lines
