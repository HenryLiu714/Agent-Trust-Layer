"""`irimi replay RUN_ID`: re-run a stored run's trigger against its recording and show changes."""

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
    RUN_ID_HELP,
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
        help=f"the run to replay: {RUN_ID_HELP}",
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
    # `cmd` is what follows `--`: a command to run instead of calling the entrypoint.
    replay.set_defaults(func=cmd_replay, cmd=[])


def cmd_replay(
    args: argparse.Namespace, *, engine_factory: compose.EngineFactory = compose.build_engine
) -> int:
    """Run `irimi replay RUN` and return one of `EXIT_CODES`.

    `--result-file` is written whether or not the replay ran; a refusal's stderr lines go in it.
    """
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
            # Ctrl-C before any run was stored.
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
    """Plan and run the replay, or return a refusal holding what irimi printed to stderr."""
    try:
        source = _load_source(args)
        plan = replay_plan.plan(source.record, list(args.cmd), args.entrypoint, args.args)
        python = _replay_python(args, plan)
    except replay_plan.Refused as exc:
        print(f"error: {exc}", file=sys.stderr)
        result = replay_report.refused(args.run_id, said)
        # A SIGTERM during the `--python` probe exits as one before the command started.
        return replace(result, exit_code=exitcodes.SIGTERM) if child.terminated else result
    if source.record.end is None:
        print(
            f"warning: run {args.run_id} did not end; it is replayed as far as it was recorded",
            file=sys.stderr,
        )
    # Made before the store opens, so nothing after it can leave the store unclosed. It is private
    # to this user, since it holds the run's args.
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
        # irimi installed as a tool runs under a Python without the agent's packages.
        print(
            f"hint: the trigger was called under irimi's own Python, {sys.executable}, which may "
            "not have the agent's packages; name the agent's Python with --python PATH",
            file=sys.stderr,
        )
    if ran.status != "ran":
        # The agent never ran; a signal exits as `irimi shadow` would.
        stored = run.run_id if ran.status == "not-spawned" else None
        result = replay_report.refused(args.run_id, said, stored)
        if ran.code in (exitcodes.SIGINT, exitcodes.SIGTERM):
            return replace(result, exit_code=ran.code)
        return result
    if plan.call is not None and ran.call is not None and not ran.call.called:
        # The trigger was never called, so there is nothing to compare.
        why = ran.call.error.message if ran.call.error is not None else "it was not called"
        cannot = f"error: cannot call {plan.call.entrypoint}: {printable(why)}"
        lines = [*said[:said_before], cannot, *said[said_before:]]
        result = replay_report.refused(args.run_id, lines, run.run_id)
        return result if ran.stopped is None else replace(result, exit_code=ran.stopped)
    try:
        rerun = plan.argv if plan.rerun else None
        return _report_replay(args, source, run.run_id, ran, said, rerun)
    except KeyboardInterrupt:
        # The replay's run is already stored and ended, so the result names it.
        return _interrupted(args, said, run.run_id)


def _load_source(args: argparse.Namespace) -> StoredRun:
    """Return the stored run to replay, read whole; raise `replay.plan.Refused` if it cannot be."""
    root = compose.store_root(args)
    if args.run_id == UNATTRIBUTED:
        # A pool of requests from many sessions, not one run.
        raise replay_plan.Refused(f"{UNATTRIBUTED} is not a run to replay")
    try:
        # Messages and the result name the full id from here on.
        args.run_id = compose.resolve_run_id(root, args.run_id)
    except LookupError as exc:
        raise replay_plan.Refused(str(exc)) from None
    try:
        return compose.load_stored_run(root, args.run_id)
    except CliError as exc:
        raise replay_plan.Refused(exc.message) from None


def _replay_python(args: argparse.Namespace, plan: replay_plan.Plan) -> str:
    """Return the Python the child calls the trigger under: irimi's own, or a probed `--python`.

    Raises Refused for a `--python` that cannot run the replay child, or with a recorded command.
    Warns when its irimi version differs from this one.
    """
    if args.python is None:
        return sys.executable
    if plan.call is None:
        raise replay_plan.Refused(
            f"run {args.run_id} is replayed by its recorded command, which names its own Python; "
            "drop --python"
        )
    named = f"--python {printable(args.python)}"
    if os.path.isdir(args.python):
        # Running a directory gives an error that hides the real mistake.
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
    """Return True if the trigger was not called because a module was not installed."""
    return call is not None and not call.called and call.cause == "builtins.ModuleNotFoundError"


def _report_replay(
    args: argparse.Namespace,
    source: StoredRun,
    run_id: str,
    ran: ChildResult,
    said: list[str],
    rerun: tuple[str, ...] | None,
) -> ReplayResult:
    """Read both runs back, print the comparison and return the result.

    `rerun` is the recorded command line a process run's replay ran again.
    """
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
    """Return the result of a replay Ctrl-C stopped while no agent ran: exit 130."""
    print("error: interrupted", file=sys.stderr)
    return replace(replay_report.refused(args.run_id, said, run_id), exit_code=exitcodes.SIGINT)


class _Tee:
    """Wrap `stream`, also keeping each whole line written in `lines`.

    Not an `io` class, whose finalizer would flush a stream the caller may have closed.
    """

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
    """Keep each line Python writes to stderr in the yielded list; a child's writes are not seen."""
    lines: list[str] = []
    with contextlib.redirect_stderr(cast(TextIO, _Tee(sys.stderr, lines))):
        yield lines
