"""`irimi runs list`, `irimi runs show` and `irimi compare`: the runs in the trace store, read
back."""

import argparse
import sys
from typing import Any

from irimi.cli import CliError, compose, os_error
from irimi.cli.options import (
    LAST_RUN,
    RUN_ID_HELP,
    add_store_option,
    epilog,
    help_formatted,
    positive_int,
)
from irimi.core import exitcodes
from irimi.protocol.trace import UNATTRIBUTED
from irimi.record.store.reader import StoreReader
from irimi.replay import compare
from irimi.report import runs

DEFAULT_RUNS_LIMIT = 20  # `irimi runs list` without --limit

COMPARE_EXIT_CODES = exitcodes.help_table(
    [exitcodes.OK, exitcodes.DIFFERENT, exitcodes.ERROR, exitcodes.BROKEN_PIPE]
)


def register(subparsers: "argparse._SubParsersAction[Any]") -> None:
    runs_parser = subparsers.add_parser(
        "runs",
        **help_formatted(
            help="read back the runs in the trace store",
            description="Read back the runs irimi stored, redacted. With no subcommand, list them.",
            epilog=epilog(["irimi runs", "irimi runs show last", "irimi runs show unattributed"]),
        ),
    )
    runs_sub = runs_parser.add_subparsers(
        dest="runs_command", metavar="<subcommand>", required=True
    )
    runs_list = runs_sub.add_parser(
        "list",
        **help_formatted(
            help="list stored runs, newest first",
            description="List the stored runs, newest first: one line each, under a header row.",
            epilog=epilog(["irimi runs list", "irimi runs list --all --store ./runs"]),
        ),
    )
    runs_list.add_argument(
        "--limit",
        type=positive_int,
        default=DEFAULT_RUNS_LIMIT,
        metavar="N",
        help=f"list at most N runs (default {DEFAULT_RUNS_LIMIT})",
    )
    runs_list.add_argument("--all", action="store_true", help="list every run")
    add_store_option(runs_list)
    runs_list.set_defaults(func=cmd_runs_list)
    runs_show = runs_sub.add_parser(
        "show",
        **help_formatted(
            help="show one stored run",
            description="Show one stored run: its record, every exchange and tool call, and the "
            "summary irimi printed\nfor it.",
            epilog=epilog(["irimi runs show", "irimi runs show 7729eae4"]),
        ),
    )
    runs_show.add_argument(
        "run_id",
        nargs="?",
        default=LAST_RUN,
        metavar="RUN",
        help=f"{RUN_ID_HELP} (default {LAST_RUN})",
    )
    add_store_option(runs_show)
    runs_show.set_defaults(func=cmd_runs_show)

    compare_runs = subparsers.add_parser(
        "compare",
        **help_formatted(
            help="show the writes two stored runs made, side by side",
            description="Show the writes two stored runs made, side by side, and exit as diff(1) "
            "does.",
            epilog=epilog(["irimi compare 7729eae4 last"], COMPARE_EXIT_CODES),
        ),
    )
    compare_runs.add_argument("run_a", metavar="RUN_A", help=f"the first run: {RUN_ID_HELP}")
    compare_runs.add_argument("run_b", metavar="RUN_B", help=f"the second run: {RUN_ID_HELP}")
    add_store_option(compare_runs)
    compare_runs.set_defaults(func=cmd_compare)


def cmd_runs_list(args: argparse.Namespace) -> int:
    """`irimi runs list`: one line per stored run, newest first."""
    root = compose.store_root(args)
    try:
        every = StoreReader(root).list_runs()
    except OSError as exc:
        print(f"error: {os_error(compose.store_subject(args), exc)}", file=sys.stderr)
        return exitcodes.ERROR
    if not every:
        print(f"no runs in {root}")
        return 0
    records = every if args.all else every[: args.limit]
    rows = []
    for record in records:
        # One damaged run is named and counted as `?`, never allowed to hide the others: the
        # store's own rule for `list_runs`.
        # Counted, never read: no body is loaded, or a few runs of LLM streams cost hundreds of
        # MB to print two numbers each.
        try:
            events = compose.load_stored_run(root, record.run_id, bodies=False).events
        except CliError as exc:
            print(
                f"warning: run {record.run_id} could not be read: {exc.__cause__}", file=sys.stderr
            )
            events = None
        rows.append(runs.run_list_columns(record, events))
    for line in runs.run_list_lines(rows, older=len(every) - len(records)):
        print(line)
    return 0


def cmd_runs_show(args: argparse.Namespace) -> int:
    """`irimi runs show RUN_ID`: a stored run's header, every event, and its summary."""
    root = compose.store_root(args)
    try:
        run_id = compose.resolve_run_id(root, args.run_id)
    except LookupError as exc:
        raise CliError(str(exc), exitcodes.ERROR) from None
    run = compose.load_stored_run(root, run_id)
    # The unattributed pool is not a run: with nothing in it there is no record to show.
    if run_id == UNATTRIBUTED and not run.events:
        print(f"no unattributed events in {root}")
        return 0
    for line in runs.run_show_lines(run, compose.maps_for_stored_runs()):
        print(line)
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    """`irimi compare RUN_A RUN_B`: two stored runs' writes side by side. Exits as diff(1) does
    (`COMPARE_EXIT_CODES`): 0 when every write is the same, 1 when any is not, 2 when a run cannot
    be read."""
    root = compose.store_root(args)
    stored = []
    for typed in (args.run_a, args.run_b):
        try:
            run_id = compose.resolve_run_id(root, typed)
        except LookupError as exc:
            raise CliError(str(exc), exitcodes.ERROR) from None
        stored.append(compose.load_stored_run(root, run_id))
    a, b = stored
    comparison = compare.compare_runs(a, b, compose.maps_for_stored_runs())
    for line in [*compare.header_lines(a.record, b.record), "", *comparison.lines()]:
        print(line)
    return comparison.exit_code
