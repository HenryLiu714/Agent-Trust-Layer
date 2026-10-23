"""What `irimi runs list`, `irimi runs show` and `irimi compare` print of stored runs: a row per
run, a run's header block and events, and a run's label. Counts come from `model.RunSummary`, so
`runs list` and a run's summary agree.
"""

import json
import os
import shlex
import time
from collections.abc import Sequence
from typing import assert_never

from irimi.core.exchange import Exchange
from irimi.protocol.trace import Event, RunRecord, StoredRun, TelemetrySeen, ToolCall, trigger_args
from irimi.report import model
from irimi.report.model import RunView, run_elapsed
from irimi.report.summary import summary_lines
from irimi.report.terminal import exchange_line, plural, printable, tool_call_line
from irimi.servicemaps import MapIndex

# `irimi runs show`'s `args:` line: enough to recognise a trigger by, not a dump of it.
MAX_ARGS_SHOWN = 500
NOT_SET = "-"
# The `args:` of a trigger nested deeper than `json.dumps` writes. Python 3.14's `json.loads` reads
# a `run.json` some levels deeper than its `json.dumps` writes back, so a damaged one reads fine
# and would then raise RecursionError out of `runs show`.
ARGS_TOO_DEEP = "(nested too deeply to show)"


def run_status(record: RunRecord) -> str:
    """`ok` or `error` for a run that ended, and `incomplete` for one that has not, or never will:
    a crashed irimi, or a run known only by its events (`attribution: "header"`)."""
    return record.end.outcome if record.end is not None else "incomplete"


# `irimi runs list`'s header row, one name per column.
RUN_LIST_HEADER = (
    "run",
    "started",
    "took",
    "status",
    "attribution",
    "trigger",
    "exchanges",
    "writes",
)
# How much of a trigger `runs list` prints: enough to tell two agents apart on one line.
MAX_TRIGGER_SHOWN = 40


def run_list_columns(record: RunRecord, events: Sequence[Event] | None) -> list[str]:
    """One `irimi runs list` row, a string per column of `RUN_LIST_HEADER`. `events` is None for
    a run whose events could not be read, which then shows `?` for both counts rather than zero.
    No body is read, so a caller may pass events loaded without their bodies."""
    started = NOT_SET
    if record.started_at is not None:
        started = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.started_at))
    elapsed = run_elapsed(record)
    duration = NOT_SET if elapsed is None else f"{elapsed:.1f}s"
    status = run_status(record)
    end = record.end
    if end is not None and end.outcome == "error" and end.exit_code is not None:
        status = f"error (exit {end.exit_code})"
    if events is None:
        counts = ["? exchanges", "? writes"]
    else:
        summary = model.build(RunView(record, events), None)
        counts = [plural(summary.exchanges, "exchange"), plural(len(summary.writes), "write")]
    return [
        record.run_id,
        started,
        duration,
        status,
        run_attribution(record),
        _trigger_column(record),
        *counts,
    ]


def _trigger_column(record: RunRecord) -> str:
    """A run's trigger as `runs list` shows it: a process run's command line with its program's
    directory left out, so two agents run by one Python differ, and any other run's trigger
    name; cut to `MAX_TRIGGER_SHOWN`."""
    trigger = record.trigger
    if trigger is None:
        return NOT_SET
    argv = record.argv
    if argv is not None:
        text = shlex.join([os.path.basename(argv[0]) or argv[0], *argv[1:]])
    else:
        text = trigger.name
    text = printable(text)
    return text if len(text) <= MAX_TRIGGER_SHOWN else text[: MAX_TRIGGER_SHOWN - 1] + "…"


def run_list_lines(rows: Sequence[Sequence[str]], older: int = 0) -> list[str]:
    """`irimi runs list`'s output: the header row and each row of `run_list_columns`, each column
    padded to its widest cell, then a line saying how many older runs were left out."""
    table = [list(RUN_LIST_HEADER), *(list(row) for row in rows)]
    widths = [max(len(row[i]) for row in table) for i in range(len(RUN_LIST_HEADER) - 1)]
    lines = [
        "  ".join([*(cell.ljust(width) for cell, width in zip(row, widths, strict=False)), row[-1]])
        for row in table
    ]
    if older:
        lines.append(f"({plural(older, 'older run')}; use --limit or --all)")
    return lines


def run_attribution(record: RunRecord) -> str:
    """`irimi runs list`'s attribution column. A run a replay stored is listed as the run it
    replayed: `replay of <id>` for the process run `irimi replay` stores, which `process` alone
    lists as a shadow run, and after its attribution for any other, since a replay's store makes
    `header` runs and `sdk` runs too and the column must not hide which (`<attribution>, replay of
    <id>`)."""
    if record.replay_of is not None:
        replayed = f"replay of {printable(record.replay_of)}"
        return replayed if record.attribution == "process" else f"{record.attribution}, {replayed}"
    return record.attribution


def run_label(record: RunRecord) -> str:
    """A stored run in one line, as `irimi compare` names each side: its trigger's name and
    entrypoint, and the agent version it reported, each one it has, escaped as `runs show`
    escapes them; "" for a run that has none. The entrypoint is left out when it is the name,
    as a trigger's default name is."""
    trigger = record.trigger
    parts = [
        trigger.name if trigger is not None else None,
        trigger.entrypoint if trigger is not None and trigger.entrypoint != trigger.name else None,
        f"agent {record.agent_version}" if record.agent_version else None,
    ]
    return "  ".join(printable(part) for part in parts if part)


def run_header_lines(record: RunRecord) -> list[str]:
    """`irimi runs show`'s header block: one `key: value` per line."""
    trigger = record.trigger
    entrypoint = (trigger.entrypoint if trigger is not None else None) or NOT_SET
    args = NOT_SET
    if trigger is not None:
        try:
            args = json.dumps(trigger_args(trigger), separators=(",", ":"), ensure_ascii=False)
        except RecursionError:
            args = ARGS_TOO_DEEP
        if len(args) > MAX_ARGS_SHOWN:
            args = args[:MAX_ARGS_SHOWN] + "…"
    lines = [f"run: {record.run_id}", f"attribution: {record.attribution}"]
    # A replay's run says so, and which run it replayed; a shadow run's block has neither line.
    if record.mode != "shadow":
        lines.append(f"mode: {record.mode}")
    if record.replay_of is not None:
        lines.append(f"replay of: {printable(record.replay_of)}")
    lines += [
        f"trigger: {printable(trigger.name) if trigger is not None else NOT_SET}",
        f"entrypoint: {printable(entrypoint)}",
        f"args: {printable(args)}",
        f"agent version: {printable(record.agent_version or NOT_SET)}",
        f"outcome: {run_status(record)}",
    ]
    error = record.end.error if record.end is not None else None
    if error is not None:
        lines.append(f"error: {printable(error.type)}: {printable(error.message)}")
    if record.end is not None and record.end.exit_code is not None:
        lines.append(f"exit code: {record.end.exit_code}")
    if record.dropped_events:
        lines.append(f"dropped events: {record.dropped_events}")
    return lines


def event_line(event: Event) -> str:
    """One stored event, as `irimi runs show` prints it: an exchange and a tool call in the
    columns the terminal printed them in, and telemetry as the host it went to."""
    match event:
        case Exchange():
            return exchange_line(event)
        case ToolCall():
            return tool_call_line(event)
        case TelemetrySeen():
            return f"telemetry {event.host}"
        case _:
            assert_never(event)


def run_show_lines(run: StoredRun, index: MapIndex | None) -> list[str]:
    """Everything `irimi runs show` prints: the header block, every event in `seq` order, and the
    run's summary, a blank line between each."""
    events = [event_line(event) for event in run.events]
    lines = run_header_lines(run.record)
    if events:
        lines += ["", *events]
    return [*lines, "", *summary_lines(RunView(run.record, run.events), index)]
