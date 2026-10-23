"""The exit summary: a `model.RunSummary` as the lines `irimi shadow`, serve and `runs show`
print. String formatting only; what each line counts is the model's.
"""

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from irimi.core.bodies import reflect
from irimi.core.exchange import Exchange
from irimi.core.money import format_minor_units
from irimi.protocol.trace import RunRecord, TelemetrySeen, ToolCall
from irimi.report import model
from irimi.report.model import (
    Faked,
    HostCounts,
    Refused,
    RunSummary,
    RunView,
    SentToTarget,
    Shown,
    TargetUnreachable,
    WriteEntry,
)
from irimi.report.terminal import answer_word, plural, printable, tool_outcome
from irimi.servicemaps import MapIndex, Route, path_params

WRITE_MARKER = "○"
REJECTED_MARKER = "✗"
# The service column of a write tool's line.
TOOL_SERVICE = "tool"
# One of the agent's own reads that irimi either edited to show this run's writes, or knows it
# could not fully show them. It hangs under the write it is about, so a reader sees the
# write and the reads that then saw it as one thing.
OVERLAY_MARKER = "↳"
SAW_IT = "saw it"
SAW_PART = "saw it in part"
DID_NOT_SHOW = "did not show it"
# The line that opens the writes block. True of every line in it, whoever answered the write.
NOTHING_HAPPENED = "None of these writes reached the real service:"
WOULD_HAVE_FIRED = "would have fired:"
# A write's status, after its service and its sentence.
FAKED = "faked"
CHECKED = "checked against live state: would succeed"
WOULD_FAIL = "would fail"
COULD_NOT_CHECK = "could not check"
NOT_CHECKED = "not checked"
UNCLASSIFIED = "unclassified"
SENT_TO_TARGET = "sent to target"
TARGET_UNREACHABLE = "could not reach target"
REFUSED_BY_IRIMI = "refused by irimi"
TOO_LARGE = "its body is too large to read whole"
DECISION_FAILED = "irimi could not decide how to answer it"
UNKNOWN_CURRENCY = "minor units, currency unknown"
# How much of a run id the summary prints: `irimi replay` and `runs show` take a unique prefix.
SHORT_ID = 8
# A host-line phrase for requests whose target irimi refused or could not reach.
UNREACHED = "that could not reach the target"
# What a live run that saw no exchange says: an agent whose client ignored the proxy, or refused
# irimi's CA, still made its calls. Not said of a stored run, whose traffic its SDK runs may hold.
NO_TRAFFIC = (
    "irimi saw no HTTP(S) traffic; if the agent made calls, they bypassed the proxy or failed TLS"
)

# A `{name}` hole in a map's `human:` template, or `{name:money}` for a field holding an amount in
# minor units. Deliberately not `str.format`: a template carries literal braces (Slack's
# `#{channel}` sits next to none, but a body field might) and `format` raises on an unmatched one,
# inside the function that prints the run's most important output.
_FIELD = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)(?::(money))?\}")
MISSING_FIELD = "?"


@dataclass(frozen=True)
class Live:
    """What only a live `irimi shadow` knows of its run, which a stored run does not keep: the
    runs the SDK started inside the process, by id, each with its stored record or None; how many
    TLS handshakes clients refused, by host; and the certificate those lines tell the reader to
    point a client at."""

    sdk_runs: Mapping[str, RunRecord | None] = field(default_factory=dict)
    tls_failures: Mapping[str, int] = field(default_factory=dict)
    ca_cert: Path | None = None


def summary_lines(
    view: RunView,
    index: MapIndex | None,
    elapsed_s: float | None = None,
    live: Live | None = None,
) -> list[str]:
    """The exit summary: what the agent tried to write, what was real, and what irimi answered.

    `index` is the loaded maps, used only to find each write's `human:` template. Without it the
    writes still get a line, spelled as the request they were. `elapsed_s` is how long the child
    ran, which a live run timed; a stored run's is its own, `ended_at - started_at`, or `0.0` for
    one with no start or no end. A replay's run opens `irimi replay`: `irimi replay` printed its
    comparison and no summary, and a block opening `irimi shadow` would say it was a shadow run.

    `live` is a live run's own facts. With SDK runs among them the header says it covers the
    whole process and the block ends with a line per run; a live run that saw no exchange says so,
    and each host whose clients refused irimi's certificate gets a line. A stored run has none of
    these lines. Where a stored line may still differ from the live one: redaction replaced a
    field a `human:` template renders, the path of a request on a credential-path host that the
    write line spells out, because no route matched it or irimi refused it with its body cut, a
    write irimi refused whose body only the store cut, spelled out, or an event the store dropped
    (docs/trace-format.md, "Reading a run back"). A process run's stored summary covers its own
    events, never the SDK runs inside it.
    """
    summary = model.build(view, index)
    if elapsed_s is None:
        elapsed_s = model.run_elapsed(view.record) or 0.0
    whole = live is not None and bool(live.sdk_runs)
    lines = _summary(summary, elapsed_s, whole)
    if live is None:
        return lines
    if not summary.exchanges:
        lines.append(f"  {NO_TRAFFIC}")
    lines += tls_failure_lines(live.tls_failures, live.ca_cert)
    if live.sdk_runs:
        lines += ["", *_sdk_run_lines(view, summary, live.sdk_runs)]
    return lines


def _summary(summary: RunSummary, elapsed_s: float, whole: bool) -> list[str]:
    """The block every summary prints. A tool call is in no count: each write tool whose fake ran
    gets a line after the HTTP writes, and a read tool gets none.

    The writes come first, under the line saying none of them reached the real service, with
    the money they name and the webhooks they would have fired; then each host's counts; then
    the totals."""
    record = summary.record
    command = "replay" if record.mode == "replay" else "shadow"
    scope = " (whole process)" if whole else ""
    total = summary.exchanges
    lines = [
        f"irimi {command} · run {record.run_id}{scope} · {plural(total, 'exchange')} · "
        f"{elapsed_s:.1f}s"
    ]
    write_lines: list[str] = []
    for entry in summary.writes:
        write_lines.append(_write_line(entry))
        write_lines += _overlay_lines(entry.reads)
    # In the order the calls ended, whatever order they came in: live, the order their posts
    # reached the control endpoint, which two tools ending at once on two threads can reverse, and
    # stored, `seq` order within each run. The start, the id and then the line itself break
    # a tie, since a call posted by hand can repeat another's id and times.
    tools = sorted(
        summary.tool_writes,
        key=lambda call: (
            call.ended_at,
            call.started_at,
            call.tool_call_id,
            tool_write_sentence(call),
        ),
    )
    write_lines += [tool_write_line(call) for call in tools]
    if write_lines:
        lines += ["", f"  {NOTHING_HAPPENED}", *write_lines, *_money_line(summary.writes)]
        if summary.would_fire:
            lines.append(f"  {WOULD_HAVE_FIRED} {', '.join(summary.would_fire)}")
    host_lines = _host_lines(summary.hosts, summary.telemetry)
    if host_lines:
        lines += ["", *host_lines]
    lines += [
        "",
        f"  {plural(total, 'exchange')} · {summary.real} real · {summary.by_irimi} answered by "
        f"irimi ({plural(summary.faked, 'faked write')}) · {summary.targets} sent to targets",
    ]
    return lines


# ---------------------------------------------------------------------------------- write lines


def tool_write_line(call: ToolCall) -> str:
    """The summary's line for a write tool whose fake ran in its place, escaped as
    `terminal.tool_call_line` is."""
    return f"  {printable(tool_write_sentence(call))}"


def tool_write_sentence(call: ToolCall) -> str:
    """`tool_write_line` without the summary's indent and before it is escaped, which `irimi
    compare` prints for it: the real function did not run, so it is a write that did not happen,
    and it takes the write marker."""
    return f"{WRITE_MARKER} {TOOL_SERVICE}  {call.name}  fake {tool_outcome(call, 'ran instead')}"


def _write_line(entry: WriteEntry) -> str:
    """One write's summary line. Escaped as `runs show` escapes a run's text: a template's fields
    are the agent's own, a newline in a Slack post would split the line, and a lone surrogate
    would make printing it raise."""
    return f"  {printable(write_sentence(entry))}"


def write_sentence(entry: WriteEntry) -> str:
    """One write: its marker, its service, the map's own sentence, and what irimi did with it.
    Its summary line without the indent and before it is escaped, which `irimi compare` prints
    for it. A write irimi refused with a body it never read whole is spelled as the request it
    was instead."""
    exchange, status = entry.exchange, entry.status
    # A `human:` sentence is never filled in from a body irimi did not read: a refused write whose
    # body was cut renders every field `?`, the same line for every such refund.
    unread = isinstance(status, Refused) and status.body_cut
    route = entry.route
    if route is not None and route.human and not unread:
        what = render_human(route.human, exchange, route)
    else:
        what = f"{exchange.request.method} {exchange.request.host}{exchange.request.path}"
    what = f"{exchange.service}  {what}"
    match status:
        case Refused(decision_failed=False):
            return f"{REJECTED_MARKER} {what}  {REFUSED_BY_IRIMI}"
        case Refused():
            why = TOO_LARGE if status.body_cut else DECISION_FAILED
            return f"{REJECTED_MARKER} {what}  {REFUSED_BY_IRIMI}: {why}"
        case TargetUnreachable():
            return f"{REJECTED_MARKER} {what}  {TARGET_UNREACHABLE} {_target_host(status.target)}"
        case SentToTarget():
            return f"{WRITE_MARKER} {what}  {SENT_TO_TARGET} {_target_host(status.target)}"
        case Faked(would_fail=str() as code):
            # A write the real service would have refused, and irimi is saying so. `✗`, not `○`,
            # and the code is the one the agent's SDK raised on.
            return f"{REJECTED_MARKER} {what}  {FAKED}; {WOULD_FAIL}: {code}"
        case Faked(checked="passed"):
            return f"{WRITE_MARKER} {what}  {FAKED}; {CHECKED}"
        case Faked(checked="not_evaluable"):
            why = _why_not_checked(exchange, status, entry.check_read)
            return f"{WRITE_MARKER} {what}  {FAKED}; {COULD_NOT_CHECK}: {why}"
        case Faked(unclassified=True):
            return f"{WRITE_MARKER} {what}  {FAKED}; {NOT_CHECKED} ({UNCLASSIFIED})"
        case _:
            return f"{WRITE_MARKER} {what}  {FAKED}; {NOT_CHECKED}"


def _target_host(target: str) -> str:
    """A target's host and port, as a write line names it."""
    return urlsplit(target).netloc or target


def _why_not_checked(exchange: Exchange, status: Faked, check_read: Exchange | None) -> str:
    """Why irimi could not check a write against live state, read off the read it made to. A
    write a replay answered from the recording made its check read in the recorded run."""
    if check_read is None and status.recorded:
        return "the recorded run could not check it"
    if check_read is None:
        return "irimi made no check read"
    if check_read.response is None:
        # No answer irimi could use: a network failure, a body too large to read, or a replay
        # whose recording holds none.
        return f"irimi's check read of {exchange.service} got no usable answer"
    if check_read.response.status != 200:
        return f"{exchange.service} answered irimi's check read with {check_read.response.status}"
    return "irimi could not tell from its check read"


def _overlay_lines(reads: Sequence[tuple[Exchange, Shown]]) -> list[str]:
    """The `↳` lines under one write: which of the agent's reads were shown it, and how fully. A
    read irimi edited is the overlay's; one it left untouched is whoever answered it, the service
    or a replay's recording."""
    lines: list[str] = []
    for ex, shown in reads:
        what = f"{ex.request.method} {ex.request.path}"
        if shown == "saw":
            verb, column = SAW_IT, "overlay"
        elif shown == "part":
            verb, column = SAW_PART, "overlay (partial)"
        else:
            verb, column = DID_NOT_SHOW, f"{answer_word(ex)} (partial)"
        lines.append(printable(f"    {OVERLAY_MARKER} {what} {verb}  {column}"))
    return lines


# ------------------------------------------------------------------------------ human templates


def _minor_units(value: object) -> int | None:
    """`value` when it can be an amount in minor units, else None."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _field_value(value: object, as_money: bool, currency: object) -> str:
    """One template field, rendered. A field the template marks `:money` is minor units of the
    write's currency (the request's, else the one its precondition read found); a currency irimi
    does not know prints the bare amount rather than a guessed one."""
    amount = _minor_units(value) if as_money else None
    if amount is None:
        return str(value)
    if isinstance(currency, str) and currency:
        return format_minor_units(amount, currency)
    return f"{amount} ({UNKNOWN_CURRENCY})"


def _fields(exchange: Exchange, route: Route | None) -> tuple[dict[str, object], object]:
    """A write's template fields and its currency, as `render_human` reads them."""
    fields: dict[str, object] = {}
    if route is not None:
        fields.update(path_params(route.path, exchange.request.path))
    fields.update(reflect(exchange.request))
    # `or` and not `setdefault`: a body that sent `currency=` sent nothing, and falls back to the
    # L3 read's currency like a body that sent no key at all.
    return fields, fields.get("currency") or exchange.precondition.currency


def render_human(template: str, exchange: Exchange, route: Route | None) -> str:
    """A map's `human:` template with this request's own fields in its holes.

    Fields come from two places and the body wins: the route pattern's `{name}` segments bound to
    the path (`/v1/charges/{charge}`), and the request body as the L0 echo reflects it, which is
    the one parser that already knows Stripe's bracketed form encoding. A hole with no field
    renders `?` rather than disappearing: a summary that silently drops what it could not read
    would claim to describe a write it did not fully understand.

    The currency is the one field that may come from outside the request: a write whose L3
    precondition read named one carries it on the exchange, and it fills in for a request that
    named none. The request still wins when it named one.
    """
    fields, currency = _fields(exchange, route)

    def one(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in fields:
            return MISSING_FIELD
        return _field_value(fields[name], match.group(2) is not None, currency)

    return _FIELD.sub(one, template)


def _amounts(exchange: Exchange, route: Route | None) -> list[tuple[int, str]]:
    """Each amount a write's `human:` sentence shows, with its lower-case currency, or "" for an
    amount whose currency irimi does not know."""
    if route is None or not route.human:
        return []
    fields, currency = _fields(exchange, route)
    code = currency.lower() if isinstance(currency, str) else ""
    found: list[tuple[int, str]] = []
    for name, spec in _FIELD.findall(route.human):
        amount = _minor_units(fields.get(name)) if spec else None
        if amount is not None:
            found.append((amount, code))
    return found


def _money_line(writes: Sequence[WriteEntry]) -> list[str]:
    """The amounts the faked writes' sentences show, summed per currency, and how much of that the
    real service would have refused. Nothing when no amount has a known currency: an amount irimi
    cannot denominate is counted apart rather than added to one it can."""
    total: dict[str, int] = {}
    failing: dict[str, int] = {}
    unknown = 0
    for entry in writes:
        status = entry.status
        if not isinstance(status, Faked):
            continue
        for value, code in _amounts(entry.exchange, entry.route):
            if not code:
                unknown += 1
                continue
            total[code] = total.get(code, 0) + value
            if status.would_fail is not None:
                failing[code] = failing.get(code, 0) + value
    if not total:
        return []
    line = f"  money: {_sum(total)} in faked writes"
    if failing:
        line += f"; {_sum(failing)} of it would fail"
    if unknown:
        line += f"; {plural(unknown, 'amount')} in an unknown currency not counted"
    return [line]


def _sum(amounts: dict[str, int]) -> str:
    return " + ".join(format_minor_units(value, code) for code, value in amounts.items())


# ----------------------------------------------------------------------------------- host lines


def _host_line(counts: HostCounts, width: int) -> str:
    """One host's counts, in the order a reader asks them: what was real, then what was not."""
    phrases: list[str] = []
    if counts.reads:
        qualified = []
        if counts.overlaid:
            qualified.append(f"{counts.overlaid} showing this run's writes")
        if counts.recorded_reads:
            qualified.append(f"{counts.recorded_reads} from the recording")
        if counts.failed_reads:
            codes = ", ".join(dict.fromkeys(counts.failed_reads))
            qualified.append(f"{len(counts.failed_reads)} failed: {codes}")
        reads = plural(counts.reads, "read")
        phrases.append(f"{reads} ({', '.join(qualified)})" if qualified else reads)
    if counts.check_reads:
        phrases.append(plural(counts.check_reads, "irimi check read"))
    if counts.target_reads:
        phrases.append(f"{plural(counts.target_reads, 'read')} from the target")
    if counts.refused_reads:
        phrases.append(f"{plural(counts.refused_reads, 'read')} {REFUSED_BY_IRIMI}")
    if counts.unreached_reads:
        phrases.append(f"{plural(counts.unreached_reads, 'read')} {UNREACHED}")
    if counts.llm:
        note = f" ({counts.recorded_llm} from the recording)" if counts.recorded_llm else ""
        phrases.append(f"{counts.llm} llm{note}")
    if counts.faked_writes:
        unclassified = counts.unclassified_writes
        note = f" ({unclassified} {UNCLASSIFIED})" if unclassified else ""
        phrases.append(f"{plural(counts.faked_writes, 'faked write')}{note}")
    if counts.refused_writes:
        phrases.append(f"{plural(counts.refused_writes, 'write')} {REFUSED_BY_IRIMI}")
    if counts.target_writes:
        phrases.append(f"{plural(counts.target_writes, 'write')} sent to the target")
    if counts.unreached_writes:
        phrases.append(f"{plural(counts.unreached_writes, 'write')} {UNREACHED}")
    return printable(f"  {counts.host:<{width}}  {'  '.join(phrases)}".rstrip())


def _host_lines(hosts: Sequence[HostCounts], telemetry: Sequence[str]) -> list[str]:
    """One line per host, plus one line for all telemetry hosts together.

    Telemetry gets a single line however many vendors it reached: a run posts to Datadog, Sentry
    and PostHog in the same breath and a line each would bury the two hosts the agent's work is
    actually on. It also says where they went, because they went there for real. It counts
    hosts only, since that is all the store keeps of a telemetry exchange.
    """
    width = max((len(counts.host) for counts in hosts), default=0)
    width = max(width, len("telemetry")) if telemetry else width
    lines = [_host_line(counts, width) for counts in hosts]
    if telemetry:
        vendors = len(set(telemetry))
        lines.append(
            printable(
                f"  {'telemetry':<{width}}  {plural(len(telemetry), 'exchange')} "
                f"to {plural(vendors, 'host')}, forwarded live"
            )
        )
    return lines


# ----------------------------------------------------------------------------- live-only lines


def _sdk_run_lines(
    view: RunView, summary: RunSummary, runs: Mapping[str, RunRecord | None]
) -> list[str]:
    """One line per SDK run inside the process, then how to replay one: the live summary counts
    the whole process, and a replay needs the SDK run's own id."""
    lines: list[str] = []
    for run_id, record in runs.items():
        trigger = record.trigger if record is not None else None
        call = MISSING_FIELD
        if trigger is not None:
            call = f"{trigger.name}({'...' if trigger.args else ''})"
        seen = sum(
            1
            for event in view.events
            if isinstance(event, Exchange | TelemetrySeen) and event.run_id == run_id
        )
        writes = sum(1 for entry in summary.writes if entry.exchange.run_id == run_id)
        counts = f"{plural(seen, 'exchange')} · {plural(writes, 'write')}"
        lines.append(printable(f"  run {run_id[:SHORT_ID]}  {call}  {counts}"))
    first = next(iter(runs), None)
    if first is not None:
        lines.append(printable(f"  replay with: irimi replay {first[:SHORT_ID]}"))
    return lines


def tls_failure_lines(failures: Mapping[str, int], ca_cert: Path | None) -> list[str]:
    """One line per host whose clients refused irimi's certificate. Such a call never became an
    exchange, so nothing else in the summary says it was made."""
    ca = str(ca_cert) if ca_cert is not None else "irimi's CA certificate"
    return [
        printable(
            f"  {plural(count, 'connection')} to {host} failed TLS: the client did not trust "
            f"irimi's CA; point it at {ca} (some clients ignore SSL_CERT_FILE)"
        )
        for host, count in sorted(failures.items())
    ]
