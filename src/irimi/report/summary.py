"""Format a `model.RunSummary` as the exit summary `irimi shadow`, serve and `runs show` print."""

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
TOOL_SERVICE = "tool"
# Marks an agent read under the write it shows, or might not fully show.
OVERLAY_MARKER = "↳"
SAW_IT = "saw it"
SAW_PART = "saw it in part"
DID_NOT_SHOW = "did not show it"
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
# `irimi replay` and `runs show` accept a unique prefix of a run id.
SHORT_ID = 8
UNREACHED = "that could not reach the target"
# Live runs only: a stored run's traffic may sit in its SDK runs.
NO_TRAFFIC = (
    "irimi saw no HTTP(S) traffic; if the agent made calls, they bypassed the proxy or failed TLS"
)

# A `{name}` or `{name:money}` (minor units) hole in a `human:` template. Not `str.format`, which
# raises on an unmatched literal brace.
_FIELD = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)(?::(money))?\}")
MISSING_FIELD = "?"


@dataclass(frozen=True)
class Live:
    """Facts only a live `irimi shadow` knows: SDK runs by id, TLS refusals by host, the CA path."""

    sdk_runs: Mapping[str, RunRecord | None] = field(default_factory=dict)
    tls_failures: Mapping[str, int] = field(default_factory=dict)
    ca_cert: Path | None = None


def summary_lines(
    view: RunView,
    index: MapIndex | None,
    elapsed_s: float | None = None,
    live: Live | None = None,
) -> list[str]:
    """Return the exit summary: what the agent tried to write, what was real, what irimi answered.

    `index` only supplies `human:` templates; without it writes are spelled as requests.
    `elapsed_s` defaults to the stored run's own duration, or 0.0. `live` adds the live-only lines
    (SDK runs, no traffic, TLS failures). A stored run's lines may differ where redaction or the
    store's body cap changed what a line reads, or events were dropped.
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
    """Return the block every summary prints: writes, money and webhooks, host counts, totals.

    Tool calls are in no count; each write tool whose fake ran gets a line after the HTTP writes."""
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
    # Sort by end time, since posts from two threads can arrive out of order. Later keys break
    # ties, since a hand-posted call can repeat another's id and times.
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
    """Return the escaped summary line for a write tool whose fake ran."""
    return f"  {printable(tool_write_sentence(call))}"


def tool_write_sentence(call: ToolCall) -> str:
    """Return `tool_write_line` unindented and unescaped, as `irimi compare` uses it."""
    return f"{WRITE_MARKER} {TOOL_SERVICE}  {call.name}  fake {tool_outcome(call, 'ran instead')}"


def _write_line(entry: WriteEntry) -> str:
    """Return one write's escaped summary line."""
    return f"  {printable(write_sentence(entry))}"


def write_sentence(entry: WriteEntry) -> str:
    """Return a write's marker, service, sentence and status, unindented and unescaped.

    A write refused with a cut body is spelled as its request."""
    exchange, status = entry.exchange, entry.status
    # A cut body would render every `human:` field as `?`.
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
            # The real service would have refused it; the code is the one the agent's SDK saw.
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
    """Return a target's host and port, as a write line names it."""
    return urlsplit(target).netloc or target


def _why_not_checked(exchange: Exchange, status: Faked, check_read: Exchange | None) -> str:
    """Return why irimi could not check a write against live state, from its check read."""
    if check_read is None and status.recorded:
        return "the recorded run could not check it"
    if check_read is None:
        return "irimi made no check read"
    if check_read.response is None:
        # A network failure, a body too large to read, or a replay whose recording holds none.
        return f"irimi's check read of {exchange.service} got no usable answer"
    if check_read.response.status != 200:
        return f"{exchange.service} answered irimi's check read with {check_read.response.status}"
    return "irimi could not tell from its check read"


def _overlay_lines(reads: Sequence[tuple[Exchange, Shown]]) -> list[str]:
    """Return the `↳` lines under one write: which agent reads showed it, and how fully."""
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
    """Return `value` if it can be an amount in minor units, else None."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _field_value(value: object, as_money: bool, currency: object) -> str:
    """Return one template field; `:money` formats in `currency`, or bare if it is unknown."""
    amount = _minor_units(value) if as_money else None
    if amount is None:
        return str(value)
    if isinstance(currency, str) and currency:
        return format_minor_units(amount, currency)
    return f"{amount} ({UNKNOWN_CURRENCY})"


def _fields(exchange: Exchange, route: Route | None) -> tuple[dict[str, object], object]:
    """Return a write's template fields and its currency, as `render_human` reads them."""
    fields: dict[str, object] = {}
    if route is not None:
        fields.update(path_params(route.path, exchange.request.path))
    fields.update(reflect(exchange.request))
    # An empty `currency=` falls back to the check read's currency, as a missing one does.
    return fields, fields.get("currency") or exchange.precondition.currency


def render_human(template: str, exchange: Exchange, route: Route | None) -> str:
    """Return a map's `human:` template filled with this request's fields.

    Fields come from the route's path params and the body (`bodies.reflect`); the body wins. A hole
    with no field renders `?`. A currency missing from the request comes from the check read.
    """
    fields, currency = _fields(exchange, route)

    def one(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in fields:
            return MISSING_FIELD
        return _field_value(fields[name], match.group(2) is not None, currency)

    return _FIELD.sub(one, template)


def _amounts(exchange: Exchange, route: Route | None) -> list[tuple[int, str]]:
    """Return each `:money` amount a write's sentence shows, with its lower-case currency or ""."""
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
    """Return the faked writes' amounts summed per currency, and how much would fail.

    Amounts in an unknown currency are counted apart; no line if no currency is known."""
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
    """Return one host's counts: what was real, then what was not."""
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
    """Return a line per host, plus one line for all telemetry hosts together.

    One telemetry line keeps several vendors from burying the agent's hosts.
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
    """Return a line per SDK run in the process, then how to replay one by its own id."""
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
    """Return a line per host whose clients refused irimi's certificate.

    Such calls never became exchanges, so nothing else in the summary shows them."""
    ca = str(ca_cert) if ca_cert is not None else "irimi's CA certificate"
    return [
        printable(
            f"  {plural(count, 'connection')} to {host} failed TLS: the client did not trust "
            f"irimi's CA; point it at {ca} (some clients ignore SSL_CERT_FILE)"
        )
        for host, count in sorted(failures.items())
    ]
