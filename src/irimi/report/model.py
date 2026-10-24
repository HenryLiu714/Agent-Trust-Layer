"""Classify a run's exchanges and group them into a `RunSummary`, before any of it is a line."""

import re
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Literal

from irimi.core.bodies import reflect
from irimi.core.exchange import (
    Exchange,
    Flag,
    PreconditionOutcome,
    failed_target,
    fake_ran,
    is_authored_write,
    is_intercepted,
    refused_by_irimi,
)
from irimi.protocol.trace import Event, RunRecord, TelemetrySeen, ToolCall
from irimi.servicemaps import MapIndex, Route

# ------------------------------------------------------------------------------------- classify

# The `↳` line an agent read gets under its write:
#   saw      the overlay edited the body to show the write
#   part     the overlay edited it, but knows what it shows is incomplete
#   did_not  untouched, and the overlay knows it is incomplete
Shown = Literal["saw", "part", "did_not"]
# Who answered an agent read: the service (live, overlaid or recorded), a target, irimi refusing
# it, irimi unable to reach its target, or anything else.
ReadBy = Literal["service", "target", "refused", "unreached", "other"]


@dataclass(frozen=True)
class AgentRead:
    """One of the agent's own reads. `failed` is its failing status or `no answer`, else None."""

    by: ReadBy
    overlaid: bool = False
    recorded: bool = False
    failed: str | None = None
    shown: Shown | None = None


@dataclass(frozen=True)
class CheckRead:
    """irimi's check read: real, but not made by the agent."""


@dataclass(frozen=True)
class Llm:
    """An llm call; `recorded` when a replay answered it from the recording."""

    recorded: bool = False


@dataclass(frozen=True)
class Faked:
    """A faked write.

    `checked` is the precondition check's outcome, or None; `would_fail` the code the real service
    would have refused it with; `recorded` when a replay answered it from the recording."""

    checked: PreconditionOutcome | None = None
    would_fail: str | None = None
    unclassified: bool = False
    recorded: bool = False


@dataclass(frozen=True)
class Refused:
    """A write irimi answered with its own error. `body_cut` when its body was never read whole."""

    decision_failed: bool = False
    body_cut: bool = False


@dataclass(frozen=True)
class SentToTarget:
    target: str


@dataclass(frozen=True)
class TargetUnreachable:
    target: str


WriteStatus = Faked | Refused | SentToTarget | TargetUnreachable


@dataclass(frozen=True)
class Write:
    """A write the summary prints a line for (`is_intercepted`).

    `authored` when it is in the write log (`is_authored_write`); `asked` when irimi made a check
    read for it in this run."""

    status: WriteStatus
    authored: bool = False
    asked: bool = False


@dataclass(frozen=True)
class Uncounted:
    """An exchange no count takes: an idempotent retry, or a write forwarded live."""


Disposition = AgentRead | CheckRead | Llm | Write | Uncounted
Answerer = Literal["real", "target", "irimi"]


def disposition(exchange: Exchange) -> Disposition:
    """Return what one exchange was, for the summary."""
    flags = exchange.flags
    recorded = Flag.FROM_RECORDING in flags
    if exchange.issued_by == "engine":
        return CheckRead()
    if exchange.kind == "read":
        return _agent_read(exchange, recorded)
    if exchange.kind == "llm":
        return Llm(recorded)
    if not is_intercepted(exchange):
        return Uncounted()
    asked = (
        exchange.answered_by.startswith("fake-")
        and exchange.precondition.outcome is not None
        and not recorded
    )
    return Write(_write_status(exchange, recorded), is_authored_write(exchange), asked)


def _agent_read(exchange: Exchange, recorded: bool) -> AgentRead:
    answered_by = exchange.answered_by
    shown: Shown | None = None
    if answered_by == "overlay":
        shown = "part" if exchange.overlay == "partial" else "saw"
    elif exchange.overlay == "partial":
        shown = "did_not"
    if answered_by in ("live", "overlay", "recording"):
        response = exchange.response
        failed = None
        if response is None:
            failed = "no answer"
        elif response.status >= 400:
            failed = str(response.status)
        return AgentRead("service", answered_by == "overlay", recorded, failed, shown)
    if answered_by == "delegated":
        return AgentRead("target", shown=shown)
    if answered_by == "irimi":
        return AgentRead("unreached" if failed_target(exchange) else "refused", shown=shown)
    return AgentRead("other", shown=shown)


def _write_status(exchange: Exchange, recorded: bool) -> WriteStatus:
    flags = exchange.flags
    if refused_by_irimi(exchange):
        return Refused(Flag.DECISION_FAILED in flags, Flag.BODY_TRUNCATED in flags)
    if failed_target(exchange):
        return TargetUnreachable(exchange.target)
    if exchange.answered_by == "delegated":
        return SentToTarget(exchange.target)
    precondition = exchange.precondition
    would_fail = None
    # An idempotency conflict means this run used the key for a different write.
    if precondition.outcome == "rejected" or Flag.IDEMPOTENCY_CONFLICT in flags:
        would_fail = precondition.rejection_code or "rejected"
    return Faked(precondition.outcome, would_fail, exchange.kind == "unknown", recorded)


def answered(exchange: Exchange) -> Answerer:
    """Return who answered an exchange, for the closing totals.

    An overlaid read is real: the service answered it first. A recorded answer or an unreachable
    target counts as irimi."""
    if exchange.answered_by in ("live", "overlay") and Flag.FROM_RECORDING not in exchange.flags:
        return "real"
    if exchange.answered_by == "delegated" and not failed_target(exchange):
        return "target"
    return "irimi"


# ---------------------------------------------------------------------------------------- model


@dataclass(frozen=True)
class RunView:
    """A run as the summary reads it: its record and its events, in order."""

    record: RunRecord
    events: Sequence[Event]


def for_summary(exchange: Exchange) -> Exchange | TelemetrySeen:
    """Return only what the summary reads of a live exchange.

    Telemetry becomes its host; others drop response body and chunks. Only a write keeps its
    request body, for its `human:` sentence."""
    if exchange.kind == "telemetry":
        return TelemetrySeen(exchange.run_id, exchange.request.host, exchange.started_at)
    request = exchange.request
    if not isinstance(disposition(exchange), Write):
        request = replace(request, body=b"")
    response = exchange.response
    if response is not None:
        response = replace(response, body=b"")
    return replace(exchange, request=request, response=response, stream_chunks=())


@dataclass(frozen=True)
class WriteEntry:
    """One write's line and what hangs under it.

    `route` supplies its `human:` template; `check_read` is irimi's check read for it; `reads` are
    the agent reads that show the write (or might not fully), each with its `↳` line."""

    exchange: Exchange
    status: WriteStatus
    route: Route | None = None
    check_read: Exchange | None = None
    reads: tuple[tuple[Exchange, Shown], ...] = ()


@dataclass(frozen=True)
class HostCounts:
    """One host's exchanges, counted: what was real, then what was not.

    `reads` are agent reads a service answered, overlaid and recorded ones included (also counted
    in `overlaid` and `recorded_reads`). irimi's check reads count only in `check_reads`, and
    reads a target answered only in `target_reads`.
    """

    host: str
    reads: int = 0
    overlaid: int = 0
    recorded_reads: int = 0
    failed_reads: tuple[str, ...] = ()
    check_reads: int = 0
    target_reads: int = 0
    refused_reads: int = 0
    unreached_reads: int = 0
    llm: int = 0
    recorded_llm: int = 0
    faked_writes: int = 0
    unclassified_writes: int = 0
    refused_writes: int = 0
    target_writes: int = 0
    unreached_writes: int = 0


@dataclass(frozen=True)
class RunSummary:
    """A run, summarised.

    `actions` are its write lines in run order: a `WriteEntry` per write, and each write tool call
    whose fake ran. `telemetry` is one host per (live) telemetry exchange. `exchanges` counts
    telemetry but no tool calls."""

    record: RunRecord
    actions: tuple[WriteEntry | ToolCall, ...] = ()
    hosts: tuple[HostCounts, ...] = ()
    telemetry: tuple[str, ...] = ()
    exchanges: int = 0
    real: int = 0
    targets: int = 0
    would_fire: tuple[str, ...] = ()

    @property
    def writes(self) -> tuple[WriteEntry, ...]:
        return tuple(action for action in self.actions if isinstance(action, WriteEntry))

    @property
    def tool_writes(self) -> tuple[ToolCall, ...]:
        return tuple(action for action in self.actions if isinstance(action, ToolCall))

    @property
    def faked(self) -> int:
        return sum(1 for entry in self.writes if isinstance(entry.status, Faked))

    @property
    def by_irimi(self) -> int:
        return self.exchanges - self.real - self.targets


# A check read the service refused for the agent's own credentials: the write would most likely
# have been refused too, so it is left out of the webhooks that would have fired.
AUTH_FAILURES: frozenset[int] = frozenset({401, 403})


def build(view: RunView, index: MapIndex | None) -> RunSummary:
    """Return `view` summarised. `index` only finds each write's route; None leaves routes unset."""
    # The store keeps only a telemetry exchange's host.
    telemetry: list[str] = []
    exchanges: list[tuple[int, Exchange]] = []
    for at, event in enumerate(view.events):
        if isinstance(event, TelemetrySeen):
            telemetry.append(event.host)
        elif isinstance(event, Exchange) and event.kind == "telemetry":
            telemetry.append(event.request.host)
        elif isinstance(event, Exchange):
            exchanges.append((at, event))
    seen = {at: disposition(ex) for at, ex in exchanges}
    checks = _check_reads(exchanges, seen)
    under: dict[int, list[tuple[Exchange, Shown]]] = {}
    # File a read under its service's latest earlier write in the write log: the overlay applies
    # only those, so a refused write gets no reads and a Slack read never sits under a refund.
    last: dict[str, int] = {}
    for at, ex in exchanges:
        what = seen[at]
        if isinstance(what, Write):
            under[at] = []
            if what.authored:
                last[ex.service] = at
        elif isinstance(what, AgentRead) and what.shown is not None and ex.service in last:
            under[last[ex.service]].append((ex, what.shown))
    actions: list[WriteEntry | ToolCall] = []
    for at, event in enumerate(view.events):
        write = seen.get(at)
        if isinstance(event, Exchange) and isinstance(write, Write):
            route = route_of(event, index)
            reads = tuple(under[at])
            actions.append(WriteEntry(event, write.status, route, checks.get(at), reads))
        elif isinstance(event, ToolCall) and fake_ran(event):
            actions.append(event)
    by_host: dict[str, list[int]] = {}
    for at, ex in exchanges:
        by_host.setdefault(ex.request.host, []).append(at)
    hosts = sorted(by_host)
    answerers = [answered(ex) for _, ex in exchanges]
    writes = [action for action in actions if isinstance(action, WriteEntry)]
    return RunSummary(
        record=view.record,
        actions=tuple(actions),
        hosts=tuple(_host_counts(host, [seen[at] for at in by_host[host]]) for host in hosts),
        telemetry=tuple(telemetry),
        exchanges=len(exchanges) + len(telemetry),
        real=len(telemetry) + answerers.count("real"),
        targets=answerers.count("target"),
        would_fire=_would_fire(writes),
    )


def _host_counts(host: str, rows: Sequence[Disposition]) -> HostCounts:
    counts: dict[str, int] = {}
    failed: list[str] = []

    def add(name: str) -> None:
        counts[name] = counts.get(name, 0) + 1

    for what in rows:
        match what:
            case CheckRead():
                add("check_reads")
            case AgentRead(by="service"):
                add("reads")
                if what.overlaid:
                    add("overlaid")
                if what.recorded:
                    add("recorded_reads")
                if what.failed is not None:
                    failed.append(what.failed)
            case AgentRead(by="target"):
                add("target_reads")
            case AgentRead(by="refused"):
                add("refused_reads")
            case AgentRead(by="unreached"):
                add("unreached_reads")
            case Llm():
                add("llm")
                if what.recorded:
                    add("recorded_llm")
            case Write(status=Faked() as faked):
                add("faked_writes")
                if faked.unclassified:
                    add("unclassified_writes")
            case Write(status=Refused()):
                add("refused_writes")
            case Write(status=SentToTarget()):
                add("target_writes")
            case Write(status=TargetUnreachable()):
                add("unreached_writes")
    return HostCounts(host, failed_reads=tuple(failed), **counts)


def route_of(exchange: Exchange, index: MapIndex | None) -> Route | None:
    """Return the route a write was classified under, for its `human:` template.

    A stored write whose path was redacted (a webhook) matches no route, so it falls back to the
    recorded `operation`."""
    if index is None:
        return None
    request = exchange.request
    matched = index.route_for(request.host, request.method, request.path)
    if matched is not None:
        return matched[1]
    return index.route_named(request.host, request.method, exchange.operation)


def _check_reads(
    exchanges: Sequence[tuple[int, Exchange]], seen: dict[int, Disposition]
) -> dict[int, Exchange]:
    """Return irimi's check read per `Write.asked` write, keyed by the write's event index.

    A check read is recorded before its write, so each write takes an unclaimed check read of its
    service made during the write, else the latest before it. Writes decided at once prefer the
    read naming their own object, then the earliest."""
    found: dict[int, Exchange] = {}
    unclaimed: list[Exchange] = []
    for at, ex in exchanges:
        what = seen[at]
        if isinstance(what, CheckRead):
            unclaimed.append(ex)
            continue
        if not (isinstance(what, Write) and what.asked):
            continue
        same = [c for c in unclaimed if c.service == ex.service and c.run_id == ex.run_id]
        during = [c for c in same if ex.started_at <= c.started_at <= ex.ended_at]
        named = _named_in(ex)
        during.sort(key=lambda c: (not _names_any(c, named), c.started_at))
        picked = during[:1] or same[-1:]
        if picked:
            found[at] = picked[0]
            unclaimed = [c for c in unclaimed if c is not picked[0]]
    return found


def _named_in(write: Exchange) -> set[str]:
    """Return the id-like strings a write names: body values and path segments of 3+ with digits."""
    values = [v for v in reflect(write.request).values() if isinstance(v, str)]
    values += write.request.path.split("/")
    return {v for v in values if len(v) >= 3 and any(char.isdigit() for char in v)}


def _names_any(read: Exchange, named: set[str]) -> bool:
    """Whether a check read's path or query holds one of `named` whole."""
    words = set(re.split(r"[/?&=]", f"{read.request.path}?{read.request.query}"))
    return not words.isdisjoint(named)


def auth_failed(check_read: Exchange | None) -> bool:
    """Return True if the service refused irimi's check read for the agent's credentials."""
    return (
        check_read is not None
        and check_read.response is not None
        and check_read.response.status in AUTH_FAILURES
    )


def _would_fire(writes: Sequence[WriteEntry]) -> tuple[str, ...]:
    """Return each webhook event the faked writes would have caused, once, in first-seen order."""
    fired = (entry.exchange for entry in writes if not auth_failed(entry.check_read))
    return tuple(dict.fromkeys(event for ex in fired for event in ex.would_fire))


def run_elapsed(record: RunRecord) -> float | None:
    """Return how long a stored run took (never negative), or None without a start and end."""
    if record.started_at is None or record.end is None:
        return None
    return max(0.0, record.end.ended_at - record.started_at)
