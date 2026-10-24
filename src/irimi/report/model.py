"""What a run did, counted and grouped, before any of it is a line.

`disposition` and `answered` read one exchange's raw fields (`kind`, `issued_by`, `answered_by`,
`flags`, `precondition`) and say what it was for the summary. `build` groups a `RunView`'s events
into a `RunSummary`: a `WriteEntry` per write the summary prints a line for, with the reads under
it, and a `HostCounts` per host. `report.summary` turns that into lines, `compare` lines up its
write entries and `report.runs` counts them, so the three agree about which writes a run made.

A live `irimi shadow` and a stored run are summarised alike: both are a `RunView`, and a live
run keeps of each exchange only what the summary reads (`for_summary`).
"""

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

# The `↳` line one of the agent's reads owes under the write it is about: the overlay edited its
# body to show the write (`saw`), edited it knowing the world it showed was incomplete (`part`),
# or left it untouched knowing the same (`did_not`). A read irimi only translated, live and
# `overlay: full`, owes none.
Shown = Literal["saw", "part", "did_not"]
# Who answered one of the agent's reads: the service (live, overlaid, or a replay's recording of
# it), a target, irimi refusing it, irimi unable to reach its target, or anything else.
ReadBy = Literal["service", "target", "refused", "unreached", "other"]


@dataclass(frozen=True)
class AgentRead:
    """One of the agent's own reads. `failed` is the status a read the service answered failed
    with, or `no answer`; None when it did not fail."""

    by: ReadBy
    overlaid: bool = False
    recorded: bool = False
    failed: str | None = None
    shown: Shown | None = None


@dataclass(frozen=True)
class CheckRead:
    """A read irimi made for its own precondition check. Real, but the agent did not make it."""


@dataclass(frozen=True)
class Llm:
    """An llm call; `recorded` when a replay answered it from the recording."""

    recorded: bool = False


@dataclass(frozen=True)
class Faked:
    """A write irimi answered with a fake of its own. `checked` is what its precondition check
    said, None when nothing was asked; `would_fail` the code the real service would have refused
    it with; `recorded` when a replay answered it from the recording."""

    checked: PreconditionOutcome | None = None
    would_fail: str | None = None
    unclassified: bool = False
    recorded: bool = False


@dataclass(frozen=True)
class Refused:
    """A write irimi answered with its own error: refused, or one it could not decide about.
    `body_cut` when irimi never read its body whole."""

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
    """One of the writes the summary prints a line for (`exchange.is_intercepted`). `authored`
    when it is in the run's write log (`exchange.is_authored_write`), so a later read may have
    been shown it; `asked` when irimi made a check read for it in this run."""

    status: WriteStatus
    authored: bool = False
    asked: bool = False


@dataclass(frozen=True)
class Uncounted:
    """Anything no count takes: a retry the idempotency store answered as the write it repeats, or
    a write forwarded live."""


Disposition = AgentRead | CheckRead | Llm | Write | Uncounted
Answerer = Literal["real", "target", "irimi"]


def disposition(exchange: Exchange) -> Disposition:
    """What one exchange was, for the summary."""
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
    # The precondition check rejected it, or this run had already used its idempotency key for a
    # different write.
    if precondition.outcome == "rejected" or Flag.IDEMPOTENCY_CONFLICT in flags:
        would_fail = precondition.rejection_code or "rejected"
    return Faked(precondition.outcome, would_fail, exchange.kind == "unknown", recorded)


def answered(exchange: Exchange) -> Answerer:
    """Who answered an exchange, as the closing totals count it. An overlaid read is real: the
    service answered it before irimi wrote the run's writes into the body. One a replay answered
    from the recording reached no service, so irimi answered it, and so did a target mitmproxy
    could not finish dialling."""
    if exchange.answered_by in ("live", "overlay") and Flag.FROM_RECORDING not in exchange.flags:
        return "real"
    if exchange.answered_by == "delegated" and not failed_target(exchange):
        return "target"
    return "irimi"


# ---------------------------------------------------------------------------------------- model


@dataclass(frozen=True)
class RunView:
    """A run as the summary reads it: its record and its events, in the order they happened."""

    record: RunRecord
    events: Sequence[Event]


def for_summary(exchange: Exchange) -> Exchange | TelemetrySeen:
    """What the summary reads of a live exchange, and no more: a telemetry exchange as the host it
    went to, as the store keeps one, and any other without its response body or its stream's
    chunks. A write keeps its request body, which its `human:` sentence reads; nothing else
    does."""
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
    """One write's line and what hangs under it. `route` is the route it was classified under,
    for its `human:` template; `check_read` irimi's check read for it, which says why a check
    could not be made; `reads` the agent's reads the overlay showed it to, or knows it could not
    fully show it, each with the `↳` line it owes."""

    exchange: Exchange
    status: WriteStatus
    route: Route | None = None
    check_read: Exchange | None = None
    reads: tuple[tuple[Exchange, Shown], ...] = ()


@dataclass(frozen=True)
class HostCounts:
    """One host's exchanges, counted in the order a reader asks: what was real, then what was not.

    `reads` are the agent's own reads a service answered: an overlaid read is one, since it went
    to the real service and came back before irimi wrote the run's writes into the body, and so is
    a read a replay answered from the recording, since the recorded run made it. `overlaid` and
    `recorded_reads` count those, whose body is not what the service sent now, and `failed_reads`
    holds each failed one's status. A read irimi made for its own precondition check is real, but
    the agent did not make it, so it is in `check_reads` and never in `reads`. A read a target
    answered is not a real read, so it is in `target_reads`.
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
    """A run, summarised. `actions` are its write lines in the order the run made them: a
    `WriteEntry` per write, and each write tool call whose fake ran. `telemetry` is one host per
    telemetry exchange, every one of them forwarded live. `exchanges` counts telemetry too, and
    no tool call."""

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
    """`view` summarised. `index` is the loaded maps, used only to find each write's route;
    without it every write still gets an entry, with no route."""
    # A telemetry exchange counts only by its host, which is all the store keeps of one.
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
    # A read is filed under the most recent write before it of its own service that is in the
    # write log (`is_authored_write`): the overlay applies only those to a read, so a refused write
    # never has a read under it and a Slack read never sits under a Stripe refund.
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
    """The route a write was classified under, for its `human:` template.

    Its path is matched, as the live classification matched it. A stored write on a
    credential-path host has a redaction placeholder for a path, which matches no route, so the
    route is then found by the `operation` it recorded: a webhook post prints `post via
    webhook: "..."` from the store as it did live."""
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
    """irimi's check read for each write it made one for in this run (`Write.asked`), keyed by
    the write's place among the events. A write whose read the run did not record has no entry.

    The engine records a check read before the write it was made for, both in the write's run,
    and makes it while deciding the write. So each such write takes an unclaimed read of its
    service made within its span, else the latest before it. Two writes decided at once both
    span both reads: each prefers the read that names an object its own request names, then the
    earliest, which the write that started first made first."""
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
    """What a write's request names that looks like an id: each string value of its body and each
    path segment, of three characters or more, holding a digit."""
    values = [v for v in reflect(write.request).values() if isinstance(v, str)]
    values += write.request.path.split("/")
    return {v for v in values if len(v) >= 3 and any(char.isdigit() for char in v)}


def _names_any(read: Exchange, named: set[str]) -> bool:
    """Whether a check read's path or query holds one of `named` whole."""
    words = set(re.split(r"[/?&=]", f"{read.request.path}?{read.request.query}"))
    return not words.isdisjoint(named)


def auth_failed(check_read: Exchange | None) -> bool:
    """Whether the service refused irimi's check read for the agent's own credentials."""
    return (
        check_read is not None
        and check_read.response is not None
        and check_read.response.status in AUTH_FAILURES
    )


def _would_fire(writes: Sequence[WriteEntry]) -> tuple[str, ...]:
    """Every webhook event the run's faked writes would have caused, each named once, in the order
    the run first listed it.

    Off `Exchange.would_fire` and never off `route.fires`: `would_fire` is already empty for
    an L3 rejection, an idempotency conflict, a replay and a delegated write. A write whose check
    read the service refused for its credentials is left out too: the real write would most
    likely have been refused the same way. De-duplicated because two refunds carry two identical
    tuples and one refund's `refund.created` must not be promised twice.
    """
    fired = (entry.exchange for entry in writes if not auth_failed(entry.check_read))
    return tuple(dict.fromkeys(event for ex in fired for event in ex.would_fire))


def run_elapsed(record: RunRecord) -> float | None:
    """How long a stored run took, or None for one with no start or no end. Never negative: the
    wall clock can step back between the two, as `Exchange` knows of its own."""
    if record.started_at is None or record.end is None:
        return None
    return max(0.0, record.end.ended_at - record.started_at)
