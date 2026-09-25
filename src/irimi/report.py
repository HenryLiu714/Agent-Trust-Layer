"""What a run prints: the startup banner, one line per exchange, and the exit summary (#13, #20).

Pure text: every function here returns lines and touches nothing. `cli` prints them.
"""

import re
from collections.abc import Sequence
from pathlib import Path

from irimi import netaddr
from irimi.echo import reflect
from irimi.exchange import (
    IDEMPOTENCY_CONFLICT_FLAG,
    IDEMPOTENT_REPLAY_FLAG,
    TARGET_FAILED_FLAG,
    Exchange,
)
from irimi.servicemap import SELF_TARGET, MapIndex, Route, is_delegated, path_params

NOT_VIRTUALIZED_NOTICE = "hosts not routed through the proxy are NOT virtualized."
BACKSTOP = "none (Phase 4)"
BACKSTOP_NOTICE = f"backstop: {BACKSTOP}"

# A delegated service or route: the banner has to say that a *routed* host may not be live either.
DELEGATED_PREFIX = "delegated:"
NOT_LOOPBACK_NOTICE = "NOT loopback - these requests leave this machine"
NOT_LOOPBACK_INDENT = " " * len(f"{DELEGATED_PREFIX} ")
RED = "\033[31m"
RESET = "\033[0m"


def banner_lines(command: str, run_id: str, host: str, port: int, ca_cert: Path) -> list[str]:
    """The three startup lines. `command` is "shadow" or "serve"."""
    return [
        f"irimi {command} · run {run_id} · listening on {host}:{port} · ca {ca_cert}",
        NOT_VIRTUALIZED_NOTICE,
        BACKSTOP_NOTICE,
    ]


def delegated_lines(index: MapIndex, color: bool = False) -> list[str]:
    """One banner line per delegated service or route: what irimi is not answering, and who is.

    `NOT_VIRTUALIZED_NOTICE` tells the reader that a host which is not routed through the proxy
    is not virtualized. Nothing said that a *routed* host may not be live either: a service or a
    route carrying an answer target is answered by an address the developer named, so the "reads
    are real" claim the banner rests on is false for that service (design D20, §4.4).

    `servicemap.is_delegated` is the shared condition, rather than a fourth spelling of it here:
    this banner, the exit summary and `irimi maps list` are three surfaces that must agree about
    which services are delegated, and they agree by construction only if they ask one function.

    A target that is not loopback got there through `--allow-target-host`, so it is named as
    leaving the machine and painted red. `color` is the caller's answer to "is this a terminal";
    captured output stays plain so a test asserts on words rather than escape codes.
    """
    lines: list[str] = []
    for sm in sorted(index.services, key=lambda s: s.service):
        if not is_delegated(sm):
            continue
        if sm.target != SELF_TARGET:
            scope = "reads + writes" if sm.target_reads else "writes"
            lines.extend(_delegated_line(sm.service, sm.target, scope, color))
        for route in sm.routes:
            if route.target != SELF_TARGET:
                what = f"{sm.service} {route.method} {route.path}"
                scope = "reads" if route.kind == "read" else "writes"
                lines.extend(_delegated_line(what, route.target, scope, color))
    return lines


def _delegated_line(what: str, target: str, scope: str, color: bool) -> list[str]:
    """The line for one delegated service or route, plus the warning line a non-loopback one owes.

    The warning is its own line rather than a tail on the first: a target long enough to be worth
    warning about is long enough that the two together wrap at 100 columns, and a wrapped warning
    is the one a reader skims past.
    """
    line = f"{DELEGATED_PREFIX} {what} → {target} ({scope})"
    if netaddr.is_local_target(target):
        return [line]
    notice = f"{NOT_LOOPBACK_INDENT}{NOT_LOOPBACK_NOTICE}"
    if color:
        return [f"{RED}{line}{RESET}", f"{RED}{notice}{RESET}"]
    return [line, notice]


def exchange_line(exchange: Exchange) -> str:
    """One line per finished exchange."""
    status = exchange.response.status if exchange.response else "-"
    flags = f"  [{', '.join(exchange.flags)}]" if exchange.flags else ""
    # Both columns are nine wide: `telemetry` and `delegated` are nine characters, and at eight
    # every row carrying one was pushed a column right in the most-read output the tool produces.
    return (
        f"{exchange.answered_by:<9} {exchange.kind:<9} {exchange.request.method} "
        f"{exchange.request.host}{exchange.request.path} -> {status}{flags}"
    )


# The kinds a shadow run answers instead of forwarding, so they are the writes the summary owes
# the reader a line each. `unknown` is here because an unclassified DELETE is a presumed write:
# that is why the policy answers it locally, and the summary has to account for it the same way.
INTERCEPTED_KINDS: frozenset[str] = frozenset({"write", "unknown"})

WRITE_MARKER = "○"
REJECTED_MARKER = "✗"
# One of the agent's own reads that irimi either edited to show this run's writes, or knows it
# could not fully show them (#48). It hangs under the write it is about, so a reader sees the
# write and the reads that then saw it as one thing.
OVERLAY_MARKER = "↳"
SAW_IT = "saw it"
SAW_PART = "saw it in part"
DID_NOT_SHOW = "did not show it"
# `fake-L0` and `fake-L1` print as `L0` and `L1`; `delegated` has no prefix and prints as it is.
FAKE_PREFIX = "fake-"
DID_NOT_HAPPEN = "These writes did not happen."
# A delegated write whose target never answered. It is neither delegated nor faked: nothing
# answered it at all, and the agent got a 502. Saying "delegated" for it would be the same
# class of lie as counting a delegated read as live (#20) - the summary would show a write
# safely handled by the stub when the stub was not running.
TARGET_UNREACHABLE = "target unreachable"

# Currencies with no minor unit: ¥4900 is four thousand nine hundred yen, not ¥49.00. Stripe's
# own zero-decimal list, which is the one the amounts in these bodies are denominated in.
ZERO_DECIMAL_CURRENCIES: frozenset[str] = frozenset(
    {
        "bif",
        "clp",
        "djf",
        "gnf",
        "jpy",
        "kmf",
        "krw",
        "mga",
        "pyg",
        "rwf",
        "ugx",
        "vnd",
        "vuv",
        "xaf",
        "xof",
        "xpf",
    }
)
CURRENCY_SYMBOLS: dict[str, str] = {"usd": "$", "eur": "€", "gbp": "£", "jpy": "¥"}

# A `{name}` hole in a map's `human:` template. Deliberately not `str.format`: a template carries
# literal braces (Slack's `#{channel}` sits next to none, but a body field might) and `format`
# raises on an unmatched one, inside the function that prints the run's most important output.
_FIELD = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
MISSING_FIELD = "?"


def _plural(count: int, word: str) -> str:
    return f"{count} {word}" if count == 1 else f"{count} {word}s"


def money(minor_units: int, currency: str) -> str:
    """Minor units as the amount a person reads: `money(4900, "usd")` -> `$49.00`.

    The currency comes from the request, because nothing else in a shadow run knows it: the
    charge it refers to was read live and we do not carry its fields across exchanges (that is
    the overlay's job, Phase 2). A body that names no currency therefore gets no formatting at
    all - see `_field_value`. Dividing by 100 without knowing the currency would print ¥49.00
    for a 4900-yen refund, which is a wrong number rather than an unformatted one.
    """
    code = currency.lower()
    figure = f"{minor_units}" if code in ZERO_DECIMAL_CURRENCIES else f"{minor_units / 100:.2f}"
    symbol = CURRENCY_SYMBOLS.get(code)
    return f"{symbol}{figure}" if symbol else f"{figure} {code.upper()}"


def _is_amount(name: str) -> bool:
    return name == "amount" or name.endswith("_amount")


def _field_value(name: str, value: object, currency: object) -> str:
    """One template field, rendered. Amounts are minor units; everything else is what was sent."""
    if _is_amount(name) and isinstance(value, int) and isinstance(currency, str) and currency:
        return money(value, currency)
    return str(value)


def render_human(template: str, exchange: Exchange, route: Route | None) -> str:
    """A map's `human:` template with this request's own fields in its holes.

    Fields come from two places and the body wins: the route pattern's `{name}` segments bound to
    the path (`/v1/charges/{charge}`), and the request body as the L0 echo reflects it, which is
    the one parser that already knows Stripe's bracketed form encoding. A hole with no field
    renders `?` rather than disappearing: a summary that silently drops what it could not read
    would claim to describe a write it did not fully understand.
    """
    fields: dict[str, object] = {}
    if route is not None:
        fields.update(path_params(route.path, exchange.request.path))
    fields.update(reflect(exchange.request))
    currency = fields.get("currency")

    def one(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in fields:
            return MISSING_FIELD
        return _field_value(name, fields[name], currency)

    return _FIELD.sub(one, template)


def _is_intercepted(exchange: Exchange) -> bool:
    """A write this run answered instead of performing - locally, or at an answer target.

    A REPLAYED write is not one of them (#46). The agent retried with a key it had already used,
    the store answered with the first write's own bytes, and it is the same write: counting it
    again would make "nine would have been rejected" count retries instead of intentions. It is
    still in the trace and still carries `idempotent-replay` on its own line. A CONFLICT stays,
    because it is a different write the real service would have refused - which is exactly the
    line the baseline report exists to print.
    """
    return (
        exchange.kind in INTERCEPTED_KINDS
        and exchange.answered_by != "live"
        and IDEMPOTENT_REPLAY_FLAG not in exchange.flags
    )


def _reached_target(exchange: Exchange) -> bool:
    """A write an answer target actually answered, as opposed to one whose target was never
    reached. `IrimiAddon.error` flags the second `target-failed` and records no response."""
    return exchange.answered_by == "delegated" and TARGET_FAILED_FLAG not in exchange.flags


def _failed_target(exchange: Exchange) -> bool:
    return TARGET_FAILED_FLAG in exchange.flags


def _host_line(host: str, rows: Sequence[Exchange], width: int) -> str:
    """One host's counts, in the order a reader asks them: what was real, then what was not."""
    phrases: list[str] = []
    # An OVERLAID read is a real read (#43): it went to the real service and came back, and irimi
    # then wrote the run's own faked writes into the body. Leaving it out of this count told the
    # reader a read they really made never happened, which is the same class of untruth as #20
    # below. It is counted here and named separately, because the body is not what Stripe sent.
    #
    # An engine-issued read is a real read that the AGENT did not make (#45). Counting it in
    # `N reads` would inflate the one number this tool rests on. #48 owns the final phrasing of
    # this block; this is the honest minimum until then.
    live_reads = sum(
        1
        for ex in rows
        if ex.kind == "read" and ex.answered_by == "live" and ex.issued_by == "agent"
    )
    overlaid_reads = sum(1 for ex in rows if ex.kind == "read" and ex.answered_by == "overlay")
    if live_reads or overlaid_reads:
        phrases.append(_plural(live_reads + overlaid_reads, "read"))
    if overlaid_reads:
        phrases.append(f"{overlaid_reads} showing this run's writes")
    engine_reads = sum(1 for ex in rows if ex.issued_by == "engine")
    if engine_reads:
        phrases.append(_plural(engine_reads, "engine read"))
    # A delegated read is NOT a real read, and `reads are real` is what that count means to
    # whoever reads it. Counting it with the live ones is the honesty bug #20 was filed for.
    target_reads = sum(1 for ex in rows if ex.kind == "read" and ex.answered_by == "delegated")
    if target_reads:
        phrases.append(f"{_plural(target_reads, 'read')} from the target")
    llm = sum(1 for ex in rows if ex.kind == "llm")
    if llm:
        phrases.append(f"{llm} llm")
    writes = [ex for ex in rows if _is_intercepted(ex)]
    if writes:
        detail = []
        unclassified = sum(1 for ex in writes if ex.kind == "unknown")
        delegated = sum(1 for ex in writes if _reached_target(ex))
        unreachable = sum(1 for ex in writes if _failed_target(ex))
        if unclassified:
            detail.append(f"{unclassified} unclassified")
        if delegated:
            detail.append(f"{delegated} delegated")
        if unreachable:
            detail.append(f"{unreachable} {TARGET_UNREACHABLE}")
        suffix = f" ({', '.join(detail)})" if detail else ""
        phrases.append(f"{_plural(len(writes), 'write')} intercepted{suffix}")
    return f"  {host:<{width}}  {'  '.join(phrases)}".rstrip()


def _host_lines(exchanges: Sequence[Exchange]) -> list[str]:
    """One line per host, plus one line for all telemetry hosts together.

    Telemetry gets a single line however many vendors it reached: a run posts to Datadog, Sentry
    and PostHog in the same breath and a line each would bury the two hosts the agent's work is
    actually on. It also says where they went, because they went there for real (#20).
    """
    by_host: dict[str, list[Exchange]] = {}
    telemetry: list[Exchange] = []
    for ex in exchanges:
        if ex.kind == "telemetry":
            telemetry.append(ex)
            continue
        by_host.setdefault(ex.request.host, []).append(ex)
    hosts = sorted(by_host)
    width = max((len(h) for h in hosts), default=0)
    width = max(width, len("telemetry")) if telemetry else width
    lines = [_host_line(host, by_host[host], width) for host in hosts]
    if telemetry:
        vendors = len({ex.request.host for ex in telemetry})
        lines.append(
            f"  {'telemetry':<{width}}  {_plural(len(telemetry), 'exchange')} "
            f"to {_plural(vendors, 'host')}, forwarded live"
        )
    return lines


def _write_line(exchange: Exchange, index: MapIndex | None) -> str:
    """One intercepted write, as the map's own sentence plus what irimi did with it."""
    matched = (
        index.route_for(exchange.request.host, exchange.request.method, exchange.request.path)
        if index is not None
        else None
    )
    route = matched[1] if matched is not None else None
    if route is not None and route.human:
        what = render_human(route.human, exchange, route)
    else:
        what = f"{exchange.request.method} {exchange.request.host}{exchange.request.path}"
    if exchange.answered_by == "delegated":
        what = f"{what} → {exchange.target}"
    if _failed_target(exchange):
        return f"  {WRITE_MARKER} {what}  unanswered ({TARGET_UNREACHABLE})"
    # A write the real service would have refused, and irimi is saying so: L3 checked it against
    # real state (#45), or this run had already used its idempotency key for a different write
    # (#46). It did not merely go unperformed, and this is the line the baseline report exists to
    # print. `✗`, not `○`, and the code is the one the agent's SDK raised on.
    if exchange.precondition == "rejected" or IDEMPOTENCY_CONFLICT_FLAG in exchange.flags:
        code = exchange.rejection_code or "rejected"
        return f"  {REJECTED_MARKER} {what}  would fail: {code}"
    label = "unclassified" if exchange.kind == "unknown" else "unvalidated"
    return f"  {WRITE_MARKER} {what}  {label} ({_fidelity(exchange)})"


def _fidelity(exchange: Exchange) -> str:
    """How faithful this answer was, as the summary says it.

    Derived from `answered_by` and `precondition` together, in one place, so a level added to
    either cannot print as the floor it is not (#20's rule, #45's second input). `L3` is not an
    `answered_by` value and never will be - the header says who answered, this says how much irimi
    knew when it did, and `L2` here means "the overlay's worth of truth, and no more": the write
    was faked, and irimi could not find out whether the service would have taken it.

    Only a locally faked answer gets the precondition spelling. A DELEGATED write reads
    `delegated` whatever L3 said about it, because `L2` is a claim about a fake irimi built and
    irimi built nothing here - the target did (#45).
    """
    level = exchange.answered_by.removeprefix(FAKE_PREFIX)
    if not exchange.answered_by.startswith(FAKE_PREFIX):
        return level
    if exchange.precondition == "passed":
        return "L3 preconditions passed"
    if exchange.precondition == "not_evaluable":
        return "L2"
    return level


def _shows_overlay(exchange: Exchange) -> bool:
    """One of the agent's own reads the summary owes a `↳` line (#48).

    Edited to show the run's writes, or known to be incomplete - not merely `overlay: full`, which
    a read irimi only translated also carries while staying `live` and untouched (#43). Never a
    read irimi issued itself: the line says which of the AGENT's reads saw the write (#45).
    """
    return (
        exchange.kind == "read"
        and exchange.issued_by == "agent"
        and (exchange.answered_by == "overlay" or exchange.overlay == "partial")
    )


def _overlay_lines(reads: Sequence[Exchange]) -> list[str]:
    """The `↳` lines under one write: which of the agent's reads were shown it, and how fully.

    Three cases, because `overlay` and `answered_by` say different things (#43). `answered_by ==
    "overlay"` means irimi edited the body; `overlay == "partial"` means irimi knows the world it
    showed was incomplete, and it can sit on a body irimi never touched - a Slack read whose
    channel the two sides spell differently (#44). A read irimi merely TRANSLATED, live and
    `overlay: full`, is not an overlay hit and gets no line at all.
    """
    lines: list[str] = []
    for ex in reads:
        what = f"{ex.request.method} {ex.request.path}"
        if ex.answered_by == "overlay" and ex.overlay != "partial":
            verb, column = SAW_IT, "overlay"
        elif ex.answered_by == "overlay":
            verb, column = SAW_PART, "overlay (partial)"
        else:
            verb, column = DID_NOT_SHOW, "live (partial)"
        lines.append(f"    {OVERLAY_MARKER} {what} {verb}  {column}")
    return lines


def _writes_with_their_reads(
    exchanges: Sequence[Exchange],
) -> list[tuple[Exchange, list[Exchange]]]:
    """Every printed write, in order, each with the overlaid reads that are about it (#48).

    A read is filed under the most recent printed write of the SAME SERVICE before it - service,
    then position, not simply "the last write". A run that refunds on Stripe and then reads Slack
    history would otherwise file the Slack read under the Stripe refund, and the overlay only ever
    applies a service's own writes to that service's reads.

    A read irimi issued itself is never here: `issued_by == "engine"` is a read the AGENT did not
    make, and the whole point of the line is to say which of the agent's reads saw the write (#45).

    Pairs rather than a dict keyed by write: `Exchange` is a mutable dataclass and so unhashable.
    A qualifying read with no earlier write of its service is left out, and cannot happen - the
    overlay stamps or flags a read only on the strength of a same-service write in the log, and
    every write-log entry is a printed write.
    """
    paired: list[tuple[Exchange, list[Exchange]]] = []
    last: dict[str, int] = {}
    for ex in exchanges:
        if _is_intercepted(ex):
            last[ex.service] = len(paired)
            paired.append((ex, []))
        elif _shows_overlay(ex) and ex.service in last:
            paired[last[ex.service]][1].append(ex)
    return paired


def _closing_lines(writes: Sequence[Exchange]) -> list[str]:
    """What did not happen, and - when something answered in irimi's place - where it went.

    Nothing intercepted, nothing to claim: a run that only read gets no closing line rather than
    a sentence about writes it never saw.

    A write whose target could not be reached gets its own sentence rather than joining the
    delegated ones. `These writes did not reach stripe` stays true either way, but `1 was
    delegated to <target>` would claim a stub answered a write no stub ever saw, and the run
    would read as a working delegation while the developer's stub was not running at all.
    """
    if not writes:
        return []
    delegated = [ex for ex in writes if _reached_target(ex)]
    unreachable = [ex for ex in writes if _failed_target(ex)]
    if not delegated and not unreachable:
        return [f"  {DID_NOT_HAPPEN}"]
    services = ", ".join(sorted({ex.service for ex in writes}))
    lines = [f"  These writes did not reach {services}."]
    if delegated:
        targets = ", ".join(sorted({ex.target for ex in delegated}))
        verb = "was" if len(delegated) == 1 else "were"
        lines.append(f"  {len(delegated)} {verb} delegated to {targets}.")
    if unreachable:
        targets = ", ".join(sorted({ex.target for ex in unreachable}))
        verb = "was" if len(unreachable) == 1 else "were"
        lines.append(
            f"  {len(unreachable)} {verb} not answered at all: {targets} could not be "
            "reached, and the agent got a 502."
        )
    return lines


def summary_lines(
    run_id: str,
    exchanges: Sequence[Exchange],
    elapsed_s: float = 0.0,
    index: MapIndex | None = None,
) -> list[str]:
    """The exit summary (#13): what ran, what was real, what irimi answered, and what that means.

    Built from the exchanges the engine reported, never from the TraceStore: telemetry is
    deliberately never recorded there (`IrimiAddon._finish`), so a summary reading the store
    would lose the telemetry count outright.

    `index` is the loaded maps, used only to find each write's `human:` template. Without it the
    writes still get a line, spelled as the request they were.
    """
    paired = _writes_with_their_reads(exchanges)
    writes = [write for write, _ in paired]
    # An overlaid read was forwarded to the real service and answered by it; irimi edited the body
    # afterwards to show the run's own faked writes. This line's buckets are about WHO answered,
    # so it belongs with `live`: counting it as `virtualized` claimed irimi had answered a read
    # the agent really made (#43). That its body was edited is said in the per-host block above,
    # and which write it saw is said by its `↳` line under that write (#48).
    live = sum(1 for ex in exchanges if ex.answered_by in ("live", "overlay"))
    delegated = sum(1 for ex in exchanges if ex.answered_by == "delegated")
    virtualized = len(exchanges) - live - delegated
    lines = [
        f"irimi shadow · run {run_id} · {_plural(len(exchanges), 'exchange')} · "
        f"{elapsed_s:.1f}s · backstop: {BACKSTOP}"
    ]
    host_lines = _host_lines(exchanges)
    if host_lines:
        lines += ["", *host_lines]
    write_lines: list[str] = []
    for write, reads in paired:
        write_lines.append(_write_line(write, index))
        write_lines += _overlay_lines(reads)
    if write_lines:
        lines += ["", *write_lines]
    # The three buckets #20 asks for, on one line: `live` means forwarded to the real service -
    # reads, inference and telemetry alike - `delegated` means an answer target answered it, and
    # `virtualized` means irimi did. The per-host block above is where the words are, because
    # `live` alone stopped being a synonym for `reads are real` the moment either of the other
    # two existed.
    lines += [
        "",
        f"  {_plural(len(exchanges), 'exchange')} · {live} live · "
        f"{delegated} delegated · {virtualized} virtualized",
    ]
    lines += _closing_lines(writes)
    return lines
