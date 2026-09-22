"""What a run prints: the startup banner, one line per exchange, and the exit summary (#13, #20).

Pure text: every function here returns lines and touches nothing. `cli` prints them.
"""

import re
from collections.abc import Sequence
from pathlib import Path

from irimi import netaddr
from irimi.echo import reflect
from irimi.exchange import TARGET_FAILED_FLAG, Exchange
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
    """A write this run answered instead of performing - locally, or at an answer target."""
    return exchange.kind in INTERCEPTED_KINDS and exchange.answered_by != "live"


def _reached_target(exchange: Exchange) -> bool:
    """A write an answer target actually answered, as opposed to one whose target was never
    reached. `IrimiAddon.error` flags the second `target-failed` and records no response."""
    return exchange.answered_by == "delegated" and TARGET_FAILED_FLAG not in exchange.flags


def _failed_target(exchange: Exchange) -> bool:
    return TARGET_FAILED_FLAG in exchange.flags


def _host_line(host: str, rows: Sequence[Exchange], width: int) -> str:
    """One host's counts, in the order a reader asks them: what was real, then what was not."""
    phrases: list[str] = []
    live_reads = sum(1 for ex in rows if ex.kind == "read" and ex.answered_by == "live")
    if live_reads:
        phrases.append(_plural(live_reads, "read"))
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
    label = "unclassified" if exchange.kind == "unknown" else "unvalidated"
    fidelity = "delegated" if exchange.answered_by == "delegated" else "L0"
    return f"  {WRITE_MARKER} {what}  {label} ({fidelity})"


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
    writes = [ex for ex in exchanges if _is_intercepted(ex)]
    live = sum(1 for ex in exchanges if ex.answered_by == "live")
    delegated = sum(1 for ex in exchanges if ex.answered_by == "delegated")
    virtualized = len(exchanges) - live - delegated
    lines = [
        f"irimi shadow · run {run_id} · {_plural(len(exchanges), 'exchange')} · "
        f"{elapsed_s:.1f}s · backstop: {BACKSTOP}"
    ]
    host_lines = _host_lines(exchanges)
    if host_lines:
        lines += ["", *host_lines]
    write_lines = [_write_line(ex, index) for ex in writes]
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
