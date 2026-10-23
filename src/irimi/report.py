"""What a run prints: the startup banner, one line per exchange, and the exit summary.

Pure text: every function here returns lines and touches nothing. `cli` prints them.
"""

import json
import os
import re
import shlex
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import assert_never
from urllib.parse import urlsplit

from irimi import netaddr
from irimi.bodies import reflect
from irimi.exchange import (
    BODY_TRUNCATED_FLAG,
    DECISION_FAILED_FLAG,
    FIDELITY_FLAGS,
    FROM_RECORDING_FLAG,
    IDEMPOTENCY_CONFLICT_FLAG,
    IDEMPOTENT_REPLAY_FLAG,
    REFUSED_FLAG,
    TARGET_FAILED_FLAG,
    AnsweredBy,
    Exchange,
    is_authored_write,
)
from irimi.servicemap import SELF_TARGET, MapIndex, Route, is_delegated, path_params
from irimi.store import StoredRun
from irimi.trace import Event, Ran, RunRecord, TelemetrySeen, ToolCall

PROXY_ONLY_NOTICE = (
    "only traffic sent through this proxy is shadowed; a client that ignores HTTPS_PROXY talks to "
    "the real service"
)
# Where the banner names the run, under serve mode, which has no run of its own: each run is the
# SDK's, named on its requests, and a request no run names is `unattributed`.
SERVE_RUNS = "run: per request (SDK), unattributed otherwise"
# The line a listener bound wider than loopback adds to the banner.
WIDE_BIND_NOTICE = (
    "any machine that can reach this port can send traffic through irimi and call its control "
    "endpoint"
)

# A delegated service or route: the banner has to say that a *routed* host may not be live either.
DELEGATED_PREFIX = "delegated:"
NOT_LOOPBACK_NOTICE = "NOT loopback - these requests leave this machine"
NOT_LOOPBACK_INDENT = " " * len(f"{DELEGATED_PREFIX} ")
RED = "\033[31m"
RESET = "\033[0m"


def banner_lines(
    command: str,
    run_id: str | None,
    host: str,
    port: int,
    ca_cert: Path,
    replay_of: str | None = None,
) -> list[str]:
    """The startup lines. `command` is "shadow", "replay" or "shadow --serve"; `run_id` is None in
    serve mode, which names its runs per request. A replay says which run it replays as which.

    A listener that is not loopback adds a line saying so: every machine that reaches the port can
    send its traffic through irimi and post runs to the control endpoint, which is not a boundary.
    `0.0.0.0` and `::` are not loopback. The reverse door stays loopback-only whatever the
    bind."""
    run = SERVE_RUNS if run_id is None else f"run {run_id}"
    lines = [
        f"irimi {command} · {run} · listening on {netaddr.authority(host, port)} · ca {ca_cert}",
        PROXY_ONLY_NOTICE,
    ]
    if replay_of is not None:
        lines.insert(1, f"replaying {replay_of} as {run_id}")
    if not netaddr.is_loopback(host):
        lines.append(f"listening on {host}: {WIDE_BIND_NOTICE}")
    return lines


def delegated_lines(index: MapIndex, color: bool = False) -> list[str]:
    """One banner line per delegated service or route: what irimi is not answering, and who is.

    `PROXY_ONLY_NOTICE` tells the reader that traffic which is not routed through the proxy is
    not shadowed. These lines say that a *routed* host may not be live either: a service or a
    route carrying an answer target is answered by an address the developer named, so the "reads
    are real" claim the banner rests on is false for that service.

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
    line = f"{DELEGATED_PREFIX} {what} -> {target} ({scope})"
    if netaddr.is_local_target(target):
        return [line]
    notice = f"{NOT_LOOPBACK_INDENT}{NOT_LOOPBACK_NOTICE}"
    if color:
        return [f"{RED}{line}{RESET}", f"{RED}{notice}{RESET}"]
    return [line, notice]


# The stream's first column: who answered, in the words the summary uses. A read irimi made for
# its own precondition check is marked apart, whoever answered it, so it never reads as the agent's.
ANSWER_WORDS: dict[AnsweredBy, str] = {
    "live": "real",
    "overlay": "real",
    "fake-L0": "faked",
    "fake-L1": "faked",
    "delegated": "target",
    "recording": "recorded",
    "irimi": "irimi",
}
CHECK_READ = "irimi-check"
TOOL_WORDS: dict[Ran, str] = {
    "real": "tool-real",
    "shadow": "tool-fake",
    "recording": "tool-recorded",
}
# Wide enough for the longest first-column word, so every stream line keeps its columns.
FIRST_COLUMN = max(len(word) for word in (*ANSWER_WORDS.values(), CHECK_READ, *TOOL_WORDS.values()))
# The flag a read the overlay edited shows in the stream, where its first column says `real`.
OVERLAY_SHOWN = "overlay"


def answer_word(exchange: Exchange) -> str:
    """The stream's first column for one exchange. An answer a replay served from the recording
    is `recorded`, an overlaid one included: no service answered it in this run."""
    if exchange.issued_by == "engine":
        return CHECK_READ
    if exchange.answered_by == "overlay" and FROM_RECORDING_FLAG in exchange.flags:
        return ANSWER_WORDS["recording"]
    return ANSWER_WORDS[exchange.answered_by]


def _shown_flags(exchange: Exchange) -> list[str]:
    """The flags the stream prints: the `fidelity:*` ones say again what the first column says."""
    shown = [flag for flag in exchange.flags if flag not in FIDELITY_FLAGS.values()]
    if exchange.answered_by == "overlay":
        shown.insert(0, OVERLAY_SHOWN)
    return shown


def exchange_line(exchange: Exchange) -> str:
    """One line per finished exchange. The host, path and flags are the agent's own text, so the
    line is escaped as `runs show` escapes a run's text."""
    status = exchange.response.status if exchange.response else "-"
    flags = _shown_flags(exchange)
    tail = f"  [{', '.join(flags)}]" if flags else ""
    # The kind column is nine wide: `telemetry` is nine characters.
    return printable(
        f"{answer_word(exchange):<{FIRST_COLUMN}} {exchange.kind:<9} {exchange.request.method} "
        f"{exchange.request.host}{exchange.request.path} -> {status}{tail}"
    )


def tool_call_line(call: ToolCall) -> str:
    """One line per tool call the SDK reported, in `exchange_line`'s columns. Its name
    and error type are whatever was posted to the control endpoint, so each control character is
    escaped, as `runs show` escapes a run's text: one call is one line."""
    outcome = _tool_outcome(call, "ok")
    word = TOOL_WORDS[call.ran]
    return printable(f"{word:<{FIRST_COLUMN}} {call.kind:<9} {call.name} -> {outcome}")


def is_stood_in(call: ToolCall) -> bool:
    """A write tool whose fake ran in its place, which the summary owes a line. Only a fake's
    call: the SDK runs a write tool's real function only while it is inactive, and then records
    nothing, so no write tool call it posts ran for real."""
    return call.kind == "write" and call.ran == "shadow"


def tool_write_line(call: ToolCall) -> str:
    """The summary's line for a write tool whose fake ran in its place, escaped as
    `tool_call_line` is."""
    return f"  {printable(tool_write_sentence(call))}"


def tool_write_sentence(call: ToolCall) -> str:
    """`tool_write_line` without the summary's indent and before it is escaped, which `irimi
    compare` prints for it: the real function did not run, so it is a write that did not happen,
    and it takes the write marker."""
    return f"{WRITE_MARKER} {TOOL_SERVICE}  {call.name}  fake {_tool_outcome(call, 'ran instead')}"


def _tool_outcome(call: ToolCall, returned: str) -> str:
    """`returned`, or `raised <type>`, before either line escapes it."""
    return returned if call.error is None else f"raised {call.error.type}"


# The kinds a shadow run answers instead of forwarding, so they are the writes the summary owes
# the reader a line each. `unknown` is here because an unclassified DELETE is a presumed write:
# that is why the policy answers it locally, and the summary has to account for it the same way.
INTERCEPTED_KINDS: frozenset[str] = frozenset({"write", "unknown"})

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
# A check read the service refused for the agent's own credentials: the write would most likely
# have been refused too, so it is left out of the webhooks that would have fired.
AUTH_FAILURES: frozenset[int] = frozenset({401, 403})
UNKNOWN_CURRENCY = "minor units, currency unknown"
# How much of a run id the summary prints: `irimi replay` and `runs show` take a unique prefix.
SHORT_ID = 8

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

    The currency comes from the request first, and from `Exchange.currency` second - the code the
    write's L3 precondition read found on the object it named, put there by the policy and never
    by a default. Those are the only two sources: a write with no such read, or one whose
    read named no currency, prints its amount as minor units of an unknown currency (see
    `_field_value`). Dividing by 100 without knowing the currency would print ¥49.00 for a
    4900-yen refund, which is a wrong number rather than an unformatted one, and that is why the
    fallback is a document irimi really read and not the first currency it can find.
    """
    code = currency.lower()
    figure = f"{minor_units}" if code in ZERO_DECIMAL_CURRENCIES else f"{minor_units / 100:.2f}"
    symbol = CURRENCY_SYMBOLS.get(code)
    return f"{symbol}{figure}" if symbol else f"{figure} {code.upper()}"


def _is_amount(name: str) -> bool:
    return name == "amount" or name.endswith("_amount")


def _minor_units(name: str, value: object) -> int | None:
    """`value` when the field `name` holds an amount in minor units, else None."""
    if _is_amount(name) and isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _field_value(name: str, value: object, currency: object) -> str:
    """One template field, rendered. Amounts are minor units; everything else is what was sent."""
    amount = _minor_units(name, value)
    if amount is None:
        return str(value)
    if isinstance(currency, str) and currency:
        return money(amount, currency)
    return f"{amount} ({UNKNOWN_CURRENCY})"


def _fields(exchange: Exchange, route: Route | None) -> tuple[dict[str, object], object]:
    """A write's template fields and its currency, as `render_human` reads them."""
    fields: dict[str, object] = {}
    if route is not None:
        fields.update(path_params(route.path, exchange.request.path))
    fields.update(reflect(exchange.request))
    # `or` and not `setdefault`: a body that sent `currency=` sent nothing, and falls back to the
    # L3 read's currency like a body that sent no key at all.
    return fields, fields.get("currency") or exchange.currency


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
        return _field_value(name, fields[name], currency)

    return _FIELD.sub(one, template)


def _amounts(exchange: Exchange, route: Route | None) -> list[tuple[int, str]]:
    """Each amount a write's `human:` sentence shows, with its lower-case currency, or "" for an
    amount whose currency irimi does not know."""
    if route is None or not route.human:
        return []
    fields, currency = _fields(exchange, route)
    code = currency.lower() if isinstance(currency, str) else ""
    found: list[tuple[int, str]] = []
    for name in _FIELD.findall(route.human):
        amount = _minor_units(name, fields.get(name))
        if amount is not None:
            found.append((amount, code))
    return found


def is_intercepted(exchange: Exchange) -> bool:
    """A write this run answered instead of performing - locally, or at an answer target: the
    writes the summary prints a line for, and the ones `irimi compare` compares.

    A REPLAYED write is not one of them. The agent retried with a key it had already used,
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


def _failed_target(exchange: Exchange) -> bool:
    """A request whose target irimi refused or could not reach, answered `irimi`, or one
    mitmproxy could not finish dialling, which stays `delegated`."""
    return TARGET_FAILED_FLAG in exchange.flags


def _refused_by_irimi(exchange: Exchange) -> bool:
    """A request irimi answered with its own error because it refused it or could not decide
    about it. Asked of the flags, which a run stored under schema 2 carries too."""
    return DECISION_FAILED_FLAG in exchange.flags or REFUSED_FLAG in exchange.flags


def _is_faked(exchange: Exchange) -> bool:
    """A write irimi answered with a fake of its own: not refused, and sent to no target."""
    return (
        not _refused_by_irimi(exchange)
        and not _failed_target(exchange)
        and exchange.answered_by != "delegated"
    )


def _would_fail(exchange: Exchange) -> bool:
    """A faked write the real service would have refused: the precondition check rejected it, or
    this run had already used its idempotency key for a different write."""
    return exchange.precondition == "rejected" or IDEMPOTENCY_CONFLICT_FLAG in exchange.flags


def _status_of(exchange: Exchange) -> str:
    return str(exchange.response.status) if exchange.response is not None else "no answer"


def _host_line(host: str, rows: Sequence[Exchange], width: int) -> str:
    """One host's counts, in the order a reader asks them: what was real, then what was not."""
    phrases: list[str] = []
    # An overlaid read is a real read: it went to the real service and came back, and irimi
    # then wrote the run's own faked writes into the body. It is counted here, and its count
    # qualifies `N reads` in brackets, because the body is not what the service sent. A read a
    # replay answered from the recording is counted with them too: the recorded run made it.
    agent_reads = [
        ex
        for ex in rows
        if ex.kind == "read"
        and ex.issued_by == "agent"
        and ex.answered_by in ("live", "overlay", "recording")
    ]
    if agent_reads:
        overlaid = sum(1 for ex in agent_reads if ex.answered_by == "overlay")
        recorded = sum(1 for ex in agent_reads if FROM_RECORDING_FLAG in ex.flags)
        failed = [ex for ex in agent_reads if ex.response is None or ex.response.status >= 400]
        qualified = []
        if overlaid:
            qualified.append(f"{overlaid} showing this run's writes")
        if recorded:
            qualified.append(f"{recorded} from the recording")
        if failed:
            codes = ", ".join(dict.fromkeys(_status_of(ex) for ex in failed))
            qualified.append(f"{len(failed)} failed: {codes}")
        reads = _plural(len(agent_reads), "read")
        phrases.append(f"{reads} ({', '.join(qualified)})" if qualified else reads)
    # A read irimi made for its own precondition check is real, but the agent did not make it,
    # so it is never counted in the agent's `N reads`.
    check_reads = sum(1 for ex in rows if ex.issued_by == "engine")
    if check_reads:
        phrases.append(_plural(check_reads, "irimi check read"))
    # A read a target answered is not a real read, and `reads are real` is what that count means.
    target_reads = sum(1 for ex in rows if ex.kind == "read" and ex.answered_by == "delegated")
    if target_reads:
        phrases.append(f"{_plural(target_reads, 'read')} from the target")
    irimi_reads = [
        ex
        for ex in rows
        if ex.kind == "read" and ex.issued_by == "agent" and ex.answered_by == "irimi"
    ]
    unreached_reads = sum(1 for ex in irimi_reads if _failed_target(ex))
    if len(irimi_reads) > unreached_reads:
        refused_reads = _plural(len(irimi_reads) - unreached_reads, "read")
        phrases.append(f"{refused_reads} {REFUSED_BY_IRIMI}")
    if unreached_reads:
        phrases.append(f"{_plural(unreached_reads, 'read')} {UNREACHED}")
    llm = [ex for ex in rows if ex.kind == "llm"]
    if llm:
        recorded_llm = sum(1 for ex in llm if FROM_RECORDING_FLAG in ex.flags)
        note = f" ({recorded_llm} from the recording)" if recorded_llm else ""
        phrases.append(f"{len(llm)} llm{note}")
    writes = [ex for ex in rows if is_intercepted(ex)]
    faked = [ex for ex in writes if _is_faked(ex)]
    if faked:
        unclassified = sum(1 for ex in faked if ex.kind == "unknown")
        note = f" ({unclassified} {UNCLASSIFIED})" if unclassified else ""
        phrases.append(f"{_plural(len(faked), 'faked write')}{note}")
    refused = sum(1 for ex in writes if _refused_by_irimi(ex))
    if refused:
        phrases.append(f"{_plural(refused, 'write')} {REFUSED_BY_IRIMI}")
    targeted = sum(1 for ex in writes if ex.answered_by == "delegated" and not _failed_target(ex))
    if targeted:
        phrases.append(f"{_plural(targeted, 'write')} sent to the target")
    unreachable = sum(1 for ex in writes if _failed_target(ex) and not _refused_by_irimi(ex))
    if unreachable:
        phrases.append(f"{_plural(unreachable, 'write')} {UNREACHED}")
    return printable(f"  {host:<{width}}  {'  '.join(phrases)}".rstrip())


# A host-line phrase for requests whose target irimi refused or could not reach.
UNREACHED = "that could not reach the target"


def _host_lines(exchanges: Sequence[Exchange], telemetry: Sequence[str]) -> list[str]:
    """One line per host, plus one line for all telemetry hosts together.

    Telemetry gets a single line however many vendors it reached: a run posts to Datadog, Sentry
    and PostHog in the same breath and a line each would bury the two hosts the agent's work is
    actually on. It also says where they went, because they went there for real.

    `telemetry` is one host per telemetry exchange, and `exchanges` holds none: the store keeps a
    telemetry exchange only as a `TelemetrySeen` with its host, so a host is all this line may
    count if a stored run is to print it.
    """
    by_host: dict[str, list[Exchange]] = {}
    for ex in exchanges:
        by_host.setdefault(ex.request.host, []).append(ex)
    hosts = sorted(by_host)
    width = max((len(h) for h in hosts), default=0)
    width = max(width, len("telemetry")) if telemetry else width
    lines = [_host_line(host, by_host[host], width) for host in hosts]
    if telemetry:
        vendors = len(set(telemetry))
        lines.append(
            printable(
                f"  {'telemetry':<{width}}  {_plural(len(telemetry), 'exchange')} "
                f"to {_plural(vendors, 'host')}, forwarded live"
            )
        )
    return lines


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


def check_reads(exchanges: Sequence[Exchange]) -> dict[int, Exchange]:
    """irimi's check read for each faked write whose precondition it asked, keyed by the write's
    `id()`. A write that made none, or whose read the run did not record, has no entry.

    The engine records a check read before the write it was made for, both in the write's run,
    so each such write takes the latest unclaimed read of its service before it, preferring one
    made while the write was being decided. A write a replay answered from the recording made no
    check read in this run and takes none."""
    found: dict[int, Exchange] = {}
    unclaimed: list[Exchange] = []
    for ex in exchanges:
        if ex.issued_by == "engine":
            unclaimed.append(ex)
            continue
        asked = (
            is_intercepted(ex)
            and ex.answered_by.startswith("fake-")
            and ex.precondition is not None
            and FROM_RECORDING_FLAG not in ex.flags
        )
        if not asked:
            continue
        same = [c for c in unclaimed if c.service == ex.service and c.run_id == ex.run_id]
        during = [c for c in same if ex.started_at <= c.started_at <= ex.ended_at]
        picked = (during or same)[-1:]
        if picked:
            found[id(ex)] = picked[0]
            unclaimed = [c for c in unclaimed if c is not picked[0]]
    return found


def _auth_failed(check_read: Exchange | None) -> bool:
    return (
        check_read is not None
        and check_read.response is not None
        and check_read.response.status in AUTH_FAILURES
    )


def _why_not_checked(exchange: Exchange, check_read: Exchange | None) -> str:
    """Why irimi could not check a write against live state, read off the read it made to. A
    write a replay answered from the recording made its check read in the recorded run."""
    if check_read is None and FROM_RECORDING_FLAG in exchange.flags:
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


def _write_line(exchange: Exchange, index: MapIndex | None, check_read: Exchange | None) -> str:
    """One intercepted write's summary line. Escaped as `runs show` escapes a run's text: a
    template's fields are the agent's own, a newline in a Slack post would split the line, and a
    lone surrogate would make printing it raise."""
    return f"  {printable(write_sentence(exchange, index, check_read))}"


def write_sentence(
    exchange: Exchange, index: MapIndex | None, check_read: Exchange | None = None
) -> str:
    """One intercepted write: its marker, its service, the map's own sentence, and what irimi did
    with it. Its summary line without the indent and before it is escaped, which `irimi compare`
    prints for it. `check_read` is irimi's check read for it (`check_reads`), which says why a
    check could not be made. A write irimi refused with a body it never read whole is spelled as
    the request it was instead."""
    refused = _refused_by_irimi(exchange)
    # A `human:` sentence is never filled in from a body irimi did not read: a refused write whose
    # body was cut renders every field `?`, the same line for every such refund.
    unread = refused and BODY_TRUNCATED_FLAG in exchange.flags
    route = route_of(exchange, index)
    if route is not None and route.human and not unread:
        what = render_human(route.human, exchange, route)
    else:
        what = f"{exchange.request.method} {exchange.request.host}{exchange.request.path}"
    what = f"{exchange.service}  {what}"
    if refused:
        if DECISION_FAILED_FLAG not in exchange.flags:
            return f"{REJECTED_MARKER} {what}  {REFUSED_BY_IRIMI}"
        why = TOO_LARGE if BODY_TRUNCATED_FLAG in exchange.flags else DECISION_FAILED
        return f"{REJECTED_MARKER} {what}  {REFUSED_BY_IRIMI}: {why}"
    if _failed_target(exchange):
        return f"{REJECTED_MARKER} {what}  {TARGET_UNREACHABLE} {_target_host(exchange.target)}"
    if exchange.answered_by == "delegated":
        return f"{WRITE_MARKER} {what}  {SENT_TO_TARGET} {_target_host(exchange.target)}"
    # A write the real service would have refused, and irimi is saying so. `✗`, not `○`, and the
    # code is the one the agent's SDK raised on.
    if _would_fail(exchange):
        code = exchange.rejection_code or "rejected"
        return f"{REJECTED_MARKER} {what}  {FAKED}; {WOULD_FAIL}: {code}"
    if exchange.precondition == "passed":
        status = CHECKED
    elif exchange.precondition == "not_evaluable":
        status = f"{COULD_NOT_CHECK}: {_why_not_checked(exchange, check_read)}"
    elif exchange.kind == "unknown":
        status = f"{NOT_CHECKED} ({UNCLASSIFIED})"
    else:
        status = NOT_CHECKED
    return f"{WRITE_MARKER} {what}  {FAKED}; {status}"


def _target_host(target: str) -> str:
    """A target's host and port, as a write line names it."""
    return urlsplit(target).netloc or target


def _shows_overlay(exchange: Exchange) -> bool:
    """One of the agent's own reads the summary owes a `↳` line.

    Edited to show the run's writes, or known to be incomplete - not merely `overlay: full`, which
    a read irimi only translated also carries while staying `live` and untouched. Never a
    read irimi issued itself: the line says which of the AGENT's reads saw the write.
    """
    return (
        exchange.kind == "read"
        and exchange.issued_by == "agent"
        and (exchange.answered_by == "overlay" or exchange.overlay == "partial")
    )


def _overlay_lines(reads: Sequence[Exchange]) -> list[str]:
    """The `↳` lines under one write: which of the agent's reads were shown it, and how fully.

    Three cases, because `overlay` and `answered_by` say different things. `answered_by ==
    "overlay"` means irimi edited the body; `overlay == "partial"` means irimi knows the world it
    showed was incomplete, and it can sit on a body irimi never touched - a Slack read whose
    channel the two sides spell differently. A read irimi merely TRANSLATED, live and
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
            # Untouched, so it is whoever answered it: the service, or a replay's recording.
            verb, column = DID_NOT_SHOW, f"{answer_word(ex)} (partial)"
        lines.append(printable(f"    {OVERLAY_MARKER} {what} {verb}  {column}"))
    return lines


def _writes_with_their_reads(
    exchanges: Sequence[Exchange],
) -> list[tuple[Exchange, list[Exchange]]]:
    """Every printed write, in order, each with the overlaid reads that are about it.

    A read is filed under the most recent write before it that the overlay could have shown it:
    one of the SAME SERVICE, and one irimi authored (`is_authored_write`, the write log's own
    rule). Not simply "the last write". A run that refunds on Stripe and then reads Slack history
    would otherwise file the Slack read under the Stripe refund; and a refund, its refused retry
    and then a list would file the list under the `✗` line, saying the agent was shown a refund
    that was refused. A refused write is printed, but it never reached the write log, so no read
    ever saw it.

    A read irimi issued itself is never here: `issued_by == "engine"` is a read the AGENT did not
    make, and the whole point of the line is to say which of the agent's reads saw the write.

    Pairs rather than a dict keyed by write: `Exchange` is a mutable dataclass and so unhashable.
    A qualifying read with no such write before it is left out, and cannot happen - the overlay
    stamps or flags a read only on the strength of a same-service write in the log, and every
    write-log entry is a printed write. A replay serves a recorded overlaid read only once every
    recorded write before it has been made (`recording.Recording.take`), so it holds there
    too. `tests/test_invariants.py` holds it over a real run.
    """
    paired: list[tuple[Exchange, list[Exchange]]] = []
    last: dict[str, int] = {}
    for ex in exchanges:
        if is_intercepted(ex):
            if is_authored_write(ex):
                last[ex.service] = len(paired)
            paired.append((ex, []))
        elif _shows_overlay(ex) and ex.service in last:
            paired[last[ex.service]][1].append(ex)
    return paired


def _would_fire(writes: Sequence[Exchange], checks: dict[int, Exchange]) -> list[str]:
    """Every webhook event the run's faked writes would have caused, each named once, in the order
    the run first listed it.

    Off `Exchange.would_fire` and never off `route.fires`: `would_fire` is already empty for
    an L3 rejection, an idempotency conflict, a replay and a delegated write. A write whose check
    read the service refused for its credentials is left out too: the real write would most
    likely have been refused the same way. De-duplicated because two refunds carry two identical
    tuples and one refund's `refund.created` must not be promised twice.
    """
    fired = (ex for ex in writes if not _auth_failed(checks.get(id(ex))))
    return list(dict.fromkeys(event for ex in fired for event in ex.would_fire))


def _money_line(writes: Sequence[Exchange], index: MapIndex | None) -> list[str]:
    """The amounts the faked writes' sentences show, summed per currency, and how much of that the
    real service would have refused. Nothing when no amount has a known currency: an amount irimi
    cannot denominate is counted apart rather than added to one it can."""
    total: dict[str, int] = {}
    failing: dict[str, int] = {}
    unknown = 0
    for ex in writes:
        if not _is_faked(ex):
            continue
        for value, code in _amounts(ex, route_of(ex, index)):
            if not code:
                unknown += 1
                continue
            total[code] = total.get(code, 0) + value
            if _would_fail(ex):
                failing[code] = failing.get(code, 0) + value
    if not total:
        return []
    line = f"  money: {_sum(total)} in faked writes"
    if failing:
        line += f"; {_sum(failing)} of it would fail"
    if unknown:
        line += f"; {_plural(unknown, 'amount')} in an unknown currency not counted"
    return [line]


def _sum(amounts: dict[str, int]) -> str:
    return " + ".join(money(value, code) for code, value in amounts.items())


def _sdk_run_lines(exchanges: Sequence[Exchange], runs: dict[str, RunRecord | None]) -> list[str]:
    """One line per SDK run inside the process, then how to replay one: the live summary counts
    the whole process, and a replay needs the SDK run's own id."""
    lines: list[str] = []
    for run_id, record in runs.items():
        trigger = record.trigger if record is not None else None
        call = MISSING_FIELD
        if trigger is not None:
            call = f"{trigger.name}({'...' if trigger.args else ''})"
        seen = [ex for ex in exchanges if ex.run_id == run_id]
        writes = sum(1 for ex in seen if is_intercepted(ex))
        counts = f"{_plural(len(seen), 'exchange')} · {_plural(writes, 'write')}"
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
            f"  {_plural(count, 'connection')} to {host} failed TLS: the client did not trust "
            f"irimi's CA; point it at {ca} (some clients ignore SSL_CERT_FILE)"
        )
        for host, count in sorted(failures.items())
    ]


def summary_lines(
    run_id: str,
    exchanges: Sequence[Exchange],
    elapsed_s: float = 0.0,
    index: MapIndex | None = None,
    tool_calls: Sequence[ToolCall] = (),
    sdk_runs: dict[str, RunRecord | None] | None = None,
    tls_failures: Mapping[str, int] | None = None,
    ca_cert: Path | None = None,
) -> list[str]:
    """The exit summary: what the agent tried to write, what was real, and what irimi answered.

    Built from the exchanges the engine reported, in the order they arrived, and the tool calls
    the control endpoint passed on, which `_summary` puts in the order they ended.
    `stored_summary_lines` prints the same block from a stored run, and both are `_summary`, so
    the two cannot drift.

    `index` is the loaded maps, used only to find each write's `human:` template. Without it the
    writes still get a line, spelled as the request they were. `sdk_runs` are the runs the SDK
    started inside the process, by id, each with its stored record or None: when there are any,
    the header says it covers the whole process and the block ends with a line per run.
    `tls_failures` counts the handshakes clients refused, by host, which a stored run does not
    keep; `ca_cert` is the certificate those lines tell the reader to point a client at.
    """
    telemetry = [ex.request.host for ex in exchanges if ex.kind == "telemetry"]
    rest = [ex for ex in exchanges if ex.kind != "telemetry"]
    lines = _summary(run_id, rest, telemetry, elapsed_s, index, tool_calls, whole=bool(sdk_runs))
    if not exchanges:
        lines.append(f"  {NO_TRAFFIC}")
    lines += tls_failure_lines(tls_failures or {}, ca_cert)
    if sdk_runs:
        lines += ["", *_sdk_run_lines(exchanges, sdk_runs)]
    return lines


def stored_summary_lines(run: StoredRun, index: MapIndex | None) -> list[str]:
    """The exit summary of a stored run: the lines `summary_lines` printed live for it.

    A `TelemetrySeen` stands for a telemetry exchange, which is always forwarded live: the policy
    never answers or delegates one. The elapsed seconds are the run's own, `ended_at - started_at`,
    where the live summary timed the child; a run with no start or no end has `0.0`.

    Where a stored line may still differ from the live one: redaction replaced a field a `human:`
    template renders, the path of a request on a credential-path host that the write line spells
    out, because no route matched it or irimi refused it with its body cut, a write irimi
    refused whose body only the store cut, spelled out, or an event the store dropped
    (docs/trace-format.md, "Reading a run back"). The live summary's lines for refused TLS
    handshakes, which the store does not keep, and for a run that saw no traffic have no stored
    line. A process run's stored summary
    covers its own events, never the SDK runs inside it. A replay's run printed no summary live,
    and its block opens `irimi replay`.
    """
    exchanges = [event for event in run.events if isinstance(event, Exchange)]
    telemetry = [event.host for event in run.events if isinstance(event, TelemetrySeen)]
    tool_calls = [event for event in run.events if isinstance(event, ToolCall)]
    elapsed = run_elapsed(run.record)
    # A replay's run opens `irimi replay · run`: `irimi replay` printed its comparison and no
    # summary, and a block opening `irimi shadow` would say a replay was a shadow run.
    command = "replay" if run.record.mode == "replay" else "shadow"
    return _summary(
        run.record.run_id, exchanges, telemetry, elapsed or 0.0, index, tool_calls, command
    )


def _summary(
    run_id: str,
    exchanges: Sequence[Exchange],
    telemetry: Sequence[str],
    elapsed_s: float,
    index: MapIndex | None,
    tool_calls: Sequence[ToolCall],
    command: str = "shadow",
    whole: bool = False,
) -> list[str]:
    """The one implementation of the summary. `exchanges` holds no telemetry; `telemetry` is one
    host per telemetry exchange, each of them forwarded live. A tool call is not an exchange
    and is in no count: each write tool whose fake ran gets a line after the HTTP writes, and a
    read tool, run for real or answered from a replay's recording, gets none.

    The writes come first, under the line saying none of them reached the real service, with
    the money they name and the webhooks they would have fired; then each host's counts; then
    the totals."""
    paired = _writes_with_their_reads(exchanges)
    writes = [write for write, _ in paired]
    checks = check_reads(exchanges)
    total = len(exchanges) + len(telemetry)
    # An overlaid read was forwarded to the real service and answered by it; irimi edited the body
    # afterwards, so it is real. One a replay answered from the recording reached no service, so
    # irimi answered it.
    real = len(telemetry) + sum(
        1
        for ex in exchanges
        if ex.answered_by in ("live", "overlay") and FROM_RECORDING_FLAG not in ex.flags
    )
    # A target mitmproxy could not finish dialling answered nothing, so it is not counted as one.
    targets = sum(1 for ex in exchanges if ex.answered_by == "delegated" and not _failed_target(ex))
    by_irimi = total - real - targets
    faked = sum(1 for ex in writes if _is_faked(ex))
    scope = " (whole process)" if whole else ""
    lines = [
        f"irimi {command} · run {run_id}{scope} · {_plural(total, 'exchange')} · {elapsed_s:.1f}s"
    ]
    write_lines: list[str] = []
    for write, reads in paired:
        write_lines.append(_write_line(write, index, checks.get(id(write))))
        write_lines += _overlay_lines(reads)
    # In the order the calls ended, whatever order they came in: live, the order their posts
    # reached the control endpoint, which two tools ending at once on two threads can reverse, and
    # stored, `seq` order within each run. The start, the id and then the line itself break
    # a tie, since a call posted by hand can repeat another's id and times.
    stood_in = sorted(
        (call for call in tool_calls if is_stood_in(call)),
        key=lambda call: (
            call.ended_at,
            call.started_at,
            call.tool_call_id,
            tool_write_sentence(call),
        ),
    )
    write_lines += [tool_write_line(call) for call in stood_in]
    if write_lines:
        lines += ["", f"  {NOTHING_HAPPENED}", *write_lines, *_money_line(writes, index)]
        events = _would_fire(writes, checks)
        if events:
            lines.append(f"  {WOULD_HAVE_FIRED} {', '.join(events)}")
    host_lines = _host_lines(exchanges, telemetry)
    if host_lines:
        lines += ["", *host_lines]
    lines += [
        "",
        f"  {_plural(total, 'exchange')} · {real} real · {by_irimi} answered by irimi "
        f"({_plural(faked, 'faked write')}) · {targets} sent to targets",
    ]
    return lines


# What a live run that saw no exchange says: an agent whose client ignored the proxy, or refused
# irimi's CA, still made its calls. Not said of a stored run, whose traffic its SDK runs may hold.
NO_TRAFFIC = (
    "irimi saw no HTTP(S) traffic; if the agent made calls, they bypassed the proxy or failed TLS"
)


# ---------------------------------------------------------------------------------- stored runs

# `irimi runs show`'s `args:` line: enough to recognise a trigger by, not a dump of it.
MAX_ARGS_SHOWN = 500
NOT_SET = "-"
# The `args:` of a trigger nested deeper than `json.dumps` writes. Python 3.14's `json.loads` reads
# a `run.json` some levels deeper than its `json.dumps` writes back, so a damaged one reads fine
# and would then raise RecursionError out of `runs show`.
ARGS_TOO_DEEP = "(nested too deeply to show)"
# A C0 or C1 control character, DEL included. A stored run's text is the agent's own - an error
# message is `str(exc)` - and printed raw, a newline breaks `runs show`'s one `key: value` per line
# and an escape sequence is obeyed by the terminal reading it. A lone surrogate too
# (`os.fsdecode` of a filename that is not UTF-8): a UTF-8 terminal cannot encode it, and printed
# raw it would raise UnicodeEncodeError out of `runs show`, and a stored run always prints.
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f\ud800-\udfff]")


def printable(text: str) -> str:
    """`text` on one line, each control character and lone surrogate spelled as its Python escape
    (`\\n`, `\\x1b`, `\\udcff`). For printing only, never for comparing: a backslash is left as it
    is, so a newline and a typed `\\n` print alike."""
    return _CONTROL.sub(lambda m: repr(m.group())[1:-1], text)


def run_elapsed(record: RunRecord) -> float | None:
    """How long a stored run took, or None for one with no start or no end. Never negative: the
    wall clock can step back between the two, as `pipeline.annotate` knows for an exchange."""
    if record.started_at is None or record.ended_at is None:
        return None
    return max(0.0, record.ended_at - record.started_at)


def run_status(record: RunRecord) -> str:
    """`ok` or `error` for a run that ended, and `incomplete` for one that has not, or never will:
    a crashed irimi, or a run known only by its events (`attribution: "header"`)."""
    return record.outcome if record.outcome is not None else "incomplete"


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
    Only each event's kind and how it was answered are read, so a caller may pass events loaded
    without their bodies."""
    started = NOT_SET
    if record.started_at is not None:
        started = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.started_at))
    elapsed = run_elapsed(record)
    duration = NOT_SET if elapsed is None else f"{elapsed:.1f}s"
    status = run_status(record)
    if record.outcome == "error" and record.exit_code is not None:
        status = f"error (exit {record.exit_code})"
    if events is None:
        counts = ["? exchanges", "? writes"]
    else:
        # Telemetry counts, as it does in the summary's `N exchanges`; a tool call does not. The
        # writes are the ones the summary prints a line for.
        seen = [event for event in events if not isinstance(event, ToolCall)]
        writes = sum(1 for ev in seen if isinstance(ev, Exchange) and is_intercepted(ev))
        counts = [_plural(len(seen), "exchange"), _plural(writes, "write")]
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
    args = trigger.args
    argv = args.get("argv") if record.attribution == "process" and isinstance(args, dict) else None
    if isinstance(argv, list) and argv and all(isinstance(word, str) for word in argv):
        words = [str(word) for word in argv]
        text = shlex.join([os.path.basename(words[0]) or words[0], *words[1:]])
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
        lines.append(f"({_plural(older, 'older run')}; use --limit or --all)")
    return lines


def run_attribution(record: RunRecord) -> str:
    """`irimi runs list`'s attribution column. A run a replay stored is listed as the run it
    replayed: `replay of <id>` for the process run `irimi replay` stores, which `process` alone
    lists as a shadow run, and after its attribution for any other, since a replay's store makes
    `header` runs and `sdk` runs too and the column must not hide which (`<attribution>, replay of
    <id>`). A record that says `mode: replay` and names no run - one no irimi writes, and one no
    stored run.json can hold, which the decoder refuses, so only one built in Python -
    is still marked (`<attribution>, replay`)."""
    if record.replay_of is not None:
        replayed = f"replay of {printable(record.replay_of)}"
        return replayed if record.attribution == "process" else f"{record.attribution}, {replayed}"
    if record.mode != "shadow":
        return f"{record.attribution}, {record.mode}"
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
            args = json.dumps(trigger.args, separators=(",", ":"), ensure_ascii=False)
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
    if record.error is not None:
        lines.append(f"error: {printable(record.error.type)}: {printable(record.error.message)}")
    if record.exit_code is not None:
        lines.append(f"exit code: {record.exit_code}")
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
    return [*lines, "", *stored_summary_lines(run, index)]
