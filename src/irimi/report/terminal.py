"""Build what a run prints as it runs; `printable` escapes the agent's text in every line."""

import re
from pathlib import Path

from irimi.core import netaddr
from irimi.core.exchange import AnsweredBy, Exchange, Flag
from irimi.core.netaddr import Target
from irimi.protocol.trace import Ran, ToolCall
from irimi.servicemaps import MapIndex, has_target

PROXY_ONLY_NOTICE = (
    "only traffic sent through this proxy is shadowed; a client that ignores HTTPS_PROXY talks to "
    "the real service"
)
# Serve mode has no run of its own: the SDK names each run on its requests.
SERVE_RUNS = "run: per request (SDK), unattributed otherwise"
WIDE_BIND_NOTICE = (
    "any machine that can reach this port can send traffic through irimi and call its control "
    "endpoint"
)

# Marks a service or route sent to a target: a routed host may not be live either.
TARGET_PREFIX = "target:"
NOT_LOOPBACK_NOTICE = "not loopback: these requests leave this machine"
NOT_LOOPBACK_INDENT = " " * len(f"{TARGET_PREFIX} ")
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
    """Return the startup lines; `run_id` is None in serve mode.

    A non-loopback listener (`0.0.0.0` and `::` too) adds `WIDE_BIND_NOTICE`; the base-URL
    listener stays loopback-only whatever the bind."""
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


def target_lines(index: MapIndex, color: bool = False) -> list[str]:
    """Return a banner line per service or route sent to a target, with non-loopback warnings."""
    lines: list[str] = []
    for sm in sorted(index.services, key=lambda s: s.service):
        if not has_target(sm):
            continue
        if sm.target is not None:
            scope = "reads + writes" if sm.target_reads else "writes"
            lines.extend(_target_line(sm.service, sm.target, scope, color))
        for route in sm.routes:
            if route.target is not None:
                what = f"{sm.service} {route.method} {route.path}"
                scope = "reads" if route.kind == "read" else "writes"
                lines.extend(_target_line(what, route.target, scope, color))
    return lines


def _target_line(what: str, target: Target, scope: str, color: bool) -> list[str]:
    """Return one target's line, plus a warning on its own line so a long URL cannot hide it."""
    line = f"{TARGET_PREFIX} {what} -> {target.url} ({scope})"
    if target.is_loopback:
        return [line]
    notice = f"{NOT_LOOPBACK_INDENT}{NOT_LOOPBACK_NOTICE}"
    if color:
        return [f"{RED}{line}{RESET}", f"{RED}{notice}{RESET}"]
    return [line, notice]


# The stream's first column: who answered, in the summary's words. irimi's check reads show as
# `CHECK_READ` whoever answered, so they never read as the agent's.
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
FIRST_COLUMN = max(len(word) for word in (*ANSWER_WORDS.values(), CHECK_READ, *TOOL_WORDS.values()))
# Shown as a flag on an overlaid read, whose first column says `real`.
OVERLAY_SHOWN = "overlay"


def answer_word(exchange: Exchange) -> str:
    """Return the stream's first column; a replay's answer is `recorded` even if overlaid."""
    if exchange.issued_by == "engine":
        return CHECK_READ
    if exchange.answered_by == "overlay" and Flag.FROM_RECORDING in exchange.flags:
        return ANSWER_WORDS["recording"]
    return ANSWER_WORDS[exchange.answered_by]


def _shown_flags(exchange: Exchange) -> list[str]:
    """Return the flags the stream prints, with `overlay` first for an overlaid read."""
    shown: list[str] = list(exchange.flags)
    if exchange.answered_by == "overlay":
        shown.insert(0, OVERLAY_SHOWN)
    return shown


def exchange_line(exchange: Exchange) -> str:
    """Return the escaped line for one finished exchange."""
    status = exchange.response.status if exchange.response else "-"
    flags = _shown_flags(exchange)
    tail = f"  [{', '.join(flags)}]" if flags else ""
    # The kind column is nine wide: `telemetry` is nine characters.
    return printable(
        f"{answer_word(exchange):<{FIRST_COLUMN}} {exchange.kind:<9} {exchange.request.method} "
        f"{exchange.request.host}{exchange.request.path} -> {status}{tail}"
    )


def tool_call_line(call: ToolCall) -> str:
    """Return the escaped line for one tool call, in `exchange_line`'s columns."""
    outcome = tool_outcome(call, "ok")
    word = TOOL_WORDS[call.ran]
    return printable(f"{word:<{FIRST_COLUMN}} {call.kind:<9} {call.name} -> {outcome}")


def tool_outcome(call: ToolCall, returned: str) -> str:
    """Return `returned`, or `raised <type>` if the call raised; unescaped."""
    return returned if call.error is None else f"raised {call.error.type}"


def plural(count: int, word: str) -> str:
    return f"{count} {word}" if count == 1 else f"{count} {word}s"


# C0 and C1 controls (DEL included) would break lines or drive the terminal; a lone surrogate
# (a non-UTF-8 filename) would raise UnicodeEncodeError on a UTF-8 terminal.
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f\ud800-\udfff]")


def printable(text: str) -> str:
    """Return `text` on one line, controls and lone surrogates as Python escapes (`\\x1b`).

    For printing, not comparing: backslashes stay, so a newline and a typed `\\n` look alike."""
    return _CONTROL.sub(lambda m: repr(m.group())[1:-1], text)
