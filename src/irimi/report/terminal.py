"""What a run prints as it runs: the startup banner, its delegation lines, and one line per
exchange and per tool call. Also `printable`, the one rule every printed line escapes the
agent's text by.
"""

import re
from pathlib import Path

from irimi import netaddr
from irimi.exchange import AnsweredBy, Exchange, Flag
from irimi.netaddr import Target
from irimi.servicemap import MapIndex, is_delegated
from irimi.trace import Ran, ToolCall

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
        if sm.target is not None:
            scope = "reads + writes" if sm.target_reads else "writes"
            lines.extend(_delegated_line(sm.service, sm.target, scope, color))
        for route in sm.routes:
            if route.target is not None:
                what = f"{sm.service} {route.method} {route.path}"
                scope = "reads" if route.kind == "read" else "writes"
                lines.extend(_delegated_line(what, route.target, scope, color))
    return lines


def _delegated_line(what: str, target: Target, scope: str, color: bool) -> list[str]:
    """The line for one delegated service or route, plus the warning line a non-loopback one owes.

    The warning is its own line rather than a tail on the first: a target long enough to be worth
    warning about is long enough that the two together wrap at 100 columns, and a wrapped warning
    is the one a reader skims past.
    """
    line = f"{DELEGATED_PREFIX} {what} -> {target.url} ({scope})"
    if target.is_loopback:
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
    if exchange.answered_by == "overlay" and Flag.FROM_RECORDING in exchange.flags:
        return ANSWER_WORDS["recording"]
    return ANSWER_WORDS[exchange.answered_by]


def _shown_flags(exchange: Exchange) -> list[str]:
    """The flags the stream prints, with `overlay` first for a read the overlay changed."""
    shown: list[str] = list(exchange.flags)
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
    outcome = tool_outcome(call, "ok")
    word = TOOL_WORDS[call.ran]
    return printable(f"{word:<{FIRST_COLUMN}} {call.kind:<9} {call.name} -> {outcome}")


def tool_outcome(call: ToolCall, returned: str) -> str:
    """`returned`, or `raised <type>`, before any line escapes it."""
    return returned if call.error is None else f"raised {call.error.type}"


def plural(count: int, word: str) -> str:
    return f"{count} {word}" if count == 1 else f"{count} {word}s"


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
