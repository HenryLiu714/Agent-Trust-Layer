"""Checkers for the rules irimi holds on every run, each returning one string per break.

A test asserts a checker empty on an honest run and non-empty on a forged one.
"""

from collections.abc import Sequence

from irimi.core.exchange import Exchange, Flag, Response
from irimi.engine.wire import stamp
from irimi.protocol.headers import ANSWERED_BY_HEADER
from irimi.protocol.trace import RunRecord
from irimi.report.model import RunView, build


def answered_by_of(response: Response | None) -> str | None:
    """The `Irimi-Answered-By` a response carries, or None."""
    if response is None:
        return None
    return next((v for k, v in response.headers if k.lower() == ANSWERED_BY_HEADER), None)


def stamping_violations(exchanges: Sequence[Exchange]) -> list[str]:
    """Every answer whose `Irimi-Answered-By` is missing, on a live forward, or names another."""
    out: list[str] = []
    for ex in exchanges:
        where = f"{ex.answered_by} {ex.kind} {ex.request.method} {ex.request.path}"
        sent = None if ex.response is None else stamp(ex.response, ex.answered_by)
        value = answered_by_of(sent)
        if ex.answered_by != "live" and sent is not None and value is None:
            out.append(
                f"answered-by: engine-answered response with no {ANSWERED_BY_HEADER}: {where}"
            )
        if ex.answered_by == "live" and value is not None:
            out.append(f"answered-by: live forward carrying {ANSWERED_BY_HEADER}={value}: {where}")
        if value is not None and value != ex.answered_by:
            out.append(
                f"answered-by: {ANSWERED_BY_HEADER}={value} does not name the answer: {where}"
            )
    return out


def reached_upstream_violations(
    exchanges: Sequence[Exchange], upstream_hits: Sequence[tuple[str, str]]
) -> list[str]:
    """Every `live` exchange the upstream never saw, and every answered-by-irimi one it did see.

    This is the answered-by header's other half: a policy that answers locally and stamps `live`
    passes `stamping_violations`, so the check is made against what the upstream received.
    """
    out: list[str] = []
    hits = list(upstream_hits)
    for ex in exchanges:
        reached = (ex.request.method, ex.request.path) in hits
        if ex.answered_by == "live" and not reached:
            out.append(f"answered-by: `live` exchange the upstream never saw: {ex.operation}")
        if ex.answered_by != "live" and reached:
            out.append(
                f"target: {ex.answered_by} exchange that reached the upstream: {ex.operation}"
            )
    return out


def owes_overlay_line(ex: Exchange) -> bool:
    """Whether `ex` is an agent read irimi overlaid, wholly or in part, so the summary owes a line.

    It restates `report.model.disposition`'s rule rather than calling it, so a model that stops
    owing the line fails here instead of agreeing with itself."""
    return (
        ex.kind == "read"
        and ex.issued_by == "agent"
        and (ex.answered_by == "overlay" or ex.overlay == "partial")
    )


def filed_reads(exchanges: Sequence[Exchange]) -> list[tuple[Exchange, list[Exchange]]]:
    """Each write the summary prints, with the reads it files under it."""
    view = RunView(RunRecord.header("t3st"), list(exchanges))
    writes = build(view, None).writes
    return [(entry.exchange, [read for read, _ in entry.reads]) for entry in writes]


def overlay_line_violations(
    exchanges: Sequence[Exchange], write_log: Sequence[Exchange]
) -> list[str]:
    """Every agent read owed a `↳` line but filed under no write, or under one the overlay lacked.

    `write_log` is every write the overlay held. A `↳` under any other write (a refused retry,
    say) claims the agent saw what it never saw. Reads match by identity, not equality."""
    filed = [(write, read) for write, reads in filed_reads(exchanges) for read in reads]
    violations = []
    for ex in exchanges:
        if not owes_overlay_line(ex):
            continue
        what = f"{ex.answered_by} read, overlay {ex.overlay}: {ex.request.method} {ex.request.path}"
        under = next((write for write, read in filed if read is ex), None)
        if under is None:
            violations.append(f"overlay line: {what} is under no {ex.service} write")
        elif not any(under is logged for logged in write_log):
            violations.append(f"overlay line: {what} is under a write the overlay never held")
    return violations


def engine_issued_violations(exchanges: Sequence[Exchange]) -> list[str]:
    """Every exchange irimi issued on its own account that is not a read.

    A check read that were anything else would be irimi making a write nobody asked for."""
    return [
        f"check read: engine-issued {ex.kind}: {ex.request.method} {ex.request.path}"
        for ex in exchanges
        if ex.issued_by == "engine" and ex.kind != "read"
    ]


def write_log_violations(exchanges: Sequence[Exchange], log: Sequence[Exchange]) -> list[str]:
    """Every way `log`, the writes the overlay was handed, disagrees with the run's `exchanges`.

    An entry must be a write that would have happened: not a read, a rejected write, a retry, an
    idempotency conflict or a repeat. A write listing webhooks must be in the log. By identity."""
    out = []
    for at, ex in enumerate(log):
        what = f"{ex.kind} {ex.request.method} {ex.request.path}"
        if ex.kind != "write":
            out.append(f"write log: holds a {what}")
        if ex.precondition.outcome == "rejected":
            out.append(f"write log: holds a rejected {what}")
        if Flag.IDEMPOTENT_REPLAY in ex.flags:
            out.append(f"write log: holds a retry {what}")
        if Flag.IDEMPOTENCY_CONFLICT in ex.flags:
            out.append(f"write log: holds a conflicting {what}")
        if any(ex is earlier for earlier in log[:at]):
            out.append(f"write log: holds {what} twice")
    for ex in exchanges:
        if ex.would_fire and not any(ex is logged for logged in log):
            what = f"{ex.request.method} {ex.request.path}"
            out.append(f"write log: {what} lists webhooks but is not in the log")
    return out
