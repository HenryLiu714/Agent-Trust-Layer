"""Checkers for the rules irimi holds on every run. Each returns what breaks a rule, one
string per break, so a test asserts it empty on an honest run and non-empty on a forged one."""

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
    """Every answer in `exchanges` whose `Irimi-Answered-By` is missing, present on a live
    forward, or names someone other than who answered, one per line."""
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
    """Invariant (a)'s other half, which only an end-to-end run can check: a `live` exchange
    reached the real service, and an engine-answered one did not.

    `stamp` cannot catch a policy that answers locally and stamps `live` - the stamp is the only
    thing it has to go on, and a liar's stamp says `live`. What the upstream actually received is
    the fact outside the policy's reach, so the check is written against it.
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
    """One of the agent's own reads that irimi edited to show a write, or knows it showed only in
    part. This deliberately restates the rule `report.model.disposition` applies rather than
    calling it: the invariant is what the summary OWES, so a model that stopped owing a line
    must fail here instead of agreeing with itself."""
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
    """Every one of the agent's reads the summary owes a `↳` line but files under no write, or
    under a write the overlay was never handed, named one per line. `write_log` is every write the
    overlay held during the run: a read cannot have been shown a write outside it, so a `↳` under
    one - a refused retry, say - tells the reader the agent saw what it never saw. Matched by
    identity, not equality: a copy of a filed read is a different read."""
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
    """Every exchange irimi issued on its own account that is not a read, named one per line.
    A precondition read that were anything else would be irimi performing a write nobody asked
    for, in a tool whose promise is that writes are virtual."""
    return [
        f"check read: engine-issued {ex.kind}: {ex.request.method} {ex.request.path}"
        for ex in exchanges
        if ex.issued_by == "engine" and ex.kind != "read"
    ]


def write_log_violations(exchanges: Sequence[Exchange], log: Sequence[Exchange]) -> list[str]:
    """Every way `log`, the writes the overlay was handed, disagrees with the run's `exchanges`:
    an entry that is not a write that would have happened (a read, a precondition rejection, a
    retry answered from its first try, an idempotency conflict, an entry twice), or a write that
    lists webhooks yet is not in the log. Matched by identity, as the log holds the run's own."""
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
