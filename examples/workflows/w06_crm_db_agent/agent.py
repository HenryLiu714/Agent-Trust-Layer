"""W6 `crm_db_agent`: tools the proxy cannot see, over a local SQLite CRM.

The Phase 3 page's feature 3: "function database_write() gets labeled, the label specifies
mock_database_write() to be called using the same parameters during mock runs". Every database
and filesystem write here is an `@sdk.tool(kind="write", shadow=...)`, so under `irimi shadow` the
stand-in runs and the real body never does. Reads are `@sdk.tool(kind="read")` and run for real.

    python -m examples.workflows.launch \\
        examples.workflows.w06_crm_db_agent.agent <scenario> [segment]

Beyond `enrich`, each scenario puts one case of #76 through the real `irimi shadow`: a write's
stand-in that raises and a retry (`merge_retry`), and the same merge right the first time
(`merge_once`), which `irimi compare` holds the retry against (#79); tools called in no run
(`outside_run`); HTTP made inside a write's stand-in (`stand_in_http`); and an async read, a sync
write that returns a coroutine and an async static method whose stand-in is a static method too
(`async_tools`); and two writes at once, the one that started first ending last
(`overlapping_writes`), which a run stores, and `irimi compare` lines up, in the order they ended
(#79).

Seeding the CRM on first run is setup, not a tool: it happens in both modes, before the trigger.
Tools carry explicit, short names (`crm.find_accounts`), the way a developer would name the
tools a report lists; the other workflows show the default, `module.qualname`.
"""

from __future__ import annotations

import asyncio
import csv
import json
import sqlite3
import sys
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from examples.workflows import agentkit, sdk

DB_NAME = "crm.sqlite3"
SCENARIOS = (
    "enrich",
    "read_after_write",
    "unlabeled_write",
    "tool_raises",
    "decoration_errors",
    "merge_retry",
    "merge_once",
    "outside_run",
    "stand_in_http",
    "async_tools",
    "overlapping_writes",
)
STALE_BEFORE = "2025-06-01"
SEED = [
    ("acc_1", "Acme", "smb", "2026-01-01", 0, None),
    ("acc_2", "Globex", "smb", "2025-01-01", 0, None),
    ("acc_3", "Initech", "enterprise", "2026-06-01", 0, None),
]


def seed() -> None:
    if (agentkit.state_dir() / DB_NAME).exists():
        return
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute(
            "CREATE TABLE accounts (id TEXT PRIMARY KEY, name TEXT, segment TEXT,"
            " last_seen TEXT, score INTEGER, industry TEXT)"
        )
        conn.executemany("INSERT INTO accounts VALUES (?, ?, ?, ?, ?, ?)", SEED)


# -- read tools: run for real, shadow or not ------------------------------------------------------


@sdk.tool(kind="read", name="crm.find_accounts")
def find_accounts(segment: str) -> list[dict[str, Any]]:
    with agentkit.sqlite(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM accounts WHERE segment = ? ORDER BY id", (segment,))
        return [dict(r) for r in rows]


@sdk.tool(kind="read", name="crm.fetch_enrichment")
def fetch_enrichment(account_id: str) -> dict[str, Any]:
    """A read tool that makes an HTTP call inside it: recorded as an ordinary exchange (#76)."""
    url = agentkit.internal("enrich.internal") + f"/v1/companies/{account_id}"
    resp = agentkit.http("GET", url, label="enrichment")
    return resp.json() or {}


@sdk.tool(kind="read", name="crm.connect_with")
def connect_with(dsn: str) -> dict[str, Any]:
    """Its argument carries a credential, so a stored tool call must redact it (#69)."""
    return {"connected": dsn.rpartition("@")[2]}


@sdk.tool(kind="read", name="crm.score_account")
def score_account(account: dict[str, Any]) -> dict[str, Any]:
    """Returns values JSON cannot hold: a recorded result is not revivable, so replay (#83) must
    run this for real."""
    if account["id"] == "acc_missing":
        raise KeyError(account["id"])
    return {"score": Decimal("0.87"), "scored_at": datetime(2026, 9, 28, tzinfo=UTC)}


@sdk.tool(kind="read", name="crm.flaky_lookup")
def flaky_lookup(account_id: str) -> dict[str, Any]:
    raise KeyError(account_id)


@sdk.tool(kind="read", name="crm.lookup_owner")
async def lookup_owner(account_id: str) -> dict[str, Any]:
    """An async read tool, awaited in the run's event loop (#76)."""
    with agentkit.sqlite(DB_NAME) as conn:
        row = conn.execute("SELECT name FROM accounts WHERE id = ?", (account_id,)).fetchone()
    await asyncio.sleep(0)
    return {"id": account_id, "owner": f"owner@{row[0].lower()}.example" if row else None}


# -- write tools: the stand-in runs under shadow, never the real body -----------------------------


def _upsert_stand_in(account: dict[str, Any]) -> dict[str, Any]:
    return {"stood_in": True, "id": account["id"]}


@sdk.tool(kind="write", shadow=_upsert_stand_in, name="crm.upsert_account")
def upsert_account(account: dict[str, Any]) -> dict[str, Any]:
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO accounts VALUES (:id, :name, :segment, :last_seen, :score,"
            " :industry)",
            account,
        )
    return {"upserted": account["id"]}


def _delete_stand_in(cutoff: str) -> int:
    return 0


@sdk.tool(kind="write", shadow=_delete_stand_in, name="crm.delete_stale")
def delete_stale(cutoff: str) -> int:
    with agentkit.sqlite(DB_NAME) as conn:
        return conn.execute("DELETE FROM accounts WHERE last_seen < ?", (cutoff,)).rowcount


def _bulk_stand_in(scores: dict[str, int]) -> int:
    return len(scores)


@sdk.tool(kind="write", shadow=_bulk_stand_in, name="crm.bulk_update")
def bulk_update(scores: dict[str, int]) -> int:
    """One transaction: all rows or none."""
    with agentkit.sqlite(DB_NAME) as conn:
        conn.executemany(
            "UPDATE accounts SET score = ? WHERE id = ?", [(s, i) for i, s in scores.items()]
        )
    return len(scores)


def _export_stand_in(rows: list[dict[str, Any]], name: str) -> str:
    return f"<not written: {name}.csv>"


@sdk.tool(kind="write", shadow=_export_stand_in, name="crm.export_csv")
def export_csv(rows: list[dict[str, Any]], name: str) -> str:
    path = agentkit.state_dir() / "exports" / f"{name}.csv"
    path.parent.mkdir(exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


async def _notify_stand_in(account_id: str, message: str) -> dict[str, Any]:
    return {"stood_in": True}


@sdk.tool(kind="write", shadow=_notify_stand_in, name="crm.notify_owner")
async def notify_owner(account_id: str, message: str) -> dict[str, Any]:
    outbox = agentkit.state_dir() / "outbox.jsonl"
    with outbox.open("a") as fh:
        fh.write(json.dumps({"account": account_id, "message": message}) + "\n")
    return {"queued": account_id}


# `overlapping_writes`' two events: the archive has started, and the flag has ended (#79).
_STARTED: asyncio.Event | None = None
_RELEASE: asyncio.Event | None = None


async def _wait_for_the_flag() -> None:
    """The archive's body, real or stood in, says it started and waits until the flag has ended."""
    assert _STARTED is not None and _RELEASE is not None
    _STARTED.set()
    await _RELEASE.wait()


async def _archive_stand_in(account_id: str) -> dict[str, Any]:
    await _wait_for_the_flag()
    return {"archived": False, "stood_in": True}


@sdk.tool(kind="write", shadow=_archive_stand_in, name="crm.archive_account")
async def archive_account(account_id: str) -> dict[str, Any]:
    await _wait_for_the_flag()
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute("UPDATE accounts SET segment = 'archived' WHERE id = ?", (account_id,))
    return {"archived": True}


async def _flag_stand_in(account_id: str) -> dict[str, Any]:
    return {"flagged": False, "stood_in": True}


@sdk.tool(kind="write", shadow=_flag_stand_in, name="crm.flag_account")
async def flag_account(account_id: str) -> dict[str, Any]:
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute("UPDATE accounts SET score = -1 WHERE id = ?", (account_id,))
    return {"flagged": True}


async def _overlapping(account_id: str) -> dict[str, Any]:
    """Two writes at once: the archive starts first and ends only once the flag has ended, its
    call recorded, so the two end in the other order from the one they started in (#79)."""
    global _STARTED, _RELEASE
    _STARTED, _RELEASE = asyncio.Event(), asyncio.Event()
    archive = asyncio.ensure_future(archive_account(account_id))
    await _STARTED.wait()
    flagged = await flag_account(account_id)
    _RELEASE.set()
    return {"flagged": flagged, "archived": await archive}


# Every exception a merge body raised, in order: the agent checks that what it caught is the very
# object the body raised, which the SDK re-raises after recording it (#76).
RAISED: list[BaseException] = []


def _missing(*ids: str) -> list[str]:
    """Which of `ids` the CRM does not hold. A read, so a stand-in may make it too."""
    with agentkit.sqlite(DB_NAME) as conn:
        marks = ", ".join("?" * len(ids))
        found = {r[0] for r in conn.execute(f"SELECT id FROM accounts WHERE id IN ({marks})", ids)}
    return sorted(set(ids) - found)


def _raise_missing(missing: list[str]) -> None:
    exc = LookupError(f"no such account: {', '.join(missing)}")
    RAISED.append(exc)
    raise exc


def _merge_stand_in(*ids: str) -> dict[str, Any]:
    """Validates as the real merge does, against the real CRM, and changes nothing. Its parameters
    are not the real function's: irimi records the args bound against the real one (#76)."""
    keep, drop = ids
    if missing := _missing(keep, drop):
        _raise_missing(missing)
    return {"kept": keep, "merged": drop, "stood_in": True}


@sdk.tool(kind="write", shadow=_merge_stand_in, name="crm.merge_accounts")
def merge_accounts(keep: str, drop: str) -> dict[str, Any]:
    if missing := _missing(keep, drop):
        _raise_missing(missing)
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute("DELETE FROM accounts WHERE id = ?", (drop,))
    return {"kept": keep, "merged": drop}


def _sync_industry_stand_in(account_id: str, industry: str) -> dict[str, Any]:
    """Checks the account upstream before saying what it would have written: HTTP made inside a
    write's stand-in is an ordinary exchange of the run (#76)."""
    url = agentkit.internal("enrich.internal") + f"/v1/companies/{account_id}"
    upstream = (agentkit.http("GET", url, label="stand_in_check").json() or {}).get("industry")
    return {"stood_in": True, "industry": industry, "upstream": upstream}


@sdk.tool(kind="write", shadow=_sync_industry_stand_in, name="crm.sync_industry")
def sync_industry(account_id: str, industry: str) -> dict[str, Any]:
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute("UPDATE accounts SET industry = ? WHERE id = ?", (industry, account_id))
    return {"synced": account_id, "industry": industry}


async def _followup(account_id: str, days: int, path: str | None) -> dict[str, Any]:
    owner = (await lookup_owner(account_id))["owner"]
    if path is None:
        return {"stood_in": True, "owner": owner, "in_days": days}
    with (agentkit.state_dir() / path).open("a") as fh:
        fh.write(json.dumps({"account": account_id, "owner": owner, "in_days": days}) + "\n")
    return {"scheduled": account_id, "owner": owner, "in_days": days}


def _schedule_stand_in(account_id: str, days: int) -> Any:
    return _followup(account_id, days, None)


@sdk.tool(kind="write", shadow=_schedule_stand_in, name="crm.schedule_followup")
def schedule_followup(account_id: str, days: int) -> Any:
    """A sync write that returns its work as a coroutine, as a plain decorator over an `async def`
    does. Its body has not run when it returns, so the call ends when the coroutine does (#76): the
    read tool the coroutine awaits is recorded first."""
    return _followup(account_id, days, "followups.jsonl")


class Outbox:
    """Tools as a class's static methods, written as a developer groups them, the stand-in beside
    the tool in the class body (#76): a `staticmethod` stand-in is judged, and called, as the
    function inside it, so an async one stands in for an async tool."""

    @staticmethod
    async def _send_stand_in(account_id: str, owner: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"stood_in": True, "to": owner}

    @sdk.tool(kind="write", shadow=_send_stand_in, name="crm.send_reminder")
    @staticmethod
    async def send_reminder(account_id: str, owner: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        with (agentkit.state_dir() / "outbox.jsonl").open("a") as fh:
            fh.write(json.dumps({"account": account_id, "to": owner}) + "\n")
        return {"sent": account_id, "to": owner}


def _touch_last_seen(account_id: str) -> None:
    """A write with no label. irimi cannot see it, so it happens under shadow too."""
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute("UPDATE accounts SET last_seen = '2026-09-28' WHERE id = ?", (account_id,))


# -- the trigger ----------------------------------------------------------------------------------


@sdk.trigger(name="enrich_accounts")
def enrich_accounts(scenario: str, segment: str) -> dict[str, Any]:
    if scenario == "read_after_write":
        before = find_accounts(segment)[0]
        upsert_account({**before, "name": before["name"] + " Renamed"})
        after = find_accounts(segment)[0]
        # No tool overlay exists: under shadow the stand-in wrote nothing, so the read tool shows
        # the old row. This is the gap, pinned on purpose.
        return {"saw_rename": after["name"].endswith(" Renamed")}
    if scenario == "unlabeled_write":
        _touch_last_seen("acc_1")
        return {"touched": "acc_1"}
    if scenario == "merge_retry":
        # The first merge names an account that does not exist; the agent catches what the tool
        # raised, the same object its body raised, and retries with the right one (#76).
        for keep, drop in (("acc_1", "acc_9"), ("acc_1", "acc_2")):
            try:
                merged = merge_accounts(keep, drop)
            except LookupError as exc:
                agentkit.obs("merge_failed", error=str(exc), same_object=exc is RAISED[-1])
                continue
            return {"merged": merged}
        return {"merged": None}
    if scenario == "merge_once":
        # `merge_retry`'s second merge, made right the first time: the run `irimi compare` holds
        # the retry against (#79).
        return {"merged": merge_accounts("acc_1", "acc_2")}
    if scenario == "outside_run":
        account = find_accounts(segment)[0]
        return {"upserted": upsert_account({**account, "score": 50})}
    if scenario == "stand_in_http":
        return {"synced": sync_industry("acc_1", "aerospace")}
    if scenario == "async_tools":
        return asyncio.run(_follow_up_on("acc_1"))
    if scenario == "overlapping_writes":
        return asyncio.run(_overlapping("acc_1"))
    if scenario == "tool_raises":
        try:
            flaky_lookup("acc_1")
        except KeyError as exc:
            agentkit.obs("lookup_failed", error=type(exc).__name__, key=str(exc))
        # Uncaught: the run ends in error.
        score_account({"id": "acc_missing"})
        return {}
    connect_with(f"postgres://crm:{agentkit.key('STRIPE_API_KEY')}@db.internal/crm")
    accounts = find_accounts(segment)
    if not accounts:
        return {"accounts": 0}
    scores = {}
    for account in accounts:
        extra = fetch_enrichment(account["id"])
        scored = score_account(account)
        agentkit.obs(
            "scored", id=account["id"], types=sorted(type(v).__name__ for v in scored.values())
        )
        upsert_account({**account, "industry": extra.get("industry")})
        scores[account["id"]] = round(float(scored["score"]) * 100)
    bulk_update(scores)
    deleted = delete_stale(STALE_BEFORE)
    exported = export_csv(accounts, segment)
    notified = asyncio.run(notify_owner(accounts[0]["id"], "enriched"))
    return {
        "accounts": len(accounts),
        "deleted": deleted,
        "exported": exported,
        "notified": notified,
    }


async def _follow_up_on(account_id: str) -> dict[str, Any]:
    owner = await lookup_owner(account_id)
    scheduled = await schedule_followup(account_id, 7)
    reminded = await Outbox.send_reminder(account_id, owner["owner"])
    return {"owner": owner["owner"], "scheduled": scheduled, "reminded": reminded}


def decoration_errors() -> list[str]:
    """Each of #76's decoration-time mistakes, which must raise TypeError before any call."""

    def real(x: int) -> int:
        return x

    async def real_async(x: int) -> int:
        return x

    def gen(x: int) -> Any:
        yield x

    not_callable: Any = "mock_database_write"
    mistakes = {
        "write_without_shadow": lambda: sdk.tool(kind="write")(real),
        "read_with_shadow": lambda: sdk.tool(kind="read", shadow=real)(real),
        "bad_kind": lambda: sdk.tool(kind="delete", shadow=real)(real),
        "stand_in_not_callable": lambda: sdk.tool(kind="write", shadow=not_callable)(real),
        "sync_stand_in_for_async": lambda: sdk.tool(kind="write", shadow=real)(real_async),
        "async_stand_in_for_sync": lambda: sdk.tool(kind="write", shadow=real_async)(real),
        "generator": lambda: sdk.tool(kind="read")(gen),
        # A stand-in that is a generator, or the function itself (#76): the second would run the
        # real write under irimi.
        "generator_stand_in": lambda: sdk.tool(kind="write", shadow=gen)(real),
        "stand_in_is_the_function": lambda: sdk.tool(kind="write", shadow=real)(real),
    }
    raised = []
    for case, attempt in mistakes.items():
        try:
            attempt()
        except TypeError as exc:
            agentkit.obs("decoration_error", case=case, message=str(exc))
            raised.append(case)
    return raised


def main(argv: list[str]) -> int:
    agentkit.start()
    if not 1 <= len(argv) <= 2 or argv[0] not in SCENARIOS:
        print(f"usage: agent.py {{{','.join(SCENARIOS)}}} [segment]", file=sys.stderr)
        return 2
    scenario = argv[0]
    segment = argv[1] if len(argv) > 1 else "smb"
    seed()
    if scenario == "decoration_errors":
        agentkit.obs("result", raised=decoration_errors())
        return 0
    if scenario == "outside_run":
        # Housekeeping before the trigger, in no run: under `irimi shadow` each call is recorded in
        # the process run `IRIMI_RUN` names (#76).
        agentkit.obs(
            "housekeeping", stale=delete_stale(STALE_BEFORE), smb=len(find_accounts("smb"))
        )
    try:
        outcome = enrich_accounts(scenario, segment)
    except KeyError as exc:
        agentkit.obs("result", error=type(exc).__name__)
        return 1
    agentkit.obs("result", **outcome)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
