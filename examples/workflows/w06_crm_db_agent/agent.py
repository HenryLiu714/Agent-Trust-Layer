"""W6 `crm_db_agent`: tools the proxy cannot see, over a local SQLite CRM.

The Phase 3 page's feature 3: "function database_write() gets labeled, the label specifies
mock_database_write() to be called using the same parameters during mock runs". Every database
and filesystem write here is an `@sdk.tool(kind="write", shadow=...)`, so under `irimi shadow` the
stand-in runs and the real body never does. Reads are `@sdk.tool(kind="read")` and run for real.

    python -m examples.workflows.w06_crm_db_agent.agent <scenario> [segment]

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
STALE_BEFORE = "2025-06-01"
SEED = [
    ("acc_1", "Acme", "smb", "2026-01-01", 0, None),
    ("acc_2", "Globex", "smb", "2025-01-01", 0, None),
    ("acc_3", "Initech", "enterprise", "2026-06-01", 0, None),
]


def db() -> sqlite3.Connection:
    return sqlite3.connect(agentkit.state_dir() / DB_NAME)


def seed() -> None:
    if (agentkit.state_dir() / DB_NAME).exists():
        return
    with db() as conn:
        conn.execute(
            "CREATE TABLE accounts (id TEXT PRIMARY KEY, name TEXT, segment TEXT,"
            " last_seen TEXT, score INTEGER, industry TEXT)"
        )
        conn.executemany("INSERT INTO accounts VALUES (?, ?, ?, ?, ?, ?)", SEED)


# -- read tools: run for real, shadow or not ------------------------------------------------------


@sdk.tool(kind="read", name="crm.find_accounts")
def find_accounts(segment: str) -> list[dict[str, Any]]:
    with db() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM accounts WHERE segment = ? ORDER BY id", (segment,))
        return [dict(r) for r in rows]


@sdk.tool(kind="read", name="crm.fetch_enrichment")
def fetch_enrichment(account_id: str) -> dict[str, Any]:
    """A read tool that makes an HTTP call inside it: recorded as an ordinary exchange (#76)."""
    enrich = agentkit.base("stripe").replace("api.stripe.com", "enrich.internal")
    resp = agentkit.http("GET", f"{enrich}/v1/companies/{account_id}", label="enrichment")
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


# -- write tools: the stand-in runs under shadow, never the real body -----------------------------


def _upsert_stand_in(account: dict[str, Any]) -> dict[str, Any]:
    return {"stood_in": True, "id": account["id"]}


@sdk.tool(kind="write", shadow=_upsert_stand_in, name="crm.upsert_account")
def upsert_account(account: dict[str, Any]) -> dict[str, Any]:
    with db() as conn:
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
    with db() as conn:
        return conn.execute("DELETE FROM accounts WHERE last_seen < ?", (cutoff,)).rowcount


def _bulk_stand_in(scores: dict[str, int]) -> int:
    return len(scores)


@sdk.tool(kind="write", shadow=_bulk_stand_in, name="crm.bulk_update")
def bulk_update(scores: dict[str, int]) -> int:
    """One transaction: all rows or none."""
    with db() as conn:
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


def _touch_last_seen(account_id: str) -> None:
    """A write with no label. irimi cannot see it, so it happens under shadow too."""
    with db() as conn:
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


def decoration_errors() -> list[str]:
    """Each of #76's decoration-time mistakes, which must raise TypeError before any call."""

    def real(x: int) -> int:
        return x

    async def real_async(x: int) -> int:
        return x

    def gen(x: int) -> Any:
        yield x

    mistakes = {
        "write_without_shadow": lambda: sdk.tool(kind="write")(real),
        "read_with_shadow": lambda: sdk.tool(kind="read", shadow=real)(real),
        "bad_kind": lambda: sdk.tool(kind="delete", shadow=real)(real),
        "sync_stand_in_for_async": lambda: sdk.tool(kind="write", shadow=real)(real_async),
        "generator": lambda: sdk.tool(kind="read")(gen),
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
    scenario = argv[0] if argv else "enrich"
    segment = argv[1] if len(argv) > 1 else "smb"
    seed()
    if scenario == "decoration_errors":
        agentkit.obs("result", raised=decoration_errors())
        return 0
    try:
        outcome = enrich_accounts(scenario, segment)
    except KeyError as exc:
        agentkit.obs("result", error=type(exc).__name__)
        return 1
    agentkit.obs("result", **outcome)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
