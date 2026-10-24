"""W6 `crm_db_agent`: read and write tools over a local SQLite CRM, which the proxy cannot see.

Each scenario puts one case of the tool API through `irimi shadow`; tools carry short names."""

from __future__ import annotations

import asyncio
import csv
import json
import sqlite3
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from irimi import sdk
from tests.e2e.workflows.agents import agentkit, observe

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
# A database host the agent cannot reach: `connect_with` raises for it as a driver does, quoting
# the DSN it was given, key and all.
UNREACHABLE_HOST = "db.unreachable.internal"
# What that driver says before the DSN: long enough that the key straddles the cut a trace makes
# of an error message (`irimi.core.limits.MAX_ERROR_MESSAGE`), and its placeholder ends before it.
CONNECT_RETRIES = "connection timed out, retrying; " * 29
# What a driver's options say before their password: long enough that the key straddles the SDK's
# repr cut (`irimi.sdk._capture.MAX_REPR`), leaving 15 characters for invariant 4 to find.
DRIVER_NOTE = "retry; " * 136


class DriverOptions:
    """A driver's options, whose repr prints the password, as many drivers' do."""

    def __init__(self, password: str) -> None:
        self.password = password

    def __repr__(self) -> str:
        return f"DriverOptions(note='{DRIVER_NOTE}', password='{self.password}')"


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


@observe.read_tool(name="crm.find_accounts")
def find_accounts(segment: str) -> list[dict[str, Any]]:
    with agentkit.sqlite(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM accounts WHERE segment = ? ORDER BY id", (segment,))
        return [dict(r) for r in rows]


@observe.read_tool(name="crm.fetch_enrichment")
def fetch_enrichment(account_id: str) -> dict[str, Any]:
    """Fetch enrichment over HTTP inside a read tool, recorded as an ordinary exchange."""
    url = agentkit.internal("enrich.internal") + f"/v1/companies/{account_id}"
    resp = agentkit.http("GET", url, label="enrichment")
    return resp.json() or {}


@observe.read_tool(name="crm.connect_with")
def connect_with(dsn: str, options: DriverOptions | None = None) -> dict[str, Any]:
    """Connect with a DSN holding a credential, which a stored tool call must redact.

    For an unreachable host it raises a message whose key straddles the trace's message cut."""
    host = dsn.rpartition("@")[2]
    if host.startswith(UNREACHABLE_HOST):
        raise ConnectionError(f"{CONNECT_RETRIES}could not connect with {dsn}")
    return {"connected": host}


@observe.read_tool(name="crm.score_account")
def score_account(account: dict[str, Any]) -> dict[str, Any]:
    """Return values JSON cannot hold, so a replay must run this for real."""
    if account["id"] == "acc_missing":
        raise KeyError(account["id"])
    return {"score": Decimal("0.87"), "scored_at": datetime(2026, 9, 28, tzinfo=UTC)}


@observe.read_tool(name="crm.flaky_lookup")
def flaky_lookup(account_id: str) -> dict[str, Any]:
    raise KeyError(account_id)


@observe.read_tool(name="crm.lookup_owner")
async def lookup_owner(account_id: str) -> dict[str, Any]:
    """Look the owner up in an async read tool, awaited in the run's event loop."""
    with agentkit.sqlite(DB_NAME) as conn:
        row = conn.execute("SELECT name FROM accounts WHERE id = ?", (account_id,)).fetchone()
    await asyncio.sleep(0)
    return {"id": account_id, "owner": f"owner@{row[0].lower()}.example" if row else None}


@observe.read_tool(name="crm.last_contact")
def last_contact(account_id: str) -> Any:
    """Return a coroutine from a sync read tool; a replay answers it with a coroutine too."""
    return _last_seen(account_id)


async def _last_seen(account_id: str) -> dict[str, Any]:
    await asyncio.sleep(0)
    with agentkit.sqlite(DB_NAME) as conn:
        row = conn.execute("SELECT last_seen FROM accounts WHERE id = ?", (account_id,)).fetchone()
    return {"id": account_id, "last_seen": row[0] if row else None}


# -- write tools: the fake runs under shadow, never the real body ---------------------------------


def _upsert_stand_in(account: dict[str, Any]) -> dict[str, Any]:
    return {"stood_in": True, "id": account["id"]}


@observe.write_tool(fake=_upsert_stand_in, name="crm.upsert_account")
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


@observe.write_tool(fake=_delete_stand_in, name="crm.delete_stale")
def delete_stale(cutoff: str) -> int:
    with agentkit.sqlite(DB_NAME) as conn:
        return conn.execute("DELETE FROM accounts WHERE last_seen < ?", (cutoff,)).rowcount


def _bulk_stand_in(scores: dict[str, int]) -> int:
    return len(scores)


@observe.write_tool(fake=_bulk_stand_in, name="crm.bulk_update")
def bulk_update(scores: dict[str, int]) -> int:
    """Update every score in one transaction: all rows or none."""
    with agentkit.sqlite(DB_NAME) as conn:
        conn.executemany(
            "UPDATE accounts SET score = ? WHERE id = ?", [(s, i) for i, s in scores.items()]
        )
    return len(scores)


def _export_stand_in(rows: list[dict[str, Any]], name: str) -> str:
    return f"<not written: {name}.csv>"


@observe.write_tool(fake=_export_stand_in, name="crm.export_csv")
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


@observe.write_tool(fake=_notify_stand_in, name="crm.notify_owner")
async def notify_owner(account_id: str, message: str) -> dict[str, Any]:
    outbox = agentkit.state_dir() / "outbox.jsonl"
    with outbox.open("a") as fh:
        fh.write(json.dumps({"account": account_id, "message": message}) + "\n")
    return {"queued": account_id}


# `overlapping_writes`' two events: the archive has started, and the flag has ended.
_STARTED: asyncio.Event | None = None
_RELEASE: asyncio.Event | None = None


async def _wait_for_the_flag() -> None:
    """Signal the archive started, then wait until the flag write has ended."""
    assert _STARTED is not None and _RELEASE is not None
    _STARTED.set()
    await _RELEASE.wait()


async def _archive_stand_in(account_id: str) -> dict[str, Any]:
    await _wait_for_the_flag()
    return {"archived": False, "stood_in": True}


@observe.write_tool(fake=_archive_stand_in, name="crm.archive_account")
async def archive_account(account_id: str) -> dict[str, Any]:
    await _wait_for_the_flag()
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute("UPDATE accounts SET segment = 'archived' WHERE id = ?", (account_id,))
    return {"archived": True}


async def _flag_stand_in(account_id: str) -> dict[str, Any]:
    return {"flagged": False, "stood_in": True}


@observe.write_tool(fake=_flag_stand_in, name="crm.flag_account")
async def flag_account(account_id: str) -> dict[str, Any]:
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute("UPDATE accounts SET score = -1 WHERE id = ?", (account_id,))
    return {"flagged": True}


async def _overlapping(account_id: str) -> dict[str, Any]:
    """Make two writes at once, the archive starting first and ending after the flag."""
    global _STARTED, _RELEASE
    _STARTED, _RELEASE = asyncio.Event(), asyncio.Event()
    archive = asyncio.ensure_future(archive_account(account_id))
    await _STARTED.wait()
    flagged = await flag_account(account_id)
    _RELEASE.set()
    return {"flagged": flagged, "archived": await archive}


# Every exception a merge body raised, so the agent can check the SDK re-raises the same object.
RAISED: list[BaseException] = []


def _missing(*ids: str) -> list[str]:
    """Return which of `ids` the CRM does not hold; a read, so a fake may make it too."""
    with agentkit.sqlite(DB_NAME) as conn:
        marks = ", ".join("?" * len(ids))  # placeholders only: the ids themselves are bound
        query = f"SELECT id FROM accounts WHERE id IN ({marks})"  # noqa: S608
        found = {r[0] for r in conn.execute(query, ids)}
    return sorted(set(ids) - found)


def _raise_missing(missing: list[str]) -> None:
    exc = LookupError(f"no such account: {', '.join(missing)}")
    RAISED.append(exc)
    raise exc


def _merge_stand_in(*ids: str) -> dict[str, Any]:
    """Validate as the real merge does and change nothing.

    Its parameters differ from the real function's; irimi records args bound to the real one."""
    keep, drop = ids
    if missing := _missing(keep, drop):
        _raise_missing(missing)
    return {"kept": keep, "merged": drop, "stood_in": True}


@observe.write_tool(fake=_merge_stand_in, name="crm.merge_accounts")
def merge_accounts(keep: str, drop: str) -> dict[str, Any]:
    if missing := _missing(keep, drop):
        _raise_missing(missing)
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute("DELETE FROM accounts WHERE id = ?", (drop,))
    return {"kept": keep, "merged": drop}


def _sync_industry_stand_in(account_id: str, industry: str) -> dict[str, Any]:
    """Check the account upstream over HTTP, an ordinary exchange of the run, and write nothing."""
    url = agentkit.internal("enrich.internal") + f"/v1/companies/{account_id}"
    upstream = (agentkit.http("GET", url, label="stand_in_check").json() or {}).get("industry")
    return {"stood_in": True, "industry": industry, "upstream": upstream}


@observe.write_tool(fake=_sync_industry_stand_in, name="crm.sync_industry")
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


@observe.write_tool(fake=_schedule_stand_in, name="crm.schedule_followup")
def schedule_followup(account_id: str, days: int) -> Any:
    """Return a coroutine from a sync write tool; the call ends when the coroutine does."""
    return _followup(account_id, days, "followups.jsonl")


class Outbox:
    """Tools as a class's static methods, with a `staticmethod` fake beside the tool."""

    @staticmethod
    async def _send_stand_in(account_id: str, owner: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"stood_in": True, "to": owner}

    @observe.write_tool(fake=_send_stand_in, name="crm.send_reminder")
    @staticmethod
    async def send_reminder(account_id: str, owner: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        with (agentkit.state_dir() / "outbox.jsonl").open("a") as fh:
            fh.write(json.dumps({"account": account_id, "to": owner}) + "\n")
        return {"sent": account_id, "to": owner}


def _touch_last_seen(account_id: str) -> None:
    """Write with no label, which irimi cannot see, so it happens under shadow too."""
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute("UPDATE accounts SET last_seen = '2026-09-28' WHERE id = ?", (account_id,))


# -- the trigger ----------------------------------------------------------------------------------


@observe.trigger(name="enrich_accounts")
def enrich_accounts(scenario: str, segment: str) -> dict[str, Any]:
    if scenario == "read_after_write":
        before = find_accounts(segment)[0]
        upsert_account({**before, "name": before["name"] + " Renamed"})
        # A write with no label, so real under shadow too: the two reads, made with the same args,
        # get different rows, and a replay that answered them out of order would say so.
        _touch_last_seen(before["id"])
        after = find_accounts(segment)[0]
        # No overlay applies to tools: under shadow the fake wrote nothing, so the read tool shows
        # the old row, a known gap pinned on purpose.
        return {
            "saw_rename": after["name"].endswith(" Renamed"),
            "last_seen": [before["last_seen"], after["last_seen"]],
        }
    if scenario == "unlabeled_write":
        _touch_last_seen("acc_1")
        return {"touched": "acc_1"}
    if scenario == "merge_retry":
        # The first merge names an account that does not exist; the agent catches what the tool
        # raised, the same object its body raised, and retries with the right one.
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
        # the retry against.
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
            connect_with(
                f"postgres://crm:{agentkit.key('STRIPE_API_KEY')}@{UNREACHABLE_HOST}/crm",
                DriverOptions(agentkit.key("OPENAI_API_KEY")),
            )
        except ConnectionError as exc:
            agentkit.obs("connect_failed", error=type(exc).__name__)
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
    # The find runs while `last_contact`'s coroutine is pending, inside that call's span; a replay
    # answering `last_contact` must still answer the find from its own record.
    pending = last_contact(account_id)
    segment = find_accounts("smb")
    contact = await pending
    scheduled = await schedule_followup(account_id, 7)
    reminded = await Outbox.send_reminder(account_id, owner["owner"])
    return {
        "owner": owner["owner"],
        "last_seen": contact["last_seen"],
        "segment": [account["id"] for account in segment],
        "scheduled": scheduled,
        "reminded": reminded,
    }


def decoration_errors() -> list[str]:
    """Return each decoration-time mistake that raised TypeError before any call."""

    def real(x: int) -> int:
        return x

    async def real_async(x: int) -> int:
        return x

    def gen(x: int) -> Any:
        yield x

    not_callable: Any = "mock_database_write"
    misused: Any = sdk  # each mistake below is a call the SDK's signature already refuses
    mistakes: dict[str, Callable[[], object]] = {
        "write_without_fake": lambda: misused.write_tool()(real),
        "read_with_fake": lambda: misused.read_tool(fake=real)(real),
        "bare_write_tool": lambda: misused.write_tool(real),
        "fake_not_callable": lambda: observe.write_tool(fake=not_callable)(real),
        "sync_fake_for_async": lambda: observe.write_tool(fake=real)(real_async),
        "async_fake_for_sync": lambda: observe.write_tool(fake=real_async)(real),
        "generator": lambda: observe.read_tool(gen),
        # A fake that is a generator, or the function itself: the second would run the real
        # write under irimi.
        "generator_fake": lambda: observe.write_tool(fake=gen)(real),
        "fake_is_the_function": lambda: observe.write_tool(fake=real)(real),
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
        # Housekeeping in no run: under `irimi shadow` it lands in the process run.
        agentkit.obs(
            "housekeeping", stale=delete_stale(STALE_BEFORE), smb=len(find_accounts("smb"))
        )
    try:
        with agentkit.lookup_gateway():
            outcome = enrich_accounts(scenario, segment)
    except KeyError as exc:
        agentkit.obs("result", error=type(exc).__name__)
        return 1
    agentkit.obs("result", **outcome)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
