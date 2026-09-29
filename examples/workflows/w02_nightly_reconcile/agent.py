"""W2 `nightly_reconcile`: a scheduled batch job, one run per invocation, many writes per run.

A cron entry starts it with a date. It pages through every Stripe charge, compares each with the
local SQLite ledger, and for each mismatch tags the Stripe customer (`metadata[reconciled]`) and
marks the ledger row reconciled. It ends with one Slack summary. This is use case 0 at volume: the
baseline report has to say what forty writes in one run would have done.

    python -m examples.workflows.launch \\
        examples.workflows.w02_nightly_reconcile.agent --date 2026-09-27 \\
        [--charges 45] [--mismatches 40] [--datetime-trigger] [--twice] [--refund-then-page]

The ledger is seeded by the agent itself before the run starts (`seed_ledger`), standing in for the
ledger the nightly job finds in production: one row per charge `ch_N000`..., the first
`--mismatches` of them off by one cent.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from typing import Any

from examples.workflows import agentkit, sdk
from examples.workflows.agentkit import stripe

CHANNEL = "C0RECON"
PAGE = 10
LEDGER = "ledger.db"


def charge_id(i: int) -> str:
    return f"ch_N{i:03d}"


def customer_id(i: int) -> str:
    return f"cus_N{i:03d}"


def seed_ledger(charges: int, mismatches: int) -> None:
    """The ledger as the job finds it. Not part of the run: setup, like a restored backup."""
    with agentkit.sqlite(LEDGER) as db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS ledger "
            "(charge TEXT PRIMARY KEY, amount INTEGER, reconciled_on TEXT)"
        )
        db.execute("DELETE FROM ledger")
        for i in range(charges):
            amount = 1000 + i + (1 if i < mismatches else 0)
            db.execute("INSERT INTO ledger VALUES (?, ?, NULL)", (charge_id(i), amount))


@sdk.tool(kind="read")
def load_ledger() -> dict[str, int]:
    with agentkit.sqlite(LEDGER) as db:
        return dict(db.execute("SELECT charge, amount FROM ledger").fetchall())


def _mark_stand_in(charge: str, date: str) -> dict[str, Any]:
    return {"stood_in": True, "charge": charge, "date": date}


@sdk.tool(kind="write", shadow=_mark_stand_in)
def mark_reconciled(charge: str, date: str) -> dict[str, Any]:
    with agentkit.sqlite(LEDGER) as db:
        db.execute("UPDATE ledger SET reconciled_on = ? WHERE charge = ?", (date, charge))
    return {"stood_in": False, "charge": charge, "date": date}


def all_charges() -> list[dict[str, Any]]:
    """Every charge, page by page, newest first, with `starting_after` as Stripe pages."""
    out: list[dict[str, Any]] = []
    after: str | None = None
    while True:
        query = f"?limit={PAGE}" + (f"&starting_after={after}" if after else "")
        resp = stripe("GET", "/v1/charges" + query, label="list_charges")
        page = resp.json()
        # A page that failed is not an empty page: reconciling a partial list would call every
        # charge after it clean. Fail the run instead.
        if not resp.ok or not isinstance(page, dict):
            raise RuntimeError(f"listing charges after {after}: HTTP {resp.status}")
        data = page.get("data") or []
        out += data
        if not page.get("has_more") or not data:
            return out
        after = data[-1]["id"]


def reconcile(date: str) -> int:
    ledger = load_ledger()
    mismatched = [c for c in all_charges() if ledger.get(c["id"]) != c["amount"]]
    for c in mismatched:
        tagged = stripe(
            "POST",
            f"/v1/customers/{c['customer']}",
            {"metadata[reconciled]": date, "metadata[charge]": c["id"]},
            label="tag_customer",
        )
        # The ledger says reconciled only once Stripe says tagged.
        if not tagged.ok:
            raise RuntimeError(f"tagging {c['customer']} for {c['id']}: HTTP {tagged.status}")
        mark_reconciled(c["id"], date)
    agentkit.obs("reconciled", date=date, mismatches=len(mismatched))
    return len(mismatched)


def refund_then_page(charge: str) -> None:
    """Refund a charge, then page its refunds from the refund just made: the cursor names an id
    this run minted, which the real Stripe has never seen (#53)."""
    refund = stripe("POST", "/v1/refunds", {"charge": charge, "amount": "100"}, label="refund")
    refund_id = (refund.json() or {}).get("id")
    first = stripe("GET", f"/v1/refunds?charge={charge}&limit=1", label="list_refunds")
    after = stripe(
        "GET",
        f"/v1/refunds?charge={charge}&limit=1&starting_after={refund_id}",
        label="page_refunds",
    )
    agentkit.obs(
        "refunds",
        minted=refund_id,
        first=[r["id"] for r in (first.json() or {}).get("data") or []],
        after=[r["id"] for r in (after.json() or {}).get("data") or []],
        after_status=after.status,
    )


def main(argv: list[str]) -> int:
    agentkit.start()
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2026-09-27")
    parser.add_argument("--charges", type=int, default=12)
    parser.add_argument("--mismatches", type=int, default=0)
    parser.add_argument("--datetime-trigger", action="store_true")
    parser.add_argument("--twice", action="store_true")
    parser.add_argument("--refund-then-page", action="store_true")
    args = parser.parse_args(argv)
    seed_ledger(args.charges, args.mismatches)

    # A scheduler hands over what it has. A `datetime` here is captured as `__irimi_repr__` under
    # #74's rules and makes the run non-replayable; an ISO string does not.
    when: Any = (
        dt.datetime.fromisoformat(args.date + "T02:00:00+00:00")
        if args.datetime_trigger
        else args.date
    )
    agentkit.obs("trigger", value=when, type=type(when).__name__)
    date = when.date().isoformat() if isinstance(when, dt.datetime) else when

    with sdk.run(name="nightly-reconcile", trigger={"date": when}):
        total = reconcile(date)
        if args.twice:
            total += reconcile(date)
        if args.refund_then_page:
            refund_then_page(charge_id(0))
        posted = agentkit.slack(
            "chat.postMessage", channel=CHANNEL, text=f"reconciled {total} for {date}"
        )
    agentkit.obs("result", total=total, slack_ok=posted.get("ok"))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
