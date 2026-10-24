"""An LLM refund agent: record a run, edit the prompt, replay it, and see which write changed.

`handle_ticket` looks the customer up in the agent's own SQLite (`db.lookup_customer`), reads the
charge from Stripe, asks Claude for a refund amount, refunds it, and records the decision
(`db.record_decision`), so the prompt decides the refund. Both SDKs reach irimi through the
environment it sets; `connect()` hands `HTTPS_PROXY` and `SSL_CERT_FILE` to stripe-python.

    python -m examples.replay.seed CHARGE CUSTOMER_NAME
    irimi shadow -- python -m examples.replay.agent TICKET_ID CHARGE MESSAGE

Environment: `STRIPE_API_KEY` (`sk_test_...` only), `STRIPE_API_BASE`, `ANTHROPIC_API_KEY`,
`ANTHROPIC_BASE_URL`, `ANTHROPIC_MODEL` (default `claude-opus-5-5`), `PROMPT_FILE` (default
`prompt.txt` beside this file) and `REFUND_DB` (default `llm_refunds.sqlite3`).
"""

import contextlib
import json
import os
import sqlite3
import sys
import urllib.parse
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import anthropic
import stripe

from irimi import sdk

DEFAULT_MODEL = "claude-opus-5-5"
DEFAULT_PROMPT = Path(__file__).with_name("prompt.txt")
DEFAULT_DB = "llm_refunds.sqlite3"
TEST_KEY_PREFIX = "sk_test_"
# The two tables `seed` creates and the tools read and write.
SCHEMA = (
    "CREATE TABLE IF NOT EXISTS customers (charge TEXT PRIMARY KEY, name TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS decisions "
    "(ticket_id TEXT NOT NULL, refund_amount INTEGER NOT NULL, reply TEXT NOT NULL)",
)
# What the model answers with; structured outputs hold its reply to this schema.
DECISION_SCHEMA = {
    "type": "object",
    "properties": {"refund_amount": {"type": "integer"}, "reply": {"type": "string"}},
    "required": ["refund_amount", "reply"],
    "additionalProperties": False,
}


@dataclass(frozen=True)
class Ticket:
    ticket_id: str
    charge: str
    message: str


def db_path() -> str:
    return os.environ.get("REFUND_DB", DEFAULT_DB)


def connect(env: Mapping[str, str]) -> None:
    """Set stripe-python up from `env`, refusing a key that is not test mode.

    Called on the trigger's path, so a replay, which calls `handle_ticket` alone, makes the check.
    """
    key = env.get("STRIPE_API_KEY", "")
    if not key.startswith(TEST_KEY_PREFIX):
        raise ValueError(f"STRIPE_API_KEY must be a test-mode key ({TEST_KEY_PREFIX}...)")
    stripe.api_key = key
    stripe.api_base = env.get("STRIPE_API_BASE", "https://api.stripe.com")
    if env.get("IRIMI_ENGINE_ACTIVE") == "1":
        # stripe-python checks certificates against the bundle it ships, never SSL_CERT_FILE.
        stripe.proxy = env["HTTPS_PROXY"]
        stripe.ca_bundle_path = env["SSL_CERT_FILE"]


def ask_model(system: str, content: str) -> dict[str, Any]:
    """Ask Claude for a decision, and check it is one. ValueError when it is not."""
    run_id = sdk.current_run_id()
    message = anthropic.Anthropic().messages.create(
        model=os.environ.get("ANTHROPIC_MODEL", DEFAULT_MODEL),
        max_tokens=16000,
        system=system,
        messages=[{"role": "user", "content": content}],
        output_config={
            "effort": "low",
            "format": {"type": "json_schema", "schema": DECISION_SCHEMA},
        },
        # irimi cannot label this client's requests with their run (it sends through httpx2), so
        # the call names its run. irimi takes the header off before forwarding.
        extra_headers={"Irimi-Run": run_id} if run_id else None,
    )
    if message.stop_reason in ("refusal", "max_tokens"):
        raise ValueError(f"the model stopped without a decision: {message.stop_reason}")
    text = next((block.text for block in message.content if block.type == "text"), None)
    try:
        decision = json.loads(text) if text is not None else None
    except json.JSONDecodeError:
        decision = None
    if not (
        isinstance(decision, dict)
        and type(decision.get("refund_amount")) is int
        and isinstance(decision.get("reply"), str)
    ):
        raise ValueError(f"the model did not answer with a decision: {text!r}")
    return {"refund_amount": decision["refund_amount"], "reply": decision["reply"]}


@sdk.read_tool(name="db.lookup_customer")
def lookup_customer(charge: str) -> dict[str, Any]:
    """Look up the customer the agent's database holds for `charge`.

    The database opens read-only, so a missing one raises rather than being created empty."""
    uri = f"file:{urllib.parse.quote(db_path())}?mode=ro"
    with contextlib.closing(sqlite3.connect(uri, uri=True)) as conn:
        row = conn.execute("SELECT name FROM customers WHERE charge = ?", (charge,)).fetchone()
    return {"charge": charge, "name": row[0] if row else None}


def record_decision_fake(ticket_id: str, decision: dict[str, Any]) -> dict[str, Any]:
    return {"recorded": False}


@sdk.write_tool(fake=record_decision_fake, name="db.record_decision")
def record_decision(ticket_id: str, decision: dict[str, Any]) -> dict[str, Any]:
    """Record what the agent decided. Under irimi the fake runs instead."""
    with contextlib.closing(sqlite3.connect(db_path())) as conn, conn:
        conn.execute(
            "INSERT INTO decisions (ticket_id, refund_amount, reply) VALUES (?, ?, ?)",
            (ticket_id, decision["refund_amount"], decision["reply"]),
        )
    return {"recorded": True}


@sdk.trigger
def handle_ticket(ticket: Ticket) -> str:
    """Decide the ticket, refund what the model decided, record it, and return the reply."""
    connect(os.environ)
    customer = lookup_customer(ticket.charge)
    charge = stripe.Charge.retrieve(ticket.charge)
    prompt = Path(os.environ.get("PROMPT_FILE") or DEFAULT_PROMPT).read_text(encoding="utf-8")
    facts = {
        "ticket": asdict(ticket),
        "customer": customer,
        "charge": {
            "id": charge.id,
            "amount": charge.amount,
            "amount_refunded": charge.amount_refunded,
            "currency": charge.currency,
        },
    }
    decision = ask_model(prompt, json.dumps(facts))
    if decision["refund_amount"] > 0:
        stripe.Refund.create(charge=ticket.charge, amount=decision["refund_amount"])
    record_decision(ticket.ticket_id, decision)
    reply: str = decision["reply"]
    return reply


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: python -m examples.replay.agent TICKET_ID CHARGE MESSAGE", file=sys.stderr)
        return 2
    print(handle_ticket(Ticket(*argv)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
