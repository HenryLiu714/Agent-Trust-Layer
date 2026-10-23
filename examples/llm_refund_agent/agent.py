"""An LLM refund agent: record a run, edit the prompt, replay it.

A support ticket comes in; `handle_ticket`, marked `@sdk.trigger`, decides what to refund:

1. `lookup_customer(charge)`, an `@sdk.read_tool` over the agent's own SQLite, which the
   proxy cannot see;
2. `GET /v1/charges/{charge}` from Stripe, a live read under irimi;
3. `POST /v1/messages` to Anthropic, the system prompt read from `PROMPT_FILE`, the ticket, the
   customer and the charge in the user turn, asking for `{"refund_amount": int, "reply": str}`;
4. `POST /v1/refunds` for that amount, when it is above zero, a write irimi fakes;
5. `record_decision(ticket_id, decision)`, an `@sdk.write_tool` over the same SQLite,
   whose stand-in runs under irimi instead.

So the prompt decides the refund: edit it, replay a recorded run (`irimi replay`), and see where
the replay diverged and which writes changed.

Stripe and Anthropic are called with stdlib `urllib.request`, as `examples/server_agent` calls
Stripe: it honours `HTTPS_PROXY` and `SSL_CERT_FILE` with no code, so the agent goes through
irimi by the environment alone. It sends a Stripe key only when it is a test-mode one (`sk_test_`):
the check is on the request path, so a replay, which calls `handle_ticket` without `main()`, makes
it too.

    python -m examples.llm_refund_agent.agent <ticket_id> <charge> <message>

Started with `python -m`, its runs record an entrypoint `irimi replay` can import. `main()`
sets nothing up that `handle_ticket` needs, since a replay calls the trigger alone; the database
is seeded beforehand (`python -m examples.llm_refund_agent.seed`).

Environment: `STRIPE_API_KEY` (`sk_test_` only), `STRIPE_API_BASE` (default
https://api.stripe.com), `ANTHROPIC_API_KEY`, `ANTHROPIC_API_BASE` (default
https://api.anthropic.com), `ANTHROPIC_MODEL` (default `claude-opus-5`), `PROMPT_FILE` (default
`prompt.txt` beside this file) and `REFUND_DB` (the SQLite file, default `llm_refunds.sqlite3`).
"""

import contextlib
import json
import os
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from irimi import sdk

# The environment the agent reads, and what it falls back to.
STRIPE_KEY_ENV = "STRIPE_API_KEY"
STRIPE_BASE_ENV = "STRIPE_API_BASE"
DEFAULT_STRIPE_BASE = "https://api.stripe.com"
ANTHROPIC_KEY_ENV = "ANTHROPIC_API_KEY"
ANTHROPIC_BASE_ENV = "ANTHROPIC_API_BASE"
DEFAULT_ANTHROPIC_BASE = "https://api.anthropic.com"
MODEL_ENV = "ANTHROPIC_MODEL"
DEFAULT_MODEL = "claude-opus-5"
PROMPT_FILE_ENV = "PROMPT_FILE"
DEFAULT_PROMPT = Path(__file__).with_name("prompt.txt")
DB_ENV = "REFUND_DB"
DEFAULT_DB = "llm_refunds.sqlite3"
ANTHROPIC_VERSION = "2023-06-01"
TEST_KEY_PREFIX = "sk_test_"
# Long enough for a model that thinks before it answers.
TIMEOUT_S = 120
# The two tables `seed` creates and the tools read and write.
SCHEMA = (
    "CREATE TABLE IF NOT EXISTS customers (charge TEXT PRIMARY KEY, name TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS decisions "
    "(ticket_id TEXT NOT NULL, refund_amount INTEGER NOT NULL, reply TEXT NOT NULL)",
)
# What the model is asked to answer with, as a JSON schema: Anthropic's structured outputs hold
# the reply to it (`output_config.format`).
DECISION_SCHEMA = {
    "type": "object",
    "properties": {
        "refund_amount": {"type": "integer"},
        "reply": {"type": "string"},
    },
    "required": ["refund_amount", "reply"],
    "additionalProperties": False,
}


@dataclass(frozen=True)
class Ticket:
    ticket_id: str
    charge: str
    message: str


def db_path() -> str:
    return os.environ.get(DB_ENV, DEFAULT_DB)


def _call(request: urllib.request.Request) -> dict[str, Any]:
    """One HTTP call, its JSON answer. An error status raises RuntimeError with the service's own
    error body, which says what was wrong where urllib's message names only the status."""
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_S) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        said = exc.read().decode(errors="replace")[:500]
        raise RuntimeError(
            f"{request.get_method()} {request.full_url}: {exc.code} {said}"
        ) from None


def stripe(method: str, path: str, form: dict[str, str] | None = None) -> dict[str, Any]:
    """One Stripe API call. Refuses, before sending anything, a key that is not test mode."""
    key = os.environ.get(STRIPE_KEY_ENV, "")
    if not key.startswith(TEST_KEY_PREFIX):
        raise ValueError(f"{STRIPE_KEY_ENV} must be a test-mode key ({TEST_KEY_PREFIX}...)")
    base = os.environ.get(STRIPE_BASE_ENV, DEFAULT_STRIPE_BASE).rstrip("/")
    data = None if form is None else urllib.parse.urlencode(form).encode()
    headers = {"Authorization": f"Bearer {key}"}
    return _call(urllib.request.Request(base + path, data=data, method=method, headers=headers))


def ask_model(system: str, content: str) -> dict[str, Any]:
    """One `POST /v1/messages`, and the decision its text block holds, checked against
    `DECISION_SCHEMA`'s two fields. ValueError for an answer that is not one. The model may think
    first: its `thinking` blocks come before the text and are passed over. A short decision, so
    `low` effort."""
    base = os.environ.get(ANTHROPIC_BASE_ENV, DEFAULT_ANTHROPIC_BASE).rstrip("/")
    body = {
        "model": os.environ.get(MODEL_ENV, DEFAULT_MODEL),
        "max_tokens": 16000,
        "system": system,
        "messages": [{"role": "user", "content": content}],
        "output_config": {
            "effort": "low",
            "format": {"type": "json_schema", "schema": DECISION_SCHEMA},
        },
    }
    headers = {
        "x-api-key": os.environ.get(ANTHROPIC_KEY_ENV, ""),
        "anthropic-version": ANTHROPIC_VERSION,
        "content-type": "application/json",
    }
    answer = _call(
        urllib.request.Request(
            base + "/v1/messages", data=json.dumps(body).encode(), method="POST", headers=headers
        )
    )
    if answer.get("stop_reason") in ("refusal", "max_tokens"):
        raise ValueError(f"the model stopped without a decision: {answer['stop_reason']}")
    text = next((b["text"] for b in answer.get("content", []) if b.get("type") == "text"), None)
    # Text that is not JSON is no decision either, and the error says what the model said.
    try:
        decision = json.loads(text) if isinstance(text, str) else None
    except json.JSONDecodeError:
        decision = None
    if not (
        isinstance(decision, dict)
        and type(decision.get("refund_amount")) is int
        and isinstance(decision.get("reply"), str)
    ):
        raise ValueError(f"the model did not answer with a decision: {text!r}")
    return {"refund_amount": decision["refund_amount"], "reply": decision["reply"]}


@sdk.read_tool
def lookup_customer(charge: str) -> dict[str, Any]:
    """The customer the agent's own database holds for `charge`. Opened read-only, so a database
    that is not there raises (`sqlite3.OperationalError`) rather than being created empty."""
    uri = f"file:{urllib.parse.quote(db_path())}?mode=ro"
    with contextlib.closing(sqlite3.connect(uri, uri=True)) as conn:
        row = conn.execute("SELECT name FROM customers WHERE charge = ?", (charge,)).fetchone()
    return {"charge": charge, "name": row[0]} if row else {"charge": charge, "name": None}


def record_decision_stand_in(ticket_id: str, decision: dict[str, Any]) -> dict[str, Any]:
    return {"recorded": False, "stood_in": True}


@sdk.write_tool(fake=record_decision_stand_in)
def record_decision(ticket_id: str, decision: dict[str, Any]) -> dict[str, Any]:
    """The agent's own record of what it decided. Under irimi it never runs: no row is written."""
    with contextlib.closing(sqlite3.connect(db_path())) as conn, conn:
        conn.execute(
            "INSERT INTO decisions (ticket_id, refund_amount, reply) VALUES (?, ?, ?)",
            (ticket_id, decision["refund_amount"], decision["reply"]),
        )
    return {"recorded": True}


@sdk.trigger
def handle_ticket(ticket: Ticket) -> str:
    """Decide the ticket, refund what the model decided, record it, and return the reply."""
    customer = lookup_customer(ticket.charge)
    charge = stripe("GET", f"/v1/charges/{urllib.parse.quote(ticket.charge, safe='')}")
    prompt = Path(os.environ.get(PROMPT_FILE_ENV) or DEFAULT_PROMPT).read_text(encoding="utf-8")
    facts = {
        "ticket": asdict(ticket),
        "customer": customer,
        "charge": {
            "id": charge["id"],
            "amount": charge["amount"],
            "amount_refunded": charge["amount_refunded"],
            "currency": charge["currency"],
        },
    }
    decision = ask_model(prompt, json.dumps(facts))
    if decision["refund_amount"] > 0:
        stripe(
            "POST",
            "/v1/refunds",
            {"charge": ticket.charge, "amount": str(decision["refund_amount"])},
        )
    record_decision(ticket.ticket_id, decision)
    return decision["reply"]


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(
            "usage: python -m examples.llm_refund_agent.agent TICKET_ID CHARGE MESSAGE",
            file=sys.stderr,
        )
        return 2
    print(handle_ticket(Ticket(*argv)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
