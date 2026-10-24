"""W1 `ticket_triage`: a helpdesk ticket webhook answered by a multi-turn Anthropic tool loop.

Tools: a Stripe read and refund, a SQLite read tool and write tool. `PROMPT` picks the prompt."""

from __future__ import annotations

import json
import os
import sys
import uuid
from dataclasses import asdict, dataclass
from typing import Any

from tests.e2e.workflows.agents import agentkit, observe

MAX_TURNS = 6
# Who is asking, new in every process: in the model call's `metadata`, never in what it asks.
SESSION = {"user_id": f"helpdesk-{uuid.uuid4().hex}"}
DB_NAME = "helpdesk.sqlite3"
# Set for a run whose helpdesk database is gone, as a replay's may be: the agent seeds nothing,
# so a real `lookup_customer` raises (`no such table`).
DB_LOST_ENV = "HELPDESK_DB_LOST"
# Set to replay an edit to the agent's code: every tool result carries the tag, so only the first
# model call, which holds no tool result, matches its recording, and the replay diverges mid-loop.
RESULT_TAG_ENV = "TOOL_RESULT_TAG"
PROMPTS = {
    "full": "You are a refund agent. Refund the full remaining amount of a disputed charge.",
    "half": "You are a refund agent. Refund half of the remaining amount of a disputed charge.",
    "too_large": "You are a refund agent. Refund double the charge amount as goodwill.",
    "double": "You are a refund agent. Refund in full, then refund again to be sure.",
    "retry": "You are a refund agent. Refund in full, and send the refund twice in case one drops.",
    "runaway": "You are a refund agent. Keep checking the charge until it changes.",
    "verify": "You are a refund agent. Refund in full; check the charge again before replying.",
    "verify_only": "You are a refund agent. Do not refund; check the charge again before replying.",
    "late_lookup": "You are a refund agent. Refund in full; only then look the customer up, and "
    "check the charge again before replying.",
}
TOOLS = [
    {"name": "get_charge", "input_schema": {"type": "object"}},
    {"name": "lookup_customer", "input_schema": {"type": "object"}},
    {"name": "issue_refund", "input_schema": {"type": "object"}},
    {"name": "send_reply", "input_schema": {"type": "object"}},
]


@dataclass(frozen=True)
class Ticket:
    ticket_id: str
    charge: str
    customer: str
    message: str


def seed(customer: str) -> None:
    """Seed the helpdesk database; this is setup, not a tool, so it happens in every mode."""
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS customers (id TEXT PRIMARY KEY, name TEXT, tier TEXT)"
        )
        conn.execute("CREATE TABLE IF NOT EXISTS replies (ticket_id TEXT, body TEXT)")
        conn.execute("INSERT OR IGNORE INTO customers VALUES (?, ?, ?)", (customer, "Ada", "gold"))


@observe.read_tool
def lookup_customer(customer: str) -> dict[str, Any]:
    with agentkit.sqlite(DB_NAME) as conn:
        row = conn.execute(
            "SELECT id, name, tier FROM customers WHERE id = ?", (customer,)
        ).fetchone()
    return {"id": row[0], "name": row[1], "tier": row[2]} if row else {"error": "no such customer"}


def _reply_stand_in(ticket_id: str, body: str) -> dict[str, Any]:
    return {"sent": True, "stood_in": True}


@observe.write_tool(fake=_reply_stand_in)
def send_reply(ticket_id: str, body: str) -> dict[str, Any]:
    with agentkit.sqlite(DB_NAME) as conn:
        conn.execute("INSERT INTO replies VALUES (?, ?)", (ticket_id, body))
    return {"sent": True}


def _tool_result(tool: str, **fields: Any) -> str:
    tag = os.environ.get(RESULT_TAG_ENV)
    return json.dumps({"tool": tool, **fields, **({"tag": tag} if tag else {})})


def run_tool(ticket: Ticket, turn: int, name: str, args: dict[str, Any]) -> str:
    if name == "get_charge":
        resp = agentkit.stripe("GET", f"/v1/charges/{args['charge']}", label=f"get_charge#{turn}")
        return _tool_result(name, status=resp.status, body=resp.json())
    if name == "lookup_customer":
        return _tool_result(name, body=lookup_customer(args["customer"]))
    if name == "issue_refund":
        # One key per turn: a call the model repeats within a turn is a retry of the same refund,
        # and a refund in a later turn is a new request with a new key.
        headers = {"Idempotency-Key": f"{ticket.ticket_id}-turn{turn}"}
        form = {"charge": args["charge"], "amount": str(args["amount"])}
        resp = agentkit.stripe(
            "POST", "/v1/refunds", form, headers=headers, label=f"issue_refund#{turn}"
        )
        body = resp.json()
        agentkit.obs(
            "refund",
            turn=turn,
            status=resp.status,
            id=body.get("id") if isinstance(body, dict) else None,
            replayed=resp.headers.get("idempotent-replayed"),
        )
        return _tool_result(name, status=resp.status, body=body)
    if name == "send_reply":
        return _tool_result(name, body=send_reply(ticket.ticket_id, args["body"]))
    return _tool_result(name, error="unknown tool")


@observe.trigger
def handle_ticket(ticket: Ticket) -> dict[str, Any]:
    system = PROMPTS[os.environ.get("PROMPT", "full")]
    messages: list[dict[str, Any]] = [
        {"role": "user", "content": json.dumps(asdict(ticket))},
    ]
    refunds: list[int] = []
    for turn in range(1, MAX_TURNS + 1):
        reply = agentkit.anthropic(
            system, messages, tools=TOOLS, label=f"llm#{turn}", metadata=SESSION
        )
        messages.append({"role": "assistant", "content": reply["content"]})
        if reply["stop_reason"] != "tool_use":
            return {"turns": turn, "refund_statuses": refunds, "final": agentkit.text_of(reply)}
        results = []
        for block in reply["content"]:
            if block["type"] != "tool_use":
                continue
            out = run_tool(ticket, turn, block["name"], block["input"])
            if block["name"] == "issue_refund":
                refunds.append(json.loads(out)["status"])
            results.append({"type": "tool_result", "tool_use_id": block["id"], "content": out})
        messages.append({"role": "user", "content": results})
    raise RuntimeError(f"ticket {ticket.ticket_id}: no answer after {MAX_TURNS} turns")


class _Helpdesk(agentkit.Handler):
    def do_POST(self) -> None:
        body = json.loads(self.rfile.read(int(self.headers["content-length"])))
        ticket = Ticket(
            body["ticket"]["id"],
            body["ticket"]["charge"],
            body["requester"],
            body["ticket"]["description"],
        )
        try:
            outcome = handle_ticket(ticket)
            status, payload = 200, {"ok": True, **outcome}
        except Exception as exc:
            status, payload = 500, {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        self.reply(status, payload)


def main(argv: list[str]) -> int:
    agentkit.start()
    if len(argv) != 4:
        print("usage: agent.py <ticket_id> <charge> <customer> <message>", file=sys.stderr)
        return 2
    ticket_id, charge, customer, message = argv
    if not os.environ.get(DB_LOST_ENV):
        seed(customer)
    # A Zendesk-shaped webhook, delivered to our own handler.
    event = {
        "ticket": {"id": ticket_id, "charge": charge, "description": message},
        "requester": customer,
    }
    with agentkit.serve(_Helpdesk) as port:
        answer = agentkit.deliver(port, "/tickets", json.dumps(event).encode()).json()
    agentkit.obs("result", **answer)
    return 0 if answer["ok"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
