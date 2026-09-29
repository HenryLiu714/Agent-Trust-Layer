"""W5 `dispute_responder`: a Stripe webhook consumer that answers disputes.

Stripe calls the agent at `POST /stripe/webhook`. The agent verifies `Stripe-Signature` and hands
the raw body to one run, `@sdk.trigger on_event(raw, signature)`: the raw bytes ARE the trigger's
argument, as they are in any webhook handler that verifies before it parses.

On `charge.dispute.created` it:

1. reads the dispute (`/v1/disputes/{id}`, a route the Stripe map does not name), the charge, the
   payment intent, whose description names the order and which Stripe returns with its
   `client_secret`, a credential, and the customer;
2. asks the model to draft evidence;
3. tags the customer (`POST /v1/customers/{id}` metadata) and alerts `#disputes` on Slack;
4. refunds the charge in full when the dispute is under $20, because fighting it costs more.

On `charge.refunded` it does nothing: a refund it issued comes back to it as an event, and an
idempotent consumer notes it and stops. Every event id is handled once, however often it arrives.

The agent delivers its own events over loopback, through no proxy: an inbound webhook is not the
agent's egress, and irimi must never see it.

    python -m examples.workflows.launch \\
        examples.workflows.w05_dispute_responder.agent <scenario>
"""

from __future__ import annotations

import hashlib
import hmac
import json
import sys
import threading
import time
from typing import Any

from examples.workflows import agentkit, sdk

DISPUTES_CHANNEL = "C0DISPUTES"
REFUND_UNDER = 2000  # minor units: disputes under $20 are refunded, not fought
BIG = "dp_BIG"
SMALL = "dp_SMALL"
SCENARIOS = ("over_threshold", "under_threshold", "cascade", "bad_signature", "replayed_event")

_handled: set[str] = set()
_lock = threading.Lock()
_refunds: list[dict[str, Any]] = []


@sdk.trigger
def on_event(raw: bytes, signature: str) -> str:
    event = json.loads(raw)
    with _lock:
        if event["id"] in _handled:
            agentkit.obs("event", id=event["id"], type=event["type"], action="already handled")
            return "duplicate"
        _handled.add(event["id"])
    if event["type"] == "charge.dispute.created":
        return _respond(event["data"]["object"]["id"])
    agentkit.obs("event", id=event["id"], type=event["type"], action="noted")
    return "noted"


def _respond(dispute_id: str) -> str:
    dispute = agentkit.stripe("GET", f"/v1/disputes/{dispute_id}", label="dispute").json()
    charge = agentkit.stripe("GET", f"/v1/charges/{dispute['charge']}", label="charge").json()
    intent = agentkit.stripe(
        "GET", f"/v1/payment_intents/{dispute['payment_intent']}", label="payment_intent"
    ).json()
    customer_id = charge["customer"]
    customer = agentkit.stripe("GET", f"/v1/customers/{customer_id}", label="customer").json()
    evidence = _draft_evidence(dispute, intent, customer)
    agentkit.stripe(
        "POST",
        f"/v1/customers/{customer_id}",
        {"metadata[dispute]": dispute_id, "metadata[evidence]": evidence[:120]},
        label="tag_customer",
    )
    amount = dispute["amount"]
    action = "refund" if amount < REFUND_UNDER else "fight"
    agentkit.slack(
        "chat.postMessage",
        channel=DISPUTES_CHANNEL,
        text=f"dispute {dispute_id} on {charge['id']} ({amount}): {action}",
    )
    if action == "refund":
        refund = agentkit.stripe(
            "POST",
            "/v1/refunds",
            {"charge": charge["id"], "reason": "requested_by_customer"},
            headers={"Idempotency-Key": f"dispute-{dispute_id}"},
            label="refund",
        )
        answer = refund.json() or {}
        agentkit.obs(
            "refund", status=refund.status, id=answer.get("id"), amount=answer.get("amount")
        )
        if refund.ok:
            _refunds.append(answer)
    agentkit.obs("event", id=dispute_id, type="charge.dispute.created", action=action)
    return action


def _draft_evidence(
    dispute: dict[str, Any], intent: dict[str, Any], customer: dict[str, Any]
) -> str:
    facts = (
        f"reason={dispute['reason']} order={intent.get('description')} customer={customer['email']}"
    )
    message = agentkit.anthropic(
        "Draft dispute evidence for a card network. Two sentences.",
        [{"role": "user", "content": facts}],
        max_tokens=512,
        label="llm",
    )
    return agentkit.text_of(message)


class _Webhook(agentkit.Handler):
    def do_POST(self) -> None:
        raw = self.rfile.read(int(self.headers.get("content-length") or 0))
        signature = self.headers.get("Stripe-Signature") or ""
        if self.path != "/stripe/webhook" or not _verified(raw, signature):
            agentkit.obs("rejected", reason="bad signature")
            self.reply(400, {"error": "bad signature"})
            return
        try:
            outcome = on_event(raw, signature)
        except Exception as exc:
            # A handler that raised answers 500, which Stripe retries and `main` counts as a
            # failure. Left to escape, it would drop the connection with no answer.
            agentkit.obs("handler_failed", error=f"{type(exc).__name__}: {exc}")
            self.reply(500, {"received": False})
            return
        self.reply(200, {"received": True, "outcome": outcome})


def _sign(raw: bytes, secret: str, t: int) -> str:
    mac = hmac.new(secret.encode(), f"{t}.".encode() + raw, hashlib.sha256).hexdigest()
    return f"t={t},v1={mac}"


def _verified(raw: bytes, header: str) -> bool:
    parts = dict(p.split("=", 1) for p in header.split(",") if "=" in p)
    t = parts.get("t", "")
    if not t.isdigit() or abs(time.time() - int(t)) > 300:
        return False
    expected = _sign(raw, agentkit.key("STRIPE_WEBHOOK_SECRET"), int(t))
    return hmac.compare_digest(expected, header)


def _deliver(port: int, event: dict[str, Any], secret: str | None = None) -> dict[str, Any]:
    """POST one event to our own server, signed the way Stripe signs it."""
    raw = json.dumps(event).encode()
    signature = _sign(raw, secret or agentkit.key("STRIPE_WEBHOOK_SECRET"), int(time.time()))
    resp = agentkit.deliver(port, "/stripe/webhook", raw, {"Stripe-Signature": signature})
    return {"status": resp.status, **resp.json()} if resp.ok else {"status": resp.status}


def dispute_created(dispute_id: str) -> dict[str, Any]:
    return {
        "id": f"evt_{dispute_id}",
        "type": "charge.dispute.created",
        "data": {"object": {"id": dispute_id, "object": "dispute"}},
    }


def charge_refunded(refund: dict[str, Any]) -> dict[str, Any]:
    """The event Stripe sends after a refund, built from the refund answer the agent got."""
    return {
        "id": f"evt_refunded_{refund['id']}",
        "type": "charge.refunded",
        "data": {"object": {"id": refund["charge"], "object": "charge", "refunds": [refund["id"]]}},
    }


def main(argv: list[str]) -> int:
    agentkit.start()
    if len(argv) != 1 or argv[0] not in SCENARIOS:
        print(f"usage: agent.py {{{','.join(SCENARIOS)}}}", file=sys.stderr)
        return 2
    scenario = argv[0]
    dispute = BIG if scenario == "over_threshold" else SMALL
    secret = "whsec_not_the_real_one" if scenario == "bad_signature" else None
    with agentkit.serve(_Webhook) as port:
        answers = [_deliver(port, dispute_created(dispute), secret)]
        if scenario == "replayed_event":
            answers.append(_deliver(port, dispute_created(dispute)))
        if scenario == "cascade":
            # In production Stripe would now send `charge.refunded` for the refund. Under shadow
            # the refund never happened, so that event never comes: this delivers the one it
            # would have been, built from irimi's answer, to prove the consumer would not loop.
            answers += [_deliver(port, charge_refunded(r)) for r in _refunds]
    agentkit.obs("result", scenario=scenario, answers=answers)
    # A forged event must be refused; every other delivery must be accepted.
    expected = 400 if scenario == "bad_signature" else 200
    return 0 if all(a["status"] == expected for a in answers) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
