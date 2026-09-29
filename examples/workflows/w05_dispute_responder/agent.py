"""W5 `dispute_responder`: a Stripe webhook consumer that answers disputes.

Stripe calls the agent at `POST /stripe/webhook`. The agent verifies `Stripe-Signature` and hands
the raw body to one run, `@sdk.trigger on_event(raw, signature)`: the raw bytes ARE the trigger's
argument, as they are in any webhook handler that verifies before it parses.

On `charge.dispute.created` it:

1. reads the dispute (`/v1/disputes/{id}`, a route the Stripe map does not name), the charge, and
   the customer;
2. asks the model to draft evidence;
3. tags the customer (`POST /v1/customers/{id}` metadata) and alerts `#disputes` on Slack;
4. refunds the charge in full when the dispute is under $20, because fighting it costs more.

On `charge.refunded` it does nothing: a refund it issued comes back to it as an event, and an
idempotent consumer notes it and stops. Every event id is handled once, however often it arrives.

The server delivers its own events over loopback with a proxy-less opener: an inbound webhook is
not the agent's egress, and irimi must never see it.

    python -m examples.workflows.w05_dispute_responder.agent <scenario>
"""

from __future__ import annotations

import hashlib
import hmac
import json
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from examples.workflows import agentkit, sdk

DISPUTES_CHANNEL = "C0DISPUTES"
REFUND_UNDER = 2000  # minor units: disputes under $20 are refunded, not fought
BIG = "dp_BIG"
SMALL = "dp_SMALL"

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
    customer_id = charge["customer"]
    customer = agentkit.stripe("GET", f"/v1/customers/{customer_id}", label="customer").json()
    evidence = _draft_evidence(dispute, customer)
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


def _draft_evidence(dispute: dict[str, Any], customer: dict[str, Any]) -> str:
    body = {
        "model": "claude-sonnet-5",
        "max_tokens": 512,
        "system": "Draft dispute evidence for a card network. Two sentences.",
        "messages": [
            {"role": "user", "content": f"reason={dispute['reason']} customer={customer['email']}"}
        ],
    }
    headers = {"x-api-key": agentkit.key("ANTHROPIC_API_KEY"), "anthropic-version": "2023-06-01"}
    url = agentkit.base("anthropic") + "/v1/messages"
    resp = agentkit.http("POST", url, json_body=body, headers=headers, label="llm")
    return agentkit.text_of(resp.json() or {})


class Webhook(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        raw = self.rfile.read(int(self.headers.get("content-length") or 0))
        signature = self.headers.get("Stripe-Signature") or ""
        if self.path != "/stripe/webhook" or not _verified(raw, signature):
            agentkit.obs("rejected", reason="bad signature")
            self._answer(400, {"error": "bad signature"})
            return
        self._answer(200, {"received": True, "outcome": on_event(raw, signature)})

    def _answer(self, status: int, doc: dict[str, Any]) -> None:
        body = json.dumps(doc).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


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
    """POST one event to our own server, signed as Stripe signs it, not through any proxy."""
    raw = json.dumps(event).encode()
    signature = _sign(raw, secret or agentkit.key("STRIPE_WEBHOOK_SECRET"), int(time.time()))
    headers = {"Content-Type": "application/json", "Stripe-Signature": signature}
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(f"http://127.0.0.1:{port}/stripe/webhook", raw, headers)
    try:
        with opener.open(req, timeout=30) as resp:
            return {"status": resp.status, **json.loads(resp.read())}
    except urllib.error.HTTPError as err:
        return {"status": err.code}


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
    scenarios = ("over_threshold", "under_threshold", "cascade", "bad_signature", "replayed_event")
    if len(argv) != 1 or argv[0] not in scenarios:
        print(f"usage: agent.py {{{','.join(scenarios)}}}", file=sys.stderr)
        return 2
    scenario = argv[0]
    server = ThreadingHTTPServer(("127.0.0.1", 0), Webhook)
    threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True).start()
    port = server.server_address[1]
    dispute = BIG if scenario == "over_threshold" else SMALL
    secret = "whsec_not_the_real_one" if scenario == "bad_signature" else None
    answers = [_deliver(port, dispute_created(dispute), secret)]
    if scenario == "replayed_event":
        answers.append(_deliver(port, dispute_created(dispute)))
    if scenario == "cascade":
        # In production Stripe would now send `charge.refunded` for the refund. Under shadow the
        # refund never happened, so that event never comes: this delivers the one it would have
        # been, built from irimi's answer, to prove the consumer would not loop on it.
        answers += [_deliver(port, charge_refunded(r)) for r in _refunds]
    server.shutdown()
    agentkit.obs("result", scenario=scenario, answers=answers)
    return 0 if all(a["status"] == 200 for a in answers) or scenario == "bad_signature" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
