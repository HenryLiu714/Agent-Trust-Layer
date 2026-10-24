"""W10 `flaky_upstream`: an agent that retries against services that fail, one failure per scenario.

A failed read fails under shadow too; a failed write never does, so its retry path never runs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import socket
import sys
import threading
import time
from collections.abc import Callable
from typing import Any

from irimi import sdk
from irimi.protocol.env import CONTROL_ENV
from tests.e2e.workflows.agents import agentkit, observe
from tests.e2e.workflows.agents.agentkit import NetworkError, Response, stripe

CHANNEL = "C0PAYOUT"
# The charge `--refund-both` refunds beside the one the model picks.
OTHER_CHARGE = "ch_PAYOUT2"
STAGGER_S = 0.1
ATTEMPTS = 3
BACKOFF_S = 0.05
RETRY_STATUSES = frozenset({429, 500, 502, 503})
READ_TIMEOUT_S = 1.0  # shorter than the fake internet's 2 s stall, so a stall is a timeout
# How a scenario takes irimi's control endpoint away from the agent (`break_control`).
CONTROL_FAULTS = ("unset", "unreachable", "refused", "stalled")
# The connections a `stalled` control endpoint accepted, and never answered.
_stalled_connections: list[socket.socket] = []

SYSTEM = (
    "You approve payout corrections. Given recent charges, answer with JSON only: "
    '{"refund": "<charge id or null>", "amount": <minor units>, "note": "<why>"}'
)


def retrying(label: str, call: Callable[[], Response]) -> Response:
    """Call `call` until it answers a non-retryable status, at most `ATTEMPTS` times."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            resp = call()
        except NetworkError as exc:
            agentkit.obs("retry", label=label, attempt=attempt, reason=type(exc).__name__)
        else:
            if resp.status not in RETRY_STATUSES:
                return resp
            agentkit.obs("retry", label=label, attempt=attempt, reason=resp.status)
        if attempt < ATTEMPTS:
            time.sleep(BACKOFF_S * attempt)
    raise RuntimeError(f"{label}: gave up after {ATTEMPTS} attempts")


def decide(charges: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the model's decision, or None when its answer is not the JSON it was asked for."""
    summary = json.dumps([{k: c[k] for k in ("id", "amount", "currency")} for c in charges])
    message = agentkit.anthropic(SYSTEM, [{"role": "user", "content": f"Charges: {summary}"}])
    text = agentkit.text_of(message)
    try:
        decision = json.loads(text)
    except ValueError:
        agentkit.obs("llm_fallback", text=text[:80])
        return None
    return decision if isinstance(decision, dict) and decision.get("refund") else None


def _refund_both_at_once(charges: list[str], amount: int) -> str:
    """Refund each charge on its own thread in this run, the second while the first is in flight."""

    def refund(charge: str) -> None:
        headers = {"Idempotency-Key": f"payout-{charge}"}
        form = {"charge": charge, "amount": str(amount)}
        resp = stripe("POST", "/v1/refunds", form, headers=headers, label=f"refund_{charge}")
        agentkit.obs("refunded", status=resp.status, id=(resp.json() or {}).get("id"))

    threads = [threading.Thread(target=sdk.propagate(refund), args=(c,)) for c in charges]
    for thread in threads:
        thread.start()
        # The first refund's request goes first, so the two are made in one order every time.
        time.sleep(STAGGER_S)
    for thread in threads:
        thread.join()
    return "refunded both"


def break_control(how: str) -> None:
    """Take the control endpoint away before the run, as a deploy can (`how`: CONTROL_FAULTS).

    The SDK never raises, so the run goes on labelled, warns once, and only its record is lost."""
    control = os.environ.get(CONTROL_ENV)
    if control is None:
        return
    if how == "unset":
        del os.environ[CONTROL_ENV]
    elif how == "unreachable":
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        os.environ[CONTROL_ENV] = f"http://127.0.0.1:{port}/_irimi"
    elif how == "stalled":
        os.environ[CONTROL_ENV] = f"http://127.0.0.1:{_stalled_endpoint()}/_irimi"
    else:
        os.environ[CONTROL_ENV] = control + "/gone"


def _stalled_endpoint() -> int:
    """Return a port that accepts every connection and answers none, so each post times out.

    The accepted connections are kept, and counted, until the agent exits."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen()

    def accept() -> None:
        while True:
            conn, _ = server.accept()
            _stalled_connections.append(conn)

    threading.Thread(target=accept, daemon=True).start()
    return int(server.getsockname()[1])


@observe.trigger
def sync_payouts(
    use_key: bool,
    raise_after_write: bool,
    sigterm_after_write: bool,
    sigint_after_write: bool,
    confirm_refund: bool = False,
    raise_with_key: bool = False,
    refund_both: bool = False,
) -> str:
    charges = retrying(
        "list_charges",
        lambda: stripe(
            "GET", "/v1/charges?limit=5", label="list_charges", timeout_s=READ_TIMEOUT_S
        ),
    )
    # The agent decodes nothing, so a bomb's bytes are what it holds, and must be the service's
    # under shadow too.
    agentkit.obs(
        "listed",
        bytes=len(charges.body),
        sha256=hashlib.sha256(charges.body).hexdigest(),
        encoding=charges.headers.get("content-encoding"),
    )
    decision = decide((charges.json() or {}).get("data") or [])
    if decision is None:
        agentkit.slack("chat.postMessage", channel=CHANNEL, text="payout sync: no action")
        return "no action"

    if refund_both:
        return _refund_both_at_once([decision["refund"], OTHER_CHARGE], int(decision["amount"]))
    form = {"charge": decision["refund"], "amount": str(decision["amount"])}
    headers = {"Idempotency-Key": f"payout-{decision['refund']}"} if use_key else {}
    refund = retrying(
        "refund", lambda: stripe("POST", "/v1/refunds", form, headers=headers, label="refund")
    )
    refund_id = (refund.json() or {}).get("id")
    agentkit.obs("refunded", status=refund.status, id=refund_id)
    if confirm_refund:
        # Read the refund back, retrying as every other call does, before telling anyone.
        confirmed = retrying(
            "confirm_refund",
            lambda: stripe("GET", f"/v1/refunds/{refund_id}", label="confirm_refund"),
        )
        agentkit.obs("confirmed", status=confirmed.status, id=(confirmed.json() or {}).get("id"))
    if raise_with_key:
        # A client library that puts the whole credential in its error, and nothing else.
        raise RuntimeError(agentkit.key("LONG_ERROR_KEY"))
    if raise_after_write:
        raise RuntimeError(f"ledger export failed after refund {refund_id}")
    if sigterm_after_write:
        # The orchestrator's deploy kills the worker mid-run.
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(5)  # never reached: SIGTERM's default action ends the process
    if sigint_after_write:
        # Ctrl-C mid-run: the SDK records the KeyboardInterrupt and re-raises. Python's handler is
        # restored first, since a shell's background job starts with SIGINT ignored.
        signal.signal(signal.SIGINT, signal.default_int_handler)
        os.kill(os.getpid(), signal.SIGINT)
        time.sleep(5)  # interrupted: the KeyboardInterrupt is raised by now
    agentkit.slack(
        "chat.postMessage",
        channel=CHANNEL,
        text=f"refunded {decision['amount']} on {form['charge']}",
    )
    return str(refund_id)


def main(argv: list[str]) -> int:
    agentkit.start()
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-key", action="store_true")
    parser.add_argument("--raise-after-write", action="store_true")
    parser.add_argument("--sigterm-after-write", action="store_true")
    parser.add_argument("--sigint-after-write", action="store_true")
    parser.add_argument("--confirm-refund", action="store_true")
    parser.add_argument("--raise-with-key", action="store_true")
    parser.add_argument("--refund-both", action="store_true")
    parser.add_argument("--control", choices=CONTROL_FAULTS)
    args = parser.parse_args(argv)
    if args.control:
        break_control(args.control)
    outcome = sync_payouts(
        not args.no_key,
        args.raise_after_write,
        args.sigterm_after_write,
        args.sigint_after_write,
        args.confirm_refund,
        args.raise_with_key,
        args.refund_both,
    )
    if args.control == "stalled":
        agentkit.obs("control_connections", count=len(_stalled_connections))
    agentkit.obs("result", outcome=outcome)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
