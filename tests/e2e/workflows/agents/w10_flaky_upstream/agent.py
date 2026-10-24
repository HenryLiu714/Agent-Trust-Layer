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
import time
from collections.abc import Callable
from typing import Any

from irimi.protocol.env import CONTROL_ENV
from tests.e2e.workflows.agents import agentkit, observe
from tests.e2e.workflows.agents.agentkit import NetworkError, Response, stripe

CHANNEL = "C0PAYOUT"
ATTEMPTS = 3
BACKOFF_S = 0.05
RETRY_STATUSES = frozenset({429, 500, 502, 503})
READ_TIMEOUT_S = 1.0  # shorter than the fake internet's 2 s stall, so a stall is a timeout
# How a scenario takes irimi's control endpoint away from the agent (`break_control`).
CONTROL_FAULTS = ("unset", "unreachable", "refused")

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
    else:
        os.environ[CONTROL_ENV] = control + "/gone"


@observe.trigger
def sync_payouts(
    use_key: bool, raise_after_write: bool, sigterm_after_write: bool, sigint_after_write: bool
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

    form = {"charge": decision["refund"], "amount": str(decision["amount"])}
    headers = {"Idempotency-Key": f"payout-{decision['refund']}"} if use_key else {}
    refund = retrying(
        "refund", lambda: stripe("POST", "/v1/refunds", form, headers=headers, label="refund")
    )
    refund_id = (refund.json() or {}).get("id")
    agentkit.obs("refunded", status=refund.status, id=refund_id)
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
    parser.add_argument("--control", choices=CONTROL_FAULTS)
    args = parser.parse_args(argv)
    if args.control:
        break_control(args.control)
    outcome = sync_payouts(
        not args.no_key,
        args.raise_after_write,
        args.sigterm_after_write,
        args.sigint_after_write,
    )
    agentkit.obs("result", outcome=outcome)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
