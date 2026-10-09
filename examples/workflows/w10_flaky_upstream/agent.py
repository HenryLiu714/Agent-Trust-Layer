"""W10 `flaky_upstream`: an agent that retries, against services that fail.

`sync_payouts` reads recent charges, asks the model which one to refund, refunds it, and tells
Slack. Every call retries on a 429, a 5xx or no answer at all, with a short backoff, and a write
carries an Idempotency-Key so its retry is the same write. The scenarios break one thing each: a
rate-limited read, a server error, a stalled read, a reset on a read and on the write, irimi's own
L3 read failing, a model that answers prose, an agent that raises, is interrupted or is killed
after its write, a write retried with no key, and irimi's own control endpoint taken away (#74).

    python -m examples.workflows.launch \\
        examples.workflows.w10_flaky_upstream.agent \\
        [--no-key] [--raise-after-write] [--sigterm-after-write] [--sigint-after-write] \\
        [--control {unset,unreachable,refused}]

What it is for: a shadow run is only as good as the failures it shows. A failed READ reaches the
real service under shadow and fails there too. A failed WRITE never does, because the write never
leaves irimi, so the agent's retry path for it is never exercised. The scenarios pin both.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sys
import time
from collections.abc import Callable
from typing import Any

from examples.workflows import agentkit, sdk
from examples.workflows.agentkit import NetworkError, Response, stripe

CHANNEL = "C0PAYOUT"
ATTEMPTS = 3
BACKOFF_S = 0.05
RETRY_STATUSES = frozenset({429, 500, 502, 503})
READ_TIMEOUT_S = 1.0  # shorter than the fake internet's 2 s stall, so a stall is a timeout
# How a scenario takes irimi's control endpoint away from the agent (`break_control`, #74).
CONTROL_FAULTS = ("unset", "unreachable", "refused")

SYSTEM = (
    "You approve payout corrections. Given recent charges, answer with JSON only: "
    '{"refund": "<charge id or null>", "amount": <minor units>, "note": "<why>"}'
)


def retrying(label: str, call: Callable[[], Response]) -> Response:
    """`call` until it answers with something other than a retryable status, `ATTEMPTS` times."""
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
    """The model's decision, or None when its answer is not the JSON it was asked for."""
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
    """Take away the control endpoint `irimi shadow` named, before the run starts, as a deploy
    can: a launcher that passes on only the variables it knows (`unset`), an endpoint that is gone
    (`unreachable`: a port nothing listens on), or one that refuses every post (`refused`: a 404).
    The SDK never raises into the agent: the run goes on, current and labelled, the SDK warns once,
    and irimi only lacks the run's record (#74). Bare, there is no endpoint to take away."""
    control = os.environ.get(agentkit.CONTROL_ENV)
    if control is None:
        return
    if how == "unset":
        del os.environ[agentkit.CONTROL_ENV]
    elif how == "unreachable":
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        os.environ[agentkit.CONTROL_ENV] = f"http://127.0.0.1:{port}/_irimi"
    else:
        os.environ[agentkit.CONTROL_ENV] = control + "/gone"


@sdk.trigger
def sync_payouts(
    use_key: bool, raise_after_write: bool, sigterm_after_write: bool, sigint_after_write: bool
) -> str:
    charges = retrying(
        "list_charges",
        lambda: stripe(
            "GET", "/v1/charges?limit=5", label="list_charges", timeout_s=READ_TIMEOUT_S
        ),
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
        # An operator presses Ctrl-C mid-run. Python raises KeyboardInterrupt, a BaseException,
        # which the SDK records as the run's error and re-raises (#74). Python's own handler is put
        # back first: a shell starts a background job with SIGINT ignored, and Python then installs
        # none, so the signal would do nothing and the run would end `ok`.
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
