"""The five invariants every shadow run, serve run and replay holds, and the canary credentials."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

from tests.e2e.workflows.agents import credentials
from tests.e2e.workflows.harness.internet import BOMB_SECRET
from tests.e2e.workflows.harness.result import Result
from tests.e2e.workflows.harness.services import CLIENT_SECRET_CANARY

# Every credential a workflow is given is a fake, greppable canary the fake services accept by
# prefix. A canary found on disk under irimi's home is a redaction leak.
CANARIES = {
    "STRIPE_API_KEY": credentials.STRIPE_API_KEY,
    "SLACK_BOT_TOKEN": credentials.SLACK_BOT_TOKEN,
    "SLACK_SIGNING_SECRET": credentials.SLACK_SIGNING_SECRET,
    "STRIPE_WEBHOOK_SECRET": credentials.STRIPE_WEBHOOK_SECRET,
    "ANTHROPIC_API_KEY": credentials.ANTHROPIC_API_KEY,
    "OPENAI_API_KEY": credentials.OPENAI_API_KEY,
    "SLACK_WEBHOOK_PATH": credentials.SLACK_WEBHOOK_PATH,
}
# The credentials a fake service hands back in a response body: one on disk is a leak on the
# response side, which the agent's canaries never reach. A bomb's first line holds one, so the
# start of a body cut at irimi's decoding cap is checked too.
SERVED_CANARIES = {"STRIPE_CLIENT_SECRET": CLIENT_SECRET_CANARY, "BOMB_SECRET": BOMB_SECRET}
# How much of a canary's start invariant 4 also looks for: a key cut before redaction loses its
# shape and reaches disk as it is. Twelve characters reach past every shape's prefix (`sk-ant-`).
CANARY_PART = 12


def check_invariants(shadow: Result, bare: Result | None = None) -> list[str]:
    """Return the invariants a shadow, serve or replay run broke, worded.

    Invariant 5 compares exit codes with `bare`, when it is given."""
    if shadow.mode not in ("shadow", "serve", "replay"):
        raise ValueError(f"check_invariants takes a shadow, serve or replay run, not {shadow.mode}")
    scenario = shadow.workflow.scenarios[shadow.scenario]
    problems = []
    # Invariant 1 watches the fake internet, so a live answer no fake served means irimi's
    # upstream was somewhere else, where a write would pass unseen.
    unserved = _live_answers_not_served(shadow)
    if unserved:
        problems.append(f"1. the agent saw live answers no fake service gave: {unserved}")
    writes = shadow.internet.writes()
    if not scenario.leaks and writes:
        problems.append(
            "1. a write reached a fake service: "
            + ", ".join(f"{r.method} {r.host}{r.path}" for r in writes)
        )
    # A replay answers the leaking read from the recording, so it may leak nothing.
    if scenario.leaks and not writes and shadow.mode != "replay":
        problems.append("1. a scenario marked `leaks` leaked nothing")
    labelled = [r for r in shadow.internet.requests() if "irimi-run" in r.headers]
    if labelled:
        problems.append(
            "2. Irimi-Run reached a fake service: "
            + ", ".join(f"{r.method} {r.host}{r.path}" for r in labelled)
        )
    real_writes = [
        t["name"] for t in shadow.events("tool") if t["kind"] == "write" and t["ran"] == "real"
    ]
    if real_writes:
        problems.append(f"3. a write tool ran for real: {real_writes}")
    roots = [shadow.home, shadow.cwd, shadow.tmp]
    if not shadow.store_root().is_relative_to(shadow.home):
        roots.append(shadow.store_root())  # a store the run was given, outside its home
    leaked = [found for root in roots for found in _canaries_under(root)]
    if leaked:
        problems.append(f"4. a canary reached disk where irimi writes: {leaked}")
    if bare is not None and not scenario.diverges and bare.exit_code != shadow.exit_code:
        problems.append(
            f"5. the agent exited {shadow.exit_code} under shadow but {bare.exit_code} bare"
        )
    return problems


def _live_answers_not_served(result: Result) -> list[str]:
    """Return each live answer from a fake host that the fake internet has no request for."""
    received = Counter((r.method, f"{r.host}{r.path}") for r in result.internet.requests())
    missing = []
    for call in result.calls():
        host = str(call["url"]).split("/", 1)[0]
        live = call.get("status") is not None and call.get("answered_by") is None
        if not live or host not in result.internet.services:
            continue
        key = (call["method"], call["url"])
        if received[key]:
            received[key] -= 1
        else:
            missing.append(f"{call['method']} {call['url']}")
    return missing


def _canaries_under(root: Path) -> list[str]:
    """Return each canary found under `root`, whole or as its first CANARY_PART characters."""
    canaries = {**CANARIES, **SERVED_CANARIES}
    found = []
    for path in root.rglob("*"):
        if path.is_file():
            data = path.read_bytes()
            for name, value in canaries.items():
                if value.encode() in data:
                    found.append(f"{name} in {path}")
                elif value[:CANARY_PART].encode() in data:
                    found.append(f"{name}'s first {CANARY_PART} characters in {path}")
    return found
