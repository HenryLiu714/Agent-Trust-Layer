"""The five invariants every shadow run, serve run and replay owes, whatever the workflow, and
the canary credentials invariant 4 looks for on disk."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

from tests.e2e.workflows.agents import credentials
from tests.e2e.workflows.harness.internet import BOMB_SECRET
from tests.e2e.workflows.harness.result import Result
from tests.e2e.workflows.harness.services import CLIENT_SECRET_CANARY

# Every credential a workflow is given is a canary: fake, recognisable, and greppable. The fake
# services accept them by prefix. A canary found on disk under irimi's home is a redaction leak.
CANARIES = {
    "STRIPE_API_KEY": credentials.STRIPE_API_KEY,
    "SLACK_BOT_TOKEN": credentials.SLACK_BOT_TOKEN,
    "SLACK_SIGNING_SECRET": credentials.SLACK_SIGNING_SECRET,
    "STRIPE_WEBHOOK_SECRET": credentials.STRIPE_WEBHOOK_SECRET,
    "ANTHROPIC_API_KEY": credentials.ANTHROPIC_API_KEY,
    "OPENAI_API_KEY": credentials.OPENAI_API_KEY,
    "SLACK_WEBHOOK_PATH": credentials.SLACK_WEBHOOK_PATH,
}
# And every credential a fake service hands back in a response body. A live read's response is
# stored, so one found on disk is a leak on the response side, which the canaries above, all sent
# by the agent, never reach. Not given to the agent: it is the service's to hand out. A
# bomb's first line holds one, so the start of a body cut at irimi's decoding cap is held to it
# too.
SERVED_CANARIES = {"STRIPE_CLIENT_SECRET": CLIENT_SECRET_CANARY, "BOMB_SECRET": BOMB_SECRET}
# How much of a canary's start invariant 4 finds on its own: what a cut made before redaction
# keeps of a key straddling it is no longer the key's shape, so it would reach disk as it is, and
# the whole canary would never be there to find. Twelve characters: past the prefix every shape
# starts with (`sk-ant-`, `whsec_`), into the part of a canary that is the canary's own.
CANARY_PART = 12


def check_invariants(shadow: Result, bare: Result | None = None) -> list[str]:
    """What every shadow run owes, whatever the workflow, and every serve run and replay.
    Returns the broken ones, worded. Invariant 5 compares exit codes with `bare`, when it
    is given."""
    if shadow.mode not in ("shadow", "serve", "replay"):
        raise ValueError(f"check_invariants takes a shadow, serve or replay run, not {shadow.mode}")
    scenario = shadow.workflow.scenarios[shadow.scenario]
    problems = []
    # Invariant 1 watches the fake internet, so it holds only if irimi's upstream IS the fake
    # internet. A live answer the agent saw that no fake served came from somewhere else, and a
    # write sent there would pass invariant 1 unseen.
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
    """Each call the agent saw answered live (no `Irimi-Answered-By`) by a host the fake internet
    serves, and that the fake internet has no matching request for."""
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
    """Each canary found whole in a file under `root`, or else its first CANARY_PART characters: a
    key cut before redaction is no longer its shape, so the part a cut kept reached disk as it was,
    which the whole canary is never found as."""
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
