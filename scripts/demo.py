"""Run an example under `irimi shadow` against a fake internet on loopback: no keys, no network.

    uv run python scripts/demo.py               # examples/hello_agent.py
    uv run python scripts/demo.py quickstart    # examples/quickstart, refunding a fake charge
    uv run python scripts/demo.py replay        # examples/replay, recording one run
    uv run python scripts/demo.py w01           # the w01_ticket_triage double_refund workflow

Every host the example calls is served by the workflow corpus's fake services, on loopback, and
irimi runs with its real shipped maps. Its home and trace store are a temporary directory, so the
demo leaves nothing behind. `make demo` runs it; `make demo EXAMPLE=quickstart` picks one.
"""

import contextlib
import json
import os
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
# The harness is in the repository's `tests` package, which is not installed.
sys.path.insert(0, str(REPO_ROOT))

from irimi.cli import main as irimi_main  # noqa: E402  (after the path above)
from irimi.config import ca  # noqa: E402
from tests.e2e.workflows.agents import credentials  # noqa: E402
from tests.e2e.workflows.harness import execute  # noqa: E402
from tests.e2e.workflows.harness.internet import SAFE_METHODS, FakeInternet, Resp  # noqa: E402
from tests.e2e.workflows.harness.scenario import environ, workflows  # noqa: E402
from tests.e2e.workflows.harness.services import (  # noqa: E402
    JsonService,
    LlmCall,
    LlmTurn,
    ScriptedLLM,
    World,
)

CHARGE = "ch_demo1"
CUSTOMER = "Ada Lovelace"
# The base-URL variables each example reads, and the host the fake internet answers it as.
BASES = {
    "GITHUB_API_BASE": "api.github.com",
    "STRIPE_API_BASE": "api.stripe.com",
    "SLACK_HOOKS_API_BASE": "hooks.slack.com",
    "ANTHROPIC_BASE_URL": "api.anthropic.com",
}
# What irimi's process inherits from the shell; everything else it is given by name.
INHERITED_ENV = ("PATH", "HOME", "USER", "LOGNAME", "LANG", "TZ", "SYSTEMROOT")
WORKFLOW = ("w01_ticket_triage", "double_refund")


@dataclass(frozen=True)
class Example:
    about: str
    command: tuple[str, ...]
    # Seeds the fake services and the run's directory, and returns the agent's own variables.
    seed: Callable[[World, Path], dict[str, str]]


def _hello(world: World, workdir: Path) -> dict[str, str]:
    world.stripe.add_charge("ch_hello")
    zen = Resp(200, "Keep it logically awesome.", {"content-type": "text/plain"})
    github = JsonService(
        ("api.github.com",),
        {("GET", "/zen"): lambda req, state: zen},
        is_write=lambda req: req.method not in SAFE_METHODS,
    )
    world.extra.append(github)
    return {}


def _quickstart(world: World, workdir: Path) -> dict[str, str]:
    world.stripe.add_charge(CHARGE)
    return {}


def _decide(call: LlmCall) -> LlmTurn:
    """Answer as `examples/replay/prompt*.txt` ask: refund what is left, or half when told."""
    facts = json.loads(call.last_user_text())
    left = facts["charge"]["amount"] - facts["charge"]["amount_refunded"]
    amount = left // 2 if "half" in call.system else left
    return LlmTurn(text=json.dumps({"refund_amount": amount, "reply": f"Refunded {amount}."}))


def _replay(world: World, workdir: Path) -> dict[str, str]:
    from examples.replay import seed  # noqa: PLC0415  (imports the anthropic SDK)

    world.stripe.add_charge(CHARGE)
    world.llm = ScriptedLLM(_decide)
    db = workdir / "llm_refunds.sqlite3"
    seed.seed(str(db), CHARGE, CUSTOMER)
    return {"REFUND_DB": str(db)}


EXAMPLES = {
    "hello": Example(
        "one real read and two writes irimi fakes",
        (str(REPO_ROOT / "examples" / "hello_agent.py"),),
        _hello,
    ),
    "quickstart": Example(
        "a stripe-python refund agent refunds, reads back and retries",
        ("-m", "examples.quickstart.agent"),
        _quickstart,
    ),
    "replay": Example(
        "an LLM refund agent records a run you could replay",
        ("-m", "examples.replay.agent", "T-1", CHARGE, "I was charged twice"),
        _replay,
    ),
}


def run(name: str, workdir: Path) -> tuple[int, FakeInternet]:
    """Run `irimi shadow -- <example>` in this process against a fake internet seeded for it.

    Returns irimi's exit code and the fake internet, whose `writes()` must be empty afterwards."""
    example = EXAMPLES[name]
    world = World()
    home = workdir / "home"
    home.mkdir(parents=True, exist_ok=True)
    agent_env = example.seed(world, workdir)
    internet = FakeInternet(world.services())
    internet.start()
    env = {k: v for k, v in os.environ.items() if k in INHERITED_ENV or k.startswith("LC_")}
    env.update(
        {
            "STRIPE_API_KEY": credentials.STRIPE_API_KEY,
            "ANTHROPIC_API_KEY": credentials.ANTHROPIC_API_KEY,
            **{var: internet.base(host) for var, host in BASES.items()},
            **agent_env,
            "IRIMI_HOME": str(home),
            "PYTHONPATH": str(REPO_ROOT),
            "PYTHONUNBUFFERED": "1",
            # irimi's own check reads are urllib in this process: no system proxy may carry
            # them past the fake names. irimi gives its child a NO_PROXY of its own.
            "NO_PROXY": "*",
            "no_proxy": "*",
        }
    )
    argv = ["shadow", "--store", str(workdir / "store"), "--", sys.executable, *example.command]
    try:
        with environ(env), contextlib.chdir(workdir), internet.resolving():
            ca.generate_ca(ca.ca_paths())
            code = irimi_main(argv)
    finally:
        internet.stop()
    return code, internet


def _workflow(workdir: Path) -> int:
    """Run the corpus's own start-here scenario, under `irimi shadow` only."""
    name, scenario = WORKFLOW
    result = execute.run(workflows()[name], scenario, "shadow", workdir)
    print("\n".join(result.irimi))
    return result.exit_code


def main(argv: list[str]) -> int:
    choices = [*EXAMPLES, "w01"]
    name = argv[0] if argv else "hello"
    if len(argv) > 1 or name not in choices:
        print(f"usage: python scripts/demo.py [{' | '.join(choices)}]", file=sys.stderr)
        return 2
    about = EXAMPLES[name].about if name in EXAMPLES else "an LLM agent refunds twice"
    print(f"demo: {about}; every service is a fake on loopback, and no key is real\n", flush=True)
    with tempfile.TemporaryDirectory(prefix="irimi-demo-") as tmp:
        if name == "w01":
            return _workflow(Path(tmp))
        code, internet = run(name, Path(tmp))
    escaped = internet.writes()
    if escaped:
        print(f"error: {len(escaped)} writes reached the fake services", file=sys.stderr)
        return 1
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
