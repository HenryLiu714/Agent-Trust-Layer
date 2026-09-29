"""Run a sample workflow's scenario, bare or under `irimi shadow`, and check what every run owes.

A workflow is a package `examples/workflows/wNN_<name>/` holding an agent (`agent.py`, started
through `examples.workflows.launch` so it keeps its real module name) and a `scenarios.py` that
defines `WORKFLOW`. A scenario seeds the fake services, names the agent's argv and environment,
and says whether anything about it is expected to differ from a bare run.

`run()` is the one place a scenario is executed:

- **bare**: the agent runs as a plain subprocess with `HTTP_PROXY` pointing at the fake internet.
  Its writes really land there. This is the baseline.
- **shadow**: `irimi shadow -- <agent>` runs in THIS process through `irimi.cli.main`, with the
  fake internet resolving its hosts, so irimi's real shipped maps, policy and overlay decide every
  call. Its writes must not land anywhere.

A Phase 3 mode, such as `serve` or `replay`, is a new branch here and not a new harness.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import os
import pkgutil
import re
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from examples.workflows.harness.internet import FakeInternet, Req
from examples.workflows.harness.services import World

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS_DIR = REPO_ROOT / "examples" / "workflows"

# Every credential a workflow is given is a canary: fake, recognisable, and greppable. The fake
# services accept them by prefix. A canary found on disk under irimi's home is a redaction leak.
CANARIES = {
    "STRIPE_API_KEY": "sk_test_CANARYstripe0000000000000000",
    "SLACK_BOT_TOKEN": "xoxb-CANARY-slack-0000000000",
    "SLACK_SIGNING_SECRET": "CANARYslacksigning00000000000000",
    "STRIPE_WEBHOOK_SECRET": "whsec_CANARYwebhook000000000000000",
    "ANTHROPIC_API_KEY": "sk-ant-CANARYanthropic0000000000000",
    "OPENAI_API_KEY": "sk-CANARYopenai000000000000000000000",
    "SLACK_WEBHOOK_PATH": "/services/T0CANARY/B0CANARY/CANARYwebhookpath000000",
}
MODES = ("bare", "shadow")
PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy")


@dataclass(frozen=True)
class Scenario:
    argv: tuple[str, ...] = ()
    env: dict[str, str] = field(default_factory=dict)
    setup: Callable[[World], None] | None = None
    # YAML documents added beside the shipped maps for this scenario, by file name.
    extra_maps: dict[str, str] = field(default_factory=dict)
    # Faults to inject once the fake internet is up: (host, action, match, times).
    faults: tuple[tuple[str, str, Callable[[Req], bool], int], ...] = ()
    # The agent's exit code under shadow is expected to differ from the bare run's.
    diverges: bool = False
    # The scenario exists to show a write irimi cannot stop; universal invariant 1 is inverted.
    leaks: bool = False
    doc: str = ""


@dataclass(frozen=True)
class Workflow:
    name: str  # the package name, e.g. "w09_scope_gauntlet"
    summary: str
    scenarios: dict[str, Scenario]

    @property
    def module(self) -> str:
        return f"examples.workflows.{self.name}.agent"


@dataclass
class Result:
    workflow: Workflow
    scenario: str
    mode: str
    exit_code: int
    obs: list[dict[str, Any]]
    irimi: list[str]  # every line irimi printed (shadow only)
    world: World
    internet: FakeInternet
    home: Path
    state: Path

    def events(self, event: str) -> list[dict[str, Any]]:
        return [o for o in self.obs if o["event"] == event]

    def calls(self, label: str | None = None) -> list[dict[str, Any]]:
        return [o for o in self.events("http") if label is None or o.get("label") == label]

    def answered(self) -> list[tuple[str, str, Any, Any]]:
        """`(method, host+path, status, Irimi-Answered-By)` per call the agent made, in order."""
        return [
            (c["method"], c["url"], c.get("status"), c.get("answered_by")) for c in self.calls()
        ]

    def result(self) -> dict[str, Any] | None:
        """The agent's final `result` event, if it logged one."""
        found = self.events("result")
        return found[-1] if found else None

    def exchange_lines(self) -> list[str]:
        """irimi's per-exchange lines: `<answered_by> <kind> <METHOD> <host><path> -> <status>`."""
        return [line for line in self.irimi if " -> " in line and not line.startswith(" ")]

    def summary(self) -> list[str]:
        """irimi's closing block, from its `N exchanges` header on."""
        for i, line in enumerate(self.irimi):
            if line.startswith("irimi shadow · run ") and re.search(r" · \d+ exchanges? · ", line):
                return self.irimi[i:]
        return []


def workflows() -> dict[str, Workflow]:
    """Every workflow package under `examples/workflows/`, found by name so a new one needs no
    registry edit."""
    found: dict[str, Workflow] = {}
    for info in pkgutil.iter_modules([str(WORKFLOWS_DIR)]):
        is_workflow = info.ispkg and info.name.startswith("w") and info.name[1:3].isdigit()
        # A package with no scenarios.py yet is half-written, not broken: skip it. One whose
        # scenarios.py fails to import is broken, and that error is not swallowed.
        if is_workflow and (WORKFLOWS_DIR / info.name / "scenarios.py").exists():
            module = importlib.import_module(f"examples.workflows.{info.name}.scenarios")
            found[info.name] = module.WORKFLOW
    return dict(sorted(found.items()))


def run(workflow: Workflow, scenario_name: str, mode: str, workdir: Path) -> Result:
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, not {mode!r}")
    scenario = workflow.scenarios[scenario_name]
    workdir.mkdir(parents=True, exist_ok=True)
    home, state, cwd = workdir / "home", workdir / "state", workdir / "cwd"
    for d in (home, state, cwd):
        d.mkdir(exist_ok=True)
    obs_path = workdir / "obs.jsonl"
    obs_path.write_text("")

    world = World()
    if scenario.setup is not None:
        scenario.setup(world)
    internet = FakeInternet(world.services())
    internet.start()
    for host, action, match, times in scenario.faults:
        internet.inject(host, action, match, times)

    env = _agent_env(internet, state, obs_path, home)
    env.update(scenario.env)
    cmd = [sys.executable, "-m", "examples.workflows.launch", workflow.module, *scenario.argv]
    irimi_out: list[str] = []
    try:
        if mode == "bare":
            env.update(
                {
                    "HTTP_PROXY": f"http://127.0.0.1:{internet.port}",
                    "http_proxy": f"http://127.0.0.1:{internet.port}",
                    "NO_PROXY": "127.0.0.1,localhost",
                    "no_proxy": "127.0.0.1,localhost",
                }
            )
            returncode = subprocess.run(cmd, env=env, cwd=cwd, timeout=180, check=False).returncode
            # The shell's spelling of a signal, as `irimi shadow` reports its child's exit.
            code = 128 - returncode if returncode < 0 else returncode
        else:
            code, irimi_out = _run_shadow(cmd, env, cwd, home, internet, scenario, workdir)
    finally:
        internet.stop()
    obs = [json.loads(line) for line in obs_path.read_text().splitlines() if line.strip()]
    return Result(workflow, scenario_name, mode, code, obs, irimi_out, world, internet, home, state)


def _agent_env(internet: FakeInternet, state: Path, obs_path: Path, home: Path) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in PROXY_VARS}
    env.pop("NO_PROXY", None)
    env.pop("no_proxy", None)
    env.update(CANARIES)
    env.update(
        {
            "STRIPE_API_BASE": internet.base("api.stripe.com"),
            "SLACK_API_BASE": internet.base("slack.com"),
            "SLACK_HOOKS_API_BASE": internet.base("hooks.slack.com"),
            "ANTHROPIC_API_BASE": internet.base("api.anthropic.com"),
            "OPENAI_API_BASE": internet.base("api.openai.com"),
            "WORKFLOW_INTERNET_PORT": str(internet.port),
            "WORKFLOW_STATE": str(state),
            "WORKFLOW_OBS": str(obs_path),
            "WORKFLOW_HTTP_TIMEOUT": "5",
            "PYTHONPATH": os.pathsep.join([str(REPO_ROOT), env.get("PYTHONPATH", "")]).rstrip(
                os.pathsep
            ),
            "PYTHONUNBUFFERED": "1",
            "IRIMI_HOME": str(home),
        }
    )
    return env


def _run_shadow(
    cmd: list[str],
    env: dict[str, str],
    cwd: Path,
    home: Path,
    internet: FakeInternet,
    scenario: Scenario,
    workdir: Path,
) -> tuple[int, list[str]]:
    from irimi import ca
    from irimi.cli import main
    from irimi.servicemap import loader

    buf = io.StringIO()
    with _environ(env), _chdir(cwd), _maps(scenario, workdir, loader), internet.resolving():
        ca.generate_ca(ca.ca_paths())
        with contextlib.redirect_stdout(buf):
            code = main(["shadow", "--port", "0", "--", *cmd])
    return code, buf.getvalue().splitlines()


@contextlib.contextmanager
def _environ(env: dict[str, str]) -> Iterator[None]:
    saved = dict(os.environ)
    os.environ.clear()
    os.environ.update(env)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


@contextlib.contextmanager
def _chdir(path: Path) -> Iterator[None]:
    saved = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(saved)


@contextlib.contextmanager
def _maps(scenario: Scenario, workdir: Path, loader: Any) -> Iterator[None]:
    """The shipped maps, plus the scenario's own, read from a directory of this run's."""
    if not scenario.extra_maps:
        yield
        return
    maps_dir = workdir / "maps"
    maps_dir.mkdir(exist_ok=True)
    for shipped in loader.shipped_dir().glob("*.yaml"):
        shutil.copy(shipped, maps_dir / shipped.name)
    for name, text in scenario.extra_maps.items():
        (maps_dir / name).write_text(text)
    saved = loader.shipped_dir
    loader.shipped_dir = lambda: maps_dir
    try:
        yield
    finally:
        loader.shipped_dir = saved


# -- the universal invariants ---------------------------------------------------------------------


def check_invariants(shadow: Result, bare: Result | None = None) -> list[str]:
    """What every shadow run owes, whatever the workflow. Returns the broken ones, worded."""
    assert shadow.mode == "shadow"
    scenario = shadow.workflow.scenarios[shadow.scenario]
    problems = []
    writes = shadow.internet.writes()
    if not scenario.leaks and writes:
        problems.append(
            "1. a write reached a fake service: "
            + ", ".join(f"{r.method} {r.host}{r.path}" for r in writes)
        )
    if scenario.leaks and not writes:
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
    leaked = _canaries_under(shadow.home)
    if leaked:
        problems.append(f"4. a canary reached disk under irimi's home: {leaked}")
    if bare is not None and not scenario.diverges and bare.exit_code != shadow.exit_code:
        problems.append(
            f"5. the agent exited {shadow.exit_code} under shadow but {bare.exit_code} bare"
        )
    return problems


def _canaries_under(root: Path) -> list[str]:
    needles = {name: value.encode() for name, value in CANARIES.items()}
    found = []
    for path in root.rglob("*"):
        if path.is_file():
            data = path.read_bytes()
            found += [
                f"{name} in {path.relative_to(root)}" for name, v in needles.items() if v in data
            ]
    return found
