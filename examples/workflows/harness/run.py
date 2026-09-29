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
import sqlite3
import subprocess
import sys
import tempfile
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from examples.workflows.harness.internet import FakeInternet, Req
from examples.workflows.harness.services import CLIENT_SECRET_CANARY, World
from irimi import ca, paths, report, runner, trace
from irimi import store as irimi_store
from irimi.cli import main as irimi_main
from irimi.exchange import Exchange
from irimi.servicemap import loader
from irimi.store import StoreReader

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
# And every credential a fake service hands back in a response body. A live read's response is
# stored, so one found on disk is a leak on the response side, which the canaries above, all sent
# by the agent, never reach (#70). Not given to the agent: it is the service's to hand out.
SERVED_CANARIES = {"STRIPE_CLIENT_SECRET": CLIENT_SECRET_CANARY}
MODES = ("bare", "shadow")
PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy")
# The only variables an agent inherits from the harness's own environment. Everything else it is
# given by name, so a developer's shell cannot change a run: an exported `IRIMI_ENGINE_ACTIVE=1`
# would turn a bare run's write tools into stand-ins, and an exported `PROMPT` or
# `SLACK_POST_CHANNEL` would change what an agent sends.
INHERITED_ENV = ("PATH", "HOME", "USER", "LOGNAME", "LANG", "TZ", "SYSTEMROOT")


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
    home: Path  # IRIMI_HOME
    state: Path  # the agent's own files (WORKFLOW_STATE)
    cwd: Path  # the working directory of irimi (shadow) and of the agent
    tmp: Path  # TMPDIR, and `tempfile`'s directory in this process during a shadow run
    # Every exchange irimi printed a line for, as it printed them (shadow only): the live
    # exchanges its `on_exchange` saw, which what the store kept is compared against (#70).
    reported: list[Exchange] = field(default_factory=list)
    # Every exchange the engine handed its trace store, in the order it did (shadow only).
    handed: list[Exchange] = field(default_factory=list)

    def events(self, event: str, *, with_time: bool = True) -> list[dict[str, Any]]:
        """Every `event` the agent logged, in order. `with_time=False` drops each one's
        timestamp, so events from two runs compare equal when only their time differs."""
        found = [o for o in self.obs if o["event"] == event]
        return found if with_time else [{k: v for k, v in o.items() if k != "t"} for o in found]

    def one(self, event: str) -> dict[str, Any]:
        """The one `event` the agent logged. Raises if it logged none, or more than one."""
        found = self.events(event)
        if len(found) != 1:
            raise AssertionError(f"expected one {event!r} event, got {len(found)}: {found}")
        return found[0]

    def result(self, *, with_time: bool = True) -> dict[str, Any] | None:
        """The agent's final `result` event, if it logged one."""
        found = self.events("result", with_time=with_time)
        return found[-1] if found else None

    def calls(self, label: str | None = None) -> list[dict[str, Any]]:
        """The agent's HTTP calls (`http` events), in order: all of them, or those with `label`."""
        return [o for o in self.events("http") if label is None or o.get("label") == label]

    def answered(self) -> list[tuple[str, Any, Any]]:
        """`(label, status, Irimi-Answered-By)` per call, in order. An unlabelled call is named by
        its host and path; a call with no answer has status None."""
        return [
            (c.get("label") or c["url"], c.get("status"), c.get("answered_by"))
            for c in self.calls()
        ]

    def by_label(self) -> dict[str, tuple[Any, Any]]:
        """`{label: (status, Irimi-Answered-By)}`, one entry per call. Raises if a call has no
        label or shares one, because then the dict could not hold every call."""
        found: dict[str, tuple[Any, Any]] = {}
        for c in self.calls():
            if not c.get("label") or c["label"] in found:
                raise AssertionError(f"{c['method']} {c['url']}: no label of its own")
            found[c["label"]] = (c.get("status"), c.get("answered_by"))
        return found

    def by_run(self) -> dict[str | None, list[str | None]]:
        """`{run id: [label, ...]}` over the calls, each run's in order; None holds the calls
        that belonged to no run."""
        found: dict[str | None, list[str | None]] = {}
        for c in self.calls():
            found.setdefault(c.get("run"), []).append(c.get("label"))
        return found

    def tools(self, kind: str | None = None) -> list[tuple[str, str]]:
        """`(tool name, which body ran)` per `@sdk.tool` call, in order: all, or one `kind`'s."""
        return [
            (t["name"], t["ran"]) for t in self.events("tool") if kind is None or t["kind"] == kind
        ]

    def query(self, db: str, sql: str) -> list[tuple[Any, ...]]:
        """Rows from the agent's own SQLite file `db`, in its state directory."""
        with contextlib.closing(sqlite3.connect(self.state / db)) as conn:
            return conn.execute(sql).fetchall()

    def exchange_lines(self) -> list[str]:
        """irimi's per-exchange lines: `<answered_by> <kind> <METHOD> <host><path> -> <status>`."""
        return [line for line in self.irimi if " -> " in line and not line.startswith(" ")]

    def stored(self) -> StoreReader:
        """The trace store this run wrote: `irimi shadow`'s default, `$IRIMI_HOME/store` (#70),
        which is `home / "store"` because the harness gives every run a home of its own."""
        return StoreReader(self.home / paths.STORE_DIR_NAME)

    def stored_events(self) -> list[trace.Event]:
        """Every event the store holds: run by run, each in `seq` order, then the unattributed
        ones. Empty when irimi stopped before it opened a store."""
        reader = self.stored()
        ids = [record.run_id for record in reader.list_runs()] + [trace.UNATTRIBUTED]
        return [event for run_id in ids for event in reader.load_run(run_id).events]

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
    home, state, cwd, tmp = (workdir / d for d in ("home", "state", "cwd", "tmp"))
    for d in (home, state, cwd, tmp):
        d.mkdir(exist_ok=True)
    obs_path = workdir / "obs.jsonl"
    obs_path.write_text("")

    world = World()
    if scenario.setup is not None:
        scenario.setup(world)
    internet = FakeInternet(world.services())
    internet.start()
    irimi_out: list[str] = []
    reported: list[Exchange] = []
    handed: list[Exchange] = []
    try:
        for host, action, match, times in scenario.faults:
            internet.inject(host, action, match, times)
        env = _agent_env(internet, state, obs_path, home, tmp)
        env.update(scenario.env)
        cmd = [sys.executable, "-m", "examples.workflows.launch", workflow.module, *scenario.argv]
        if mode == "bare":
            code = _run_bare(cmd, env, cwd, internet)
        else:
            with _watching_the_store(reported, handed):
                code, irimi_out = _run_shadow(cmd, env, cwd, tmp, internet, scenario, workdir)
    finally:
        internet.stop()
    obs = [json.loads(line) for line in obs_path.read_text().splitlines() if line.strip()]
    return Result(
        workflow,
        scenario_name,
        mode,
        code,
        obs,
        irimi_out,
        world,
        internet,
        home,
        state,
        cwd,
        tmp,
        reported,
        handed,
    )


def _agent_env(
    internet: FakeInternet, state: Path, obs_path: Path, home: Path, tmp: Path
) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k in INHERITED_ENV or k.startswith("LC_")}
    env.update(CANARIES)
    env.update(
        {
            "STRIPE_API_BASE": internet.base("api.stripe.com"),
            "SLACK_API_BASE": internet.base("slack.com"),
            "SLACK_HOOKS_API_BASE": internet.base("hooks.slack.com"),
            "ANTHROPIC_API_BASE": internet.base("api.anthropic.com"),
            "OPENAI_API_BASE": internet.base("api.openai.com"),
            "LANGSMITH_API_BASE": internet.base("api.smith.langchain.com"),
            "WORKFLOW_INTERNET_PORT": str(internet.port),
            "WORKFLOW_STATE": str(state),
            "WORKFLOW_OBS": str(obs_path),
            "WORKFLOW_HTTP_TIMEOUT": "5",
            "PYTHONPATH": str(REPO_ROOT),
            "PYTHONUNBUFFERED": "1",
            "IRIMI_HOME": str(home),
            "TMPDIR": str(tmp),
        }
    )
    return env


def _run_bare(cmd: list[str], env: dict[str, str], cwd: Path, internet: FakeInternet) -> int:
    """The agent as a plain subprocess. Every proxy variable names the fake internet, so a client
    that honours any of them never dials a real address: an `https://` URL gets the fake's refusal
    of `CONNECT`, not a real TLS connection."""
    proxy = f"http://127.0.0.1:{internet.port}"
    env = {**env, **dict.fromkeys(PROXY_VARS, proxy)}
    env["NO_PROXY"] = env["no_proxy"] = runner.NO_PROXY_VALUE  # as `irimi shadow` sets it
    returncode = subprocess.run(cmd, env=env, cwd=cwd, timeout=180, check=False).returncode
    return runner.exit_code_for(returncode)


def _run_shadow(
    cmd: list[str],
    env: dict[str, str],
    cwd: Path,
    tmp: Path,
    internet: FakeInternet,
    scenario: Scenario,
    workdir: Path,
) -> tuple[int, list[str]]:
    """`irimi shadow -- <agent>` in this process, as a user would run it from `cwd`.

    irimi's L3 read is urllib in this process. With no proxy variable set, urllib on macOS falls
    back to the system's proxy settings, which would carry the read (and its canary credential)
    past the fake names to a real proxy. `no_proxy=*` keeps it direct. irimi overwrites both
    spellings for its child, so the agent still sees exactly irimi's `NO_PROXY`.
    """
    buf = io.StringIO()
    harness_env = {**env, "NO_PROXY": "*", "no_proxy": "*"}
    with (
        _environ(harness_env),
        contextlib.chdir(cwd),
        _tempdir(tmp),
        _maps(scenario, workdir),
        internet.resolving(),
    ):
        ca.generate_ca(ca.ca_paths())
        with contextlib.redirect_stdout(buf):
            code = irimi_main(["shadow", "--port", "0", "--", *cmd])
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
def _watching_the_store(reported: list[Exchange], handed: list[Exchange]) -> Iterator[None]:
    """`irimi shadow` unchanged, but each exchange it prints a line for is appended to `reported`,
    and each one its engine hands the trace store to `handed`. `cli` imports `DirectoryStore` when
    it opens the store and calls `report.exchange_line` from its `on_exchange`, so swapping the two
    modules' names is enough, as `tests/test_trace_e2e.py` does (#70)."""
    real = irimi_store.DirectoryStore
    line = report.exchange_line

    def printing(ex: Exchange) -> str:
        reported.append(ex)
        return line(ex)

    class Handed(real):
        def record(self, exchange: Exchange) -> None:
            handed.append(exchange)
            super().record(exchange)

    irimi_store.DirectoryStore = Handed
    report.exchange_line = printing
    try:
        yield
    finally:
        irimi_store.DirectoryStore = real
        report.exchange_line = line


@contextlib.contextmanager
def _tempdir(path: Path) -> Iterator[None]:
    """`tempfile`'s directory in this process: it reads TMPDIR once and caches it."""
    saved = tempfile.tempdir
    tempfile.tempdir = str(path)
    try:
        yield
    finally:
        tempfile.tempdir = saved


@contextlib.contextmanager
def _maps(scenario: Scenario, workdir: Path) -> Iterator[None]:
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
    if shadow.mode != "shadow":
        raise ValueError(f"check_invariants takes a shadow run, not a {shadow.mode} one")
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
    leaked = [
        found for root in (shadow.home, shadow.cwd, shadow.tmp) for found in _canaries_under(root)
    ]
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
    needles = {name: value.encode() for name, value in {**CANARIES, **SERVED_CANARIES}.items()}
    found = []
    for path in root.rglob("*"):
        if path.is_file():
            data = path.read_bytes()
            found += [f"{name} in {path}" for name, v in needles.items() if v in data]
    return found
