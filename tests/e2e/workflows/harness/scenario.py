"""Sample workflows and their scenarios, found by name, and the maps and environment a run gets."""

from __future__ import annotations

import contextlib
import importlib
import os
import pkgutil
import shutil
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

from irimi.servicemaps import loader
from tests.e2e.workflows.harness.internet import Req
from tests.e2e.workflows.harness.services import World

REPO_ROOT = Path(__file__).resolve().parents[4]
# One package per workflow, `wNN_<name>/`, holding `agent.py` and `scenarios.py`.
AGENTS_DIR = REPO_ROOT / "tests" / "e2e" / "workflows" / "agents"
AGENTS_PACKAGE = "tests.e2e.workflows.agents"


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
    # The scenario shows a write irimi cannot stop, so the no-write-escapes invariant is inverted.
    leaks: bool = False
    doc: str = ""
    # The irimi flags it runs with under `irimi serve`, which every scenario also runs under.
    serve_flags: tuple[str, ...] = ()
    # The irimi flags it runs with under `irimi shadow -- <cmd>`.
    shadow_flags: tuple[str, ...] = ()


@dataclass(frozen=True)
class Workflow:
    name: str  # the package name, e.g. "w09_scope_gauntlet"
    summary: str
    scenarios: dict[str, Scenario]

    @property
    def module(self) -> str:
        return f"{AGENTS_PACKAGE}.{self.name}.agent"


def workflows() -> dict[str, Workflow]:
    """Return every workflow package under `agents/`, found by name rather than registered."""
    found: dict[str, Workflow] = {}
    for info in pkgutil.iter_modules([str(AGENTS_DIR)]):
        is_workflow = info.ispkg and info.name.startswith("w") and info.name[1:3].isdigit()
        # A package with no scenarios.py is half-written and skipped; an import error still raises.
        if is_workflow and (AGENTS_DIR / info.name / "scenarios.py").exists():
            module = importlib.import_module(f"{AGENTS_PACKAGE}.{info.name}.scenarios")
            found[info.name] = module.WORKFLOW
    return dict(sorted(found.items()))


def cases() -> list[tuple[str, str]]:
    """Return `(workflow, scenario)` for every scenario of every workflow, in order."""
    return [(name, scenario) for name, wf in workflows().items() for scenario in wf.scenarios]


@contextlib.contextmanager
def environ(env: dict[str, str]) -> Iterator[None]:
    """Make `os.environ` exactly `env` for the block."""
    saved = dict(os.environ)
    os.environ.clear()
    os.environ.update(env)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


@contextlib.contextmanager
def scenario_maps(scenario: Scenario, workdir: Path) -> Iterator[None]:
    """Load the shipped maps plus the scenario's own, from a directory of this run's."""
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
