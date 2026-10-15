"""Run each sample workflow's scenario once per mode per session, however many tests read it.

`run_workflow(name, scenario, mode)` returns the cached `Result`. The universal invariants in
`test_invariants.py` and each workflow's own `test_wNN_*.py` share one run, so adding a test costs
nothing and adding a scenario costs two agent runs (bare and shadow).

`shared_store_run(name, scenario, mode, store)` runs a scenario under `irimi shadow` (or serve)
into the one store its workflow's runs share, or into the one named `store`, once per session, so
`irimi compare` can read any two (#79).

`traced_run(name, scenario)` runs a scenario's shadow run once more, uncached, and returns it with
the most memory Python held at once while it ran (#94).

`replayed(name, scenario, env)` records a scenario under `irimi shadow` and replays its one run in
the same home, with `env` over the scenario's environment, once per session (#82).
"""

import tracemalloc
from pathlib import Path

import pytest

from examples.workflows.harness.run import Result, replay, replayed_run_id, run, workflows

_CACHE: dict[tuple[str, str, str], Result] = {}
_SHARED: dict[tuple[str, str, str], Result] = {}  # (store, scenario, mode)
_STORES: dict[str, Path] = {}
_REPLAYS: dict[tuple[str, str, tuple[tuple[str, str], ...]], tuple[Result, Result]] = {}


@pytest.fixture
def run_workflow(tmp_path_factory):
    def get(name: str, scenario: str, mode: str) -> Result:
        key = (name, scenario, mode)
        if key not in _CACHE:
            workdir = tmp_path_factory.mktemp(f"{name}-{scenario}-{mode}")
            _CACHE[key] = run(workflows()[name], scenario, mode, workdir)
        return _CACHE[key]

    return get


@pytest.fixture
def shared_store_run(tmp_path_factory):
    """Not `run_workflow`'s cached run: that one recorded into a store of its own.

    `store` names one of the workflow's shared stores, for a test whose pin reads a store's
    `unattributed`: every serve run recorded into a store files what no run named there (#77), so
    a pin over it holds only while nothing else records into that store."""

    def get(name: str, scenario: str, mode: str = "shadow", store: str = "") -> Result:
        where = f"{name}-{store}" if store else name
        if where not in _STORES:
            _STORES[where] = tmp_path_factory.mktemp(f"{where}-store")
        key = (where, scenario, mode)
        if key not in _SHARED:
            workdir = tmp_path_factory.mktemp(f"{name}-{scenario}-{mode}-shared")
            _SHARED[key] = run(workflows()[name], scenario, mode, workdir, _STORES[where])
        return _SHARED[key]

    return get


@pytest.fixture
def traced_run(run_workflow, tmp_path_factory):
    """`(Result, peak bytes)` of one more shadow run of a scenario, with `tracemalloc` tracing every
    allocation Python makes in this process, which is where `irimi shadow` runs: its hooks, its
    store and the fake internet, but not the agent. The cached run goes first, so the imports, the
    CA and anything a scenario builds once are not counted."""

    def get(name: str, scenario: str) -> tuple[Result, int]:
        run_workflow(name, scenario, "shadow")
        workdir = tmp_path_factory.mktemp(f"{name}-{scenario}-traced")
        tracemalloc.start()
        try:
            result = run(workflows()[name], scenario, "shadow", workdir)
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        return result, peak

    return get


@pytest.fixture
def replayed(tmp_path_factory):
    """`(recorded, replay)`: the scenario recorded under `irimi shadow` into a home of its own, and
    its one run (`replayed_run_id`) replayed in that home with `env` over the scenario's own (#82).
    Not `run_workflow`'s shadow run: a replay records into the store it answers from, and that
    run's pins read its store whole. A recording is replayed once, so each `env` records its own."""

    def get(name: str, scenario: str, env: dict[str, str] | None = None) -> tuple[Result, Result]:
        key = (name, scenario, tuple(sorted((env or {}).items())))
        if key not in _REPLAYS:
            workdir = tmp_path_factory.mktemp(f"{name}-{scenario}-replay")
            recorded = run(workflows()[name], scenario, "shadow", workdir / "recorded")
            replayed_run = replay(recorded, replayed_run_id(recorded), workdir / "replay", env)
            _REPLAYS[key] = (recorded, replayed_run)
        return _REPLAYS[key]

    return get
