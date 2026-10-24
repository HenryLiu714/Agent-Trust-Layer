"""Fixtures that run each workflow scenario once per mode per session, however many tests read it.

A run can be plain, into a shared store, traced for memory, or replayed."""

import tracemalloc
from pathlib import Path

import pytest

from tests.e2e.workflows.harness.execute import replay, replayed_run_id, run
from tests.e2e.workflows.harness.result import Result
from tests.e2e.workflows.harness.scenario import workflows

_CACHE: dict[tuple[str, str, str], Result] = {}
_SHARED: dict[tuple[str, str, str], Result] = {}  # (store, scenario, mode)
_STORES: dict[str, Path] = {}
# (workflow, scenario, env, the run replayed, by the scenario's command, irimi replay's options,
# the mode the run was recorded in)
_ReplayKey = tuple[str, str, tuple[tuple[str, str], ...], str, bool, tuple[str, ...], str]
_REPLAYS: dict[_ReplayKey, tuple[Result, Result]] = {}


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
    """Run a scenario into its workflow's shared store, or the one `store` names, once per session.

    A pin over a store's `unattributed` needs a store nothing else records into, hence `store`."""

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
    """Return `(Result, peak bytes)` of one more, uncached shadow run, traced by `tracemalloc`.

    The cached run goes first, so imports, the CA and one-off builds are not counted."""

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
def replayed(run_workflow, tmp_path_factory):
    """Return `(recorded, replay)`: the cached shadow or serve run and its replay, once per session.

    The replay runs in a copy of the recording's home and store, so `recorded` keeps its store as
    recorded and `replay.stored()` holds both runs."""

    def get(
        name: str,
        scenario: str,
        env: dict[str, str] | None = None,
        run_id: str | None = None,
        command: bool = False,
        options: tuple[str, ...] = (),
        mode: str = "shadow",
    ) -> tuple[Result, Result]:
        recorded = run_workflow(name, scenario, mode)
        run_id = run_id if run_id is not None else replayed_run_id(recorded)
        env_key = tuple(sorted((env or {}).items()))
        key = (name, scenario, env_key, run_id, command, options, mode)
        if key not in _REPLAYS:
            workdir = tmp_path_factory.mktemp(f"{name}-{scenario}-{mode}-replay")
            _REPLAYS[key] = (
                recorded,
                replay(recorded, run_id, workdir / "replay", env, options, command=command),
            )
        return _REPLAYS[key]

    return get
