"""Run each sample workflow's scenario once per mode per session, however many tests read it.

`run_workflow(name, scenario, mode)` returns the cached `Result`. The universal invariants in
`test_invariants.py` and each workflow's own `test_wNN_*.py` share one run, so adding a test costs
nothing and adding a scenario costs two agent runs (bare and shadow).

`shared_store_run(name, scenario, mode, store)` runs a scenario under `irimi shadow` (or serve)
into the one store its workflow's runs share, or into the one named `store`, once per session, so
`irimi compare` can read any two (#79).

`traced_run(name, scenario)` runs a scenario's shadow run once more, uncached, and returns it with
the most memory Python held at once while it ran (#94).

`replayed(name, scenario, env, run_id, command, options, mode)` replays a run of a scenario's
shadow run, or its serve run (#85), by `irimi replay`, in a copy of its home, with `env` over the
scenario's environment and `options` given to `irimi replay` (`--call`, `--args`), once per session
(#82, #84).
"""

import tracemalloc
from pathlib import Path

import pytest

from examples.workflows.harness.run import Result, replay, replayed_run_id, run, workflows

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
def replayed(run_workflow, tmp_path_factory):
    """`(recorded, replay)`: `run_workflow`'s shadow run of the scenario, or with `mode="serve"` its
    serve run, a deployed agent's recording (#85), and its run `run_id` - by default its one run,
    `replayed_run_id` - replayed by `irimi replay` with `env` over the scenario's own (#82, #84),
    once per session: what `irimi replay` runs for the run, or with `command` the scenario's own
    agent command, and `options` given to `irimi replay` as well (`--call`, `--args`, #84).

    The replay runs in a copy of the recording's `IRIMI_HOME`, its store included, which `replay`
    makes: a replay records into the store it answers from, and the shadow run's own pins read its
    store whole. So `recorded` is that shadow run, its store as it recorded it, and
    `replay.stored()` holds the recording and the replay. Each replay gets a copy of its own, so no
    replay reads another's, and a replay of this replay copies it in turn (#85)."""

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
            # `replay` runs in a copy of the recording's home of its own (#85), so neither the
            # recording nor this cached replay is changed by a replay made after it.
            workdir = tmp_path_factory.mktemp(f"{name}-{scenario}-{mode}-replay")
            _REPLAYS[key] = (
                recorded,
                replay(recorded, run_id, workdir / "replay", env, options, command=command),
            )
        return _REPLAYS[key]

    return get
