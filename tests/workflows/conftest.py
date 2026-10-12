"""Run each sample workflow's scenario once per mode per session, however many tests read it.

`run_workflow(name, scenario, mode)` returns the cached `Result`. The universal invariants in
`test_invariants.py` and each workflow's own `test_wNN_*.py` share one run, so adding a test costs
nothing and adding a scenario costs two agent runs (bare and shadow).

`traced_run(name, scenario)` runs a scenario's shadow run once more, uncached, and returns it with
the most memory Python held at once while it ran (#94).
"""

import tracemalloc

import pytest

from examples.workflows.harness.run import Result, run, workflows

_CACHE: dict[tuple[str, str, str], Result] = {}


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
