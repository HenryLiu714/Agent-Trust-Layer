"""Run each sample workflow's scenario once per mode per session, however many tests read it.

`run_workflow(name, scenario, mode)` returns the cached `Result`. The universal invariants in
`test_invariants.py` and each workflow's own `test_wNN_*.py` share one run, so adding a test costs
nothing and adding a scenario costs two agent runs (bare and shadow).
"""

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
