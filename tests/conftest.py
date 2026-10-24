"""Isolate every test from the developer's own `$IRIMI_HOME` and `./irimi.maps.yaml`.

Both fixtures are autouse, and the SDK's process-wide state is reset after each test.
"""

import pytest

from irimi.config import paths
from irimi.sdk import _state

pytest_plugins = ["tests.support.fixtures"]


@pytest.fixture(autouse=True)
def no_ambient_irimi(tmp_path, monkeypatch):
    """Point `$IRIMI_HOME` and the working directory at empty dirs, and unset `$IRIMI_STORE`."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "irimi-home"))
    monkeypatch.delenv(paths.IRIMI_STORE_ENV, raising=False)
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)


@pytest.fixture(autouse=True)
def no_sdk_state_left_behind():
    """Undo the SDK's HTTP patches and reset its reporter and once-only warnings after each test.

    `instrument()` patches classes for the life of the process, which here is the whole suite."""
    yield
    _state.reset_for_tests()
