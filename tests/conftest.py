"""Test-wide isolation from the developer's own irimi setup.

Isolate every test from the developer's own `$IRIMI_HOME` and `./irimi.maps.yaml`, and reset the
SDK's process-wide state after each test. The fixture is autouse, so a new test file inherits it.
"""

import pytest

from irimi.config import paths
from irimi.sdk import _state


@pytest.fixture(autouse=True)
def no_ambient_irimi(tmp_path, monkeypatch):
    """$IRIMI_HOME and the working directory both point somewhere fresh and empty, and no
    $IRIMI_STORE moves the store out of that home."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "irimi-home"))
    monkeypatch.delenv(paths.IRIMI_STORE_ENV, raising=False)
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)


@pytest.fixture(autouse=True)
def no_sdk_state_left_behind():
    """Every patch a test's runs made to the HTTP clients is undone after it, and the SDK's
    reporter and once-only warnings start fresh: `instrument()` patches classes for the life of
    the process, which here is the whole suite."""
    yield
    _state.reset_for_tests()
