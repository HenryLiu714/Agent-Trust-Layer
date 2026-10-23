"""Test-wide isolation from the developer's own irimi setup.

Isolate every test from the developer's own `$IRIMI_HOME` and `./irimi.maps.yaml`, and undo the
SDK's HTTP patches after each test. The fixture is autouse, so a new test file inherits it.
"""

import pytest

from irimi import paths
from irimi.sdk import instrumentation


@pytest.fixture(autouse=True)
def no_ambient_irimi(tmp_path, monkeypatch):
    """$IRIMI_HOME and the working directory both point somewhere fresh and empty."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "irimi-home"))
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)


@pytest.fixture(autouse=True)
def no_instrumentation_left_behind():
    """Every patch a test's runs made to the HTTP clients is undone after it: `instrument()`
    patches classes for the life of the process, which here is the whole suite."""
    yield
    instrumentation._uninstall()
