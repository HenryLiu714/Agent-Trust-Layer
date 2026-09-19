"""Test-wide isolation from the developer's own irimi setup.

The map loader reads `./irimi.maps.yaml`, then `$IRIMI_HOME/maps.yaml`. A developer who followed
the README and created an overrides file must not see this suite fail: before this fixture existed
in three separate copies, a real overrides file failed 31 of 76 tests. One copy now, autouse, so a
new test file inherits it instead of rediscovering the problem.
"""

import pytest

from irimi import paths


@pytest.fixture(autouse=True)
def no_ambient_irimi(tmp_path, monkeypatch):
    """$IRIMI_HOME and the working directory both point somewhere fresh and empty."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "irimi-home"))
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
