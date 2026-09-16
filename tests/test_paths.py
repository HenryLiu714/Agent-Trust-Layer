from pathlib import Path

from irimi import paths


def test_irimi_home_defaults_to_dot_irimi(monkeypatch):
    monkeypatch.delenv(paths.IRIMI_HOME_ENV, raising=False)
    assert paths.irimi_home() == Path.home() / ".irimi"


def test_irimi_home_honours_env(monkeypatch, tmp_path):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    assert paths.irimi_home() == tmp_path
    assert paths.ca_dir() == tmp_path / "ca"


def test_irimi_home_expands_tilde(monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, "~/custom")
    assert paths.irimi_home() == Path.home() / "custom"
