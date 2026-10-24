"""`config.paths`: where irimi keeps its home, and the listener's defaults."""

from pathlib import Path

from irimi.config import paths


def test_irimi_home_defaults_to_dot_irimi(monkeypatch):
    monkeypatch.delenv(paths.IRIMI_HOME_ENV, raising=False)
    assert paths.irimi_home() == Path.home() / ".irimi"


def test_irimi_home_follows_its_environment_variable(monkeypatch, tmp_path):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    assert paths.irimi_home() == tmp_path
    assert paths.ca_dir() == tmp_path / "ca"


def test_irimi_home_expands_a_tilde(monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, "~/custom")
    assert paths.irimi_home() == Path.home() / "custom"


def test_mitmproxys_directory_is_under_irimi_home(monkeypatch, tmp_path):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    assert paths.mitm_dir() == paths.irimi_home() / "mitm"


def test_the_listener_defaults_to_loopback_port_4000():
    from irimi.cli import options

    assert options.DEFAULT_PORT == 4000
    assert options.LISTEN_HOST == "127.0.0.1"
