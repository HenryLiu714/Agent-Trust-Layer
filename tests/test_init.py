from irimi import ca, paths
from irimi.cli import NON_HTTP_NOTICE, main


def _setup(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    return ca.ca_paths()


def test_init_creates_ca_and_prints_paths(tmp_path, monkeypatch, capsys):
    p = _setup(tmp_path, monkeypatch)
    assert main(["init"]) == 0
    out = capsys.readouterr().out
    assert p.key.is_file() and p.cert.is_file()
    assert str(p.key) in out and str(p.cert) in out
    assert NON_HTTP_NOTICE in out


def test_init_second_run_is_noop(tmp_path, monkeypatch, capsys):
    p = _setup(tmp_path, monkeypatch)
    assert main(["init"]) == 0
    key_before, cert_before = p.key.read_bytes(), p.cert.read_bytes()
    capsys.readouterr()
    assert main(["init"]) == 0
    out = capsys.readouterr().out
    assert "already exists" in out
    assert NON_HTTP_NOTICE in out
    assert p.key.read_bytes() == key_before
    assert p.cert.read_bytes() == cert_before


def test_init_force_regenerates(tmp_path, monkeypatch):
    p = _setup(tmp_path, monkeypatch)
    assert main(["init"]) == 0
    key_before = p.key.read_bytes()
    assert main(["init", "--force"]) == 0
    assert p.key.read_bytes() != key_before


def test_init_incomplete_dir_errors_without_force(tmp_path, monkeypatch, capsys):
    p = _setup(tmp_path, monkeypatch)
    assert main(["init"]) == 0
    p.cert.unlink()
    assert main(["init"]) == 1
    assert "incomplete" in capsys.readouterr().err
    assert main(["init", "--force"]) == 0
    assert p.cert.is_file()
