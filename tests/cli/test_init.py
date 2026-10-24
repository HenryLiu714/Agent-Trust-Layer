"""`irimi init`: making the CA, keeping one that exists, and refusing a key it did not write."""

from irimi.cli import main
from irimi.cli.init import NON_HTTP_NOTICE
from irimi.config import ca, paths


def _setup(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    return ca.ca_paths()


def test_init_creates_ca_and_prints_paths(tmp_path, monkeypatch, capsys):
    p = _setup(tmp_path, monkeypatch)
    assert main(["init"]) == 0
    out = capsys.readouterr().out
    assert p.key.is_file() and p.cert.is_file()
    assert str(p.key) in out and str(p.cert) in out
    assert out.splitlines()[-2:] == [NON_HTTP_NOTICE, "next: irimi shadow -- python3 your_agent.py"]


def test_a_second_init_changes_nothing(tmp_path, monkeypatch, capsys):
    """An existing CA is left alone, and only the run that generates one prints the HTTP note."""
    p = _setup(tmp_path, monkeypatch)
    assert main(["init"]) == 0
    key_before, cert_before = p.key.read_bytes(), p.cert.read_bytes()
    capsys.readouterr()
    assert main(["init"]) == 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.splitlines() == [
        f"hint: CA already exists at {p.key.parent}; use --force to regenerate"
    ]
    assert p.key.read_bytes() == key_before
    assert p.cert.read_bytes() == cert_before


def test_init_force_makes_a_new_ca(tmp_path, monkeypatch):
    p = _setup(tmp_path, monkeypatch)
    assert main(["init"]) == 0
    key_before = p.key.read_bytes()
    assert main(["init", "--force"]) == 0
    assert p.key.read_bytes() != key_before


def test_init_incomplete_dir_errors_without_force(tmp_path, monkeypatch, capsys):
    p = _setup(tmp_path, monkeypatch)
    assert main(["init"]) == 0
    p.cert.unlink()
    assert main(["init"]) == 2
    assert "incomplete" in capsys.readouterr().err
    assert main(["init", "--force"]) == 0
    assert p.cert.is_file()


def test_init_refuses_a_symlinked_key(tmp_path, monkeypatch, capsys):
    p = _setup(tmp_path, monkeypatch)
    p.key.parent.mkdir(parents=True)
    victim = tmp_path / "victim"
    victim.write_bytes(b"do not touch")
    p.key.symlink_to(victim)
    assert main(["init", "--force"]) == 2
    assert capsys.readouterr().err.splitlines() == [
        f"error: could not write the CA to {p.key.parent}: too many levels of symbolic links"
    ]
    assert victim.read_bytes() == b"do not touch"


def test_the_ca_state_tells_present_partial_and_absent_apart(tmp_path, monkeypatch):
    p = _setup(tmp_path, monkeypatch)
    assert ca.state(p) == "absent"
    ca.generate_ca(p)
    assert ca.state(p) == "present"
    p.cert.unlink()
    assert ca.state(p) == "partial"


def test_the_non_http_notice_names_the_way_to_label_a_non_http_write(tmp_path, monkeypatch, capsys):
    """The notice says a non-HTTP side effect is real only when its function is unlabelled."""
    _setup(tmp_path, monkeypatch)
    assert main(["init"]) == 0
    out = capsys.readouterr().out
    assert "unless" in NON_HTTP_NOTICE and "@sdk.write_tool" in NON_HTTP_NOTICE
    assert NON_HTTP_NOTICE in out
