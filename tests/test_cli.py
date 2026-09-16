import pytest

from irimi import __version__
from irimi.cli import main


def test_version_flag(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"irimi {__version__}"


def test_no_command_prints_help_and_fails(capsys):
    assert main([]) == 1
    assert "usage: irimi" in capsys.readouterr().out
