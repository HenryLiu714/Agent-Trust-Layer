import pytest

from irimi import servicemap
from irimi.cli import main

BAD_OVERRIDE = "service: stripe\ntarget: http://stub.example:3000\n"
GOOD_OVERRIDE = "service: stripe\ntarget: http://127.0.0.1:3000\n"


def test_maps_list_prints_every_host_with_its_route_count(capsys):
    assert main(["maps", "list"]) == 0
    out = capsys.readouterr().out
    assert "irimi maps · 10 service(s) · 14 host(s) · 8 pattern(s) · 47 route(s)" in out
    assert "api.stripe.com" in out and "stripe" in out and "10 routes" in out
    assert "hooks.slack.com" in out and "slack" in out and "10 routes" in out
    assert "*.ingest.sentry.io" in out and "sentry" in out
    assert "target: self" in out
    assert "overrides from" not in out


def test_maps_list_names_the_overrides_file_and_its_target(tmp_path, capsys):
    (tmp_path / "cwd" / servicemap.CWD_OVERRIDE_NAME).write_text(GOOD_OVERRIDE)
    assert main(["maps", "list"]) == 0
    out = capsys.readouterr().out
    assert "target: http://127.0.0.1:3000" in out
    assert f"overrides from {tmp_path / 'cwd' / servicemap.CWD_OVERRIDE_NAME}" in out


def test_maps_list_reports_a_refused_override(tmp_path, capsys):
    (tmp_path / "cwd" / servicemap.CWD_OVERRIDE_NAME).write_text(BAD_OVERRIDE)
    assert main(["maps", "list"]) == 1
    assert "is not loopback" in capsys.readouterr().err


def test_maps_without_a_subcommand_is_a_usage_error(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["maps"])
    assert exc.value.code == 2


def test_maps_help_mentions_list(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["maps", "--help"])
    assert exc.value.code == 0
    assert "list" in capsys.readouterr().out


@pytest.mark.parametrize("command", ["serve", "shadow"])
def test_a_refused_map_stops_the_proxy_before_anything_else(tmp_path, capsys, command):
    """Fail closed, and before the CA check: a bad policy is not something to start a proxy on."""
    (tmp_path / "cwd" / servicemap.CWD_OVERRIDE_NAME).write_text(BAD_OVERRIDE)
    argv = [command, "--port", "0"] + (["--", "true"] if command == "shadow" else [])
    assert main(argv) == 1
    err = capsys.readouterr().err
    assert "is not loopback" in err
    assert "irimi init" not in err


ROUTE_OVERRIDE = """service: slack
routes:
  - match:
      method: POST
      path: /api/chat.postMessage
    target: http://127.0.0.1:3111
"""


def test_maps_list_shows_a_route_level_target(tmp_path, capsys):
    """The listing read the *service* target, so a target set on a single route was invisible and
    every one of Slack's three hosts printed `target: self` for a partly delegated service (#20)."""
    (tmp_path / "cwd" / servicemap.CWD_OVERRIDE_NAME).write_text(ROUTE_OVERRIDE)
    assert main(["maps", "list"]) == 0
    out = capsys.readouterr().out
    assert "route POST /api/chat.postMessage → http://127.0.0.1:3111" in out
    # Under every host the service claims, because each of those rows says `target: self`.
    assert out.count("route POST /api/chat.postMessage → http://127.0.0.1:3111") == 3


def test_maps_list_adds_no_route_lines_when_no_route_is_delegated(capsys):
    assert main(["maps", "list"]) == 0
    assert "route " not in capsys.readouterr().out
