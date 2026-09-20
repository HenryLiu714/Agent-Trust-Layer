import argparse
import io
import socket
import sys
from pathlib import Path

import pytest

from irimi import __version__, ca, cli, paths, runner, servicemap
from irimi.cli import main


def test_version_flag(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"irimi {__version__}"


def test_no_command_prints_help_and_fails(capsys):
    assert main([]) == 1
    assert "usage: irimi" in capsys.readouterr().out


def test_serve_without_ca_fails(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    assert main(["serve"]) == 1
    assert "irimi init" in capsys.readouterr().err


def test_help_lists_serve(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    assert "serve" in capsys.readouterr().out


def test_serve_help_has_port(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["serve", "--help"])
    assert exc.value.code == 0
    assert "--port" in capsys.readouterr().out


def test_serve_port_in_use_fails(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    ca.generate_ca(ca.ca_paths())
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        assert main(["serve", "--port", str(sock.getsockname()[1])]) == 1
        assert "did not start" in capsys.readouterr().err
    finally:
        sock.close()


def test_help_lists_shadow(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    assert "shadow" in capsys.readouterr().out


def test_shadow_help_has_port(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["shadow", "--help"])
    assert exc.value.code == 0
    assert "--port" in capsys.readouterr().out


def test_no_mode_env_var_anywhere():
    src = Path(__file__).resolve().parents[1] / "src" / "irimi"
    offenders = [
        f"{py.name}: {line.strip()}"
        for py in src.rglob("*.py")
        for line in py.read_text().splitlines()
        if "IRIMI_MODE" in line
    ]
    assert offenders == [], "mode comes only from the subcommand:\n" + "\n".join(offenders)


def test_serve_help_has_allow_host(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["serve", "--help"])
    assert exc.value.code == 0
    assert "--allow-host" in capsys.readouterr().out


def test_shadow_help_has_allow_host(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["shadow", "--help"])
    assert exc.value.code == 0
    assert "--allow-host" in capsys.readouterr().out


def _shipped_index():
    """The shipped maps with no overrides file in play: a real ./irimi.maps.yaml or
    $IRIMI_HOME/maps.yaml would otherwise change the index, or refuse to load at all.
    tests/conftest.py points both at fresh empty directories, so there is nothing to isolate."""
    from irimi import servicemap

    return servicemap.load(maps_dir=servicemap.shipped_dir())


def test_reverse_hosts_are_the_mapped_hosts():
    from irimi.cli import _reverse_hosts

    index = _shipped_index()
    assert _reverse_hosts(argparse.Namespace(allow_host=[]), index) == index.hosts
    assert {"api.stripe.com", "slack.com"} <= index.hosts


def test_reverse_hosts_adds_allow_host_lower_cased():
    from irimi.cli import _reverse_hosts

    index = _shipped_index()
    args = argparse.Namespace(allow_host=[" Foo.Example ", "", "bar.example"])
    assert _reverse_hosts(args, index) == index.hosts | {"foo.example", "bar.example"}


def test_engine_config_carries_the_maps():
    """`serve` and `shadow` both build the engine's config here, and the classifier only sees the
    maps because `maps=index` is part of it."""
    from irimi.cli import _engine_config

    index = _shipped_index()
    args = argparse.Namespace(allow_host=[], port=4321)
    cfg = _engine_config(args, index, "t3st", ca.ca_paths())
    assert cfg.maps is index
    assert cfg.reverse_hosts == index.hosts
    assert (cfg.run_id, cfg.listen_host, cfg.listen_port) == ("t3st", paths.LISTEN_HOST, 4321)


def test_engine_config_defaults_to_no_maps(tmp_path, monkeypatch):
    """An EngineConfig built without maps classifies by the verb rule alone. That is the default
    the engine's own tests rely on; the CLI always passes an index."""
    from irimi.engine import EngineConfig

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    cfg = EngineConfig(
        run_id="t3st",
        ca=ca.ca_paths(),
        confdir=paths.mitm_dir(),
        listen_host=paths.LISTEN_HOST,
        listen_port=0,
    )
    assert cfg.maps.services == ()
    assert cfg.maps.hosts == frozenset()


@pytest.mark.parametrize("value", ["127.0.0.1:8443", "https://x.example", "x.example/", " "])
def test_allow_host_rejects_scheme_port_or_path(value, capsys):
    with pytest.raises(SystemExit) as exc:
        main(["serve", "--allow-host", value])
    assert exc.value.code == 2
    assert "bare host name" in capsys.readouterr().err


def test_allow_host_is_lower_cased():
    from irimi.cli import build_parser

    assert build_parser().parse_args(["serve", "--allow-host", " Foo.Example "]).allow_host == [
        "foo.example"
    ]


def test_target_arg_parses_a_service_and_a_route_spec():
    from irimi.cli import _target_arg

    assert _target_arg("api.stripe.com=http://127.0.0.1:3000") == (
        "api.stripe.com",
        "",
        "http://127.0.0.1:3000",
    )
    assert _target_arg("API.Stripe.com/v1/refunds=http://127.0.0.1:3000/refund") == (
        "api.stripe.com",
        "/v1/refunds",
        "http://127.0.0.1:3000/refund",
    )


def test_a_trailing_slash_on_the_host_is_the_service_target():
    """`api.stripe.com/=<url>` is how a reader used to nginx `proxy_pass` spells "the whole
    service". It was read as the route path `/`, which no map claims, so a spelling that means
    the right thing failed with "no `write` or `unknown` route matches '/'" (#32)."""
    from irimi.cli import _target_arg

    assert _target_arg("api.stripe.com/=http://127.0.0.1:3000") == (
        "api.stripe.com",
        "",
        "http://127.0.0.1:3000",
    )
    # And the loader takes it: the spelling has to reach the service target, not merely parse.
    from irimi import servicemap

    index = servicemap.load(targets=[_target_arg("api.stripe.com/=http://127.0.0.1:3000")])
    stripe = index.service_for("api.stripe.com")
    assert stripe is not None and stripe.target == "http://127.0.0.1:3000"


@pytest.mark.parametrize(
    "value",
    [
        "api.stripe.com",  # no '='
        "=http://127.0.0.1:3000",  # no host
        "api.stripe.com=",  # no url
        "api.stripe.com:443=http://127.0.0.1:3000",  # a port belongs in the url, not the host
    ],
)
def test_a_malformed_target_flag_is_an_argparse_error(value):
    from irimi.cli import _target_arg

    with pytest.raises(argparse.ArgumentTypeError):
        _target_arg(value)


@pytest.mark.parametrize("command", ["serve", "shadow"])
def test_the_target_flags_are_on_both_engine_commands(command):
    from irimi.cli import build_parser

    args = build_parser().parse_args(
        [
            command,
            "--target",
            "api.stripe.com/v1/refunds=http://127.0.0.1:3000",
            "--target-reads",
            "api.stripe.com",
            "--allow-target-host",
            "stub.internal",
        ]
        + (["--", "true"] if command == "shadow" else [])
    )
    assert args.target == [("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000")]
    assert args.target_reads == ["api.stripe.com"]
    assert args.allow_target_host == ["stub.internal"]


def test_a_non_loopback_target_fails_closed_without_the_escape_hatch(capsys):
    code = main(["serve", "--port", "0", "--target", "api.stripe.com=http://example.com"])
    assert code == 1
    assert "is not loopback" in capsys.readouterr().err


def test_a_target_on_the_listeners_own_port_fails_closed(capsys):
    code = main(["serve", "--port", "4000", "--target", "api.stripe.com=http://127.0.0.1:4000"])
    assert code == 1
    assert "own listener" in capsys.readouterr().err


def test_a_target_naming_an_unmapped_host_fails_closed(capsys):
    code = main(["serve", "--port", "0", "--target", "nope.example=http://127.0.0.1:3000"])
    assert code == 1
    assert "no loaded service map claims host" in capsys.readouterr().err


def test_the_escape_hatch_warning_says_what_it_allows():
    from irimi.cli import _escape_hatch_warning, build_parser

    plain = build_parser().parse_args(["serve"])
    assert _escape_hatch_warning(plain) is None
    loud = build_parser().parse_args(["serve", "--allow-target-host", "stub.internal"])
    warning = _escape_hatch_warning(loud)
    assert warning.startswith("WARNING:")
    assert "stub.internal" in warning
    assert "leave this machine" in warning


# ------------------------------------------------- the startup lines reach a pipe (flushing)


class _PipedStdout(io.StringIO):
    """A stdout that behaves like a pipe: written text is invisible until `flush()`.

    Python gives a piped stdout a block buffer, so an unflushed `print` sits in it until
    something else forces it out. `serve` may print nothing else for minutes, so this stands in
    for `irimi serve > log` and asserts what a reader of that log can actually see.
    """

    def __init__(self):
        super().__init__()
        self.flushed: list[str] = []
        self._buffer: list[str] = []

    def write(self, text):
        self._buffer.append(text)
        return len(text)

    def flush(self):
        self.flushed.extend(self._buffer)
        self._buffer.clear()

    def isatty(self):
        return False

    @property
    def visible(self) -> str:
        return "".join(self.flushed)


def _delegated_service(target: str):
    return servicemap.ServiceMap(
        service="stripe", hosts=frozenset({"api.stripe.com"}), routes=(), target=target
    )


def _startup_args(**kwargs):
    kwargs.setdefault("allow_target_host", [])
    return argparse.Namespace(**kwargs)


def _startup_on_a_pipe(monkeypatch, index, args=None):
    out = _PipedStdout()
    monkeypatch.setattr(sys, "stdout", out)
    cli.print_startup(
        "serve", args or _startup_args(), index, "7f3a", "127.0.0.1", 4000, Path("/ca.pem")
    )
    return out.visible


def test_the_banner_reaches_a_piped_stdout_before_anything_else_is_printed(monkeypatch):
    """`irimi serve | tee` showed nothing until the first exchange forced a flush, and a serve
    nobody talks to showed nothing at all - not the port, not the CA path, not `backstop`."""
    visible = _startup_on_a_pipe(monkeypatch, servicemap.MapIndex())
    assert "listening on 127.0.0.1:4000" in visible
    assert "/ca.pem" in visible
    assert runner.BACKSTOP_NOTICE in visible
    assert runner.NOT_VIRTUALIZED_NOTICE in visible


def test_a_delegated_services_warning_reaches_a_piped_stdout_too(monkeypatch):
    """The line that says the agent's requests are leaving this machine is the last one that may
    wait for an exchange to flush it."""
    sm = _delegated_service("http://10.0.0.9:3000")
    visible = _startup_on_a_pipe(monkeypatch, servicemap.MapIndex((sm,)))
    assert "delegated: stripe → http://10.0.0.9:3000 (writes)" in visible
    assert runner.NOT_LOOPBACK_NOTICE in visible


def test_the_escape_hatch_warning_goes_to_stderr_flushed(monkeypatch, capsys):
    out = _PipedStdout()
    monkeypatch.setattr(sys, "stdout", out)
    cli.print_startup(
        "shadow",
        _startup_args(allow_target_host=["stub.internal"]),
        servicemap.MapIndex(),
        "7f3a",
        "127.0.0.1",
        4000,
        Path("/ca.pem"),
    )
    assert "--allow-target-host stub.internal" in capsys.readouterr().err
    assert "--allow-target-host" not in out.visible


def test_serve_and_shadow_print_the_same_startup_lines(monkeypatch):
    """They drifted once: `shadow` flushed and `serve` did not. One function now, so a future
    line added for one is added for both."""
    import inspect

    source = inspect.getsource(cli)
    assert source.count("print_startup(") == 3  # the definition plus one call each
