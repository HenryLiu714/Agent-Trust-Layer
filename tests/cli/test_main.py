"""The `irimi` command itself: parser, help, startup lines and the composition root."""

import argparse
import io
import sys
from pathlib import Path

import pytest

from irimi import __version__, cli, servicemaps
from irimi.cli import compose, main, options
from irimi.config import ca, paths
from irimi.core import exitcodes
from irimi.core.netaddr import Target
from irimi.decide.policy import ShadowPolicy
from irimi.engine import Replay, Serve, Shadow
from irimi.engine.mitm import MitmEngine
from irimi.report.terminal import NOT_LOOPBACK_NOTICE, PROXY_ONLY_NOTICE

# ------------------------------------------------------------- the precondition reader


def test_shadow_mode_is_never_built_without_a_reader(tmp_path, monkeypatch):
    """The composition root gives shadow mode's policy a reader, so precondition checks run.

    A bare `ShadowPolicy()` has none and checks nothing, which is what other tests rely on."""
    from irimi.cli import compose, options
    from irimi.decide.reader import NoReader
    from tests.support.doubles import NullStore

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    index = servicemaps.MapIndex(tuple(servicemaps.loader.load_shipped()))
    args = argparse.Namespace(base_url_host=[], port=4321, bind=options.LISTEN_HOST)
    p = ca.ca_paths()
    run = compose.Run(index, p, "t3st", compose._engine_config(args, index, "t3st", p), NullStore())
    engine = compose.build_engine(run, on_exchange=lambda ex: None)
    assert isinstance(engine, MitmEngine)
    assert isinstance(engine.policy, ShadowPolicy)
    assert not isinstance(engine.policy.reader, NoReader)
    assert engine.policy.maps is run.config.maps
    assert engine.policy.maps is index


def test_version_prints_the_installed_version(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"irimi {__version__}"


def test_no_command_prints_help_and_is_a_usage_error(capsys):
    """A bare `irimi` exits 2, as a bare `irimi runs list --bogus` does: it was invoked wrongly."""
    assert main([]) == exitcodes.ERROR
    assert "usage: irimi" in capsys.readouterr().err


def test_runs_and_maps_with_no_subcommand_list(tmp_path, capsys):
    """`irimi runs` and `irimi maps` are their `list`, options included."""
    assert main(["runs", "--store", str(tmp_path)]) == exitcodes.OK
    assert capsys.readouterr().out == f"no runs in {tmp_path}\n"
    assert main(["maps"]) == exitcodes.OK
    listed = capsys.readouterr().out
    assert main(["maps", "list"]) == exitcodes.OK
    assert capsys.readouterr().out == listed


def test_maps_list_previews_a_target(capsys):
    """`maps list` takes the map options, so a `--target` shows before any proxy starts."""
    assert main(["maps", "list", "--target", "api.stripe.com=http://127.0.0.1:3000"]) == 0
    assert "http://127.0.0.1:3000" in capsys.readouterr().out
    assert main(["maps", "--target", "api.stripe.com=http://example.com"]) == exitcodes.ERROR
    assert "is not loopback" in capsys.readouterr().err


def test_irimi_store_is_the_default_store(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv(paths.IRIMI_STORE_ENV, str(tmp_path / "elsewhere"))
    assert main(["runs", "list"]) == exitcodes.OK
    assert capsys.readouterr().out == f"no runs in {tmp_path / 'elsewhere'}\n"


@pytest.mark.parametrize(
    "argv",
    [["runs", "list", "--", "x"], ["compare", "a", "b", "--", "x"], ["init", "--"]],
)
def test_only_shadow_and_replay_take_a_command(argv, capsys):
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == exitcodes.ERROR
    assert (
        f"unrecognized arguments: {' '.join(argv[argv.index('--') :])}" in capsys.readouterr().err
    )


def test_everything_after_the_first_double_dash_is_the_command():
    """`--` is split off before argparse sees it, so the command's flags are never irimi's."""
    args = cli.parse_args(["shadow", "--quiet", "--", "env", "--", "--port", "9"])
    assert (args.cmd, args.quiet, args.port) == (["env", "--", "--port", "9"], True, 0)
    replay = cli.parse_args(["replay", "r1", "--", "python3", "-m", "agent", "--quiet"])
    assert (replay.run_id, replay.cmd, replay.quiet) == (
        "r1",
        ["python3", "-m", "agent", "--quiet"],
        False,
    )


def test_a_stdout_closed_before_irimi_started_is_written_nothing(tmp_path, monkeypatch):
    """`main` does not raise when stdout is None, as Python sets it when fd 1 starts closed."""
    monkeypatch.setattr(sys, "stdout", None)
    assert main(["runs", "list", "--store", str(tmp_path)]) == 0


class _GoneReader(io.StringIO):
    """A stream whose reader has gone: every write fails as a closed pipe's does."""

    def write(self, text: str) -> int:
        raise BrokenPipeError(32, "Broken pipe")


def test_a_stderr_whose_reader_went_with_stdout_closed_ends_quietly(tmp_path, monkeypatch):
    """A broken stderr with stdout None exits 141 and raises nothing past `main`."""
    monkeypatch.setattr(sys, "stdout", None)
    monkeypatch.setattr(sys, "stderr", _GoneReader())
    assert main(["runs", "show", "absent", "--store", str(tmp_path)]) == exitcodes.BROKEN_PIPE


def test_python_m_irimi_is_the_command_and_importing_it_runs_nothing():
    """`python -m irimi` runs the command, and importing `irimi.__main__` runs nothing."""
    import subprocess

    ran = subprocess.run(
        [sys.executable, "-m", "irimi", "--version"], capture_output=True, check=False
    )
    assert (ran.returncode, ran.stdout) == (0, f"irimi {__version__}\n".encode())
    imported = subprocess.run(
        [sys.executable, "-c", "import irimi.__main__"], capture_output=True, check=False
    )
    assert (imported.returncode, imported.stdout, imported.stderr) == (0, b"", b"")


def test_a_command_s_proxy_takes_any_free_port_and_a_sidecar_s_a_fixed_one(capsys):
    """`shadow` and `replay` default to a free port, and `serve` to a fixed one.

    A child learns its proxy's port from its environment; a sidecar's agents are pointed at one."""
    assert cli.parse_args(["serve"]).port == options.DEFAULT_PORT
    assert cli.parse_args(["replay", "abc"]).port == 0
    assert cli.parse_args(["shadow", "--", "true"]).port == 0
    with pytest.raises(SystemExit):
        main(["shadow", "--help"])
    assert "0, any free port" in " ".join(capsys.readouterr().out.split())


def test_no_module_reads_a_mode_from_the_environment():
    src = Path(__file__).resolve().parents[2] / "src" / "irimi"
    offenders = [
        f"{py.name}: {line.strip()}"
        for py in src.rglob("*.py")
        for line in py.read_text().splitlines()
        if "IRIMI_MODE" in line
    ]
    assert offenders == [], "mode comes only from the subcommand:\n" + "\n".join(offenders)


@pytest.mark.parametrize(
    ("command", "codes"),
    [
        ("shadow", [2, 126, 127, 130, 141, 143]),
        ("replay", [0, 1, 2, 130, 141, 143]),
        ("compare", [0, 1, 2, 141]),
    ],
)
def test_the_exit_codes_a_ci_job_keys_on_are_in_the_help(command, codes, capsys):
    with pytest.raises(SystemExit):
        main([command, "--help"])
    out = capsys.readouterr().out
    table = out[out.index("exit status:\n") :].splitlines()[1:]
    listed = [int(line.split()[0]) for line in table if line.split()[0].isdigit()]
    assert listed == codes
    assert all(f"  {code:<5}{exitcodes.MEANINGS[code]}" in table for code in codes)


def test_every_command_has_a_description_and_examples():
    parser = cli.build_parser()
    commands = [*cli._parser(None)[1].items()]
    for name in ("runs", "maps"):
        nested = cli._parser(name)[1][name]
        sub = next(a for a in nested._actions if isinstance(a, argparse._SubParsersAction))
        commands += [(f"{name} {n}", p) for n, p in sub.choices.items()]
    assert parser.epilog is not None and parser.epilog.startswith("examples:")
    for name, command in commands:
        assert command.description, name
        assert command.epilog is not None and command.epilog.startswith("examples:\n  irimi "), name


def _subcommand_helps(parser: argparse.ArgumentParser) -> dict[str, str | None]:
    """Return each of `parser`'s subcommands by name, with its one-line `help=`."""
    subs = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
    return {choice.dest: choice.help for sub in subs for choice in sub._choices_actions}


def test_the_readme_cli_block_says_what_each_command_s_help_says():
    """Every command the README's CLI block lists exists, described in its own `help=` words."""
    import re

    readme = (Path(__file__).resolve().parents[2] / "README.md").read_text()
    block = readme.split("\n## CLI\n", 1)[1].split("```\n", 2)[1]
    listed = []
    for line in block.splitlines():
        usage, description = re.split(r"\s{2,}", line.strip(), maxsplit=1)
        words = usage.split()
        assert words[0] == "irimi", line
        root, commands = cli._parser(words[1])
        said, command = _subcommand_helps(root), words[1]
        assert command in said, line
        # `runs show`, and `runs [list]`: a subcommand, the default one in brackets.
        nested = _subcommand_helps(commands[command])
        if len(words) > 2 and words[2].strip("[]") in nested:
            said, command = nested, words[2].strip("[]")
        assert description == said[command], line
        listed.append(words[1])
    assert set(listed) == set(cli.COMMANDS)


def test_shadow_is_listed_before_serve(capsys):
    with pytest.raises(SystemExit):
        main(["--help"])
    out = capsys.readouterr().out
    assert out.index("    shadow ") < out.index("    serve ")


def _shipped_index():
    """Return the shipped maps with no overrides file in play."""
    from irimi import servicemaps

    return servicemaps.load(maps_dir=servicemaps.loader.shipped_dir())


def _reverse_hosts(args, index):
    return compose._engine_config(args, index, "t3st", ca.ca_paths()).reverse_hosts


def test_reverse_hosts_are_the_mapped_hosts():
    index = _shipped_index()
    args = cli.parse_args(["serve"])
    assert _reverse_hosts(args, index) == index.hosts
    assert {"api.stripe.com", "slack.com"} <= index.hosts


def test_reverse_hosts_adds_base_url_host_lower_cased():
    index = _shipped_index()
    args = cli.parse_args(
        ["serve", "--base-url-host", " Foo.Example ", "--base-url-host", "bar.example"]
    )
    assert _reverse_hosts(args, index) == index.hosts | {"foo.example", "bar.example"}


def test_engine_config_carries_the_maps():
    """The engine config holds the maps the classifier uses, `--bind`'s address and the mode."""
    from irimi.cli.compose import _engine_config

    index = _shipped_index()
    args = argparse.Namespace(base_url_host=[], port=4321, bind="0.0.0.0")
    cfg = _engine_config(args, index, "t3st", ca.ca_paths())
    assert cfg.maps is index
    assert cfg.reverse_hosts == index.hosts
    assert (cfg.run_id, cfg.listen_host, cfg.listen_port) == ("t3st", "0.0.0.0", 4321)
    assert cfg.mode == Shadow()
    assert _engine_config(args, index, "t3st", ca.ca_paths(), Serve()).mode == Serve()
    for command in (["serve"], ["shadow", "--", "true"]):
        assert cli.parse_args(command).bind == options.LISTEN_HOST


def test_the_composition_root_builds_the_service_overlay_and_records_into_the_run_s_store():
    """The engine gets a `ServiceOverlay` and records into the store `prepare_run` opened."""
    from irimi.decide.overlay import ServiceOverlay
    from irimi.engine.mitm import MitmEngine
    from tests.support.doubles import NullStore

    index = _shipped_index()
    args = argparse.Namespace(base_url_host=[], port=4321, bind=options.LISTEN_HOST)
    p = ca.ca_paths()
    run = compose.Run(index, p, "t3st", compose._engine_config(args, index, "t3st", p), NullStore())
    engine = compose.build_engine(run, on_exchange=lambda ex: None)
    assert isinstance(engine, MitmEngine)
    assert isinstance(engine.overlay, ServiceOverlay)
    assert engine.config.maps is index
    assert engine.store is run.store


def test_the_composition_root_builds_a_replay_policy_over_the_run_s_recording():
    """A replay config's policy and reader use the recording; shadow mode reads upstream."""
    from dataclasses import replace

    from irimi.decide.policy import ShadowPolicy
    from irimi.decide.reader import UpstreamReader
    from irimi.engine.mitm import MitmEngine
    from irimi.replay.index import RecordingReader, ReplayIndex
    from irimi.replay.policy import ReplayPolicy
    from tests.support.doubles import NullStore

    index = _shipped_index()
    args = argparse.Namespace(base_url_host=[], port=4321, bind=options.LISTEN_HOST)
    p = ca.ca_paths()
    config = compose._engine_config(args, index, "t3st", p)
    recording = ReplayIndex.from_events([], b"k" * 32, index)
    run = compose.Run(
        index, p, "t3st", replace(config, mode=Replay("rec1")), NullStore(), None, recording
    )
    engine = compose.build_engine(run, on_exchange=lambda ex: None)
    assert isinstance(engine, MitmEngine)
    assert isinstance(engine.policy, ReplayPolicy)
    assert engine.policy.recording is recording
    assert engine.policy.maps is index
    assert isinstance(engine.policy.shadow.reader, RecordingReader)
    shadow = compose.build_engine(
        compose.Run(index, p, "t3st", config, NullStore()), lambda ex: None
    )
    assert isinstance(shadow, MitmEngine)
    assert isinstance(shadow.policy, ShadowPolicy)
    assert isinstance(shadow.policy.reader, UpstreamReader)


@pytest.mark.parametrize(
    "argv",
    [
        ["replay", "r1", "--port", "0", "--bogus"],
        ["replay", "r1", "--port", "0", "--bogus", "--", "true"],
        ["runs", "list", "--bogus"],
    ],
)
def test_a_word_no_argument_takes_is_refused_as_parse_args_refuses_it(argv, tmp_path, capsys):
    """A stray word before `--` gets argparse's message and exit 2, with or without a command."""
    store = tmp_path / "store"
    with pytest.raises(SystemExit) as exit_:
        main([*argv[:2], "--store", str(store), *argv[2:]])
    assert exit_.value.code == 2
    assert "irimi: error: unrecognized arguments: --bogus" in capsys.readouterr().err
    assert not store.exists()


@pytest.mark.parametrize(
    ("argv", "abbreviated"),
    [
        (["shadow", "--allow-r", "x", "--", "true"], "--allow-r"),
        (["runs", "list", "--lim", "1"], "--lim"),
    ],
)
def test_an_abbreviated_flag_is_a_usage_error(argv, abbreviated, tmp_path, monkeypatch, capsys):
    """No flag is accepted by a prefix, so one that lets traffic leave is typed in full."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    with pytest.raises(SystemExit) as exit_:
        main(argv)
    assert exit_.value.code == 2
    assert f"unrecognized arguments: {abbreviated}" in capsys.readouterr().err


def test_engine_config_defaults_to_no_maps(tmp_path, monkeypatch):
    """An `EngineConfig` built without maps has an empty index; the CLI always passes one."""
    from irimi.engine import EngineConfig

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    cfg = EngineConfig(
        run_id="t3st",
        ca=ca.ca_paths(),
        confdir=paths.mitm_dir(),
        listen_host=options.LISTEN_HOST,
        listen_port=0,
    )
    assert cfg.maps.services == ()
    assert cfg.maps.hosts == frozenset()


@pytest.mark.parametrize("value", ["127.0.0.1:8443", "https://x.example", "x.example/", " "])
def test_base_url_host_rejects_scheme_port_or_path(value, capsys):
    with pytest.raises(SystemExit) as exc:
        main(["serve", "--base-url-host", value])
    assert exc.value.code == 2
    assert "bare host name" in capsys.readouterr().err


def test_base_url_host_is_lower_cased():
    args = cli.parse_args(["serve", "--base-url-host", " Foo.Example "])
    assert args.base_url_host == ["foo.example"]


def test_target_arg_parses_a_service_and_a_route_spec():
    from irimi.cli.options import _target_arg

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
    """`api.stripe.com/=<url>`, as nginx's `proxy_pass` spells it, targets the whole service."""
    from irimi.cli.options import _target_arg

    assert _target_arg("api.stripe.com/=http://127.0.0.1:3000") == (
        "api.stripe.com",
        "",
        "http://127.0.0.1:3000",
    )
    # And the loader takes it: the spelling has to reach the service target, not merely parse.
    from irimi import servicemaps

    index = servicemaps.load(targets=[_target_arg("api.stripe.com/=http://127.0.0.1:3000")])
    stripe = index.service_for("api.stripe.com")
    assert stripe is not None and stripe.target is not None
    assert stripe.target.url == "http://127.0.0.1:3000"


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
    from irimi.cli.options import _target_arg

    with pytest.raises(argparse.ArgumentTypeError):
        _target_arg(value)


@pytest.mark.parametrize("command", ["serve", "shadow"])
def test_the_target_flags_are_on_both_engine_commands(command):
    args = cli.parse_args(
        [
            command,
            "--target",
            "api.stripe.com/v1/refunds=http://127.0.0.1:3000",
            "--target-reads",
            "api.stripe.com",
            "--allow-remote-target",
            "stub.internal",
        ]
        + (["--", "true"] if command == "shadow" else [])
    )
    assert args.target == [("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000")]
    assert args.target_reads == ["api.stripe.com"]
    assert args.allow_remote_target == ["stub.internal"]


def test_a_non_loopback_target_fails_closed_without_the_escape_hatch(capsys):
    code = main(["serve", "--port", "0", "--target", "api.stripe.com=http://example.com"])
    assert code == exitcodes.ERROR
    assert "is not loopback" in capsys.readouterr().err


def test_a_target_on_the_listeners_own_port_fails_closed(capsys):
    code = main(["serve", "--port", "4000", "--target", "api.stripe.com=http://127.0.0.1:4000"])
    assert code == exitcodes.ERROR
    assert "own listener" in capsys.readouterr().err


def test_a_target_on_the_bind_address_fails_closed(capsys):
    """A target on `--bind`'s address and irimi's own port is refused before anything starts."""
    code = main(
        [
            "serve",
            "--port",
            "4000",
            "--bind",
            "10.0.0.5",
            "--allow-remote-target",
            "10.0.0.5",
            "--target",
            "api.stripe.com=http://10.0.0.5:4000",
        ]
    )
    assert code == exitcodes.ERROR
    assert "own listener" in capsys.readouterr().err


def test_a_target_naming_an_unmapped_host_fails_closed(capsys):
    code = main(["serve", "--port", "0", "--target", "nope.example=http://127.0.0.1:3000"])
    assert code == exitcodes.ERROR
    assert "no loaded service map claims host" in capsys.readouterr().err


def test_the_escape_hatch_warning_says_what_it_allows():
    from irimi.cli.compose import _escape_hatch_warning

    plain = cli.parse_args(["serve"])
    assert _escape_hatch_warning(plain) is None
    loud = cli.parse_args(["serve", "--allow-remote-target", "stub.internal"])
    warning = _escape_hatch_warning(loud)
    assert warning == (
        "warning: targets may leave this machine (--allow-remote-target stub.internal)"
    )


def test_a_ca_near_its_expiry_is_warned_of(tmp_path, monkeypatch):
    """Within `ca.EXPIRY_WARNING` of the CA's end, or after it, startup warns and names the fix."""
    import datetime as dt

    from irimi.cli.compose import _ca_expiry_warning

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    p = ca.ca_paths()
    ca.generate_ca(p)
    expires = ca.expires_at(p.cert)
    assert expires is not None
    now = dt.datetime.now(dt.UTC)
    assert dt.timedelta(days=ca.CA_VALID_DAYS - 1) < expires - now <= dt.timedelta(days=825)
    assert _ca_expiry_warning(p.cert) is None
    assert _ca_expiry_warning(tmp_path / "missing.pem") is None
    for ends, said in (
        (now + dt.timedelta(days=10), "expires"),
        (now - dt.timedelta(1), "expired"),
    ):
        monkeypatch.setattr(ca, "expires_at", lambda cert, ends=ends: ends)
        assert _ca_expiry_warning(p.cert) == (
            f"warning: irimi's CA {said} on {ends.date().isoformat()}; run irimi init --force "
            "for a new one"
        )


# ------------------------------------------------- the startup lines reach a pipe (flushing)


class _PipedStdout(io.StringIO):
    """A stdout that behaves like a pipe: written text is invisible until `flush()`.

    `serve` may print nothing else for minutes, so an unflushed line would sit in the buffer."""

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
    return servicemaps.ServiceMap(
        service="stripe",
        hosts=frozenset({"api.stripe.com"}),
        routes=(),
        target=Target.parse(target),
    )


def _startup_args(**kwargs):
    kwargs.setdefault("allow_remote_target", [])
    return argparse.Namespace(**kwargs)


def _startup_on_a_pipe(monkeypatch, index, args=None):
    out = _PipedStdout()
    monkeypatch.setattr(sys, "stdout", out)
    compose.print_startup(
        "serve", args or _startup_args(), index, "7f3a", "127.0.0.1", 4000, Path("/ca.pem")
    )
    return out.visible


def test_the_banner_reaches_a_piped_stdout_before_anything_else_is_printed(monkeypatch):
    """`irimi serve | tee` shows the banner at once, before any exchange flushes it."""
    visible = _startup_on_a_pipe(monkeypatch, servicemaps.MapIndex())
    assert "listening on 127.0.0.1:4000" in visible
    assert "/ca.pem" in visible
    assert PROXY_ONLY_NOTICE in visible


def test_a_delegated_services_warning_reaches_a_piped_stdout_too(monkeypatch):
    """The line saying requests leave this machine is flushed at startup too."""
    sm = _delegated_service("http://10.0.0.9:3000")
    visible = _startup_on_a_pipe(monkeypatch, servicemaps.MapIndex((sm,)))
    assert "delegated: stripe -> http://10.0.0.9:3000 (writes)" in visible
    assert NOT_LOOPBACK_NOTICE in visible


def test_the_escape_hatch_warning_goes_to_stderr_flushed(monkeypatch, capsys):
    out = _PipedStdout()
    monkeypatch.setattr(sys, "stdout", out)
    compose.print_startup(
        "shadow",
        _startup_args(allow_remote_target=["stub.internal"]),
        servicemaps.MapIndex(),
        "7f3a",
        "127.0.0.1",
        4000,
        Path("/ca.pem"),
    )
    assert "--allow-remote-target stub.internal" in capsys.readouterr().err
    assert "--allow-remote-target" not in out.visible


def test_serve_and_shadow_print_the_same_startup_lines(monkeypatch):
    """One function prints both, so a line added for one is added for both, flushed alike."""
    source = "".join(py.read_text() for py in Path(cli.__file__).parent.glob("*.py"))
    assert source.count("print_startup(") == 3  # the definition plus one call each
