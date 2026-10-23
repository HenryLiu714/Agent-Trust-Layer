import argparse
import io
import socket
import sys
from pathlib import Path

import pytest

from irimi import __version__, ca, cli, paths, report, servicemap
from irimi.cli import BROKEN_PIPE_EXIT_CODE, main


def test_version_flag(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"irimi {__version__}"


def test_no_command_prints_help_and_fails(capsys):
    assert main([]) == 1
    assert "usage: irimi" in capsys.readouterr().out


def test_a_stdout_closed_before_irimi_started_is_written_nothing(tmp_path, monkeypatch):
    """Python makes `sys.stdout` None when fd 1 is closed at startup (`irimi runs list >&-`):
    `print` writes nothing to it, and `main`'s flush of stdout (#142) must not raise there."""
    monkeypatch.setattr(sys, "stdout", None)
    assert main(["runs", "list", "--store", str(tmp_path)]) == 0


class _GoneReader(io.StringIO):
    """A stream whose reader has gone: every write fails as a closed pipe's does."""

    def write(self, text: str) -> int:
        raise BrokenPipeError(32, "Broken pipe")


def test_a_stderr_whose_reader_went_with_stdout_closed_ends_quietly(tmp_path, monkeypatch):
    """`irimi runs show X >&- 2>&1`-like: the first write that fails is stderr's, and stdout is
    None. That is the reader gone too (#142): 141, and nothing raised past `main`."""
    monkeypatch.setattr(sys, "stdout", None)
    monkeypatch.setattr(sys, "stderr", _GoneReader())
    assert main(["runs", "show", "absent", "--store", str(tmp_path)]) == BROKEN_PIPE_EXIT_CODE


def test_python_m_irimi_is_the_command_and_importing_it_runs_nothing():
    """`sdk.replay()` runs `python -m irimi` (#84); a tool that imports every module, as pydoc
    does, must not start the command."""
    import subprocess

    ran = subprocess.run([sys.executable, "-m", "irimi", "--version"], capture_output=True)
    assert (ran.returncode, ran.stdout) == (0, f"irimi {__version__}\n".encode())
    imported = subprocess.run([sys.executable, "-c", "import irimi.__main__"], capture_output=True)
    assert (imported.returncode, imported.stdout, imported.stderr) == (0, b"", b"")


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
    maps because `maps=index` is part of it. The bind is `--listen-host`'s, and `serve` is set only
    for serve mode, whose health answer says so (#73, #77)."""
    from irimi.cli import _engine_config, build_parser

    index = _shipped_index()
    args = argparse.Namespace(allow_host=[], port=4321, listen_host="0.0.0.0")
    cfg = _engine_config(args, index, "t3st", ca.ca_paths())
    assert cfg.maps is index
    assert cfg.reverse_hosts == index.hosts
    assert (cfg.run_id, cfg.listen_host, cfg.listen_port) == ("t3st", "0.0.0.0", 4321)
    assert cfg.serve is False
    assert _engine_config(args, index, "t3st", ca.ca_paths(), serve=True).serve is True
    for command in (["serve"], ["shadow", "--", "true"]):
        assert build_parser().parse_args(command).listen_host == paths.LISTEN_HOST


def test_the_composition_root_builds_the_service_overlay_over_the_run_s_maps():
    """The overlay classifies a read against the same maps the engine does (#43): a second index
    would let the two disagree about which service a read belongs to. The engine records into the
    store `_prepare_run` opened, the one `irimi shadow` starts and ends its run in (#70)."""
    from irimi.overlay import ServiceOverlay
    from irimi.store import NullStore

    index = _shipped_index()
    args = argparse.Namespace(allow_host=[], port=4321, listen_host=paths.LISTEN_HOST)
    p = ca.ca_paths()
    run = cli._Run(index, p, "t3st", cli._engine_config(args, index, "t3st", p), NullStore())
    engine = cli._build_engine(run, on_exchange=lambda ex: None)
    assert isinstance(engine.overlay, ServiceOverlay)
    assert engine.overlay.maps is run.config.maps
    assert engine.overlay.maps is index
    assert engine.store is run.store


def test_the_composition_root_builds_a_replay_policy_over_the_run_s_recording():
    """A config naming a run to replay gets a `ReplayPolicy` over the recording `_prepare_run`
    read, and a reader over the recording, never the upstream (#82).
    One that names none gets shadow mode's policy, reading the real upstream (#45)."""
    from dataclasses import replace

    from irimi.policy import ReplayPolicy, ShadowPolicy
    from irimi.reader import UpstreamReader
    from irimi.recording import Recording, RecordingReader
    from irimi.store import NullStore

    index = _shipped_index()
    args = argparse.Namespace(allow_host=[], port=4321, listen_host=paths.LISTEN_HOST)
    p = ca.ca_paths()
    config = cli._engine_config(args, index, "t3st", p)
    recording = Recording.from_events([], b"k" * 32, index)
    run = cli._Run(
        index, p, "t3st", replace(config, replay_of="rec1"), NullStore(), None, recording
    )
    engine = cli._build_engine(run, on_exchange=lambda ex: None)
    assert isinstance(engine.policy, ReplayPolicy)
    assert engine.policy.recording is recording
    assert engine.policy.maps is index
    assert isinstance(engine.policy.shadow.reader, RecordingReader)
    shadow = cli._build_engine(cli._Run(index, p, "t3st", config, NullStore()), lambda ex: None)
    assert isinstance(shadow.policy, ShadowPolicy)
    assert isinstance(shadow.policy.reader, UpstreamReader)


def test_a_replay_of_a_run_the_store_does_not_hold_starts_nothing(tmp_path, monkeypatch, capsys):
    """`irimi replay` reads the recording before anything runs, and a run the store does not hold
    is refused, exit 2 as `irimi compare` refuses it: an error, no command and no store (#82,
    #84)."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    monkeypatch.chdir(tmp_path)
    ca.generate_ca(ca.ca_paths())
    marker = tmp_path / "ran"
    command = [sys.executable, "-c", f"open({str(marker)!r}, 'w')"]
    assert main(["replay", "nosuchrun", "--port", "0", "--", *command]) == 2
    assert capsys.readouterr().err == f"error: no run nosuchrun in {tmp_path / 'store'}\n"
    assert not marker.exists()
    assert not (tmp_path / "store").exists()


@pytest.mark.parametrize("damage", ["events", "run.json", "newer", "blob"])
def test_a_replay_of_a_damaged_run_starts_nothing(tmp_path, monkeypatch, capsys, damage):
    """A recording `StoreReader` cannot read - a line that does not parse, a run.json that does
    not, a newer schema version, a missing blob - refuses the replay as `runs show` refuses the
    run, with no traceback, no command and no run of its own in the store, and exits 2 (#82,
    #84)."""
    import json

    from irimi import redact
    from irimi.exchange import Exchange, Request, Response
    from irimi.store import DirectoryStore

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    monkeypatch.chdir(tmp_path)
    ca.generate_ca(ca.ca_paths())
    store = tmp_path / "store"
    recorded = DirectoryStore(store, redact.load_key(tmp_path))
    recorded.record(
        Exchange(
            request=Request("GET", "https", "api.stripe.com", 443, "/v1/charges/ch_1", "", (), b""),
            response=Response(200, (), b"{}"),
            service="stripe",
            operation="charges.retrieve",
            kind="read",
            answered_by="live",
            validation="unvalidated",
            run_id="rec1",
        )
    )
    recorded.close()
    run = store / "runs" / "rec1"
    if damage == "events":
        (run / "events.jsonl").write_text("{\n" + (run / "events.jsonl").read_text())
    elif damage == "run.json":
        (run / "run.json").write_text("{")
    elif damage == "newer":
        record = json.loads((run / "run.json").read_text())
        (run / "run.json").write_text(json.dumps(record | {"schema_version": 3}))
    else:
        for blob in (store / "blobs").iterdir():
            blob.unlink()
    marker = tmp_path / "ran"
    command = [sys.executable, "-c", f"open({str(marker)!r}, 'w')"]
    assert main(["replay", "rec1", "--port", "0", "--", *command]) == 2
    err = capsys.readouterr().err
    assert err.startswith("error: run rec1 could not be read: ")
    assert err.count("\n") == 1
    assert not marker.exists()
    assert [p.name for p in (store / "runs").iterdir()] == ["rec1"]


def test_the_unattributed_pool_is_not_a_run_to_replay(tmp_path, monkeypatch, capsys):
    """`unattributed` holds every serve session's requests no run named: there is no one run in it
    to answer from (#82). Refused with exit 2 (#84)."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    monkeypatch.chdir(tmp_path)
    ca.generate_ca(ca.ca_paths())
    command = [sys.executable, "-c", "pass"]
    assert main(["replay", "unattributed", "--port", "0", "--", *command]) == 2
    assert capsys.readouterr().err == "error: unattributed is not a run to replay\n"
    assert not (tmp_path / "store").exists()


@pytest.mark.parametrize(
    "argv",
    [
        ["replay", "r1", "--port", "0", "--bogus"],
        ["replay", "r1", "--port", "0", "--bogus", "--", "true"],
        ["runs", "list", "--bogus"],
    ],
)
def test_a_word_no_argument_takes_is_refused_as_parse_args_refuses_it(argv, tmp_path, capsys):
    """`main` lets a replay's words after its first `--` go unplaced, which an older argparse
    (Python 3.12.3) leaves them once an option sits between RUN_ID and `--`. Any other unplaced
    word is refused with argparse's own message and exit 2, on every Python (#177)."""
    store = tmp_path / "store"
    with pytest.raises(SystemExit) as exit_:
        main([*argv[:2], "--store", str(store), *argv[2:]])
    assert exit_.value.code == 2
    assert "irimi: error: unrecognized arguments: --bogus" in capsys.readouterr().err
    assert not store.exists()


@pytest.mark.parametrize(
    ("argv", "abbreviated"),
    [
        (["shadow", "--allow-t", "x", "--", "true"], "--allow-t"),
        (["runs", "list", "--lim", "1"], "--lim"),
    ],
)
def test_an_abbreviated_flag_is_a_usage_error(argv, abbreviated, tmp_path, monkeypatch, capsys):
    """A flag that lets traffic leave the machine has to be typed in full, so no flag is
    accepted by a prefix of its name."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    with pytest.raises(SystemExit) as exit_:
        main(argv)
    assert exit_.value.code == 2
    assert f"unrecognized arguments: {abbreviated}" in capsys.readouterr().err


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


def test_a_target_on_the_listen_host_fails_closed(capsys):
    """`--listen-host` is the listener's address too, so a target the escape hatch lets name it,
    on irimi's own port, is refused before anything starts (#77)."""
    code = main(
        [
            "serve",
            "--port",
            "4000",
            "--listen-host",
            "10.0.0.5",
            "--allow-target-host",
            "10.0.0.5",
            "--target",
            "api.stripe.com=http://10.0.0.5:4000",
        ]
    )
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
    nobody talks to showed nothing at all - not the port, not the CA path, not the proxy-only
    notice."""
    visible = _startup_on_a_pipe(monkeypatch, servicemap.MapIndex())
    assert "listening on 127.0.0.1:4000" in visible
    assert "/ca.pem" in visible
    assert report.PROXY_ONLY_NOTICE in visible


def test_a_delegated_services_warning_reaches_a_piped_stdout_too(monkeypatch):
    """The line that says the agent's requests are leaving this machine is the last one that may
    wait for an exchange to flush it."""
    sm = _delegated_service("http://10.0.0.9:3000")
    visible = _startup_on_a_pipe(monkeypatch, servicemap.MapIndex((sm,)))
    assert "delegated: stripe → http://10.0.0.9:3000 (writes)" in visible
    assert report.NOT_LOOPBACK_NOTICE in visible


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


# ------------------------------------------------- the trace store under serve and shadow (#70)


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    ca.generate_ca(ca.ca_paths())
    return tmp_path


def _child(source: str = "pass") -> list[str]:
    return ["--port", "0", "--", sys.executable, "-c", source]


def _spy_on_the_store(monkeypatch, close=None):
    """Swap the class `_open_store` builds for a real `DirectoryStore` that remembers each
    `close()` and the thread it came from, and may do `close(store)` after the real one."""
    import threading

    from irimi.store import DirectoryStore

    class _Spy(DirectoryStore):
        closed: list[str] = []

        def close(self):
            super().close()
            _Spy.closed.append(threading.current_thread().name)
            if close is not None:
                close(self)

    monkeypatch.setattr("irimi.store.DirectoryStore", _Spy)
    return _Spy


@pytest.mark.parametrize("command", ["serve", "shadow"])
@pytest.mark.parametrize(
    "key", ["short", "directory", "dangling"], ids=["wrong-size", "directory", "dangling"]
)
def test_a_redaction_key_that_cannot_be_used_stops_the_run_with_one_line(
    home, capsys, command, key
):
    """A run that could not redact what it stores must not start (#69, #70), in `serve` as in
    `shadow`: one `error:` line, exit 1, no traceback, and no store on disk."""
    path = home / paths.REDACT_KEY_NAME
    if key == "short":
        path.write_bytes(b"short")
    elif key == "directory":
        path.mkdir()
    else:
        path.symlink_to(home / "nowhere")
    argv = [command, "--port", "0"] + (
        ["--", sys.executable, "-c", "pass"] if command == "shadow" else []
    )
    assert main(argv) == 1
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and err[0].startswith("error: ") and str(path) in err[0]
    assert not (home / "store").exists()


@pytest.mark.parametrize("command", ["serve", "shadow"])
def test_a_store_that_cannot_be_written_stops_the_run_with_one_line(home, capsys, command):
    """A store that would drop every event behind one warning must not start a run that looks
    recorded (#70), in `serve` as in `shadow`: a file, and - unless root, whom no mode stops - a
    read-only directory and one that cannot be created under it."""
    import os

    stores = [home / "ca" / "ca.pem"]
    if os.geteuid() != 0:
        read_only = home / "read-only"
        read_only.mkdir(mode=0o500)
        stores += [read_only, read_only / "below"]
    for store in stores:
        argv = [command, "--port", "0", "--store", str(store)]
        if command == "shadow":
            argv += ["--", sys.executable, "-c", "pass"]
        assert main(argv) == 1
        err = capsys.readouterr().err.strip().splitlines()
        assert len(err) == 1 and err[0].startswith("error: "), err


def test_shadow_stores_its_process_run_with_the_agent_and_engine_versions(home, monkeypatch):
    """The process run's record (#70): its argv, which a replay runs again, the agent's version
    from `IRIMI_AGENT_VERSION`, this irimi's version, and how the child exited."""
    from irimi.store import StoreReader
    from irimi.trace import ErrorInfo

    monkeypatch.setenv(paths.AGENT_VERSION_ENV, "agent-2.0.1")
    assert main(["shadow", *_child("raise SystemExit(3)")]) == 3
    (record,) = StoreReader(home / "store").list_runs()
    assert (record.attribution, record.mode) == ("process", "shadow")
    assert record.trigger is not None
    assert record.trigger.args == {"argv": [sys.executable, "-c", "raise SystemExit(3)"]}
    assert (record.agent_version, record.engine_version, record.sdk_version) == (
        "agent-2.0.1",
        __version__,
        None,
    )
    assert (record.outcome, record.exit_code, record.error) == (
        "error",
        3,
        ErrorInfo("exit", "exited 3"),
    )
    assert record.started_at is not None and record.ended_at is not None
    assert record.started_at <= record.ended_at
    assert len(record.run_id) == 16


def test_shadow_closes_the_store_when_the_proxy_never_starts(home, monkeypatch, capsys):
    """`cli` opened the store, so `cli` closes it on every path out (#70), and not only when an
    engine thread got far enough to close it for it: one that never started, or outlived
    `handle.stop()`, did not."""
    from irimi import runner
    from irimi.engine import EngineStartError

    spy = _spy_on_the_store(monkeypatch)

    def refuse(engine, timeout=runner.READY_TIMEOUT_S):
        raise EngineStartError("proxy did not start on 127.0.0.1:0: refused by the test")

    monkeypatch.setattr(runner, "start_engine", refuse)
    assert main(["shadow", *_child()]) == 1
    assert "refused by the test" in capsys.readouterr().err
    assert spy.closed != []
    assert list((home / "store").iterdir()) == []  # no run began, so none is on disk


class _ReaderGoneWhen(io.StringIO):
    """stdout, whose reader goes away once `gone` is set: every later write fails as a closed
    pipe's does (#142)."""

    gone = False

    def write(self, text: str) -> int:
        if self.gone:
            raise BrokenPipeError(32, "Broken pipe")
        return super().write(text)


@pytest.mark.parametrize("when", ["at-the-banner", "at-the-summary"])
def test_a_reader_gone_costs_shadow_no_run_and_leaves_no_engine(home, monkeypatch, when):
    """`irimi shadow -- cmd | head -1` (#142). A reader gone by the banner stops irimi before the
    command starts, so no run is stored; one gone by the summary, once the command has ended,
    finds the run ended with the command's exit code. Either way irimi exits 141 with the engine
    stopped and the store closed: the write that failed was inside the `try` whose `finally`
    stops them (`_under_the_engine`), or after it."""
    import threading

    from irimi import runner
    from irimi.store import StoreReader

    spy = _spy_on_the_store(monkeypatch)
    out = _ReaderGoneWhen()
    out.gone = when == "at-the-banner"
    real_end = runner.end_process_run

    def end_then_go(*args, **kwargs):
        real_end(*args, **kwargs)
        out.gone = True

    monkeypatch.setattr(runner, "end_process_run", end_then_go)
    monkeypatch.setattr(sys, "stdout", out)
    before = set(threading.enumerate())
    assert main(["shadow", *_child("raise SystemExit(3)")]) == BROKEN_PIPE_EXIT_CODE
    assert spy.closed != []
    assert [t for t in threading.enumerate() if t not in before and t.is_alive()] == []
    stored = StoreReader(home / "store").list_runs()
    if when == "at-the-banner":
        assert stored == []
    else:
        assert [(r.outcome, r.exit_code) for r in stored] == [("error", 3)]


def test_a_reader_gone_by_serve_s_banner_closes_its_store_and_releases_its_lock(home, monkeypatch):
    """`irimi serve | head -1`, its reader gone before the banner (#142): 141, with the engine
    stopped, the store closed and its `serve.lock` released, so the next serve may take it."""
    import threading

    from irimi.store import ServeLock

    spy = _spy_on_the_store(monkeypatch)
    out = _ReaderGoneWhen()
    out.gone = True
    monkeypatch.setattr(sys, "stdout", out)
    before = set(threading.enumerate())
    assert main(["serve", "--port", "0"]) == BROKEN_PIPE_EXIT_CODE
    assert spy.closed != []
    assert [t for t in threading.enumerate() if t not in before and t.is_alive()] == []
    ServeLock.hold(home / "store").release()


def test_a_ctrl_c_while_the_store_closes_still_prints_the_summary(home, monkeypatch, capsys):
    """The impatient second Ctrl-C of `handle.stop()` can land in the store's close as well, which
    waits up to `store.CLOSE_TIMEOUT_S` for the writer. It must not cost the summary or the
    child's exit code (#70)."""
    import threading

    def interrupt(store):
        if threading.current_thread() is threading.main_thread():
            raise KeyboardInterrupt

    _spy_on_the_store(monkeypatch, close=interrupt)
    try:
        code = main(["shadow", *_child()])
    except KeyboardInterrupt:  # caught here, or it would stop the whole pytest session
        pytest.fail("a Ctrl-C in the store's close escaped `irimi shadow`")
    assert code == 0
    assert "0 exchanges · 0 live" in capsys.readouterr().out


def test_serve_prints_a_line_for_each_tool_call(home, monkeypatch, capsys):
    """`serve` wires the control endpoint's tool calls to its stream, as it does exchanges (#76).
    The engine here refuses to start, so only the wiring is under test. Its `run` serves until
    `shutdown`, as a real engine's does: a `run` that returned at once would stop the loop before
    `wait_ready` ran, and the test would fail on the ready timeout instead of the refusal."""
    import asyncio
    import threading

    from irimi.engine import EngineStartError
    from irimi.trace import ToolCall

    wired = []
    stopped = threading.Event()

    class _Refuses:
        async def run(self) -> None:
            while not stopped.is_set():
                await asyncio.sleep(0.01)

        async def wait_ready(self) -> None:
            raise EngineStartError("not started")

        def shutdown(self) -> None:
            stopped.set()

    def build(run, on_exchange, on_tool_call):
        wired.append(on_tool_call)
        return _Refuses()

    monkeypatch.setattr(cli, "_build_engine", build)
    assert main(["serve", "--port", "0"]) == 1
    assert "error: not started" in capsys.readouterr().err.splitlines()
    [on_tool_call] = wired
    on_tool_call(ToolCall("t", "r", "db.write", "write", "shadow", {}, None, None, 1.0, 2.0))
    assert "shadow    write     tool db.write -> ok" in capsys.readouterr().out.splitlines()


def test_serve_records_under_the_store_it_was_given(home, monkeypatch):
    """`--store` on `serve` is where its exchanges go, not `$IRIMI_HOME/store` (#70). Serve mode
    starts no process run, so an exchange that names no run is stored in `unattributed/` and
    builds none (#77), and the store is closed when serve stops. Stopped by its `stop` event, as
    the sample-workflow harness stops it, rather than by a signal to the test process."""
    import threading
    import urllib.request
    from http.server import BaseHTTPRequestHandler, HTTPServer

    from irimi.store import StoreReader
    from irimi.trace import UNATTRIBUTED

    class _Hello(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("content-length", "5")
            self.end_headers()
            self.wfile.write(b"hello")

        def log_message(self, *args):
            pass

    upstream = HTTPServer(("127.0.0.1", 0), _Hello)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    stop = threading.Event()
    got = []

    def drive(port):
        proxy = urllib.request.ProxyHandler({"http": f"http://127.0.0.1:{port}"})
        url = f"http://127.0.0.1:{upstream.server_address[1]}/hello"
        try:
            got.append(urllib.request.build_opener(proxy).open(url, timeout=10).read())
        finally:  # stop serve whatever happened, or the test would wait on it forever
            stop.set()

    def startup(mode, args, index, run_id, host, port, cert):
        threading.Thread(target=drive, args=(port,), daemon=True).start()

    monkeypatch.setattr(cli, "print_startup", startup)
    elsewhere = home / "elsewhere"
    args = cli.build_parser().parse_args(
        ["shadow", "--serve", "--port", "0", "--store", str(elsewhere)]
    )
    try:
        assert args.func(args, stop) == 0
    finally:
        upstream.shutdown()
    assert got == [b"hello"]
    assert not (home / "store").exists()
    assert StoreReader(elsewhere).list_runs() == []
    stored = StoreReader(elsewhere).load_run(UNATTRIBUTED)
    assert [(ex.request.path, ex.response.body) for ex in stored.events] == [("/hello", b"hello")]
