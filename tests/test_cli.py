import argparse
import io
import socket
import sys
from pathlib import Path

import pytest

from irimi import __version__, ca, cli, paths, report, servicemap
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


def test_the_composition_root_builds_the_service_overlay_over_the_run_s_maps():
    """The overlay classifies a read against the same maps the engine does (#43): a second index
    would let the two disagree about which service a read belongs to. The engine records into the
    store `_prepare_run` opened, the one `irimi shadow` starts and ends its run in (#70)."""
    from irimi.overlay import ServiceOverlay
    from irimi.store import NullStore

    index = _shipped_index()
    args = argparse.Namespace(allow_host=[], port=4321)
    p = ca.ca_paths()
    run = cli._Run(index, p, "t3st", cli._engine_config(args, index, "t3st", p), NullStore())
    engine = cli._build_engine(run, on_exchange=lambda ex: None)
    assert isinstance(engine.overlay, ServiceOverlay)
    assert engine.overlay.maps is run.config.maps
    assert engine.overlay.maps is index
    assert engine.store is run.store


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
    assert report.BACKSTOP_NOTICE in visible
    assert report.NOT_VIRTUALIZED_NOTICE in visible


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


def test_serve_records_under_the_store_it_was_given(home, monkeypatch):
    """`--store` on `serve` is where its exchanges go, not `$IRIMI_HOME/store` (#70). `serve`
    starts no process run (#77 owns serve mode), so its exchanges make a `header` run, and the
    store is closed when `serve` stops."""
    import asyncio
    import threading
    import urllib.request
    from http.server import BaseHTTPRequestHandler, HTTPServer

    from irimi.store import StoreReader

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
    engines = []
    build = cli._build_engine
    monkeypatch.setattr(
        cli, "_build_engine", lambda run, cb: engines.append(build(run, cb)) or engines[-1]
    )
    got = []

    def drive(port):
        proxy = urllib.request.ProxyHandler({"http": f"http://127.0.0.1:{port}"})
        url = f"http://127.0.0.1:{upstream.server_address[1]}/hello"
        try:
            got.append(urllib.request.build_opener(proxy).open(url, timeout=10).read())
        finally:  # stop `serve` whatever happened, or the test would wait on it forever
            loop.call_soon_threadsafe(engines[0].shutdown)

    def startup(mode, args, index, run_id, host, port, cert):
        nonlocal loop
        loop = asyncio.get_running_loop()
        threading.Thread(target=drive, args=(port,), daemon=True).start()

    loop = None
    monkeypatch.setattr(cli, "print_startup", startup)
    elsewhere = home / "elsewhere"
    try:
        assert main(["serve", "--port", "0", "--store", str(elsewhere)]) == 0
    finally:
        upstream.shutdown()
    assert got == [b"hello"]
    assert not (home / "store").exists()
    (record,) = StoreReader(elsewhere).list_runs()
    assert record.attribution == "header" and record.outcome is None
    stored = StoreReader(elsewhere).load_run(record.run_id)
    assert [(ex.request.path, ex.response.body) for ex in stored.events] == [("/hello", b"hello")]
