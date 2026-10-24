"""The CLI's refusals, its help, and serve's TLS warning, through the real parser and engine."""

import argparse
import os
import socket
import ssl
import subprocess
import sys
import threading
import time

import pytest

from irimi import cli
from irimi.cli import compose, main, serve
from irimi.config import ca, paths
from irimi.core import exitcodes


def _refusal(argv: list[str], capsys: pytest.CaptureFixture[str]) -> list[str]:
    """What `irimi <argv>` says on stderr as it exits 2 for a bad invocation: the usage line it
    starts with, then every line from the error on."""
    with pytest.raises(SystemExit) as exit_:
        main(argv)
    assert exit_.value.code == exitcodes.ERROR
    lines = capsys.readouterr().err.splitlines()
    error = next(i for i, line in enumerate(lines) if ": error: " in line)
    return [lines[0], *lines[error:]]


@pytest.mark.parametrize(
    ("argv", "old", "new"),
    [
        (["shadow", "--listen-host", "0.0.0.0", "--", "true"], "--listen-host", "--bind"),
        (["serve", "--allow-host", "x.test"], "--allow-host", "--base-url-host"),
        (
            ["shadow", "--allow-target-host=x.test", "--", "true"],
            "--allow-target-host",
            "--allow-remote-target",
        ),
        (["replay", "last", "--call", "agent:handle"], "--call", "--entrypoint"),
    ],
)
def test_a_renamed_flag_is_refused_by_its_command_with_the_name_to_use(
    argv, old, new, capsys, tmp_path
):
    err = _refusal(argv, capsys)
    assert err[0].startswith(f"usage: irimi {argv[0]} ")
    assert err[1].startswith(f"irimi {argv[0]}: error: unrecognized arguments: {old}")
    assert err[2:] == [f"hint: {old} is spelled {new}"]
    assert not (tmp_path / "irimi-home" / "store").exists()


def test_shadow_serve_is_refused_with_a_pointer_at_irimi_serve(capsys):
    err = _refusal(["shadow", "--serve"], capsys)
    assert err[1:] == [
        "irimi shadow: error: unrecognized arguments: --serve",
        "hint: serve mode is its own command: irimi serve",
    ]


@pytest.mark.parametrize("command", ["shadow", "replay last"])
def test_a_command_typed_without_its_dashes_is_refused_with_where_they_go(command, capsys):
    words = command.split()
    err = _refusal([*words, "--quiet", "python3", "agent.py"], capsys)
    assert err[1:] == [
        f"irimi {words[0]}: error: unrecognized arguments: python3 agent.py",
        "hint: put the command to run after --",
    ]


def test_an_abbreviated_flag_is_refused_by_the_subcommand_it_was_typed_to(capsys):
    """`irimi runs` runs `runs list`, so its refusal shows `runs list`'s usage."""
    err = _refusal(["runs", "--lim", "1"], capsys)
    assert err[0].startswith("usage: irimi runs list ")
    assert err[1:] == ["irimi runs list: error: unrecognized arguments: --lim 1"]


def test_words_after_dashes_on_a_command_that_runs_none_are_refused_by_that_command(capsys):
    err = _refusal(["runs", "show", "last", "--", "x"], capsys)
    assert err[1:] == ["irimi runs show: error: unrecognized arguments: -- x"]


def _every_parser(parser: argparse.ArgumentParser) -> list[argparse.ArgumentParser]:
    found = [parser]
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for sub in action.choices.values():
                found += _every_parser(sub)
    return found


def test_no_parser_accepts_an_abbreviated_flag():
    parsers = _every_parser(cli.build_parser())
    assert len(parsers) == 13
    assert [p.prog for p in parsers if p.allow_abbrev] == []


@pytest.mark.parametrize(
    "argv",
    [
        ["shadow", "--bin=0.0.0.0", "--", "true"],
        ["shadow", "--allow-remote", "x.test", "--", "true"],
        ["replay", "--entry=a:b", "last"],
        ["runs", "list", "--lim=1"],
    ],
)
def test_an_abbreviation_is_refused_with_or_without_an_equals_sign(argv, capsys):
    err = _refusal(argv, capsys)
    assert "error: unrecognized arguments:" in err[1]


def test_an_abbreviated_required_flag_does_not_count_as_given(capsys):
    err = _refusal(["runs", "prune", "--older=1d"], capsys)
    assert err[1:] == [
        "irimi runs prune: error: the following arguments are required: --older-than"
    ]


def test_replay_offers_no_run_it_refuses(capsys):
    """`irimi replay unattributed` is refused, so replay's help never offers it; the commands
    that read the pool do."""
    for argv, offers in (
        (["replay", "--help"], False),
        (["runs", "show", "--help"], True),
        (["runs", "rm", "--help"], True),
        (["compare", "--help"], True),
    ):
        with pytest.raises(SystemExit):
            main(argv)
        said = " ".join(capsys.readouterr().out.split())
        assert ("unattributed for traffic no run claimed" in said) is offers, argv


def test_serve_warns_once_per_host_whose_client_refused_irimi_s_ca(tmp_path, monkeypatch, capsys):
    """A client that does not trust irimi's CA never sends a request, so no run's summary can say
    so: serve warns on stderr, once per host, naming the CA to point the client at."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "home"))
    ca.generate_ca(ca.ca_paths())
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    other = ca.CAPaths(key=elsewhere / "ca.key", cert=elsewhere / "ca.pem")
    ca.generate_ca(other)  # what the client trusts instead
    engines: list[object] = []

    def build(*args, **kwargs):
        engines.append(compose.build_engine(*args, **kwargs))
        return engines[-1]

    args = cli.parse_args(["serve", "--port", "0", "--quiet"])
    stop = threading.Event()
    codes: list[int] = []
    server = threading.Thread(
        target=lambda: codes.append(serve.cmd_serve(args, stop, engine_factory=build))
    )
    server.start()
    try:
        port = _listening_port(engines)
        for _ in range(2):
            tunnel = _tunnel(port, "api.distrustful.test")
            context = ssl.create_default_context(cafile=str(other.cert))
            with pytest.raises(ssl.SSLCertVerificationError):
                context.wrap_socket(tunnel, server_hostname="api.distrustful.test")
            tunnel.close()
        printed = _stderr_once_it_says("failed TLS", capsys)
    finally:
        stop.set()
        server.join(30)
    assert codes == [exitcodes.OK]
    warned = [line for line in printed if "failed TLS" in line]
    assert warned == [
        "warning: 1 connection to api.distrustful.test failed TLS: the client did not trust "
        f"irimi's CA; point it at {ca.ca_paths().cert} (some clients ignore SSL_CERT_FILE)"
    ]


def _stderr_once_it_says(
    text: str, capsys: pytest.CaptureFixture[str], timeout: float = 10
) -> list[str]:
    """serve's stderr lines, read until one holds `text` and for half a second more, so a
    second handshake's report, on the proxy's loop as the first was, has had its time too."""
    lines: list[str] = []
    deadline = time.monotonic() + timeout
    while not any(text in line for line in lines):
        assert time.monotonic() < deadline, f"serve never said {text!r}"
        time.sleep(0.05)
        lines += capsys.readouterr().err.splitlines()
    time.sleep(0.5)
    return lines + capsys.readouterr().err.splitlines()


def _listening_port(engines: list[object], timeout: float = 30) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if engines:
            port = engines[0].listen_port()  # type: ignore[attr-defined]
            if port is not None:
                return int(port)
        time.sleep(0.05)
    raise AssertionError("serve's proxy never listened")


def _tunnel(proxy_port: int, host: str) -> socket.socket:
    """A CONNECT tunnel through the proxy to `host`:443, before any TLS."""
    s = socket.create_connection(("127.0.0.1", proxy_port), timeout=10)
    s.sendall(f"CONNECT {host}:443 HTTP/1.1\r\nHost: {host}:443\r\n\r\n".encode())
    answer = b""
    while b"\r\n\r\n" not in answer:
        answer += s.recv(1024)
    assert answer.startswith(b"HTTP/1.1 200"), answer
    return s


@pytest.mark.parametrize("command", [["shadow", "--", "true"], ["runs", "list"]])
def test_a_command_run_with_its_stdout_closed_runs_without_a_traceback(command, tmp_path):
    """`irimi shadow -- agent >&-` leaves Python no stdout at all, which is no terminal either."""
    env = {**os.environ, paths.IRIMI_HOME_ENV: str(tmp_path / "home")}
    env.pop(paths.IRIMI_STORE_ENV, None)
    irimi = [sys.executable, "-m", "irimi"]
    subprocess.run([*irimi, "init"], env=env, capture_output=True, check=True)
    done = subprocess.run(
        [*irimi, *command],
        env=env,
        stdin=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=lambda: os.close(1),  # the child closes its own stdout
        check=False,
    )
    assert (done.returncode, done.stderr) == (exitcodes.OK, "")


@pytest.mark.parametrize("older_than", ["0h", "0d", "00h"])
def test_prune_refuses_a_duration_of_nothing(older_than, tmp_path, capsys):
    """Every run, the one still starting included, is older than no time at all."""
    store = tmp_path / "store"
    argv = ["runs", "prune", "--store", str(store), "--older-than", older_than]
    assert _refusal(argv, capsys)[1:] == [
        f"irimi runs prune: error: argument --older-than: {older_than!r}: give a positive whole "
        "number of hours or days, as 12h or 30d"
    ]
    assert not store.exists()
