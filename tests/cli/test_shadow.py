"""`irimi shadow`: the child it runs, the banner and summary it prints, and how it stops."""

import json
import socket
import sys
import threading

import pytest

from irimi import __version__
from irimi.cli import main, runner
from irimi.config import ca, paths
from irimi.core import exitcodes
from irimi.protocol.env import AGENT_VERSION_ENV
from irimi.report.terminal import PROXY_ONLY_NOTICE
from tests.support.engine import ReaderGoneWhen, python_child, spy_on_the_store
from tests.support.waiting import eventually


def _env_dump_source(dump) -> str:
    return f"import json, os, sys;open({str(dump)!r}, 'w').write(json.dumps(dict(os.environ)))"


def _base_url_source(path: str) -> str:
    """Child that sends a base-URL request to its listener, then prints the reply."""
    return (
        "import http.client, os, urllib.parse;"
        "u = urllib.parse.urlparse(os.environ['HTTPS_PROXY']);"
        "c = http.client.HTTPConnection(u.hostname, u.port, timeout=10);"
        f"c.request('GET', {path!r});"
        "r = c.getresponse(); print(r.status, r.read().decode())"
    )


def test_the_child_is_given_the_proxy_environment(home, capsys):
    dump = home / "env.json"
    assert main(["shadow", *python_child(_env_dump_source(dump))]) == 0
    env = json.loads(dump.read_text())

    assert env["IRIMI_ENGINE_ACTIVE"] == "1"
    run_id = env["IRIMI_RUN"]
    assert len(run_id) == 16  # a directory name in the store, unique across months of runs
    assert all(c in "0123456789abcdef" for c in run_id)
    assert env["NO_PROXY"] == "localhost,127.0.0.1"
    assert env["HTTPS_PROXY"].startswith("http://127.0.0.1:")
    assert int(env["HTTPS_PROXY"].rsplit(":", 1)[1]) != 0
    cert = str(ca.ca_paths().cert)
    assert env["SSL_CERT_FILE"] == cert
    assert env["REQUESTS_CA_BUNDLE"] == cert
    assert env["CURL_CA_BUNDLE"] == cert
    assert env["NODE_EXTRA_CA_CERTS"] == cert
    assert env["NODE_USE_ENV_PROXY"] == "1"


def test_child_cert_env_is_not_the_mitm_bundle(home):
    dump = home / "env.json"
    assert main(["shadow", *python_child(_env_dump_source(dump))]) == 0
    env = json.loads(dump.read_text())

    assert paths.MITM_CA_BUNDLE_NAME not in env["SSL_CERT_FILE"]
    with open(env["SSL_CERT_FILE"], "rb") as fh:
        assert b"PRIVATE KEY" not in fh.read()


def test_run_id_in_env_matches_banner(home, capsys):
    dump = home / "env.json"
    assert main(["shadow", *python_child(_env_dump_source(dump))]) == 0
    out = capsys.readouterr().out
    env = json.loads(dump.read_text())
    assert env["IRIMI_RUN"] in out


def test_shadow_prints_its_banner(home, capsys):
    assert main(["shadow", *python_child("pass")]) == 0
    out = capsys.readouterr().out
    assert "listening on 127.0.0.1:" in out
    assert "only traffic sent through this proxy is shadowed" in out
    assert str(ca.ca_paths().cert) in out


def test_the_childs_traffic_is_recorded(home, upstream, capfd):
    # capfd, not capsys: the child writes to the inherited fd 1, which capsys does not see.
    src = (
        "import http.client, os, urllib.parse;"
        "u = urllib.parse.urlparse(os.environ['HTTPS_PROXY']);"
        "c = http.client.HTTPConnection(u.hostname, u.port, timeout=10);"
        f"c.request('GET', 'http://127.0.0.1:{upstream}/hello', "
        f"headers={{'host': '127.0.0.1:{upstream}'}});"
        "r = c.getresponse(); print(r.status, r.read().decode())"
    )
    assert main(["shadow", *python_child(src)]) == 0
    out = capfd.readouterr().out
    assert "200 hello from upstream" in out
    assert "real" in out
    assert "read" in out
    assert "GET" in out
    assert "1 exchange ·" in out
    assert "1 read" in out
    assert "1 exchange · 1 real · 0 answered by irimi (0 faked writes) · 0 sent to targets" in out


def test_quiet_prints_the_banner_and_the_summary_and_no_line_per_exchange(home, upstream, capfd):
    # capfd, not capsys: the child writes to the inherited fd 1, which capsys does not see.
    src = (
        "import http.client, os, urllib.parse;"
        "u = urllib.parse.urlparse(os.environ['HTTPS_PROXY']);"
        "c = http.client.HTTPConnection(u.hostname, u.port, timeout=10);"
        f"c.request('GET', 'http://127.0.0.1:{upstream}/hello', "
        f"headers={{'host': '127.0.0.1:{upstream}'}});"
        "r = c.getresponse(); print(r.status, r.read().decode())"
    )
    assert main(["shadow", "--quiet", *python_child(src)]) == 0
    out = capfd.readouterr().out
    assert "200 hello from upstream" in out
    assert "only traffic sent through this proxy is shadowed" in out
    assert "1 exchange · 1 real · 0 answered by irimi (0 faked writes) · 0 sent to targets" in out
    assert " -> " not in out
    assert "/hello" not in out


def test_shadow_exits_with_the_childs_code(home):
    assert main(["shadow", *python_child("import sys; sys.exit(7)")]) == 7


def test_a_child_ended_by_sigterm_exits_143(home):
    assert (
        main(["shadow", *python_child("import os, signal; os.kill(os.getpid(), signal.SIGTERM)")])
        == 143
    )


def test_a_command_that_does_not_resolve_is_refused_before_the_banner(home, capsys):
    """A command that does not resolve is refused before the banner, and stores no run."""
    assert main(["shadow", "--port", "0", "--", "irimi-no-such-binary"]) == 127
    captured = capsys.readouterr()
    assert captured.err.splitlines() == ["error: command not found: irimi-no-such-binary"]
    assert captured.out == ""
    assert not (home / "store" / "runs").exists() or not any((home / "store" / "runs").iterdir())


def test_a_command_that_is_not_executable_is_refused_before_the_banner(home, tmp_path, capsys):
    script = tmp_path / "agent.sh"
    script.write_text("#!/bin/sh\n")
    assert main(["shadow", "--", str(script)]) == 126
    captured = capsys.readouterr()
    assert captured.err.splitlines() == [f"error: could not run {script}: permission denied"]
    assert captured.out == ""


def test_a_client_that_refuses_irimi_s_ca_is_named_in_the_summary(home, tmp_path, capfd):
    """A client that distrusts irimi's CA is counted by host, with the CA to point it at.

    It never sends a request, so the summary also says irimi saw no traffic."""
    other = ca.CAPaths(key=tmp_path / "other.key", cert=tmp_path / "other.pem")
    ca.generate_ca(other)
    source = (
        "import ssl, urllib.request\n"
        f"context = ssl.create_default_context(cafile={str(other.cert)!r})\n"
        "try:\n"
        "    urllib.request.urlopen('https://api.distrustful.test/', context=context, timeout=10)\n"
        "except Exception:\n"
        "    pass\n"
    )
    assert main(["shadow", *python_child(source)]) == 0
    lines = capfd.readouterr().out.splitlines()
    assert lines[-2:] == [
        "  irimi saw no HTTP(S) traffic; if the agent made calls, they bypassed the proxy or "
        "failed TLS",
        "  1 connection to api.distrustful.test failed TLS: the client did not trust irimi's CA; "
        f"point it at {ca.ca_paths().cert} (some clients ignore SSL_CERT_FILE)",
    ]


def test_the_summary_starts_on_a_line_of_its_own_after_a_child_that_left_one_open(home, capfd):
    """A newline ends any line the child left open before the summary's header."""
    assert main(["shadow", *python_child("import sys; sys.stdout.write('no newline')")]) == 0
    lines = capfd.readouterr().out.splitlines()
    [header] = [i for i, line in enumerate(lines) if " · 0 exchanges · " in line]
    assert lines[header].startswith("irimi shadow · run ")
    assert lines[header - 1] == "no newline"


def test_a_store_that_cannot_be_a_directory_is_named_by_the_flag(home, capsys):
    assert main(["shadow", "--store", "/dev/null/x", *python_child("pass")]) == 2
    assert capsys.readouterr().err.splitlines() == ["error: --store /dev/null/x: not a directory"]


def test_shadow_with_nothing_to_run_is_a_usage_error(home, capsys):
    usage = "usage: irimi shadow [options] -- COMMAND [ARGS...]"
    for argv in (["shadow"], ["shadow", "--"], ["shadow", "--port", "0", "--"]):
        with pytest.raises(SystemExit) as exc:
            main(argv)
        assert exc.value.code == exitcodes.ERROR
        assert capsys.readouterr().err.splitlines() == [
            usage,
            f"irimi shadow: error: nothing to run: {usage.removeprefix('usage: ')}",
        ]


def test_shadow_without_a_ca_is_refused(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    assert main(["shadow", "--port", "0", "--", sys.executable, "-c", "pass"]) == 2
    captured = capsys.readouterr()
    assert "irimi init" in captured.err
    assert "listening on" not in captured.out


def test_engine_start_failure_does_not_spawn_child(home, monkeypatch, capsys):
    monkeypatch.chdir(home)
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        port = sock.getsockname()[1]
        argv = [
            "shadow",
            "--port",
            str(port),
            "--",
            sys.executable,
            "-c",
            "open('SHOULD_NOT_EXIST','w')",
        ]
        assert main(argv) == 2
        assert capsys.readouterr().err.splitlines() == [
            f"error: port {port} on 127.0.0.1 is in use; pick another with --port, or --port 0 "
            "for any free port"
        ]
        assert not (home / "SHOULD_NOT_EXIST").exists()
    finally:
        sock.close()


def test_interrupt_before_ready_exits_cleanly(home, monkeypatch, capsys):
    """Ctrl-C during engine startup must not escape main() as a traceback."""
    from irimi.cli import runner

    monkeypatch.chdir(home)  # so SHOULD_NOT_EXIST would land here, not in the repo
    real = runner.start_engine

    def interrupted(engine, timeout=runner.READY_TIMEOUT_S):
        handle = real(engine, timeout)
        handle.stop()
        raise KeyboardInterrupt

    monkeypatch.setattr(runner, "start_engine", interrupted)
    assert main(["shadow", *python_child("open('SHOULD_NOT_EXIST','w')")]) == 130
    assert "interrupted before the proxy was ready" in capsys.readouterr().err
    assert not (home / "SHOULD_NOT_EXIST").exists()


def test_interrupt_while_waiting_exits_cleanly(home, monkeypatch, capsys):
    """Ctrl-C that escapes wait_for_child still stops the engine and prints the summary."""
    from irimi.cli import runner

    monkeypatch.setattr(
        runner, "wait_for_child", lambda proc, child: (_ for _ in ()).throw(KeyboardInterrupt)
    )
    assert main(["shadow", *python_child("pass")]) == 130
    assert "0 exchanges" in capsys.readouterr().out


def test_a_base_url_request_to_an_unlisted_host_is_refused(home, capfd):
    # capfd, not capsys: the child writes to the inherited fd 1, which capsys does not see.
    assert main(["shadow", *python_child(_base_url_source("/evil.example/x"))]) == 0
    out = capfd.readouterr().out
    assert '403 {"error": {"type": "irimi_refused", "message": "irimi refused this request: ' in out
    assert "base-URL host 'evil.example' is not in a loaded map or --base-url-host" in out
    # The refusal is recorded and counted as irimi's own answer; a GET, so not among the writes.
    assert "1 exchange" in out
    assert "1 read refused by irimi" in out
    assert "None of these writes" not in out


def test_base_url_host_admits_one_more_host(home, capfd):
    argv = [
        "shadow",
        "--port",
        "0",
        "--base-url-host",
        "127.0.0.1",
        "--",
        sys.executable,
        "-c",
        _base_url_source("/127.0.0.1:1/x"),
    ]
    assert main(argv) == 0
    out = capfd.readouterr().out
    assert "502" in out
    assert "127.0.0.1/x" in out
    assert "[upstream-error]" in out
    assert "1 exchange ·" in out


def test_banner_names_a_delegated_service(home, tmp_path, capsys):
    """The banner names a service whose requests go to a target."""
    from irimi import servicemaps

    (tmp_path / "cwd" / servicemaps.loader.CWD_OVERRIDE_NAME).write_text(
        "service: stripe\ntarget: http://127.0.0.1:3000\ntarget_reads: true\n"
    )
    assert main(["shadow", *python_child("pass")]) == 0
    out = capsys.readouterr().out
    assert "target: stripe -> http://127.0.0.1:3000 (reads + writes)" in out
    # Added to the usual banner, not in place of it.
    assert "only traffic sent through this proxy is shadowed" in out


def test_the_banner_says_when_a_target_left_the_machine(home, capsys):
    argv = [
        "shadow",
        "--port",
        "0",
        "--target",
        "api.stripe.com=http://stub.example:3000",
        "--allow-remote-target",
        "stub.example",
        "--",
        sys.executable,
        "-c",
        "pass",
    ]
    assert main(argv) == 0
    captured = capsys.readouterr()
    assert "target: stripe -> http://stub.example:3000 (writes)" in captured.out
    assert "not loopback" in captured.out
    assert "--allow-remote-target" in captured.err  # the escape-hatch warning


def test_no_delegated_service_leaves_the_banner_alone(home, capsys):
    assert main(["shadow", *python_child("pass")]) == 0
    lines = capsys.readouterr().out.splitlines()
    banner = lines[: lines.index("")] if "" in lines else lines
    assert PROXY_ONLY_NOTICE in banner
    assert not [line for line in banner if line.startswith("target:")]


def test_sigterm_stops_the_command_ends_the_run_and_closes_the_store(home, upstream):
    """A SIGTERM is passed to the command, and irimi ends the run, prints the summary and exits 143.

    Python's default action would end irimi at once, leaving the command running proxy-less."""
    import os
    import signal
    import subprocess
    import time

    from irimi.core.exchange import Exchange
    from irimi.record.store.reader import StoreReader

    ready = home / "child.pid"
    src = (
        "import http.client, os, time, urllib.parse;"
        "u = urllib.parse.urlparse(os.environ['HTTPS_PROXY']);"
        "c = http.client.HTTPConnection(u.hostname, u.port, timeout=10);"
        f"c.request('GET', 'http://127.0.0.1:{upstream}/hello', "
        f"headers={{'host': '127.0.0.1:{upstream}'}});"
        "c.getresponse().read();"
        f"open({str(ready)!r} + '.tmp', 'w').write(str(os.getpid()));"
        f"os.replace({str(ready)!r} + '.tmp', {str(ready)!r});"
        "time.sleep(60)"
    )
    env = {**os.environ, paths.IRIMI_HOME_ENV: str(home)}
    env.pop("STRIPE_API_KEY", None)
    proc = subprocess.Popen(
        [sys.executable, "-m", "irimi", "shadow", *python_child(src)],
        cwd=home,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:

        def command_started() -> bool:
            assert proc.poll() is None, proc.communicate()
            return ready.exists()

        eventually(command_started, timeout=30, interval=0.02, message="the command never started")
        child = int(ready.read_text())
        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        out, err = proc.communicate(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    # Passed on at once, not after `wait_for_child`'s grace: the command was told, not waited out.
    assert time.monotonic() - started < runner.CHILD_GRACE_S
    assert proc.returncode == 143, err
    with pytest.raises(ProcessLookupError):
        os.kill(child, 0)
    [record] = StoreReader(home / "store").list_runs()
    assert record.end is not None
    assert (record.end.outcome, record.end.exit_code) == ("error", 143)
    [event] = StoreReader(home / "store").load_run(record.run_id).events
    assert isinstance(event, Exchange)
    assert event.request.path == "/hello"
    assert "1 exchange ·" in out
    assert "Traceback" not in err


def test_sigterm_before_the_command_starts_runs_nothing(home, monkeypatch, capsys):
    """A SIGTERM during startup is held, then stops irimi with 143 before the command starts."""
    import signal

    from irimi.cli import compose
    from irimi.record.store.reader import StoreReader

    monkeypatch.chdir(home)  # so SHOULD_NOT_EXIST would land here, not in the repo
    prepare = compose.prepare_run

    def terminated_while_preparing(*args, **kwargs):
        run = prepare(*args, **kwargs)
        signal.raise_signal(signal.SIGTERM)
        return run

    monkeypatch.setattr(compose, "prepare_run", terminated_while_preparing)
    outside: list[int] = []

    def record(signum, frame):
        outside.append(signum)

    saved = signal.signal(signal.SIGTERM, record)
    try:
        assert main(["shadow", *python_child("open('SHOULD_NOT_EXIST','w')")]) == 143
        after = signal.getsignal(signal.SIGTERM)
    finally:
        signal.signal(signal.SIGTERM, saved)
    assert outside == [] and after is record  # irimi's handler, gone again once it returned
    assert "terminated before the command started" in capsys.readouterr().err
    assert not (home / "SHOULD_NOT_EXIST").exists()
    assert StoreReader(home / "store").list_runs() == []


def test_sigterm_while_the_command_starts_is_passed_to_it_once_it_exists(home, monkeypatch):
    """A SIGTERM as the command starts is held and passed on once irimi holds the command."""
    import signal
    import subprocess
    import time

    from irimi.record.store.reader import StoreReader

    real = subprocess.Popen

    def started_then_terminated(*args, **kwargs):
        proc = real(*args, **kwargs)
        signal.raise_signal(signal.SIGTERM)
        return proc

    monkeypatch.setattr(subprocess, "Popen", started_then_terminated)
    started = time.monotonic()
    assert main(["shadow", *python_child("import time; time.sleep(60)")]) == 143
    assert time.monotonic() - started < 30
    [record] = StoreReader(home / "store").list_runs()
    assert record.end is not None
    assert (record.end.outcome, record.end.exit_code) == ("error", 143)


def test_sigterm_after_the_command_exited_signals_no_pid(home, monkeypatch):
    """A SIGTERM after the command is reaped signals no pid, which may be another process's."""
    import os
    import signal

    from irimi.cli import runner
    from irimi.record.store.reader import StoreReader

    killed: list[tuple[int, int]] = []
    real_kill, real_end = os.kill, runner.end_process_run

    def kill(pid, signum):
        killed.append((pid, signum))
        real_kill(pid, signum)

    def terminated_while_ending(*args, **kwargs):
        signal.raise_signal(signal.SIGTERM)
        return real_end(*args, **kwargs)

    monkeypatch.setattr(os, "kill", kill)
    monkeypatch.setattr(runner, "end_process_run", terminated_while_ending)
    assert main(["shadow", *python_child("pass")]) == 0
    assert killed == []
    [record] = StoreReader(home / "store").list_runs()
    assert record.end is not None
    assert (record.end.outcome, record.end.exit_code) == ("ok", 0)


def test_an_ignored_sigterm_stays_ignored_and_a_thread_installs_nothing(home, monkeypatch):
    """An ignored SIGTERM stays ignored, and a run off the main thread installs no handler."""
    import signal

    from irimi.cli import compose

    during: list[object] = []
    prepare = compose.prepare_run

    def seen(*args, **kwargs):
        during.append(signal.getsignal(signal.SIGTERM))
        return prepare(*args, **kwargs)

    monkeypatch.setattr(compose, "prepare_run", seen)
    saved = signal.signal(signal.SIGTERM, signal.SIG_IGN)
    try:
        assert main(["shadow", *python_child("pass")]) == 0
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_IGN
    finally:
        signal.signal(signal.SIGTERM, saved)
    assert during == [signal.SIG_IGN]

    codes: list[int] = []
    thread = threading.Thread(
        target=lambda: codes.append(main(["shadow", *python_child("exit(3)")]))
    )
    thread.start()
    thread.join(60)
    assert codes == [3]
    assert during[1] is saved


def test_the_top_level_help_lists_shadow(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    assert "shadow" in capsys.readouterr().out


def test_shadow_help_documents_port(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["shadow", "--help"])
    assert exc.value.code == 0
    assert "--port" in capsys.readouterr().out


def test_shadow_help_has_base_url_host(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["shadow", "--help"])
    assert exc.value.code == 0
    assert "--base-url-host" in capsys.readouterr().out


@pytest.mark.parametrize("command", ["serve", "shadow"])
@pytest.mark.parametrize(
    "key", ["short", "directory", "dangling"], ids=["wrong-size", "directory", "dangling"]
)
def test_a_redaction_key_that_cannot_be_used_stops_the_run_with_one_line(
    home, capsys, command, key
):
    """An unusable redaction key stops the run with one `error:` line, before any store or child."""
    path = home / paths.REDACT_KEY_NAME
    if key == "short":
        path.write_bytes(b"short")
    elif key == "directory":
        path.mkdir()
    else:
        path.symlink_to(home / "nowhere")
    marker = home / "ran"
    child = ["--", sys.executable, "-c", f"open({str(marker)!r}, 'w')"]
    argv = [command, "--port", "0"] + (child if command == "shadow" else [])
    assert main(argv) == exitcodes.ERROR
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and err[0].startswith("error: ") and str(path) in err[0]
    if key == "short":
        assert "32" in err[0]
    assert not (home / "store").exists()
    assert not marker.exists()


@pytest.mark.parametrize("command", ["serve", "shadow"])
def test_a_store_that_cannot_be_written_stops_the_run_with_one_line(home, capsys, command):
    """An unwritable store stops the run with one `error:` line, and the child never runs.

    Read-only directories are skipped as root, whom no mode stops."""
    import os

    stores = [home / "ca" / "ca.pem"]
    if os.geteuid() != 0:
        read_only = home / "read-only"
        read_only.mkdir(mode=0o500)
        stores += [read_only, read_only / "below"]
    marker = home / "ran"
    for store in stores:
        argv = [command, "--port", "0", "--store", str(store)]
        if command == "shadow":
            argv += ["--", sys.executable, "-c", f"open({str(marker)!r}, 'w')"]
        assert main(argv) == exitcodes.ERROR
        err = capsys.readouterr().err.strip().splitlines()
        assert len(err) == 1 and err[0].startswith("error: "), err
        assert not marker.exists()


def test_shadow_stores_its_process_run_with_the_agent_and_engine_versions(home, monkeypatch):
    """The process run records its argv, both versions and how the child exited."""
    from irimi.protocol.trace import ErrorInfo, trigger_args
    from irimi.record.store.reader import StoreReader

    monkeypatch.setenv(AGENT_VERSION_ENV, "agent-2.0.1")
    assert main(["shadow", *python_child("raise SystemExit(3)")]) == 3
    (record,) = StoreReader(home / "store").list_runs()
    assert (record.attribution, record.mode) == ("process", "shadow")
    assert record.trigger is not None
    assert trigger_args(record.trigger) == {"argv": [sys.executable, "-c", "raise SystemExit(3)"]}
    assert (record.agent_version, record.engine_version, record.sdk_version) == (
        "agent-2.0.1",
        __version__,
        None,
    )
    assert record.end is not None
    assert (record.end.outcome, record.end.exit_code, record.end.error) == (
        "error",
        3,
        ErrorInfo("exit", "exited 3"),
    )
    assert record.started_at is not None and record.end.ended_at is not None
    assert record.started_at <= record.end.ended_at
    assert len(record.run_id) == 16


def test_shadow_closes_the_store_when_the_proxy_never_starts(home, monkeypatch, capsys):
    """The CLI closes the store it opened even when the proxy never starts."""
    from irimi.cli import runner
    from irimi.engine import EngineStartError

    spy = spy_on_the_store(monkeypatch)

    def refuse(engine, timeout=runner.READY_TIMEOUT_S):
        raise EngineStartError("proxy did not start on 127.0.0.1:0: refused by the test")

    monkeypatch.setattr(runner, "start_engine", refuse)
    assert main(["shadow", *python_child()]) == exitcodes.ERROR
    assert "refused by the test" in capsys.readouterr().err
    assert spy.closed != []
    assert list((home / "store").iterdir()) == []  # no run began, so none is on disk


@pytest.mark.parametrize("when", ["at-the-banner", "at-the-summary"])
def test_a_reader_gone_costs_shadow_no_run_and_leaves_no_engine(home, monkeypatch, when):
    """A reader gone exits 141 with the engine stopped and the store closed.

    Gone by the banner, no run is stored; gone by the summary, the run has the command's exit."""
    import threading

    from irimi.cli import runner
    from irimi.record.store.reader import StoreReader

    spy = spy_on_the_store(monkeypatch)
    out = ReaderGoneWhen()
    out.gone = when == "at-the-banner"
    real_end = runner.end_process_run

    def end_then_go(*args, **kwargs):
        real_end(*args, **kwargs)
        out.gone = True

    monkeypatch.setattr(runner, "end_process_run", end_then_go)
    monkeypatch.setattr(sys, "stdout", out)
    before = set(threading.enumerate())
    assert main(["shadow", *python_child("raise SystemExit(3)")]) == exitcodes.BROKEN_PIPE
    assert spy.closed != []
    assert [t for t in threading.enumerate() if t not in before and t.is_alive()] == []
    stored = StoreReader(home / "store").list_runs()
    if when == "at-the-banner":
        assert stored == []
    else:
        ends = [(r.end.outcome, r.end.exit_code) if r.end is not None else None for r in stored]
        assert ends == [("error", 3)]


def test_a_ctrl_c_while_the_store_closes_still_prints_the_summary(home, monkeypatch, capsys):
    """A Ctrl-C during the store's close costs neither the summary nor the child's exit code."""
    import threading

    def interrupt(store):
        if threading.current_thread() is threading.main_thread():
            raise KeyboardInterrupt

    spy_on_the_store(monkeypatch, close=interrupt)
    try:
        code = main(["shadow", *python_child()])
    except KeyboardInterrupt:  # caught here, or it would stop the whole pytest session
        pytest.fail("a Ctrl-C in the store's close escaped `irimi shadow`")
    assert code == 0
    assert "0 exchanges · 0 real" in capsys.readouterr().out
