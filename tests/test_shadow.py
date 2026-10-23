import json
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from irimi import ca, paths
from irimi.cli import main


class _Upstream(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"hello from upstream"
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def upstream():
    srv = HTTPServer(("127.0.0.1", 0), _Upstream)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield srv.server_address[1]
    srv.shutdown()


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    ca.generate_ca(ca.ca_paths())
    return tmp_path


def _py(source: str) -> list[str]:
    return ["--port", "0", "--", sys.executable, "-c", source]


def _env_dump_source(dump) -> str:
    return f"import json, os, sys;open({str(dump)!r}, 'w').write(json.dumps(dict(os.environ)))"


def _reverse_source(path: str) -> str:
    """Child that talks to the reverse door on the listener it was given, then prints the reply."""
    return (
        "import http.client, os, urllib.parse;"
        "u = urllib.parse.urlparse(os.environ['HTTPS_PROXY']);"
        "c = http.client.HTTPConnection(u.hostname, u.port, timeout=10);"
        f"c.request('GET', {path!r});"
        "r = c.getresponse(); print(r.status, r.read().decode())"
    )


def test_child_receives_proxy_env(home, capsys):
    dump = home / "env.json"
    assert main(["shadow", *_py(_env_dump_source(dump))]) == 0
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
    assert main(["shadow", *_py(_env_dump_source(dump))]) == 0
    env = json.loads(dump.read_text())

    assert paths.MITM_CA_BUNDLE_NAME not in env["SSL_CERT_FILE"]
    with open(env["SSL_CERT_FILE"], "rb") as fh:
        assert b"PRIVATE KEY" not in fh.read()


def test_run_id_in_env_matches_banner(home, capsys):
    dump = home / "env.json"
    assert main(["shadow", *_py(_env_dump_source(dump))]) == 0
    out = capsys.readouterr().out
    env = json.loads(dump.read_text())
    assert env["IRIMI_RUN"] in out


def test_banner_is_printed(home, capsys):
    assert main(["shadow", *_py("pass")]) == 0
    out = capsys.readouterr().out
    assert "listening on 127.0.0.1:" in out
    assert "only traffic sent through this proxy is shadowed" in out
    assert str(ca.ca_paths().cert) in out


def test_child_traffic_is_recorded(home, upstream, capfd):
    # capfd, not capsys: the child writes to the inherited fd 1, which capsys does not see.
    src = (
        "import http.client, os, urllib.parse;"
        "u = urllib.parse.urlparse(os.environ['HTTPS_PROXY']);"
        "c = http.client.HTTPConnection(u.hostname, u.port, timeout=10);"
        f"c.request('GET', 'http://127.0.0.1:{upstream}/hello', "
        f"headers={{'host': '127.0.0.1:{upstream}'}});"
        "r = c.getresponse(); print(r.status, r.read().decode())"
    )
    assert main(["shadow", *_py(src)]) == 0
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
    assert main(["shadow", "--quiet", *_py(src)]) == 0
    out = capfd.readouterr().out
    assert "200 hello from upstream" in out
    assert "only traffic sent through this proxy is shadowed" in out
    assert "1 exchange · 1 real · 0 answered by irimi (0 faked writes) · 0 sent to targets" in out
    assert " -> " not in out
    assert "/hello" not in out


def test_exit_code_propagates(home):
    assert main(["shadow", *_py("import sys; sys.exit(7)")]) == 7


def test_signal_exit_code(home):
    assert main(["shadow", *_py("import os, signal; os.kill(os.getpid(), signal.SIGTERM)")]) == 143


def test_missing_command(home, capsys):
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
    """A client trusting some other CA fails its handshake and never sends a request: the summary
    counts the refused connection by host, names the CA to point it at, and says irimi saw no
    traffic."""
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
    assert main(["shadow", *_py(source)]) == 0
    lines = capfd.readouterr().out.splitlines()
    assert lines[-2:] == [
        "  irimi saw no HTTP(S) traffic; if the agent made calls, they bypassed the proxy or "
        "failed TLS",
        "  1 connection to api.distrustful.test failed TLS: the client did not trust irimi's CA; "
        f"point it at {ca.ca_paths().cert} (some clients ignore SSL_CERT_FILE)",
    ]


def test_the_summary_starts_on_a_line_of_its_own_after_a_child_that_left_one_open(home, capfd):
    """irimi cannot see what the child wrote to their shared stdout, so a newline ends whatever
    line the child left open before the summary's header."""
    assert main(["shadow", *_py("import sys; sys.stdout.write('no newline')")]) == 0
    lines = capfd.readouterr().out.splitlines()
    [header] = [i for i, line in enumerate(lines) if " · 0 exchanges · " in line]
    assert lines[header].startswith("irimi shadow · run ")
    assert lines[header - 1] == "no newline"


def test_a_store_that_cannot_be_a_directory_is_named_by_the_flag(home, capsys):
    assert main(["shadow", "--store", "/dev/null/x", *_py("pass")]) == 1
    assert capsys.readouterr().err.splitlines() == ["error: --store /dev/null/x: not a directory"]


def test_no_command_given(home, capsys):
    usage = "error: nothing to run; usage: irimi shadow -- <command> [args...]"
    assert main(["shadow"]) == 1
    assert capsys.readouterr().err.splitlines() == [usage]
    assert main(["shadow", "--"]) == 1
    assert capsys.readouterr().err.splitlines() == [usage]


def test_no_ca(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    assert main(["shadow", "--port", "0", "--", sys.executable, "-c", "pass"]) == 1
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
        assert main(argv) == 1
        assert capsys.readouterr().err.splitlines() == [
            f"error: port {port} on 127.0.0.1 is in use; pick another with --port, or --port 0 "
            "for any free port"
        ]
        assert not (home / "SHOULD_NOT_EXIST").exists()
    finally:
        sock.close()


def test_interrupt_before_ready_exits_cleanly(home, monkeypatch, capsys):
    """Ctrl-C during engine startup must not escape main() as a traceback."""
    from irimi import runner

    monkeypatch.chdir(home)  # so SHOULD_NOT_EXIST would land here, not in the repo
    real = runner.start_engine

    def interrupted(engine, timeout=runner.READY_TIMEOUT_S):
        handle = real(engine, timeout)
        handle.stop()
        raise KeyboardInterrupt

    monkeypatch.setattr(runner, "start_engine", interrupted)
    assert main(["shadow", *_py("open('SHOULD_NOT_EXIST','w')")]) == 130
    assert "interrupted before the proxy was ready" in capsys.readouterr().err
    assert not (home / "SHOULD_NOT_EXIST").exists()


def test_interrupt_while_waiting_exits_cleanly(home, monkeypatch, capsys):
    """Ctrl-C that escapes _wait_for_child still stops the engine and prints the summary."""
    from irimi import cli

    monkeypatch.setattr(
        cli, "_wait_for_child", lambda proc, child: (_ for _ in ()).throw(KeyboardInterrupt)
    )
    assert main(["shadow", *_py("pass")]) == 130
    assert "0 exchanges" in capsys.readouterr().out


def test_reverse_door_refuses_unlisted_host(home, capfd):
    # capfd, not capsys: the child writes to the inherited fd 1, which capsys does not see.
    assert main(["shadow", *_py(_reverse_source("/evil.example/x"))]) == 0
    out = capfd.readouterr().out
    assert '403 {"error": {"type": "irimi_refused", "message": "irimi refused this request: ' in out
    assert "base-URL host 'evil.example' is not in a loaded map or --allow-host" in out
    # The refusal is irimi's own answer, so it is recorded and counted like one. It was a GET, so
    # it is not listed among the writes, and its host line says irimi refused it.
    assert "1 exchange" in out
    assert "1 read refused by irimi" in out
    assert "None of these writes" not in out


def test_reverse_door_allow_host(home, capfd):
    argv = [
        "shadow",
        "--port",
        "0",
        "--allow-host",
        "127.0.0.1",
        "--",
        sys.executable,
        "-c",
        _reverse_source("/127.0.0.1:1/x"),
    ]
    assert main(argv) == 0
    out = capfd.readouterr().out
    assert "502" in out
    assert "127.0.0.1/x" in out
    assert "[upstream-error]" in out
    assert "1 exchange ·" in out


def test_banner_names_a_delegated_service(home, tmp_path, capsys):
    """A routed host may not be live either, and the banner says so."""
    from irimi import servicemap

    (tmp_path / "cwd" / servicemap.loader.CWD_OVERRIDE_NAME).write_text(
        "service: stripe\ntarget: http://127.0.0.1:3000\ntarget_reads: true\n"
    )
    assert main(["shadow", *_py("pass")]) == 0
    out = capsys.readouterr().out
    assert "delegated: stripe -> http://127.0.0.1:3000 (reads + writes)" in out
    # The existing banner is unchanged, not replaced.
    assert "only traffic sent through this proxy is shadowed" in out


def test_the_banner_says_when_a_target_left_the_machine(home, capsys):
    argv = [
        "shadow",
        "--port",
        "0",
        "--target",
        "api.stripe.com=http://stub.example:3000",
        "--allow-target-host",
        "stub.example",
        "--",
        sys.executable,
        "-c",
        "pass",
    ]
    assert main(argv) == 0
    captured = capsys.readouterr()
    assert "delegated: stripe -> http://stub.example:3000 (writes)" in captured.out
    assert "NOT loopback" in captured.out
    assert "--allow-target-host" in captured.err  # the existing escape-hatch warning, unchanged


def test_no_delegated_service_leaves_the_banner_alone(home, capsys):
    assert main(["shadow", *_py("pass")]) == 0
    assert "delegated:" not in capsys.readouterr().out


def test_a_redaction_key_irimi_did_not_write_stops_shadow_with_one_line(home, capsys):
    """A run that could not redact what it stores must not start: one `error:` line,
    exit 1, no traceback, and the child never runs."""
    (home / paths.REDACT_KEY_NAME).write_bytes(b"short")
    marker = home / "ran"
    assert main(["shadow", *_py(f"open({str(marker)!r}, 'w')")]) == 1
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and err[0].startswith("error: ") and "32" in err[0]
    assert not marker.exists()


def test_a_store_that_cannot_be_written_stops_shadow_with_one_line(home, tmp_path, capsys):
    """A store that would drop every event behind one warning must not start a run that looks
    recorded."""
    not_a_dir = tmp_path / "a-file"
    not_a_dir.write_text("")
    marker = home / "ran"
    assert main(["shadow", "--store", str(not_a_dir), *_py(f"open({str(marker)!r}, 'w')")]) == 1
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and err[0].startswith("error: ")
    assert not marker.exists()


def test_sigterm_stops_the_command_ends_the_run_and_closes_the_store(home, upstream):
    """SIGTERM is how a supervisor or a container runtime stops `irimi shadow`. It is passed to
    the command, whose exit then ends the run as any exit does: the run is stored ended with the
    command's code, what was queued is written before irimi exits, the summary is printed, and
    irimi exits with the command's code, 143 for a command SIGTERM ended. Python's default action
    would end irimi at once, leaving the command running with its proxy gone, the run incomplete
    and the store never closed."""
    import os
    import signal
    import subprocess
    import time

    from irimi.store import StoreReader

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
        [sys.executable, "-m", "irimi.cli", "shadow", *_py(src)],
        cwd=home,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 30
        while not ready.exists():
            assert proc.poll() is None, proc.communicate()
            assert time.monotonic() < deadline, "the command never started"
            time.sleep(0.02)
        child = int(ready.read_text())
        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        out, err = proc.communicate(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    # Passed on at once, not after `_wait_for_child`'s grace: the command was told, not waited out.
    assert time.monotonic() - started < 5
    assert proc.returncode == 143, err
    with pytest.raises(ProcessLookupError):
        os.kill(child, 0)
    [record] = StoreReader(home / "store").list_runs()
    assert (record.outcome, record.exit_code) == ("error", 143)
    [event] = StoreReader(home / "store").load_run(record.run_id).events
    assert event.request.path == "/hello"
    assert "1 exchange ·" in out
    assert "Traceback" not in err


def test_sigterm_before_the_command_starts_runs_nothing(home, monkeypatch, capsys):
    """A SIGTERM that arrives while irimi loads its maps or binds is held until the command would
    start, and the command then never does: no run is stored, the store is closed and irimi exits
    143."""
    import signal

    from irimi import cli
    from irimi.store import StoreReader

    monkeypatch.chdir(home)  # so SHOULD_NOT_EXIST would land here, not in the repo
    prepare = cli._prepare_run

    def terminated_while_preparing(*args, **kwargs):
        run = prepare(*args, **kwargs)
        signal.raise_signal(signal.SIGTERM)
        return run

    monkeypatch.setattr(cli, "_prepare_run", terminated_while_preparing)
    outside: list[int] = []

    def record(signum, frame):
        outside.append(signum)

    saved = signal.signal(signal.SIGTERM, record)
    try:
        assert main(["shadow", *_py("open('SHOULD_NOT_EXIST','w')")]) == 143
        after = signal.getsignal(signal.SIGTERM)
    finally:
        signal.signal(signal.SIGTERM, saved)
    assert outside == [] and after is record  # irimi's handler, gone again once it returned
    assert "terminated before the command started" in capsys.readouterr().err
    assert not (home / "SHOULD_NOT_EXIST").exists()
    assert StoreReader(home / "store").list_runs() == []


def test_sigterm_while_the_command_starts_is_passed_to_it_once_it_exists(home, monkeypatch):
    """A SIGTERM that lands after the command was started but before irimi holds it, when there is
    no command to pass it to, is held and passed on as the command is adopted: the command
    ends at once, and the run is stored with its 143."""
    import signal
    import subprocess
    import time

    from irimi.store import StoreReader

    real = subprocess.Popen

    def started_then_terminated(*args, **kwargs):
        proc = real(*args, **kwargs)
        signal.raise_signal(signal.SIGTERM)
        return proc

    monkeypatch.setattr(subprocess, "Popen", started_then_terminated)
    started = time.monotonic()
    assert main(["shadow", *_py("import time; time.sleep(60)")]) == 143
    assert time.monotonic() - started < 30
    [record] = StoreReader(home / "store").list_runs()
    assert (record.outcome, record.exit_code) == ("error", 143)


def test_sigterm_after_the_command_exited_signals_no_pid(home, monkeypatch):
    """Once the command has been reaped its pid may be another process's, so a SIGTERM that comes
    while irimi ends the run and closes the store is passed to no one: the run keeps the
    command's own exit, and irimi closes the store and exits with it."""
    import os
    import signal

    from irimi import runner
    from irimi.store import StoreReader

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
    assert main(["shadow", *_py("pass")]) == 0
    assert killed == []
    [record] = StoreReader(home / "store").list_runs()
    assert (record.outcome, record.exit_code) == ("ok", 0)


def test_an_ignored_sigterm_stays_ignored_and_a_thread_installs_nothing(home, monkeypatch):
    """`irimi shadow` started with SIGTERM ignored leaves it ignored, as serve does, and one run off
    the main thread, where Python allows no handler, installs none and still runs its command.
    """
    import signal

    from irimi import cli

    during: list[object] = []
    prepare = cli._prepare_run

    def seen(*args, **kwargs):
        during.append(signal.getsignal(signal.SIGTERM))
        return prepare(*args, **kwargs)

    monkeypatch.setattr(cli, "_prepare_run", seen)
    saved = signal.signal(signal.SIGTERM, signal.SIG_IGN)
    try:
        assert main(["shadow", *_py("pass")]) == 0
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_IGN
    finally:
        signal.signal(signal.SIGTERM, saved)
    assert during == [signal.SIG_IGN]

    codes: list[int] = []
    thread = threading.Thread(target=lambda: codes.append(main(["shadow", *_py("exit(3)")])))
    thread.start()
    thread.join(60)
    assert codes == [3]
    assert during[1] is saved
