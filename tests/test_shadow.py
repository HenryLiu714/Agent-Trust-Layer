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


def test_child_receives_proxy_env(home, capsys):
    dump = home / "env.json"
    assert main(["shadow", *_py(_env_dump_source(dump))]) == 0
    env = json.loads(dump.read_text())

    assert env["IRIMI_ENGINE_ACTIVE"] == "1"
    run_id = env["IRIMI_RUN"]
    assert len(run_id) == 4
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
    assert "NOT virtualized" in out
    assert "backstop: none (Phase 4)" in out
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
    assert "live" in out
    assert "read" in out
    assert "GET" in out
    assert "1 exchange(s)" in out
    assert "read=1" in out


def test_exit_code_propagates(home):
    assert main(["shadow", *_py("import sys; sys.exit(7)")]) == 7


def test_signal_exit_code(home):
    assert main(["shadow", *_py("import os, signal; os.kill(os.getpid(), signal.SIGTERM)")]) == 143


def test_missing_command(home, capsys):
    assert main(["shadow", "--port", "0", "--", "irimi-no-such-binary"]) == 127
    assert "command not found" in capsys.readouterr().err


def test_no_command_given(home, capsys):
    assert main(["shadow"]) == 1
    assert "Usage: irimi shadow" in capsys.readouterr().err
    assert main(["shadow", "--"]) == 1
    assert "Usage: irimi shadow" in capsys.readouterr().err


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
        assert "did not start" in capsys.readouterr().err
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
        cli, "_wait_for_child", lambda proc: (_ for _ in ()).throw(KeyboardInterrupt)
    )
    assert main(["shadow", *_py("pass")]) == 130
    assert "0 exchange(s)" in capsys.readouterr().out
