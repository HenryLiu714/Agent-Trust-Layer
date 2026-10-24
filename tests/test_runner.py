"""`cli.runner`: the child's environment, the engine thread, and the exit code a run ends with."""

import asyncio
import threading
from pathlib import Path
from typing import cast

import pytest

from irimi.cli.runner import EngineThread, child_env, exit_code_for
from irimi.engine import Engine


def test_child_env_sets_proxy_and_ca():
    env = child_env({}, "127.0.0.1", 4000, Path("/ca.pem"), "7f3a")
    assert env["HTTP_PROXY"] == "http://127.0.0.1:4000"
    assert env["HTTPS_PROXY"] == "http://127.0.0.1:4000"
    assert env["http_proxy"] == "http://127.0.0.1:4000"
    assert env["https_proxy"] == "http://127.0.0.1:4000"
    assert env["NO_PROXY"] == "localhost,127.0.0.1"
    assert env["no_proxy"] == "localhost,127.0.0.1"
    for var in ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "NODE_EXTRA_CA_CERTS"):
        assert env[var] == "/ca.pem"
    assert env["NODE_USE_ENV_PROXY"] == "1"
    assert env["IRIMI_ENGINE_ACTIVE"] == "1"
    assert env["IRIMI_RUN"] == "7f3a"
    # The control endpoint on the same listener, with no trailing slash.
    assert env["IRIMI_CONTROL"] == "http://127.0.0.1:4000/_irimi"


def test_child_env_preserves_base_and_does_not_mutate():
    base = {"PATH": "/bin", "HTTP_PROXY": "old"}
    env = child_env(base, "127.0.0.1", 4000, Path("/ca.pem"), "7f3a")
    assert env["PATH"] == "/bin"
    assert env["HTTP_PROXY"] == "http://127.0.0.1:4000"
    assert base["HTTP_PROXY"] == "old"


def test_an_engine_that_never_binds_is_an_engine_start_error_with_a_message():
    """`start_engine` gives a ready timeout a message, so every command prints the same line."""
    from irimi.cli.runner import start_engine
    from irimi.engine import EngineStartError

    stopped = threading.Event()

    class _NeverReady:
        async def run(self) -> None:
            while not stopped.is_set():
                await asyncio.sleep(0.01)

        async def wait_ready(self) -> None:
            await asyncio.Event().wait()

        def shutdown(self) -> None:
            stopped.set()

    with pytest.raises(EngineStartError) as refused:
        start_engine(cast(Engine, _NeverReady()), timeout=1.0)
    assert str(refused.value) == "proxy did not start within 1s"
    assert stopped.is_set()


def test_a_childs_status_becomes_the_exit_code_a_shell_would_give():
    assert exit_code_for(0) == 0
    assert exit_code_for(7) == 7
    assert exit_code_for(-15) == 143
    assert exit_code_for(-2) == 130
    assert exit_code_for(-9) == 137


def test_engine_thread_stop_is_idempotent():
    loop = asyncio.new_event_loop()

    class _StubEngine:
        def shutdown(self) -> None:
            # Like MitmEngine.shutdown(), this reaches into the serving loop; a closed one raises.
            loop.call_soon_threadsafe(loop.stop)

    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    handle = EngineThread(engine=cast(Engine, _StubEngine()), thread=thread, loop=loop)
    handle.stop()
    assert not thread.is_alive()
    assert loop.is_closed()
    handle.stop()  # a second stop must not reach the closed loop
