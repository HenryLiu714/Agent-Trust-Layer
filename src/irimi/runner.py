"""Process-level plumbing for `irimi shadow`: the child's environment and the engine thread.

No mitmproxy here. The engine is driven through the Engine protocol only. What a run *prints* is
`irimi.report`.
"""

import asyncio
import threading
from dataclasses import dataclass
from pathlib import Path

from irimi import __version__
from irimi.engine import Engine, EngineStartError
from irimi.exchange import CONTROL_PREFIX
from irimi.paths import CONTROL_ENV, ENGINE_ACTIVE_ENV, RUN_ENV
from irimi.store import TraceStore
from irimi.trace import SCHEMA_VERSION, ErrorInfo, JSONValue, RunRecord, Trigger

# The variables issue #3 specifies, plus `IRIMI_CONTROL` from #73. Nothing is added to this list
# without a new issue. The `IRIMI_*` names are declared in `paths`, where the SDK can import them,
# and imported above, so `runner.RUN_ENV` still names them.
NO_PROXY_VALUE = "localhost,127.0.0.1"
# The agent's own version, if its deployment names one; the process run records it (#70).
AGENT_VERSION_ENV = "IRIMI_AGENT_VERSION"

READY_TIMEOUT_S = 30.0
STOP_TIMEOUT_S = 30.0
CHILD_GRACE_S = 10.0


def child_env(
    base: dict[str, str], host: str, port: int, ca_cert: Path, run_id: str
) -> dict[str, str]:
    """`base` plus the proxy and CA variables. `base` is never mutated.

    `ca_cert` must be the CA certificate (`~/.irimi/ca/ca.pem`), never the mitmproxy bundle,
    which also contains the private key.
    """
    proxy = f"http://{host}:{port}"
    cert = str(ca_cert)
    env = dict(base)
    env.update(
        {
            "HTTP_PROXY": proxy,
            "HTTPS_PROXY": proxy,
            "http_proxy": proxy,
            "https_proxy": proxy,
            "NO_PROXY": NO_PROXY_VALUE,
            "no_proxy": NO_PROXY_VALUE,
            "SSL_CERT_FILE": cert,
            "REQUESTS_CA_BUNDLE": cert,
            "CURL_CA_BUNDLE": cert,
            "NODE_EXTRA_CA_CERTS": cert,
            "NODE_USE_ENV_PROXY": "1",
            ENGINE_ACTIVE_ENV: "1",
            RUN_ENV: run_id,
            # The control endpoint on this same listener (#73), with no trailing slash, so the
            # SDK appends `/runs/<id>/start` and friends.
            CONTROL_ENV: proxy + CONTROL_PREFIX.rstrip("/"),
        }
    )
    return env


def exit_code_for(returncode: int) -> int:
    """Shell convention: a child killed by signal N exits 128 + N (Popen reports -N)."""
    return 128 - returncode if returncode < 0 else returncode


def process_run(
    run_id: str, cmd: list[str], agent_version: str | None, started_at: float
) -> RunRecord:
    """The record `irimi shadow` stores for its child before spawning it (#70): one process tree
    is one run, and its trigger is the command line, which a replay can run again."""
    argv: list[JSONValue] = list(cmd)
    return RunRecord(
        schema_version=SCHEMA_VERSION,
        run_id=run_id,
        mode="shadow",
        attribution="process",
        trigger=Trigger(name=cmd[0], entrypoint=None, args={"argv": argv}, replayable=True),
        agent_version=agent_version,
        engine_version=__version__,
        sdk_version=None,
        started_at=started_at,
        ended_at=None,
        outcome=None,
        error=None,
        exit_code=None,
    )


def end_process_run(store: TraceStore, run_id: str, code: int, ended_at: float) -> None:
    """Close the process run with the child's exit code: `ok` for 0, `error` for anything else,
    a signal included (#70)."""
    if code == 0:
        store.end_run(run_id, ended_at, "ok", exit_code=code)
    else:
        store.end_run(run_id, ended_at, "error", ErrorInfo("exit", f"exited {code}"), code)


@dataclass
class EngineThread:
    """An Engine running on its own event loop in a daemon thread."""

    engine: Engine
    thread: threading.Thread
    loop: asyncio.AbstractEventLoop

    def port(self) -> int:
        port = self.engine.listen_port()
        assert port is not None  # only called after start_engine() returned
        return port

    def stop(self) -> None:
        """Idempotent. Returns once the engine thread has finished."""
        if self.loop.is_closed():  # already stopped; shutdown() would touch the closed loop
            return
        self.engine.shutdown()
        self.thread.join(timeout=STOP_TIMEOUT_S)
        if not self.thread.is_alive():  # closing a loop its thread still runs raises
            self.loop.close()


def start_engine(engine: Engine, timeout: float = READY_TIMEOUT_S) -> EngineThread:
    """Run `engine` on a background loop and return once its listener is bound.

    Raises EngineStartError (from wait_ready) if it never binds; the caller must then not spawn
    a child. The thread and loop are cleaned up before the exception propagates.
    """
    loop = asyncio.new_event_loop()

    def serve() -> None:
        # A failed bind reaches the caller through wait_ready(); letting it escape the thread as
        # well would print a traceback next to the caller's own error message.
        try:
            loop.run_until_complete(engine.run())
        except EngineStartError:
            pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    handle = EngineThread(engine=engine, thread=thread, loop=loop)
    try:
        asyncio.run_coroutine_threadsafe(engine.wait_ready(), loop).result(timeout=timeout)
    except BaseException:
        handle.stop()
        raise
    return handle
