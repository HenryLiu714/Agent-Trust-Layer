"""Process-level plumbing for `irimi shadow`: the child's environment and the engine thread.

No mitmproxy here. The engine is driven through the Engine protocol only. What a run *prints* is
`irimi.report`.
"""

import asyncio
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

from irimi import __version__, netaddr
from irimi.engine import Engine, EngineStartError
from irimi.exchange import CONTROL_PREFIX
from irimi.paths import (
    CONTROL_ENV,
    ENGINE_ACTIVE_ENV,
    PROXY_ENVS,
    REPLAY_ARGS_FILE_ENV,
    REPLAY_CALL_ENV,
    REPLAY_OUTCOME_FILE_ENV,
    REPLAY_RUN_ENV,
    RUN_ENV,
)
from irimi.store import TraceStore
from irimi.trace import SCHEMA_VERSION, ErrorInfo, JSONValue, RunRecord, Trigger

# The variables issue #3 specifies, plus `IRIMI_CONTROL` from #73 and, in a replay only,
# `IRIMI_REPLAY_RUN` from #83 and, for a replay that calls a trigger, `IRIMI_REPLAY_CALL`,
# `IRIMI_REPLAY_ARGS_FILE` and `IRIMI_REPLAY_OUTCOME_FILE` from #84. Nothing is added to this list
# without a new issue. The `IRIMI_*` names are declared in `paths`, where the SDK can import them,
# and imported above, so `runner.RUN_ENV` still names them. `IRIMI_AGENT_VERSION` is there too
# (`paths.AGENT_VERSION_ENV`): the process run records it (#70) and the SDK's runs do (#74). So
# are the four proxy variables (`paths.PROXY_ENVS`), which the SDK reads to find irimi (#75).
NO_PROXY_VALUE = "localhost,127.0.0.1"

READY_TIMEOUT_S = 30.0
STOP_TIMEOUT_S = 30.0
CHILD_GRACE_S = 10.0
_CALL_ENVS = (REPLAY_CALL_ENV, REPLAY_ARGS_FILE_ENV, REPLAY_OUTCOME_FILE_ENV)


class ChildCall(NamedTuple):
    """The trigger a replay's child calls (#84): its `module:qualname`, the file holding its
    captured args, and the file the child writes how the call ended to."""

    entrypoint: str
    args_file: Path
    outcome_file: Path


def child_env(
    base: dict[str, str],
    host: str,
    port: int,
    ca_cert: Path,
    run_id: str,
    *,
    replaying: bool = False,
    call: ChildCall | None = None,
) -> dict[str, str]:
    """`base` plus the proxy and CA variables. `base` is never mutated.

    `replaying` says the engine answers from a stored run (#83): the child is told so in
    `IRIMI_REPLAY_RUN`, naming `run_id`. Otherwise any `IRIMI_REPLAY_RUN` in `base` is dropped, so a
    shell that exported one cannot make a shadow run's SDK ask for recorded results.

    `call` is the trigger a replay's child calls (#84), for `irimi.sdk.replay_child`; without it
    its three variables are dropped, as `IRIMI_REPLAY_RUN` is.

    `ca_cert` must be the CA certificate (`~/.irimi/ca/ca.pem`), never the mitmproxy bundle,
    which also contains the private key. An IPv6 listen host is bracketed, as a URL spells it
    (#77).
    """
    proxy = f"http://{netaddr.authority(host, port)}"
    cert = str(ca_cert)
    env = dict(base)
    env.update(
        {
            # The SDK labels a run's requests only on a connection to what these name (#75).
            **dict.fromkeys(PROXY_ENVS, proxy),
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
    for name in (REPLAY_RUN_ENV, *_CALL_ENVS):
        env.pop(name, None)
    if replaying:
        env[REPLAY_RUN_ENV] = run_id
    if call is not None:
        values = (call.entrypoint, str(call.args_file), str(call.outcome_file))
        env.update(zip(_CALL_ENVS, values, strict=True))
    return env


def exit_code_for(returncode: int) -> int:
    """Shell convention: a child killed by signal N exits 128 + N (Popen reports -N)."""
    return 128 - returncode if returncode < 0 else returncode


def process_run(
    run_id: str,
    cmd: list[str],
    agent_version: str | None,
    started_at: float,
    trigger: Trigger | None = None,
) -> RunRecord:
    """The record `irimi shadow` stores for its child before spawning it (#70): one process tree
    is one run, and its trigger is the command line, which a replay can run again. `irimi replay`
    gives the trigger it replays instead (#84), and its store makes the record a replay's
    (`trace.mode_for`, #82)."""
    argv: list[JSONValue] = list(cmd)
    if trigger is None:
        trigger = Trigger(name=cmd[0], entrypoint=None, args={"argv": argv}, replayable=True)
    return RunRecord(
        schema_version=SCHEMA_VERSION,
        run_id=run_id,
        mode="shadow",
        attribution="process",
        trigger=trigger,
        agent_version=agent_version,
        engine_version=__version__,
        sdk_version=None,
        started_at=started_at,
        ended_at=None,
        outcome=None,
        error=None,
        exit_code=None,
    )


def end_process_run(
    store: TraceStore, run_id: str, code: int, ended_at: float, error: ErrorInfo | None = None
) -> None:
    """Close the process run with the child's exit code: `ok` for 0, `error` for anything else,
    a signal included (#70). `error` is what a replay's child said its call raised (#84), which the
    run records in place of its exit."""
    if code == 0:
        store.end_run(run_id, ended_at, "ok", exit_code=code)
    else:
        error = error or ErrorInfo("exit", f"exited {code}")
        store.end_run(run_id, ended_at, "error", error, code)


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
