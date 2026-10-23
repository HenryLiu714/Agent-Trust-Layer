"""Process-level plumbing for `irimi shadow`: the child's environment and the engine thread.

No mitmproxy here. The engine is driven through the Engine protocol only. What a run *prints* is
`irimi.report`.
"""

import asyncio
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

from irimi import netaddr
from irimi.engine import Engine, EngineStartError
from irimi.protocol.control import CONTROL_PREFIX
from irimi.protocol.env import (
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
from irimi.trace import ErrorInfo

# Hosts the child reaches directly, bypassing the proxy.
NO_PROXY_VALUE = "localhost,127.0.0.1"

READY_TIMEOUT_S = 30.0
STOP_TIMEOUT_S = 30.0
CHILD_GRACE_S = 10.0
_CALL_ENVS = (REPLAY_CALL_ENV, REPLAY_ARGS_FILE_ENV, REPLAY_OUTCOME_FILE_ENV)


class ChildCall(NamedTuple):
    """The trigger a replay's child calls: its `module:qualname`, the file holding its
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
    """`base` plus the variables that route the child through irimi. `base` is never mutated.

        HTTP(S)_PROXY, http(s)_proxy  the listener (`protocol.env.PROXY_ENVS`)
        NO_PROXY, no_proxy            `NO_PROXY_VALUE`
        SSL_CERT_FILE and the like    irimi's CA certificate
        NODE_USE_ENV_PROXY=1          makes Node honour the proxy variables
        IRIMI_ENGINE_ACTIVE=1         turns the SDK on
        IRIMI_RUN                     the run id
        IRIMI_CONTROL                 the control endpoint's base URL
        IRIMI_REPLAY_RUN              set only for a replay; removed otherwise
        IRIMI_REPLAY_CALL,            set only for a replay that calls a trigger: its entrypoint,
        IRIMI_REPLAY_ARGS_FILE,       its args file and the file the child writes its outcome
        IRIMI_REPLAY_OUTCOME_FILE     to; removed otherwise

    The `IRIMI_*` names are declared in `protocol.env`, where the SDK can import them.
    `IRIMI_AGENT_VERSION` is read from the environment the child inherits, not set here.

    `replaying` says the engine answers from a stored run: the child is told so in
    `IRIMI_REPLAY_RUN`, naming `run_id`. Otherwise any `IRIMI_REPLAY_RUN` in `base` is dropped, so a
    shell that exported one cannot make a shadow run's SDK ask for recorded results.

    `call` is the trigger a replay's child calls, for `irimi.sdk.replay_child`; without it
    its three variables are dropped, as `IRIMI_REPLAY_RUN` is.

    `ca_cert` must be the CA certificate (`~/.irimi/ca/ca.pem`), never the mitmproxy bundle,
    which also contains the private key. An IPv6 listen host is bracketed, as a URL spells it.
    """
    proxy = f"http://{netaddr.authority(host, port)}"
    cert = str(ca_cert)
    env = dict(base)
    env.update(
        {
            # The SDK labels a run's requests only on a connection to what these name.
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
            # The control endpoint on this same listener, with no trailing slash, so the
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


def end_process_run(
    store: TraceStore, run_id: str, code: int, ended_at: float, error: ErrorInfo | None = None
) -> None:
    """Close the process run with the child's exit code: `ok` for 0, `error` for anything else,
    a signal included. `error` is what a replay's child said its call raised, which the
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

    Raises EngineStartError, always with a message, if it never binds or does not within
    `timeout`; the caller must then not spawn a child. The thread and loop are cleaned up before
    the exception propagates.
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
    except TimeoutError:
        handle.stop()
        raise EngineStartError(f"proxy did not start within {timeout:.0f}s") from None
    except EngineStartError as exc:
        handle.stop()
        if not str(exc):
            raise EngineStartError("proxy did not start") from None
        raise
    except BaseException:
        handle.stop()
        raise
    return handle
