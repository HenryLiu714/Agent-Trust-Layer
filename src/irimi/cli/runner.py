"""Process plumbing for proxy commands: child env, engine thread, signals and Python probes."""

import asyncio
import contextlib
import os
import signal
import subprocess
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, NamedTuple

from irimi.core import netaddr
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
from irimi.protocol.trace import ErrorInfo
from irimi.record.store import RunStore

NO_PROXY_VALUE = "localhost,127.0.0.1"

READY_TIMEOUT_S = 30.0
STOP_TIMEOUT_S = 30.0
CHILD_GRACE_S = 10.0
_CALL_ENVS = (REPLAY_CALL_ENV, REPLAY_ARGS_FILE_ENV, REPLAY_OUTCOME_FILE_ENV)
SERVE_STOP_SIGNALS = ("SIGINT", "SIGTERM")
# Imports only irimi, never the agent's code: that runs only in the child, under the engine.
PYTHON_PROBE = "import irimi.replay.child, irimi; print(irimi.__version__)"
PYTHON_PROBE_TIMEOUT_S = 30.0


class ChildCall(NamedTuple):
    """The trigger a replay's child calls, its args file, and the file it writes its outcome to."""

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
    """Return a copy of `base` plus the variables that route the child through irimi.

    Proxy:
        HTTP(S)_PROXY, http(s)_proxy  the listener (`protocol.env.PROXY_ENVS`)
        NO_PROXY, no_proxy            `NO_PROXY_VALUE`
        NODE_USE_ENV_PROXY=1          makes Node honour the proxy variables
    CA bundle, each irimi's CA certificate:
        SSL_CERT_FILE, REQUESTS_CA_BUNDLE, CURL_CA_BUNDLE, NODE_EXTRA_CA_CERTS
    irimi:
        IRIMI_ENGINE_ACTIVE=1         turns the SDK on
        IRIMI_RUN                     the run id
        IRIMI_CONTROL                 the control endpoint's base URL
        IRIMI_REPLAY_RUN              set only for a replay; removed otherwise
        IRIMI_REPLAY_CALL,            set only for a replay that calls a trigger: its entrypoint,
        IRIMI_REPLAY_ARGS_FILE,       its args file and the file the child writes its outcome
        IRIMI_REPLAY_OUTCOME_FILE     to; removed otherwise

    Replay variables in `base` are always dropped. `ca_cert` must be the CA certificate, never the
    mitmproxy bundle, which also holds the private key.
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
            # No trailing slash: the SDK appends `/runs/<id>/...`.
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
    """Return the shell exit code: 128 + N for a child killed by signal N (Popen reports -N)."""
    return 128 - returncode if returncode < 0 else returncode


def end_process_run(
    store: RunStore, run_id: str, code: int, ended_at: float, error: ErrorInfo | None = None
) -> None:
    """End the process run: `ok` for exit code 0, else `error` or what a replay's child raised."""
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
        """Stop the engine and wait for its thread. Idempotent."""
        if self.loop.is_closed():  # already stopped; shutdown() would touch the closed loop
            return
        self.engine.shutdown()
        self.thread.join(timeout=STOP_TIMEOUT_S)
        if not self.thread.is_alive():  # closing a loop its thread still runs raises
            self.loop.close()


def start_engine(engine: Engine, timeout: float = READY_TIMEOUT_S) -> EngineThread:
    """Return `engine` running in a thread once bound; else stop it and raise EngineStartError."""
    loop = asyncio.new_event_loop()

    def serve() -> None:
        # A failed bind reaches the caller via wait_ready(); raising here prints a second traceback.
        with contextlib.suppress(EngineStartError):
            loop.run_until_complete(engine.run())

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


class Child:
    """The command `irimi shadow` or `irimi replay` runs, which receives irimi's SIGTERM.

    Passing SIGTERM on lets the run end normally. One held before the command exists stops it from
    starting; a reaped command is never signalled, so a recycled pid is never hit.
    """

    def __init__(self) -> None:
        self.terminated = False
        self.interrupted = False  # Ctrl-C arrived while the command ran
        self._proc: subprocess.Popen[bytes] | None = None

    def on_sigterm(self, signum: int, _frame: object) -> None:
        self.terminated = True
        self._pass_on(signum)

    def adopt(self, proc: "subprocess.Popen[bytes]") -> None:
        """Take `proc` as the command, passing on any SIGTERM held while it started."""
        self._proc = proc
        if self.terminated:
            self._pass_on(signal.SIGTERM)

    def _pass_on(self, signum: int) -> None:
        proc = self._proc
        if proc is not None and proc.returncode is None:
            with contextlib.suppress(OSError):
                os.kill(proc.pid, signum)


@contextlib.contextmanager
def passing_sigterm_to(child: Child) -> Iterator[Child]:
    """Route SIGTERM to `child.on_sigterm` for the block (main thread only; SIG_IGN stays)."""
    if (
        threading.current_thread() is not threading.main_thread()
        or signal.getsignal(signal.SIGTERM) is signal.SIG_IGN
    ):
        yield child
        return
    previous = signal.signal(signal.SIGTERM, child.on_sigterm)
    try:
        yield child
    finally:
        # None means a handler not installed from Python, which `signal` cannot restore.
        signal.signal(signal.SIGTERM, signal.SIG_DFL if previous is None else previous)


@contextlib.contextmanager
def stopping_on_signals() -> Iterator[list[int]]:
    """Record SIGINT and SIGTERM in the yielded list for the block, and do nothing else.

    The handler only appends: it can run inside `Event.wait`, where `Event.set` would deadlock. Main
    thread only. After a signal, both stay ignored on exit so a second cannot cut shutdown short.
    """
    signalled: list[int] = []
    if threading.current_thread() is not threading.main_thread():
        yield signalled
        return
    previous = {}
    for name in SERVE_STOP_SIGNALS:
        signum = getattr(signal, name)
        if signal.getsignal(signum) is signal.SIG_IGN:
            continue
        previous[signum] = signal.signal(signum, lambda got, _frame: signalled.append(got))
    try:
        yield signalled
    finally:
        for signum, handler in previous.items():
            restored = signal.SIG_DFL if handler is None else handler
            signal.signal(signum, signal.SIG_IGN if signalled else restored)


def watch_engine(thread: threading.Thread, stop: threading.Event, died: threading.Event) -> None:
    """Stop serve when the engine thread ends; set `died` if unasked, so the sidecar looks dead."""
    thread.join()
    if not stop.is_set():
        died.set()
    stop.set()


def wait_for_child(proc: "subprocess.Popen[bytes]", child: Child) -> int:
    """Return the child's returncode; on Ctrl-C set `child.interrupted`, wait, then escalate."""
    try:
        return proc.wait()
    except KeyboardInterrupt:
        child.interrupted = True
    try:
        return proc.wait(timeout=CHILD_GRACE_S)
    except KeyboardInterrupt:
        pass
    except Exception:
        pass
    proc.terminate()
    try:
        return proc.wait(timeout=CHILD_GRACE_S)
    except Exception:
        pass
    proc.kill()
    try:
        return proc.wait(timeout=CHILD_GRACE_S)
    except Exception:
        # SIGKILL did not reap it (uninterruptible I/O); do not block the CLI forever.
        return -signal.SIGKILL


class PythonProbeFailed(Exception):
    """`probe_python` could not ask the Python: `reason` says how, `detail` why, unescaped."""

    def __init__(
        self, reason: Literal["timeout", "cannot-run", "no-replay-child"], detail: str
    ) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def probe_python(python: str) -> str | None:
    """Return the irimi version `python` has, or None. Raises PythonProbeFailed if it cannot ask."""
    timeout = PYTHON_PROBE_TIMEOUT_S
    try:
        probe = subprocess.run(
            [python, "-c", PYTHON_PROBE],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise PythonProbeFailed("timeout", f"{timeout:.0f}s") from None
    except (OSError, ValueError) as exc:  # ValueError: a path holding a NUL
        why = exc.strerror if isinstance(exc, OSError) and exc.strerror else str(exc)
        raise PythonProbeFailed("cannot-run", why) from None
    if probe.returncode != 0:
        said = probe.stderr.strip().splitlines()
        why = said[-1] if said else f"it exited {probe.returncode}"
        raise PythonProbeFailed("no-replay-child", why)
    printed = probe.stdout.strip().splitlines()
    return printed[-1] if printed else None
