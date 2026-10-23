"""Process-level plumbing for the commands that run a proxy: the child's environment, the engine
thread, signal handling, waiting for the child and asking a Python for its irimi.

No mitmproxy here. The engine is driven through the Engine protocol only. What a run *prints* is
`irimi.report`.
"""

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
# The signals that stop a serve: Ctrl-C, and what a container runtime or supervisor sends.
SERVE_STOP_SIGNALS = ("SIGINT", "SIGTERM")
# What `probe_python` runs under the Python it asks: it imports irimi's replay child, which calls
# the trigger and writes how the call ended, and prints that irimi's version. It imports irimi
# alone: the agent's code runs only in the child, under the replay's engine.
PYTHON_PROBE = "import irimi._replay_child, irimi; print(irimi.__version__)"
PYTHON_PROBE_TIMEOUT_S = 30.0


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

    The `IRIMI_*` names are declared in `protocol.env`, where the SDK can import them.
    `IRIMI_AGENT_VERSION` is read from the environment the child inherits, not set here.

    `replaying` says the engine answers from a stored run: the child is told so in
    `IRIMI_REPLAY_RUN`, naming `run_id`. Otherwise any `IRIMI_REPLAY_RUN` in `base` is dropped, so a
    shell that exported one cannot make a shadow run's SDK ask for recorded results.

    `call` is the trigger a replay's child calls, for `irimi._replay_child`; without it
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


class Child:
    """The command `irimi shadow` or `irimi replay` runs, as SIGTERM to irimi sees it.

    A supervisor or a container runtime stops `irimi shadow` with SIGTERM, which reaches irimi
    alone, not its command. Its default action would end irimi at once, with the command still
    running and its proxy gone, the run never ended and the store never closed. So SIGTERM is
    passed to the command instead, and the command's exit ends the run as any exit does. One that
    arrives before the command exists is held, and the command is then never started.

    The handler takes no lock, as serve's does not: `os.kill` and two attribute reads. A command
    already reaped has its `returncode` and is never signalled, so a recycled pid is never hit."""

    def __init__(self) -> None:
        self.terminated = False
        # A Ctrl-C reached irimi while the command ran (`wait_for_child`), which a replay that it
        # stopped says.
        self.interrupted = False
        self._proc: subprocess.Popen[bytes] | None = None

    def on_sigterm(self, signum: int, _frame: object) -> None:
        self.terminated = True
        self._pass_on(signum)

    def adopt(self, proc: "subprocess.Popen[bytes]") -> None:
        """`proc` is the command now. A SIGTERM that came while it was being started, when there
        was no command to pass it to, is passed to it here."""
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
    """For the length of the block, SIGTERM is `child.on_sigterm`, on the main thread only, where
    Python allows a handler; a SIGTERM ignored when irimi started stays ignored, as under serve.
    The handler found is put back on the way out. SIGINT is untouched: Ctrl-C reaches the whole
    foreground process group, the command included, and `wait_for_child` handles it."""
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
        # None: the handler was not installed from Python, which `signal` cannot put back.
        signal.signal(signal.SIGTERM, signal.SIG_DFL if previous is None else previous)


@contextlib.contextmanager
def stopping_on_signals() -> Iterator[list[int]]:
    """For the length of the block, SIGINT and SIGTERM append their number to the list it yields
    and do nothing else: no KeyboardInterrupt to unwind through the engine thread's join, and a
    second signal while the store closes is ignored rather than cutting its last writes short. A
    container runtime sends SIGTERM and waits, which is what serve exists for.

    A handler takes no lock. It runs on the main thread between two bytecodes, which may be
    inside `Event.wait` holding the event's own lock, so a handler calling `Event.set` there
    waits on itself forever: the list is the whole of its work, and the main thread polls it.
    Off the main thread, where Python refuses a handler, nothing is installed.

    A signal ignored when serve started stays ignored: `irimi serve &` from a script inherits an
    ignored SIGINT, and a Ctrl-C meant for the script must not stop it. Once a signal has arrived,
    serve is stopping, so on the way out the signals are ignored rather than given back: a second
    one during the interpreter's exit would otherwise end it with the default action. With none,
    the handlers found are put back."""
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
            # None: the handler was not installed from Python, which `signal` cannot put back.
            restored = signal.SIG_DFL if handler is None else handler
            signal.signal(signum, signal.SIG_IGN if signalled else restored)


def watch_engine(thread: threading.Thread, stop: threading.Event, died: threading.Event) -> None:
    """Stop serve when the engine's thread ends, and say whether it ended first. An engine that
    stops on its own ends serve as a failure: a sidecar whose proxy is gone must not look alive
    to its supervisor."""
    thread.join()
    if not stop.is_set():
        died.set()
    stop.set()


def wait_for_child(proc: "subprocess.Popen[bytes]", child: Child) -> int:
    """Wait for the child, surviving Ctrl-C, which `child.interrupted` then says. Returns Popen's
    returncode (negative if signalled).

    Ctrl-C in a terminal reaches the whole foreground process group, so the child already got its
    own SIGINT; give it time to exit, then escalate.
    """
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
        # SIGKILL did not reap it (uninterruptible I/O). Don't block the CLI forever.
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
    """The irimi version `python` has, as the last line `PYTHON_PROBE` printed under it, or None
    when it printed none. Raises PythonProbeFailed when it does not answer within
    `PYTHON_PROBE_TIMEOUT_S`, cannot be run, or cannot import irimi's replay child."""
    timeout = PYTHON_PROBE_TIMEOUT_S
    try:
        probe = subprocess.run(
            [python, "-c", PYTHON_PROBE],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
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
