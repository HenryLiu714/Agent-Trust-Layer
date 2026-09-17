"""Process-level plumbing for `irimi shadow`: child environment, banner, summary, engine thread.

No mitmproxy here. The engine is driven through the Engine protocol only.
"""

import asyncio
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from irimi.engine import Engine, EngineStartError
from irimi.exchange import Exchange

# Exactly the variables issue #3 specifies. Nothing is added to this list without a new issue.
NO_PROXY_VALUE = "localhost,127.0.0.1"
ENGINE_ACTIVE_ENV = "IRIMI_ENGINE_ACTIVE"
RUN_ENV = "IRIMI_RUN"

NOT_VIRTUALIZED_NOTICE = "hosts not routed through the proxy are NOT virtualized."
BACKSTOP_NOTICE = "backstop: none (Phase 4)"

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
        }
    )
    return env


def banner_lines(command: str, run_id: str, host: str, port: int, ca_cert: Path) -> list[str]:
    """The three startup lines. `command` is "shadow" or "serve"."""
    return [
        f"irimi {command} · run {run_id} · listening on {host}:{port} · ca {ca_cert}",
        NOT_VIRTUALIZED_NOTICE,
        BACKSTOP_NOTICE,
    ]


def exchange_line(exchange: Exchange) -> str:
    """One line per finished exchange."""
    status = exchange.response.status if exchange.response else "-"
    flags = f"  [{', '.join(exchange.flags)}]" if exchange.flags else ""
    return (
        f"{exchange.answered_by:<8} {exchange.kind:<8} {exchange.request.method} "
        f"{exchange.request.host}{exchange.request.path} -> {status}{flags}"
    )


def _kind_counts(exchanges: Sequence[Exchange]) -> str:
    """e.g. "read=2, unknown=1". Empty string when there are none. Sorted by kind name."""
    counts: dict[str, int] = {}
    for ex in exchanges:
        counts[ex.kind] = counts.get(ex.kind, 0) + 1
    return ", ".join(f"{kind}={counts[kind]}" for kind in sorted(counts))


def summary_lines(run_id: str, exchanges: Sequence[Exchange]) -> list[str]:
    """Minimal exit summary. Issue #13 replaces this with the real one."""
    live = [ex for ex in exchanges if ex.answered_by == "live"]
    virtualized = [ex for ex in exchanges if ex.answered_by != "live"]
    return [
        f"irimi run {run_id} · {len(exchanges)} exchange(s)",
        f"  live:        {len(live)}  {_kind_counts(live)}".rstrip(),
        f"  virtualized: {len(virtualized)}  {_kind_counts(virtualized)}".rstrip(),
    ]


def exit_code_for(returncode: int) -> int:
    """Shell convention: a child killed by signal N exits 128 + N (Popen reports -N)."""
    return 128 - returncode if returncode < 0 else returncode


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
