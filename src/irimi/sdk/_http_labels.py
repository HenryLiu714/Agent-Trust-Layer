"""Put `Irimi-Run` on every request a run sends to irimi, so the proxy files it under its run.

`instrument()` patches `http.client.HTTPConnection.putrequest` (urllib, requests/urllib3) and, if
installed, `httpx.HTTPTransport` and `httpx.AsyncHTTPTransport`. Any other transport (aiohttp, or
anthropic 2.x, which uses httpx2) needs the `Irimi-Run` header set by hand. The header goes only
to irimi's listener (`IRIMI_CONTROL` or a proxy variable), so a run's id reaches no other service.
"""

import contextlib
import contextvars
import functools
import http.client
import importlib
import importlib.util
import os
import threading
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import urlsplit

from irimi.protocol.env import CONTROL_ENV, PROXY_ENVS
from irimi.protocol.headers import RUN_HEADER
from irimi.sdk._context import current_run_id, is_active

DEFAULT_PORTS = {"http": 80, "https": 443}
LISTENER_ENVS = (CONTROL_ENV, *PROXY_ENVS)

# The SDK's own control posts name their run in the path and carry no `Irimi-Run`.
_UNLABELLED: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "irimi_unlabelled", default=False
)

# Reentrant: patching imports httpx, and an import hook may start a run on the thread holding it;
# that call returns at once (`_patching`) instead of deadlocking.
_lock = threading.RLock()
# Set even when a patch raised, so nothing is retried or wrapped twice.
_installed = False
_patching = False
_patched: list[tuple[type, str, Any]] = []


def instrument() -> None:
    """Label each request a run sends through irimi with the run's id.

    Patches `http.client` and httpx process-wide. The SDK calls it on the first run; call it at
    startup to cover clients that cache connections earlier. A no-op unless irimi is active.

    Example:
        sdk.instrument()
    """
    global _installed, _patching  # noqa: PLW0603  # the patch is process-wide, and so are these
    if _installed or not is_active():
        return
    with _lock:
        if _installed or _patching:
            return
        _patching = True
        try:
            _patch(http.client.HTTPConnection, "putrequest", _labelling_putrequest)
            if importlib.util.find_spec("httpx") is not None:
                httpx = importlib.import_module("httpx")
                _patch(httpx.HTTPTransport, "handle_request", _labelling_handle_request)
                _patch(
                    httpx.AsyncHTTPTransport,
                    "handle_async_request",
                    _labelling_handle_async_request,
                )
        finally:
            _installed, _patching = True, False


@contextlib.contextmanager
def unlabelled() -> Iterator[None]:
    """Send requests inside without `Irimi-Run`, even to irimi."""
    token = _UNLABELLED.set(True)
    try:
        yield
    finally:
        _UNLABELLED.reset(token)


def run_id_for(host: str | bytes, port: int | None) -> str | None:
    """Return the current run's id if `host:port` is irimi's listener, else None. Never raises."""
    try:
        run_id = current_run_id()
        if run_id is None or _UNLABELLED.get():
            return None
        if isinstance(host, bytes):
            host = host.decode("ascii", "replace")
        listeners = _listeners(tuple(os.environ.get(name, "") for name in LISTENER_ENVS))
        return run_id if (host.lower(), port) in listeners else None
    except Exception:
        return None


@functools.lru_cache(maxsize=8)
def _listeners(urls: tuple[str, ...]) -> frozenset[tuple[str, int]]:
    """Return the `(host, port)` each URL in `urls` names, skipping any without a host."""
    found = set()
    for given in urls:
        # urllib, requests and httpx all treat a schemeless proxy (`127.0.0.1:4000`) as http.
        url = f"http://{given}" if given and "://" not in given else given
        try:
            parts = urlsplit(url)
            port = parts.port or DEFAULT_PORTS.get(parts.scheme)
        except ValueError:  # a port that is not a number
            continue
        if parts.hostname and port is not None:
            found.add((parts.hostname, port))
    return frozenset(found)


def _patch(owner: type, name: str, wrap: Callable[[Any], Any]) -> None:
    original = getattr(owner, name)
    setattr(owner, name, wrap(original))
    _patched.append((owner, name, original))


def uninstall() -> None:
    """Restore every method `instrument` replaced, so the next call patches afresh. For tests."""
    global _installed, _patching  # noqa: PLW0603  # the patch is process-wide, and so are these
    with _lock:
        while _patched:
            owner, name, original = _patched.pop()
            setattr(owner, name, original)
        _installed = _patching = False


def _labelling_putrequest(original: Callable[..., None]) -> Callable[..., None]:
    @functools.wraps(original)
    def putrequest(self: http.client.HTTPConnection, *args: Any, **kwargs: Any) -> None:
        original(self, *args, **kwargs)
        # `host` and `port` are the proxy's when there is one (CONNECT tunnels included).
        run_id = run_id_for(self.host, self.port)
        if run_id is not None:
            self.putheader(RUN_HEADER, run_id)

    return putrequest


def _httpx_run_id(transport: Any, request: Any) -> str | None:
    """Return the run id for an httpx request by its proxy or URL; None if it has `Irimi-Run`."""
    try:
        if RUN_HEADER in request.headers:
            return None
        proxy = getattr(getattr(transport, "_pool", None), "_proxy_url", None)
        if proxy is not None:
            scheme = proxy.scheme.decode("ascii", "replace")
            return run_id_for(proxy.host, proxy.port or DEFAULT_PORTS.get(scheme))
        url = request.url
        return run_id_for(url.host, url.port or DEFAULT_PORTS.get(url.scheme))
    except Exception:
        return None


@contextlib.contextmanager
def _labelled(transport: Any, request: Any) -> Iterator[None]:
    """Set `Irimi-Run` on `request` for the duration of the block, if `_httpx_run_id` gives one."""
    run_id = _httpx_run_id(transport, request)
    if run_id is None:
        yield
        return
    request.headers[RUN_HEADER] = run_id
    try:
        yield
    finally:
        # httpx builds a redirect from these headers, and the next hop may not go through irimi.
        del request.headers[RUN_HEADER]


def _labelling_handle_request(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    def handle_request(self: Any, request: Any) -> Any:
        with _labelled(self, request):
            return original(self, request)

    return handle_request


def _labelling_handle_async_request(original: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(original)
    async def handle_async_request(self: Any, request: Any) -> Any:
        with _labelled(self, request):
            return await original(self, request)

    return handle_async_request
