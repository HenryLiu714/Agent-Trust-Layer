"""Put `Irimi-Run` on every request a run sends to irimi, so the proxy files it under its run.

`instrument()` patches `http.client.HTTPConnection.putrequest` (urllib, requests/urllib3,
stripe-python's sync client, slack_sdk's `WebClient`) and, when httpx is installed, its two
transports (the OpenAI and Anthropic SDKs). The header goes only on a connection to irimi's own
listener, the host and port `IRIMI_CONTROL` or a proxy variable names, so a run's id never reaches
a service irimi is not in front of; the SDK's own posts carry none (`unlabelled`). The patches are
on classes, installed on the first run and never while the SDK is inactive. aiohttp is not covered.
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

# The port a URL with none names, by scheme.
DEFAULT_PORTS = {"http": 80, "https": 443}
# The variables that name irimi's listener: the control endpoint's, and the proxy's, which
# `runner.child_env` sets from the same `PROXY_ENVS`.
LISTENER_ENVS = (CONTROL_ENV, *PROXY_ENVS)

# Set while the SDK sends its own posts to the control endpoint: they name their run in their
# path, and carry no `Irimi-Run`.
_UNLABELLED: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "irimi_unlabelled", default=False
)

# Reentrant, because patching imports httpx, and an import hook may start a run on the thread that
# holds it: that call returns at once (`_patching`) rather than wait on itself forever.
_lock = threading.RLock()
# Set, under `_lock`, once every patch is in place, so a call that finds it set never sends a run's
# request ahead of them; and set even when a patch raised, so that patch is never retried and
# nothing is ever wrapped twice.
_installed = False
# Set, under `_lock`, while the first call patches, so a call made inside it never patches again.
_patching = False
# What `instrument` replaced, as (class, attribute, original), so `uninstall` can put it back.
_patched: list[tuple[type, str, Any]] = []


def instrument() -> None:
    """Label each request a run sends through irimi with the run's id.

    Patches `http.client` and httpx (if installed) process-wide, on their classes, so a request
    a run sends to irimi's listener carries `Irimi-Run` and irimi files it under that run. The SDK
    calls it on the first run; call it at startup to cover clients that cache connections before
    then. A no-op unless irimi is running the process, and safe to call more than once.

    Example:
        from irimi import sdk

        sdk.instrument()
    """
    global _installed, _patching
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
    """Requests sent inside carry no `Irimi-Run`, even to irimi: the SDK's own control posts."""
    token = _UNLABELLED.set(True)
    try:
        yield
    finally:
        _UNLABELLED.reset(token)


def run_id_for(host: str | bytes, port: int | None) -> str | None:
    """The run id a request sent over a connection to `host:port` carries: the current run's, if
    there is one and the connection is to irimi's listener; None otherwise, and inside
    `unlabelled()`. Never raises: a request it cannot decide for goes out unlabelled."""
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
    """irimi's listener as `(host, port)`s: each URL in `urls` that names a host, its port spelled
    out. Cached by the variables' values, which are read at each request."""
    found = set()
    for url in urls:
        # A proxy spelled with no scheme, `127.0.0.1:4000`, is an http proxy to urllib, requests
        # and httpx alike, so it names irimi as surely as one spelled in full.
        if url and "://" not in url:
            url = f"http://{url}"
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
    """Put back every method `instrument` replaced, so the next call patches afresh. For tests
    (`_state.reset_for_tests`): an agent has no reason to call it."""
    global _installed, _patching
    with _lock:
        while _patched:
            owner, name, original = _patched.pop()
            setattr(owner, name, original)
        _installed = _patching = False


def _labelling_putrequest(original: Callable[..., None]) -> Callable[..., None]:
    @functools.wraps(original)
    def putrequest(self: http.client.HTTPConnection, *args: Any, **kwargs: Any) -> None:
        original(self, *args, **kwargs)
        # `host` and `port` are where the connection goes: the proxy's when there is one, a
        # CONNECT tunnel's included, and the origin's otherwise.
        run_id = run_id_for(self.host, self.port)
        if run_id is not None:
            self.putheader(RUN_HEADER, run_id)

    return putrequest


def _httpx_run_id(transport: Any, request: Any) -> str | None:
    """The run id an httpx request carries, decided as `putrequest` decides, by where the
    transport connects: its proxy's address when it has one (httpcore's `_proxy_url`), the
    request URL's otherwise. None when the request already names a run of its own."""
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
    """`request` carries its run's id while inside, if `_httpx_run_id` gives it one. Both httpx
    transports send inside it, so the sync and async paths share one rule."""
    run_id = _httpx_run_id(transport, request)
    if run_id is None:
        yield
        return
    request.headers[RUN_HEADER] = run_id
    try:
        yield
    finally:
        # Off again once sent: httpx builds a redirect from this request's headers, and the next
        # hop may not go through irimi. Each hop is labelled by its own transport.
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
