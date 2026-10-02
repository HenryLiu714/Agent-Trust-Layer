"""How the SDK reaches irimi: one POST to the control endpoint per run start and end (#74).

`ControlClient.post` sends a JSON body to `{IRIMI_CONTROL}/runs/{run_id}/{action}`, the route the
control endpoint answers (#73), with stdlib urllib and NEVER THROUGH A PROXY: the endpoint is
irimi's own listener, `IRIMI_CONTROL` names it directly, and the agent's `HTTP_PROXY` points at
that same listener, which would only hand the request back to itself. It waits `TIMEOUT_S` at most.

IT NEVER RAISES. A post that fails costs irimi its record of the run, never the agent its call:
an unset `IRIMI_CONTROL`, a refused connection, a timeout, an answer outside 2xx, or anything else.
Each is logged at WARNING on the `irimi.sdk` logger, once per process per kind of failure, so a
worker handling a thousand messages against a stopped irimi logs one line, not two thousand.
"""

import logging
import os
import threading
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any, Literal, Protocol

from irimi import paths
from irimi.exchange import RUNS_ROUTE
from irimi.sdk.capture import serialized

LOGGER_NAME = "irimi.sdk"
logger = logging.getLogger(LOGGER_NAME)

TIMEOUT_S = 2.0
# The most of a refusal's body a warning quotes. The endpoint's own are shorter (#73).
MAX_DETAIL = 200

# The run actions the SDK posts, a subset of the endpoint's `exchange.RUN_ACTIONS`. #76 adds
# `tool-calls`.
Action = Literal["start", "end"]
# The kinds of failure a warning is logged once for.
Failure = Literal["unset", "rejected", "timeout", "unreachable", "failed"]


class Reporter(Protocol):
    """Where a run's start and end are sent. `ControlClient` is the one the SDK uses."""

    def post(self, run_id: str, action: Action, doc: Mapping[str, Any]) -> None: ...


class ControlClient:
    """Posts to the control endpoint named by `IRIMI_CONTROL`, read at each post."""

    def __init__(self) -> None:
        # No ProxyHandler entries: urllib's default opener reads HTTP_PROXY, this one never does.
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirects())
        self._warned: set[Failure] = set()
        self._lock = threading.Lock()

    def post(self, run_id: str, action: Action, doc: Mapping[str, Any]) -> None:
        """Send `doc`; on any failure, warn (once per kind) and return. Never raises."""
        try:
            failure = self._send(run_id, action, doc)
        except Exception as exc:  # a fault of the SDK's own, which the agent must not pay for
            failure = ("failed", f"{type(exc).__name__}: {exc}")
        if failure is not None:
            self._warn_once(failure[0], failure[1], run_id, action)

    def _send(
        self, run_id: str, action: Action, doc: Mapping[str, Any]
    ) -> tuple[Failure, str] | None:
        # irimi sets the variable with no trailing slash (#73), but a deployment that sets it by
        # hand (#77) may type one, which would make every route `//runs/...`, a 404.
        base = os.environ.get(paths.CONTROL_ENV, "").rstrip("/")
        if not base:
            return "unset", f"{paths.CONTROL_ENV} is not set"
        request = urllib.request.Request(
            f"{base}/{RUNS_ROUTE}/{run_id}/{action}",
            data=serialized(dict(doc)),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with self._opener.open(request, timeout=TIMEOUT_S) as response:
                response.read()
        except urllib.error.HTTPError as exc:  # an answer outside 2xx, a redirect included
            with exc:
                said = exc.read(MAX_DETAIL).decode("utf-8", "replace")
            return "rejected", f"HTTP {exc.code}: {said}"
        except urllib.error.URLError as exc:  # no answer: urllib wraps the socket's error
            if isinstance(exc.reason, TimeoutError):
                return "timeout", f"no answer in {TIMEOUT_S}s"
            return "unreachable", str(exc.reason)
        except TimeoutError:  # the answer started, then stalled
            return "timeout", f"no answer in {TIMEOUT_S}s"
        except OSError as exc:
            return "unreachable", f"{type(exc).__name__}: {exc}"
        return None

    def _warn_once(self, kind: Failure, detail: str, run_id: str, action: Action) -> None:
        with self._lock:
            if kind in self._warned:
                return
            self._warned.add(kind)
        logger.warning(
            "irimi could not record the %s of run %s (%s: %s). The run goes on, its requests "
            "still labelled; further failures of this kind are not logged.",
            action,
            run_id,
            kind,
            detail,
        )


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Never follows a redirect, so a 3xx is refused as an HTTPError like any answer outside 2xx.
    irimi's endpoint never sends one: it comes from something else on that port, and urllib would
    have followed a 301, 302 or 303 as a GET to wherever it pointed, outside the proxy, and called
    the run recorded when that answered (#74)."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None
