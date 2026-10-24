"""Post run starts, run ends and tool calls to irimi's control endpoint, and look up replays.

`ControlClient` posts to `{IRIMI_CONTROL}/runs/{run_id}/{action}` with stdlib urllib, bypassing
any proxy, and waits at most `TIMEOUT_S`.

- Never raises (the SDK never raises); each kind of failure is warned of once per process.
- A failed lookup is a miss, so the tool runs for real.
- A timeout or unreachable endpoint pauses every send for `PAUSE_S`.
- A proxy speaking another protocol major answers `426`.
"""

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from irimi.core.limits import MAX_ANSWER
from irimi.core.opener import no_redirect_opener
from irimi.protocol.control import (
    PROTOCOL_VERSION,
    RECORDED_ACTION,
    RECORDED_LOOKUP_FIELDS,
    RUNS_ROUTE,
    TOOL_CALLS_ACTION,
    RecordedOutcome,
    RunAction,
    recorded_outcome_from_json,
)
from irimi.protocol.env import CONTROL_ENV
from irimi.protocol.headers import PROTOCOL_HEADER
from irimi.protocol.trace import JSONValue
from irimi.sdk._capture import serialized
from irimi.sdk._http_labels import unlabelled

LOGGER_NAME = "irimi.sdk"
logger = logging.getLogger(LOGGER_NAME)

TIMEOUT_S = 2.0
PAUSE_S = 30.0
# How much of a refusal's body a warning quotes.
MAX_DETAIL = 200

# `paused` is a send the breaker skipped; it is never warned of, as the failure that opened the
# breaker was.
Failure = Literal["unset", "rejected", "timeout", "unreachable", "failed", "mismatch", "paused"]
PAUSING: frozenset[Failure] = frozenset({"timeout", "unreachable"})


class Reporter(Protocol):
    """Receive run starts, run ends and tool calls, and answer replay lookups."""

    def post(self, run_id: str, action: RunAction, doc: Mapping[str, Any]) -> None: ...

    def recorded(self, run_id: str, name: str, args: JSONValue) -> RecordedOutcome | None: ...


@dataclass(frozen=True)
class _Failed:
    """Describe a request that got no 2xx; `status` is set when there was an answer."""

    kind: Failure
    detail: str
    status: int | None = None


class ControlClient:
    """Post to the control endpoint named by `IRIMI_CONTROL`, read at each post."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        # A 3xx comes from something else on that port, never irimi, so it is refused.
        self._opener = no_redirect_opener()
        # Keyed by route too, so a failed lookup never silences a post's warning.
        self._warned: set[tuple[str, Failure]] = set()
        self._lock = threading.Lock()
        self._clock = clock
        self._paused_until = float("-inf")

    def post(self, run_id: str, action: RunAction, doc: Mapping[str, Any]) -> None:
        """Send `doc`; on failure, warn once per kind. Never raises (the SDK never raises)."""
        try:
            sent = self._send(f"{run_id}/{action}", doc)
        except Exception as exc:  # the SDK never raises
            sent = _Failed("failed", f"{type(exc).__name__}: {exc}")
        if isinstance(sent, _Failed) and sent.kind != "paused":
            what = "a tool call" if action == TOOL_CALLS_ACTION else f"the {action}"
            self._warn_once(
                "post",
                sent,
                f"record {what} of run {run_id}",
                "the run goes on, its requests still labelled",
            )

    def recorded(self, run_id: str, name: str, args: JSONValue) -> RecordedOutcome | None:
        """Return the recorded outcome of read tool `name` called with `args`, or None for a miss.

        A `404` is a silent miss; any other failure or undecodable answer is warned of once per
        kind and is also a miss. Never raises (the SDK never raises).
        """
        lookup = dict(zip(RECORDED_LOOKUP_FIELDS, (name, args), strict=True))
        try:
            sent = self._send(f"{run_id}/{RECORDED_ACTION}", lookup)
            if isinstance(sent, _Failed):
                if sent.status == 404 or sent.kind == "paused":
                    return None
            else:
                answer = json.loads(sent)
                if isinstance(answer, dict):
                    return recorded_outcome_from_json(answer)
                sent = _Failed("rejected", "the answer is not a recorded call")
        except Exception as exc:  # TraceFormatError included
            sent = _Failed("failed", f"{type(exc).__name__}: {exc}")
        self._warn_once(
            "lookup",
            sent,
            f"look up a recorded call of tool {name} in run {run_id}",
            "the tool runs for real",
        )
        return None

    def _send(self, route: str, doc: Mapping[str, Any]) -> bytes | _Failed:
        """POST `doc` to `{IRIMI_CONTROL}/runs/{route}`; return the body or the failure."""
        if self._clock() < self._paused_until:
            return _Failed("paused", "the circuit breaker is open")
        sent = self._post(route, doc)
        if isinstance(sent, _Failed) and sent.kind in PAUSING:
            with self._lock:
                self._paused_until = self._clock() + PAUSE_S
        return sent

    def _post(self, route: str, doc: Mapping[str, Any]) -> bytes | _Failed:
        # A hand-set value may end in a slash, which would make every route `//runs/...`, a 404.
        base = os.environ.get(CONTROL_ENV, "").rstrip("/")
        if not base:
            return _Failed("unset", f"{CONTROL_ENV} is not set")
        request = urllib.request.Request(  # noqa: S310  # the http URL irimi set for its endpoint
            f"{base}/{RUNS_ROUTE}/{route}",
            data=serialized(dict(doc)),
            method="POST",
            headers={"Content-Type": "application/json", PROTOCOL_HEADER: PROTOCOL_VERSION},
        )
        try:
            # The route names the run, so `instrument()` must not add `Irimi-Run`.
            with unlabelled(), self._opener.open(request, timeout=TIMEOUT_S) as response:
                body: bytes = response.read(MAX_ANSWER + 1)
        except urllib.error.HTTPError as exc:  # any non-2xx answer, redirects included
            with exc:
                said = exc.read(MAX_DETAIL).decode("utf-8", "replace")
            if exc.code == 426:
                theirs = exc.headers.get(PROTOCOL_HEADER) or "unknown"
                return _Failed("mismatch", f"{base} speaks {theirs[:MAX_DETAIL]}", exc.code)
            return _Failed("rejected", f"HTTP {exc.code}: {said}", exc.code)
        except urllib.error.URLError as exc:  # no answer: urllib wraps the socket's error
            if isinstance(exc.reason, TimeoutError):
                return _Failed("timeout", f"no answer in {TIMEOUT_S}s")
            return _Failed("unreachable", str(exc.reason))
        except TimeoutError:  # the answer started, then stalled
            return _Failed("timeout", f"no answer in {TIMEOUT_S}s")
        except OSError as exc:
            return _Failed("unreachable", f"{type(exc).__name__}: {exc}")
        if len(body) > MAX_ANSWER:
            return _Failed("rejected", f"the answer is past {MAX_ANSWER} bytes")
        return body

    def _warn_once(self, route: str, failed: _Failed, what: str, consequence: str) -> None:
        # The protocol versions are the same for every route, so a mismatch is warned of once.
        key = ("any" if failed.kind == "mismatch" else route, failed.kind)
        with self._lock:
            if key in self._warned:
                return
            self._warned.add(key)
        if failed.kind == "mismatch":
            logger.warning(
                "irimi: this SDK speaks control protocol %s, but the proxy at %s; runs are not "
                "recorded until the two match",
                PROTOCOL_VERSION,
                failed.detail,
            )
            return
        if failed.kind in PAUSING:
            consequence += f"; irimi is not asked again for {PAUSE_S:g}s"
        logger.warning(
            "irimi: could not %s (%s: %s); %s; further failures of this kind are not logged",
            what,
            failed.kind,
            failed.detail,
            consequence,
        )
