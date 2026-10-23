"""How the SDK reaches irimi: one POST to the control endpoint per run start, run end and
tool call, and in a replay one per read tool call, asking for its recorded result.

`ControlClient.post` sends a JSON body to `{IRIMI_CONTROL}/runs/{run_id}/{action}`, the route the
control endpoint answers, with stdlib urllib and NEVER THROUGH A PROXY: the endpoint is
irimi's own listener, `IRIMI_CONTROL` names it directly, and the agent's `HTTP_PROXY` points at
that same listener, which would only hand the request back to itself. It waits `TIMEOUT_S` at most.

IT NEVER RAISES. A post that fails costs irimi its record of the run, never the agent its call:
an unset `IRIMI_CONTROL`, a refused connection, a timeout, an answer outside 2xx, or anything else.
A lookup that fails is a miss, and the tool runs for real; a `404` is the endpoint's own miss, and
is not a failure.
Each is logged at WARNING on the `irimi.sdk` logger, once per process per kind of failure, so a
worker handling a thousand messages against a stopped irimi logs one line, not two thousand.
"""

import json
import logging
import os
import threading
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from irimi import paths, trace
from irimi.exchange import RECORDED_ACTION, RUNS_ROUTE, TOOL_CALLS_ACTION
from irimi.sdk.capture import MAX_CAPTURED_BYTES, serialized
from irimi.sdk.instrumentation import unlabelled
from irimi.trace import JSONValue, RecordedOutcome

LOGGER_NAME = "irimi.sdk"
logger = logging.getLogger(LOGGER_NAME)

TIMEOUT_S = 2.0
# The most of a refusal's body a warning quotes. The endpoint's own are shorter.
MAX_DETAIL = 200
# The most of an answer the SDK reads. irimi's largest is a recorded tool call's outcome,
# whose result the SDK posted at most MAX_CAPTURED_BYTES of; one past this is from something else
# on the port, and is refused rather than read into the agent's memory to its end.
MAX_ANSWER = 16 * MAX_CAPTURED_BYTES

# The run actions the SDK posts: the endpoint's `exchange.RUN_ACTIONS`.
Action = Literal["start", "tool-calls", "end"]
# The kinds of failure a warning is logged once for.
Failure = Literal["unset", "rejected", "timeout", "unreachable", "failed"]


class Reporter(Protocol):
    """Where a run's start, its end and its tool calls are sent, and, in a replay, where a read
    tool's recorded result is asked for. `ControlClient` is the one the SDK uses."""

    def post(self, run_id: str, action: Action, doc: Mapping[str, Any]) -> None: ...

    def recorded(self, run_id: str, name: str, args: JSONValue) -> RecordedOutcome | None: ...


@dataclass(frozen=True)
class _Failed:
    """A request that got no 2xx: the kind of failure, what to say of it, and the status of an
    answer outside 2xx."""

    kind: Failure
    detail: str
    status: int | None = None


class ControlClient:
    """Posts to the control endpoint named by `IRIMI_CONTROL`, read at each post."""

    def __init__(self) -> None:
        # No ProxyHandler entries: urllib's default opener reads HTTP_PROXY, this one never does.
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirects())
        # By what failed and how: a lookup that fails never silences a post's warning.
        self._warned: set[tuple[str, Failure]] = set()
        self._lock = threading.Lock()

    def post(self, run_id: str, action: Action, doc: Mapping[str, Any]) -> None:
        """Send `doc`; on any failure, warn (once per kind) and return. Never raises."""
        try:
            sent = self._send(f"{run_id}/{action}", doc)
        except Exception as exc:  # a fault of the SDK's own, which the agent must not pay for
            sent = _Failed("failed", f"{type(exc).__name__}: {exc}")
        if isinstance(sent, _Failed):
            what = "a tool call" if action == TOOL_CALLS_ACTION else f"the {action}"
            self._warn_once(
                "post",
                sent,
                f"record {what} of run {run_id}",
                "The run goes on, its requests still labelled",
            )

    def recorded(self, run_id: str, name: str, args: JSONValue) -> RecordedOutcome | None:
        """The recorded outcome of the read tool `name` called with `args`, from a replay's
        recording: the endpoint's answer to `runs/{run_id}/tool-calls/recorded`, read by
        `trace.recorded_outcome_from_json`, or None for a miss. Its `404` is the endpoint's miss,
        said by no warning; any other failure, and an answer that does not decode, is warned once
        per kind and is a miss too. Never raises."""
        lookup = dict(zip(trace.RECORDED_LOOKUP_FIELDS, (name, args), strict=True))
        try:
            sent = self._send(f"{run_id}/{RECORDED_ACTION}", lookup)
            if isinstance(sent, _Failed):
                if sent.status == 404:
                    return None
            else:
                answer = json.loads(sent)
                if isinstance(answer, dict):
                    return trace.recorded_outcome_from_json(answer)
                sent = _Failed("rejected", "the answer is not a recorded call")
        except Exception as exc:  # TraceFormatError included: an answer this cannot read
            sent = _Failed("failed", f"{type(exc).__name__}: {exc}")
        self._warn_once(
            "lookup",
            sent,
            f"look up a recorded call of tool {name} in run {run_id}",
            "The tool runs for real",
        )
        return None

    def _send(self, route: str, doc: Mapping[str, Any]) -> bytes | _Failed:
        """POST `doc` to `{IRIMI_CONTROL}/runs/{route}`: the answer's body, or why there is none."""
        # irimi sets the variable with no trailing slash, but a deployment that sets it by
        # hand may type one, which would make every route `//runs/...`, a 404.
        base = os.environ.get(paths.CONTROL_ENV, "").rstrip("/")
        if not base:
            return _Failed("unset", f"{paths.CONTROL_ENV} is not set")
        request = urllib.request.Request(
            f"{base}/{RUNS_ROUTE}/{route}",
            data=serialized(dict(doc)),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            # To irimi's own listener, which `instrument()` labels: the route names the run.
            with unlabelled(), self._opener.open(request, timeout=TIMEOUT_S) as response:
                body: bytes = response.read(MAX_ANSWER + 1)
        except urllib.error.HTTPError as exc:  # an answer outside 2xx, a redirect included
            with exc:
                said = exc.read(MAX_DETAIL).decode("utf-8", "replace")
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
        with self._lock:
            if (route, failed.kind) in self._warned:
                return
            self._warned.add((route, failed.kind))
        logger.warning(
            "irimi could not %s (%s: %s). %s; further failures of this kind are not logged.",
            what,
            failed.kind,
            failed.detail,
            consequence,
        )


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Never follows a redirect, so a 3xx is refused as an HTTPError like any answer outside 2xx.
    irimi's endpoint never sends one: it comes from something else on that port, and urllib would
    have followed a 301, 302 or 303 as a GET to wherever it pointed, outside the proxy, and called
    the run recorded when that answered."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None
