"""What every sample agent shares: its HTTP client, its observation log, its configuration, and
the plumbing of a service that delivers its own inbound calls.

An agent here is an ordinary program. It reads its service base URLs and keys from the
environment, as a deployed agent would, and makes its calls with the stdlib (`urllib`), for the
reason #78 gives: CI installs only the `dev` group, and `urllib` honours `HTTPS_PROXY` and
`SSL_CERT_FILE` with no extra code.

The one thing it does that a real agent would not is keep an observation log, `obs()`: one JSON
line per event in the file `WORKFLOW_OBS` names. The harness asserts on it, so a test can see what
the AGENT saw (a status, an `Irimi-Answered-By`, a branch it took) and not only what irimi printed.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import signal
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

OBS_ENV = "WORKFLOW_OBS"
STATE_ENV = "WORKFLOW_STATE"
TIMEOUT_ENV = "WORKFLOW_HTTP_TIMEOUT"
# The port the harness serves every host name on. Unset in production, where a name's own port
# (80) serves it.
PORT_ENV = "WORKFLOW_INTERNET_PORT"
# An agent that hangs would hang the test that runs it, because `irimi shadow` waits for its
# child. SIGALRM's default action ends the process, so a stuck agent dies instead.
WATCHDOG_S = 90
# Where `irimi shadow` tells its child the control endpoint is (`irimi.paths.CONTROL_ENV`, #73),
# spelled here because an agent is an ordinary program that does not import irimi.
CONTROL_ENV = "IRIMI_CONTROL"
# The process run's id (`irimi.paths.RUN_ENV`), spelled here for the same reason (#73).
RUN_ENV = "IRIMI_RUN"
# The logger irimi's SDK speaks on (`irimi.sdk` is its child), spelled here for the same reason. A
# WARNING there is the SDK's only sign that a run's record was lost (#74).
IRIMI_LOGGER = "irimi"

_obs_lock = threading.Lock()


def start() -> None:
    """Call first in every agent's `main()`. Logs the proxy and the control endpoint (#73) the
    agent was given, so the harness can hold every run to what `irimi shadow` sets, and every bare
    run to having neither from irimi. From here on, each record an irimi logger emits is a `log`
    event too (#74)."""
    if hasattr(signal, "SIGALRM"):
        signal.alarm(WATCHDOG_S)
    logging.getLogger(IRIMI_LOGGER).addHandler(_LogToObs())
    obs(
        "start",
        proxy=proxy(),
        control=os.environ.get(CONTROL_ENV),
    )


class _LogToObs(logging.Handler):
    """Each record an irimi logger emits in the agent, as a `log` event. The SDK never raises
    into the agent: a control endpoint it cannot reach is one WARNING on `irimi.sdk` (#74), and
    this is how the harness sees it, and sees that no other run logs one."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            obs("log", logger=record.name, level=record.levelname, message=record.getMessage())
        except Exception:
            self.handleError(record)


def proxy() -> str | None:
    """The HTTP proxy the agent was given, as urllib reads it: `irimi shadow`'s listener, the fake
    internet in a bare run, or None."""
    return os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")


def obs(event: str, **data: Any) -> None:
    """Append one event to the observation log. A no-op when the log is not configured."""
    path = os.environ.get(OBS_ENV)
    if not path:
        return
    line = json.dumps({"event": event, "t": time.time(), **data}, default=repr)
    with _obs_lock, open(path, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def base(service: str) -> str:
    """`STRIPE_API_BASE` and friends, with the real service as the default."""
    defaults = {
        "stripe": "https://api.stripe.com",
        "slack": "https://slack.com",
        "slack_hooks": "https://hooks.slack.com",
        "anthropic": "https://api.anthropic.com",
        "openai": "https://api.openai.com",
        "langsmith": "https://api.smith.langchain.com",
    }
    return os.environ.get(f"{service.upper()}_API_BASE") or defaults[service]


def internal(host: str) -> str:
    """An internal service's base URL, `http://<host>`, on the harness's port when there is one."""
    port = os.environ.get(PORT_ENV)
    return f"http://{host}:{port}" if port else f"http://{host}"


def key(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"error: set {name}")
    return value


def state_dir() -> Path:
    """Where an agent keeps its local state (SQLite files, exports)."""
    path = Path(os.environ.get(STATE_ENV) or ".")
    path.mkdir(parents=True, exist_ok=True)
    return path


def timeout() -> float:
    return float(os.environ.get(TIMEOUT_ENV) or 10)


@contextlib.contextmanager
def sqlite(name: str) -> Iterator[sqlite3.Connection]:
    """One transaction on the SQLite file `name` in the state directory: committed if the block
    ends normally, rolled back if it raises, and always closed. `sqlite3`'s own context manager
    commits but never closes."""
    with contextlib.closing(sqlite3.connect(state_dir() / name)) as conn, conn:
        yield conn


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: bytes

    @property
    def answered_by(self) -> str | None:
        return self.headers.get("irimi-answered-by")

    def json(self) -> Any:
        try:
            return json.loads(self.body)
        except ValueError:
            return None

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


class NetworkError(Exception):
    """The request got no whole HTTP answer: refused, reset, timed out, or cut off mid-body."""


def http(
    method: str,
    url: str,
    *,
    json_body: Any = None,
    form: dict[str, Any] | None = None,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout_s: float | None = None,
    label: str | None = None,
) -> Response:
    """One request with urllib. An HTTP error status is returned, not raised; a request with no
    answer raises `NetworkError`. Either way the observation log gets one `http` event."""
    sent = dict(headers or {})
    data = body
    if json_body is not None:
        data = json.dumps(json_body).encode()
        sent.setdefault("Content-Type", "application/json")
    elif form is not None:
        data = urlencode(form).encode()
        sent.setdefault("Content-Type", "application/x-www-form-urlencoded")
    run_id = _current_run_id()
    if run_id is not None and not _sdk_labels_requests():
        sent.setdefault("Irimi-Run", run_id)
    request = urllib.request.Request(url, data=data, method=method, headers=sent)
    try:
        resp = _open(urllib.request.urlopen, request, timeout_s or timeout())
    except (OSError, HTTPException) as err:  # URLError is an OSError
        obs_http(method, url, label, error=type(err).__name__)
        parts = urlsplit(url)
        raise NetworkError(f"{method} {parts.hostname}{parts.path}: {err}") from err
    obs_http(method, url, label, status=resp.status, answered_by=resp.answered_by)
    return resp


def _open(opener: Any, request: urllib.request.Request, timeout_s: float) -> Response:
    """`opener(request)`, read whole, with an HTTP error status returned as a `Response`."""
    try:
        with opener(request, timeout=timeout_s) as raw:
            return Response(raw.status, {k.lower(): v for k, v in raw.headers.items()}, raw.read())
    except urllib.error.HTTPError as err:
        with err:  # it holds the connection open until closed
            return Response(err.code, {k.lower(): v for k, v in err.headers.items()}, err.read())


def obs_http(
    method: str,
    url: str,
    label: str | None,
    *,
    status: int | None = None,
    answered_by: str | None = None,
    error: str | None = None,
    **extra: Any,
) -> None:
    """Log one call as `http()` logs it, for a call made some other way (another client, a raw
    socket). A call with no answer logs its `error` and no `status` at all."""
    parts = urlsplit(url)
    record: dict[str, Any] = {"method": method, "url": f"{parts.hostname}{parts.path}"}
    record.update(label=label, run=_current_run_id(), **extra)
    if error is not None:
        record["error"] = error
    else:
        record.update(status=status, answered_by=answered_by)
    obs("http", **record)


def _current_run_id() -> str | None:
    # Imported late: `sdk` imports this module.
    from examples.workflows import sdk

    return sdk.current_run_id()


def _sdk_labels_requests() -> bool:
    from examples.workflows import sdk

    return sdk.labels_requests()


# -- the services every workflow talks to --------------------------------------------------------


def stripe(method: str, path: str, form: dict[str, Any] | None = None, **kw: Any) -> Response:
    headers = {"Authorization": f"Bearer {key('STRIPE_API_KEY')}", **kw.pop("headers", {})}
    return http(method, base("stripe") + path, form=form, headers=headers, **kw)


def slack(method: str, *, label: str | None = None, **params: Any) -> dict[str, Any]:
    """A Slack Web API call. Slack answers errors at 200 with `ok: false`, so this returns the
    document and leaves `ok` to the caller."""
    headers = {"Authorization": f"Bearer {key('SLACK_BOT_TOKEN')}"}
    url = f"{base('slack')}/api/{method}"
    resp = http("POST", url, json_body=params, headers=headers, label=label)
    doc = resp.json()
    return doc if isinstance(doc, dict) else {"ok": False, "error": f"http_{resp.status}"}


def anthropic(
    system: str,
    messages: list[dict[str, Any]],
    *,
    tools: list[dict[str, Any]] | None = None,
    max_tokens: int = 1024,
    label: str | None = None,
) -> dict[str, Any]:
    """One Anthropic Messages call. An error status raises: an agent has no answer to go on."""
    body: dict[str, Any] = {
        "model": "claude-sonnet-5",
        "max_tokens": max_tokens,
        "system": system,
        "messages": messages,
    }
    if tools:
        body["tools"] = tools
    headers = {"x-api-key": key("ANTHROPIC_API_KEY"), "anthropic-version": "2023-06-01"}
    url = base("anthropic") + "/v1/messages"
    resp = http("POST", url, json_body=body, headers=headers, label=label)
    doc = resp.json()
    if not resp.ok or not isinstance(doc, dict):
        raise RuntimeError(f"anthropic answered {resp.status}")
    return doc


def text_of(message: dict[str, Any]) -> str:
    return "".join(
        b.get("text", "") for b in message.get("content") or [] if b.get("type") == "text"
    )


# -- a service that delivers its own inbound calls -------------------------------------------------
#
# A webhook consumer or an endpoint serves on loopback and, standing in for its caller (a helpdesk,
# Slack, Stripe), delivers each inbound call to itself. An inbound call is not the agent's egress,
# so it goes through no proxy: irimi must never see it.


class Handler(BaseHTTPRequestHandler):
    """A request handler that answers JSON and keeps the stdlib's per-request log off stderr."""

    def reply(self, status: int, doc: dict[str, Any]) -> None:
        body = json.dumps(doc).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


@contextlib.contextmanager
def serve(handler: type[BaseHTTPRequestHandler]) -> Iterator[int]:
    """Serve `handler` on a free loopback port for the block, and yield the port."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True).start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()


def deliver(port: int, path: str, body: bytes, headers: dict[str, str] | None = None) -> Response:
    """POST one inbound call to our own server on `port`, through no proxy. It is not logged:
    the observation log's `http` events are the agent's egress."""
    sent = {"Content-Type": "application/json", **(headers or {})}
    request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", body, sent, method="POST")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return _open(opener.open, request, 60)
