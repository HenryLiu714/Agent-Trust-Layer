"""What every sample agent shares: its HTTP client, its observation log, and its configuration.

An agent here is an ordinary program. It reads its service base URLs and keys from the
environment, as a deployed agent would, and makes its calls with the stdlib (`urllib`), for the
reason #78 gives: CI installs only the `dev` group, and `urllib` honours `HTTPS_PROXY` and
`SSL_CERT_FILE` with no extra code.

The one thing it does that a real agent would not is keep an observation log, `obs()`: one JSON
line per event in the file `WORKFLOW_OBS` names. The harness asserts on it, so a test can see what
the AGENT saw (a status, an `Irimi-Answered-By`, a branch it took) and not only what irimi printed.
"""

from __future__ import annotations

import json
import os
import signal
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

OBS_ENV = "WORKFLOW_OBS"
STATE_ENV = "WORKFLOW_STATE"
TIMEOUT_ENV = "WORKFLOW_HTTP_TIMEOUT"
# An agent that hangs would hang the test that runs it, because `irimi shadow` waits for its
# child. SIGALRM's default action ends the process, so a stuck agent dies instead.
WATCHDOG_S = 90

_obs_lock = threading.Lock()


def start() -> None:
    """Call first in every agent's `main()`."""
    if hasattr(signal, "SIGALRM"):
        signal.alarm(WATCHDOG_S)


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
    }
    return os.environ.get(f"{service.upper()}_API_BASE") or defaults[service]


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
    """The request got no HTTP answer at all: refused, reset, or timed out."""


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
    if run_id is not None:
        sent.setdefault("Irimi-Run", run_id)
    parts = urlsplit(url)
    where = f"{parts.hostname}{parts.path}"
    request = urllib.request.Request(url, data=data, method=method, headers=sent)
    try:
        with urllib.request.urlopen(request, timeout=timeout_s or timeout()) as raw:
            resp = Response(raw.status, {k.lower(): v for k, v in raw.headers.items()}, raw.read())
    except urllib.error.HTTPError as err:
        resp = Response(err.code, {k.lower(): v for k, v in err.headers.items()}, err.read())
    except (urllib.error.URLError, OSError) as err:
        obs("http", method=method, url=where, label=label, error=type(err).__name__, run=run_id)
        raise NetworkError(f"{method} {where}: {err}") from err
    obs(
        "http",
        method=method,
        url=where,
        label=label,
        status=resp.status,
        answered_by=resp.answered_by,
        run=run_id,
    )
    return resp


def _current_run_id() -> str | None:
    # Imported late: `sdk` imports this module.
    from examples.workflows import sdk

    return sdk.current_run_id()


# -- the services every workflow talks to --------------------------------------------------------


def stripe(method: str, path: str, form: dict[str, Any] | None = None, **kw: Any) -> Response:
    headers = {"Authorization": f"Bearer {key('STRIPE_API_KEY')}", **kw.pop("headers", {})}
    return http(method, base("stripe") + path, form=form, headers=headers, **kw)


def slack(method: str, **params: Any) -> dict[str, Any]:
    """A Slack Web API call. Slack answers errors at 200 with `ok: false`, so this returns the
    document and leaves `ok` to the caller."""
    headers = {"Authorization": f"Bearer {key('SLACK_BOT_TOKEN')}"}
    resp = http("POST", f"{base('slack')}/api/{method}", json_body=params, headers=headers)
    doc = resp.json()
    return doc if isinstance(doc, dict) else {"ok": False, "error": f"http_{resp.status}"}


def anthropic(
    system: str,
    messages: list[dict[str, Any]],
    *,
    tools: list[dict[str, Any]] | None = None,
    max_tokens: int = 1024,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "model": "claude-sonnet-5",
        "max_tokens": max_tokens,
        "system": system,
        "messages": messages,
    }
    if tools:
        body["tools"] = tools
    headers = {"x-api-key": key("ANTHROPIC_API_KEY"), "anthropic-version": "2023-06-01"}
    resp = http("POST", base("anthropic") + "/v1/messages", json_body=body, headers=headers)
    doc = resp.json()
    if not resp.ok or not isinstance(doc, dict):
        raise RuntimeError(f"anthropic answered {resp.status}")
    return doc


def text_of(message: dict[str, Any]) -> str:
    return "".join(
        b.get("text", "") for b in message.get("content") or [] if b.get("type") == "text"
    )
