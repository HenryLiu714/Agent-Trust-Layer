"""A server-style refund agent: the fixture for Phase 3's exit test (#78).

A long-lived HTTP server that irimi does not launch, as a production agent is. Each `POST /refund`
is one run, `handle_refund`, marked with `@sdk.trigger`, which:

1. reads the charge from Stripe (`GET /v1/charges/{charge}`), a live read under irimi;
2. marks it refunded in the agent's own SQLite (`mark_refunded`), a write the proxy cannot see,
   labelled `@sdk.tool(kind="write")` so that under irimi only its stand-in runs;
3. refunds it (`POST /v1/refunds`, form body `charge=...`), a write irimi fakes;
4. answers `200 {"refund": "<id>"}`.

`GET /healthz` answers `200`. Two requests at once are two runs, each on its own thread
(`ThreadingHTTPServer`), and neither sees the other's calls.

Stripe is called with stdlib `urllib.request`, not stripe-python: urllib honours `HTTPS_PROXY` and
`SSL_CERT_FILE` with no code, so the agent goes through irimi's forward proxy and trusts its CA by
the environment alone. stripe-python verifies against the CA bundle it ships, never irimi's, and
the reverse door it would need instead is loopback-only, so another container cannot reach it.

    PORT=8080 STRIPE_API_KEY=sk_test_... python -m examples.server_agent

Start it by module name, as above, never as `python app.py`: run as `__main__`, its trigger's
entrypoint would be `__main__:handle_refund`, which replay (#84) cannot import, so it refuses to.

Environment: `PORT` (default 8080; 0 picks a free one), `HOST` (default 127.0.0.1; a container
needs 0.0.0.0), `STRIPE_API_KEY`, `STRIPE_API_BASE` (default https://api.stripe.com; the exit test
points it at a loopback stub), `REFUND_DB` (the SQLite file `mark_refunded` writes, default
`refunds.sqlite3`) and `IRIMI_EXAMPLE_BARRIER`, for the exit test only: `2` holds each request
after its charge read until two are there, so the two runs are open at once.
"""

import contextlib
import json
import os
import sqlite3
import threading
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from irimi import sdk

BARRIER_ENV = "IRIMI_EXAMPLE_BARRIER"
# The write tool's name in irimi's trace and summary (`○ tool db.mark_refunded ...`).
TOOL_NAME = "db.mark_refunded"
# How long a request held at the barrier waits for the other: a test that sent one only fails.
BARRIER_TIMEOUT_S = 30.0
_barrier = (
    threading.Barrier(int(os.environ[BARRIER_ENV]), timeout=BARRIER_TIMEOUT_S)
    if os.environ.get(BARRIER_ENV)
    else None
)


def stripe(method: str, path: str, form: dict[str, str] | None = None) -> dict[str, Any]:
    """One Stripe API call, its JSON answer. An error status raises `urllib.error.HTTPError`."""
    base = os.environ.get("STRIPE_API_BASE", "https://api.stripe.com").rstrip("/")
    data = None if form is None else urllib.parse.urlencode(form).encode()
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {os.environ.get('STRIPE_API_KEY', '')}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def _mark_refunded_stand_in(charge: str) -> dict[str, Any]:
    return {"stood_in": True}


@sdk.tool(kind="write", shadow=_mark_refunded_stand_in, name=TOOL_NAME)
def mark_refunded(charge: str) -> dict[str, Any]:
    """The agent's own record of the refund. Under irimi it never runs: the file is not created."""
    path = os.environ.get("REFUND_DB", "refunds.sqlite3")
    with contextlib.closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("CREATE TABLE IF NOT EXISTS refunded (charge TEXT PRIMARY KEY)")
        conn.execute("INSERT OR REPLACE INTO refunded (charge) VALUES (?)", (charge,))
    return {"charge": charge}


@sdk.trigger
def handle_refund(charge: str, reason: str) -> str:
    """Refund `charge` in full, and return the refund's id. `reason` is the caller's, kept as the
    run's trigger argument."""
    stripe("GET", f"/v1/charges/{urllib.parse.quote(charge, safe='')}")
    if _barrier is not None:
        _barrier.wait()
    mark_refunded(charge)
    refund = stripe("POST", "/v1/refunds", {"charge": charge})
    return str(refund["id"])


class _Handler(BaseHTTPRequestHandler):
    # A client that names more body than it sends is cut off, not waited on forever (#78).
    timeout = 30.0

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._reply(200, {"ok": True})
        else:
            self._reply(404, {"error": f"no route {self.path}"})

    def do_POST(self) -> None:
        if self.path != "/refund":
            self._reply(404, {"error": f"no route {self.path}"})
            return
        try:
            # A negative length would read to the end of a connection the client holds open (#78).
            length = int(self.headers.get("content-length") or 0)
            if length < 0:
                raise ValueError(f"content-length {length}")
            body = json.loads(self.rfile.read(length))
            charge, reason = body["charge"], body["reason"]
            if not (isinstance(charge, str) and charge and isinstance(reason, str)):
                raise TypeError("charge must be a non-empty string, and reason a string")
        except (ValueError, KeyError, TypeError) as exc:
            self._reply(400, {"error": f"expected {{'charge': ..., 'reason': ...}}: {exc}"})
            return
        try:
            refund = handle_refund(charge, reason)
        except Exception as exc:  # the run is stored in error; the caller is told why
            self._reply(502, {"error": f"{type(exc).__name__}: {exc}"})
            return
        self._reply(200, {"refund": refund})

    def _reply(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


def main() -> int:
    host = os.environ.get("HOST", "127.0.0.1")
    server = ThreadingHTTPServer((host, int(os.environ.get("PORT", "8080"))), _Handler)
    print(f"listening on {host}:{server.server_address[1]}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    # Run as a script, every run's entrypoint would be `__main__:handle_refund`, which replay (#84)
    # cannot import: refuse, and say how to start it (#74).
    raise SystemExit("start the server agent by module name: python -m examples.server_agent")
