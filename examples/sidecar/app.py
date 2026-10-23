"""A refund agent as a long-lived HTTP server, beside `irimi serve` rather than launched by it.

Each `POST /refund` with `{"charge": "ch_...", "reason": "..."}` is one run, `handle_refund`,
marked `@sdk.trigger`, which:

1. reads the charge from Stripe, a live read under irimi;
2. refunds it, a write irimi fakes;
3. records the refund in the agent's own SQLite through `db.mark_refunded`, an `@sdk.write_tool`,
   once Stripe has accepted it; under irimi only its fake runs;
4. answers `200 {"refund": "re_..."}`.

`GET /healthz` answers `200`. Two requests at once are two runs, each on its own thread, and
neither sees the other's calls.

Stripe is called with stdlib `urllib.request`, which takes `HTTPS_PROXY` and `SSL_CERT_FILE` from
the environment, so the app goes through irimi with no code of its own.

    PORT=8080 STRIPE_API_KEY=sk_test_... python -m examples.sidecar.app

Environment: `PORT` (default 8080; 0 picks a free one), `HOST` (default 127.0.0.1; a container
needs 0.0.0.0), `STRIPE_API_KEY` (`sk_test_...` only), `STRIPE_API_BASE` (default
https://api.stripe.com) and `REFUND_DB` (default `refunds.sqlite3`).
"""

import contextlib
import json
import os
import sqlite3
import sys
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from irimi import sdk

TEST_KEY_PREFIX = "sk_test_"


def stripe(method: str, path: str, form: dict[str, str] | None = None) -> dict[str, Any]:
    """One Stripe API call, its JSON answer. Refuses, before sending anything, a key that is not
    test mode. An error status raises `urllib.error.HTTPError`."""
    key = os.environ.get("STRIPE_API_KEY", "")
    if not key.startswith(TEST_KEY_PREFIX):
        raise ValueError(f"STRIPE_API_KEY must be a test-mode key ({TEST_KEY_PREFIX}...)")
    base = os.environ.get("STRIPE_API_BASE", "https://api.stripe.com").rstrip("/")
    data = None if form is None else urllib.parse.urlencode(form).encode()
    headers = {"Authorization": f"Bearer {key}"}
    request = urllib.request.Request(base + path, data=data, method=method, headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        answer: dict[str, Any] = json.load(response)
        return answer


def mark_refunded_fake(charge: str, refund: str) -> dict[str, Any]:
    return {"recorded": False}


@sdk.write_tool(fake=mark_refunded_fake, name="db.mark_refunded")
def mark_refunded(charge: str, refund: str) -> dict[str, Any]:
    """The agent's own record of the refund. Under irimi the fake runs instead."""
    path = os.environ.get("REFUND_DB", "refunds.sqlite3")
    with contextlib.closing(sqlite3.connect(path)) as conn, conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS refunded (charge TEXT PRIMARY KEY, refund TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT OR REPLACE INTO refunded (charge, refund) VALUES (?, ?)", (charge, refund)
        )
    return {"recorded": True}


@sdk.trigger
def handle_refund(charge: str, reason: str) -> str:
    """Refund `charge` in full, record it, and return the refund's id. `reason` is the caller's,
    kept as the run's trigger argument."""
    stripe("GET", f"/v1/charges/{urllib.parse.quote(charge, safe='')}")
    refund = str(stripe("POST", "/v1/refunds", {"charge": charge})["id"])
    mark_refunded(charge, refund)
    return refund


class _Handler(BaseHTTPRequestHandler):
    # A client that names more body than it sends is cut off, not waited on forever.
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
            # A negative length would read to the end of a connection the client holds open.
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
    sys.exit(main())
