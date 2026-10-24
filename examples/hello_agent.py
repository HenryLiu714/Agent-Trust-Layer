"""One real read and two writes irimi fakes: `irimi shadow -- python3 examples/hello_agent.py`."""

import os
import urllib.request

GITHUB = os.environ.get("GITHUB_API_BASE", "https://api.github.com")
STRIPE = os.environ.get("STRIPE_API_BASE", "https://api.stripe.com")
SLACK = os.environ.get("SLACK_HOOKS_API_BASE", "https://hooks.slack.com")
STRIPE_KEY = os.environ.get("STRIPE_API_KEY", "sk_test_hello")
JSON = {"Content-Type": "application/json"}
if not STRIPE_KEY.startswith("sk_test_"):
    raise SystemExit("error: STRIPE_API_KEY must be a test-mode key (sk_test_...)")


def call(what: str, url: str, body: bytes | None = None, **headers: str) -> None:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, body, headers), timeout=30) as r:
            print(what, r.status, "answered by", r.headers.get("Irimi-Answered-By", "the service"))
    except urllib.request.HTTPError as exc:
        print(what, exc.code, "answered by", exc.headers.get("Irimi-Answered-By", "the service"))


call("read  ", f"{GITHUB}/zen")
call("refund", f"{STRIPE}/v1/refunds", b"charge=ch_hello", Authorization=f"Bearer {STRIPE_KEY}")
call("slack ", f"{SLACK}/services/T0/B0/hello", b'{"text": "refunded ch_hello"}', **JSON)
