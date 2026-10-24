"""The refund scenario the end-to-end tests share.

A loopback Stripe with one refundable charge, and a child that refunds it through `irimi shadow`.
"""

import functools
import importlib.util
import sys
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any

from irimi.cli import main
from irimi.core.money import format_minor_units
from irimi.decide import reader
from tests.support.maps import PRECONDITION_STRIPE_MAP, complete_for_check
from tests.support.upstreams import (
    Answer,
    FakeStripe,
    precondition_stripe,
    serve,
    stripe_list,
)

CHARGE_ID = "ch_3QTESTONLY000000"

DEMO_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "demo.py"


@functools.cache
def demo() -> ModuleType:
    """`scripts/demo.py`, loaded by its path, since `scripts/` is not a package.

    Its `run(example, workdir)` runs an example under `irimi shadow` against the fake internet."""
    spec = importlib.util.spec_from_file_location("irimi_demo", DEMO_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # its dataclasses look their module up by name
    spec.loader.exec_module(module)
    return module


REFUND_AMOUNT_MINOR = 100  # the base-URL child's own refund: a partial one of the charge's 4900


def _base_url_charge(charge_id: str) -> dict[str, object]:
    return {
        "id": charge_id,
        "object": "charge",
        "status": "succeeded",
        "paid": True,
        "refunded": False,
        "amount": 4900,
        "amount_refunded": 0,
        "currency": "usd",
    }


def base_url_stripe() -> dict[str, Any]:
    """The base-URL refund's Stripe, for `serve(FakeStripe, threaded=True, **base_url_stripe())`.

    Any charge is refundable (irimi's check read) and any other read is the charge list (the
    agent's). Under shadow the refund is faked, so no POST may reach it."""
    return {
        "any_charge": lambda handler, charge_id: _base_url_charge(charge_id),
        "not_found": Answer(200, stripe_list(_base_url_charge(CHARGE_ID), has_more=None)),
    }


class StandInReader(reader.UpstreamReader):
    """`UpstreamReader` pointed at the loopback fake Stripe.

    In base-URL mode the write names a real host, so irimi's check read dials `api.stripe.com`.
    A test may make no network call, so this reader sends that read to loopback instead.
    """

    port = 0

    def __call__(self, request):
        return super().__call__(
            replace(request, scheme="http", host="127.0.0.1", port=StandInReader.port)
        )


# The NO_PROXY trap: `irimi shadow` and `irimi replay` give their child
# `NO_PROXY=localhost,127.0.0.1`, so a child calling a loopback stub by address bypasses irimi and
# "the stub heard nothing" passes for the wrong reason. The children here name the proxy
# explicitly; the agents in `tests/e2e` call names only irimi's process resolves, or run with no
# `NO_PROXY`, and prove their routing from the store.

# Placeholders rather than `str.format`: the source below is full of literal braces.
BASE_URL_CHILD = '''
"""Both doors, spelled the way the two SDKs in the fixture spell them.

The read goes through the forward proxy, which is what `requests`, `httpx` and `slack_sdk` do
with `HTTPS_PROXY` set. The refund goes through the reverse door, which is what `stripe-python`
does once `stripe.api_base` is the listener origin plus the upstream host, the base-URL mode
`examples/quickstart/README.md` names.
"""
import http.client, os, urllib.parse

door = urllib.parse.urlparse(os.environ["HTTPS_PROXY"])


def call(method, target, host, body=None):
    conn = http.client.HTTPConnection(door.hostname, door.port, timeout=10)
    # `credentials.AGENT_STRIPE_KEY`, joined here: the child imports nothing from the tests.
    headers = {"host": host, "authorization": "Bearer sk_test_" + "notreal"}
    if body is not None:
        headers["content-type"] = "application/x-www-form-urlencoded"
    conn.request(method, target, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    stamp = resp.getheader("Irimi-Answered-By")
    conn.close()
    return resp.status, data, stamp


status, charges, stamp = call(
    "GET", "http://127.0.0.1:__CHARGES_PORT__/v1/charges", "127.0.0.1:__CHARGES_PORT__"
)
print("READ", status, stamp, charges.decode())

status, refund, stamp = call(
    "POST",
    "/api.stripe.com/v1/refunds",
    "127.0.0.1:%d" % door.port,
    body="charge=__CHARGE__&amount=__AMOUNT__",
)
print("WRITE", status, stamp, refund.decode())
'''

# The loopback Stripe's one refundable charge: 4900, nothing refunded (`precondition_stripe`).
REFUND_CHARGE = "ch_REAL1"

REFUND_AMOUNT = 4900

# The refund names no currency; irimi's check read of `ch_REAL1` finds `usd` on the charge.
REFUND_MONEY = format_minor_units(REFUND_AMOUNT, "usd")

# The refund agent's summary below its header. The overlaid reads hang under the refund they saw,
# and irimi's check reads are counted apart from the agent's `N reads`.
REFUND_SUMMARY = [
    "",
    "  None of these writes reached the real service:",
    f"  ○ stripe  refund {REFUND_MONEY} on {REFUND_CHARGE}  faked; checked against live state: "
    "would succeed",
    "    ↳ GET /v1/refunds saw it  overlay",
    f"    ↳ GET /v1/charges/{REFUND_CHARGE} saw it  overlay",
    f"  ✗ stripe  refund {REFUND_MONEY} on {REFUND_CHARGE}  faked; would fail: "
    "charge_already_refunded",
    f"  money: {format_minor_units(2 * REFUND_AMOUNT, 'usd')} in faked writes; "
    f"{REFUND_MONEY} of it would fail",
    "  would have fired: refund.created, charge.refunded",
    "",
    "  127.0.0.1  3 reads (2 showing this run's writes)  2 irimi check reads  2 faked writes",
    "",
    "  7 exchanges · 5 real · 2 answered by irimi (2 faked writes) · 0 sent to targets",
]

# The refund agent's five calls, sent through `HTTP_PROXY` as absolute URLs. Placeholders rather
# than `str.format`, as in BASE_URL_CHILD.
REFUND_CHILD = """
import http.client, os, urllib.parse, uuid

proxy = urllib.parse.urlparse(os.environ["HTTP_PROXY"])
base = "http://127.0.0.1:__STUB_PORT__"


def call(method, path, body=None):
    conn = http.client.HTTPConnection(proxy.hostname, proxy.port, timeout=10)
    headers = {"host": "127.0.0.1:__STUB_PORT__"}
    if body is not None:
        headers["content-type"] = "application/x-www-form-urlencoded"
        headers["idempotency-key"] = str(uuid.uuid4())
    conn.request(method, base + path, body=body, headers=headers)
    resp = conn.getresponse()
    resp.read()
    print("CALL", method, path, resp.status, resp.getheader("Irimi-Answered-By"))
    conn.close()


refund = "charge=__CHARGE__&amount=__AMOUNT__"
call("GET", "/v1/charges")
call("POST", "/v1/refunds", refund)
call("GET", "/v1/refunds?charge=__CHARGE__&limit=10")
call("GET", "/v1/charges/__CHARGE__")
call("POST", "/v1/refunds", refund)
"""


def run_refund_scenario(
    tmp_path, monkeypatch, *flags: str, stripe_map: str = PRECONDITION_STRIPE_MAP, calls: str = ""
) -> None:
    """Run the refund agent's five calls under the real `irimi shadow`; Stripe hears no write.

    `flags` go before the `--`, and `calls` are more `call(...)` lines for the child after the five.
    """
    stripe = serve(FakeStripe, threaded=True, **precondition_stripe())
    maps_dir = tmp_path / "shipped"
    maps_dir.mkdir()
    (maps_dir / "stripe.yaml").write_text(stripe_map)
    complete_for_check(maps_dir)
    monkeypatch.setattr("irimi.servicemaps.loader.shipped_dir", lambda: maps_dir)
    child = tmp_path / "child.py"
    child.write_text(
        (REFUND_CHILD + calls)
        .replace("__STUB_PORT__", str(stripe))
        .replace("__CHARGE__", REFUND_CHARGE)
        .replace("__AMOUNT__", str(REFUND_AMOUNT))
    )
    try:
        assert main(["shadow", "--port", "0", *flags, "--", sys.executable, str(child)]) == 0
    finally:
        stripe.stop()
    assert [m for m, _ in stripe.seen if m != "GET"] == []
