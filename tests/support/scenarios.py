"""The refund scenario the end-to-end tests share: a loopback Stripe with one refundable
charge, and a child that refunds it through `irimi shadow`."""

import sys
from dataclasses import replace
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
    """The Stripe on the other side of the base-URL refund, for `serve(FakeStripe, threaded=True,
    **base_url_stripe())`: any charge is a refundable one, and any other read is the charge list.
    `GET /v1/charges/{id}` is irimi's precondition read before it fakes the refund; the bare
    collection is the agent's own read. A POST reaching it at all is the failure: under shadow
    the refund is answered by irimi and this server must never hear about it."""
    return {
        "any_charge": lambda handler, charge_id: _base_url_charge(charge_id),
        "not_found": Answer(200, stripe_list(_base_url_charge(CHARGE_ID), has_more=None)),
    }


class StandInReader(reader.UpstreamReader):
    """`UpstreamReader` pointed at the loopback stand-in.

    In a real run through the door the precondition read dials `api.stripe.com`, which is correct
    and is the whole reason the forward-proxy criterion goes through the FORWARD proxy instead.
    A test may not make a network call, so the one thing the door forces -
    a real host on the write - is redirected here and nowhere else in the product.
    """

    port = 0

    def __call__(self, request):
        return super().__call__(
            replace(request, scheme="http", host="127.0.0.1", port=StandInReader.port)
        )


# A stub on loopback and `NO_PROXY`: `irimi shadow` and `irimi replay` give their child
# `NO_PROXY=localhost,127.0.0.1`, so a child that calls a loopback stub by address goes straight to
# it, irimi sees nothing, and every assertion that the stub heard nothing passes for the wrong
# reason. So the children here name the proxy explicitly, and the agents in `tests/e2e` either
# call real names only irimi's process resolves or run with no `NO_PROXY` at all; each proves its
# routing from the store, where every stub heard exactly what irimi stored as live.

# Placeholders rather than `str.format`: the source below is full of literal braces.
BASE_URL_CHILD = '''
"""Both doors, spelled the way the two SDKs in the fixture spell them.

The read goes through the forward proxy, which is what `requests`, `httpx` and `slack_sdk` do
with `HTTPS_PROXY` set. The refund goes through the reverse door, which is what `stripe-python`
does once `stripe.api_base` is the listener origin plus the upstream host - it ships its own CA
bundle and ignores the proxy variables, and `examples/refund_agent/agent.py` builds exactly this
URL in `door_base()`.
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

# The amount as a person reads it: the refund names no currency and irimi's own precondition read
# of `ch_REAL1` found `usd` on the charge, which the write's exchange carries.
REFUND_MONEY = format_minor_units(REFUND_AMOUNT, "usd")

# The refund agent's summary under its header line, as the criterion and its CLI twin both assert
# it: the overlaid reads hang under the refund they saw, and the engine's reads are counted but
# kept out of the agent's `N reads`.
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

# Placeholders rather than `str.format`, as in CHILD. The same five calls as the criterion above,
# spelled as a child process sends them through `HTTP_PROXY`: an absolute URL to the proxy.
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
    """The refund agent's five calls from a child of the real `irimi shadow`, against the loopback
    Stripe, which never hears a write. `flags` go before the `--`, and `calls` are more of the
    child's own `call(...)` lines after the five. Shared with `tests/e2e/test_trace_roundtrip.py`
    and `tests/cli/test_runs.py`, which read back what the run stored."""
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
