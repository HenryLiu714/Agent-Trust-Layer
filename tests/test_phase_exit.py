"""The Phase 1 exit test (#13): the whole tool, end to end, on the write it was built for.

Two tests, and the difference between them is the whole argument:

* `test_a_refund_through_the_door_is_answered_by_irimi_and_never_leaves_the_machine` is the
  **exit criterion**. It runs `irimi shadow` over a child that speaks the exact wire shape
  `stripe-python` produces through the reverse door - `stripe.api_base` is an origin plus the
  upstream host, and `Refund.create` posts a form body - and it needs no key, no network and no
  SDK, so it runs on every `uv run pytest -q`. A criterion that can skip is a criterion Phase 1
  can close without ever having run.

* `test_the_refund_agent_leaves_no_refund_on_a_real_test_mode_charge` is the live version the
  issue describes. It runs `examples/refund_agent/agent.py` itself against Stripe test mode and
  re-reads the charge afterwards. It skips without `STRIPE_API_KEY` and the `stripe` SDK, which
  is exactly why it is not the criterion - but it is the one that proves the claim against the
  real ledger, so it stays, and the orchestrator is expected to run it once by hand.

What the hermetic test gives up: it proves the refund never left this machine, not that Stripe's
ledger is unchanged. Those differ only if irimi both answered the write locally and forwarded it,
which is what the local server here would catch - it is the only thing on the other side, and it
records everything it is given.
"""

import json
import os
import re
import sys
import threading
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from irimi import ca, paths, policy
from irimi.cli import main

AGENT_PATH = Path(__file__).resolve().parent.parent / "examples" / "refund_agent" / "agent.py"

CHARGE_ID = "ch_3QTESTONLY000000"
REFUND_AMOUNT_MINOR = 100  # `examples/refund_agent/agent.py`'s own REFUND_AMOUNT_MINOR


class _Stripe(BaseHTTPRequestHandler):
    """Everything on the other side of the proxy. It answers reads and records every request.

    A POST reaching it at all is the failure the exit test exists to detect: under shadow the
    refund is answered by irimi and this server must never hear about it.
    """

    seen: list[tuple[str, str, bytes]] = []

    def _record(self) -> bytes:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        _Stripe.seen.append((self.command, self.path, body))
        return body

    def do_GET(self):
        self._record()
        one = re.fullmatch(r"/v1/charges/([A-Za-z0-9_]+)", self.path.partition("?")[0])
        charge_id = one.group(1) if one else CHARGE_ID
        charge = {
            "id": charge_id,
            "object": "charge",
            "status": "succeeded",
            "paid": True,
            "refunded": False,
            "amount": 4900,
            "amount_refunded": 0,
            "currency": "usd",
        }
        # `GET /v1/charges/{id}` is the L3 precondition read irimi makes before faking the refund
        # (#45); the bare collection is the agent's own read.
        document = charge if one else {"object": "list", "data": [charge]}
        payload = json.dumps(document).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        self._record()
        self.send_response(500)
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


class _StandInReader(policy.UpstreamReader):
    """`UpstreamReader` pointed at the loopback stand-in (#45).

    In a real run through the door the precondition read dials `api.stripe.com`, which is correct
    and is the whole reason Phase 2's exit test goes through the FORWARD proxy instead (Notion,
    Phase 2 § Exit test). A test may not make a network call, so the one thing the door forces -
    a real host on the write - is redirected here and nowhere else in the product.
    """

    port = 0

    def __call__(self, request):
        return super().__call__(
            replace(request, scheme="http", host="127.0.0.1", port=_StandInReader.port)
        )


@pytest.fixture
def stripe_stand_in():
    _Stripe.seen = []
    # Threaded: irimi's own precondition read runs on a worker thread as of #45, and a
    # single-threaded stand-in would serialize it against the agent's traffic.
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Stripe)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    yield srv.server_address[1]
    srv.shutdown()


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    ca.generate_ca(ca.ca_paths())
    return tmp_path


# Placeholders rather than `str.format`: the source below is full of literal braces.
CHILD = '''
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
    headers = {"host": host, "authorization": "Bearer sk_test_notreal"}
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


def test_a_refund_through_the_door_is_answered_by_irimi_and_never_leaves_the_machine(
    home, tmp_path, stripe_stand_in, capfd, monkeypatch
):
    """The Phase 1 exit criterion. One run of the real CLI: a live read that reaches the world,
    a refund that does not, an echo the caller can parse, and a summary that says so."""
    # `cli._build_engine` imports the reader inside the function, so this is what it builds.
    monkeypatch.setattr("irimi.policy.UpstreamReader", _StandInReader)
    _StandInReader.port = stripe_stand_in
    child = tmp_path / "child.py"
    child.write_text(
        CHILD.replace("__CHARGES_PORT__", str(stripe_stand_in))
        .replace("__CHARGE__", CHARGE_ID)
        .replace("__AMOUNT__", str(REFUND_AMOUNT_MINOR))
    )
    argv = ["shadow", "--port", "0", "--", sys.executable, str(child)]
    assert main(argv) == 0
    out = capfd.readouterr().out

    # 1. The read was real: it reached the only server there is, came back from it, and carries
    #    no `Irimi-Answered-By` - absence is how a live forward says it was not ours (#12). The
    #    second GET is irimi's own precondition read of the charge, made before it faked the
    #    refund (#45) - and it reaching this stand-in is the proof it did not reach Stripe.
    assert [(m, p) for m, p, _ in _Stripe.seen] == [
        ("GET", "/v1/charges"),
        ("GET", f"/v1/charges/{CHARGE_ID}"),
    ]
    read = next(line for line in out.splitlines() if line.startswith("READ "))
    assert read.split(" ", 3)[:3] == ["READ", "200", "None"]
    assert CHARGE_ID in read

    # 2. The write was answered by irimi, and says so on the wire (#12).
    write = next(line for line in out.splitlines() if line.startswith("WRITE "))
    _, status, stamp, payload = write.split(" ", 3)
    assert status == "200"
    assert stamp == "fake-L1"

    # 3. The answer is something an SDK can read: the id it minted, the object, and the fields
    #    the caller sent back unchanged. `stripe.Refund.create(...).id` is this assertion in the
    #    SDK. Since #41 it is a whole refund object, so the fields the caller never sent - the
    #    ones an agent branches on - are there too.
    body = json.loads(payload)
    assert body["object"] == "refund"
    assert body["id"].startswith("re_")
    assert body["balance_transaction"].startswith("txn_")
    assert body["charge"] == CHARGE_ID
    assert body["amount"] == REFUND_AMOUNT_MINOR
    assert body["status"] == "succeeded"
    assert body["currency"] == "usd"
    assert "livemode" not in body  # the real refund object has no such field

    # 4. And it never left: nothing on the other side was ever asked to do anything.
    assert [(m, p) for m, p, _ in _Stripe.seen if m != "GET"] == []

    # 5. The summary says all of it, in the map's own words.
    assert f"  ○ refund {REFUND_AMOUNT_MINOR} on {CHARGE_ID}  unvalidated (L1)" in out
    assert "  These writes did not happen." in out
    assert "  api.stripe.com  1 write intercepted" in out
    assert "1 read" in out


@pytest.mark.skipif(
    not os.environ.get("STRIPE_API_KEY"),
    reason="live phase-exit check: set STRIPE_API_KEY to a sk_test_ key and seed a charge",
)
def test_the_refund_agent_leaves_no_refund_on_a_real_test_mode_charge(home, capfd):
    """The live half of the exit criterion, run by hand. The refund agent itself, against Stripe
    test mode: the POST is answered by the engine, the agent gets a real-shaped `re_` id, and a
    live re-read of the charge afterwards shows the refund never happened.

        uv sync --group examples && uv run pytest -q tests/test_phase_exit.py
    """
    stripe = pytest.importorskip("stripe", reason="live phase-exit check needs the stripe SDK")
    stripe.api_key = os.environ["STRIPE_API_KEY"]
    assert stripe.api_key.startswith("sk_test_"), "test mode only"

    code = main(["shadow", "--port", "0", "--", sys.executable, str(AGENT_PATH)])
    out = capfd.readouterr().out
    if code == 3:
        pytest.skip("no refundable charge: run `python examples/refund_agent/seed.py` first")
    assert code == 0, out

    result = re.search(r"AGENT-RESULT charge=(\S+) refund=(\S+) amount=(\d+)", out)
    assert result is not None, out
    charge_id, refund_id, amount = result.group(1), result.group(2), int(result.group(3))
    assert refund_id.startswith("re_"), "the agent did not get a parseable refund id"

    charge = stripe.Charge.retrieve(charge_id)
    assert charge.amount_refunded == 0, "a refund reached Stripe"
    assert charge.refunded is False
    assert list(stripe.Refund.list(charge=charge_id).data) == []
    assert f"  ○ refund {amount} on {charge_id}  unvalidated (L1)" in out
    assert "  These writes did not happen." in out
