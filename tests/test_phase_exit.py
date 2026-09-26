"""The phase exit tests: the whole tool, end to end, on the write it was built for.

Two phases, a hermetic criterion for each, and one live check that has grown with them. The
difference between a criterion and the live check is the whole argument:

* `test_a_refund_through_the_door_is_answered_by_irimi_and_never_leaves_the_machine` is the
  **Phase 1 exit criterion** (#13). It runs `irimi shadow` over a child that speaks the exact wire
  shape `stripe-python` produces through the reverse door - `stripe.api_base` is an origin plus the
  upstream host, and `Refund.create` posts a form body - and it needs no key, no network and no
  SDK, so it runs on every `uv run pytest -q`. A criterion that can skip is a criterion a phase
  can close without ever having run. It is left as Phase 1 closed it, and it keeps the reverse
  door covered.

* `test_the_refund_agents_four_calls_through_the_forward_proxy_are_answered_shadow_mode_correctly`
  is the **Phase 2 exit criterion** (#48). It drives the refund agent's first read and then its
  four calls - refund, list refunds, re-read, retry - through the FORWARD proxy against a loopback
  Stripe, and asserts the answers, the exchanges and the summary block line by line. It does not
  go through the reverse door on purpose: the door rewrites the host to `api.stripe.com`, so
  irimi's own L3 precondition read would dial the real Stripe (Notion, Phase 2 § Exit test). The
  Phase 1 test answers that by swapping in `_StandInReader`; this one needs no stand-in at all.
  `test_the_same_five_calls_under_irimi_shadow_print_the_phase_2_summary` is its twin through the
  real CLI: the same calls from a child of `irimi shadow`, proving the engine the criterion builds
  is the one `cli._build_engine` builds, and that the block it prints on exit is the one pinned.

* `test_the_refund_agent_leaves_no_refund_on_a_real_test_mode_charge` is the live version both
  phases describe. It runs `examples/refund_agent/agent.py` itself against Stripe test mode and
  re-reads the charge afterwards. It skips without `STRIPE_API_KEY` and the `stripe` SDK, which
  is exactly why it is not a criterion - but it is the one that proves the claim against the
  real ledger, so it stays, and it is run once by hand before a phase closes.

What the hermetic tests give up: they prove the refund never left this machine, not that Stripe's
ledger is unchanged. Those differ only if irimi both answered the write locally and forwarded it,
which is what the local server here would catch - it is the only thing on the other side, and it
records everything it is given.
"""

import json
import os
import re
import sys
import threading
import uuid
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from irimi import ca, paths, pipeline, policy, report
from irimi.cli import main
from irimi.overlay import ServiceOverlay
from tests.test_engine_mitm import (
    PRECONDITION_STRIPE_MAP,
    _config,
    _maps,
    _PreconditionStub,
    _read_via_proxy,
    _start,
)

AGENT_PATH = Path(__file__).resolve().parent.parent / "examples" / "refund_agent" / "agent.py"

CHARGE_ID = "ch_3QTESTONLY000000"
REFUND_AMOUNT_MINOR = 100  # the Phase 1 child's own refund: a partial one of `_Stripe`'s 4900


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
    # `$1.00`, not `100`: the child posts no currency, and `_Stripe`'s charge is in `usd`, which
    # irimi's own precondition read of it carried onto the write's exchange (#60).
    assert (
        f"  ○ refund {report.money(REFUND_AMOUNT_MINOR, 'usd')} on {CHARGE_ID}  "
        "unvalidated (L3 preconditions passed)" in out
    )
    assert "  These writes did not happen." in out
    assert "1 read" in out

    # 6. And irimi checked with Stripe before answering (#45). The charge GET in (1) is a read
    #    irimi issued on its own account, and it is recorded `issued_by: engine` so that it can be
    #    counted apart from the agent's: "reads are real" means the reads the AGENT made. It did
    #    reach the service, so the run's `live` bucket holds it beside the agent's own read. The
    #    agent's read went to the stand-in's own address, so Stripe's line holds no read of the
    #    agent's at all - before #45 it said `1 read` here, and that read was irimi's.
    host = next(line for line in out.splitlines() if line.startswith("  api.stripe.com  "))
    assert host == "  api.stripe.com  1 engine read  1 write intercepted"
    assert "  3 exchanges · 2 live · 0 delegated · 1 virtualized" in out


# The loopback Stripe's one refundable charge: 4900, nothing refunded (`tests.test_engine_mitm`).
PHASE2_CHARGE = "ch_REAL1"
PHASE2_AMOUNT = 4900
# The amount as a person reads it, since #60: the refund names no currency and irimi's own
# precondition read of `ch_REAL1` found `usd` on the charge. Notion's target block asked for
# `$49.00` from the start; #48 printed `4900` because the currency had nowhere to travel.
PHASE2_MONEY = report.money(PHASE2_AMOUNT, "usd")
# The Phase 2 summary under its header line, as the criterion and its CLI twin both assert it: the
# overlaid reads hang under the refund they saw, and the engine's reads are counted but kept out
# of the agent's `N reads` (#45, #48). Notion's target block, character for character.
PHASE2_SUMMARY = [
    "",
    "  127.0.0.1  3 reads (2 showing this run's writes)  2 engine reads  2 writes intercepted",
    "",
    f"  ○ refund {PHASE2_MONEY} on {PHASE2_CHARGE}  unvalidated (L3 preconditions passed)",
    "    ↳ GET /v1/refunds saw it  overlay",
    f"    ↳ GET /v1/charges/{PHASE2_CHARGE} saw it  overlay",
    f"  ✗ refund {PHASE2_MONEY} on {PHASE2_CHARGE}  would fail: charge_already_refunded",
    "",
    "  7 exchanges · 5 live · 0 delegated · 2 virtualized",
    "  These writes did not happen. Would have fired: refund.created, charge.refunded.",
]


@pytest.fixture
def phase2_stub(tmp_path, monkeypatch):
    """The shared loopback Stripe, and an engine over it built the way `cli._build_engine` builds
    one: the real overlay, and a policy holding the real reader. Nothing is swapped for a
    stand-in - through the forward proxy the precondition read goes where the agent's reads go.
    Yields (proxy port, stub port, exchanges, maps)."""
    _PreconditionStub.seen = []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _PreconditionStub)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    maps = _maps(tmp_path, monkeypatch, doc=PRECONDITION_STRIPE_MAP)
    eng, seen, stop = _start(
        _config(tmp_path, monkeypatch, maps=maps),
        overlay=ServiceOverlay(maps),
        policy=policy.ShadowPolicy(reader=policy.UpstreamReader(), maps=maps),
    )
    yield eng.listen_port(), srv.server_address[1], seen, maps
    stop()
    srv.shutdown()


def _stripe_refund(proxy, stub):
    """`stripe.Refund.create(charge=, amount=)` on the wire: a form body, and a fresh
    `Idempotency-Key` on every call, because stripe-python defaults one per POST (Notion, Phase 2
    § Architecture changes). A retry is therefore a new request to the idempotency store, and it
    is L3 that must refuse it - a shared key would have the store replay the first answer (#46)."""
    return _read_via_proxy(
        proxy,
        f"http://127.0.0.1:{stub}/v1/refunds",
        extra_headers={
            "content-type": "application/x-www-form-urlencoded",
            "idempotency-key": str(uuid.uuid4()),
        },
        method="POST",
        body=f"charge={PHASE2_CHARGE}&amount={PHASE2_AMOUNT}".encode(),
    )


def test_the_refund_agents_four_calls_through_the_forward_proxy_are_answered_shadow_mode_correctly(
    phase2_stub,
):
    """The Phase 2 exit criterion (#48). The refund agent's first read and then its four calls -
    refund the charge in full, list its refunds, re-read the charge, retry the refund - through the
    forward proxy, with no key, no network and no SDK.

    The calls are made from this process with `_read_via_proxy`, not from a child under `irimi
    shadow`: the claim is about what the proxy answers each call with, `irimi shadow`'s spawning of
    a child is already the Phase 1 test's subject, and a second subprocess would buy nothing but a
    layer of stdout to parse. The wire shape is stripe-python's all the same.

    The refund is for the charge's FULL amount, as the agent's is. A partial refund leaves the
    charge refundable, and the retry would be accepted rather than refused (#45).
    """
    proxy, stub, seen, maps = phase2_stub
    base = f"http://127.0.0.1:{stub}"

    # 1. The agent's first read is real: it reached the stub and carries no stamp (#12).
    status, headers, _ = _read_via_proxy(proxy, f"{base}/v1/charges")
    assert status == 200
    assert pipeline.ANSWERED_BY_HEADER not in headers

    # 2. The refund is irimi's, and a whole refund object an SDK can parse (#41).
    status, headers, data = _stripe_refund(proxy, stub)
    assert status == 200
    assert headers[pipeline.ANSWERED_BY_HEADER] == "fake-L1"
    refund = json.loads(data)
    assert refund["object"] == "refund"
    assert refund["id"].startswith("re_")
    assert refund["balance_transaction"].startswith("txn_")
    assert (refund["charge"], refund["amount"]) == (PHASE2_CHARGE, PHASE2_AMOUNT)

    # 3. The agent's refunds list, filtered as an agent filters it, is shown the refund in full:
    #    `charge` and `limit` are both parameters the overlay knows how to apply (#43).
    status, headers, data = _read_via_proxy(
        proxy, f"{base}/v1/refunds?charge={PHASE2_CHARGE}&limit=10"
    )
    assert status == 200
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"
    assert [r["id"] for r in json.loads(data)["data"]] == [refund["id"]]

    # 4. The charge re-read is shown refunded, though the real charge is untouched (#43).
    status, headers, data = _read_via_proxy(proxy, f"{base}/v1/charges/{PHASE2_CHARGE}")
    assert status == 200
    assert headers[pipeline.ANSWERED_BY_HEADER] == "overlay"
    charge = json.loads(data)
    assert charge["amount_refunded"] == PHASE2_AMOUNT
    assert charge["refunded"] is True

    # 5. The retry is refused in Stripe's own words, by L3 and not by the store: it carries its own
    #    key, and what makes it refusable is the first refund, which only irimi's write log knows.
    status, headers, data = _stripe_refund(proxy, stub)
    assert status == 400
    assert headers[pipeline.ANSWERED_BY_HEADER] == "fake-L1"
    assert json.loads(data)["error"]["code"] == "charge_already_refunded"

    # Each refund was checked with the real service first, by a read irimi issued on its own
    # account and counted apart from the agent's (#45).
    issued = [ex for ex in seen if ex.issued_by == "engine"]
    assert [(ex.kind, ex.request.method, ex.request.path) for ex in issued] == [
        ("read", "GET", f"/v1/charges/{PHASE2_CHARGE}"),
    ] * 2
    writes = [ex for ex in seen if ex.kind == "write"]
    assert [ex.precondition for ex in writes] == ["passed", "rejected"]
    assert writes[1].rejection_code == "charge_already_refunded"
    # On the exchange, not only in the summary line: the refused refund would have fired nothing,
    # and the closing clause must be built from exactly this (#47).
    assert [ex.would_fire for ex in writes] == [("refund.created", "charge.refunded"), ()]

    # Nothing on the other side was ever asked to do anything.
    assert [m for m, _ in _PreconditionStub.seen if m != "GET"] == []

    # The summary, line by line.
    run_id = seen[0].run_id
    assert report.summary_lines(run_id, seen, 1.0, maps) == [
        f"irimi shadow · run {run_id} · 7 exchanges · 1.0s · backstop: none (Phase 4)",
        *PHASE2_SUMMARY,
    ]


# Placeholders rather than `str.format`, as in CHILD. The same five calls as the criterion above,
# spelled as a child process sends them through `HTTP_PROXY`: an absolute URL to the proxy.
PHASE2_CHILD = """
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


def test_the_same_five_calls_under_irimi_shadow_print_the_phase_2_summary(
    home, tmp_path, capfd, monkeypatch
):
    """The criterion's twin through the real CLI (#48). The criterion builds its engine the way
    `cli._build_engine` builds one; this proves that is the engine `irimi shadow` really builds -
    the real overlay, a policy holding the real reader - and that the block it prints on exit is
    the one the criterion pins. Nothing is swapped but where the shipped maps are read from: a
    directory holding the loopback Stripe's map, so the stub's host is the `stripe` service. The
    child uses the forward proxy explicitly, because `irimi shadow` exempts loopback from it
    through `NO_PROXY` and the stub is on loopback."""
    _PreconditionStub.seen = []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _PreconditionStub)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    maps_dir = tmp_path / "shipped"
    maps_dir.mkdir()
    (maps_dir / "stripe.yaml").write_text(PRECONDITION_STRIPE_MAP)
    monkeypatch.setattr("irimi.servicemap.loader.shipped_dir", lambda: maps_dir)
    child = tmp_path / "child.py"
    child.write_text(
        PHASE2_CHILD.replace("__STUB_PORT__", str(srv.server_address[1]))
        .replace("__CHARGE__", PHASE2_CHARGE)
        .replace("__AMOUNT__", str(PHASE2_AMOUNT))
    )
    try:
        assert main(["shadow", "--port", "0", "--", sys.executable, str(child)]) == 0
    finally:
        srv.shutdown()
    lines = capfd.readouterr().out.splitlines()

    assert [line for line in lines if line.startswith("CALL ")] == [
        "CALL GET /v1/charges 200 None",
        "CALL POST /v1/refunds 200 fake-L1",
        f"CALL GET /v1/refunds?charge={PHASE2_CHARGE}&limit=10 200 overlay",
        f"CALL GET /v1/charges/{PHASE2_CHARGE} 200 overlay",
        "CALL POST /v1/refunds 400 fake-L1",
    ]
    assert [m for m, _ in _PreconditionStub.seen if m != "GET"] == []
    # The banner opens `irimi shadow · run ...` too; the summary's header is the one with a count.
    header = next(i for i, line in enumerate(lines) if " · 7 exchanges · " in line)
    assert lines[header].startswith("irimi shadow · run ")
    assert lines[header + 1 : header + 1 + len(PHASE2_SUMMARY)] == PHASE2_SUMMARY


@pytest.mark.skipif(
    not os.environ.get("STRIPE_API_KEY"),
    reason="live phase-exit check: set STRIPE_API_KEY to a sk_test_ key and seed a charge",
)
def test_the_refund_agent_leaves_no_refund_on_a_real_test_mode_charge(home, capfd):
    """The live half of both exit criteria, run by hand once before a phase closes
    (CONTRIBUTING.md). The refund agent itself, through the reverse door, against Stripe test mode:
    it refunds a charge in full, finds its refund in the list, re-reads the charge refunded,
    retries and is refused - every answer irimi's - and a live re-read of the charge afterwards
    shows none of it happened.

        uv sync --group examples && uv run python examples/refund_agent/seed.py
        STRIPE_API_KEY=sk_test_... uv run pytest -q -rs tests/test_phase_exit.py

    With SLACK_BOT_TOKEN and SLACK_CHANNEL set the agent also posts and reads the channel back,
    and the post must be in what it read. SLACK_CHANNEL has to be a channel id for that: a post
    to `#general` is read back from a channel irimi cannot match to it (#44).
    """
    stripe = pytest.importorskip("stripe", reason="live phase-exit check needs the stripe SDK")
    stripe.api_key = os.environ["STRIPE_API_KEY"]
    assert stripe.api_key.startswith("sk_test_"), "test mode only"

    code = main(["shadow", "--port", "0", "--", sys.executable, str(AGENT_PATH)])
    out = capfd.readouterr().out
    if code == 3:
        pytest.skip("no refundable charge: run `python examples/refund_agent/seed.py` first")
    assert code == 0, out

    # The agent's whole contract is this one line, so nothing here parses its prose.
    line = next((row for row in out.splitlines() if row.startswith("AGENT-RESULT ")), None)
    assert line is not None, out
    result = dict(pair.split("=", 1) for pair in line.split()[1:])
    assert set(result) == {
        "charge",
        "refund",
        "amount",
        "currency",
        "listed",
        "refunded",
        "retry",
    }, line
    charge_id, amount = result["charge"], int(result["amount"])
    # The agent prints the charge's currency since #60, because irimi now formats the amount in the
    # summary from the code its own precondition read found on that charge - so the expected line
    # cannot be built from the amount alone.
    money = report.money(amount, result["currency"])
    assert result["refund"].startswith("re_"), "the agent did not get a parseable refund id"
    # What the agent saw: its refund in the list and the charge refunded in full, both overlaid,
    # and the retry refused by L3 because of a refund that exists only in irimi's write log.
    assert result["listed"] == "yes", line
    assert int(result["refunded"]) == amount, line
    assert result["retry"] == "charge_already_refunded", line

    # What Stripe holds - the assertion that matters: none of it happened.
    charge = stripe.Charge.retrieve(charge_id)
    assert charge.amount_refunded == 0, "a refund reached Stripe"
    assert charge.refunded is False
    assert list(stripe.Refund.list(charge=charge_id).data) == []

    # The summary says so. Through the door every Stripe call is on `api.stripe.com`, the agent's
    # reads and irimi's two precondition reads alike (#45).
    lines = out.splitlines()
    assert (
        "  api.stripe.com  3 reads (2 showing this run's writes)  2 engine reads  "
        "2 writes intercepted"
    ) in lines
    write = lines.index(f"  ○ refund {money} on {charge_id}  unvalidated (L3 preconditions passed)")
    assert lines[write + 1 : write + 4] == [
        "    ↳ GET /v1/refunds saw it  overlay",
        f"    ↳ GET /v1/charges/{charge_id} saw it  overlay",
        f"  ✗ refund {money} on {charge_id}  would fail: charge_already_refunded",
    ]
    assert (
        "  These writes did not happen. Would have fired: refund.created, charge.refunded."
    ) in lines

    # Slack is optional. Its read-back is asserted to show the post, as Notion words it - not to
    # be `overlay: full`, which only a channel id makes it (#44).
    if os.environ.get("SLACK_BOT_TOKEN") and os.environ.get("SLACK_CHANNEL"):
        history = next((row for row in lines if row.startswith("slack: history on ")), None)
        assert history is not None, out
        assert " shows " in history, history
