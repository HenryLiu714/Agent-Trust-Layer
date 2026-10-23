"""The stamping invariants, in one file, written to fail loudly.

Three claims irimi makes about every exchange, checked against a real run rather than against a
hand-built list, and each one demonstrated to fail on a policy that breaks it:

  (a) every answer the engine decided carries `Irimi-Answered-By`, its own errors included, and
      no live forward does;
  (b) a `delegated` exchange never reached the host the agent addressed;
  (c) a write L3 rejected never enters the write log, nor does an idempotent replay or conflict,
      a read irimi issued is never a write, and every read the overlay showed a write
      sits in the summary under a printed write of its own service that the overlay really
      held.

`stamping_violations` is the machinery for (a): it returns a list of strings rather than
asserting, so the same function can be asserted empty for an honest run and non-empty for a
deliberately broken one. A test that cannot fail is the one thing an invariant file must not ship.
"""

import argparse
import asyncio
import http.client
import json
import socket
import threading
from collections.abc import Sequence
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer

import pytest

from irimi import ca, classify, ingress, paths, servicemap, wire
from irimi.engine import EngineConfig, mitm
from irimi.engine.mitm import MitmEngine
from irimi.exchange import KINDS, Exchange, Flag, Request, Response
from irimi.overlay import NoOverlay, Overlaid, ServiceOverlay
from irimi.policy import Decided, Forward, Respond, ShadowPolicy
from irimi.protocol.headers import ANSWERED_BY_HEADER
from irimi.replay.index import ReplayIndex
from irimi.replay.policy import ReplayPolicy
from irimi.report.model import RunView, build, for_summary
from irimi.report.summary import OVERLAY_MARKER, Live, summary_lines
from irimi.trace import RunRecord
from irimi.wire import stamp
from tests.doubles import NullStore

# One host, one read route and one write route. The servers in this file all listen on loopback,
# so the map claims 127.0.0.1; hosts carry no port, so their random ports do not matter.
DEMO_MAP = """
version: 1
service: demo
hosts:
  - 127.0.0.1
routes:
  - match:
      method: GET
      path: /hello
    operation: things.list
    kind: read
    human: list things
  - match:
      method: POST
      path: /things
    operation: things.create
    kind: write
    human: create a thing
"""


class _Recorder(BaseHTTPRequestHandler):
    """An upstream that writes down every request it is given. Invariant (b) reads that list."""

    seen: list[tuple[str, str]] = []

    def _answer(self):
        _Recorder.seen.append((self.command, self.path))
        body = b'{"from": "upstream"}'
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = _answer
    do_POST = _answer

    def log_message(self, *args):
        pass


class _Target(BaseHTTPRequestHandler):
    seen: list[tuple[str, str]] = []

    def do_POST(self):
        _Target.seen.append((self.command, self.path))
        body = b'{"from": "target"}'
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _StreamTarget(BaseHTTPRequestHandler):
    """A target that answers a write with server-sent events, so the response streams."""

    seen: list[tuple[str, str]] = []

    def do_POST(self):
        _StreamTarget.seen.append((self.command, self.path))
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.end_headers()
        self.wfile.write(b"data: one\n\n")
        self.wfile.flush()
        self.wfile.write(b"data: two\n\n")
        self.wfile.flush()

    def log_message(self, *args):
        pass


def _serve(handler):
    handler.seen = []
    srv = HTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    return srv


@pytest.fixture
def upstream():
    srv = _serve(_Recorder)
    yield srv.server_address[1]
    srv.shutdown()


@pytest.fixture
def stub():
    srv = _serve(_Target)
    yield srv.server_address[1]
    srv.shutdown()


def _maps(tmp_path, monkeypatch, targets=()):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "maps-home"))
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    (maps_dir / "demo.yaml").write_text(DEMO_MAP)
    return servicemap.load(cwd=tmp_path, maps_dir=maps_dir, targets=targets)


def _run(tmp_path, monkeypatch, maps, calls, policy=None, overlay=None):
    """Serve `maps`, let `calls(port)` reach the proxy, and hand back what the run produced."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    p = ca.ca_paths()
    ca.generate_ca(p)
    cfg = EngineConfig(
        run_id="t3st",
        ca=p,
        confdir=paths.mitm_dir(),
        listen_host="127.0.0.1",
        listen_port=0,
        reverse_hosts=frozenset(),
        maps=maps,
    )
    seen: list[Exchange] = []
    eng = MitmEngine(
        cfg,
        policy or ShadowPolicy(),
        NullStore(),
        overlay or NoOverlay(),
        on_exchange=seen.append,
    )
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=lambda: loop.run_until_complete(eng.run()), daemon=True)
    thread.start()
    asyncio.run_coroutine_threadsafe(eng.wait_ready(), loop).result(timeout=15)
    try:
        replies = calls(eng.listen_port())
    finally:
        eng.shutdown()
        thread.join(timeout=15)
        loop.close()
    return seen, replies


def _via_proxy(proxy_port, method, url, body=None):
    conn = http.client.HTTPConnection("127.0.0.1", proxy_port, timeout=10)
    headers = {"host": url.split("/")[2]}
    if body is not None:
        headers["content-type"] = "application/json"
    conn.request(method, url, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    stamped = resp.getheader(ANSWERED_BY_HEADER)
    conn.close()
    return resp.status, data, stamped


# ------------------------------------------------------------------- the invariants themselves


def _header(response: Response | None) -> str | None:
    if response is None:
        return None
    return next((v for k, v in response.headers if k.lower() == ANSWERED_BY_HEADER), None)


def stamping_violations(exchanges: Sequence[Exchange]) -> list[str]:
    """Every way the exchanges in `exchanges` break invariant (a), named one per line."""
    out: list[str] = []
    for ex in exchanges:
        where = f"{ex.answered_by} {ex.kind} {ex.request.method} {ex.request.path}"
        sent = None if ex.response is None else stamp(ex.response, ex.answered_by)
        value = _header(sent)
        if ex.answered_by != "live" and sent is not None and value is None:
            out.append(f"(a) engine-answered response with no {ANSWERED_BY_HEADER}: {where}")
        if ex.answered_by == "live" and value is not None:
            out.append(f"(a) live forward carrying {ANSWERED_BY_HEADER}={value}: {where}")
        if value is not None and value != ex.answered_by:
            out.append(f"(a) {ANSWERED_BY_HEADER}={value} does not name the answer: {where}")
    return out


def _exchange(answered_by, kind="write", headers=()):
    request = Request(
        method="POST",
        scheme="https",
        host="127.0.0.1",
        port=443,
        path="/things",
        query="",
        headers=(),
        body=b"",
    )
    return Exchange(
        request=request,
        response=Response(status=200, headers=headers, body=b"{}"),
        service="demo",
        operation="things.create",
        kind=kind,
        answered_by=answered_by,
        run_id="t3st",
    )


# ----------------------------------------------------------------------------------------- (a)


def test_a_real_run_answers_every_way_and_breaks_no_invariant(
    tmp_path, monkeypatch, upstream, stub
):
    """One live read, one locally answered write and one delegated write, in one run. The point of
    driving a real engine is that the exchanges are the ones the product built, not ones a test
    arranged to pass its own check."""
    maps = _maps(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{stub}/w")]
    )

    def calls(port):
        return [
            _via_proxy(port, "GET", f"http://127.0.0.1:{upstream}/hello"),
            _via_proxy(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}"),
            _via_proxy(port, "DELETE", f"http://127.0.0.1:{upstream}/unmapped"),
        ]

    seen, replies = _run(tmp_path, monkeypatch, maps, calls)
    assert sorted(ex.answered_by for ex in seen) == ["delegated", "fake-L0", "live"]
    assert stamping_violations(seen) == []
    # The live read reached the real upstream and neither of the other two did, which is the half
    # of (a) that no pure function can see, and is invariant (b) for the delegated one.
    assert _Recorder.seen == [("GET", "/hello")]
    assert reached_upstream_violations(seen, _Recorder.seen) == []
    # And on the wire, which is where it matters: the header the client actually received.
    assert [stamped for _, _, stamped in replies] == [None, "delegated", "fake-L0"]


def test_the_locally_answered_write_is_stamped_and_the_live_read_is_not(
    tmp_path, monkeypatch, upstream
):
    maps = _maps(tmp_path, monkeypatch)

    def calls(port):
        return [
            _via_proxy(port, "GET", f"http://127.0.0.1:{upstream}/hello"),
            _via_proxy(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}"),
        ]

    seen, replies = _run(tmp_path, monkeypatch, maps, calls)
    assert [stamped for _, _, stamped in replies] == [None, "fake-L0"]
    assert stamping_violations(seen) == []


def test_a_replay_stamps_what_it_answered_and_leaves_a_live_forward_bare(
    tmp_path, monkeypatch, upstream
):
    """(a) in a replay: a read answered from the recording carries `recording`, the write
    answered with its recorded answer carries its recorded level, and the read the recording had
    no second answer for goes live and carries nothing. Nothing in these requests is a secret, so
    the recording is the run's own exchanges, as the store would have written them."""
    maps = _maps(tmp_path, monkeypatch)

    def calls(port):
        return [
            _via_proxy(port, "GET", f"http://127.0.0.1:{upstream}/hello"),
            _via_proxy(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}"),
        ]

    recorded, _ = _run(tmp_path, monkeypatch, maps, calls)
    _Recorder.seen = []
    policy = ReplayPolicy(ReplayIndex.from_events(recorded, b"k" * 32, maps), maps)
    seen, replies = _run(
        tmp_path,
        monkeypatch,
        maps,
        lambda port: [*calls(port), _via_proxy(port, "GET", f"http://127.0.0.1:{upstream}/hello")],
        policy=policy,
    )
    assert [stamped for _, _, stamped in replies] == ["recording", "fake-L0", None]
    assert [ex.answered_by for ex in seen] == ["recording", "fake-L0", "live"]
    assert stamping_violations(seen) == []
    assert _Recorder.seen == [("GET", "/hello")]


def test_the_answer_a_failed_decision_gives_is_stamped_too(tmp_path, monkeypatch, upstream):
    """A path that never sets a `Plan`, and so never reaches `response()`. A 502 sent with no
    header would read as the real service's answer to a client using the header to tell the two
    apart. `stamping_violations` cannot see that, because it calls `stamp` on the recorded
    Exchange and reads the header that call *would* produce - so this test asserts on `stamped`,
    the header the client actually received.
    """

    class _BrokenPolicy:
        name = "broken"

        def decide(self, request, classification, write_log=(), run_id="", target=None):
            raise RuntimeError("the decision exploded")

    maps = _maps(tmp_path, monkeypatch)

    def calls(port):
        return [_via_proxy(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")]

    seen, ((status, data, stamped),) = _run(
        tmp_path, monkeypatch, maps, calls, policy=_BrokenPolicy()
    )
    assert status == 502 and json.loads(data)["error"]["type"] == "irimi_decision_failed"
    assert (seen[0].answered_by, stamped) == ("irimi", "irimi")
    assert stamping_violations(seen) == []
    assert _Recorder.seen == [], "the write reached the real upstream"


def _via_base_url(proxy_port, method, path):
    """A request in base-URL mode: origin-form, addressed to the listener itself."""
    conn = http.client.HTTPConnection("127.0.0.1", proxy_port, timeout=10)
    conn.request(method, path, body=b"{}", headers={"host": f"127.0.0.1:{proxy_port}"})
    resp = conn.getresponse()
    data = resp.read()
    stamped = resp.getheader(ANSWERED_BY_HEADER)
    conn.close()
    return resp.status, data, stamped


def _closed_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _boom(*args, **kwargs):
    raise RuntimeError("exploded")


@pytest.mark.parametrize(
    ("case", "status", "flag"),
    [
        ("unparseable", 400, Flag.REFUSED),
        ("unroutable", 400, Flag.REFUSED),
        ("base-url-refused", 403, Flag.REFUSED),
        ("unrewritable", 400, Flag.REFUSED),
        ("target-unreachable", 502, Flag.TARGET_FAILED),
    ],
)
def test_irimis_own_errors_are_stamped_json_and_recorded(
    tmp_path, monkeypatch, upstream, case, status, flag
):
    """(a) for every error irimi answers with itself: the request hook's refusals and a target it
    cannot reach. Each carries `Irimi-Answered-By: irimi` on the wire, has the one JSON error
    shape, and is in the trace flagged with why. One that skipped any of the three would read as
    the real service's answer, or be missing from the summary."""
    targets = []
    if case == "target-unreachable":
        targets = [("127.0.0.1", "/things", f"http://127.0.0.1:{_closed_port()}/w")]
    maps = _maps(tmp_path, monkeypatch, targets=targets)
    if case == "unparseable":
        monkeypatch.setattr(mitm, "_request_from_flow", _boom)
    elif case == "unroutable":
        monkeypatch.setattr(ingress, "detect_door", _boom)
    elif case == "unrewritable":
        monkeypatch.setattr(ingress, "rewrite_reverse", _boom)

    def calls(port):
        if case in ("base-url-refused", "unrewritable"):
            return [_via_base_url(port, "POST", "/api.stripe.com/v1/refunds")]
        return [_via_proxy(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")]

    seen, ((got, data, stamped),) = _run(tmp_path, monkeypatch, maps, calls)
    assert (got, stamped) == (status, "irimi")
    error = json.loads(data)["error"]
    assert set(error) == {"type", "message"} and error["type"].startswith("irimi_")
    (ex,) = seen
    assert ex.answered_by == "irimi" and flag in ex.flags
    assert ex.response is not None and ex.response.status == status
    assert stamping_violations(seen) == []
    assert _Recorder.seen == [], "the request reached the real upstream"


def test_a_policy_that_answers_locally_without_saying_so_is_caught(tmp_path, monkeypatch, upstream):
    """The deliberately broken fourth policy. It synthesizes an answer and stamps it `live`, so
    the agent's write never leaves the machine while the exchange claims the real service did it.
    The engine records `live`, `stamp` therefore adds no header, and the upstream saw nothing -
    the three facts invariant (a) exists to make contradictory."""

    class _LiarPolicy:
        name = "liar"

        def decide(self, request, classification, write_log=(), run_id="", target=None):
            if classification.kind == "read":
                return Decided(Forward())
            return Decided(
                Respond(Response(200, (("content-type", "application/json"),), b"{}"), "live")
            )

    maps = _maps(tmp_path, monkeypatch)

    def calls(port):
        return [_via_proxy(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")]

    seen, replies = _run(tmp_path, monkeypatch, maps, calls, policy=_LiarPolicy())
    (ex,) = seen
    assert ex.answered_by == "live"
    assert _Recorder.seen == [], "the write never reached the upstream, so `live` is a lie"
    assert replies[0][2] is None, "and it carries no header to give that away"
    assert reached_upstream_violations(seen, _Recorder.seen) != []


def reached_upstream_violations(
    exchanges: Sequence[Exchange], upstream_hits: Sequence[tuple[str, str]]
) -> list[str]:
    """Invariant (a)'s other half, which only an end-to-end run can check: a `live` exchange
    reached the real service, and an engine-answered one did not.

    `stamp` cannot catch a policy that answers locally and stamps `live` - the stamp is the only
    thing it has to go on, and a liar's stamp says `live`. What the upstream actually received is
    the fact outside the policy's reach, so the check is written against it.
    """
    out: list[str] = []
    hits = list(upstream_hits)
    for ex in exchanges:
        reached = (ex.request.method, ex.request.path) in hits
        if ex.answered_by == "live" and not reached:
            out.append(f"(a) `live` exchange the upstream never saw: {ex.operation}")
        if ex.answered_by != "live" and reached:
            out.append(f"(b) {ex.answered_by} exchange that reached the upstream: {ex.operation}")
    return out


def test_a_header_that_names_the_wrong_answer_is_caught():
    forged = _exchange("live", headers=((ANSWERED_BY_HEADER, "fake-L0"),))
    assert stamping_violations([forged]) != []


def test_every_answer_shadow_policy_gives_is_stamped_for_who_gave_it(tmp_path, monkeypatch):
    """(a) over every kind and every answer `ShadowPolicy` can give."""
    maps = _maps(tmp_path, monkeypatch)
    policy = ShadowPolicy()
    stamped = []
    for kind in KINDS:
        for method in ("GET", "POST", "DELETE"):
            request = wire.parse(method, "https", "127.0.0.1", 443, "/things", (), b"{}")
            cls = classify.classify(request, maps)
            cls = classify.Classification(
                cls.service, cls.operation, kind, cls.flags, cls.matched, cls.service_map
            )
            decision = policy.decide(request, cls).decision
            stamped.append(
                Exchange(
                    request=request,
                    response=decision.response if isinstance(decision, Respond) else None,
                    service=cls.service,
                    operation=cls.operation,
                    kind=cls.kind,
                    answered_by=decision.answered_by,
                    run_id="t3st",
                    flags=cls.flags,
                )
            )
    assert stamping_violations(stamped) == []


# ------------------------------------------------------------------------------------- (b)


def test_a_delegated_exchange_never_reaches_the_host_the_agent_addressed(
    tmp_path, monkeypatch, upstream, stub
):
    """(b). The agent addressed the upstream; the target answered. The upstream must have no
    record of it at all - a delegated write that also reached production is the failure this
    whole feature exists to prevent."""
    maps = _maps(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{stub}/w")]
    )

    def calls(port):
        return [_via_proxy(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")]

    seen, replies = _run(tmp_path, monkeypatch, maps, calls)
    (ex,) = seen
    assert ex.answered_by == "delegated"
    assert ex.request.host == "127.0.0.1" and ex.request.path == "/things"
    assert _Target.seen == [("POST", "/w")]
    assert _Recorder.seen == [], "the delegated write reached the real upstream"
    assert reached_upstream_violations(seen, _Recorder.seen) == []
    assert replies[0][2] == "delegated"


@pytest.fixture
def stream_stub():
    srv = _serve(_StreamTarget)
    yield srv.server_address[1]
    srv.shutdown()


def test_a_streamed_delegated_answer_is_stamped_too(tmp_path, monkeypatch, upstream, stream_stub):
    """`stamp` cannot stamp this one: mitmproxy has already sent the headers by the time the
    `response` hook runs, so the rebuilt response never reaches the client. Deleting the stamp in
    `responseheaders` leaves this the only test that goes red."""
    maps = _maps(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{stream_stub}/w")],
    )

    def calls(port):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        conn.request(
            "POST",
            f"http://127.0.0.1:{upstream}/things",
            body=b"{}",
            headers={"host": f"127.0.0.1:{upstream}", "content-type": "application/json"},
        )
        resp = conn.getresponse()
        stamped = resp.getheader(ANSWERED_BY_HEADER)
        body = resp.read()
        conn.close()
        return stamped, body

    seen, (stamped, body) = _run(tmp_path, monkeypatch, maps, calls)
    assert _StreamTarget.seen == [("POST", "/w")]
    assert body == b"data: one\n\ndata: two\n\n"
    assert stamped == "delegated"
    (ex,) = seen
    assert ex.answered_by == "delegated"


# ------------------------------------------------------------------------ the L3 reader


def test_shadow_mode_is_never_built_without_a_reader(tmp_path, monkeypatch):
    """A policy with no reader does not do L3 and records nothing about it - by design, so bare
    `ShadowPolicy()`s in tests keep their meaning. The hole that leaves is a shadow run silently
    skipping L3 because the composition root forgot an argument: the `--allow-host` failure shape
    again, configured and never consulted. So the one construction site is pinned here."""
    from irimi import cli
    from irimi.reader import NoReader
    from tests.doubles import NullStore

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    index = servicemap.MapIndex(tuple(servicemap.loader.load_shipped()))
    args = argparse.Namespace(allow_host=[], port=4321, listen_host=paths.LISTEN_HOST)
    p = ca.ca_paths()
    run = cli._Run(index, p, "t3st", cli._engine_config(args, index, "t3st", p), NullStore())
    engine = cli._build_engine(run, on_exchange=lambda ex: None)
    assert isinstance(engine.policy, ShadowPolicy)
    assert not isinstance(engine.policy.reader, NoReader)
    assert engine.policy.maps is run.config.maps
    assert engine.policy.maps is index


# ------------------------------------------------------------------------------------- (c)

# The shipped Stripe map's two routes that L3 touches, claimed on loopback: the refund that names
# `charge_refundable`, and the charge read its precondition probe has to classify as.
STRIPE_MAP = """
version: 1
service: stripe
hosts:
  - 127.0.0.1
routes:
  - match:
      method: POST
      path: /v1/refunds
    operation: refunds.create
    kind: write
    human: refund {amount:money} on {charge}
    fixture: refund
    precondition: charge_refundable
    ids:
      id: re_
    fires:
      - refund.created
      - charge.refunded
  - match:
      method: GET
      path: /v1/charges/{charge}
    operation: charges.retrieve
    kind: read
    human: get charge {charge}
"""


class _Charges(BaseHTTPRequestHandler):
    """Real Stripe state for L3: `ch_REAL1` is refundable, `ch_FULL1` is fully refunded."""

    def do_GET(self):
        charge_id = self.path.rsplit("/", 1)[-1]
        refunded = 4900 if charge_id == "ch_FULL1" else 0
        body = json.dumps(
            {
                "id": charge_id,
                "object": "charge",
                "amount": 4900,
                "amount_refunded": refunded,
                "refunded": refunded == 4900,
                "currency": "usd",
            }
        ).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _LogCapturingOverlay:
    """Keeps each write log it was given, and hands every read back untouched - or to `inner`, the
    real overlay, when there is one. The addon's `write_log` is its own; what an overlay is handed
    is the only view of it a test should take."""

    def __init__(self, inner: ServiceOverlay | None = None) -> None:
        self.logs: list[tuple[Exchange, ...]] = []
        self.inner = inner

    def __call__(
        self, write_log, classification, read_request, upstream_response, translated_from=None
    ):
        self.logs.append(tuple(write_log))
        if self.inner is not None:
            return self.inner(
                write_log, classification, read_request, upstream_response, translated_from
            )
        return Overlaid(upstream_response)

    def rewrite(self, write_log, classification, read_request):
        self.logs.append(tuple(write_log))
        if self.inner is not None:
            return self.inner.rewrite(write_log, classification, read_request)
        return None


def _l3_run(tmp_path, monkeypatch):
    """A refund L3 passes, one it rejects, then a read, through an engine holding the real
    reader. Hands back the exchanges and the overlay that watched the write log."""
    seen, overlay, replies = _stripe_run(
        tmp_path, monkeypatch, [("ch_REAL1", 100, None), ("ch_FULL1", 100, None)]
    )
    assert replies == [200, 400, 200]
    return seen, overlay


def _stripe_run(tmp_path, monkeypatch, refunds, *, real_overlay=False):
    """Each `(charge, amount, idempotency key or None)` in `refunds` as a refund, then a charge
    read, through an engine holding the real reader. Hands back the exchanges, the overlay that
    watched the write log, and every status in order. With `real_overlay` that overlay passes each
    read on to the real `ServiceOverlay`, which applies the accepted refunds to the closing read."""
    from irimi.reader import UpstreamReader

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "maps-home"))
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    (maps_dir / "stripe.yaml").write_text(STRIPE_MAP)
    maps = servicemap.load(cwd=tmp_path, maps_dir=maps_dir)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Charges)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    stripe = srv.server_address[1]
    overlay = _LogCapturingOverlay(ServiceOverlay() if real_overlay else None)

    def refund(port, charge, amount, key):
        headers = {
            "host": f"127.0.0.1:{stripe}",
            "content-type": "application/x-www-form-urlencoded",
        }
        if key is not None:
            headers["idempotency-key"] = key
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        conn.request(
            "POST",
            f"http://127.0.0.1:{stripe}/v1/refunds",
            body=f"charge={charge}&amount={amount}".encode(),
            headers=headers,
        )
        resp = conn.getresponse()
        resp.read()
        conn.close()
        return resp.status

    def calls(port):
        return [
            *(refund(port, charge, amount, key) for charge, amount, key in refunds),
            _via_proxy(port, "GET", f"http://127.0.0.1:{stripe}/v1/charges/ch_REAL1")[0],
        ]

    try:
        seen, replies = _run(
            tmp_path,
            monkeypatch,
            maps,
            calls,
            policy=ShadowPolicy(reader=UpstreamReader(), maps=maps),
            overlay=overlay,
        )
    finally:
        srv.shutdown()
    return seen, overlay, replies


def test_a_rejected_write_is_never_in_the_write_log(tmp_path, monkeypatch):
    """L3 said the real service would have refused it, and the agent got that refusal. In the
    write log it would be replayed onto later reads - an effect for a write that happened in no
    world. Pinned at the log the overlay is handed, not at one of its downstream effects; a passed
    refund goes first so the log is not empty for the trivial reason."""
    seen, overlay = _l3_run(tmp_path, monkeypatch)
    assert [ex.precondition.outcome for ex in seen if ex.kind == "write"] == ["passed", "rejected"]
    assert overlay.logs, "the overlay was never handed the write log"
    for log in overlay.logs:
        assert [ex.precondition.outcome for ex in log] == ["passed"]


def test_a_replayed_write_is_never_in_the_write_log_twice(tmp_path, monkeypatch):
    """The agent's retry with the key it already sent is the same write. A second entry
    would have the overlay apply one refund twice."""
    seen, overlay, replies = _stripe_run(
        tmp_path, monkeypatch, [("ch_REAL1", 100, "k-1"), ("ch_REAL1", 100, "k-1")]
    )
    assert replies == [200, 200, 200]
    writes = [ex for ex in seen if ex.kind == "write"]
    assert [Flag.IDEMPOTENT_REPLAY in ex.flags for ex in writes] == [False, True]
    assert overlay.logs, "the overlay was never handed the write log"
    for log in overlay.logs:
        assert len(log) == 1
        assert Flag.IDEMPOTENT_REPLAY not in log[0].flags


def test_an_idempotency_conflict_is_never_in_the_write_log(tmp_path, monkeypatch):
    """A key reused for a different write was refused, as the real service would refuse it:
    nothing was minted and there is no effect to replay."""
    seen, overlay, replies = _stripe_run(
        tmp_path, monkeypatch, [("ch_REAL1", 100, "k-2"), ("ch_REAL1", 250, "k-2")]
    )
    assert replies == [200, 400, 200]
    writes = [ex for ex in seen if ex.kind == "write"]
    assert [Flag.IDEMPOTENCY_CONFLICT in ex.flags for ex in writes] == [False, True]
    assert overlay.logs, "the overlay was never handed the write log"
    for log in overlay.logs:
        assert len(log) == 1
        assert Flag.IDEMPOTENCY_CONFLICT not in log[0].flags


def test_only_a_write_that_would_have_happened_lists_its_webhooks(tmp_path, monkeypatch):
    """`would_fire` is a claim about a write irimi accepted, and the write log is the run's own
    record of which those are. Said once, as the one-directional property: an exchange that
    lists webhooks is one the overlay was handed as a write. The three that list nothing do so
    for three different reasons - L3 said the service would have refused it, the service would
    have refused the reused key, and a replay's events are already on the first write's own
    exchange."""
    seen, overlay, replies = _stripe_run(
        tmp_path,
        monkeypatch,
        [
            ("ch_REAL1", 100, None),  # accepted
            ("ch_FULL1", 100, None),  # L3 rejection
            ("ch_REAL1", 100, "k-1"),  # accepted
            ("ch_REAL1", 100, "k-1"),  # replay
            ("ch_REAL1", 100, "k-2"),  # accepted
            ("ch_REAL1", 250, "k-2"),  # conflict
        ],
    )
    assert replies == [200, 400, 200, 200, 200, 400, 200]
    writes = [ex for ex in seen if ex.kind == "write"]
    assert [ex.would_fire != () for ex in writes] == [True, False, True, False, True, False]
    for ex in writes:
        if ex.would_fire:
            assert ex.would_fire == ("refund.created", "charge.refunded")
    for ex in seen:
        refused_or_replayed = (
            Flag.IDEMPOTENT_REPLAY in ex.flags
            or Flag.IDEMPOTENCY_CONFLICT in ex.flags
            or ex.precondition.outcome == "rejected"
        )
        if refused_or_replayed or ex.kind == "read":
            assert ex.would_fire == (), (ex.kind, ex.flags, ex.precondition.outcome)

    assert overlay.logs, "the overlay was never handed the write log"
    logged = {(ex.request.path, ex.request.body) for ex in overlay.logs[-1]}
    for ex in writes:
        if ex.would_fire:
            assert (ex.request.path, ex.request.body) in logged


def _owes_overlay_line(ex: Exchange) -> bool:
    """One of the agent's own reads that irimi edited to show a write, or knows it showed only in
    part. This deliberately restates the rule `report.model.disposition` applies rather than
    calling it: the invariant is what the summary OWES, so a model that stopped owing a line
    must fail here instead of agreeing with itself."""
    return (
        ex.kind == "read"
        and ex.issued_by == "agent"
        and (ex.answered_by == "overlay" or ex.overlay == "partial")
    )


def _filed(exchanges: Sequence[Exchange]) -> list[tuple[Exchange, list[Exchange]]]:
    """Each write the summary prints, with the reads it files under it."""
    view = RunView(RunRecord.header("t3st"), list(exchanges))
    writes = build(view, None).writes
    return [(entry.exchange, [read for read, _ in entry.reads]) for entry in writes]


def overlay_line_violations(
    exchanges: Sequence[Exchange], write_log: Sequence[Exchange]
) -> list[str]:
    """Every one of the agent's reads the summary owes a `↳` line but files under no write, or
    under a write the overlay was never handed, named one per line. `write_log` is every write the
    overlay held during the run: a read cannot have been shown a write outside it, so a `↳` under
    one - a refused retry, say - tells the reader the agent saw what it never saw. Matched by
    identity, not equality: a copy of a filed read is a different read."""
    filed = [(write, read) for write, reads in _filed(exchanges) for read in reads]
    violations = []
    for ex in exchanges:
        if not _owes_overlay_line(ex):
            continue
        what = f"{ex.answered_by} read, overlay {ex.overlay}: {ex.request.method} {ex.request.path}"
        under = next((write for write, read in filed if read is ex), None)
        if under is None:
            violations.append(f"(c) {what} is under no {ex.service} write")
        elif not any(under is logged for logged in write_log):
            violations.append(f"(c) {what} is under a write the overlay never held")
    return violations


def test_every_overlaid_read_has_a_write_of_its_own_service_to_sit_under(tmp_path, monkeypatch):
    """An overlaid read shows the agent a write that never happened, and the summary's `↳` line
    is the only place a reader learns which of the agent's reads were shown one. The report
    files each under the latest earlier write of its own service and quietly skips one it cannot
    place; that skip is safe only because a real run never produces such a read - the overlay
    edits or flags a read only on the strength of a same-service write in the log, and every entry
    there is a printed write. And each is filed under a write the overlay really held: the run's
    L3 rejection falls between the accepted refund and the read, and a refused write is printed
    but never logged, so a `↳` under the `✗` would say the agent saw a refund that was refused.
    Held over a real run through the real overlay."""
    seen, overlay, replies = _stripe_run(
        tmp_path,
        monkeypatch,
        [("ch_REAL1", 100, None), ("ch_FULL1", 100, None)],
        real_overlay=True,
    )
    assert replies == [200, 400, 200]
    owed = [ex for ex in seen if _owes_overlay_line(ex)]
    assert [ex.answered_by for ex in owed] == ["overlay"], "the run showed the agent no write"
    held = [ex for log in overlay.logs for ex in log]
    assert [ex.precondition.outcome for ex in held] == ["passed"] * len(held)
    assert overlay_line_violations(seen, held) == []
    view = RunView(RunRecord.header("t3st"), [*map(for_summary, seen)])
    lines = summary_lines(view, None, live=Live())
    assert sum(line.lstrip().startswith(OVERLAY_MARKER) for line in lines) == len(owed)

    # And the check can fail: a read before any write, one whose service wrote nothing, and one
    # filed under a write the overlay never held.
    early, elsewhere = replace(owed[0]), replace(owed[0], service="slack")
    assert len(overlay_line_violations([early, *seen, elsewhere], held)) == 2
    assert len(overlay_line_violations(seen, [])) == 1


def test_a_replayed_rejection_is_never_in_the_write_log(tmp_path, monkeypatch):
    """A retry of a write L3 rejected gets the same refusal back, and stays out of the log on
    both counts: it is a rejection and it is a replay. A passed refund goes first so
    the log is not empty for the trivial reason."""
    seen, overlay, replies = _stripe_run(
        tmp_path,
        monkeypatch,
        [("ch_REAL1", 100, None), ("ch_FULL1", 100, "k-3"), ("ch_FULL1", 100, "k-3")],
    )
    assert replies == [200, 400, 400, 200]
    writes = [ex for ex in seen if ex.kind == "write"]
    assert [ex.precondition.outcome for ex in writes] == ["passed", "rejected", "rejected"]
    assert Flag.IDEMPOTENT_REPLAY in writes[-1].flags
    assert overlay.logs, "the overlay was never handed the write log"
    for log in overlay.logs:
        assert [ex.precondition.outcome for ex in log] == ["passed"]


def engine_issued_violations(exchanges: Sequence[Exchange]) -> list[str]:
    """Every exchange irimi issued on its own account that is not a read, named one per line.
    A precondition read that were anything else would be irimi performing a write nobody asked
    for, in a tool whose promise is that writes are virtual."""
    return [
        f"(c) engine-issued {ex.kind}: {ex.request.method} {ex.request.path}"
        for ex in exchanges
        if ex.issued_by == "engine" and ex.kind != "read"
    ]


def test_an_engine_issued_exchange_is_never_a_write(tmp_path, monkeypatch):
    seen, _ = _l3_run(tmp_path, monkeypatch)
    assert [ex.issued_by for ex in seen].count("engine") == 2
    assert engine_issued_violations(seen) == []
    # And the check can fail: an engine-issued write is caught.
    forged = replace(_exchange("fake-L0", kind="write"), issued_by="engine")
    assert engine_issued_violations([forged]) != []
