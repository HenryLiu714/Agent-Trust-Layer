"""The Phase 1 stamping invariants (#12), in one file, written to fail loudly.

Five claims irimi makes about every exchange, checked against a real run rather than against a
hand-built list, and each one demonstrated to fail on a policy that breaks it:

  (a) every answer the engine decided carries `Irimi-Answered-By`, and no live forward does;
  (b) nothing carrying that header is stamped anything but `unvalidated`;
  (c) `ShadowPolicy` cannot produce `validated` at all - that label is reserved for record mode;
  (d) a `delegated` exchange never reached the host the agent addressed;
  (e) a write L3 rejected never enters the write log, and a read irimi issued is never a write.

`stamping_violations` is the machinery for (a) and (b): it returns a list of strings rather than
asserting, so the same function can be asserted empty for an honest run and non-empty for a
deliberately broken one. A test that cannot fail is the one thing an invariant file must not ship.
"""

import argparse
import asyncio
import http.client
import json
import threading
from collections.abc import Sequence
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer

import pytest

from irimi import ca, paths, pipeline, servicemap
from irimi.engine import EngineConfig
from irimi.engine.mitm import MitmEngine
from irimi.exchange import KINDS, Exchange, Request, Response
from irimi.overlay import NoOverlay, Overlaid
from irimi.pipeline import ANSWERED_BY_HEADER, annotate, respond
from irimi.policy import Answer, ShadowPolicy
from irimi.store import NullStore

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
    """An upstream that writes down every request it is given. Invariant (d) reads that list."""

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
    """A target that answers a write with server-sent events, so the response streams (#28)."""

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
    """Every way the exchanges in `exchanges` break invariant (a) or (b), named one per line."""
    out: list[str] = []
    for ex in exchanges:
        where = f"{ex.answered_by} {ex.kind} {ex.request.method} {ex.request.path}"
        sent = respond(ex)
        stamp = _header(sent)
        if ex.answered_by != "live" and sent is not None and stamp is None:
            out.append(f"(a) engine-answered response with no {ANSWERED_BY_HEADER}: {where}")
        if ex.answered_by == "live" and stamp is not None:
            out.append(f"(a) live forward carrying {ANSWERED_BY_HEADER}={stamp}: {where}")
        if stamp is not None and stamp != ex.answered_by:
            out.append(f"(a) {ANSWERED_BY_HEADER}={stamp} does not name the answer: {where}")
        if stamp is not None and ex.validation != "unvalidated":
            out.append(f"(b) {ANSWERED_BY_HEADER}={stamp} on a {ex.validation} exchange: {where}")
    return out


def _exchange(answered_by, kind="write", validation="unvalidated", headers=()):
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
        validation=validation,
        run_id="t3st",
    )


# ------------------------------------------------------------------------------ (a) and (b)


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
    # of (a) that no pure function can see, and is invariant (d) for the delegated one.
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


def test_the_answer_a_failed_decision_gives_is_stamped_too(tmp_path, monkeypatch, upstream):
    """The one path that never sets `_Pending`, and so never reaches `response()` - which is the
    only place `respond` was being called from.

    It answered a 502 the engine decided, recorded it as `fake-L0`, and sent it with no header at
    all: a client using the header to tell our answer from the real service's read this one as the
    real service's. `stamping_violations` could not see it either, because it calls `respond` on
    the recorded Exchange and reads the header that call *would* produce - so this test asserts on
    `stamped`, the header the client actually received.
    """

    class _BrokenPolicy:
        name = "broken"

        def answer(self, request, classification, write_log=(), run_id=""):
            raise RuntimeError("the decision exploded")

    maps = _maps(tmp_path, monkeypatch)

    def calls(port):
        return [_via_proxy(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")]

    seen, ((status, data, stamped),) = _run(
        tmp_path, monkeypatch, maps, calls, policy=_BrokenPolicy()
    )
    assert status == 502 and json.loads(data)["error"]["type"] == "irimi_decision_failed"
    assert (seen[0].answered_by, stamped) == ("fake-L0", "fake-L0")
    assert stamping_violations(seen) == []
    assert _Recorder.seen == [], "the write reached the real upstream"


def test_a_policy_that_answers_locally_without_saying_so_is_caught(tmp_path, monkeypatch, upstream):
    """The deliberately broken fourth policy. It synthesizes an answer and stamps it `live`, so
    the agent's write never leaves the machine while the exchange claims the real service did it.
    The engine records `live`, `respond` therefore adds no header, and the upstream saw nothing -
    the three facts invariant (a) exists to make contradictory."""

    class _LiarPolicy:
        name = "liar"

        def answer(self, request, classification, write_log=(), run_id=""):
            if classification.kind == "read":
                return Answer(answered_by="live", response=None)
            return Answer(
                answered_by="live",
                response=Response(200, (("content-type", "application/json"),), b"{}"),
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

    `respond` cannot catch a policy that answers locally and stamps `live` - the stamp is the only
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
            out.append(f"(d) {ex.answered_by} exchange that reached the upstream: {ex.operation}")
    return out


def test_a_validated_stamp_on_an_answered_exchange_is_caught():
    """(b) is not reachable through `annotate`, so the case is built by hand: the check has to
    fail on it, or it is not checking anything."""
    assert stamping_violations([_exchange("fake-L0", validation="validated")]) != []
    assert stamping_violations([_exchange("fake-L0")]) == []


def test_a_header_that_names_the_wrong_answer_is_caught():
    forged = _exchange("live", headers=((ANSWERED_BY_HEADER, "fake-L0"),))
    assert stamping_violations([forged]) != []


# ------------------------------------------------------------------------------------- (c)


def test_shadow_policy_cannot_produce_validated(tmp_path, monkeypatch):
    """(c). `annotate` takes no validation argument and writes the literal `unvalidated`; the only
    other inhabitant of `Validation` is reserved for record mode in Phase 3. Every kind and every
    answer `ShadowPolicy` can give, through the same function the engine calls."""
    maps = _maps(tmp_path, monkeypatch)
    policy = ShadowPolicy()
    stamped = []
    for kind in KINDS:
        for method in ("GET", "POST", "DELETE"):
            request = pipeline.parse(method, "https", "127.0.0.1", 443, "/things", (), b"{}")
            cls = pipeline.classify(request, maps)
            cls = pipeline.Classification(
                cls.service, cls.operation, kind, cls.flags, cls.matched, cls.service_map
            )
            answer = policy.answer(request, cls)
            stamped.append(annotate(request, answer.response, cls, answer.answered_by, "t3st"))
    assert {ex.validation for ex in stamped} == {"unvalidated"}
    assert stamping_violations(stamped) == []


def test_annotate_has_no_way_to_ask_for_validated():
    """The type-level half of (c): there is no parameter to pass it through, so no policy, present
    or future, can reach `validated` without editing `annotate` itself."""
    import inspect

    assert "validation" not in inspect.signature(annotate).parameters


# ------------------------------------------------------------------------------------- (d)


def test_a_delegated_exchange_never_reaches_the_host_the_agent_addressed(
    tmp_path, monkeypatch, upstream, stub
):
    """(d). The agent addressed the upstream; the target answered. The upstream must have no
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
    """`respond` cannot stamp this one: mitmproxy has already sent the headers by the time the
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


# ------------------------------------------------------------------------ the L3 reader (#45)


def test_shadow_mode_is_never_built_without_a_reader(tmp_path, monkeypatch):
    """A policy with no reader does not do L3 and records nothing about it - by design, so bare
    `ShadowPolicy()`s in tests keep their meaning. The hole that leaves is a shadow run silently
    skipping L3 because the composition root forgot an argument: the `--allow-host` failure shape
    again, configured and never consulted. So the one construction site is pinned here."""
    from irimi import cli
    from irimi.policy import NoReader

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    index = servicemap.MapIndex(tuple(servicemap.load_shipped()))
    args = argparse.Namespace(allow_host=[], port=4321)
    p = ca.ca_paths()
    run = cli._Run(index, p, "t3st", cli._engine_config(args, index, "t3st", p))
    engine = cli._build_engine(run, on_exchange=lambda ex: None)
    assert isinstance(engine.policy, ShadowPolicy)
    assert not isinstance(engine.policy.reader, NoReader)
    assert engine.policy.maps is run.config.maps
    assert engine.policy.maps is index


# ------------------------------------------------------------------------------------- (e)

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
    human: refund {amount} on {charge}
    fixture: refund
    precondition: charge_refundable
    ids:
      id: re_
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
    """Hands every read back untouched and keeps each write log it was given. The addon's
    `write_log` is its own; what an overlay is handed is the only view of it a test should take."""

    def __init__(self) -> None:
        self.logs: list[tuple[Exchange, ...]] = []

    def __call__(self, write_log, read_request, upstream_response):
        self.logs.append(tuple(write_log))
        return Overlaid(upstream_response)

    def rewrite(self, write_log, read_request):
        self.logs.append(tuple(write_log))
        return read_request


def _l3_run(tmp_path, monkeypatch):
    """A refund L3 passes, one it rejects, then a read, through an engine holding the real
    reader. Hands back the exchanges and the overlay that watched the write log."""
    from irimi.policy import UpstreamReader

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "maps-home"))
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    (maps_dir / "stripe.yaml").write_text(STRIPE_MAP)
    maps = servicemap.load(cwd=tmp_path, maps_dir=maps_dir)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Charges)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    stripe = srv.server_address[1]
    overlay = _LogCapturingOverlay()

    def refund(port, charge):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        conn.request(
            "POST",
            f"http://127.0.0.1:{stripe}/v1/refunds",
            body=f"charge={charge}&amount=100".encode(),
            headers={
                "host": f"127.0.0.1:{stripe}",
                "content-type": "application/x-www-form-urlencoded",
            },
        )
        resp = conn.getresponse()
        resp.read()
        conn.close()
        return resp.status

    def calls(port):
        return [
            refund(port, "ch_REAL1"),
            refund(port, "ch_FULL1"),
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
    assert replies == [200, 400, 200]
    return seen, overlay


def test_a_rejected_write_is_never_in_the_write_log(tmp_path, monkeypatch):
    """L3 said the real service would have refused it, and the agent got that refusal. In the
    write log it would be replayed onto later reads - an effect for a write that happened in no
    world. Pinned at the log the overlay is handed, not at one of its downstream effects; a passed
    refund goes first so the log is not empty for the trivial reason."""
    seen, overlay = _l3_run(tmp_path, monkeypatch)
    assert [ex.precondition for ex in seen if ex.kind == "write"] == ["passed", "rejected"]
    assert overlay.logs, "the overlay was never handed the write log"
    for log in overlay.logs:
        assert [ex.precondition for ex in log] == ["passed"]


def engine_issued_violations(exchanges: Sequence[Exchange]) -> list[str]:
    """Every exchange irimi issued on its own account that is not a read, named one per line.
    A precondition read that were anything else would be irimi performing a write nobody asked
    for, in a tool whose promise is that writes are virtual."""
    return [
        f"(e) engine-issued {ex.kind}: {ex.request.method} {ex.request.path}"
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
