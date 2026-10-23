"""Every answer irimi decided carries `Irimi-Answered-By`, its own errors included, and no live
forward does; an exchange sent to a target never reached the host the agent addressed."""

import http.client
import json
import socket

import pytest

from irimi import servicemaps
from irimi.config import paths
from irimi.core.exchange import KINDS, Exchange, Flag, Response
from irimi.decide.policy import Decided, Forward, Respond, ShadowPolicy
from irimi.engine import ingress, mitm, wire
from irimi.protocol.headers import ANSWERED_BY_HEADER
from irimi.replay.index import ReplayIndex
from irimi.replay.policy import ReplayPolicy
from irimi.servicemaps import classify
from tests.support.builders import demo_exchange
from tests.support.engine import run_engine, via_proxy_stamped
from tests.support.invariants import reached_upstream_violations, stamping_violations
from tests.support.upstreams import Logged, serve

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


class _Recorder(Logged):
    """An upstream that writes down every request it is given. The target rule reads that list."""

    def _answer(self):
        self.seen.append((self.command, self.path))
        body = b'{"from": "upstream"}'
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = _answer
    do_POST = _answer


class _Target(Logged):
    def do_POST(self):
        self.seen.append((self.command, self.path))
        body = b'{"from": "target"}'
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _StreamTarget(Logged):
    """A target that answers a write with server-sent events, so the response streams."""

    def do_POST(self):
        self.seen.append((self.command, self.path))
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.end_headers()
        self.wfile.write(b"data: one\n\n")
        self.wfile.flush()
        self.wfile.write(b"data: two\n\n")
        self.wfile.flush()


@pytest.fixture
def upstream():
    port = serve(_Recorder)
    yield port
    port.stop()


@pytest.fixture
def stub():
    port = serve(_Target)
    yield port
    port.stop()


def _maps(tmp_path, monkeypatch, targets=()):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "maps-home"))
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    (maps_dir / "demo.yaml").write_text(DEMO_MAP)
    return servicemaps.load(cwd=tmp_path, maps_dir=maps_dir, targets=targets)


# --------------------------------------------------- every answer irimi decided is stamped


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
            via_proxy_stamped(port, "GET", f"http://127.0.0.1:{upstream}/hello"),
            via_proxy_stamped(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}"),
            via_proxy_stamped(port, "DELETE", f"http://127.0.0.1:{upstream}/unmapped"),
        ]

    seen, replies = run_engine(tmp_path, monkeypatch, maps, calls)
    assert sorted(ex.answered_by for ex in seen) == ["delegated", "fake-L0", "live"]
    assert stamping_violations(seen) == []
    # The live read reached the real upstream and neither of the other two did, which is the half
    # of the stamping rule that no pure function can see, and is the target rule for the delegated
    # one.
    assert upstream.seen == [("GET", "/hello")]
    assert reached_upstream_violations(seen, upstream.seen) == []
    # And on the wire, which is where it matters: the header the client actually received.
    assert [stamped for _, _, stamped in replies] == [None, "delegated", "fake-L0"]


def test_the_locally_answered_write_is_stamped_and_the_live_read_is_not(
    tmp_path, monkeypatch, upstream
):
    maps = _maps(tmp_path, monkeypatch)

    def calls(port):
        return [
            via_proxy_stamped(port, "GET", f"http://127.0.0.1:{upstream}/hello"),
            via_proxy_stamped(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}"),
        ]

    seen, replies = run_engine(tmp_path, monkeypatch, maps, calls)
    assert [stamped for _, _, stamped in replies] == [None, "fake-L0"]
    assert stamping_violations(seen) == []


def test_a_replay_stamps_what_it_answered_and_leaves_a_live_forward_bare(
    tmp_path, monkeypatch, upstream
):
    """Stamping in a replay: a read answered from the recording carries `recording`, the write
    answered with its recorded answer carries its recorded level, and the read the recording had
    no second answer for goes live and carries nothing. Nothing in these requests is a secret, so
    the recording is the run's own exchanges, as the store would have written them."""
    maps = _maps(tmp_path, monkeypatch)

    def calls(port):
        return [
            via_proxy_stamped(port, "GET", f"http://127.0.0.1:{upstream}/hello"),
            via_proxy_stamped(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}"),
        ]

    recorded, _ = run_engine(tmp_path, monkeypatch, maps, calls)
    upstream.seen.clear()
    policy = ReplayPolicy(ReplayIndex.from_events(recorded, b"k" * 32, maps), maps)
    seen, replies = run_engine(
        tmp_path,
        monkeypatch,
        maps,
        lambda port: [
            *calls(port),
            via_proxy_stamped(port, "GET", f"http://127.0.0.1:{upstream}/hello"),
        ],
        policy=policy,
    )
    assert [stamped for _, _, stamped in replies] == ["recording", "fake-L0", None]
    assert [ex.answered_by for ex in seen] == ["recording", "fake-L0", "live"]
    assert stamping_violations(seen) == []
    assert upstream.seen == [("GET", "/hello")]


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
        return [via_proxy_stamped(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")]

    seen, ((status, data, stamped),) = run_engine(
        tmp_path, monkeypatch, maps, calls, policy=_BrokenPolicy()
    )
    assert status == 502 and json.loads(data)["error"]["type"] == "irimi_decision_failed"
    assert (seen[0].answered_by, stamped) == ("irimi", "irimi")
    assert stamping_violations(seen) == []
    assert upstream.seen == [], "the write reached the real upstream"


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
    """Stamping for every error irimi answers with itself: the request hook's refusals and a target
    it cannot reach. Each carries `Irimi-Answered-By: irimi` on the wire, has the one JSON error
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
        return [via_proxy_stamped(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")]

    seen, ((got, data, stamped),) = run_engine(tmp_path, monkeypatch, maps, calls)
    assert (got, stamped) == (status, "irimi")
    error = json.loads(data)["error"]
    assert set(error) == {"type", "message"} and error["type"].startswith("irimi_")
    (ex,) = seen
    assert ex.answered_by == "irimi" and flag in ex.flags
    assert ex.response is not None and ex.response.status == status
    assert stamping_violations(seen) == []
    assert upstream.seen == [], "the request reached the real upstream"


def test_a_policy_that_answers_locally_without_saying_so_is_caught(tmp_path, monkeypatch, upstream):
    """The deliberately broken fourth policy. It synthesizes an answer and stamps it `live`, so
    the agent's write never leaves the machine while the exchange claims the real service did it.
    The engine records `live`, `stamp` therefore adds no header, and the upstream saw nothing -
    the three facts the stamping rule exists to make contradictory."""

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
        return [via_proxy_stamped(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")]

    seen, replies = run_engine(tmp_path, monkeypatch, maps, calls, policy=_LiarPolicy())
    (ex,) = seen
    assert ex.answered_by == "live"
    assert upstream.seen == [], "the write never reached the upstream, so `live` is a lie"
    assert replies[0][2] is None, "and it carries no header to give that away"
    assert reached_upstream_violations(seen, upstream.seen) != []


def test_a_header_that_names_the_wrong_answer_is_caught():
    forged = demo_exchange("live", headers=((ANSWERED_BY_HEADER, "fake-L0"),))
    assert stamping_violations([forged]) != []


def test_every_answer_shadow_policy_gives_is_stamped_for_who_gave_it(tmp_path, monkeypatch):
    """Stamping over every kind and every answer `ShadowPolicy` can give."""
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


# ------------------------------------------ a target's answer never reached the addressed host


def test_a_delegated_exchange_never_reaches_the_host_the_agent_addressed(
    tmp_path, monkeypatch, upstream, stub
):
    """The agent addressed the upstream; the target answered. The upstream must have no
    record of it at all - a delegated write that also reached production is the failure this
    whole feature exists to prevent."""
    maps = _maps(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{stub}/w")]
    )

    def calls(port):
        return [via_proxy_stamped(port, "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")]

    seen, replies = run_engine(tmp_path, monkeypatch, maps, calls)
    (ex,) = seen
    assert ex.answered_by == "delegated"
    assert ex.request.host == "127.0.0.1" and ex.request.path == "/things"
    assert stub.seen == [("POST", "/w")]
    assert upstream.seen == [], "the delegated write reached the real upstream"
    assert reached_upstream_violations(seen, upstream.seen) == []
    assert replies[0][2] == "delegated"


@pytest.fixture
def stream_stub():
    port = serve(_StreamTarget)
    yield port
    port.stop()


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

    seen, (stamped, body) = run_engine(tmp_path, monkeypatch, maps, calls)
    assert stream_stub.seen == [("POST", "/w")]
    assert body == b"data: one\n\ndata: two\n\n"
    assert stamped == "delegated"
    (ex,) = seen
    assert ex.answered_by == "delegated"
