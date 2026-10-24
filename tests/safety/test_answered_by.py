"""The answered-by header: every answer irimi decided carries it, and no live forward does.

An exchange sent to a target never reaches the host the agent addressed.
"""

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
    """An upstream that logs every request it is given, for the target checks to read."""

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
    """A live read, a faked write and a write sent to a target, in one real run, all stamped right.

    A real engine builds the exchanges, so no test arranged them to pass its own check."""
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
    # Only the live read reached the upstream: the half of the rule no pure function can see.
    assert upstream.seen == [("GET", "/hello")]
    assert reached_upstream_violations(seen, upstream.seen) == []
    # The header the client received on the wire.
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
    """A replay stamps recorded answers with `recording` or their level, and a live forward bare.

    The read the recording has no second answer for goes live. Nothing here is a secret, so the
    recording is the run's own exchanges."""
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
    """A failed decision's 502 is stamped `irimi` on the wire, not only in the trace.

    `stamping_violations` stamps the recorded exchange itself, so this asserts on the header the
    client received.
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
    """Each of irimi's own errors is stamped `irimi`, has the JSON error shape and is recorded.

    That covers the request hook's refusals and an unreachable target, each flagged with why."""
    targets = []
    if case == "target-unreachable":
        targets = [("127.0.0.1", "/things", f"http://127.0.0.1:{_closed_port()}/w")]
    maps = _maps(tmp_path, monkeypatch, targets=targets)
    if case == "unparseable":
        monkeypatch.setattr(mitm, "_request_from_flow", _boom)
    elif case == "unroutable":
        monkeypatch.setattr(ingress, "detect_door", _boom)
    elif case == "unrewritable":
        monkeypatch.setattr(ingress, "rewrite_base_url", _boom)

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
    """A policy that fakes a write but labels it `live` is caught by what the upstream saw.

    The exchange says `live` and carries no header, yet the upstream heard nothing."""

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


# ------------------------------------------ a target's answer never reaches the addressed host


def test_a_delegated_exchange_never_reaches_the_host_the_agent_addressed(
    tmp_path, monkeypatch, upstream, stub
):
    """A write sent to a target leaves no trace on the host the agent addressed."""
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
    """A target's streamed answer is stamped, which only `responseheaders` can do.

    mitmproxy sends a streamed response's headers before the `response` hook runs."""
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
