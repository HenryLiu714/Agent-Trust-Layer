"""`ReplayPolicy` through a real engine (#82): a run is recorded under shadow mode into a real
`DirectoryStore`, read back, and replayed against a Stripe-shaped stub and an LLM stub on loopback,
each of which writes down every request it is given, so a test can prove what the replay never
sent anywhere.

The maps are a test map's, not the shipped ones: the stubs live on loopback, and map hosts carry no
port, so the Stripe stub is `127.0.0.1` and the LLM stub is `localhost`.
"""

import asyncio
import dataclasses
import http.client
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from irimi import ca, paths, pipeline, redact, servicemap
from irimi.engine import EngineConfig
from irimi.engine.mitm import IrimiAddon, MitmEngine
from irimi.exchange import (
    DECISION_FAILED_FLAG,
    FROM_RECORDING_FLAG,
    REPLAY_MISS_FLAG,
    Request,
    Response,
)
from irimi.overlay import ServiceOverlay
from irimi.policy import Answer, ReplayPolicy, ShadowPolicy
from irimi.reader import UpstreamReader
from irimi.recording import Recording
from irimi.store import DirectoryStore, NullStore, StoreReader
from tests.test_invariants import stamping_violations

STRIPE_MAP = """
version: 1
service: stripe
hosts:
  - 127.0.0.1
routes:
  - match:
      method: GET
      path: /v1/charges/{charge}
    operation: charges.retrieve
    kind: read
    human: get charge {charge}
  - match:
      method: POST
      path: /v1/refunds
    operation: refunds.create
    kind: write
    human: refund {amount} on {charge}
    fixture: refund
    precondition: charge_refundable
    fires:
      - refund.created
    ids:
      id: re_
    volatile:
      - idempotency_key
  - match:
      method: GET
      path: /v1/refunds
    operation: refunds.list
    kind: read
    human: list refunds
  - match:
      method: GET
      path: /v1/refunds/{refund}
    operation: refunds.retrieve
    kind: read
    human: get refund {refund}
"""

LLM_MAP = """
version: 1
service: anthropic
hosts:
  - localhost
routes:
  - match:
      method: POST
      path: /v1/messages
    operation: messages.create
    kind: llm
    human: message with {model}
"""

CHARGE = {
    "id": "ch_REAL1",
    "object": "charge",
    "amount": 4900,
    "amount_refunded": 0,
    "refunded": False,
    "currency": "usd",
}
FORM = "application/x-www-form-urlencoded"


class _Stripe(BaseHTTPRequestHandler):
    """A Stripe that answers a charge, refuses every id it never minted, and 500s a POST, which a
    replay must never send it."""

    seen: list[tuple[str, str]] = []
    charge_status = 200

    def do_GET(self):
        _Stripe.seen.append(("GET", self.path))
        if self.path == "/v1/charges/ch_REAL1":
            status, document = _Stripe.charge_status, CHARGE
        elif self.path.split("?", 1)[0] == "/v1/refunds":
            status, document = 200, {"object": "list", "has_more": False, "data": []}
        else:
            status, document = 404, {"error": {"code": "resource_missing"}}
        body = json.dumps(document).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        _Stripe.seen.append(("POST", self.path))
        self.send_response(500)
        self.send_header("content-length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


class _Llm(BaseHTTPRequestHandler):
    """A model that answers with its system prompt and a counter, so two answers to two calls are
    never the same bytes. A body asking to stream gets three server-sent events."""

    seen: list[dict] = []

    def do_POST(self):
        document = json.loads(self.rfile.read(int(self.headers["content-length"])))
        _Llm.seen.append(document)
        text = f"{document.get('system', '')} #{len(_Llm.seen)}"
        self.send_response(200)
        if document.get("stream") is True:
            self.send_header("content-type", "text/event-stream")
            self.end_headers()
            for part in ("message_start", text, "message_stop"):
                self.wfile.write(f"data: {json.dumps({'part': part})}\n\n".encode())
                self.wfile.flush()
            return
        body = json.dumps({"id": "msg_1", "content": [{"type": "text", "text": text}]}).encode()
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def _serve(handler):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    return srv


@pytest.fixture
def stubs():
    _Stripe.seen, _Stripe.charge_status, _Llm.seen = [], 200, []
    stripe, llm = _serve(_Stripe), _serve(_Llm)
    yield stripe.server_address[1], llm.server_address[1]
    stripe.shutdown()
    llm.shutdown()


@pytest.fixture
def home(tmp_path, monkeypatch):
    """`$IRIMI_HOME`: the CA, the redaction key and the store. Empty when the maps load, so a
    developer's own `maps.yaml` is not merged into them."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "home"))
    return tmp_path / "home"


@pytest.fixture
def maps(tmp_path, home):
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir()
    (maps_dir / "stripe.yaml").write_text(STRIPE_MAP)
    (maps_dir / "anthropic.yaml").write_text(LLM_MAP)
    return servicemap.load(cwd=tmp_path, maps_dir=maps_dir)


class _Session:
    """One engine over the test maps, answering by `policy` and recording into the store under
    `home`. `call` sends one request through it; `stop` closes it, and its store with it."""

    def __init__(self, home, maps, policy, run_id):
        p = ca.ca_paths()
        ca.generate_ca(p)
        cfg = EngineConfig(
            run_id=run_id,
            ca=p,
            confdir=paths.mitm_dir(),
            listen_host="127.0.0.1",
            listen_port=0,
            maps=maps,
        )
        self.seen = []
        self.engine = MitmEngine(
            cfg,
            policy,
            DirectoryStore(home / "store", redact.load_key(home)),
            ServiceOverlay(maps),
            on_exchange=self.seen.append,
        )
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=lambda: self.loop.run_until_complete(self.engine.run()), daemon=True
        )
        self.thread.start()
        asyncio.run_coroutine_threadsafe(self.engine.wait_ready(), self.loop).result(timeout=15)

    def call(self, method, url, body=None, headers=None):
        """`(status, headers, body)` of one request through the proxy, in absolute form."""
        conn = http.client.HTTPConnection("127.0.0.1", self.engine.listen_port(), timeout=10)
        sent = {"host": url.split("/")[2], **(headers or {})}
        conn.request(method, url, body=body, headers=sent)
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        return resp.status, {k.lower(): v for k, v in resp.getheaders()}, data

    def stop(self):
        self.engine.shutdown()
        self.thread.join(timeout=15)
        self.loop.close()


def _record(home, maps, script):
    """Run `script(session)` under shadow mode, as the run `rec`, and return what it stored."""
    session = _Session(home, maps, ShadowPolicy(reader=UpstreamReader(), maps=maps), "rec")
    try:
        script(session)
    finally:
        session.stop()
    return StoreReader(home / "store").load_run("rec").events


def _replay(home, maps, events, script, run_id="rep"):
    """Run `script(session)` as a replay of `events`, as the run `run_id`, with every request
    either stub saw so far forgotten. Returns the session, stopped."""
    _Stripe.seen, _Llm.seen = [], []
    recording = Recording.from_events(events, redact.load_key(home), maps)
    policy = ReplayPolicy(recording, maps=maps)
    session = _Session(home, maps, policy, run_id)
    try:
        script(session)
    finally:
        session.stop()
    return session


def _message(llm, system="Be brief.", stream=False, **extra):
    body = {"model": "m", "system": system, "messages": [{"role": "user", "content": "hi"}]}
    if stream:
        body["stream"] = True
    return f"http://localhost:{llm}/v1/messages", json.dumps(body | extra).encode()


def _refund(session, stripe, amount=100, key="k-1"):
    body = f"charge=ch_REAL1&amount={amount}&idempotency_key={key}".encode()
    headers = {"content-type": FORM, "idempotency-key": key}
    return session.call("POST", f"http://127.0.0.1:{stripe}/v1/refunds", body, headers)


def _charge(session, stripe):
    return session.call("GET", f"http://127.0.0.1:{stripe}/v1/charges/ch_REAL1")


# --------------------------------------------------------------------------------------- llm


def test_an_llm_call_that_differs_only_in_who_asked_is_answered_from_the_recording(
    home, maps, stubs
):
    """`metadata` and `stream_options` say who asked and how, never what: the call is a hit,
    served byte for byte and stamped `recording`, and the model is never asked."""
    _, llm = stubs
    url, body = _message(llm)
    recorded = []
    events = _record(home, maps, lambda s: recorded.append(s.call("POST", url, body)))
    other = json.loads(body) | {
        "metadata": {"user_id": "someone-else"},
        "stream_options": {"include_usage": True},
    }
    replies = []
    session = _replay(home, maps, events, lambda s: replies.append(s.call("POST", url, _j(other))))
    (status, headers, data), (ex,) = replies[0], session.seen
    assert (status, data) == (recorded[0][0], recorded[0][2])
    assert headers[pipeline.ANSWERED_BY_HEADER] == "recording"
    assert ex.answered_by == "recording"
    assert _Llm.seen == []


def _j(document):
    return json.dumps(document).encode()


def test_a_cache_control_block_anywhere_does_not_change_the_match(home, maps, stubs):
    """A prompt-caching hint is removed at any depth, so a system prompt sent as a block with one
    matches the same block recorded without it."""
    _, llm = stubs
    url = f"http://localhost:{llm}/v1/messages"
    block = {"type": "text", "text": "Be brief."}
    recorded_body = {
        "model": "m",
        "system": [block],
        "messages": [{"role": "user", "content": "hi"}],
    }
    events = _record(home, maps, lambda s: s.call("POST", url, _j(recorded_body)))
    cached = recorded_body | {"system": [block | {"cache_control": {"type": "ephemeral"}}]}
    session = _replay(home, maps, events, lambda s: s.call("POST", url, _j(cached)))
    assert [ex.answered_by for ex in session.seen] == ["recording"]
    assert _Llm.seen == []


def test_a_changed_system_prompt_is_a_miss_that_asks_the_model(home, maps, stubs):
    _, llm = stubs
    url, body = _message(llm)
    events = _record(home, maps, lambda s: s.call("POST", url, body))
    _, changed = _message(llm, system="Be thorough.")
    session = _replay(home, maps, events, lambda s: s.call("POST", url, changed))
    (ex,) = session.seen
    assert (ex.answered_by, REPLAY_MISS_FLAG in ex.flags) == ("live", True)
    assert [m["system"] for m in _Llm.seen] == ["Be thorough."]


def test_a_recorded_stream_answers_a_streaming_call_and_never_a_plain_one(home, maps, stubs):
    """A streamed answer is served as its joined chunks to a call that asks to stream. The same
    prompt asked without streaming is a miss: it gets an answer of its own, from the model."""
    _, llm = stubs
    url, streaming = _message(llm, stream=True)
    recorded = {}
    events = _record(home, maps, lambda s: recorded.setdefault("r", s.call("POST", url, streaming)))
    assert recorded["r"][2].count(b"data: ") == 3

    replies = []
    session = _replay(home, maps, events, lambda s: replies.append(s.call("POST", url, streaming)))
    (status, headers, data), (ex,) = replies[0], session.seen
    assert (status, data) == (200, recorded["r"][2])
    assert headers["content-type"] == "text/event-stream"
    assert ex.answered_by == "recording"
    assert _Llm.seen == []

    _, plain = _message(llm)
    session = _replay(home, maps, events, lambda s: s.call("POST", url, plain))
    (ex,) = session.seen
    assert (ex.answered_by, REPLAY_MISS_FLAG in ex.flags) == ("live", True)
    assert len(_Llm.seen) == 1


def test_a_secret_in_a_request_body_matches_its_placeholder_in_the_recording(home, maps, stubs):
    """The recording holds the secret's placeholder (#69). The live call is redacted with the same
    key before it is keyed, so the same secret matches, and another does not."""
    _, llm = stubs
    url, body = _message(llm, system="The key is sk_live_REPLAYSECRET123.")
    events = _record(home, maps, lambda s: s.call("POST", url, body))
    stored = [ex for ex in events if ex.kind == "llm"][0]
    assert b"sk_live_REPLAYSECRET123" not in stored.request.body
    session = _replay(home, maps, events, lambda s: s.call("POST", url, body))
    assert [ex.answered_by for ex in session.seen] == ["recording"]
    _, other = _message(llm, system="The key is sk_live_ANOTHERSECRET456.")
    session = _replay(home, maps, events, lambda s: s.call("POST", url, other))
    assert [ex.answered_by for ex in session.seen] == ["live"]


# ------------------------------------------------------------------------------------- reads


def test_a_read_is_answered_once_per_time_it_was_recorded(home, maps, stubs):
    """Each recorded read answers once: two recorded, two answered from the recording, and the
    third reaches the stub."""
    stripe, _ = stubs
    events = _record(home, maps, lambda s: [_charge(s, stripe) for _ in range(2)])
    replies = []
    session = _replay(
        home, maps, events, lambda s: replies.extend(_charge(s, stripe) for _ in range(3))
    )
    assert [ex.answered_by for ex in session.seen] == ["recording", "recording", "live"]
    assert REPLAY_MISS_FLAG in session.seen[2].flags
    assert [r[1].get(pipeline.ANSWERED_BY_HEADER) for r in replies] == [
        "recording",
        "recording",
        None,
    ]
    assert _Stripe.seen == [("GET", "/v1/charges/ch_REAL1")]


# ------------------------------------------------------------------------------------ writes


def _refund_then_read_it(stripe, amount=100, key="k-1"):
    def script(session):
        status, _, data = _refund(session, stripe, amount, key)
        assert status == 200
        minted = json.loads(data)["id"]
        session.call("GET", f"http://127.0.0.1:{stripe}/v1/refunds/{minted}")

    return script


def test_the_same_write_is_answered_with_the_recorded_answer(home, maps, stubs):
    """A different `Idempotency-Key` header and `idempotency_key` field are not a different write.
    It is answered with the recorded body at the recorded level, with the recorded L3 outcome and
    currency, and the read of the id it minted is answered from the recording too."""
    stripe, _ = stubs
    events = _record(home, maps, _refund_then_read_it(stripe))
    recorded = [ex for ex in events if ex.kind == "write"][0]
    session = _replay(home, maps, events, _refund_then_read_it(stripe, key="k-other"))
    write, read = session.seen
    assert write.response.body == recorded.response.body
    assert (
        (write.answered_by, write.precondition, write.currency)
        == (
            recorded.answered_by,
            recorded.precondition,
            recorded.currency,
        )
        == ("fake-L1", "passed", "usd")
    )
    assert write.flags == ("fidelity:L1", FROM_RECORDING_FLAG)
    assert write.would_fire == ("refund.created",)
    # Stripe has never heard of the minted id, so the overlay answered its read when it was
    # recorded (#52): it is still the overlay's answer, from the recording.
    assert (read.answered_by, read.flags, read.overlay) == (
        "overlay",
        ("fidelity:overlay", FROM_RECORDING_FLAG),
        "full",
    )
    assert _Stripe.seen == []
    assert stamping_violations(session.seen) == []


def test_a_different_write_is_faked_and_checked_against_the_recorded_read(home, maps, stubs):
    """The amount changed, so the recording has no answer: shadow mode fakes it, never forwards it,
    and decides its precondition from the recording's own read of the charge. The stub is not
    asked for the charge: a replay never checks a precondition against live state."""
    stripe, _ = stubs
    events = _record(home, maps, lambda s: _refund(s, stripe, 100))
    session = _replay(home, maps, events, lambda s: _refund(s, stripe, 250))
    engine_read, write = session.seen
    assert (write.answered_by, write.precondition) == ("fake-L1", "passed")
    assert REPLAY_MISS_FLAG in write.flags
    assert (engine_read.issued_by, engine_read.answered_by) == ("engine", "recording")
    assert _Stripe.seen == []


def test_a_write_the_recording_would_reject_is_rejected(home, maps, stubs):
    stripe, _ = stubs
    events = _record(home, maps, lambda s: _refund(s, stripe, 100))
    replies = []
    session = _replay(home, maps, events, lambda s: replies.append(_refund(s, stripe, 9900)))
    assert replies[0][0] == 400
    write = session.seen[-1]
    assert (write.precondition, REPLAY_MISS_FLAG in write.flags) == ("rejected", True)
    assert _Stripe.seen == []


def test_a_missed_write_with_no_recorded_read_is_not_evaluable(home, maps, stubs):
    """The recording made no read of this charge, and the replay does not make one either."""
    stripe, llm = stubs
    url, body = _message(llm)
    events = _record(home, maps, lambda s: s.call("POST", url, body))
    session = _replay(home, maps, events, lambda s: _refund(s, stripe, 100))
    engine_read, write = session.seen
    assert (engine_read.answered_by, engine_read.response) == ("recording", None)
    assert engine_read.flags == ("fidelity:recording", REPLAY_MISS_FLAG)
    assert write.precondition == "not_evaluable"
    assert _Stripe.seen == []


def test_a_recorded_read_that_failed_is_served_again_to_the_precondition(home, maps, stubs):
    """The recorded run's read of the charge got a 500, so a write that misses is checked against
    that 500 and not against the stub, which now answers 200: the write stays L2, as it was."""
    stripe, _ = stubs
    _Stripe.charge_status = 500
    events = _record(home, maps, lambda s: _refund(s, stripe, 100))
    _Stripe.charge_status = 200
    session = _replay(home, maps, events, lambda s: _refund(s, stripe, 250))
    engine_read, write = session.seen
    assert (engine_read.answered_by, engine_read.response.status) == ("recording", 500)
    assert write.precondition == "not_evaluable"
    assert _Stripe.seen == []


def test_after_a_write_misses_a_recorded_read_goes_live_and_shows_the_new_write(home, maps, stubs):
    """The recorded read of the charge shows the recorded refund. Once the refund missed, that is
    not this run's world: the read goes to the stub and the overlay applies the new refund."""
    stripe, _ = stubs

    def script(amount):
        def run(session):
            _refund(session, stripe, amount)
            _charge(session, stripe)

        return run

    events = _record(home, maps, script(100))
    replies = []
    session = _replay(
        home, maps, events, lambda s: (_refund(s, stripe, 250), replies.append(_charge(s, stripe)))
    )
    read = session.seen[-1]
    assert (read.answered_by, REPLAY_MISS_FLAG in read.flags) == ("overlay", True)
    assert json.loads(replies[0][2])["amount_refunded"] == 250
    assert _Stripe.seen == [("GET", "/v1/charges/ch_REAL1")]


def test_a_matcher_that_raises_decides_a_miss_and_still_never_forwards_a_write(
    home, maps, stubs, monkeypatch
):
    stripe, _ = stubs
    events = _record(home, maps, lambda s: _refund(s, stripe, 100))

    def broken(*args, **kwargs):
        raise RuntimeError("the matcher broke")

    monkeypatch.setattr(Recording, "take", broken)
    session = _replay(home, maps, events, lambda s: _refund(s, stripe, 100))
    write = session.seen[-1]
    assert (write.answered_by, REPLAY_MISS_FLAG in write.flags) == ("fake-L1", True)
    assert ("POST", "/v1/refunds") not in _Stripe.seen


def test_a_replay_stamps_every_answer_it_decided(home, maps, stubs):
    """The stamping rule holds in a replay: an answer from the recording says `recording`, a
    replayed write its recorded level, and a live forward nothing."""
    stripe, llm = stubs
    url, body = _message(llm)

    def script(session):
        session.call("POST", url, body)
        _charge(session, stripe)
        _refund(session, stripe, 100)

    events = _record(home, maps, script)

    def changed(session):
        session.call("POST", url, body)
        _charge(session, stripe)
        _refund(session, stripe, 250)
        _charge(session, stripe)

    session = _replay(home, maps, events, changed)
    assert stamping_violations(session.seen) == []
    assert sorted({ex.answered_by for ex in session.seen}) == ["fake-L1", "overlay", "recording"]


def test_an_answer_the_engine_cannot_send_fails_closed_and_forwards_nothing(home, maps, stubs):
    """A replay answers with a response read off disk, whose headers need not encode as latin-1.
    `Recording.take` keeps such a recording from being served, and the engine's own guard is
    the second half: building the response raised out of the request hook, which forwarded the
    write (#82). It is answered as a decision that raised is."""
    stripe, _ = stubs

    class _Unsendable:
        name = "unsendable"

        def answer(self, request, classification, write_log=(), run_id=""):
            body = Response(200, (("x-note", "€"),), b"{}")
            return Answer(answered_by="fake-L0", response=body, flags=("fidelity:L0",))

    session = _Session(home, maps, _Unsendable(), "rep")
    try:
        status, headers, _ = _refund(session, stripe, 100)
    finally:
        session.stop()
    assert (status, headers[pipeline.ANSWERED_BY_HEADER]) == (502, "fake-L0")
    (ex,) = session.seen
    assert DECISION_FAILED_FLAG in ex.flags
    assert _Stripe.seen == []


def test_a_read_the_overlay_translates_is_recorded_translated_whoever_answers_it(home, maps, stubs):
    """A page whose cursor names the refund the run minted is translated before it is forwarded
    (#43), and recorded so. The engine translates a read before the policy decides (#82), so a
    replay looks the translated read up, records it translated too, and a replay of that replay
    finds it the same way. Neither replay sends the stub anything."""
    stripe, _ = stubs

    def script(session):
        _, _, data = _refund(session, stripe)
        minted = json.loads(data)["id"]
        session.call("GET", f"http://127.0.0.1:{stripe}/v1/refunds?starting_after={minted}")

    events = _record(home, maps, script)
    [recorded] = [e for e in events if e.kind == "read" and e.request.path == "/v1/refunds"]
    assert "starting_after" not in recorded.request.query
    session = _replay(home, maps, events, script)
    page = session.seen[-1]
    assert FROM_RECORDING_FLAG in page.flags
    assert page.request.query == recorded.request.query
    again = StoreReader(home / "store").load_run("rep").events
    session = _replay(home, maps, again, script, run_id="rep2")
    assert FROM_RECORDING_FLAG in session.seen[-1].flags
    assert _Stripe.seen == []


class _Asked:
    """A policy that writes down each request it is asked, and answers with `answer`."""

    name = "asked"

    def __init__(self, answer):
        self.answer_with, self.asked = answer, []

    def answer(self, request, classification, write_log=(), run_id=""):
        self.asked.append(request)
        return self.answer_with


class _Translating:
    """An overlay whose request side marks every read it is asked to translate."""

    def __init__(self):
        self.translated = []

    def __call__(self, write_log, read_request, upstream_response):
        raise AssertionError("not asked here")

    def rewrite(self, write_log, read_request):
        self.translated.append(read_request)
        return dataclasses.replace(read_request, query="translated=1")


def _decided(home, maps, policy, overlay, host="127.0.0.1"):
    """`IrimiAddon._decide` over one read, with one write already in the log."""
    config = EngineConfig(
        run_id="t",
        ca=ca.ca_paths(),
        confdir=paths.mitm_dir(),
        listen_host="127.0.0.1",
        listen_port=0,
        maps=maps,
    )
    addon = IrimiAddon(config, policy, NullStore(), overlay, None, lambda *a: None)
    read = Request("GET", "https", host, 443, "/v1/refunds", "starting_after=re_1", (), b"")
    write = pipeline.annotate(read, None, pipeline.unclassified(read), "fake-L0", "t")
    return addon._decide(read, (write,), "t", False)


@pytest.mark.parametrize(
    ("answer", "on_the_flow"),
    [
        (Answer(answered_by="live", response=None), True),
        (Answer(answered_by="recording", response=Response(200, (), b"{}")), False),
    ],
    ids=["forwarded", "answered-from-the-recording"],
)
def test_the_engine_asks_and_records_a_read_translated_and_edits_only_a_forwarded_flow(
    home, maps, answer, on_the_flow
):
    """The engine translates a read before the policy decides, so the policy is asked, and the
    trace records, the read as irimi would ask it whoever answers; only a read forwarded live
    carries the translation onto the flow (#82)."""
    policy, overlay = _Asked(answer), _Translating()
    decision = _decided(home, maps, policy, overlay)
    assert [r.query for r in policy.asked] == ["translated=1"]
    assert decision.request.query == "translated=1"
    assert (decision.rewritten is not None) is on_the_flow


def test_the_engine_never_translates_a_delegated_read(home, tmp_path):
    """A delegated read's target owns that service's state, so the overlay never touches it,
    on either side (D20, #43)."""
    maps_dir = tmp_path / "targeted"
    maps_dir.mkdir()
    (maps_dir / "stripe.yaml").write_text(STRIPE_MAP)
    targeted = servicemap.load(
        cwd=tmp_path,
        maps_dir=maps_dir,
        targets=[("127.0.0.1", "", "http://127.0.0.1:9")],
        target_reads=["127.0.0.1"],
    )
    policy, overlay = _Asked(Answer(answered_by="delegated", response=None)), _Translating()
    decision = _decided(home, targeted, policy, overlay)
    assert overlay.translated == []
    assert decision.request.query == "starting_after=re_1"
    assert decision.rewritten is None


def test_a_read_recorded_after_a_write_the_replay_did_not_make_goes_live(home, maps, stubs):
    """The recorded run refunded and then read the charge, which showed the refund. A replay that
    reads the charge without refunding - a prompt edit stopped it - is not shown a refund it never
    made: the read misses, the replay diverges, and the stub answers it (#82)."""
    stripe, _ = stubs

    def refund_then_read(session):
        _refund(session, stripe, 100)
        _charge(session, stripe)

    events = _record(home, maps, refund_then_read)
    replies = []
    session = _replay(home, maps, events, lambda s: replies.append(_charge(s, stripe)))
    (read,) = session.seen
    assert (read.answered_by, read.flags) == ("live", (REPLAY_MISS_FLAG,))
    assert json.loads(replies[0][2])["amount_refunded"] == 0
    assert _Stripe.seen == [("GET", "/v1/charges/ch_REAL1")]
