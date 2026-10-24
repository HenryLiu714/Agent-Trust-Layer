"""`ReplayPolicy` through a real engine: record a run under shadow, then replay it against stubs.

The test maps put the Stripe stub on `127.0.0.1` and the LLM stub on `localhost`."""

import asyncio
import dataclasses
import http.client
import json
import threading
from typing import cast

import pytest

from irimi import servicemaps
from irimi.config import ca, paths
from irimi.core.exchange import Exchange, Flag, Request, Response
from irimi.core.netaddr import Target
from irimi.decide.overlay import ServiceOverlay
from irimi.decide.policy import Decided, Delegate, Forward, Respond, ShadowPolicy
from irimi.decide.reader import UpstreamReader
from irimi.decide.targets import ForwardTo
from irimi.decide.writelog import Translation
from irimi.engine import EngineConfig
from irimi.engine.ingress import Admitted
from irimi.engine.mitm import IrimiAddon, MitmEngine
from irimi.protocol.headers import ANSWERED_BY_HEADER
from irimi.record import redact
from irimi.record.store.reader import StoreReader
from irimi.record.store.writer import DirectoryStore
from irimi.replay.index import ReplayIndex
from irimi.replay.policy import ReplayPolicy
from irimi.servicemaps import classify
from tests.support.builders import FORM_CONTENT_TYPE
from tests.support.credentials import OTHER_PROMPT_SECRET, PROMPT_SECRET
from tests.support.doubles import NullStore
from tests.support.invariants import stamping_violations
from tests.support.maps import REPLAY_LLM_MAP, REPLAY_STRIPE_MAP
from tests.support.upstreams import REPLAY_CHARGE, Answer


@pytest.fixture
def home(tmp_path, monkeypatch):
    """Return an empty `$IRIMI_HOME` for the CA, redaction key and store."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "home"))
    return tmp_path / "home"


@pytest.fixture
def maps(tmp_path, home):
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir()
    (maps_dir / "stripe.yaml").write_text(REPLAY_STRIPE_MAP)
    (maps_dir / "anthropic.yaml").write_text(REPLAY_LLM_MAP)
    return servicemaps.load(cwd=tmp_path, maps_dir=maps_dir)


class _Session:
    """One engine over the test maps, answering by `policy` and recording under `home`.

    `call` sends one request through it; `stop` stops it and closes its store."""

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
        self.seen: list[Exchange] = []
        self.engine = MitmEngine(
            cfg,
            policy,
            DirectoryStore(home / "store", redact.load_key(home)),
            ServiceOverlay(),
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
        self.engine.store.close()


def _record_run(home, maps, script):
    """Run `script(session)` under shadow mode, as the run `rec`, and return what it stored."""
    session = _Session(home, maps, ShadowPolicy(reader=UpstreamReader(), maps=maps), "rec")
    try:
        script(session)
    finally:
        session.stop()
    return StoreReader(home / "store").load_run("rec").events


def _replay(stubs, home, maps, events, script, run_id="rep"):
    """Run `script(session)` as `run_id`, a replay of `events`; return the stopped session.

    Both stubs forget what they saw first."""
    stubs.forget()
    recording = ReplayIndex.from_events(events, redact.load_key(home), maps)
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


def _post_refund(session, stripe, amount=100, key="k-1"):
    body = f"charge=ch_REAL1&amount={amount}".encode()
    headers = {"content-type": FORM_CONTENT_TYPE, "idempotency-key": key}
    return session.call("POST", f"http://127.0.0.1:{stripe}/v1/refunds", body, headers)


def _charge(session, stripe):
    return session.call("GET", f"http://127.0.0.1:{stripe}/v1/charges/ch_REAL1")


# --------------------------------------------------------------------------------------- llm


def test_an_llm_call_that_differs_only_in_who_asked_is_answered_from_the_recording(
    home, maps, replay_stubs
):
    """An LLM call differing only in `metadata` and `stream_options` is served from the recording.

    It is served byte for byte, stamped `recording`, and the model is never asked."""
    _, llm = replay_stubs
    url, body = _message(llm)
    recorded = []
    events = _record_run(home, maps, lambda s: recorded.append(s.call("POST", url, body)))
    other = json.loads(body) | {
        "metadata": {"user_id": "someone-else"},
        "stream_options": {"include_usage": True},
    }
    replies = []
    session = _replay(
        replay_stubs, home, maps, events, lambda s: replies.append(s.call("POST", url, _j(other)))
    )
    (status, headers, data), (ex,) = replies[0], session.seen
    assert (status, data) == (recorded[0][0], recorded[0][2])
    assert headers[ANSWERED_BY_HEADER] == "recording"
    assert ex.answered_by == "recording"
    assert replay_stubs.llm.seen == []


def _j(document):
    return json.dumps(document).encode()


def test_a_cache_control_block_anywhere_does_not_change_the_match(home, maps, replay_stubs):
    """A `cache_control` hint at any depth is ignored when matching against the recording."""
    _, llm = replay_stubs
    url = f"http://localhost:{llm}/v1/messages"
    block = {"type": "text", "text": "Be brief."}
    recorded_body = {
        "model": "m",
        "system": [block],
        "messages": [{"role": "user", "content": "hi"}],
    }
    events = _record_run(home, maps, lambda s: s.call("POST", url, _j(recorded_body)))
    cached = recorded_body | {"system": [block | {"cache_control": {"type": "ephemeral"}}]}
    session = _replay(replay_stubs, home, maps, events, lambda s: s.call("POST", url, _j(cached)))
    assert [ex.answered_by for ex in session.seen] == ["recording"]
    assert replay_stubs.llm.seen == []


def test_a_changed_system_prompt_is_a_miss_that_asks_the_model(home, maps, replay_stubs):
    _, llm = replay_stubs
    url, body = _message(llm)
    events = _record_run(home, maps, lambda s: s.call("POST", url, body))
    _, changed = _message(llm, system="Be thorough.")
    session = _replay(replay_stubs, home, maps, events, lambda s: s.call("POST", url, changed))
    (ex,) = session.seen
    assert (ex.answered_by, Flag.REPLAY_MISS in ex.flags) == ("live", True)
    assert [m["system"] for m in replay_stubs.llm.seen] == ["Be thorough."]


def test_a_recorded_stream_answers_a_streaming_call_and_never_a_plain_one(home, maps, replay_stubs):
    """A recorded stream answers a streaming call; the same prompt unstreamed is a miss."""
    _, llm = replay_stubs
    url, streaming = _message(llm, stream=True)
    recorded: dict = {}
    events = _record_run(
        home, maps, lambda s: recorded.setdefault("r", s.call("POST", url, streaming))
    )
    assert recorded["r"][2].count(b"data: ") == 3

    replies: list = []
    session = _replay(
        replay_stubs, home, maps, events, lambda s: replies.append(s.call("POST", url, streaming))
    )
    (status, headers, data), (ex,) = replies[0], session.seen
    assert (status, data) == (200, recorded["r"][2])
    assert headers["content-type"] == "text/event-stream"
    assert ex.answered_by == "recording"
    assert replay_stubs.llm.seen == []

    _, plain = _message(llm)
    session = _replay(replay_stubs, home, maps, events, lambda s: s.call("POST", url, plain))
    (ex,) = session.seen
    assert (ex.answered_by, Flag.REPLAY_MISS in ex.flags) == ("live", True)
    assert len(replay_stubs.llm.seen) == 1


def test_a_secret_in_a_request_body_matches_its_placeholder_in_the_recording(
    home, maps, replay_stubs
):
    """A live call is redacted before matching, so the same secret matches and another does not."""
    _, llm = replay_stubs
    url, body = _message(llm, system=f"The key is {PROMPT_SECRET}.")
    events = _record_run(home, maps, lambda s: s.call("POST", url, body))
    stored = next(ex for ex in events if ex.kind == "llm")
    assert PROMPT_SECRET.encode() not in stored.request.body
    session = _replay(replay_stubs, home, maps, events, lambda s: s.call("POST", url, body))
    assert [ex.answered_by for ex in session.seen] == ["recording"]
    _, other = _message(llm, system=f"The key is {OTHER_PROMPT_SECRET}.")
    session = _replay(replay_stubs, home, maps, events, lambda s: s.call("POST", url, other))
    assert [ex.answered_by for ex in session.seen] == ["live"]


# ------------------------------------------------------------------------------------- reads


def test_a_read_is_answered_once_per_time_it_was_recorded(home, maps, replay_stubs):
    """Each recorded read answers once: two recorded reads answer two, and the third goes live."""
    stripe, _ = replay_stubs
    events = _record_run(home, maps, lambda s: [_charge(s, stripe) for _ in range(2)])
    replies: list = []
    session = _replay(
        replay_stubs,
        home,
        maps,
        events,
        lambda s: replies.extend(_charge(s, stripe) for _ in range(3)),
    )
    assert [ex.answered_by for ex in session.seen] == ["recording", "recording", "live"]
    assert Flag.REPLAY_MISS in session.seen[2].flags
    assert [r[1].get(ANSWERED_BY_HEADER) for r in replies] == [
        "recording",
        "recording",
        None,
    ]
    assert replay_stubs.stripe.seen == [("GET", "/v1/charges/ch_REAL1")]


# ------------------------------------------------------------------------------------ writes


def _refund_then_read_it(stripe, amount=100, key="k-1"):
    def script(session):
        status, _, data = _post_refund(session, stripe, amount, key)
        assert status == 200
        minted = json.loads(data)["id"]
        session.call("GET", f"http://127.0.0.1:{stripe}/v1/refunds/{minted}")

    return script


def test_the_same_write_is_answered_with_the_recorded_answer(home, maps, replay_stubs):
    """The same write with another `Idempotency-Key` gets the recorded answer, check and currency.

    The read of the id it minted is answered from the recording too."""
    stripe, _ = replay_stubs
    events = _record_run(home, maps, _refund_then_read_it(stripe))
    recorded = next(ex for ex in events if ex.kind == "write")
    session = _replay(replay_stubs, home, maps, events, _refund_then_read_it(stripe, key="k-other"))
    write, read = session.seen
    assert write.response.body == recorded.response.body
    assert (
        (write.answered_by, write.precondition.outcome, write.precondition.currency)
        == (
            recorded.answered_by,
            recorded.precondition.outcome,
            recorded.precondition.currency,
        )
        == ("fake-L1", "passed", "usd")
    )
    assert write.flags == (Flag.FROM_RECORDING,)
    assert write.would_fire == ("refund.created",)
    # Stripe has never heard of the minted id, so the recorded read is the overlay's answer.
    assert (read.answered_by, read.flags, read.overlay) == (
        "overlay",
        (Flag.FROM_RECORDING,),
        "full",
    )
    assert replay_stubs.stripe.seen == []
    assert stamping_violations(session.seen) == []


def test_a_different_write_is_faked_and_checked_against_the_recorded_read(home, maps, replay_stubs):
    """A changed write is faked and checked against the recording's read, never live state."""
    stripe, _ = replay_stubs
    events = _record_run(home, maps, lambda s: _post_refund(s, stripe, 100))
    session = _replay(replay_stubs, home, maps, events, lambda s: _post_refund(s, stripe, 250))
    engine_read, write = session.seen
    assert (write.answered_by, write.precondition.outcome) == ("fake-L1", "passed")
    assert Flag.REPLAY_MISS in write.flags
    assert (engine_read.issued_by, engine_read.answered_by) == ("engine", "recording")
    assert replay_stubs.stripe.seen == []


def test_a_write_the_recording_would_reject_is_rejected(home, maps, replay_stubs):
    stripe, _ = replay_stubs
    events = _record_run(home, maps, lambda s: _post_refund(s, stripe, 100))
    replies = []
    session = _replay(
        replay_stubs, home, maps, events, lambda s: replies.append(_post_refund(s, stripe, 9900))
    )
    assert replies[0][0] == 400
    write = session.seen[-1]
    assert (write.precondition.outcome, Flag.REPLAY_MISS in write.flags) == ("rejected", True)
    assert replay_stubs.stripe.seen == []


def test_a_missed_write_with_no_recorded_read_is_not_evaluable(home, maps, replay_stubs):
    """A missed write with no recorded read is not evaluable, and the replay makes no read."""
    stripe, llm = replay_stubs
    url, body = _message(llm)
    events = _record_run(home, maps, lambda s: s.call("POST", url, body))
    session = _replay(replay_stubs, home, maps, events, lambda s: _post_refund(s, stripe, 100))
    engine_read, write = session.seen
    assert (engine_read.answered_by, engine_read.response) == ("recording", None)
    assert engine_read.flags == (Flag.REPLAY_MISS,)
    assert write.precondition.outcome == "not_evaluable"
    assert replay_stubs.stripe.seen == []


def test_a_recorded_read_that_failed_is_served_again_to_the_precondition(home, maps, replay_stubs):
    """A missed write is checked against the recorded 500, not the stub's 200: not evaluable."""
    stripe, _ = replay_stubs
    replay_stubs.stripe.answers["/v1/charges/ch_REAL1"] = Answer(500, REPLAY_CHARGE)
    events = _record_run(home, maps, lambda s: _post_refund(s, stripe, 100))
    del replay_stubs.stripe.answers["/v1/charges/ch_REAL1"]
    session = _replay(replay_stubs, home, maps, events, lambda s: _post_refund(s, stripe, 250))
    engine_read, write = session.seen
    assert (engine_read.answered_by, engine_read.response.status) == ("recording", 500)
    assert write.precondition.outcome == "not_evaluable"
    assert replay_stubs.stripe.seen == []


def test_after_a_write_misses_a_recorded_read_goes_live_and_shows_the_new_write(
    home, maps, replay_stubs
):
    """After a write misses, the recorded read goes live and the overlay applies the new write.

    The recorded read shows the recorded refund, which this run did not make."""
    stripe, _ = replay_stubs

    def script(amount):
        def run(session):
            _post_refund(session, stripe, amount)
            _charge(session, stripe)

        return run

    events = _record_run(home, maps, script(100))
    replies = []
    session = _replay(
        replay_stubs,
        home,
        maps,
        events,
        lambda s: (_post_refund(s, stripe, 250), replies.append(_charge(s, stripe))),  # type: ignore[func-returns-value]
    )
    read = session.seen[-1]
    assert (read.answered_by, Flag.REPLAY_MISS in read.flags) == ("overlay", True)
    assert json.loads(replies[0][2])["amount_refunded"] == 250
    assert replay_stubs.stripe.seen == [("GET", "/v1/charges/ch_REAL1")]


def test_a_miss_whose_upstream_then_fails_still_says_it_missed(home, maps, replay_stubs):
    """A miss whose upstream fails keeps `replay-miss` beside `upstream-error`.

    A read whose recorded upstream failed has no answer to serve, so it is forwarded."""
    stripe, _ = replay_stubs
    url = f"http://127.0.0.1:{stripe}/v1/charges/ch_RESET"
    events = _record_run(home, maps, lambda s: s.call("GET", url))
    (recorded,) = events
    assert (recorded.answered_by, recorded.flags) == ("live", (Flag.UPSTREAM_ERROR,))
    session = _replay(replay_stubs, home, maps, events, lambda s: s.call("GET", url))
    (read,) = session.seen
    assert (read.answered_by, read.response) == ("live", None)
    assert read.flags == (Flag.REPLAY_MISS, Flag.UPSTREAM_ERROR)
    assert replay_stubs.stripe.seen == [("GET", "/v1/charges/ch_RESET")]


def test_a_matcher_that_raises_decides_a_miss_and_still_never_forwards_a_write(
    home, maps, replay_stubs, monkeypatch
):
    stripe, _ = replay_stubs
    events = _record_run(home, maps, lambda s: _post_refund(s, stripe, 100))

    def broken(*args, **kwargs):
        raise RuntimeError("the matcher broke")

    monkeypatch.setattr(ReplayIndex, "answer_for", broken)
    session = _replay(replay_stubs, home, maps, events, lambda s: _post_refund(s, stripe, 100))
    write = session.seen[-1]
    assert (write.answered_by, Flag.REPLAY_MISS in write.flags) == ("fake-L1", True)
    assert ("POST", "/v1/refunds") not in replay_stubs.stripe.seen


def test_a_replay_stamps_every_answer_it_decided(home, maps, replay_stubs):
    """The answered-by header holds in a replay, for recorded answers, faked writes and forwards."""
    stripe, llm = replay_stubs
    url, body = _message(llm)

    def script(session):
        session.call("POST", url, body)
        _charge(session, stripe)
        _post_refund(session, stripe, 100)

    events = _record_run(home, maps, script)

    def changed(session):
        session.call("POST", url, body)
        _charge(session, stripe)
        _post_refund(session, stripe, 250)
        _charge(session, stripe)

    session = _replay(replay_stubs, home, maps, events, changed)
    assert stamping_violations(session.seen) == []
    assert sorted({ex.answered_by for ex in session.seen}) == ["fake-L1", "overlay", "recording"]


def test_an_answer_the_engine_cannot_send_fails_closed_and_forwards_nothing(
    home, maps, replay_stubs
):
    """An answer whose headers cannot encode as latin-1 gets irimi's 502 and forwards nothing.

    `ReplayIndex.answer_for` refuses such a recording; this is the engine's own guard behind it."""
    stripe, _ = replay_stubs

    class _Unsendable:
        name = "unsendable"

        def decide(self, request, classification, write_log=(), run_id="", target=None):
            body = Response(200, (("x-note", "€"),), b"{}")
            return Decided(Respond(body, "fake-L0"))

    session = _Session(home, maps, _Unsendable(), "rep")
    try:
        status, headers, _ = _post_refund(session, stripe, 100)
    finally:
        session.stop()
    assert (status, headers[ANSWERED_BY_HEADER]) == (502, "irimi")
    (ex,) = session.seen
    assert Flag.DECISION_FAILED in ex.flags
    assert replay_stubs.stripe.seen == []


def test_the_reads_an_answer_issued_are_recorded_when_it_cannot_be_sent(home, maps, replay_stubs):
    """A check read is still recorded, after the refusal, when its decision cannot be sent."""
    stripe, _ = replay_stubs
    url = f"http://127.0.0.1:{stripe}/v1/charges/ch_REAL1"
    probe = Request("GET", "http", "127.0.0.1", stripe, "/v1/charges/ch_REAL1", "", (), b"")
    cls = classify.classify(probe, maps)
    read = Exchange(
        request=probe,
        response=Response(200, (), b"{}"),
        service=cls.service,
        operation=cls.operation,
        kind=cls.kind,
        answered_by="live",
        run_id="rep",
        flags=cls.flags,
        issued_by="engine",
    )

    class _Unsendable:
        name = "unsendable"

        def decide(self, request, classification, write_log=(), run_id="", target=None):
            body = Response(200, (("x-note", "€"),), b"{}")
            return Decided(Respond(body, "fake-L0"), issued=(read,))

    session = _Session(home, maps, _Unsendable(), "rep")
    try:
        status, _, _ = _post_refund(session, stripe, 100)
    finally:
        session.stop()
    assert status == 502
    refusal, issued = session.seen
    assert Flag.DECISION_FAILED in refusal.flags
    assert (issued.issued_by, issued.request.url) == ("engine", url)
    events = StoreReader(home / "store").load_run("rep").events
    assert [cast(Exchange, e).issued_by for e in events] == ["agent", "engine"]


def test_a_read_the_overlay_translates_is_recorded_translated_whoever_answers_it(
    home, maps, replay_stubs
):
    """A translated read is recorded translated, and found so by a replay and a replay of that.

    Neither replay sends the stub anything."""
    stripe, _ = replay_stubs

    def script(session):
        _, _, data = _post_refund(session, stripe)
        minted = json.loads(data)["id"]
        session.call("GET", f"http://127.0.0.1:{stripe}/v1/refunds?starting_after={minted}")

    events = _record_run(home, maps, script)
    [recorded] = [e for e in events if e.kind == "read" and e.request.path == "/v1/refunds"]
    assert "starting_after" not in recorded.request.query
    session = _replay(replay_stubs, home, maps, events, script)
    page = session.seen[-1]
    assert Flag.FROM_RECORDING in page.flags
    assert page.request.query == recorded.request.query
    again = StoreReader(home / "store").load_run("rep").events
    session = _replay(replay_stubs, home, maps, again, script, run_id="rep2")
    assert Flag.FROM_RECORDING in session.seen[-1].flags
    assert replay_stubs.stripe.seen == []


class _Asked:
    """A policy that writes down each request it is asked, and answers with `answer`."""

    name = "asked"

    def __init__(self, answer):
        self.answer_with, self.asked = answer, []

    def decide(self, request, classification, write_log=(), run_id="", target=None):
        self.asked.append(request)
        return self.answer_with


class _Translating:
    """An overlay whose request side marks every read it is asked to translate."""

    def __init__(self):
        self.translated = []

    def __call__(
        self, write_log, classification, read_request, upstream_response, translated_from=None
    ):
        raise AssertionError("not asked here")

    def rewrite(self, write_log, classification, read_request):
        self.translated.append(read_request)
        return Translation(dataclasses.replace(read_request, query="translated=1"), "x=1")


def _decided(home, maps, policy, overlay, host="127.0.0.1"):
    """`ShadowSession.plan` over one read, with one write already in the log."""
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
    unknown = classify.unclassified(read)
    write = Exchange(
        request=read,
        response=None,
        service=unknown.service,
        operation=unknown.operation,
        kind=unknown.kind,
        answered_by="fake-L0",
        run_id="t",
        flags=unknown.flags,
    )
    addon.session.write_log.add(write)
    return addon.session.plan(Admitted(read, "t", "forward", ("127.0.0.1", 4000)))


def _carries_a_translation(plan):
    return isinstance(plan.decision, Forward) and plan.decision.translation is not None


@pytest.mark.parametrize(
    ("answer", "on_the_flow"),
    [
        (Decided(Forward()), True),
        (Decided(Respond(Response(200, (), b"{}"), "recording")), False),
    ],
    ids=["forwarded", "answered-from-the-recording"],
)
def test_the_engine_asks_and_records_a_read_translated_and_edits_only_a_forwarded_flow(
    home, maps, answer, on_the_flow
):
    """The policy sees and the trace records a read translated; only a forward edits the flow."""
    policy, overlay = _Asked(answer), _Translating()
    decision = _decided(home, maps, policy, overlay)
    assert [r.query for r in policy.asked] == ["translated=1"]
    assert decision.request.query == "translated=1"
    assert _carries_a_translation(decision) is on_the_flow


def test_the_engine_never_translates_a_delegated_read(home, tmp_path, refused_port):
    """A read sent to a target is never translated: the target owns that service's state."""
    maps_dir = tmp_path / "targeted"
    maps_dir.mkdir()
    (maps_dir / "stripe.yaml").write_text(REPLAY_STRIPE_MAP)
    targeted = servicemaps.load(
        cwd=tmp_path,
        maps_dir=maps_dir,
        targets=[("127.0.0.1", "", f"http://127.0.0.1:{refused_port}")],
        target_reads=["127.0.0.1"],
    )
    delegated = Decided(Delegate(ForwardTo(Target.parse(f"http://127.0.0.1:{refused_port}"))))
    policy, overlay = _Asked(delegated), _Translating()
    decision = _decided(home, targeted, policy, overlay)
    assert overlay.translated == []
    assert decision.request.query == "starting_after=re_1"
    assert not _carries_a_translation(decision)


def test_a_read_recorded_after_a_write_the_replay_did_not_make_goes_live(home, maps, replay_stubs):
    """A replay that skips the recorded refund is not shown it: the read after it goes live."""
    stripe, _ = replay_stubs

    def refund_then_read(session):
        _post_refund(session, stripe, 100)
        _charge(session, stripe)

    events = _record_run(home, maps, refund_then_read)
    replies = []
    session = _replay(
        replay_stubs, home, maps, events, lambda s: replies.append(_charge(s, stripe))
    )
    (read,) = session.seen
    assert (read.answered_by, read.flags) == ("live", (Flag.REPLAY_MISS,))
    assert json.loads(replies[0][2])["amount_refunded"] == 0
    assert replay_stubs.stripe.seen == [("GET", "/v1/charges/ch_REAL1")]


def test_an_unchanged_replay_compares_the_same_as_its_recording(home, maps, replay_stubs):
    """An unchanged replay prints every write and `↳` line as its recording did, all rows `=`."""
    from irimi.core import exitcodes
    from irimi.replay import compare
    from irimi.report.model import RunView
    from irimi.report.summary import summary_lines

    stripe, llm = replay_stubs
    url, body = _message(llm)

    def script(session):
        session.call("POST", url, body)
        _charge(session, stripe)
        _, _, data = _post_refund(session, stripe, 100)
        _charge(session, stripe)
        minted = json.loads(data)["id"]
        session.call("GET", f"http://127.0.0.1:{stripe}/v1/refunds?starting_after={minted}")

    events = _record_run(home, maps, script)
    session = _replay(replay_stubs, home, maps, events, script)
    assert all(Flag.FROM_RECORDING in ex.flags for ex in session.seen)
    assert (replay_stubs.stripe.seen, replay_stubs.llm.seen) == ([], [])
    reader = StoreReader(home / "store")
    recorded, replayed = reader.load_run("rec"), reader.load_run("rep")
    comparison = compare.compare_runs(recorded, replayed, maps)
    assert [row.mark for row in comparison.rows] == ["same"]
    assert comparison.exit_code == exitcodes.OK

    def writes(run):
        lines = summary_lines(RunView(run.record, run.events), maps)
        return [line for line in lines if line.lstrip()[:1] in ("○", "✗", "↳")]

    assert writes(replayed) == writes(recorded)
    assert any("↳" in line for line in writes(recorded))
