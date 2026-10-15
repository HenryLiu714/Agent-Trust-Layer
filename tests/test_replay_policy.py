"""`recording.Recording` and `policy.ReplayPolicy` over hand-built stored runs (#82).

A stored run here is a list of `Exchange`s, as `StoredRun.events` holds them: the store redacts
before it writes, so a recorded body that held a secret holds its placeholder, which `_stored`
makes with the same key the live side is redacted with. The real engine is
`tests/test_replay_engine.py`'s.
"""

import json
from dataclasses import replace

import pytest

from irimi import redact, servicemap
from irimi.exchange import (
    BODY_TRUNCATED_FLAG,
    DECISION_FAILED_FLAG,
    FIDELITY_L1_FLAG,
    FIDELITY_RECORDING_FLAG,
    FIXTURE_FAILED_FLAG,
    FROM_RECORDING_FLAG,
    IDEMPOTENCY_CONFLICT_FLAG,
    IDEMPOTENT_REPLAY_FLAG,
    LIVE_KINDS,
    REDACTION_FAILED_FLAG,
    REPLAY_MISS_FLAG,
    STREAM_TRUNCATED_FLAG,
    TARGET_FAILED_FLAG,
    UNCLASSIFIED_FLAG,
    UPSTREAM_ERROR_FLAG,
    Exchange,
    Request,
    Response,
)
from irimi.pipeline import classify
from irimi.policy import ReplayPolicy
from irimi.reader import UpstreamReader, answered_as
from irimi.recording import Recording, RecordingReader
from irimi.trace import TelemetrySeen, ToolCall

SHIPPED = servicemap.MapIndex(tuple(servicemap.load_shipped()))
KEY = b"k" * 32
JSON = "application/json"
FORM = "application/x-www-form-urlencoded"
OK = Response(200, (("content-type", JSON),), b'{"id": "ch_1", "amount": 4900}')


def _req(method="GET", host="api.stripe.com", path="/v1/charges/ch_1", body=b"", ct=None, **kw):
    headers = (("content-type", ct),) if ct is not None else ()
    return Request(
        method=method,
        scheme="https",
        host=host,
        port=443,
        path=path,
        query=kw.pop("query", ""),
        headers=headers + tuple(kw.pop("headers", ())),
        body=body,
    )


def _stored(request, response=OK, **fields):
    """The exchange the store wrote for `request`: classified against the shipped maps, answered
    live when it is a live kind and at L0 when it is not, as shadow mode answers them, and redacted
    with KEY, as `DirectoryStore` redacts before it writes."""
    cls = classify(request, SHIPPED)
    kind = fields.pop("kind", cls.kind)
    ex = Exchange(
        request=request,
        response=response,
        service=cls.service,
        operation=cls.operation,
        kind=kind,
        answered_by=fields.pop("answered_by", "live" if kind in LIVE_KINDS else "fake-L0"),
        validation="unvalidated",
        run_id="rec",
        flags=cls.flags + tuple(fields.pop("flags", ())),
        **fields,
    )
    return redact.redact_exchange(ex, KEY)


def _recording(*events):
    return Recording.from_events(list(events), KEY, SHIPPED)


def _llm(content="hi", stream=False, **extra):
    body = {"model": "m", "messages": [{"role": "user", "content": content}]} | extra
    if stream:
        body["stream"] = True
    return _req("POST", "api.anthropic.com", "/v1/messages", json.dumps(body).encode(), JSON)


def _refund(amount=100, key="k-1"):
    body = f"charge=ch_1&amount={amount}&idempotency_key={key}".encode()
    return _req("POST", path="/v1/refunds", body=body, ct=FORM, headers=(("idempotency-key", key),))


REFUND_ANSWER = Response(200, (("content-type", JSON),), b'{"id": "re_RECORDED1", "amount": 100}')


def _recorded_refund(amount=100, **fields):
    defaults = {
        "answered_by": "fake-L1",
        "flags": (FIDELITY_L1_FLAG,),
        "precondition": "passed",
        "currency": "usd",
        "would_fire": ("refund.created", "charge.refunded"),
    }
    return _stored(_refund(amount), REFUND_ANSWER, **(defaults | fields))


def _answer(policy, request, write_log=(), run_id="rep"):
    return policy.answer(request, classify(request, SHIPPED), write_log, run_id)


# -------------------------------------------------------------------------------- recording


def test_each_recorded_exchange_answers_once_in_order():
    first = _stored(_req(), replace(OK, body=b'{"n": 1}'))
    second = _stored(_req(), replace(OK, body=b'{"n": 2}'))
    recording = _recording(first, second)
    taken = [recording.take(_req(), "read") for _ in range(3)]
    assert [None if t is None else t.response.body for t in taken] == [
        b'{"n": 1}',
        b'{"n": 2}',
        None,
    ]


def test_headers_and_the_port_never_decide_a_match():
    recording = _recording(_stored(_req(headers=(("user-agent", "a"),))))
    live = replace(_req(headers=(("user-agent", "b"), ("authorization", "Bearer x"))), port=8443)
    assert recording.take(live, "read") is not None


def test_the_query_is_compared_as_its_pairs_in_any_order():
    recording = _recording(_stored(_req(path="/v1/charges", query="limit=3&customer=cus_1")))
    assert recording.take(_req(path="/v1/charges", query="customer=cus_1&limit=3"), "read")
    assert recording.take(_req(path="/v1/charges", query="limit=4"), "read") is None


def test_a_volatile_field_never_decides_a_match_and_any_other_does():
    """`idempotency_key` is `volatile:` on the shipped refund route."""
    recording = _recording(_recorded_refund(), _recorded_refund())
    assert recording.take(_refund(key="k-other"), "write") is not None
    assert recording.take(_refund(amount=250), "write") is None


def test_a_form_body_and_a_json_body_with_the_same_fields_are_the_same_write():
    """Both are read with `bodies.reflect`, which reads a form's `1` as an integer."""
    recording = _recording(_stored(_req("POST", path="/v1/refunds", body=b"amount=1", ct=FORM)))
    json_body = _req("POST", path="/v1/refunds", body=b'{"amount": 1}', ct=JSON)
    assert recording.take(json_body, "write") is not None


def test_a_number_respelled_by_redaction_still_matches():
    """A JSON body a redaction rule changed is stored compactly re-serialized, `1.10` as `1.1`
    (#69). The key compares parsed JSON, never stored bytes against live bytes."""
    body = b'{"amount": 1.10, "password": "hunter2"}'
    stored = _stored(_req("POST", path="/v1/refunds", body=body, ct=JSON))
    assert b"1.10" not in stored.request.body
    recording = _recording(stored)
    assert recording.take(_req("POST", path="/v1/refunds", body=body, ct=JSON), "write")


def test_a_body_reflect_cannot_read_is_compared_whole():
    recording = _recording(_stored(_req("POST", path="/v1/refunds", body=b"\x00raw", ct="x/y")))
    assert (
        recording.take(_req("POST", path="/v1/refunds", body=b"\x00other", ct="x/y"), "write")
        is None
    )
    assert recording.take(_req("POST", path="/v1/refunds", body=b"\x00raw", ct="x/y"), "write")


def test_a_read_never_matches_a_recorded_write():
    recording = _recording(_stored(_req("POST", path="/v1/refunds", body=b"a=1", ct=FORM)))
    assert recording.take(_req("POST", path="/v1/refunds", body=b"a=1", ct=FORM), "read") is None


@pytest.mark.parametrize(
    "extra",
    [
        {"metadata": {"user_id": "u"}},
        {"user": "u"},
        {"stream_options": {"include_usage": True}},
        {"prompt_cache_key": "p"},
    ],
)
def test_an_llm_call_that_differs_only_in_who_asked_matches(extra):
    recording = _recording(_stored(_llm()))
    assert recording.take(_llm(**extra), "llm") is not None


def test_a_cache_control_hint_at_any_depth_never_decides_a_match():
    recording = _recording(_stored(_llm(content=[{"type": "text", "text": "hi"}])))
    hinted = [{"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}]
    assert recording.take(_llm(content=hinted), "llm") is not None


def test_an_llm_call_that_asks_something_else_is_a_miss():
    recording = _recording(_stored(_llm()))
    assert recording.take(_llm(content="bye"), "llm") is None


def test_an_llm_body_that_is_not_json_is_compared_whole():
    raw = _req("POST", "api.anthropic.com", "/v1/messages", b"not json", "text/plain")
    recording = _recording(_stored(raw))
    assert recording.take(raw, "llm") is not None


def test_a_streamed_answer_answers_only_a_call_that_asks_to_stream():
    """Whether it streamed is beside the key, so a call that does not ask leaves the streamed
    answer for one that does."""
    streamed = _stored(_llm(stream=True), Response(200, (), b"data: 1\n\n"), stream_chunks=(9,))
    recording = _recording(streamed)
    assert recording.take(_llm(), "llm") is None
    assert recording.take(_llm(stream=True), "llm") is not None


def test_an_event_stream_that_sent_no_body_still_streamed():
    empty = Response(204, (("content-type", "text/event-stream"),), b"")
    recording = _recording(_stored(_llm(stream=True), empty))
    assert recording.take(_llm(), "llm") is None
    assert recording.take(_llm(stream=True), "llm") is not None


@pytest.mark.parametrize(
    "fields",
    [
        {"flags": (BODY_TRUNCATED_FLAG,)},
        {"flags": (STREAM_TRUNCATED_FLAG,)},
        {"flags": (DECISION_FAILED_FLAG,)},
        {"flags": (TARGET_FAILED_FLAG,)},
        {"flags": (UPSTREAM_ERROR_FLAG,)},
        {"flags": (REDACTION_FAILED_FLAG,)},
        {"answered_by": "delegated"},
    ],
)
def test_a_recorded_answer_that_is_not_whole_is_a_miss_in_its_place(fields):
    """It is still the agent's first such request: the request in its place misses, and the next
    one meets the second recording."""
    recording = _recording(_stored(_req(), **fields), _stored(_req()))
    assert recording.take(_req(), "read") is None
    assert recording.take(_req(), "read") is not None


def test_a_recorded_request_that_got_no_answer_is_a_miss_in_its_place():
    recording = _recording(_stored(_req(), None, flags=(UPSTREAM_ERROR_FLAG,)), _stored(_req()))
    assert recording.take(_req(), "read") is None
    assert recording.take(_req(), "read") is not None


def test_a_secret_matches_the_placeholder_the_recording_holds():
    secret = _llm(content="the key is sk_live_ABCDEF123456")
    stored = _stored(secret)
    assert b"sk_live_ABCDEF123456" not in stored.request.body
    recording = _recording(stored)
    assert recording.take(_llm(content="the key is sk_live_OTHER9876"), "llm") is None
    assert recording.take(secret, "llm") is not None


def test_engine_reads_answer_only_the_precondition_and_are_never_used_up():
    engine = _stored(_req(), issued_by="engine")
    recording = _recording(engine)
    assert recording.take(_req(), "read") is None
    reader = RecordingReader(recording)
    assert reader(_req()) == engine.response
    assert reader(_req()) == engine.response
    assert reader(_req(path="/v1/charges/ch_2")) is None


def test_an_agent_read_never_answers_a_precondition():
    assert RecordingReader(_recording(_stored(_req())))(_req()) is None


def test_telemetry_and_tool_calls_are_not_recorded_answers():
    tool = ToolCall("t1", "rec", "tool", "read", "real", {}, {}, None, 0.0, 0.0)
    recording = _recording(TelemetrySeen("rec", "api.datadoghq.com", 0.0), tool)
    assert recording.take(_req(), "read") is None


# ----------------------------------------------------------------------------------- policy


def test_a_recorded_read_is_served_as_it_was_recorded():
    recorded = Response(
        200,
        (
            ("content-type", JSON),
            ("content-encoding", "gzip"),
            ("content-length", "999"),
            ("transfer-encoding", "chunked"),
            ("irimi-answered-by", "overlay"),
            ("request-id", "req_1"),
        ),
        b'{"amount_refunded": 100}',
    )
    stored = _stored(
        _req(), recorded, answered_by="overlay", flags=("fidelity:overlay",), overlay="full"
    )
    answer = _answer(ReplayPolicy(_recording(stored), SHIPPED), _req())
    assert answer.response == Response(
        200, (("content-type", JSON), ("request-id", "req_1")), recorded.body
    )


def test_an_answer_from_the_recording_is_what_it_was_and_says_where_it_came_from():
    """A read the overlay edited is still the overlay's, its fidelity carried, so the summary
    hangs its `↳` line under the write again. One the service answered is `recording`: irimi
    answered it this time, from disk. Both carry `from-recording` (#82)."""
    edited = _stored(_req(), answered_by="overlay", flags=("fidelity:overlay",), overlay="full")
    answer = _answer(ReplayPolicy(_recording(edited), SHIPPED), _req())
    assert (answer.answered_by, answer.flags, answer.overlay) == (
        "overlay",
        ("fidelity:overlay", FROM_RECORDING_FLAG),
        "full",
    )
    answer = _answer(ReplayPolicy(_recording(_stored(_req())), SHIPPED), _req())
    assert (answer.answered_by, answer.flags, answer.overlay) == (
        "recording",
        (FIDELITY_RECORDING_FLAG, FROM_RECORDING_FLAG),
        None,
    )


def test_a_read_the_recording_lacks_goes_live_flagged_a_miss():
    answer = _answer(ReplayPolicy(_recording(), SHIPPED), _req())
    assert (answer.answered_by, answer.response, answer.flags) == (
        "live",
        None,
        (REPLAY_MISS_FLAG,),
    )


def test_telemetry_is_never_looked_up_and_never_flagged():
    request = _req("POST", "api.datadoghq.com", "/api/v2/series", b"{}", JSON)
    recording = _recording(_stored(request))
    answer = _answer(ReplayPolicy(recording, SHIPPED), request)
    assert (answer.answered_by, answer.flags) == ("live", ())


def test_a_delegated_write_is_never_looked_up_and_never_flagged():
    maps = servicemap.MapIndex(
        tuple(
            servicemap.apply_cli_targets(
                list(servicemap.load_shipped()),
                [("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000/refund")],
                (),
            )
        )
    )
    recording = Recording.from_events([_recorded_refund()], KEY, maps)
    request = _refund()
    answer = ReplayPolicy(recording, maps).answer(request, classify(request, maps))
    assert (answer.answered_by, REPLAY_MISS_FLAG in answer.flags) == ("delegated", False)


def test_the_same_write_gets_the_recorded_answer_at_its_recorded_level():
    """Its body, level, L3 outcome, currency and webhooks, and never an overlay fidelity, which
    only a read can carry, whatever a recording edited by hand says."""
    recorded = _recorded_refund(flags=(FIDELITY_L1_FLAG, FIXTURE_FAILED_FLAG, BODY_TRUNCATED_FLAG))
    recorded = replace(recorded, flags=(FIDELITY_L1_FLAG, FIXTURE_FAILED_FLAG), overlay="full")
    answer = _answer(ReplayPolicy(_recording(recorded), SHIPPED), _refund(key="k-other"))
    assert answer.answered_by == "fake-L1"
    assert answer.response.body == REFUND_ANSWER.body
    assert answer.flags == (FIDELITY_L1_FLAG, FIXTURE_FAILED_FLAG, FROM_RECORDING_FLAG)
    assert (answer.precondition, answer.currency) == ("passed", "usd")
    assert answer.would_fire == ("refund.created", "charge.refunded")
    assert answer.issued == ()
    assert answer.overlay is None


def test_a_replayed_write_carries_only_its_answers_flags():
    """The classifier's flags come back from this request's own classification, and a flag the
    engine put on the recorded exchange is a fact about that exchange."""
    request = _req("POST", path="/v1/unmapped", body=b"a=1", ct=FORM)
    recorded = _stored(
        request,
        REFUND_ANSWER,
        answered_by="fake-L0",
        flags=("fidelity:L0", FIXTURE_FAILED_FLAG, "some-later-flag"),
    )
    assert UNCLASSIFIED_FLAG in recorded.flags
    answer = _answer(ReplayPolicy(_recording(recorded), SHIPPED), request)
    assert answer.flags == ("fidelity:L0", FIXTURE_FAILED_FLAG, FROM_RECORDING_FLAG)


def test_a_rejected_write_is_rejected_again():
    refused = Response(400, (("content-type", JSON),), b'{"error": {"code": "amount_too_large"}}')
    recorded = _stored(
        _refund(),
        refused,
        answered_by="fake-L1",
        flags=(FIDELITY_L1_FLAG,),
        precondition="rejected",
        rejection_code="amount_too_large",
    )
    answer = _answer(ReplayPolicy(_recording(recorded), SHIPPED), _refund())
    assert (answer.response.status, answer.precondition, answer.rejection_code) == (
        400,
        "rejected",
        "amount_too_large",
    )


def test_a_write_the_recording_lacks_is_faked_checked_against_the_recording_and_flagged():
    engine = _stored(
        _req(),
        Response(
            200,
            (("content-type", JSON),),
            json.dumps(
                {
                    "id": "ch_1",
                    "object": "charge",
                    "amount": 4900,
                    "amount_refunded": 0,
                    "refunded": False,
                    "currency": "usd",
                }
            ).encode(),
        ),
        issued_by="engine",
    )
    answer = _answer(ReplayPolicy(_recording(engine, _recorded_refund()), SHIPPED), _refund(250))
    assert (answer.answered_by, answer.precondition, answer.currency) == (
        "fake-L1",
        "passed",
        "usd",
    )
    assert answer.flags[-1] == REPLAY_MISS_FLAG
    (read,) = answer.issued
    assert (read.issued_by, read.answered_by) == ("engine", "recording")
    assert read.flags == (FIDELITY_RECORDING_FLAG, FROM_RECORDING_FLAG)


def test_a_write_miss_ends_serving_reads_and_an_llm_miss_does_not():
    recording = _recording(_stored(_req()), _stored(_req()), _recorded_refund())
    policy = ReplayPolicy(recording, SHIPPED)
    assert _answer(policy, _llm(content="new")).flags == (REPLAY_MISS_FLAG,)
    assert _answer(policy, _req()).answered_by == "recording"
    assert not policy.recording.diverged
    assert _answer(policy, _refund(250)).flags[-1] == REPLAY_MISS_FLAG
    assert policy.recording.diverged
    answer = _answer(policy, _req())
    assert (answer.answered_by, answer.flags) == ("live", (REPLAY_MISS_FLAG,))


def test_a_matched_write_holds_its_idempotency_key_for_a_retry_that_misses():
    """Stripe refuses a key reused for a different write. A replay answered the first from the
    recording, so it is the replay's store that has to remember the key (#46)."""
    policy = ReplayPolicy(_recording(_recorded_refund()), SHIPPED)
    assert FROM_RECORDING_FLAG in _answer(policy, _refund(key="k-9")).flags
    answer = _answer(policy, _refund(amount=250, key="k-9"))
    assert IDEMPOTENCY_CONFLICT_FLAG in answer.flags
    assert answer.response.status == 400


def test_a_retry_of_a_matched_write_the_recording_has_no_second_answer_for_is_shadow_mode_s():
    """The retry misses, and shadow mode's store answers it with the first answer's bytes as an
    idempotent replay: the recorded answer, but not given by the recording this time."""
    policy = ReplayPolicy(_recording(_recorded_refund()), SHIPPED)
    first = _answer(policy, _refund(key="k-9"))
    retry = _answer(policy, _refund(key="k-9"))
    assert retry.response.body == first.response.body
    assert retry.flags == (FIDELITY_L1_FLAG, IDEMPOTENT_REPLAY_FLAG, REPLAY_MISS_FLAG)


def test_a_long_conversation_is_keyed_as_an_llm_call():
    """`json_body` reads a JSON body of any length, so a conversation past
    `bodies.MAX_BODY_BYTES` still loses who asked before it is compared."""
    long = "x" * 2_100_000
    recording = _recording(_stored(_llm(content=long, stream=True), stream_chunks=(31,)))
    assert recording.take(_llm(content=long, stream=True, metadata={"user_id": "u"}), "llm")


def test_a_matcher_that_raises_decides_a_miss(monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("the matcher broke")

    monkeypatch.setattr(Recording, "take", broken)
    policy = ReplayPolicy(_recording(_recorded_refund()), SHIPPED)
    answer = _answer(policy, _refund())
    assert answer.answered_by == "fake-L1"
    assert answer.response is not None
    assert answer.flags[-1] == REPLAY_MISS_FLAG
    assert _answer(policy, _req()).answered_by == "live"  # the write missed: reads go live


def test_a_write_irimi_refused_in_the_recording_is_decided_afresh():
    """A write whose body decoded past the cap was refused with irimi's own 502 and recorded with
    an empty body (#94). That refusal is not an answer to give the write again."""
    refusal = Response(
        502, (("content-type", JSON),), b'{"error": {"type": "irimi_decision_failed"}}'
    )
    recorded = _stored(
        replace(_refund(), body=b""),
        refusal,
        answered_by="fake-L0",
        flags=(DECISION_FAILED_FLAG, BODY_TRUNCATED_FLAG),
    )
    answer = _answer(ReplayPolicy(_recording(recorded), SHIPPED), replace(_refund(), body=b""))
    assert (answer.response.status, answer.flags[-1]) == (200, REPLAY_MISS_FLAG)


def test_the_policy_is_named_replay():
    assert ReplayPolicy(_recording()).name == "replay"


# ------------------------------------------------------------------------- what is servable


def test_a_recorded_header_the_engine_cannot_send_is_a_miss():
    """irimi's writer stores headers it read as latin-1, but a run from elsewhere may hold any
    text, and a header mitmproxy cannot encode would raise in the request hook (#82)."""
    odd = Response(200, (("content-type", JSON), ("x-note", "€")), b"{}")
    recording = _recording(_stored(_req(), odd), _stored(_req()))
    assert recording.take(_req(), "read") is None
    assert recording.take(_req(), "read") is not None


@pytest.mark.parametrize("status", [42, 100, 101, 600, 999])
def test_a_recorded_status_no_response_can_carry_is_a_miss(status):
    """A 1xx is never a final answer, and a 101 switched the recorded connection to a protocol a
    replay cannot carry on; past 599 is no status HTTP defines (#82)."""
    assert _recording(_stored(_req(), replace(OK, status=status))).take(_req(), "read") is None


def test_an_empty_event_stream_200_recorded_before_streams_were_kept_is_a_miss():
    """Before #71 a streamed answer was stored with an empty body and no chunks."""
    empty = Response(200, (("content-type", "text/event-stream"),), b"")
    recording = _recording(_stored(_llm(stream=True), empty))
    assert recording.take(_llm(stream=True), "llm") is None


def test_only_an_llm_call_is_bucketed_by_streaming():
    """A read whose recorded answer was an event stream is served like any read."""
    sse = Response(200, (("content-type", "text/event-stream"),), b"data: 1\n\n")
    recording = _recording(_stored(_req(path="/v1/events"), sse, stream_chunks=(9,)))
    assert recording.take(_req(path="/v1/events"), "read") is not None


def test_a_streaming_call_answered_with_an_error_is_answered_with_it_again():
    """The bucket is the recorded call's own: it asked to stream and got a JSON 429, so a call
    that asks to stream gets the 429 back, and the agent's retry path replays."""
    limited = Response(429, (("content-type", JSON),), b'{"error": "overloaded"}')
    recording = _recording(_stored(_llm(stream=True), limited))
    taken = recording.take(_llm(stream=True), "llm")
    assert taken is not None and taken.response.status == 429


def test_a_streamed_answer_is_never_served_to_a_call_that_did_not_ask_for_one():
    """A provider that streams by path rather than by body records a call that did not ask for a
    stream and got one. A live call that does not ask either is never served a stream."""
    sse = Response(200, (("content-type", "text/event-stream"),), b"data: 1\n\n")
    recording = _recording(_stored(_llm(), sse, stream_chunks=(9,)))
    assert recording.take(_llm(), "llm") is None


def test_a_precondition_read_is_the_first_the_recording_can_serve():
    failed = _stored(_req(), None, issued_by="engine", flags=(UPSTREAM_ERROR_FLAG,))
    good = _stored(_req(), issued_by="engine")
    assert RecordingReader(_recording(failed, good))(_req()) == good.response


# ----------------------------------------------------------------------- policy, after review


def test_a_replay_diverges_for_the_whole_engine():
    """A replay is one run against one engine, whose write log is the engine's: a write that
    missed is in the world every later read is shown, whichever run asks (#82). Serve mode, many
    runs at once, refuses to replay (`tests/test_cli.py`)."""
    recording = _recording(_stored(_req()), _stored(_req()))
    policy = ReplayPolicy(recording, SHIPPED)
    assert _answer(policy, _refund(250), run_id="A").flags[-1] == REPLAY_MISS_FLAG
    assert _answer(policy, _req(), run_id="B").answered_by == "live"


def test_a_recorded_write_irimi_did_not_fake_is_never_answered_again():
    """Served under its recorded `answered_by`, a write recorded `live` would reach the agent as a
    fake with no stamp, and the trace would call it live."""
    recorded = _stored(_refund(), REFUND_ANSWER, answered_by="live")
    answer = _answer(ReplayPolicy(_recording(recorded), SHIPPED), _refund())
    assert (answer.answered_by, answer.flags[-1]) == ("fake-L1", REPLAY_MISS_FLAG)


def test_a_reader_that_says_nothing_answers_live_and_the_recording_s_says_so():
    """`reader.answered_as` is the one rule for what a precondition read is recorded as: a reader
    that names nothing, as any plain callable, is live (#82)."""
    for answered in (True, False):
        assert answered_as(lambda request: None, answered) == ("live", ())
        assert answered_as(UpstreamReader(), answered) == ("live", ())
    assert answered_as(RecordingReader(_recording()), True) == (
        "recording",
        (FIDELITY_RECORDING_FLAG, FROM_RECORDING_FLAG),
    )
    assert answered_as(RecordingReader(_recording()), False) == (
        "recording",
        (FIDELITY_RECORDING_FLAG, REPLAY_MISS_FLAG),
    )


def test_a_read_recorded_after_a_write_the_replay_has_not_made_is_a_miss_that_diverges():
    """It shows that write, which this run never made (a prompt edit stopped it). The reads
    before the write are still answered from the recording."""
    shown = _stored(_req(), answered_by="overlay", flags=("fidelity:overlay",), overlay="full")
    recording = _recording(_stored(_req()), _recorded_refund(), shown)
    policy = ReplayPolicy(recording, SHIPPED)
    assert _answer(policy, _req()).answered_by == "recording"
    answer = _answer(policy, _req())
    assert (answer.answered_by, answer.flags) == ("live", (REPLAY_MISS_FLAG,))
    assert recording.diverged


def test_a_read_recorded_after_a_write_the_replay_made_is_answered():
    shown = _stored(_req(), answered_by="overlay", flags=("fidelity:overlay",), overlay="full")
    policy = ReplayPolicy(_recording(_recorded_refund(), shown), SHIPPED)
    assert FROM_RECORDING_FLAG in _answer(policy, _refund()).flags
    assert _answer(policy, _req()).answered_by == "overlay"
    assert not policy.recording.diverged


def test_a_recorded_write_that_was_refused_is_not_one_a_later_read_shows():
    """Only a write that entered the write log (`is_authored_write`) is one a later read shows,
    so a recorded write L3 rejected leaves the reads after it to the recording."""
    refused = _recorded_refund(precondition="rejected")
    policy = ReplayPolicy(_recording(refused, _stored(_req())), SHIPPED)
    assert _answer(policy, _req()).answered_by == "recording"


def test_a_recorded_write_irimi_could_not_decide_about_is_not_one_a_later_read_shows():
    """A write whose body decoded past the cap was refused with irimi's own 502 before any policy
    was asked (#94), and the engine never put it in the write log. A replay refuses it the same
    way, so no policy is asked about it then either: counted among the recorded writes, it was
    never made, and the read after it missed and diverged an unchanged replay (#82)."""
    refusal = Response(
        502, (("content-type", JSON),), b'{"error": {"type": "irimi_decision_failed"}}'
    )
    refused = _stored(
        replace(_refund(), body=b""),
        refusal,
        kind="unknown",
        answered_by="fake-L0",
        flags=(UNCLASSIFIED_FLAG, DECISION_FAILED_FLAG, BODY_TRUNCATED_FLAG),
    )
    policy = ReplayPolicy(_recording(refused, _stored(_req())), SHIPPED)
    answer = _answer(policy, _req())
    assert (answer.answered_by, answer.flags) == (
        "recording",
        (FIDELITY_RECORDING_FLAG, FROM_RECORDING_FLAG),
    )
    assert not policy.recording.diverged


# ------------------------------------------------------- what a recorded answer was made from


def _keyed_refund(amount, key, charge="ch_1"):
    """A refund whose only idempotency key is the header, as stripe-python sends it."""
    body = f"charge={charge}&amount={amount}".encode()
    return _req("POST", path="/v1/refunds", body=body, ct=FORM, headers=(("idempotency-key", key),))


def _recorded_keyed(amount, key, answer=REFUND_ANSWER, **fields):
    defaults = {"answered_by": "fake-L1", "flags": (FIDELITY_L1_FLAG,)}
    return _stored(_keyed_refund(amount, key), answer, **(defaults | fields))


CHARGE = Response(
    200,
    (("content-type", JSON),),
    json.dumps(
        {
            "id": "ch_1",
            "object": "charge",
            "amount": 4900,
            "amount_refunded": 0,
            "refunded": False,
            "currency": "usd",
        }
    ).encode(),
)
TOO_LARGE = Response(400, (("content-type", JSON),), b'{"error": {"code": "amount_too_large"}}')


def test_a_retry_never_takes_the_answer_to_a_second_distinct_write():
    """The recorded run made two refunds; this one retries its first. Stripe answers the retry
    with the first refund again, so the recorded second refund - another minted id, a second entry
    in the write log - is not its answer (#46, #82)."""
    second = replace(REFUND_ANSWER, body=b'{"id": "re_RECORDED2", "amount": 100}')
    recording = _recording(_recorded_keyed(100, "k1"), _recorded_keyed(100, "k2", second))
    policy = ReplayPolicy(recording, SHIPPED)
    assert _answer(policy, _keyed_refund(100, "k1")).response.body == REFUND_ANSWER.body
    retry = _answer(policy, _keyed_refund(100, "k1"))
    assert retry.response.body == REFUND_ANSWER.body
    assert retry.flags == (FIDELITY_L1_FLAG, IDEMPOTENT_REPLAY_FLAG, REPLAY_MISS_FLAG)


def test_a_fresh_write_never_takes_a_recorded_retry():
    """The recorded run retried its refund; this one makes a second refund with a new key. Stripe
    makes a second refund, so the recorded retry - the first refund's bytes, kept out of the write
    log - is not its answer (#46, #82)."""
    retried = _recorded_keyed(100, "k1", flags=(FIDELITY_L1_FLAG, IDEMPOTENT_REPLAY_FLAG))
    policy = ReplayPolicy(_recording(_recorded_keyed(100, "k1"), retried), SHIPPED)
    assert FROM_RECORDING_FLAG in _answer(policy, _keyed_refund(100, "k1")).flags
    fresh = _answer(policy, _keyed_refund(100, "k2"))
    assert fresh.flags == (FIDELITY_L1_FLAG, REPLAY_MISS_FLAG)
    assert fresh.response.body != REFUND_ANSWER.body


def test_a_recorded_retry_answers_the_same_retry():
    """The other half: a retry the recorded run made too is answered from the recording, as the
    retry it was."""
    retried = _recorded_keyed(100, "k1", flags=(FIDELITY_L1_FLAG, IDEMPOTENT_REPLAY_FLAG))
    policy = ReplayPolicy(_recording(_recorded_keyed(100, "k1"), retried), SHIPPED)
    _answer(policy, _keyed_refund(100, "k1"))
    retry = _answer(policy, _keyed_refund(100, "k1"))
    assert retry.flags == (FIDELITY_L1_FLAG, IDEMPOTENT_REPLAY_FLAG, FROM_RECORDING_FLAG)
    assert not policy.recording.diverged


def test_a_key_reused_for_another_write_is_refused_whatever_the_recording_holds():
    """The recording holds a refund of 100. This run sent a refund of 250 under key `k`, then the
    refund of 100 under the same key, which Stripe refuses with `idempotency_error` (#46)."""
    policy = ReplayPolicy(_recording(_recorded_keyed(100, "k-recorded")), SHIPPED)
    assert REPLAY_MISS_FLAG in _answer(policy, _keyed_refund(250, "k")).flags
    reused = _answer(policy, _keyed_refund(100, "k"))
    assert (reused.response.status, reused.flags[-2:]) == (
        400,
        (IDEMPOTENCY_CONFLICT_FLAG, REPLAY_MISS_FLAG),
    )


def test_a_checked_write_recorded_after_a_write_the_replay_has_not_made_is_checked_again():
    """The recorded run refunded 1000 of a 4900 charge, so its refund of 4900 was refused. This
    run never refunded the 1000, and against the recording's own read of the charge its 4900 is
    taken (#45, #82)."""
    engine = _stored(_req(), CHARGE, issued_by="engine")
    first = _recorded_keyed(1000, "a", precondition="passed")
    refused = _recorded_keyed(
        4900, "b", TOO_LARGE, precondition="rejected", rejection_code="amount_too_large"
    )
    policy = ReplayPolicy(_recording(engine, first, refused), SHIPPED)
    answer = _answer(policy, _keyed_refund(4900, "b"))
    assert (answer.response.status, answer.precondition) == (200, "passed")
    assert answer.flags[-1] == REPLAY_MISS_FLAG
    assert policy.recording.diverged


def test_a_checked_write_after_the_replay_diverged_is_checked_again():
    """This run refunded 2000 first, which the recorded run never did, so the recorded refund of
    4900 passed against a charge this run has already refunded part of. Checked again, against the
    recording's read of the charge and this run's write log, it is refused (#45, #82)."""
    engine = _stored(_req(), CHARGE, issued_by="engine")
    recorded = _recorded_keyed(4900, "a", precondition="passed")
    policy = ReplayPolicy(_recording(engine, recorded), SHIPPED)
    new = _keyed_refund(2000, "x")
    cls = classify(new, SHIPPED)
    made = policy.answer(new, cls, (), "rep")
    assert made.flags[-1] == REPLAY_MISS_FLAG
    logged = Exchange(
        request=new,
        response=made.response,
        service=cls.service,
        operation=cls.operation,
        kind=cls.kind,
        answered_by=made.answered_by,
        validation="unvalidated",
        run_id="rep",
        flags=made.flags,
        precondition=made.precondition,
    )
    answer = _answer(policy, _keyed_refund(4900, "a"), write_log=(logged,))
    assert (answer.response.status, answer.precondition) == (400, "rejected")
    assert answer.flags[-1] == REPLAY_MISS_FLAG


def test_an_unchecked_write_is_answered_from_the_recording_after_the_replay_diverged():
    """A write no `precondition:` checked was not made from the world: its recorded answer is
    still the one, and its minted ids the recorded ones."""
    unchecked = _req("POST", path="/v1/customers/cus_1", body=b"email=a%40b.c", ct=FORM)
    assert not classify(unchecked, SHIPPED).route.precondition
    recorded = _stored(unchecked, REFUND_ANSWER, answered_by="fake-L0", flags=("fidelity:L0",))
    policy = ReplayPolicy(_recording(recorded), SHIPPED)
    _answer(policy, _keyed_refund(2000, "x"))
    assert policy.recording.diverged
    assert FROM_RECORDING_FLAG in _answer(policy, unchecked).flags


def test_the_recording_never_hands_out_a_read_once_the_replay_has_diverged():
    """Divergence and the take are one step under the recording's lock: a read whose policy saw
    the replay undiverged cannot be served after a write on another thread diverged it."""
    recording = _recording(_stored(_req()))
    recording.diverge()
    assert recording.take(_req(), "read") is None


def test_an_llm_call_whose_query_differs_is_a_miss():
    """A query can say what an llm call asks (`?alt=sse`, `?api-version=`) as much as a read's."""
    recording = _recording(_stored(_llm()))
    asked = replace(_llm(), query="alt=sse")
    assert recording.take(asked, "llm") is None
    assert recording.take(_llm(), "llm") is not None


def test_a_request_in_another_account_is_another_request():
    """`Stripe-Account` says whose `ch_1` a read names and whose charge a refund refunds, as the
    write log, the idempotency store and L3 already compare it (`services.scope_of`). Any other
    header still decides nothing."""
    in_a = (("stripe-account", "acct_A"),)
    read_a = _stored(_req(headers=in_a), replace(OK, body=b'{"a": 1}'))
    refund_a = _stored(replace(_refund(), headers=_refund().headers + in_a), REFUND_ANSWER)
    recording = _recording(read_a, refund_a)
    in_b = (("stripe-account", "acct_B"),)
    assert recording.take(_req(headers=in_b), "read") is None
    assert recording.take(replace(_refund(), headers=_refund().headers + in_b), "write") is None
    taken = recording.take(_req(headers=(*in_a, ("user-agent", "x"))), "read")
    assert taken is not None and taken.response.body == b'{"a": 1}'
    assert recording.take(replace(_refund(), headers=_refund().headers + in_a), "write")
