"""W7 `streaming_assistant`: SSE in both directions, pinned per scenario.

The agent forwards Anthropic's text deltas to its own caller as they arrive, and assembles an
OpenAI tool call from its deltas. Streams are read seven bytes at a time, so every event is split.
"""

import json
import re
from itertools import accumulate

from examples.workflows.w07_streaming_assistant.scenarios import (
    ANSWER,
    CHANNEL,
    LONG_ANSWER,
    SECRET,
    pair_answer,
    pair_decision,
)
from irimi import redact, trace
from irimi.exchange import STREAM_TRUNCATED_FLAG, UPSTREAM_ERROR_FLAG, Exchange
from irimi.trace import ErrorInfo, TelemetrySeen

W = "w07_streaming_assistant"
TRACE_LINE = "live      telemetry POST api.smith.langchain.com/runs -> 202"
LLM_LINES = [
    "live      llm       POST api.openai.com/v1/embeddings -> 200",
    "live      llm       POST api.anthropic.com/v1/messages -> 200",
    "live      llm       POST api.openai.com/v1/chat/completions -> 200",
]
# A stream that broke after irimi had sent the agent its headers: stored with what arrived (#71).
BROKEN_STREAM_LINE = (
    "live      llm       POST api.anthropic.com/v1/messages -> 200"
    "  [upstream-error, stream-truncated]"
)
MESSAGE_START = b"event: message_start\n"
MESSAGE_STOP = b'event: message_stop\ndata: {"type": "message_stop"}\n\n'


def stored_exchanges(result) -> dict[str, Exchange]:
    """The run's stored exchanges by request path; W7 makes each call at most once."""
    exchanges = [e for e in result.stored_events() if isinstance(e, Exchange)]
    by_path = {e.request.path: e for e in exchanges}
    assert len(by_path) == len(exchanges)
    return by_path


def data_lines(body: bytes) -> list[dict]:
    """Each SSE `data:` line of a stored stream that holds a JSON document, parsed."""
    return [
        json.loads(line[len("data: ") :])
        for line in body.decode().splitlines()
        if line.startswith("data: {")
    ]


def text_deltas(body: bytes) -> str:
    """The text an Anthropic stream's `text_delta` events carry, joined."""
    return "".join(
        d["delta"]["text"] for d in data_lines(body) if d["type"] == "content_block_delta"
    )


def boundaries(chunks: tuple[int, ...]) -> list[int]:
    """The offsets in a streamed body where one recorded chunk ends and the next begins."""
    return list(accumulate(chunks))[:-1]


def line_around(body: bytes, needle: bytes) -> tuple[int, int]:
    """The start of the line of `body` that holds `needle`, and the end of it, past its LF."""
    at = body.index(needle)
    return body.rindex(b"\n", 0, at) + 1, body.index(b"\n", at) + 1


def test_both_streams_pass_through_irimi_whole_and_unchanged(run_workflow):
    bare = run_workflow(W, "normal", "bare")
    shadow = run_workflow(W, "normal", "shadow")
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    result = shadow.result()
    assert result["outcome"] == "ok"
    assert result["caller_text"] == result["answer"] == ANSWER
    assert result["caller_events"][0] == "start" and result["caller_events"][-1] == "done"
    # Every model call is forwarded live, streamed or not, and none is stamped; so is the trace.
    assert shadow.exchange_lines() == [*LLM_LINES, TRACE_LINE]
    assert [c["answered_by"] for c in shadow.calls()] == [None, None, None, None]


def test_a_telemetry_post_is_stored_as_having_happened_and_its_body_is_not(run_workflow):
    """Telemetry is forwarded and its requests and responses are never stored (#70): the trace
    POST is one `TelemetrySeen` line, host and time, and no blob holds its body."""
    shadow = run_workflow(W, "normal", "shadow")
    [trace_call] = shadow.calls("trace")
    assert (trace_call["status"], trace_call["answered_by"]) == (202, None)
    [sent] = shadow.internet.requests("api.smith.langchain.com")
    assert json.loads(sent.body)["inputs"] == {"question": "How do refunds work?"}
    seen = [e for e in shadow.stored_events() if isinstance(e, TelemetrySeen)]
    assert [e.host for e in seen] == ["api.smith.langchain.com"]
    blobs = shadow.home / "store" / "blobs"
    assert trace.body_ref(sent.body).sha256 not in {p.name for p in blobs.iterdir()}
    # The run it landed in, the chat's `sdk` run with its three model calls, prints it as an
    # event and counts it in its stored summary: telemetry in a stored summary, which no other
    # corpus run exercises (#72). The streams are in it too: `stream_post` is plain urllib, which
    # the SDK labels (#75).
    reader = shadow.stored()
    [holder] = [
        r.run_id
        for r in reader.list_runs()
        if any(isinstance(e, TelemetrySeen) for e in reader.load_run(r.run_id).events)
    ]
    code, out, err = shadow.runs("show", holder)
    assert (code, err) == (0, [])
    # The run has a start and an end now (#74), so its elapsed time is real: normalised here.
    assert [re.sub(r" · \d+\.\ds · ", " · <elapsed> · ", line) for line in out[7:]] == [
        "",
        "live      llm       POST api.openai.com/v1/embeddings -> 200",
        "live      llm       POST api.anthropic.com/v1/messages -> 200",
        "live      llm       POST api.openai.com/v1/chat/completions -> 200",
        "telemetry api.smith.langchain.com",
        "",
        f"irimi shadow · run {holder} · 4 exchanges · <elapsed> · backstop: none (Phase 4)",
        "",
        "  api.anthropic.com  1 llm",
        "  api.openai.com     2 llm",
        "  telemetry          1 exchange to 1 host, forwarded live",
        "",
        "  4 exchanges · 4 live · 0 delegated · 0 virtualized",
    ]


def test_a_streamed_answer_is_stored_as_the_chunks_the_agent_was_sent(run_workflow):
    """#71: the engine streams an SSE answer through and stores what it sent, joined, with the
    length of each chunk. Both streams are stored whole, to their last event, and the Anthropic
    text deltas on disk are the answer the agent assembled. The fake writes one event per chunk,
    5 ms apart, and TCP may coalesce writes, so more than one chunk is pinned and not a count. The
    embeddings answer is not streamed, and has no chunks."""
    shadow = run_workflow(W, "normal", "shadow")
    stored = stored_exchanges(shadow)
    assert sorted(stored) == ["/v1/chat/completions", "/v1/embeddings", "/v1/messages"]
    # All three in the chat's run, the streams too: urllib is labelled by the SDK (#75).
    [chat] = shadow.sdk_runs()
    assert {ex.run_id for ex in stored.values()} == {chat.run_id}
    ends = {"/v1/messages": MESSAGE_STOP, "/v1/chat/completions": b"data: [DONE]\n\n"}
    for path, end in ends.items():
        ex = stored[path]
        assert ex.response is not None and ex.response.status == 200
        assert (ex.response.header("content-type") or "").startswith("text/event-stream")
        assert json.loads(ex.request.body)["stream"] is True
        assert ex.response.body.endswith(end)
        assert len(ex.stream_chunks) > 1
        assert sum(ex.stream_chunks) == len(ex.response.body)
        assert ex.flags == ()
    anthropic = stored["/v1/messages"].response
    assert anthropic is not None
    deltas = [
        d["delta"]["text"] for d in data_lines(anthropic.body) if d["type"] == "content_block_delta"
    ]
    assert "".join(deltas) == shadow.result()["answer"] == ANSWER
    embeddings = stored["/v1/embeddings"]
    assert embeddings.response is not None and json.loads(embeddings.response.body)["data"]
    assert embeddings.stream_chunks == ()


def test_streams_read_with_httpx_are_stored_whole_in_the_chats_run(run_workflow):
    """The Anthropic and OpenAI SDKs stream with httpx, which the SDK labels at its transport and
    unlabels as soon as the transport returns, while the stream is still being read (#75). The
    agent sees what it sees with urllib, both streams are stored whole and in the chat's run, as
    the chunks it was sent (#71), and no upstream saw `Irimi-Run` (universal invariant 2)."""
    bare = run_workflow(W, "httpx_streams", "bare")
    shadow = run_workflow(W, "httpx_streams", "shadow")
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    assert shadow.result()["caller_text"] == shadow.result()["answer"] == ANSWER
    assert shadow.exchange_lines() == LLM_LINES
    stored = stored_exchanges(shadow)
    [chat] = shadow.sdk_runs()
    assert {ex.run_id for ex in stored.values()} == {chat.run_id}
    assert shadow.stored().load_run(shadow.process_run_id()).events == []
    ends = {"/v1/messages": MESSAGE_STOP, "/v1/chat/completions": b"data: [DONE]\n\n"}
    for path, end in ends.items():
        ex = stored[path]
        assert ex.response is not None and ex.response.body.endswith(end)
        assert len(ex.stream_chunks) > 1 and sum(ex.stream_chunks) == len(ex.response.body)
        assert ex.flags == ()
    anthropic = stored["/v1/messages"].response
    assert anthropic is not None and text_deltas(anthropic.body) == ANSWER
    assert [r for r in shadow.internet.requests() if "irimi-run" in r.headers] == []


def test_a_secret_inside_a_stream_reaches_the_caller_intact_and_never_disk(run_workflow):
    """Redaction is for disk only (#69): the live stream is never rewritten, and the stored one
    (#71) holds the secret nowhere whole. The agent passes Anthropic's answer on to OpenAI, so the
    secret is whole in that request body, and stored there as its placeholder. The secret is not
    one of the harness's canaries, so invariant 4 does not look for it: this test does."""
    shadow = run_workflow(W, "secret_in_stream", "shadow")
    assert SECRET in shadow.result()["caller_text"]
    assert shadow.exchange_lines() == LLM_LINES
    written = [
        path
        for root in (shadow.home, shadow.cwd, shadow.tmp)
        for path in root.rglob("*")
        if path.is_file() and SECRET.encode() in path.read_bytes()
    ]
    assert written == []
    stored = stored_exchanges(shadow)
    hidden = redact.placeholder(redact.load_key(shadow.home), SECRET).encode()
    assert hidden in stored["/v1/chat/completions"].request.body


def test_a_secret_a_stream_sends_in_pieces_reaches_disk_in_pieces(run_workflow):
    """The fake sends Anthropic's text 12 characters per `text_delta` event, and redaction reads
    a stream line by line (#69), so the secret is never whole on any line it reads."""
    shadow = run_workflow(W, "secret_in_stream", "shadow")
    anthropic = stored_exchanges(shadow)["/v1/messages"].response
    assert anthropic is not None
    deltas = [
        d["delta"]["text"] for d in data_lines(anthropic.body) if d["type"] == "content_block_delta"
    ]
    assert "".join(deltas) == f"The test key is {SECRET}. Keep it out of logs."
    # LOOKS WRONG: every piece of the secret is on disk as it was sent, so the stored stream
    # holds the whole secret for anyone who joins its deltas. Redacting over reassembled deltas
    # is #92's, not #71's.
    assert [d for d in deltas if d[:4] in ("CANA", " is ")] == [" is sk_live_", "CANARYstream"]


def test_a_caller_that_hangs_up_ends_the_run_in_error(run_workflow):
    bare = run_workflow(W, "caller_disconnects", "bare")
    shadow = run_workflow(W, "caller_disconnects", "shadow")
    assert bare.exit_code == shadow.exit_code == 3
    assert shadow.result()["caller_events"] == ["start"]
    assert shadow.result()["outcome"] == "client_gone"
    assert [(e["outcome"], e["error"]) for e in shadow.events("run.end")] == [
        ("error", "ClientGone")
    ]
    # Stored under the exception's full name (#74): an agent's own class, by its module.
    [run] = shadow.sdk_runs()
    assert (run.outcome, run.error) == (
        "error",
        ErrorInfo("examples.workflows.w07_streaming_assistant.agent.ClientGone", "BrokenPipeError"),
    )
    # The agent stopped reading Anthropic's stream when its own caller left, and never asked
    # OpenAI anything.
    assert [c["label"] for c in shadow.calls()] == ["embed", "anthropic_stream"]
    # LOOKS WRONG: the upstream answered fine; it was the AGENT that closed the stream. irimi
    # records the abandoned stream as an upstream failure (#93).
    assert shadow.exchange_lines() == [LLM_LINES[0], BROKEN_STREAM_LINE]
    # The stream the agent abandoned is stored with what irimi had copied of it by then, and
    # says it is not the whole stream (#71): it opens with `message_start` and never ends.
    stream = stored_exchanges(shadow)["/v1/messages"]
    assert stream.flags == (UPSTREAM_ERROR_FLAG, STREAM_TRUNCATED_FLAG)
    assert stream.response is not None and stream.response.status == 200
    assert stream.response.body.startswith(MESSAGE_START)
    assert not stream.response.body.endswith(MESSAGE_STOP)
    assert sum(stream.stream_chunks) == len(stream.response.body)


def test_an_upstream_reset_mid_stream_is_stored_as_a_broken_stream(run_workflow):
    """The fake frames its streams as real LLM APIs do, chunked, so a reset mid-stream is a body
    that never finished, and not an end of body (#71). The agent sees its stream break, and irimi
    stores the one event that arrived, flagged as not the whole stream."""
    bare = run_workflow(W, "upstream_reset", "bare")
    shadow = run_workflow(W, "upstream_reset", "shadow")
    assert bare.exit_code == shadow.exit_code == 4
    # Bare, the reset reaches the agent as a reset. Under shadow it reaches it as a chunked body
    # that stopped short: irimi's own connection to the agent was not reset.
    assert bare.events("stream_error", with_time=False) == [
        {"event": "stream_error", "label": "anthropic_stream", "error": "ConnectionResetError"}
    ]
    assert shadow.events("stream_error", with_time=False) == [
        {"event": "stream_error", "label": "anthropic_stream", "error": "IncompleteRead"}
    ]
    assert shadow.exchange_lines() == [LLM_LINES[0], BROKEN_STREAM_LINE]
    assert shadow.result()["caller_events"] == ["start", "error"]
    stream = stored_exchanges(shadow)["/v1/messages"]
    assert stream.flags == (UPSTREAM_ERROR_FLAG, STREAM_TRUNCATED_FLAG)
    assert stream.response is not None and stream.response.status == 200
    assert stream.response.body.startswith(MESSAGE_START)
    assert data_lines(stream.response.body)[0]["type"] == "message_start"
    assert stream.stream_chunks == (len(stream.response.body),)


def test_a_reset_before_the_stream_opens_reaches_the_agent_as_an_unstamped_502(run_workflow):
    bare = run_workflow(W, "reset_before_stream", "bare")
    shadow = run_workflow(W, "reset_before_stream", "shadow")
    assert bare.exit_code == shadow.exit_code == 4
    assert bare.result()["error"] == "anthropic_stream: ConnectionResetError"
    # LOOKS WRONG: under shadow the proxy answers the reset with a 502 of its own, unstamped: the
    # agent sees an HTTP status where bare it saw no answer (as in W10's `reset_on_read`).
    assert shadow.result()["error"] == "anthropic_stream: HTTP 502"
    [stream] = shadow.calls("anthropic_stream")
    assert (stream["status"], stream["answered_by"]) == (502, None)
    assert shadow.exchange_lines() == [
        LLM_LINES[0],
        "live      llm       POST api.anthropic.com/v1/messages -> -  [upstream-error]",
    ]
    for result in (bare, shadow):
        assert result.result()["caller_events"] == ["start", "error"]
    # No stream ever opened, so nothing is stored as one (#71).
    stored = stored_exchanges(shadow)
    assert stored["/v1/messages"].response is None
    assert [e.stream_chunks for e in stored.values()] == [(), ()]


def test_a_tool_call_streamed_in_pieces_becomes_one_faked_slack_post(run_workflow):
    bare = run_workflow(W, "tool_call_across_deltas", "bare")
    shadow = run_workflow(W, "tool_call_across_deltas", "shadow")
    text = "Summary: refunds go back to the card within 5-10 days."
    posted = {"ok": True, "channel": CHANNEL, "text": text}
    assert bare.result()["posted"] == shadow.result()["posted"] == posted
    assert [m["text"] for m in bare.world.slack.messages[CHANNEL]] == [text]
    assert shadow.world.slack.messages[CHANNEL] == []
    assert shadow.exchange_lines() == [
        *LLM_LINES,
        "live      read      POST slack.com/api/conversations.info -> 200",
        "fake-L1   write     POST slack.com/api/chat.postMessage -> 200  [fidelity:L1]",
    ]
    assert f'  ○ post to #{CHANNEL}: "{text}"  unvalidated (L3 preconditions passed)' in (
        shadow.summary()
    )
    # The stored OpenAI stream holds every 5-character argument delta `SplittingLLM` sent, in
    # order: the tool call is recorded as it streamed, not as the agent assembled it (#71).
    openai = stored_exchanges(shadow)["/v1/chat/completions"].response
    assert openai is not None
    calls = [d["choices"][0]["delta"].get("tool_calls") for d in data_lines(openai.body)]
    pieces = [call[0]["function"]["arguments"] for call in calls if call]
    arguments = json.dumps({"channel": CHANNEL, "text": text})
    assert pieces == ["", *(arguments[i : i + 5] for i in range(0, len(arguments), 5))]


def test_a_secret_a_write_ends_inside_is_redacted_in_the_joined_stream(run_workflow):
    """#71 with chunk boundaries that fall mid-line. The fake writes a 16 KB Anthropic stream in
    pieces that each end inside a `data:` line, one of them inside the secret, which is whole on
    its line. Redaction reads the stream joined (#69), so the secret reaches disk as its
    placeholder, never as two halves. The chunk lengths are recomputed over the redacted body
    line by line: a boundary in a line redaction left alone keeps its place, and the one that
    fell inside the secret's line, which redaction re-serialized, moves to that line's end."""
    bare = run_workflow(W, "secret_split_across_writes", "bare")
    shadow = run_workflow(W, "secret_split_across_writes", "shadow")
    assert bare.exit_code == shadow.exit_code == 0
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    assert shadow.result()["caller_text"] == shadow.result()["answer"] == LONG_ANSWER
    assert shadow.exchange_lines() == LLM_LINES
    # What the engine copied, before redaction: the stream as sent, and a chunk boundary inside
    # the secret. Chunked framing hands the proxy each write as its own chunk at least.
    [sent] = [e for e in shadow.handed if e.request.path == "/v1/messages"]
    assert sent.response is not None
    sent_body, sent_cuts = sent.response.body, boundaries(sent.stream_chunks)
    assert text_deltas(sent_body) == LONG_ANSWER
    assert len(sent_body) > 16_000 and sum(sent.stream_chunks) == len(sent_body)
    at = sent_body.index(SECRET.encode())
    assert any(at < cut < at + len(SECRET) for cut in sent_cuts)
    # What the store holds.
    stored = stored_exchanges(shadow)["/v1/messages"]
    assert stored.response is not None and stored.flags == ()
    body = stored.response.body
    hidden = redact.placeholder(redact.load_key(shadow.home), SECRET)
    assert SECRET.encode() not in body
    assert text_deltas(body) == LONG_ANSWER.replace(SECRET, hidden)
    assert body.endswith(MESSAGE_STOP)
    assert sum(stored.stream_chunks) == len(body)
    sent_start, sent_end = line_around(sent_body, SECRET.encode())
    start, end = line_around(body, hidden.encode())
    assert start == sent_start  # redaction changed that one line and no other
    moved = len(body) - len(sent_body)
    assert boundaries(stored.stream_chunks) == [
        *(cut for cut in sent_cuts if cut <= sent_start),
        end,
        *(cut + moved for cut in sent_cuts if cut >= sent_end),
    ]
    written = [
        path
        for root in (shadow.home, shadow.cwd, shadow.tmp)
        for path in root.rglob("*")
        if path.is_file() and SECRET.encode() in path.read_bytes()
    ]
    assert written == []


def test_two_streams_at_once_are_each_stored_against_their_own_request(run_workflow):
    """#71 under concurrency. Two callers ask at once, and the fake LLM answers neither of a
    provider's two calls until both have arrived, so irimi copies two Anthropic streams, then two
    OpenAI streams, at the same time. Each chat's answers name its caller, so a chunk copied into
    the other flow's recording would show: each stored stream is whole, and is the answer to the
    request it is stored with."""
    bare = run_workflow(W, "two_callers", "bare")
    shadow = run_workflow(W, "two_callers", "shadow")
    assert bare.exit_code == shadow.exit_code == 0
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    for n, caller in enumerate(shadow.result()["callers"], start=1):
        assert caller["question"] == f"How do refunds work? (caller {n})"
        assert caller["caller_text"] == caller["answer"] == pair_answer(str(n))
        assert caller["decision"] == pair_decision(str(n))
        assert caller["caller_events"][0] == "start" and caller["caller_events"][-1] == "done"
    assert sorted(shadow.exchange_lines()) == sorted(LLM_LINES * 2)
    stored = [e for e in shadow.stored_events() if isinstance(e, Exchange)]
    ends = {"/v1/messages": MESSAGE_STOP, "/v1/chat/completions": b"data: [DONE]\n\n"}
    for path, end in ends.items():
        streams = [e for e in stored if e.request.path == path]
        assert len(streams) == 2
        assert max(e.started_at for e in streams) < min(e.ended_at for e in streams)
        callers = []
        for ex in streams:
            assert ex.response is not None and ex.flags == ()
            assert ex.response.body.endswith(end)
            assert len(ex.stream_chunks) > 1
            assert sum(ex.stream_chunks) == len(ex.response.body)
            asked = json.loads(ex.request.body)["messages"][-1]["content"]
            caller = "1" if "caller 1" in asked else "2"
            callers.append(caller)
            if path == "/v1/messages":
                assert text_deltas(ex.response.body) == pair_answer(caller)
            else:
                assert asked == pair_answer(caller)
                content = [
                    d["choices"][0]["delta"].get("content") or ""
                    for d in data_lines(ex.response.body)
                ]
                assert "".join(content) == pair_decision(caller)
        assert sorted(callers) == ["1", "2"]
    # Two concurrent requests do not mix (Phase 3's exit): each chat is a run of its own, and
    # every call it made, both streams included, is stored in it and in no other, by the
    # `Irimi-Run` the SDK put on each as it was sent (#75), though the two chats' streams were in
    # the air together.
    runs = {r.trigger.args["question"][-2]: r.run_id for r in shadow.sdk_runs() if r.trigger}
    assert sorted(runs) == ["1", "2"]
    for caller, run_id in runs.items():
        held = [e for e in shadow.stored().load_run(run_id).events if isinstance(e, Exchange)]
        assert sorted(e.request.path for e in held) == sorted([*ends, "/v1/embeddings"])
        for ex in held:
            if ex.request.path == "/v1/chat/completions":
                assert json.loads(ex.request.body)["messages"][-1]["content"] == pair_answer(caller)
            else:
                asked = json.loads(ex.request.body)
                question = asked["input"] if "input" in asked else asked["messages"][-1]["content"]
                assert question == f"How do refunds work? (caller {caller})"
    process = shadow.stored().load_run(shadow.process_run_id()).events
    assert process == []


def test_the_callers_send_function_is_stored_by_its_repr_and_cannot_be_replayed(run_workflow):
    """#74's capture of a value that is not data: `chat(question, send)` is the ordinary shape of
    a streaming handler, and `send` is a closure over the caller's connection. It is stored as
    its repr and makes the run not replayable; the question is stored as itself."""
    shadow = run_workflow(W, "normal", "shadow")
    [run] = shadow.sdk_runs()
    assert run.trigger is not None and not run.trigger.replayable
    assert run.trigger.entrypoint == "examples.workflows.w07_streaming_assistant.agent:chat"
    args = run.trigger.args
    assert isinstance(args, dict) and args["question"] == "How do refunds work?"
    send = args["send"]
    assert isinstance(send, dict) and set(send) == {"__irimi_repr__"}
    assert str(send["__irimi_repr__"]).startswith("<function ")
