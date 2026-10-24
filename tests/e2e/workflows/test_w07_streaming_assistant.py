"""W7 `streaming_assistant`: SSE in both directions, pinned per scenario.

The agent reads streams seven bytes at a time, so every event arrives split."""

import hashlib
import json
import re
from itertools import accumulate

import pytest

from irimi.core.exchange import Exchange, Flag
from irimi.core.limits import MAX_DECODED_BODY, MAX_STORED_BODY
from irimi.protocol import trace
from irimi.protocol.trace import ErrorInfo, TelemetrySeen
from irimi.record import redact
from tests.e2e.workflows.agents.w07_streaming_assistant.agent import HUNG_UP
from tests.e2e.workflows.agents.w07_streaming_assistant.scenarios import (
    ANSWER,
    CHANNEL,
    LONG_ANSWER,
    SECRET,
    pair_answer,
    pair_decision,
)
from tests.e2e.workflows.harness.compare import compare, rows_not_shown
from tests.e2e.workflows.harness.internet import BOMB_LINE
from tests.e2e.workflows.harness.invariants import check_invariants

W = "w07_streaming_assistant"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
MODES = ("shadow", "serve")
TRACE_LINE = "real          telemetry POST api.smith.langchain.com/runs -> 202"
LLM_LINES = [
    "real          llm       POST api.openai.com/v1/embeddings -> 200",
    "real          llm       POST api.anthropic.com/v1/messages -> 200",
    "real          llm       POST api.openai.com/v1/chat/completions -> 200",
]
# A stream that broke after irimi had sent the agent its headers: stored with what arrived.
BROKEN_STREAM_LINE = (
    "real          llm       POST api.anthropic.com/v1/messages -> 200"
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


@pytest.mark.parametrize("mode", MODES)
def test_both_streams_pass_through_irimi_whole_and_unchanged(run_workflow, mode):
    bare = run_workflow(W, "normal", "bare")
    shadow = run_workflow(W, "normal", mode)
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    result = shadow.result()
    assert result["outcome"] == "ok"
    assert result["caller_text"] == result["answer"] == ANSWER
    assert result["caller_events"][0] == "start" and result["caller_events"][-1] == "done"
    # Every model call is forwarded live, streamed or not, and none is stamped; so is the trace.
    assert shadow.exchange_lines() == [*LLM_LINES, TRACE_LINE]
    assert [c["answered_by"] for c in shadow.calls()] == [None, None, None, None]


@pytest.mark.parametrize("mode", MODES)
def test_a_telemetry_post_is_stored_as_having_happened_and_its_body_is_not(run_workflow, mode):
    """A telemetry post is stored as one `TelemetrySeen` line, and no blob holds its body."""
    shadow = run_workflow(W, "normal", mode)
    [trace_call] = shadow.calls("trace")
    assert (trace_call["status"], trace_call["answered_by"]) == (202, None)
    [sent] = shadow.internet.requests("api.smith.langchain.com")
    assert json.loads(sent.body)["inputs"] == {"question": "How do refunds work?"}
    seen = [e for e in shadow.stored_events() if isinstance(e, TelemetrySeen)]
    assert [e.host for e in seen] == ["api.smith.langchain.com"]
    blobs = shadow.home / "store" / "blobs"
    assert trace.body_ref(sent.body).sha256 not in {p.name for p in blobs.iterdir()}
    # The chat's run prints the post as an event and counts it in its stored summary; no other
    # corpus run puts telemetry in a stored summary.
    reader = shadow.stored()
    [holder] = [
        r.run_id
        for r in reader.list_runs()
        if any(isinstance(e, TelemetrySeen) for e in reader.load_run(r.run_id).events)
    ]
    code, out, err = shadow.runs("show", holder)
    assert (code, err) == (0, [])
    # The run has a start and an end, so its elapsed time is real: normalised here.
    assert [re.sub(r" · \d+\.\ds$", " · <elapsed>", line) for line in out[7:]] == [
        "",
        "real          llm       POST api.openai.com/v1/embeddings -> 200",
        "real          llm       POST api.anthropic.com/v1/messages -> 200",
        "real          llm       POST api.openai.com/v1/chat/completions -> 200",
        "telemetry api.smith.langchain.com",
        "",
        f"irimi shadow · run {holder} · 4 exchanges · <elapsed>",
        "",
        "  api.anthropic.com  1 llm",
        "  api.openai.com     2 llm",
        "  telemetry          1 exchange to 1 host, forwarded live",
        "",
        "  4 exchanges · 4 real · 0 answered by irimi (0 faked writes) · 0 sent to targets",
    ]


@pytest.mark.parametrize("mode", MODES)
def test_a_streamed_answer_is_stored_as_the_chunks_the_agent_was_sent(run_workflow, mode):
    """A streamed answer is stored whole, as what the engine sent, with each chunk's length.

    TCP may coalesce the fake's writes, so the test pins more than one chunk, not a count. The
    unstreamed embeddings answer has no chunks."""
    shadow = run_workflow(W, "normal", mode)
    stored = stored_exchanges(shadow)
    assert sorted(stored) == ["/v1/chat/completions", "/v1/embeddings", "/v1/messages"]
    # All three in the chat's run, the streams too: urllib is labelled by the SDK.
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


@pytest.mark.parametrize("mode", MODES)
def test_streams_read_with_httpx_are_stored_whole_in_the_chats_run(run_workflow, mode):
    """Streams read with httpx are stored whole in the chat's run, as with urllib.

    The SDK labels httpx at its transport, which returns before the stream is read. No upstream
    sees `Irimi-Run`."""
    bare = run_workflow(W, "httpx_streams", "bare")
    shadow = run_workflow(W, "httpx_streams", mode)
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    assert shadow.result()["caller_text"] == shadow.result()["answer"] == ANSWER
    assert shadow.exchange_lines() == LLM_LINES
    stored = stored_exchanges(shadow)
    [chat] = shadow.sdk_runs()
    assert {ex.run_id for ex in stored.values()} == {chat.run_id}
    assert shadow.stored().load_run(shadow.engine_run_id()).events == []
    ends = {"/v1/messages": MESSAGE_STOP, "/v1/chat/completions": b"data: [DONE]\n\n"}
    for path, end in ends.items():
        ex = stored[path]
        assert ex.response is not None and ex.response.body.endswith(end)
        assert len(ex.stream_chunks) > 1 and sum(ex.stream_chunks) == len(ex.response.body)
        assert ex.flags == ()
    anthropic = stored["/v1/messages"].response
    assert anthropic is not None and text_deltas(anthropic.body) == ANSWER
    assert [r for r in shadow.internet.requests() if "irimi-run" in r.headers] == []


@pytest.mark.parametrize("mode", MODES)
def test_a_secret_inside_a_stream_reaches_the_caller_intact_and_never_disk(run_workflow, mode):
    """Redaction rewrites only what reaches disk: the live stream reaches the caller unchanged.

    The agent passes the secret on to OpenAI, whose stored request holds its placeholder. The secret
    is not a harness canary, so this test, not the invariant check, looks for it on disk."""
    shadow = run_workflow(W, "secret_in_stream", mode)
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


@pytest.mark.parametrize("mode", MODES)
def test_a_secret_a_stream_sends_in_pieces_reaches_disk_in_pieces(run_workflow, mode):
    """A secret split across 12-character deltas is never whole on a line redaction reads."""
    shadow = run_workflow(W, "secret_in_stream", mode)
    anthropic = stored_exchanges(shadow)["/v1/messages"].response
    assert anthropic is not None
    deltas = [
        d["delta"]["text"] for d in data_lines(anthropic.body) if d["type"] == "content_block_delta"
    ]
    assert "".join(deltas) == f"The test key is {SECRET}. Keep it out of logs."
    # KNOWN BUG (#92): each delta is redacted on its own, so the stored stream holds the whole
    # secret for anyone who joins its deltas.
    assert [d for d in deltas if d[:4] in ("CANA", " is ")] == [" is sk_live_", "CANARYstream"]


@pytest.mark.parametrize("mode", MODES)
def test_a_caller_that_hangs_up_ends_the_run_in_error(run_workflow, mode):
    bare = run_workflow(W, "caller_disconnects", "bare")
    shadow = run_workflow(W, "caller_disconnects", mode)
    assert bare.exit_code == shadow.exit_code == 3
    assert shadow.result()["caller_events"] == ["start"]
    assert shadow.result()["outcome"] == "client_gone"
    assert shadow.result()["error"] == HUNG_UP
    assert shadow.result()["caused_by_os_error"] is True
    assert [(e["outcome"], e["error"]) for e in shadow.events("run.end")] == [
        ("error", "ClientGone")
    ]
    # Stored under the agent's own class's full name, with the agent's message, which is the
    # same on macOS (EPIPE) and Linux (ECONNRESET).
    [run] = shadow.sdk_runs()
    assert (run.end.outcome, run.end.error) == (
        "error",
        ErrorInfo("tests.e2e.workflows.agents.w07_streaming_assistant.agent.ClientGone", HUNG_UP),
    )
    # The agent stopped reading Anthropic's stream when its own caller left, and never asked
    # OpenAI anything.
    assert [c["label"] for c in shadow.calls()] == ["embed", "anthropic_stream"]
    # KNOWN BUG (#93): irimi records a stream the agent closed as an upstream failure, though the
    # upstream answered fine.
    assert shadow.exchange_lines() == [LLM_LINES[0], BROKEN_STREAM_LINE]
    # The abandoned stream is stored as far as irimi had copied it, flagged as not whole.
    stream = stored_exchanges(shadow)["/v1/messages"]
    assert stream.flags == (Flag.UPSTREAM_ERROR, Flag.STREAM_TRUNCATED)
    assert stream.response is not None and stream.response.status == 200
    assert stream.response.body.startswith(MESSAGE_START)
    assert not stream.response.body.endswith(MESSAGE_STOP)
    assert sum(stream.stream_chunks) == len(stream.response.body)


@pytest.mark.parametrize("mode", MODES)
def test_an_upstream_reset_mid_stream_is_stored_as_a_broken_stream(run_workflow, mode):
    """A reset mid-stream is stored as the events that arrived, flagged as not the whole stream.

    The fake frames streams chunked, as real LLM APIs do, so a reset is an unfinished body."""
    bare = run_workflow(W, "upstream_reset", "bare")
    shadow = run_workflow(W, "upstream_reset", mode)
    assert bare.exit_code == shadow.exit_code == 4
    # Under shadow the agent sees a chunked body that stopped short, since irimi's own connection
    # to it was not reset.
    assert bare.events("stream_error", with_time=False) == [
        {"event": "stream_error", "label": "anthropic_stream", "error": "ConnectionResetError"}
    ]
    assert shadow.events("stream_error", with_time=False) == [
        {"event": "stream_error", "label": "anthropic_stream", "error": "IncompleteRead"}
    ]
    assert shadow.exchange_lines() == [LLM_LINES[0], BROKEN_STREAM_LINE]
    assert shadow.result()["caller_events"] == ["start", "error"]
    stream = stored_exchanges(shadow)["/v1/messages"]
    assert stream.flags == (Flag.UPSTREAM_ERROR, Flag.STREAM_TRUNCATED)
    assert stream.response is not None and stream.response.status == 200
    assert stream.response.body.startswith(MESSAGE_START)
    assert data_lines(stream.response.body)[0]["type"] == "message_start"
    assert stream.stream_chunks == (len(stream.response.body),)


@pytest.mark.parametrize("mode", MODES)
def test_a_reset_before_the_stream_opens_reaches_the_agent_as_an_unstamped_502(run_workflow, mode):
    bare = run_workflow(W, "reset_before_stream", "bare")
    shadow = run_workflow(W, "reset_before_stream", mode)
    assert bare.exit_code == shadow.exit_code == 4
    assert bare.result()["error"] == "anthropic_stream: ConnectionResetError"
    # KNOWN BUG: under shadow the proxy answers the reset with an unstamped 502 of its own, so the
    # agent sees an HTTP status where bare it saw no answer (as in W10's `reset_on_read`).
    assert shadow.result()["error"] == "anthropic_stream: HTTP 502"
    [stream] = shadow.calls("anthropic_stream")
    assert (stream["status"], stream["answered_by"]) == (502, None)
    assert shadow.exchange_lines() == [
        LLM_LINES[0],
        "real          llm       POST api.anthropic.com/v1/messages -> -  [upstream-error]",
    ]
    for result in (bare, shadow):
        assert result.result()["caller_events"] == ["start", "error"]
    # No stream ever opened, so nothing is stored as one.
    stored = stored_exchanges(shadow)
    assert stored["/v1/messages"].response is None
    assert [e.stream_chunks for e in stored.values()] == [(), ()]


@pytest.mark.parametrize("mode", MODES)
def test_a_tool_call_streamed_in_pieces_becomes_one_faked_slack_post(run_workflow, mode):
    bare = run_workflow(W, "tool_call_across_deltas", "bare")
    shadow = run_workflow(W, "tool_call_across_deltas", mode)
    text = "Summary: refunds go back to the card within 5-10 days."
    posted = {"ok": True, "channel": CHANNEL, "text": text}
    assert bare.result()["posted"] == shadow.result()["posted"] == posted
    assert [m["text"] for m in bare.world.slack.messages[CHANNEL]] == [text]
    assert shadow.world.slack.messages[CHANNEL] == []
    assert shadow.exchange_lines() == [
        *LLM_LINES,
        "irimi-check   read      POST slack.com/api/conversations.info -> 200",
        "faked         write     POST slack.com/api/chat.postMessage -> 200",
    ]
    assert (
        f'  ○ slack  post to #{CHANNEL}: "{text}"  faked; checked against live state: would succeed'
        in (shadow.summaries())
    )
    # The tool call is recorded as it streamed, in 5-character deltas, not as the agent built it.
    openai = stored_exchanges(shadow)["/v1/chat/completions"].response
    assert openai is not None
    calls = [d["choices"][0]["delta"].get("tool_calls") for d in data_lines(openai.body)]
    pieces = [call[0]["function"]["arguments"] for call in calls if call]
    arguments = json.dumps({"channel": CHANNEL, "text": text})
    assert pieces == ["", *(arguments[i : i + 5] for i in range(0, len(arguments), 5))]


@pytest.mark.parametrize("mode", MODES)
def test_a_secret_a_write_ends_inside_is_redacted_in_the_joined_stream(run_workflow, mode):
    """A secret split across two chunk writes is redacted in the joined stream.

    Redaction reads the stream joined, so the secret reaches disk as its placeholder. A chunk
    boundary keeps its place, except the one inside the rewritten line, which moves to its end."""
    bare = run_workflow(W, "secret_split_across_writes", "bare")
    shadow = run_workflow(W, "secret_split_across_writes", mode)
    assert bare.exit_code == shadow.exit_code == 0
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    assert shadow.result()["caller_text"] == shadow.result()["answer"] == LONG_ANSWER
    assert shadow.exchange_lines() == LLM_LINES
    # Before redaction: the stream as sent, with a chunk boundary inside the secret.
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


@pytest.mark.parametrize("mode", MODES)
def test_two_streams_at_once_are_each_stored_against_their_own_request(run_workflow, mode):
    """Two concurrent streams are each stored whole, against the request they answer.

    The fake LLM holds each provider's two calls until both arrive, and each answer names its
    caller, so a chunk copied into the other recording would show."""
    bare = run_workflow(W, "two_callers", "bare")
    shadow = run_workflow(W, "two_callers", mode)
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
    # Each chat is its own run, and every call it made is stored in it alone, by the `Irimi-Run`
    # the SDK put on each request.
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
    process = shadow.stored().load_run(shadow.engine_run_id()).events
    assert process == []


@pytest.mark.parametrize("mode", MODES)
def test_the_callers_send_function_is_stored_by_its_repr_and_cannot_be_replayed(run_workflow, mode):
    """A callable trigger argument is stored as its repr and makes the run not replayable by call.

    `send` is a closure over the caller's connection. `irimi replay` refuses the run, and `--args`
    cannot pass a callable, so only `-- CMD` replays it, as every replay below does."""
    shadow = run_workflow(W, "normal", mode)
    [run] = shadow.sdk_runs()
    assert run.trigger is not None and not run.trigger.replayable
    assert run.trigger.entrypoint == "tests.e2e.workflows.agents.w07_streaming_assistant.agent:chat"
    args = run.trigger.args
    assert isinstance(args, dict) and args["question"] == "How do refunds work?"
    send = args["send"]
    assert isinstance(send, dict) and set(send) == {"__irimi_repr__"}
    assert str(send["__irimi_repr__"]).startswith("<function ")
    code, out, err = shadow.command("replay", run.run_id, "--store", str(shadow.store_root()))
    assert (code, out) == (2, [])
    assert err == [
        f"error: run {run.run_id}'s trigger arguments were not recorded replayably (a value was "
        "recorded as its description); pass --args"
    ]


@pytest.mark.parametrize("mode", MODES)
def test_a_gzip_bomb_streams_to_the_agent_whole_and_is_recorded_cut(run_workflow, mode):
    """A gzip-bombed stream reaches the agent as sent and is recorded cut, flagged once.

    The 2 MiB decode to a GiB. irimi passes every chunk on, decodes only to its cap, and records the
    stored share of the decoded lines as one chunk; the secret in it never reaches disk whole."""
    bare = run_workflow(W, "gzip_bomb_stream", "bare")
    shadow = run_workflow(W, "gzip_bomb_stream", mode)
    assert bare.exit_code == shadow.exit_code == 4
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    result = shadow.result()
    said = f"The test key is {SECRET}. Keep it out of logs."
    assert (result["outcome"], result["error"]) == (
        "upstream_broke",
        "anthropic_stream: ended without message_stop",
    )
    assert result["caller_text"] == said
    assert result["caller_events"][0] == "start" and result["caller_events"][-1] == "error"
    # The agent read every byte the service wrote, in both modes.
    for run in (bare, shadow):
        [sent] = run.world.llm.sent
        assert run.events("stream_encoded", with_time=False) == [
            {
                "event": "stream_encoded",
                "label": "anthropic_stream",
                "encoding": "gzip",
                "bytes": len(sent),
                "sha256": hashlib.sha256(sent).hexdigest(),
                "decoded": 1024 * 1024,
            }
        ]
    assert bare.world.llm.sent == shadow.world.llm.sent
    assert [c["answered_by"] for c in shadow.calls("anthropic_stream")] == [None]
    assert shadow.exchange_lines() == [
        LLM_LINES[0],
        "real          llm       POST api.anthropic.com/v1/messages -> 200  [stream-truncated]",
    ]
    # Recorded as the start of what it decodes to, to its last whole line, as one chunk.
    [head] = shadow.world.llm.heads
    lines = MAX_STORED_BODY // len(BOMB_LINE) + 1
    recorded = redact.complete_lines((head + BOMB_LINE * lines)[:MAX_STORED_BODY])
    [handed] = [e for e in shadow.handed if e.request.path == "/v1/messages"]
    assert handed.response is not None and handed.response.body == recorded
    assert (handed.flags, handed.stream_chunks) == ((Flag.STREAM_TRUNCATED,), (len(recorded),))
    # Stored in the chat's run, flagged once.
    stream = stored_exchanges(shadow)["/v1/messages"]
    [chat] = shadow.sdk_runs()
    assert stream.run_id == chat.run_id
    assert stream.flags == (Flag.STREAM_TRUNCATED,)
    assert stream.response is not None and stream.response.body.startswith(MESSAGE_START)
    # In 12-character deltas the secret is never whole on a line, as in `secret_in_stream`.
    assert text_deltas(stream.response.body) == said
    assert stream.response.body == recorded
    assert stream.stream_chunks == (len(stream.response.body),)
    # The run and, under `irimi shadow`, the process run; serve mode has none.
    runs = 2 if mode == "shadow" else 1
    assert [r.dropped_events for r in shadow.stored().list_runs()] == [0] * runs
    written = [
        path
        for root in (shadow.home, shadow.cwd, shadow.tmp)
        for path in root.rglob("*")
        if path.is_file() and SECRET.encode() in path.read_bytes()
    ]
    assert written == []


def test_a_streamed_gzip_bomb_costs_irimi_its_cap_in_memory_not_what_it_decodes_to(traced_run):
    """A streamed gzip bomb costs irimi at most twice its decoding cap and a margin in memory."""
    result, peak = traced_run(W, "gzip_bomb_stream")
    assert result.exit_code == 4
    assert "[stream-truncated]" in result.exchange_lines()[-1]
    assert peak < 2 * MAX_DECODED_BODY + 64 * 1024 * 1024


@pytest.mark.parametrize("mode", MODES)
def test_a_stream_its_trigger_hands_off_is_stored_in_its_run_after_the_run_ended(
    run_workflow, mode
):
    """A stream the trigger returns unread is stored in its run, after the run ended.

    In `hand_off` the request goes out inside the run and the stream ends after it, as W3's
    `task_outlives_trigger` calls do.

    KNOWN BUG (#125): under serve mode the run's block, printed once its end is on disk and before
    the stream is, says the run made no exchange, while the stored run and `irimi runs show` hold
    the stream."""
    bare = run_workflow(W, "hand_off", "bare")
    shadow = run_workflow(W, "hand_off", mode)
    assert shadow.exit_code == bare.exit_code == 0
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    assert shadow.result()["caller_events"][-1] == "done"
    [start] = shadow.events("run.start")
    assert [(c["label"], c["run"]) for c in shadow.calls()] == [
        ("anthropic_stream", start["run"]),
        ("models", None),
    ]
    [run] = shadow.sdk_runs()
    assert (run.run_id, run.end.outcome) == (start["run"], "ok")
    stored = stored_exchanges(shadow)
    stream, models = stored["/v1/messages"], stored["/v1/models"]
    assert stream.response is not None and stream.response.body.endswith(MESSAGE_STOP)
    assert stream.run_id == run.run_id and models.run_id == shadow.engine_run_id()
    assert run.end.ended_at is not None and stream.started_at < run.end.ended_at < stream.ended_at
    if mode == "shadow":
        return
    assert shadow.run_blocks()[run.run_id][-1] == (
        "  0 exchanges · 0 real · 0 answered by irimi (0 faked writes) · 0 sent to targets"
    )
    code, out, err = shadow.runs("show", run.run_id)
    assert (code, err) == (0, [])
    assert (
        out[-1]
        == "  1 exchange · 1 real · 0 answered by irimi (0 faked writes) · 0 sent to targets"
    )


# ---------------------------------------------------------------------- irimi compare

CHAT = "tests.e2e.workflows.agents.w07_streaming_assistant.agent:chat"
# The trigger's args hold the handler's `send` callable, stored as its repr, address and all,
# so no two chats' args are ever the same.
ARGS_DIFFER = "  (the triggers' args differ)"


@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_of_two_chats_streamed_at_once_shows_no_write(run_workflow, mode):
    """`irimi compare` counts no streamed model call as a write, and exits 0 on two chats."""
    shadow = run_workflow(W, "two_callers", mode)
    a, b = (e["run"] for e in shadow.events("run.start"))
    assert shadow.compare_runs(a, b) == (
        0,
        [
            f"irimi compare · {a} vs {b}",
            f"  A  {CHAT}",
            f"  B  {CHAT}",
            ARGS_DIFFER,
            "",
            "  0 same · 0 changed · 0 only in A · 0 only in B",
        ],
        [],
    )


@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_shows_the_post_a_streamed_tool_call_made_and_nothing_else(
    shared_store_run, mode
):
    """`irimi compare` shows the Slack post a streamed tool call made as the one row.

    `normal` and `tool_call_across_deltas` stream the same model calls; the streams and the
    LangSmith trace are no rows. B runs under shadow or serve."""
    normal = shared_store_run(W, "normal")
    posted = shared_store_run(W, "tool_call_across_deltas", mode)
    assert check_invariants(posted) == []
    code, out, err = compare(normal, posted)
    a, b = (result.one("run.start")["run"] for result in (normal, posted))
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
        f"  A  {CHAT}",
        f"  B  {CHAT}",
        ARGS_DIFFER,
        "",
        f'  + ○ slack  post to #{CHANNEL}: "Summary: refunds go back to the card within 5-10 '
        'days."  faked; checked against live state: would succeed',
        "",
        "  0 same · 0 changed · 0 only in A · 1 only in B",
    ]
    assert rows_not_shown(posted, a, b, out) == []


# ------------------------------------------------------------------------------- replay
#
# Every chat's trigger is given its caller's `send`, a callable `irimi replay` cannot call it with
# (above), so each replay here runs the agent's own command (`-- <cmd>`).

RECORDED_LINES = [line.replace("real    ", "recorded") + "  [from-recording]" for line in LLM_LINES]


def test_both_streams_replay_from_the_recording_and_the_agent_reassembles_them(replayed):
    """A replay serves a recorded stream in one piece, and the agent reassembles it as before.

    The trace post is telemetry, which a replay forwards as shadow mode does."""
    recorded, replay = replayed(W, "normal", command=True)
    assert replay.result(with_time=False) == recorded.result(with_time=False)
    assert replay.result()["caller_text"] == ANSWER
    assert replay.exchange_lines() == [*RECORDED_LINES, TRACE_LINE]
    assert [r.host for r in replay.internet.requests()] == ["api.smith.langchain.com"]


def test_a_secret_a_stream_sent_in_pieces_replays_whole_and_matches_its_placeholder(replayed):
    """A secret recorded in pieces replays whole, and the request carrying it matches its record.

    The live request, redacted with the store's key, matches the recorded placeholder."""
    recorded, replay = replayed(W, "secret_in_stream", command=True)
    assert SECRET in replay.result()["caller_text"]
    assert replay.result(with_time=False) == recorded.result(with_time=False)
    assert replay.exchange_lines() == RECORDED_LINES
    assert replay.internet.requests() == []


def test_a_secret_stored_redacted_replays_as_its_placeholder(replayed):
    """A secret stored as its placeholder replays as the placeholder, and nothing diverges."""
    recorded, replay = replayed(W, "secret_split_across_writes", command=True)
    hidden = redact.placeholder(redact.load_key(replay.home), SECRET)
    assert recorded.result()["caller_text"] == LONG_ANSWER
    assert replay.result()["caller_text"] == LONG_ANSWER.replace(SECRET, hidden)
    assert replay.exchange_lines() == RECORDED_LINES
    assert replay.internet.requests() == []


# The replay's flags for a stream recorded cut: a miss, which the fake's fault cuts again. A miss
# keeps `replay-miss` beside the engine's `upstream-error`.
CUT_STREAMS = {
    "caller_disconnects": (Flag.REPLAY_MISS, Flag.UPSTREAM_ERROR, Flag.STREAM_TRUNCATED),
    "upstream_reset": (Flag.REPLAY_MISS, Flag.UPSTREAM_ERROR, Flag.STREAM_TRUNCATED),
    "reset_before_stream": (Flag.REPLAY_MISS, Flag.UPSTREAM_ERROR),
    "gzip_bomb_stream": (Flag.REPLAY_MISS, Flag.STREAM_TRUNCATED),
}


@pytest.mark.parametrize("scenario", CUT_STREAMS)
def test_a_stream_recorded_cut_is_never_served_and_goes_to_the_model_again(replayed, scenario):
    """A replay never serves a stream recorded cut: it is a miss, sent to the model again."""
    recorded, replay = replayed(W, scenario, command=True)
    [cut] = [ex for ex in recorded.reported if ex.request.path == "/v1/messages"]
    assert {Flag.STREAM_TRUNCATED, Flag.UPSTREAM_ERROR} & set(cut.flags)
    [stream] = [ex for ex in replay.reported if ex.request.path == "/v1/messages"]
    assert (stream.answered_by, stream.flags) == ("live", CUT_STREAMS[scenario])
    assert [ex.answered_by for ex in replay.reported if ex.request.path == "/v1/embeddings"] == [
        "recording"
    ]
    assert [(r.method, r.host, r.path) for r in replay.internet.requests()] == [
        ("POST", "api.anthropic.com", "/v1/messages")
    ]
    assert replay.agent_exit_code() == recorded.exit_code


def test_replaying_one_of_two_lockstep_chats_leaves_the_other_waiting(run_workflow, replayed):
    """Replaying one of two lockstep chats leaves the other's live stream waiting, and exits 4.

    The replayed chat is answered from the recording; the other misses, and its stream waits for a
    partner until the fake gives up. No write is forwarded."""
    recorded = run_workflow(W, "two_callers", "shadow")
    for source in recorded.sdk_runs():
        _, replay = replayed(W, "two_callers", run_id=source.run_id, command=True)
        assert (recorded.exit_code, replay.agent_exit_code()) == (0, 4)
        answered = [ex for ex in replay.reported if "from-recording" in ex.flags]
        assert sorted(ex.request.path for ex in answered) == [
            "/v1/chat/completions",
            "/v1/embeddings",
            "/v1/messages",
        ]
        assert {ex.run_id for ex in answered} != {source.run_id}
        [left] = [
            ex
            for ex in replay.reported
            if ex.request.path == "/v1/messages" and (ex.answered_by == "live")
        ]
        # A miss whose upstream failed, and says both, as above.
        assert (left.response, left.flags) == (None, (Flag.REPLAY_MISS, Flag.UPSTREAM_ERROR))
        assert [r.path for r in replay.internet.requests()] == ["/v1/embeddings", "/v1/messages"]
        assert replay.internet.writes() == []


ENCODED = ("encoding", "bytes", "sha256")


@pytest.mark.parametrize("mode", MODES)
def test_a_gzip_encoded_stream_reaches_the_agent_as_sent_and_is_stored_decoded(run_workflow, mode):
    """A gzip stream reaches the agent as sent and is stored decoded, with its `content-encoding`.

    The agent decodes it itself, bare and under irimi alike."""
    bare = run_workflow(W, "gzip_stream", "bare")
    shadow = run_workflow(W, "gzip_stream", mode)
    assert shadow.result(with_time=False) == bare.result(with_time=False)
    [sent] = bare.events("stream_encoded")
    [got] = shadow.events("stream_encoded")
    assert {k: got[k] for k in ENCODED} == {k: sent[k] for k in ENCODED}
    assert sent["encoding"] == "gzip"
    [stream] = [ex for ex in shadow.reported if ex.request.path == "/v1/messages"]
    assert stream.response.header("content-encoding") == "gzip"
    assert stream.response.body.startswith(b"event: message_start")
    assert stream.flags == ()


def test_a_gzip_encoded_stream_replays_decoded_without_its_encoding(replayed):
    """A replay serves a recorded gzip stream decoded, without its `content-encoding` header.

    Under the recorded header the agent would gunzip plain text and fail."""
    recorded, replay = replayed(W, "gzip_stream", command=True)
    assert recorded.events("stream_encoded") and replay.events("stream_encoded") == []
    assert replay.events("stream_error") == []
    assert replay.result(with_time=False) == recorded.result(with_time=False)
    assert [c["answered_by"] for c in replay.calls("anthropic_stream")] == ["recording"]
    assert replay.internet.requests() == []
