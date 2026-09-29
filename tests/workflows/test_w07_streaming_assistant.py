"""W7 `streaming_assistant`: SSE in both directions, pinned per scenario.

The agent forwards Anthropic's text deltas to its own caller as they arrive, and assembles an
OpenAI tool call from its deltas. Streams are read seven bytes at a time, so every event is split.
"""

import json

from examples.workflows.w07_streaming_assistant.scenarios import ANSWER, CHANNEL, SECRET
from irimi import trace
from irimi.trace import TelemetrySeen

W = "w07_streaming_assistant"
TRACE_LINE = "live      telemetry POST api.smith.langchain.com/runs -> 202"
LLM_LINES = [
    "live      llm       POST api.openai.com/v1/embeddings -> 200",
    "live      llm       POST api.anthropic.com/v1/messages -> 200",
    "live      llm       POST api.openai.com/v1/chat/completions -> 200",
]


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


def test_a_secret_inside_a_stream_reaches_the_caller_intact_and_never_disk(run_workflow):
    """Redaction is for disk only (#69): the live stream is never rewritten. The store keeps a
    streamed body empty (#28), so nothing of the stream is on disk to find today; once #71
    records SSE bodies, this is the scenario that proves the secret is redacted there. The secret
    is not one of the harness's canaries, so invariant 4 does not look for it: this test does."""
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


def test_a_caller_that_hangs_up_ends_the_run_in_error(run_workflow):
    bare = run_workflow(W, "caller_disconnects", "bare")
    shadow = run_workflow(W, "caller_disconnects", "shadow")
    assert bare.exit_code == shadow.exit_code == 3
    assert shadow.result()["caller_events"] == ["start"]
    assert shadow.result()["outcome"] == "client_gone"
    assert [(e["outcome"], e["error"]) for e in shadow.events("run.end")] == [
        ("error", "ClientGone")
    ]
    # The agent stopped reading Anthropic's stream when its own caller left, and never asked
    # OpenAI anything.
    assert [c["label"] for c in shadow.calls()] == ["embed", "anthropic_stream"]
    # LOOKS WRONG: the upstream answered fine; it was the AGENT that closed the stream. irimi
    # records the abandoned stream as an upstream failure with no response.
    assert shadow.exchange_lines() == [
        LLM_LINES[0],
        "live      llm       POST api.anthropic.com/v1/messages -> -  [upstream-error]",
    ]


def test_an_upstream_reset_mid_stream_reaches_the_agent_as_a_clean_end(run_workflow):
    bare = run_workflow(W, "upstream_reset", "bare")
    shadow = run_workflow(W, "upstream_reset", "shadow")
    assert bare.exit_code == shadow.exit_code == 4
    # Bare, the reset reaches the agent as a reset.
    assert bare.events("stream_error", with_time=False) == [
        {"event": "stream_error", "label": "anthropic_stream", "error": "ConnectionResetError"}
    ]
    # LOOKS WRONG: under shadow the same reset arrives as a clean end of body, so only the missing
    # `message_stop` tells the agent anything broke, and irimi records a whole 200 with no flag.
    # A stored stream (#71) would look complete.
    assert shadow.events("stream_error", with_time=False) == [
        {"event": "stream_error", "label": "anthropic_stream", "error": "truncated"}
    ]
    assert shadow.exchange_lines() == LLM_LINES[:2]
    assert shadow.result()["caller_events"] == ["start", "error"]


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
