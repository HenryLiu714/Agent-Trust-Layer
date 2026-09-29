"""W7 `streaming_assistant`: SSE in both directions, pinned per scenario.

The agent forwards Anthropic's text deltas to its own caller as they arrive, and assembles an
OpenAI tool call from its deltas. Streams are read seven bytes at a time, so every event is split.
"""

from examples.workflows.w07_streaming_assistant.scenarios import ANSWER, CHANNEL, SECRET

W = "w07_streaming_assistant"
LLM_LINES = [
    "live      llm       POST api.openai.com/v1/embeddings -> 200",
    "live      llm       POST api.anthropic.com/v1/messages -> 200",
    "live      llm       POST api.openai.com/v1/chat/completions -> 200",
]


def without_time(event):
    return {k: v for k, v in event.items() if k != "t"}


def test_both_streams_pass_through_irimi_whole_and_unchanged(run_workflow):
    bare = run_workflow(W, "normal", "bare")
    shadow = run_workflow(W, "normal", "shadow")
    assert without_time(shadow.result()) == without_time(bare.result())
    result = shadow.result()
    assert result["outcome"] == "ok"
    assert result["caller_text"] == result["answer"] == ANSWER
    assert result["caller_events"][0] == "start" and result["caller_events"][-1] == "done"
    # Every model call is forwarded live, streamed or not, and none is stamped.
    assert shadow.exchange_lines() == LLM_LINES
    assert [c["answered_by"] for c in shadow.calls()] == [None, None, None]


def test_a_secret_inside_a_stream_reaches_the_caller_intact(run_workflow):
    """Redaction is for disk only (#69): the live stream is never rewritten. The store is still
    `NullStore`, so invariant 4 (no canary under irimi's home) holds trivially today; once #70 and
    #71 record SSE bodies, this is the scenario that proves the secret is redacted on disk."""
    shadow = run_workflow(W, "secret_in_stream", "shadow")
    assert SECRET in shadow.result()["caller_text"]
    assert shadow.exchange_lines() == LLM_LINES


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
    assert [without_time(e) for e in bare.events("stream_error")] == [
        {"event": "stream_error", "label": "anthropic_stream", "error": "ConnectionResetError"}
    ]
    # LOOKS WRONG: under shadow the same reset arrives as a clean end of body, so only the missing
    # `message_stop` tells the agent anything broke, and irimi records a whole 200 with no flag.
    # A stored stream (#71) would look complete.
    assert [without_time(e) for e in shadow.events("stream_error")] == [
        {"event": "stream_error", "label": "anthropic_stream", "error": "truncated"}
    ]
    assert shadow.exchange_lines() == LLM_LINES[:2]
    assert shadow.result()["caller_events"] == ["start", "error"]


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
