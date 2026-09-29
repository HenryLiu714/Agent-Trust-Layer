"""W4 `slack_ops_bot`: a Slack Events API bot, pinned per scenario.

The labels and events are in `examples/workflows/w04_slack_ops_bot/agent.py`.
"""

from examples.workflows.harness.run import CANARIES

W = "w04_slack_ops_bot"
WEBHOOK_PATH = CANARIES["SLACK_WEBHOOK_PATH"]


def answered(result):
    """`(slack method or host+path, Irimi-Answered-By)` per call, in order."""
    return [(c["url"].removeprefix("slack.com/api/"), c["answered_by"]) for c in result.calls()]


def one(result, event):
    found = result.events(event)
    assert len(found) == 1, f"{event}: {found}"
    return found[0]


MENTION_SHADOW = [
    ("conversations.replies", None),
    ("users.info", None),
    ("api.anthropic.com/v1/messages", None),
    ("chat.postMessage", "fake-L1"),
    ("reactions.add", "fake-L0"),
    ("files.upload", "fake-L0"),
    (f"hooks.slack.com{WEBHOOK_PATH}", "fake-L0"),
    ("conversations.replies", "overlay"),
]


def test_url_verification_answers_the_challenge_and_calls_nothing(run_workflow):
    for mode in ("bare", "shadow"):
        result = run_workflow(W, "url_verification", mode)
        assert result.result()["answers"] == [{"status": 200, "challenge": "chal-4711"}]
        assert result.calls() == []
        assert result.internet.requests() == []


def test_a_mention_is_one_run_and_its_reply_is_read_back_through_the_overlay(run_workflow):
    shadow = run_workflow(W, "mention_in_thread", "shadow")
    assert answered(shadow) == MENTION_SHADOW
    assert one(shadow, "readback")["sees_reply"] is True
    # One trigger, one run, and every call the run made carried its id (stripped by irimi).
    assert [e["name"] for e in shadow.events("run.start")] == ["on_mention"]
    run_id = one(shadow, "run.start")["run"]
    assert {c["run"] for c in shadow.calls()} == {run_id}
    # L3 probed the channel before faking the post (#45).
    lines = shadow.exchange_lines()
    assert "live      read      POST slack.com/api/conversations.info -> 200" in lines
    assert (
        '  ○ post to #C0OPS: "Payouts are on schedule."  unvalidated (L3 preconditions passed)'
        in (shadow.summary())
    )
    # The bare run sees the same thing, from the real (fake) Slack.
    bare = run_workflow(W, "mention_in_thread", "bare")
    assert one(bare, "readback")["sees_reply"] is True
    assert [r.path for r in bare.internet.writes()] == [
        "/api/chat.postMessage",
        "/api/reactions.add",
        "/api/files.upload",
        WEBHOOK_PATH,
    ]


def test_the_incoming_webhooks_secret_path_is_printed_until_87_lands(run_workflow):
    shadow = run_workflow(W, "mention_in_thread", "shadow")
    # #87: the per-exchange line still prints the webhook's secret path. When #87 lands this
    # assertion flips: the line must name the route and not the credential.
    assert (
        f"fake-L0   write     POST hooks.slack.com{WEBHOOK_PATH} -> 200  [fidelity:L0]"
        in shadow.exchange_lines()
    )


def test_the_overlay_line_hangs_under_the_webhook_post_it_cannot_have_seen(run_workflow):
    shadow = run_workflow(W, "mention_in_thread", "shadow")
    summary = shadow.summary()
    webhook = summary.index('  ○ post via webhook: "ops bot answered alice"  unvalidated (L0)')
    # Looks wrong: the read-back saw the chat.postMessage reply, but its ↳ line hangs under the
    # incoming-webhook post, because hooks.slack.com is the same `slack` service and the webhook
    # was the run's most recent authored Slack write. No read can ever show a webhook post.
    assert summary[webhook + 1] == "    ↳ POST /api/conversations.replies saw it  overlay"


def test_a_reply_under_the_runs_own_minted_thread_is_answered_from_the_write_log(run_workflow):
    shadow = run_workflow(W, "own_thread", "shadow")
    assert answered(shadow)[3:5] == [
        ("chat.postMessage", "fake-L1"),
        ("chat.postMessage", "fake-L1"),
    ]
    # The thread's parent exists only in irimi's write log: real Slack says `thread_not_found`,
    # and irimi answers the read from the write log instead (#52).
    readback = one(shadow, "readback")
    assert (readback["ok"], readback["sees_reply"], readback["messages"]) == (True, True, 2)
    assert answered(shadow)[-1] == ("conversations.replies", "overlay")
    stub_saw = [r for r in shadow.internet.requests() if r.path == "/api/conversations.replies"]
    assert len(stub_saw) == 2  # the live read before, and the forwarded read-back irimi replaced
    bare = run_workflow(W, "own_thread", "bare")
    assert (one(bare, "readback")["sees_reply"], one(bare, "readback")["messages"]) == (True, 2)


def test_a_post_to_a_channel_name_is_unprobed_and_its_read_back_misses(run_workflow):
    shadow = run_workflow(W, "channel_by_name", "shadow")
    bare = run_workflow(W, "channel_by_name", "bare")
    # Real Slack answers a post to `#ops` with the channel's id; irimi's fake echoes the name back
    # (#44), so the agent's read-back names a channel Slack does not know.
    assert one(bare, "post_reply")["channel"] == "C0OPS"
    assert one(shadow, "post_reply")["channel"] == "#ops"
    assert one(bare, "readback")["sees_reply"] is True
    readback = one(shadow, "readback")
    assert (readback["ok"], readback["error"], readback["sees_reply"]) == (
        False,
        "channel_not_found",
        False,
    )
    # A name is not probed by L3 (#45), so the post is L2, not "L3 preconditions passed". The
    # doubled `##` is the human template's `#{channel}` over a name that already has one.
    lines = shadow.exchange_lines()
    assert "live      read      POST slack.com/api/conversations.info -> 200" not in lines
    summary = shadow.summary()
    assert '  ○ post to ##ops: "Payouts are on schedule."  unvalidated (L2)' in summary
    assert "    ↳ POST /api/conversations.replies did not show it  live (partial)" in summary


def test_a_missing_scope_is_refused_by_slack_but_faked_as_passed_by_shadow(run_workflow):
    bare = run_workflow(W, "missing_scope", "bare")
    shadow = run_workflow(W, "missing_scope", "shadow")
    assert (bare.exit_code, shadow.exit_code) == (1, 0)
    assert one(bare, "post_reply")["error"] == "missing_scope"
    assert one(shadow, "post_reply")["ok"] is True
    # Looks wrong: L3's probe is `conversations.info`, which a token without `chat:write` can
    # still call, so the check passes and the summary claims "L3 preconditions passed" for a post
    # Slack would have refused. L3 cannot see scopes; `not_evaluable` would be the honest answer.
    assert (
        '  ○ post to #C0OPS: "Payouts are on schedule."  unvalidated (L3 preconditions passed)'
        in (shadow.summary())
    )


def test_an_archived_channel_is_refused_the_same_way_bare_and_under_shadow(run_workflow):
    bare = run_workflow(W, "archived_channel", "bare")
    shadow = run_workflow(W, "archived_channel", "shadow")
    assert bare.exit_code == shadow.exit_code == 1
    assert one(bare, "post_reply")["error"] == one(shadow, "post_reply")["error"] == "is_archived"
    assert '  ✗ post to #C0OLDOPS: "Payouts are on schedule."  would fail: is_archived' in (
        shadow.summary()
    )
    # The agent does not read back after a refused post.
    assert shadow.events("readback") == []


def test_a_retried_delivery_is_acknowledged_and_handled_once(run_workflow):
    for mode in ("bare", "shadow"):
        result = run_workflow(W, "duplicate_delivery", mode)
        deliveries = [(d["retry"], d["duplicate"]) for d in result.events("delivery")]
        assert deliveries == [(None, False), ("1", True)]
        assert result.result()["answers"] == [{"status": 200, "ok": True}] * 2
        posts = [c for c in result.calls() if c["url"] == "slack.com/api/chat.postMessage"]
        assert len(posts) == 1
    shadow = run_workflow(W, "duplicate_delivery", "shadow")
    assert len(shadow.events("run.start")) == 1
