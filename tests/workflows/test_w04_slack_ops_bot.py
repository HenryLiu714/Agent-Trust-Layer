"""W4 `slack_ops_bot`: a Slack Events API bot, pinned per scenario.

The events are in `examples/workflows/w04_slack_ops_bot/agent.py`. Its calls carry no labels, so
`Result.answered()` names each by host and path.
"""

import pytest

from examples.workflows.harness.run import CANARIES, check_invariants, compare, rows_not_shown
from irimi import redact
from irimi.exchange import Exchange

W = "w04_slack_ops_bot"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi (#77).
MODES = ("shadow", "serve")
WEBHOOK_PATH = CANARIES["SLACK_WEBHOOK_PATH"]
API = "slack.com/api/"

MENTION_SHADOW = [
    (f"{API}conversations.replies", 200, None),
    (f"{API}users.info", 200, None),
    ("api.anthropic.com/v1/messages", 200, None),
    (f"{API}chat.postMessage", 200, "fake-L1"),
    (f"{API}reactions.add", 200, "fake-L0"),
    (f"{API}files.upload", 200, "fake-L0"),
    (f"hooks.slack.com{WEBHOOK_PATH}", 200, "fake-L0"),
    (f"{API}conversations.replies", 200, "overlay"),
]


def test_url_verification_answers_the_challenge_and_calls_nothing(run_workflow):
    for mode in ("bare", "shadow", "serve"):
        result = run_workflow(W, "url_verification", mode)
        assert result.result()["answers"] == [{"status": 200, "challenge": "chal-4711"}]
        assert result.calls() == []
        assert result.internet.requests() == []


@pytest.mark.parametrize("mode", MODES)
def test_a_mention_is_one_run_and_its_reply_is_read_back_through_the_overlay(run_workflow, mode):
    shadow = run_workflow(W, "mention_in_thread", mode)
    assert shadow.answered() == MENTION_SHADOW
    assert shadow.one("readback")["sees_reply"] is True
    # One trigger, one run, and every call the run made carried its id (stripped by irimi).
    assert [e["name"] for e in shadow.events("run.start")] == ["on_mention"]
    run_id = shadow.one("run.start")["run"]
    assert {c["run"] for c in shadow.calls()} == {run_id}
    # L3 probed the channel before faking the post (#45).
    lines = shadow.exchange_lines()
    assert "live      read      POST slack.com/api/conversations.info -> 200" in lines
    assert (
        '  ○ post to #C0OPS: "Payouts are on schedule."  unvalidated (L3 preconditions passed)'
        in (shadow.summaries())
    )
    # The bare run sees the same thing, from the real (fake) Slack.
    bare = run_workflow(W, "mention_in_thread", "bare")
    assert bare.one("readback")["sees_reply"] is True
    # The L0 `reactions.add` fake answers the one key Slack answers.
    assert shadow.one("reacted")["keys"] == bare.one("reacted")["keys"] == ["ok"]
    assert [r.path for r in bare.internet.writes()] == [
        "/api/chat.postMessage",
        "/api/reactions.add",
        "/api/files.upload",
        WEBHOOK_PATH,
    ]


@pytest.mark.parametrize("mode", MODES)
def test_the_incoming_webhooks_secret_path_is_printed_until_87_lands(run_workflow, mode):
    shadow = run_workflow(W, "mention_in_thread", mode)
    # #87: the per-exchange line still prints the webhook's secret path. When #87 lands this
    # assertion flips: the line must name the route and not the credential.
    assert (
        f"fake-L0   write     POST hooks.slack.com{WEBHOOK_PATH} -> 200  [fidelity:L0]"
        in shadow.exchange_lines()
    )
    # The store already hides it (#69, #70): the whole path is one placeholder, and invariant 4
    # finds the canary in no file under irimi's home.
    [hook] = [
        e
        for e in shadow.stored_events()
        if isinstance(e, Exchange) and e.request.host.startswith("hooks.")
    ]
    placeholder = redact.placeholder(redact.load_key(shadow.home), WEBHOOK_PATH)
    assert (hook.request.path, hook.answered_by) == ("/" + placeholder, "fake-L0")


@pytest.mark.parametrize("mode", MODES)
def test_runs_show_prints_the_webhook_post_as_the_maps_sentence_from_its_stored_run(
    run_workflow, mode
):
    """The stored webhook path is a placeholder no route matches, so `irimi runs show` found the
    route by the operation the exchange recorded (#72). Its event line spells the placeholder, as
    stored, and its summary line is the map's sentence, as `irimi shadow` printed it."""
    shadow = run_workflow(W, "mention_in_thread", mode)
    (run_id,) = [r.run_id for r in shadow.sdk_runs()]
    code, out, err = shadow.runs("show", run_id)
    assert (code, err) == (0, [])
    placeholder = redact.placeholder(redact.load_key(shadow.home), WEBHOOK_PATH)
    assert f"fake-L0   write     POST hooks.slack.com/{placeholder} -> 200  [fidelity:L0]" in out
    sentence = '  ○ post via webhook: "ops bot answered alice"  unvalidated (L0)'
    assert sentence in shadow.summaries()
    assert sentence in out
    assert not any(line.startswith("  ○ POST hooks.slack.com") for line in out)


@pytest.mark.parametrize("mode", MODES)
def test_the_overlay_line_hangs_under_the_webhook_post_it_cannot_have_seen(run_workflow, mode):
    shadow = run_workflow(W, "mention_in_thread", mode)
    summary = shadow.summaries()
    webhook = summary.index('  ○ post via webhook: "ops bot answered alice"  unvalidated (L0)')
    # LOOKS WRONG: the read-back saw the chat.postMessage reply, but its ↳ line hangs under the
    # incoming-webhook post, because hooks.slack.com is the same `slack` service and the webhook
    # was the run's most recent authored Slack write. No read can ever show a webhook post.
    assert summary[webhook + 1] == "    ↳ POST /api/conversations.replies saw it  overlay"


@pytest.mark.parametrize("mode", MODES)
def test_a_reply_under_the_runs_own_minted_thread_is_answered_from_the_write_log(
    run_workflow, mode
):
    shadow = run_workflow(W, "own_thread", mode)
    assert shadow.answered()[3:5] == [
        (f"{API}chat.postMessage", 200, "fake-L1"),
        (f"{API}chat.postMessage", 200, "fake-L1"),
    ]
    # The thread's parent exists only in irimi's write log: real Slack says `thread_not_found`,
    # and irimi answers the read from the write log instead (#52).
    readback = shadow.one("readback")
    assert (readback["ok"], readback["sees_reply"], readback["messages"]) == (True, True, 2)
    assert shadow.answered()[-1] == (f"{API}conversations.replies", 200, "overlay")
    stub_saw = [r for r in shadow.internet.requests() if r.path == "/api/conversations.replies"]
    assert len(stub_saw) == 2  # the live read before, and the forwarded read-back irimi replaced
    bare = run_workflow(W, "own_thread", "bare")
    assert (bare.one("readback")["sees_reply"], bare.one("readback")["messages"]) == (True, 2)


@pytest.mark.parametrize("mode", MODES)
def test_a_post_to_a_channel_name_is_unprobed_and_its_read_back_misses(run_workflow, mode):
    shadow = run_workflow(W, "channel_by_name", mode)
    bare = run_workflow(W, "channel_by_name", "bare")
    # Real Slack answers a post to `#ops` with the channel's id; irimi's fake echoes the name back
    # (#44), so the agent's read-back names a channel Slack does not know.
    assert bare.one("post_reply")["channel"] == "C0OPS"
    assert shadow.one("post_reply")["channel"] == "#ops"
    assert bare.one("readback")["sees_reply"] is True
    readback = shadow.one("readback")
    assert (readback["ok"], readback["error"], readback["sees_reply"]) == (
        False,
        "channel_not_found",
        False,
    )
    # A name is not probed by L3 (#45), so the post is L2, not "L3 preconditions passed".
    # LOOKS WRONG (no issue yet; #61 covers ids only): the doubled `##` is the human template's
    # `#{channel}` over a name that already has one.
    lines = shadow.exchange_lines()
    assert "live      read      POST slack.com/api/conversations.info -> 200" not in lines
    summary = shadow.summaries()
    assert '  ○ post to ##ops: "Payouts are on schedule."  unvalidated (L2)' in summary
    assert "    ↳ POST /api/conversations.replies did not show it  live (partial)" in summary


@pytest.mark.parametrize("mode", MODES)
def test_a_missing_scope_is_refused_by_slack_but_faked_as_passed_by_shadow(run_workflow, mode):
    bare = run_workflow(W, "missing_scope", "bare")
    shadow = run_workflow(W, "missing_scope", mode)
    assert (bare.exit_code, shadow.exit_code) == (1, 0)
    assert bare.one("post_reply")["error"] == "missing_scope"
    assert shadow.one("post_reply")["ok"] is True
    # LOOKS WRONG: L3's probe is `conversations.info`, which a token without `chat:write` can
    # still call, so the check passes and the summary claims "L3 preconditions passed" for a post
    # Slack would have refused. L3 cannot see scopes; `not_evaluable` would be the honest answer.
    assert (
        '  ○ post to #C0OPS: "Payouts are on schedule."  unvalidated (L3 preconditions passed)'
        in (shadow.summaries())
    )


@pytest.mark.parametrize("mode", MODES)
def test_an_archived_channel_is_refused_the_same_way_bare_and_under_shadow(run_workflow, mode):
    bare = run_workflow(W, "archived_channel", "bare")
    shadow = run_workflow(W, "archived_channel", mode)
    assert bare.exit_code == shadow.exit_code == 1
    assert bare.one("post_reply")["error"] == shadow.one("post_reply")["error"] == "is_archived"
    assert '  ✗ post to #C0OLDOPS: "Payouts are on schedule."  would fail: is_archived' in (
        shadow.summaries()
    )
    # The agent does not read back after a refused post.
    assert shadow.events("readback") == []


def test_a_retried_delivery_is_acknowledged_and_handled_once(run_workflow):
    for mode in ("bare", "shadow", "serve"):
        result = run_workflow(W, "duplicate_delivery", mode)
        deliveries = [(d["retry"], d["duplicate"]) for d in result.events("delivery")]
        assert deliveries == [(None, False), ("1", True)]
        assert result.result()["answers"] == [{"status": 200, "ok": True}] * 2
        posts = [c for c in result.calls() if c["url"] == "slack.com/api/chat.postMessage"]
        assert len(posts) == 1
    shadow = run_workflow(W, "duplicate_delivery", "shadow")
    assert len(shadow.events("run.start")) == 1
    # Under serve mode (#77) the two deliveries are one run, stored and summarized once: the
    # retry is acknowledged before a trigger is called, so it starts none.
    serve = run_workflow(W, "duplicate_delivery", "serve")
    [run] = serve.events("run.start")
    assert [r.run_id for r in serve.stored().list_runs()] == [run["run"]]
    [block] = serve.run_blocks().values()
    assert [line for line in block if "○ post to" in line] == [
        '  ○ post to #C0OPS: "Payouts are on schedule."  unvalidated (L3 preconditions passed)'
    ]


# ---------------------------------------------------------------------- irimi compare (#79)

ON_MENTION = "on_mention  examples.workflows.w04_slack_ops_bot.agent:on_mention  agent -"
ANSWER_POST = 'post to #C0OPS: "Payouts are on schedule."  unvalidated (L3 preconditions passed)'
WEBHOOK_POST = '○ post via webhook: "ops bot answered alice"  unvalidated (L0)'
AGAINST_A_PLAIN_MENTION = {
    # Slack would refuse the bot's post for a missing scope, which L3 cannot see (#45): the run
    # is faked as if it posted, so it compares the same, exit 0.
    "missing_scope": (
        0,
        [],
        [
            f"  = ○ {ANSWER_POST}",
            "  = ○ add :eyes: to a message in C0OPS  unvalidated (L0)",
            "  = ○ upload status.csv to C0OPS  unvalidated (L0)",
            f"  = {WEBHOOK_POST}",
        ],
    ),
    # The same mention in an archived channel: L3 says the post would fail, and the reaction and
    # the upload go to the other channel. The webhook's post is the same.
    "archived_channel": (
        1,
        ["  (the triggers' args differ)"],
        [
            f"  ~ A ○ {ANSWER_POST}",
            '    B ✗ post to #C0OLDOPS: "Payouts are on schedule."  would fail: is_archived',
            "  ~ A ○ add :eyes: to a message in C0OPS  unvalidated (L0)",
            "    B ○ add :eyes: to a message in C0OLDOPS  unvalidated (L0)",
            "  ~ A ○ upload status.csv to C0OPS  unvalidated (L0)",
            "    B ○ upload status.csv to C0OLDOPS  unvalidated (L0)",
            f"  = {WEBHOOK_POST}",
        ],
    ),
}


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", AGAINST_A_PLAIN_MENTION)
def test_irimi_compare_shows_what_l3_saw_of_slack_s_refusals(shared_store_run, scenario, mode):
    """`mention_in_thread` against a mention Slack would refuse, recorded into one store, B under
    shadow or serve (#79): a refusal L3 checks is a `~` whose B line carries `✗`, and one it
    cannot see is no difference at all."""
    code, differ, rows = AGAINST_A_PLAIN_MENTION[scenario]
    plain = shared_store_run(W, "mention_in_thread")
    refused = shared_store_run(W, scenario, mode)
    assert check_invariants(refused) == []
    out = compare(plain, refused)
    a, b = (result.one("run.start")["run"] for result in (plain, refused))
    totals = [sum(row.startswith(p) for row in rows) for p in ("  = ", "  ~ ")]
    assert out == (
        code,
        [
            f"irimi compare · {a} vs {b} · a comparison, not a verdict: nothing was replayed",
            f"  A  {ON_MENTION}",
            f"  B  {ON_MENTION}",
            *differ,
            "",
            *rows,
            "",
            "  {} same · {} changed · 0 only in A · 0 only in B".format(*totals),
        ],
        [],
    )
    assert rows_not_shown(refused, a, b, out[1]) == []


@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_tells_a_newline_from_a_typed_backslash_n(shared_store_run, mode):
    """`two_line_answer` posts an answer holding a newline, and `typed_escape_answer` one holding
    a backslash and an `n`. irimi prints each escaped, so the summary's line does not break
    (#72), and the two print alike; compare compares what was posted, so the pair is changed, and
    prints a pair whose lines print alike with each backslash escaped too, so its two lines say
    how the posts differ: the newline as `\\n`, the typed one as `\\\\n` (#79). Recorded into one
    store, B under shadow or serve."""
    newline = shared_store_run(W, "two_line_answer")
    typed = shared_store_run(W, "typed_escape_answer", mode)
    assert check_invariants(typed) == []
    code, out, err = compare(newline, typed)
    a, b = (result.one("run.start")["run"] for result in (newline, typed))
    l3 = "unvalidated (L3 preconditions passed)"
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b} · a comparison, not a verdict: nothing was replayed",
        f"  A  {ON_MENTION}",
        f"  B  {ON_MENTION}",
        "",
        f'  ~ A ○ post to #C0OPS: "Payouts are on schedule.\\nNext run at 09:00."  {l3}',
        f'    B ○ post to #C0OPS: "Payouts are on schedule.\\\\nNext run at 09:00."  {l3}',
        "  = ○ add :eyes: to a message in C0OPS  unvalidated (L0)",
        "  = ○ upload status.csv to C0OPS  unvalidated (L0)",
        f"  = {WEBHOOK_POST}",
        "",
        "  3 same · 1 changed · 0 only in A · 0 only in B",
    ]
    assert rows_not_shown(typed, a, b, out) == []


def test_rows_not_shown_finds_a_row_no_summary_printed_and_a_pair_not_told_apart(
    shared_store_run,
):
    """The harness's check that each compared row is a line its own run's summary printed, held
    to catching something: over the newline against the typed backslash-n, it passes compare's
    own output, and finds a row added, a row from the wrong side, and a pair whose escaped lines
    are not the two posts the summaries printed (#79)."""
    newline = shared_store_run(W, "two_line_answer")
    typed = shared_store_run(W, "typed_escape_answer")
    a, b = (result.one("run.start")["run"] for result in (newline, typed))
    code, out, err = compare(newline, typed)
    assert rows_not_shown(typed, a, b, out) == []
    pair = out.index(next(row for row in out if row.startswith("  ~ A ")))
    added = '  + ○ post to #C0OPS: "Payouts are late."  unvalidated (L3 preconditions passed)'
    assert rows_not_shown(typed, a, b, [*out[:-2], added, *out[-2:]]) == [added]
    # B's line printed as a row only A made.
    only_a = f"  - {out[pair + 1].removeprefix('    B ')}"
    assert rows_not_shown(typed, a, b, [*out[:-2], only_a, *out[-2:]]) == [only_a]
    # The pair's lines told apart from each other, but not as the two posts the summaries printed.
    forged = [out[pair].replace("\\nNext", "\\\\\\\\nNext"), out[pair + 1]]
    tampered = [*out[:pair], *forged, *out[pair + 2 :]]
    assert rows_not_shown(typed, a, b, tampered) == forged
