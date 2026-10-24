"""W4 `slack_ops_bot`: a Slack Events API bot, pinned per scenario.

The events are in `examples/workflows/w04_slack_ops_bot/agent.py`. Its calls carry no labels, so
`Result.answered()` names each by host and path.
"""

import pytest

from examples.workflows.harness.run import CANARIES, check_invariants, compare, rows_not_shown
from irimi.core.exchange import Exchange
from irimi.record import redact

W = "w04_slack_ops_bot"
CHECKED = "faked; checked against live state: would succeed"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
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
    assert [e["name"] for e in shadow.events("run.start")] == [ON_MENTION]
    run_id = shadow.one("run.start")["run"]
    assert {c["run"] for c in shadow.calls()} == {run_id}
    # L3 probed the channel before faking the post.
    lines = shadow.exchange_lines()
    assert "irimi-check   read      POST slack.com/api/conversations.info -> 200" in lines
    assert f'  ○ slack  post to #C0OPS: "Payouts are on schedule."  {CHECKED}' in (
        shadow.summaries()
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
    # The per-exchange line prints the webhook's secret path. When it names the route and not
    # the credential, this assertion flips. See #87.
    assert (
        f"faked         write     POST hooks.slack.com{WEBHOOK_PATH} -> 200"
        in shadow.exchange_lines()
    )
    # The store already hides it: the whole path is one placeholder, and invariant 4
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
    route by the operation the exchange recorded. Its event line spells the placeholder, as
    stored, and its summary line is the map's sentence, as `irimi shadow` printed it."""
    shadow = run_workflow(W, "mention_in_thread", mode)
    (run_id,) = [r.run_id for r in shadow.sdk_runs()]
    code, out, err = shadow.runs("show", run_id)
    assert (code, err) == (0, [])
    placeholder = redact.placeholder(redact.load_key(shadow.home), WEBHOOK_PATH)
    assert f"faked         write     POST hooks.slack.com/{placeholder} -> 200" in out
    sentence = '  ○ slack  post via webhook: "ops bot answered alice"  faked; not checked'
    assert sentence in shadow.summaries()
    assert sentence in out
    assert not any(line.startswith("  ○ POST hooks.slack.com") for line in out)


@pytest.mark.parametrize("mode", MODES)
def test_the_overlay_line_hangs_under_the_webhook_post_it_cannot_have_seen(run_workflow, mode):
    shadow = run_workflow(W, "mention_in_thread", mode)
    summary = shadow.summaries()
    webhook = summary.index(
        '  ○ slack  post via webhook: "ops bot answered alice"  faked; not checked'
    )
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
    # and irimi answers the read from the write log instead.
    readback = shadow.one("readback")
    assert (readback["ok"], readback["sees_reply"], readback["messages"]) == (True, True, 2)
    assert shadow.answered()[-1] == (f"{API}conversations.replies", 200, "overlay")
    stub_saw = [r for r in shadow.internet.requests() if r.path == "/api/conversations.replies"]
    assert len(stub_saw) == 2  # the live read before, and the forwarded read-back irimi replaced
    bare = run_workflow(W, "own_thread", "bare")
    assert (bare.one("readback")["sees_reply"], bare.one("readback")["messages"]) == (True, 2)


def test_a_post_fixture_the_fixtures_file_lacks_is_refused_and_nothing_runs(run_workflow):
    """`services.check` holds every `fixture:` to the fixtures irimi ships when the maps load, so a
    route naming an object `slack.json` lacks refuses the map: no proxy, no store, no agent."""
    for mode in MODES:
        shadow = run_workflow(W, "post_fixture_missing", mode)
        assert shadow.exit_code == 2
        assert shadow.irimi == []
        assert shadow.obs == []
        assert shadow.internet.requests() == []
        assert sorted(path.name for path in shadow.home.iterdir()) == ["ca"]
    assert run_workflow(W, "post_fixture_missing", "bare").exit_code == 0


@pytest.mark.parametrize("mode", MODES)
def test_a_post_to_a_channel_name_is_unprobed_and_its_read_back_misses(run_workflow, mode):
    shadow = run_workflow(W, "channel_by_name", mode)
    bare = run_workflow(W, "channel_by_name", "bare")
    # Real Slack answers a post to `#ops` with the channel's id; irimi's fake echoes the name back,
    # so the agent's read-back names a channel Slack does not know.
    assert bare.one("post_reply")["channel"] == "C0OPS"
    assert shadow.one("post_reply")["channel"] == "#ops"
    assert bare.one("readback")["sees_reply"] is True
    readback = shadow.one("readback")
    assert (readback["ok"], readback["error"], readback["sees_reply"]) == (
        False,
        "channel_not_found",
        False,
    )
    # A name is not probed by L3, so irimi could not check the post.
    # LOOKS WRONG (no issue yet; #61 covers ids only): the doubled `##` is the human template's
    # `#{channel}` over a name that already has one.
    lines = shadow.exchange_lines()
    assert "irimi-check   read      POST slack.com/api/conversations.info -> 200" not in lines
    summary = shadow.summaries()
    assert (
        '  ○ slack  post to ##ops: "Payouts are on schedule."'
        "  faked; could not check: irimi made no check read"
    ) in summary
    assert "    ↳ POST /api/conversations.replies did not show it  real (partial)" in summary


@pytest.mark.parametrize("mode", MODES)
def test_a_missing_scope_is_refused_by_slack_but_faked_as_passed_by_shadow(run_workflow, mode):
    bare = run_workflow(W, "missing_scope", "bare")
    shadow = run_workflow(W, "missing_scope", mode)
    assert (bare.exit_code, shadow.exit_code) == (1, 0)
    assert bare.one("post_reply")["error"] == "missing_scope"
    assert shadow.one("post_reply")["ok"] is True
    # LOOKS WRONG: L3's probe is `conversations.info`, which a token without `chat:write` can
    # still call, so the check passes and the summary says it would succeed for a post
    # Slack would have refused. L3 cannot see scopes; `not_evaluable` would be the honest answer.
    assert f'  ○ slack  post to #C0OPS: "Payouts are on schedule."  {CHECKED}' in (
        shadow.summaries()
    )


@pytest.mark.parametrize("mode", MODES)
def test_an_archived_channel_is_refused_the_same_way_bare_and_under_shadow(run_workflow, mode):
    bare = run_workflow(W, "archived_channel", "bare")
    shadow = run_workflow(W, "archived_channel", mode)
    assert bare.exit_code == shadow.exit_code == 1
    assert bare.one("post_reply")["error"] == shadow.one("post_reply")["error"] == "is_archived"
    assert (
        '  ✗ slack  post to #C0OLDOPS: "Payouts are on schedule."  faked; would fail: is_archived'
        in (shadow.summaries())
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
    # Under serve mode the two deliveries are one run, stored and summarized once: the
    # retry is acknowledged before a trigger is called, so it starts none.
    serve = run_workflow(W, "duplicate_delivery", "serve")
    [run] = serve.events("run.start")
    assert [r.run_id for r in serve.stored().list_runs()] == [run["run"]]
    [block] = serve.run_blocks().values()
    assert [line for line in block if "○ slack  post to" in line] == [
        f'  ○ slack  post to #C0OPS: "Payouts are on schedule."  {CHECKED}'
    ]


# ---------------------------------------------------------------------- irimi compare

ON_MENTION = "examples.workflows.w04_slack_ops_bot.agent:on_mention"
ANSWER_POST = (
    'slack  post to #C0OPS: "Payouts are on schedule."'
    "  faked; checked against live state: would succeed"
)
WEBHOOK_POST = '○ slack  post via webhook: "ops bot answered alice"  faked; not checked'
AGAINST_A_PLAIN_MENTION = {
    # Slack would refuse the bot's post for a missing scope, which L3 cannot see: the run
    # is faked as if it posted, so it compares the same, exit 0.
    "missing_scope": (
        0,
        [],
        [
            f"  = ○ {ANSWER_POST}",
            "  = ○ slack  add :eyes: to a message in C0OPS  faked; not checked",
            "  = ○ slack  upload status.csv to C0OPS  faked; not checked",
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
            '    B ✗ slack  post to #C0OLDOPS: "Payouts are on schedule."'
            "  faked; would fail: is_archived",
            "  ~ A ○ slack  add :eyes: to a message in C0OPS  faked; not checked",
            "    B ○ slack  add :eyes: to a message in C0OLDOPS  faked; not checked",
            "  ~ A ○ slack  upload status.csv to C0OPS  faked; not checked",
            "    B ○ slack  upload status.csv to C0OLDOPS  faked; not checked",
            f"  = {WEBHOOK_POST}",
        ],
    ),
}


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", AGAINST_A_PLAIN_MENTION)
def test_irimi_compare_shows_what_l3_saw_of_slack_s_refusals(shared_store_run, scenario, mode):
    """`mention_in_thread` against a mention Slack would refuse, recorded into one store, B under
    shadow or serve: a refusal L3 checks is a `~` whose B line carries `✗`, and one it
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
            f"irimi compare · {a} vs {b}",
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
    a backslash and an `n`. irimi prints each escaped, so the summary's line does not break,
    and the two print alike; compare compares what was posted, so the pair is changed, and
    prints a pair whose lines print alike with each backslash escaped too, so its two lines say
    how the posts differ: the newline as `\\n`, the typed one as `\\\\n`. Recorded into one
    store, B under shadow or serve."""
    newline = shared_store_run(W, "two_line_answer")
    typed = shared_store_run(W, "typed_escape_answer", mode)
    assert check_invariants(typed) == []
    code, out, err = compare(newline, typed)
    a, b = (result.one("run.start")["run"] for result in (newline, typed))
    l3 = "faked; checked against live state: would succeed"
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
        f"  A  {ON_MENTION}",
        f"  B  {ON_MENTION}",
        "",
        f'  ~ A ○ slack  post to #C0OPS: "Payouts are on schedule.\\nNext run at 09:00."  {l3}',
        f'    B ○ slack  post to #C0OPS: "Payouts are on schedule.\\\\nNext run at 09:00."  {l3}',
        "  = ○ slack  add :eyes: to a message in C0OPS  faked; not checked",
        "  = ○ slack  upload status.csv to C0OPS  faked; not checked",
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
    are not the two posts the summaries printed."""
    newline = shared_store_run(W, "two_line_answer")
    typed = shared_store_run(W, "typed_escape_answer")
    a, b = (result.one("run.start")["run"] for result in (newline, typed))
    _code, out, _err = compare(newline, typed)
    assert rows_not_shown(typed, a, b, out) == []
    pair = out.index(next(row for row in out if row.startswith("  ~ A ")))
    added = f'  + ○ slack  post to #C0OPS: "Payouts are late."  {CHECKED}'
    assert rows_not_shown(typed, a, b, [*out[:-2], added, *out[-2:]]) == [added]
    # B's line printed as a row only A made.
    only_a = f"  - {out[pair + 1].removeprefix('    B ')}"
    assert rows_not_shown(typed, a, b, [*out[:-2], only_a, *out[-2:]]) == [only_a]
    # The pair's lines told apart from each other, but not as the two posts the summaries printed.
    forged = [out[pair].replace("\\nNext", "\\\\\\\\nNext"), out[pair + 1]]
    tampered = [*out[:pair], *forged, *out[pair + 2 :]]
    assert rows_not_shown(typed, a, b, tampered) == forged


@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_escapes_every_write_that_prints_like_another(shared_store_run, mode):
    """B posts the typed backslash-n and then the newline, A the newline alone. The newline post
    is one both made, and the typed one only B made; the two print alike, so compare prints both
    rows with each backslash escaped, the `=` row's newline as `\\n` and the `+` row's typed one
    as `\\\\n`, even though they are no pair."""
    newline = shared_store_run(W, "two_line_answer")
    both = shared_store_run(W, "typed_escape_then_two_lines", mode)
    assert check_invariants(both) == []
    code, out, err = compare(newline, both)
    a, b = (result.one("run.start")["run"] for result in (newline, both))
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
        f"  A  {ON_MENTION}",
        f"  B  {ON_MENTION}",
        "",
        f'  + ○ slack  post to #C0OPS: "Payouts are on schedule.\\\\nNext run at 09:00."  '
        f"{CHECKED}",
        f'  = ○ slack  post to #C0OPS: "Payouts are on schedule.\\nNext run at 09:00."  {CHECKED}',
        "  = ○ slack  add :eyes: to a message in C0OPS  faked; not checked",
        "  = ○ slack  upload status.csv to C0OPS  faked; not checked",
        f"  = {WEBHOOK_POST}",
        "",
        "  4 same · 0 changed · 0 only in A · 1 only in B",
    ]
    assert rows_not_shown(both, a, b, out) == []
    # The `+` row alone, with nothing else printed alike, is not a line B's summary printed.
    alone = [*out[:5], *out[6:]]
    assert rows_not_shown(both, a, b, alone) == [out[4]]
