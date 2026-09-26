"""Slack's L2 effects (#44): what a faked post does to a later live read, as pure functions."""

import copy
import json

import pytest

from irimi.exchange import Request
from irimi.services.model import Read, Write
from irimi.services.slack import THREAD_FIELDS, apply_read

# Minted `ts` values sit above every real one, as `echo.slack_ts` guarantees (#42).
MINTED1 = "1800000000.000001"
MINTED2 = "1800000000.000002"
REAL_OLD = "1700000000.000100"
REAL_NEW = "1700000000.000200"
BOT = "U00000000"


def _message(ts, text="hi", user=BOT):
    return {"type": "message", "text": text, "user": user, "ts": ts, "bot_id": "B00000000"}


def _post_write(channel="C0123", ts=MINTED1, text="hi", **posted):
    """A `chat.postMessage` as `echo.slack_l1_body` answers it: the envelope echoes the posted
    channel verbatim, and `message.ts` is the envelope's minted `ts`."""
    return Write(
        operation="chat.postMessage",
        posted={"channel": channel, "text": text, **posted},
        answer={"ok": True, "channel": channel, "ts": ts, "message": _message(ts, text)},
    )


def _reply_write(thread_ts, ts=MINTED1, channel="C0123", user=BOT, **posted):
    write = _post_write(channel, ts, text="a reply", thread_ts=thread_ts, **posted)
    write.answer["message"]["user"] = user
    return write


def _read(operation, **posted):
    """A Slack read: a POST whose every parameter is in the JSON body slack_sdk sends."""
    request = Request(
        method="POST",
        scheme="https",
        host="slack.com",
        port=443,
        path=f"/api/{operation}",
        query="",
        headers=(("content-type", "application/json;charset=utf-8"),),
        body=json.dumps(posted).encode(),
    )
    return Read(operation=operation, request=request, posted=posted)


def _history(**posted):
    return _read("conversations.history", **{"channel": "C0123", **posted})


def _replies(ts, **posted):
    return _read("conversations.replies", **{"channel": "C0123", "ts": ts, **posted})


def _history_page(*messages, has_more=False, next_cursor=""):
    return {
        "ok": True,
        "messages": [m if isinstance(m, dict) else _message(m, "real") for m in messages],
        "has_more": has_more,
        "pin_count": 0,
        "response_metadata": {"next_cursor": next_cursor},
    }


def _replies_page(parent, *replies, has_more=False):
    return {
        "ok": True,
        "messages": [parent, *(_message(ts, "real reply") for ts in replies)],
        "has_more": has_more,
    }


def _threaded_parent(ts=REAL_OLD, reply_count=1, users=("U1",), with_users_count=True):
    parent = _message(ts, "parent", user="U1")
    parent.update(
        thread_ts=ts,
        reply_count=reply_count,
        latest_reply=REAL_NEW,
        reply_users=list(users),
    )
    if with_users_count:
        parent["reply_users_count"] = len(users)
    return parent


def _ts_of(document):
    return [m["ts"] for m in document["messages"]]


# ------------------------------------------------------------------------------- history


def test_a_faked_post_is_at_the_head_of_history_page_one():
    out = apply_read(_history(), _history_page(REAL_NEW, REAL_OLD), [_post_write()])
    assert _ts_of(out.document) == [MINTED1, REAL_NEW, REAL_OLD]
    assert out.document["messages"][0] == _message(MINTED1)
    assert out.changed is True
    assert out.partial is False


def test_two_posts_are_in_ts_order_newest_first():
    writes = [_post_write(ts=MINTED1), _post_write(ts=MINTED2)]
    out = apply_read(_history(), _history_page(REAL_OLD), writes)
    assert _ts_of(out.document) == [MINTED2, MINTED1, REAL_OLD]
    assert out.changed is True


def test_a_post_to_another_channel_id_is_skipped_and_not_partial():
    page = _history_page(REAL_OLD)
    before = copy.deepcopy(page)
    out = apply_read(_history(), page, [_post_write(channel="C0999")])
    assert out.document == before
    assert out.changed is False
    assert out.partial is False


def test_a_post_to_a_channel_name_read_back_by_id_is_partial():
    """`chat.postMessage` accepts `#general` and the echo keeps it verbatim, where real Slack
    would have answered with the id. irimi cannot tell whether it is `C0123`, so it says so."""
    page = _history_page(REAL_OLD)
    before = copy.deepcopy(page)
    out = apply_read(_history(), page, [_post_write(channel="#general")])
    assert out.document == before
    assert out.changed is False
    assert out.partial is True


def test_a_post_already_in_the_page_is_not_inserted_twice():
    page = _history_page(_message(MINTED1), REAL_OLD)
    before = copy.deepcopy(page)
    out = apply_read(_history(), page, [_post_write()])
    assert out.document == before
    assert out.changed is False
    assert out.partial is False


def test_a_history_page_is_never_truncated_and_keeps_slacks_paging():
    """Slack's cursor is opaque and server-issued: dropping the oldest real message to keep the
    page at its limit would make the cursor skip it for good (#44)."""
    out = apply_read(
        _history(limit=2),
        _history_page(REAL_NEW, REAL_OLD, has_more=True, next_cursor="bmV4dA=="),
        [_post_write()],
    )
    assert _ts_of(out.document) == [MINTED1, REAL_NEW, REAL_OLD]
    assert out.document["has_more"] is True
    assert out.document["response_metadata"] == {"next_cursor": "bmV4dA=="}
    assert out.changed is True
    assert out.partial is False


def test_a_cursor_page_gets_no_insertion_and_is_not_partial():
    page = _history_page(REAL_OLD)
    before = copy.deepcopy(page)
    out = apply_read(_history(cursor="bmV4dA=="), page, [_post_write()])
    assert out.document == before
    assert out.changed is False
    assert out.partial is False


def test_a_cursor_page_still_updates_a_threaded_parent_on_it():
    out = apply_read(
        _history(cursor="bmV4dA=="),
        _history_page(_threaded_parent()),
        [_reply_write(REAL_OLD)],
    )
    assert _ts_of(out.document) == [REAL_OLD]
    assert out.document["messages"][0]["reply_count"] == 2
    assert out.document["messages"][0]["latest_reply"] == MINTED1
    assert out.changed is True
    assert out.partial is False


def test_a_threaded_reply_is_not_in_history_but_moves_its_parent():
    out = apply_read(
        _history(),
        _history_page(REAL_NEW, _threaded_parent(REAL_OLD)),
        [_reply_write(REAL_OLD, user="U2")],
    )
    assert _ts_of(out.document) == [REAL_NEW, REAL_OLD]
    parent = out.document["messages"][1]
    assert parent["reply_count"] == 2
    assert parent["latest_reply"] == MINTED1
    assert parent["reply_users"] == ["U1", "U2"]
    assert out.changed is True
    assert out.partial is False


def test_a_reply_broadcast_is_in_history_and_partial():
    """Slack marks a broadcast `subtype: thread_broadcast`; irimi's minted message carries none."""
    out = apply_read(
        _history(),
        _history_page(_threaded_parent(REAL_OLD)),
        [_reply_write(REAL_OLD, reply_broadcast=True)],
    )
    assert _ts_of(out.document) == [MINTED1, REAL_OLD]
    assert out.document["messages"][1]["reply_count"] == 2
    assert out.changed is True
    assert out.partial is True


def test_reply_broadcast_on_a_top_level_post_is_not_a_broadcast():
    """Slack ignores `reply_broadcast` without a `thread_ts`, so the post is plain and complete."""
    out = apply_read(_history(), _history_page(REAL_OLD), [_post_write(reply_broadcast=True)])
    assert _ts_of(out.document) == [MINTED1, REAL_OLD]
    assert out.partial is False


def test_reply_broadcast_sent_as_the_text_false_is_not_a_broadcast():
    """A form body carries booleans as text, and `"false"` is truthy."""
    out = apply_read(
        _history(),
        _history_page(_threaded_parent(REAL_OLD)),
        [_reply_write(REAL_OLD, reply_broadcast="false")],
    )
    assert _ts_of(out.document) == [REAL_OLD]
    assert out.partial is False


def test_a_faked_parent_in_history_carries_its_faked_reply():
    out = apply_read(
        _history(),
        _history_page(REAL_OLD),
        [_post_write(ts=MINTED1), _reply_write(MINTED1, ts=MINTED2)],
    )
    assert _ts_of(out.document) == [MINTED1, REAL_OLD]
    assert out.document["messages"][0]["reply_count"] == 1
    assert out.document["messages"][0]["latest_reply"] == MINTED2


# ------------------------------------------------------------------------------- replies


def test_a_reply_is_appended_at_the_tail_of_a_replies_page():
    """Replies come parent first, then OLDEST first, so a faked reply is last (#44)."""
    out = apply_read(
        _replies(REAL_OLD),
        _replies_page(_threaded_parent(REAL_OLD), REAL_NEW),
        [_reply_write(REAL_OLD, ts=MINTED2), _reply_write(REAL_OLD, ts=MINTED1)],
    )
    assert _ts_of(out.document) == [REAL_OLD, REAL_NEW, MINTED1, MINTED2]
    assert out.document["messages"][0]["reply_count"] == 3
    assert out.document["messages"][0]["latest_reply"] == MINTED2
    assert out.changed is True
    assert out.partial is False


def test_a_replies_page_with_more_behind_it_gets_no_reply_but_its_parent_still_counts_it():
    """The reply belongs on the last page, so this one is not `partial`. The parent's counts still
    move: Slack reports the thread's true `reply_count` on every page of it (#44)."""
    out = apply_read(
        _replies(REAL_OLD),
        _replies_page(_threaded_parent(REAL_OLD), REAL_NEW, has_more=True),
        [_reply_write(REAL_OLD)],
    )
    assert _ts_of(out.document) == [REAL_OLD, REAL_NEW]
    assert out.document["messages"][0]["reply_count"] == 2
    assert out.changed is True
    assert out.partial is False


def test_a_middle_replies_page_without_the_parent_is_unchanged():
    page = {"ok": True, "messages": [_message(REAL_NEW, "real reply")], "has_more": True}
    before = copy.deepcopy(page)
    out = apply_read(_replies(REAL_OLD, cursor="bmV4dA=="), page, [_reply_write(REAL_OLD)])
    assert out.document == before
    assert out.changed is False
    assert out.partial is False


def test_a_first_reply_adds_the_closed_thread_field_set():
    parent = _message(REAL_OLD, "parent", user="U1")
    out = apply_read(_replies(REAL_OLD), _replies_page(parent), [_reply_write(REAL_OLD, user="U2")])
    head = out.document["messages"][0]
    assert {name: head[name] for name in THREAD_FIELDS} == {
        "thread_ts": REAL_OLD,
        "reply_count": 1,
        "reply_users_count": 1,
        "latest_reply": MINTED1,
        "reply_users": ["U2"],
    }
    assert "subscribed" not in head
    assert "last_read" not in head
    assert _ts_of(out.document) == [REAL_OLD, MINTED1]
    assert out.changed is True
    assert out.partial is False


def test_a_threaded_parent_is_updated_and_its_users_count_only_when_slack_sent_one():
    counted = apply_read(
        _replies(REAL_OLD),
        _replies_page(_threaded_parent(REAL_OLD), REAL_NEW),
        [_reply_write(REAL_OLD, user="U2")],
    )
    head = counted.document["messages"][0]
    assert head["reply_count"] == 2
    assert head["reply_users"] == ["U1", "U2"]
    assert head["reply_users_count"] == 2
    assert head["thread_ts"] == REAL_OLD

    uncounted = apply_read(
        _replies(REAL_OLD),
        _replies_page(_threaded_parent(REAL_OLD, with_users_count=False), REAL_NEW),
        [_reply_write(REAL_OLD, user="U1")],
    )
    head = uncounted.document["messages"][0]
    assert head["reply_count"] == 2
    assert head["reply_users"] == ["U1"]
    assert "reply_users_count" not in head


def test_a_threaded_parent_gains_no_thread_field_slack_left_out():
    """The `THREAD_FIELDS` exception is for a parent with no thread at all."""
    parent = _threaded_parent(REAL_OLD, with_users_count=False)
    del parent["latest_reply"], parent["reply_users"]
    out = apply_read(_replies(REAL_OLD), _replies_page(parent), [_reply_write(REAL_OLD)])
    head = out.document["messages"][0]
    assert head["reply_count"] == 2
    assert "latest_reply" not in head
    assert "reply_users" not in head
    assert out.partial is False


def test_a_users_count_without_the_users_it_counts_is_partial_and_untouched():
    """With no `reply_users` there is no telling whether the reply's author is a new one."""
    parent = _threaded_parent(REAL_OLD)
    del parent["reply_users"]
    before = copy.deepcopy(parent)
    out = apply_read(
        _replies(REAL_OLD), _replies_page(parent, has_more=True), [_reply_write(REAL_OLD)]
    )
    assert out.document["messages"][0] == before
    assert out.changed is False
    assert out.partial is True


def test_a_reply_to_another_thread_leaves_the_page_alone():
    page = _replies_page(_threaded_parent(REAL_OLD), REAL_NEW)
    before = copy.deepcopy(page)
    out = apply_read(_replies(REAL_OLD), page, [_reply_write("1600000000.000001")])
    assert out.document == before
    assert out.changed is False
    assert out.partial is False


def test_a_faked_reply_carries_the_thread_it_is_in():
    """Real Slack puts `thread_ts` on every reply it shows, and irimi knows the value exactly - the
    caller posted it. A reply without it is a body production cannot produce (#44, #55)."""
    out = apply_read(
        _replies(REAL_OLD),
        _replies_page(_threaded_parent(REAL_OLD), REAL_NEW),
        [_reply_write(REAL_OLD)],
    )
    assert out.document["messages"][-1]["thread_ts"] == REAL_OLD
    assert "parent_user_id" not in out.document["messages"][-1], "not irimi's to know"


def test_a_faked_top_level_post_gains_no_thread_ts():
    """The other half of #55: a post that is in no thread must not claim to be in one."""
    out = apply_read(_history(), _history_page(REAL_OLD), [_post_write()])
    assert "thread_ts" not in out.document["messages"][0]


def test_a_broadcast_in_history_carries_the_thread_it_is_in():
    out = apply_read(
        _history(),
        _history_page(_threaded_parent(REAL_OLD)),
        [_reply_write(REAL_OLD, reply_broadcast=True)],
    )
    assert out.document["messages"][0]["thread_ts"] == REAL_OLD


def test_the_write_logs_message_is_never_mutated():
    """The answer's `message` is shared by reference with the write log (#44); a faked parent
    grows thread fields on a copy only."""
    parent = _post_write(ts=MINTED1)
    original = copy.deepcopy(parent.answer)
    apply_read(_history(), _history_page(REAL_OLD), [parent, _reply_write(MINTED1, ts=MINTED2)])
    assert parent.answer == original


# ---------------------------------------------------------------------------- the window


def test_latest_at_a_real_ts_excludes_the_minted_message():
    page = _history_page(REAL_OLD)
    before = copy.deepcopy(page)
    out = apply_read(_history(latest=REAL_NEW), page, [_post_write()])
    assert out.document == before
    assert out.changed is False
    assert out.partial is False


def test_oldest_zero_includes_the_minted_message():
    out = apply_read(_history(oldest=0), _history_page(REAL_OLD), [_post_write()])
    assert _ts_of(out.document) == [MINTED1, REAL_OLD]
    assert out.partial is False

    as_text = apply_read(_history(oldest="0"), _history_page(REAL_OLD), [_post_write()])
    assert _ts_of(as_text.document) == [MINTED1, REAL_OLD]


def test_a_bound_is_compared_as_a_number_and_not_as_text():
    """`"1800000000.000001" < "999999999.000001"` as strings; as a `ts` it is far above."""
    out = apply_read(_history(oldest="999999999.000001"), _history_page(), [_post_write()])
    assert _ts_of(out.document) == [MINTED1]


def test_inclusive_decides_only_equality_with_a_bound():
    shut = apply_read(_history(oldest=MINTED1), _history_page(), [_post_write()])
    assert _ts_of(shut.document) == []
    assert shut.partial is False

    open_ = apply_read(_history(oldest=MINTED1, inclusive=True), _history_page(), [_post_write()])
    assert _ts_of(open_.document) == [MINTED1]


def test_a_malformed_bound_is_partial():
    page = _history_page(REAL_OLD)
    before = copy.deepcopy(page)
    out = apply_read(_history(oldest="yesterday"), page, [_post_write()])
    assert out.document == before
    assert out.changed is False
    assert out.partial is True


# ------------------------------------------------------------------------------ the rest


def test_an_unknown_read_parameter_is_partial():
    page = _history_page(REAL_OLD)
    before = copy.deepcopy(page)
    out = apply_read(_history(include_pins=True), page, [_post_write()])
    assert out.document == before
    assert out.partial is True
    assert out.changed is False


def test_include_all_metadata_is_known_to_replies_too():
    """Slack documents it on both methods; `REPLIES_PARAMS` is `HISTORY_PARAMS` plus `ts` (#44)."""
    out = apply_read(
        _replies(REAL_OLD, include_all_metadata=True),
        _replies_page(_threaded_parent(REAL_OLD)),
        [_reply_write(REAL_OLD)],
    )
    assert _ts_of(out.document) == [REAL_OLD, MINTED1]
    assert out.partial is False


def test_the_token_parameter_is_known():
    out = apply_read(_history(token="xoxb-1"), _history_page(REAL_OLD), [_post_write()])
    assert _ts_of(out.document) == [MINTED1, REAL_OLD]
    assert out.partial is False


def test_an_error_body_is_never_edited_and_partial_only_when_a_post_applies():
    error = {"ok": False, "error": "ratelimited"}
    mine = apply_read(_history(), dict(error), [_post_write()])
    assert mine.document == error
    assert mine.changed is False
    assert mine.partial is True

    other = apply_read(_history(), dict(error), [_post_write(channel="C0999")])
    assert other.document == error
    assert other.changed is False
    assert other.partial is False


def test_replies_of_a_minted_parent_is_answered_from_the_write_log():
    """#52's Slack half. Slack answers `thread_not_found` at HTTP 200 for a thread it never saw -
    and this thread is one irimi minted, parent and reply both. The page is built rather than
    edited, because there is no live body to preserve, and no status moves: Slack's error is
    already a 200, which is the evidence #52 weighed."""
    parent = _post_write(ts=MINTED1)
    reply = _reply_write(MINTED1, ts=MINTED2)
    out = apply_read(_replies(MINTED1), {"ok": False, "error": "thread_not_found"}, [parent, reply])
    assert out.status is None
    assert out.changed is True
    assert out.partial is False
    assert out.document["ok"] is True
    assert out.document["has_more"] is False
    assert _ts_of(out.document) == [MINTED1, MINTED2]
    # The parent carries the thread fields its own reply gives it, which is what makes a replies
    # page one production could produce (#44's THREAD_FIELDS rule).
    head = out.document["messages"][0]
    assert head["thread_ts"] == MINTED1
    assert head["reply_count"] == 1
    assert head["latest_reply"] == MINTED2


def test_a_minted_parent_with_no_replies_answers_the_message_alone():
    """Real Slack answers `conversations.replies` on an un-replied message with that message and
    nothing else. A minted one is no different (#52)."""
    parent = _post_write(ts=MINTED1)
    out = apply_read(_replies(MINTED1), {"ok": False, "error": "thread_not_found"}, [parent])
    assert out.changed is True
    assert out.partial is False
    assert _ts_of(out.document) == [MINTED1]


def test_replies_of_a_thread_this_run_did_not_mint_is_left_alone():
    """`thread_not_found` for a thread irimi has nothing to do with is Slack's own answer about
    Slack's own state. Untouched, and `partial` only on `_would_apply`'s existing rule (#52)."""
    error = {"ok": False, "error": "thread_not_found"}
    out = apply_read(_replies(REAL_OLD), dict(error), [_post_write(ts=MINTED1)])
    assert out.document == error
    assert out.changed is False
    assert out.status is None


@pytest.mark.parametrize("code", ["ratelimited", "invalid_auth", "missing_scope"])
def test_a_minted_thread_read_that_slack_refused_for_another_reason_is_not_answered(code):
    """Only `thread_not_found` is Slack saying it never saw the thread. A rate limit or a bad token
    on the same read is Slack's answer about the caller, which production would have sent for the
    real thread too, so building the page over it would turn an error into a success. The error
    stands, `partial` because the run holds posts this read would have shown (#52)."""
    error = {"ok": False, "error": code}
    out = apply_read(
        _replies(MINTED1), dict(error), [_post_write(ts=MINTED1), _reply_write(MINTED1, ts=MINTED2)]
    )
    assert out.document == error
    assert out.changed is False
    assert out.partial is True
    assert out.status is None


@pytest.mark.parametrize(
    "shaping",
    [
        {"limit": 2},
        {"latest": REAL_NEW},
        {"oldest": REAL_OLD},
        {"cursor": "c1"},
        {"inclusive": True},
    ],
)
def test_a_minted_thread_read_that_asks_for_part_of_it_stays_partial(shaping):
    """`_minted_thread` builds the WHOLE thread, so a read asking for a window, a limit or a page of
    it is one it must decline: answering `limit=1` with the whole thread, or `latest=<a real ts>`
    with replies Slack would have left out, is the half-apply this module's header forbids in its
    most visible form. The error stands and the read says the world irimi showed is incomplete
    (#52)."""
    error = {"ok": False, "error": "thread_not_found"}
    out = apply_read(
        _replies(MINTED1, **shaping),
        dict(error),
        [_post_write(ts=MINTED1), _reply_write(MINTED1, ts=MINTED2)],
    )
    assert out.document == error
    assert out.changed is False
    assert out.partial is True
    assert out.status is None


def test_a_minted_parent_read_with_an_unreadable_parameter_stays_partial():
    """A parameter the effects cannot read means irimi cannot say what this page should hold, so
    the page is not built and the error stands - today's answer, kept for the cases #52 does not
    close (#44's `REPLIES_PARAMS` rule)."""
    error = {"ok": False, "error": "thread_not_found"}
    out = apply_read(_replies(MINTED1, oddity="x"), dict(error), [_post_write(ts=MINTED1)])
    assert out.document == error
    assert out.changed is False
    assert out.partial is True


def test_a_minted_parent_in_a_channel_the_read_spells_differently_stays_partial():
    """`chat.postMessage` takes `#general` and `conversations.replies` requires the id, so irimi
    cannot tell these are the same channel. Leaving a post off a page is one kind of incomplete;
    building a whole page for a channel irimi cannot match is another kind of wrong, so this one
    needs a definite match and does not get one (#44, #52)."""
    error = {"ok": False, "error": "thread_not_found"}
    out = apply_read(_replies(MINTED1), dict(error), [_post_write(channel="#general", ts=MINTED1)])
    assert out.document == error
    assert out.changed is False
    assert out.partial is True


def test_a_minted_thread_whose_reply_has_no_message_to_show_stays_partial():
    """The fixture failed and that reply degraded to `fake-L0` (#42): the run put a message in this
    thread and the overlay has nothing to show for it, so the page cannot be built honestly and
    Slack's own error stands (#52).

    The PARENT's own message cannot be missing here and still reach this point - see
    `_minted_thread`'s docstring and #64.
    """
    error = {"ok": False, "error": "thread_not_found"}
    parent = _post_write(ts=MINTED1)
    mute = Write(
        operation="chat.postMessage",
        posted={"channel": "C0123", "text": "a reply", "thread_ts": MINTED1},
        answer={"ok": True, "channel": "C0123", "ts": MINTED2},
    )
    out = apply_read(_replies(MINTED1), dict(error), [parent, mute])
    assert out.document == error
    assert out.changed is False
    assert out.partial is True
    assert out.status is None


def test_a_post_whose_answer_has_no_message_is_partial_and_unchanged():
    """The fixture failed to load and the write degraded to `fake-L0` (#42): the run minted a
    message for this channel and the overlay has nothing to show for it."""
    write = Write(
        operation="chat.postMessage",
        posted={"channel": "C0123", "text": "hi"},
        answer={"ok": True, "channel": "C0123", "ts": MINTED1},
    )
    page = _history_page(REAL_OLD)
    before = copy.deepcopy(page)
    out = apply_read(_history(), page, [write])
    assert out.document == before
    assert out.changed is False
    assert out.partial is True


def test_a_post_whose_message_ts_is_not_a_string_is_partial_and_unchanged():
    write = _post_write()
    write.answer["message"]["ts"] = 1800000000
    page = _history_page(REAL_OLD)
    before = copy.deepcopy(page)
    out = apply_read(_history(), page, [write])
    assert out.document == before
    assert out.partial is True


@pytest.mark.parametrize(
    ("operation", "document"),
    [
        ("conversations.info", {"ok": True, "channel": {"id": "C0123", "name": "general"}}),
        ("users.info", {"ok": True, "user": {"id": "U1", "name": "someone"}}),
    ],
)
def test_an_unmodelled_operation_is_unchanged(operation, document):
    before = copy.deepcopy(document)
    out = apply_read(_read(operation, channel="C0123"), document, [_post_write()])
    assert out.document == before
    assert out.changed is False
    assert out.partial is False


def test_a_body_that_is_not_an_object_is_unchanged():
    out = apply_read(_history(), [1, 2, 3], [_post_write()])
    assert out.document == [1, 2, 3]
    assert out.changed is False
    assert out.partial is False


def test_an_answer_that_already_carries_its_thread_ts_inserts_the_same_reply():
    """Since #55 a faked reply's own answer names the thread, so `_posts`' completion of its copy
    writes the value it already holds. The inserted message must be the same either way, or the
    two halves of #44/#55 would disagree about one reply.

    `_reply_write` builds the pre-#55 answer (`thread_ts` in `posted` only); this builds the
    post-#55 one and asserts the page comes out identical.
    """
    without = _reply_write(REAL_OLD)
    with_it = _reply_write(REAL_OLD)
    with_it.answer["message"]["thread_ts"] = REAL_OLD

    pages = []
    for write in (without, with_it):
        out = apply_read(
            _replies(REAL_OLD), _replies_page(_threaded_parent(REAL_OLD), REAL_NEW), [write]
        )
        assert out.changed is True
        assert out.partial is False
        pages.append(out.document)
    assert pages[0] == pages[1]
    assert pages[0]["messages"][-1]["thread_ts"] == REAL_OLD


# ----------------------------------------------------------------------------- slack_sdk


def _sdk_response(slack_sdk, operation, document):
    response = slack_sdk.web.slack_response.SlackResponse(
        client=None,
        http_verb="POST",
        api_url=f"https://slack.com/api/{operation}",
        req_args={},
        data=json.loads(json.dumps(document)),
        headers={},
        status_code=200,
    )
    response.validate()
    return response


def test_slack_sdk_reads_the_faked_post_back_out_of_history():
    """#44's done-when, for history."""
    slack_sdk = pytest.importorskip("slack_sdk")
    out = apply_read(_history(), _history_page(REAL_OLD), [_post_write(text="refund issued")])
    response = _sdk_response(slack_sdk, "conversations.history", out.document)
    assert response["messages"][0]["text"] == "refund issued"
    assert response["messages"][0]["ts"] == MINTED1


def test_slack_sdk_reads_the_faked_reply_back_out_of_replies():
    """#44's done-when, for replies."""
    slack_sdk = pytest.importorskip("slack_sdk")
    out = apply_read(
        _replies(REAL_OLD),
        _replies_page(_threaded_parent(REAL_OLD), REAL_NEW),
        [_reply_write(REAL_OLD)],
    )
    response = _sdk_response(slack_sdk, "conversations.replies", out.document)
    assert response["messages"][-1]["text"] == "a reply"
    assert response["messages"][-1]["ts"] == MINTED1
    assert response["messages"][0]["reply_count"] == 2


def test_slack_sdk_reads_a_minted_thread_back_out_of_replies():
    """#52's Slack done-when against the real SDK: `ok: true` is what the SDK reads to decide the
    call succeeded, and a built page has to satisfy `validate()` like any other."""
    slack_sdk = pytest.importorskip("slack_sdk")
    out = apply_read(
        _replies(MINTED1),
        {"ok": False, "error": "thread_not_found"},
        [_post_write(ts=MINTED1, text="refund issued"), _reply_write(MINTED1, ts=MINTED2)],
    )
    response = _sdk_response(slack_sdk, "conversations.replies", out.document)
    assert response["messages"][0]["text"] == "refund issued"
    assert response["messages"][-1]["ts"] == MINTED2
