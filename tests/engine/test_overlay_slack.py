"""Slack's effects applied through the engine to reads of a loopback Slack."""

import json

import pytest

from irimi.protocol.headers import ANSWERED_BY_HEADER
from tests.support.engine import engine_config, slack_call, start_engine
from tests.support.maps import SLACK_OVERLAY_MAP, map_index
from tests.support.upstreams import FakeSlack, serve

REAL_SLACK_TS = "1700000000.000100"

REAL_SLACK_MESSAGE = {"type": "message", "ts": REAL_SLACK_TS, "text": "real", "user": "U1"}


def _history(posted):
    return {"ok": True, "messages": [dict(REAL_SLACK_MESSAGE)], "has_more": False}


def _replies(posted):
    """One real message in whatever thread is asked about. Any other thread than `REAL_SLACK_TS`
    is `thread_not_found` at HTTP 200, which is how a read of a thread irimi minted arrives."""
    if posted.get("ts") != REAL_SLACK_TS:
        return {"ok": False, "error": "thread_not_found"}
    return _history(posted)


# A Slack-shaped upstream: one real message, in whatever channel or thread is asked about and with
# no replies of its own. Only the two reads may reach it.
SLACK_READS = {"/api/conversations.history": _history, "/api/conversations.replies": _replies}


@pytest.fixture
def slack_stub(tmp_path, monkeypatch):
    """`stripe_stub`'s twin over SLACK_OVERLAY_MAP. Yields (proxy port, stub port, exchanges)."""
    from irimi.decide.overlay import ServiceOverlay

    slack = serve(FakeSlack, threaded=True, methods=SLACK_READS)
    maps = map_index(tmp_path, monkeypatch, doc=SLACK_OVERLAY_MAP)
    eng, seen, stop = start_engine(
        engine_config(tmp_path, monkeypatch, maps=maps), overlay=ServiceOverlay()
    )
    yield eng.listen_port(), slack, seen
    stop()
    slack.stop()


def test_a_slack_history_read_after_a_faked_post_shows_the_post_first(slack_stub):
    proxy, stub, seen = slack_stub
    status, _, data = slack_call(
        proxy, stub, "chat.postMessage", {"channel": "C0123", "text": "hi"}
    )
    assert status == 200
    minted = json.loads(data)["ts"]
    status, headers, data = slack_call(proxy, stub, "conversations.history", {"channel": "C0123"})
    assert status == 200
    assert [m["ts"] for m in json.loads(data)["messages"]] == [minted, REAL_SLACK_TS]
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    assert seen[-1].answered_by == "overlay"
    assert seen[-1].overlay == "full"
    assert stub.seen == [("POST", "/api/conversations.history")], "a faked post reached Slack"


def test_a_slack_history_read_of_another_channel_is_forwarded_unstamped(slack_stub):
    """Two different channel ids are a definite no, not a "cannot tell": the read is left as
    Slack sent it and is not `partial`."""
    proxy, stub, seen = slack_stub
    status, _, _ = slack_call(proxy, stub, "chat.postMessage", {"channel": "C0123", "text": "hi"})
    assert status == 200
    # The log holds a faked write, so the overlay IS asked about the read below.
    assert seen[-1].kind == "write" and seen[-1].answered_by.startswith("fake-")
    status, headers, data = slack_call(proxy, stub, "conversations.history", {"channel": "C0999"})
    assert status == 200
    assert [m["ts"] for m in json.loads(data)["messages"]] == [REAL_SLACK_TS]
    assert ANSWERED_BY_HEADER not in headers
    assert seen[-1].answered_by == "live"
    assert seen[-1].overlay is None


def test_a_slack_replies_read_after_a_faked_reply_shows_it_at_the_tail(slack_stub):
    """A replies read after a faked reply: the reply last, the parent's counts moved, the whole
    page stamped `overlay` - and the post itself never reached Slack."""
    proxy, stub, seen = slack_stub
    status, _, data = slack_call(
        proxy,
        stub,
        "chat.postMessage",
        {"channel": "C0123", "thread_ts": REAL_SLACK_TS, "text": "on it"},
    )
    assert status == 200
    minted = json.loads(data)["ts"]
    status, headers, data = slack_call(
        proxy, stub, "conversations.replies", {"channel": "C0123", "ts": REAL_SLACK_TS}
    )
    assert status == 200
    messages = json.loads(data)["messages"]
    assert [m["ts"] for m in messages] == [REAL_SLACK_TS, minted]
    assert messages[0]["reply_count"] == 1
    assert messages[0]["latest_reply"] == minted
    assert messages[-1]["thread_ts"] == REAL_SLACK_TS
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    assert seen[-1].answered_by == "overlay"
    assert seen[-1].overlay == "full"
    assert stub.seen == [("POST", "/api/conversations.replies")], "a faked post reached Slack"


def test_a_slack_read_the_effects_cannot_place_is_recorded_partial_and_left_alone(slack_stub):
    """`chat.postMessage` takes `#general`, `conversations.history` requires the id, and irimi's own
    echo carries the posted spelling - so the two sides cannot be matched. The body is left exactly
    as Slack sent it and the exchange says the world irimi showed is incomplete."""
    proxy, stub, seen = slack_stub
    status, _, _ = slack_call(
        proxy, stub, "chat.postMessage", {"channel": "#general", "text": "hi"}
    )
    assert status == 200
    status, headers, data = slack_call(proxy, stub, "conversations.history", {"channel": "C0123"})
    assert status == 200
    assert [m["ts"] for m in json.loads(data)["messages"]] == [REAL_SLACK_TS]
    assert ANSWERED_BY_HEADER not in headers, "nothing of irimi's is in this body"
    assert seen[-1].answered_by == "live"
    assert seen[-1].overlay == "partial"


def test_a_faked_post_answers_with_its_thread_or_with_no_thread_key_through_the_proxy(slack_stub):
    """Off the real proxy, a threaded post's own answer names the thread it was
    posted in, and a top-level post's answer has no `thread_ts` key at all - not a `null` one,
    which real Slack never sends. Neither reached Slack."""
    proxy, stub, _ = slack_stub
    status, _, data = slack_call(
        proxy,
        stub,
        "chat.postMessage",
        {"channel": "C0123", "thread_ts": REAL_SLACK_TS, "text": "on it"},
    )
    assert status == 200
    assert json.loads(data)["message"]["thread_ts"] == REAL_SLACK_TS
    status, _, data = slack_call(
        proxy, stub, "chat.postMessage", {"channel": "C0123", "text": "hi"}
    )
    assert status == 200
    assert "thread_ts" not in json.loads(data)["message"]
    assert stub.seen == [], "a faked post reached Slack"


def test_a_slack_replies_read_of_a_minted_thread_is_answered_not_thread_not_found(slack_stub):
    """A minted Slack thread read back, end to end. The agent posts, then reads the thread its
    own post started - which Slack has never heard of and refuses at HTTP 200. irimi answers with
    the thread from its write log, and the post still never reached Slack."""
    proxy, stub, seen = slack_stub
    status, _, data = slack_call(
        proxy, stub, "chat.postMessage", {"channel": "C0123", "text": "refunded"}
    )
    assert status == 200
    minted = json.loads(data)["ts"]
    status, headers, data = slack_call(
        proxy, stub, "conversations.replies", {"channel": "C0123", "ts": minted}
    )
    assert status == 200
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    document = json.loads(data)
    assert document["ok"] is True
    assert [m["ts"] for m in document["messages"]] == [minted]
    assert document["messages"][0]["text"] == "refunded"
    ex = seen[-1]
    assert (ex.kind, ex.answered_by, ex.overlay, ex.issued_by) == (
        "read",
        "overlay",
        "full",
        "agent",
    )
    # The read reached Slack and the post did not: it is still a real read.
    assert stub.seen == [("POST", "/api/conversations.replies")], "a faked post reached Slack"
