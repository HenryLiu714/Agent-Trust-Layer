"""Slack's L2 effects: what a faked write does to a later live read (#44).

The design's v0.1 table, by the write that causes each effect:

    chat.postMessage        a top-level post: at the head of page 1 of `conversations.history`
                            a threaded reply: at the tail of the last page of
                            `conversations.replies`, and on its parent - wherever the parent
                            appears - `reply_count`, `latest_reply` and `reply_users`
                            a `reply_broadcast` reply: both, and the history read is `partial`

Nothing else Slack answers is overlaid. `conversations.info`, `conversations.list`, `auth.test`,
`users.info` and the search endpoints are left exactly as Slack sent them.

Two rules run through all of it, both inherited from `stripe.py`:

* **Never invent a field the live object does not have.** A field Slack's own response omits is
  one the agent can never see in production. The one exception is `THREAD_FIELDS`, below: Slack's
  own closed set, added to a parent whose first reply the run faked. The rule is about *live*
  objects, so it does not reach a message irimi minted itself: `_posts` completes its own copy of a
  faked reply with the `thread_ts` the caller posted, which real Slack would have sent (#55).
* **Say `partial` rather than half-apply.** A page the table does not model, a parameter it cannot
  read, a channel it cannot tell is the one asked about, a post whose answer carries no message to
  show: the document is left exactly as Slack sent it and the exchange records that the world irimi
  showed is incomplete.

Slack's reads are POSTs, so every parameter comes from `read.posted` - the read's own body, which
the overlay reflected - and never from the query string. A form body delivers `oldest=0` as an int
and a JSON body can deliver a float, so every value is coerced defensively and anything unreadable
is "cannot tell".

Pure functions over plain data: no clock, no minting, no I/O, no module state. Every `ts` used here
comes off a write's answer. Phase 5 replay runs these same functions over a recording.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from irimi.services.model import (
    NOT_EVALUABLE,
    Applied,
    Check,
    NotEvaluable,
    Probe,
    Proposal,
    Read,
    Rejection,
    Write,
)

SERVICE = "slack"
# The body parameters each overlaid read understands. Anything else means irimi cannot say where the
# faked message belongs on this page, and the read is `partial`, on `stripe.KNOWN_LIST_PARAMS`'
# precedent. `token` is one slack_sdk may send in the body; it is known and ignored.
HISTORY_PARAMS = frozenset(
    {"token", "channel", "cursor", "limit", "oldest", "latest", "inclusive", "include_all_metadata"}
)
# `conversations.replies` takes every argument `conversations.history` does, plus the `ts` naming
# the thread - so it is derived rather than spelled twice, which is what #44 meant by "the same set
# with `ts`". Slack documents `include_all_metadata` on both methods; listing it on one only made a
# read that named it needlessly `partial`.
REPLIES_PARAMS = HISTORY_PARAMS | {"ts"}
# The fields Slack puts on a message once it has replies, in the order they are added. Adding them
# to a parent that had none is the one place in this package an effect adds a field the live object
# lacked. `stripe._customer`'s rule exists to stop a CALLER'S posted field being pasted onto a
# service object; these are Slack's own fields, the set is closed, and irimi knows every value
# exactly. Omitting them is its own untruth: a replies page showing a reply while the parent has no
# `reply_count` is a body production cannot produce. `subscribed` and `last_read` are the viewer's
# state, not the thread's, and stay out. Decided on the Phase 2 page, Sep 25, 2026 (#44).
THREAD_FIELDS = ("thread_ts", "reply_count", "reply_users_count", "latest_reply", "reply_users")
# A channel id as `conversations.history` requires it. A name like `#general` is not one.
_CHANNEL_ID = re.compile(r"[CDG][A-Z0-9]+")
_TS = re.compile(r"(\d{1,12})(?:\.(\d{1,6}))?")


@dataclass(frozen=True)
class _Post:
    """One `chat.postMessage` from the write log, as the effects need it."""

    channel: str | None
    thread_ts: str | None  # None for a top-level post
    broadcast: bool
    # A shallow copy of the answer's `message`, never the write log's own dict (#44): a minted
    # parent that has replies gets thread fields added to it, and a reply gets the `thread_ts` the
    # caller posted. None when the answer carried no message to show - the fixture failed and the
    # write degraded to `fake-L0` (#42).
    message: dict[str, Any] | None

    @property
    def ts(self) -> str | None:
        return None if self.message is None else self.message["ts"]


def apply_read(read: Read, document: Any, writes: Sequence[Write]) -> Applied:
    """The run's faked Slack writes, applied to one live read's parsed body."""
    if not isinstance(document, dict):
        return Applied(document)
    posts = _posts(writes)
    if document.get("ok") is not True:
        # Never edit an error: half-turning it into a success is the untruth this module's header
        # forbids. It is `partial` when the run holds a post this read would have shown. That
        # includes `conversations.replies` of a thread irimi minted, which Slack answers
        # `thread_not_found` at HTTP 200; answering it from the write log is #52.
        return Applied(document, partial=any(_would_apply(read, post) for post in posts))
    if read.operation == "conversations.history":
        return _history(read, document, posts)
    if read.operation == "conversations.replies":
        return _replies(read, document, posts)
    return Applied(document)


# ------------------------------------------------------------------------------- the effects


def _history(read: Read, document: dict[str, Any], posts: Sequence[_Post]) -> Applied:
    """Page 1 of a channel's history: the faked top-level posts first, and every threaded parent on
    any page carrying its faked replies."""
    posted = read.posted
    channel = posted.get("channel")
    if not any(_in_channel(post.channel, channel) is not False for post in posts):
        return Applied(document)
    if any(name not in HISTORY_PARAMS for name in posted):
        return Applied(document, partial=True)
    messages = document.get("messages")
    if not isinstance(messages, list):
        return Applied(document, partial=True)
    changed = partial = False
    # Page 1 only. A cursor page is not `partial`: irimi rewrites nothing for Slack, so Slack's own
    # paging holds exactly the real messages it would have held, and a faked post belongs on page 1
    # alone.
    if not posted.get("cursor"):
        shown = _shown(messages)
        head: list[_Post] = []
        for post in posts:
            if post.thread_ts is not None and not post.broadcast:
                continue  # a reply lives in its thread; history shows only its parent
            verdict = _belongs(post, posted)
            if verdict is False:
                continue
            if verdict is None or post.message is None:
                # The run posted a message irimi cannot place or cannot show. Leaving it out of a
                # page that then claims to be complete is the untruth this module forbids.
                partial = True
                continue
            if post.broadcast:
                # Slack marks a broadcast `subtype: thread_broadcast`; irimi's minted one has none.
                partial = True
            # `changed` must mean the body really moved (#43), so a post already there is skipped.
            if post.ts not in shown:
                head.append(post)
        if head:
            # Newest first, as `conversations.history` answers, and a minted `ts` is above every
            # real one. Never truncated, and `has_more` and `next_cursor` are left as Slack sent
            # them: the page runs over its `limit` by the faked posts. Stripe can truncate because
            # `starting_after=<last id shown>` is formed by the agent from the page it was given,
            # so nothing is lost; Slack's cursor is opaque and server-issued, so dropping the
            # oldest real message would make the cursor skip one the agent can never reach (#44).
            messages = [p.message for p in sorted(head, key=_order, reverse=True)] + messages
            document["messages"] = messages
            changed = True
    # Every page, cursor or not: a threaded parent can sit on any of them, the minted ones just
    # inserted included.
    moved, unsure = _threads(messages, posts, channel)
    return Applied(document, changed=changed or moved, partial=partial or unsure)


def _replies(read: Read, document: dict[str, Any], posts: Sequence[_Post]) -> Applied:
    """A thread: the parent carrying its faked replies, and those replies at the tail of the last
    page."""
    posted = read.posted
    channel = posted.get("channel")
    thread = posted.get("ts")
    mine = [
        post
        for post in posts
        if post.thread_ts is not None
        and (not isinstance(thread, str) or post.thread_ts == thread)
        and _in_channel(post.channel, channel) is not False
    ]
    if not mine:
        return Applied(document)
    if any(name not in REPLIES_PARAMS for name in posted):
        return Applied(document, partial=True)
    messages = document.get("messages")
    if not isinstance(messages, list) or not isinstance(thread, str):
        return Applied(document, partial=True)
    changed, partial = _threads(messages, posts, channel)
    # `conversations.replies` answers the parent first and then the replies OLDEST-first - #44 first
    # assumed newest-first, which is what a reader will expect - so a faked reply, newer than every
    # real one, belongs at the tail of the LAST page. A page with more behind it gets no insertion
    # and is not `partial`: the reply genuinely belongs further on, and flagging a complete page
    # would be a false alarm. Its parent's counts still move, above: Slack reports the thread's
    # true `reply_count` on every page of it.
    if document.get("has_more") is not True:
        shown = _shown(messages)
        tail: list[_Post] = []
        for post in mine:
            verdict = _belongs(post, posted)
            if verdict is False:
                continue
            if verdict is None or post.message is None:
                partial = True
                continue
            if post.ts not in shown:
                tail.append(post)
        if tail:
            document["messages"] = messages + [p.message for p in sorted(tail, key=_order)]
            changed = True
    return Applied(document, changed=changed, partial=partial)


def _threads(messages: list[Any], posts: Sequence[_Post], channel: Any) -> tuple[bool, bool]:
    """`_thread_fields` on every message a faked reply answers. Returns `(changed, partial)`."""
    shown = _shown(messages)
    replies: dict[str, list[_Post]] = {}
    partial = False
    for post in posts:
        if post.thread_ts is None or post.thread_ts not in shown:
            continue
        here = _in_channel(post.channel, channel)
        if here is False:
            continue
        if here is None or post.message is None:
            # Counting a reply irimi could not show, or one it cannot tell is in this channel,
            # would half-apply it. The parent is left as Slack sent it and the read says so.
            partial = True
            continue
        replies.setdefault(post.thread_ts, []).append(post)
    changed = False
    for message in messages:
        if not isinstance(message, dict) or message.get("ts") not in replies:
            continue
        moved = _thread_fields(message, replies[message["ts"]])
        if moved is None:
            partial = True
        elif moved:
            changed = True
    return changed, partial


def _thread_fields(parent: dict[str, Any], replies: Sequence[_Post]) -> bool | None:
    """A parent's thread fields, moved by its faked replies: True when a value moved, False when
    none did, and None when the parent's own fields cannot be read and nothing was touched."""
    newest = max(replies, key=_order).ts
    users = [p.message.get("user") for p in replies if p.message is not None]
    values: dict[str, Any]
    if "reply_count" in parent:
        # Only the fields Slack already sent: the exception in `THREAD_FIELDS` is for a parent with
        # no thread at all, not a licence to fill in what a threaded one left out (#44).
        count = parent["reply_count"]
        if not _is_int(count):
            return None
        values = {"reply_count": count + len(replies)}
        if "latest_reply" in parent:
            values["latest_reply"] = newest
        added: int | None = None
        if "reply_users" in parent:
            known = parent["reply_users"]
            if not isinstance(known, list):
                return None
            values["reply_users"] = _merged(known, users)
            added = len(values["reply_users"]) - len(known)
        if "reply_users_count" in parent:
            # Slack caps `reply_users` while `reply_users_count` is the true distinct count, so the
            # count is bumped by the users newly added rather than recomputed, and a replier already
            # past the cap is counted twice. Without `reply_users` there is no telling who is new.
            if not _is_int(parent["reply_users_count"]) or added is None:
                return None
            values["reply_users_count"] = parent["reply_users_count"] + added
    else:
        merged = _merged([], users)
        own = (parent["ts"], len(replies), len(merged), newest, merged)
        values = dict(zip(THREAD_FIELDS, own, strict=True))
    changed = False
    for name, value in values.items():
        if name not in parent or parent[name] != value:
            parent[name] = value
            changed = True
    return changed


# ------------------------------------------------------------------------------------ helpers


def _posts(writes: Sequence[Write]) -> list[_Post]:
    """One record per `chat.postMessage` in the run. The only Slack write with an effect."""
    out: list[_Post] = []
    for write in writes:
        if write.operation != "chat.postMessage":
            continue
        channel = write.answer.get("channel") or write.posted.get("channel")
        thread_ts = write.posted.get("thread_ts")
        message = write.answer.get("message")
        usable = (
            isinstance(message, dict)
            and isinstance(message.get("ts"), str)
            and _ts(message["ts"]) is not None
        )
        if thread_ts in (None, ""):
            thread_ts = None
        elif not isinstance(thread_ts, str):
            # A reply to a thread irimi cannot name: nothing it can place, so nothing to show.
            usable = False
            thread_ts = None
        own = dict(message) if usable and isinstance(message, dict) else None
        if own is not None and thread_ts is not None:
            # Every reply real Slack shows carries the thread it is in, and irimi knows the value
            # exactly - the caller posted it. Omitting it puts a reply with no `thread_ts` on a
            # `conversations.replies` page, which is a body production cannot produce: the untruth
            # `THREAD_FIELDS` exists to stop, on the reply's side of the thread. Written onto this
            # copy only, never the write log's own message. `parent_user_id` stays out: it is the
            # parent's field, not always on the page, and not one irimi always knows. The write's
            # OWN answer still omits `thread_ts` because the fixture never names it for
            # `echo._reflect_over` to write over; that half is #55. Decided Sep 25, 2026 (#44).
            own["thread_ts"] = thread_ts
        out.append(
            _Post(
                channel=channel if isinstance(channel, str) and channel else None,
                thread_ts=thread_ts,
                # Slack ignores `reply_broadcast` on a top-level post, and a form body sends it as
                # the text `false`, which is truthy (#44).
                broadcast=thread_ts is not None
                and _flag(write.posted.get("reply_broadcast")) is True,
                message=own,
            )
        )
    return out


def _would_apply(read: Read, post: _Post) -> bool:
    """Whether a post would have touched this read, had Slack answered it with a success."""
    posted = read.posted
    if _in_channel(post.channel, posted.get("channel")) is False:
        return False
    if read.operation == "conversations.history":
        return True
    if read.operation == "conversations.replies":
        thread = posted.get("ts")
        if not isinstance(thread, str):
            return post.thread_ts is not None
        return thread in (post.thread_ts, post.ts)
    return False


def _belongs(post: _Post, posted: dict[str, Any]) -> bool | None:
    """Whether a post belongs on this read's page: True, False, or None for "cannot tell".

    A definite no from either test wins over a "cannot tell" from the other.
    """
    verdicts = (_in_channel(post.channel, posted.get("channel")), _in_window(post.ts, posted))
    if False in verdicts:
        return False
    if None in verdicts:
        return None
    return True


def _in_channel(mine: str | None, asked: Any) -> bool | None:
    """Whether a post's channel is the one this read asks about: True, False, or None for "cannot
    tell", on `stripe._passes_filters`' precedent.

    None is the case irimi forces on itself. `chat.postMessage` accepts `#general` while
    `conversations.history` requires `C0123`, and real Slack answers the post with the channel id
    while `echo.slack_l1_body` echoes the posted value verbatim - so the mismatch is irimi's own,
    and silently leaving the agent's own message off a page that then claims to be complete is the
    untruth this module's header forbids. The caller turns None into `partial` instead (#44).
    """
    if not isinstance(asked, str) or not asked or mine is None:
        return None
    if mine == asked:
        return True
    if _CHANNEL_ID.fullmatch(mine) and _CHANNEL_ID.fullmatch(asked):
        return False
    return None


def _in_window(ts: str | None, posted: dict[str, Any]) -> bool | None:
    """Whether `ts` falls inside the read's `oldest` / `latest` bounds: True, False, or None.

    An absent bound is no constraint, and `inclusive` (default false) decides only exact equality
    with one. A minted `ts` is above every real one, so `latest=<a real ts>` definitely excludes it,
    and that is correct rather than a gap.
    """
    at = _ts(ts)
    inclusive = _flag(posted.get("inclusive"))
    verdicts: list[bool | None] = []
    for name, below in (("oldest", False), ("latest", True)):
        if posted.get(name) is None:
            continue
        bound = _ts(posted[name])
        if at is None or bound is None:
            verdicts.append(None)
        elif at == bound:
            verdicts.append(inclusive)
        else:
            verdicts.append((at < bound) == below)
    if False in verdicts:
        return False
    if None in verdicts:
        return None
    return True


def _ts(value: Any) -> tuple[int, int] | None:
    """A `ts` as the `(seconds, microseconds)` pair it sorts by, or None when it will not parse.

    Not a string: `"1700000000.000001" < "999999999.000001"` lexicographically. Not a float: at
    Slack's magnitude a float loses the microsecond the whole `ts` scheme exists to keep distinct
    (#42). A form body delivers `oldest=0` as an int. A JSON body can deliver a float, which is
    read through its shortest `repr` - the same digits `json.dumps` sent Slack.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return (value, 0) if value >= 0 else None
    if isinstance(value, float):
        value = repr(value)
    if not isinstance(value, str):
        return None
    match = _TS.fullmatch(value)
    if match is None:
        return None
    return int(match[1]), int((match[2] or "").ljust(6, "0"))


def _order(post: _Post) -> tuple[int, int]:
    return _ts(post.ts) or (0, 0)


def _flag(value: Any) -> bool | None:
    """A boolean parameter as a form or a JSON body may carry it, or None when it is neither.
    Absent is Slack's default, false."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.lower() in ("true", "1", "false", "0"):
        return value.lower() in ("true", "1")
    return None


def _shown(messages: Sequence[Any]) -> set[str]:
    return {m["ts"] for m in messages if isinstance(m, dict) and isinstance(m.get("ts"), str)}


def _merged(known: list[Any], users: Sequence[Any]) -> list[Any]:
    """`known`, then each new user id in order. A new list: `known` may be the write log's."""
    merged = list(known)
    for user in users:
        if isinstance(user, str) and user not in merged:
            merged.append(user)
    return merged


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


# ------------------------------------------------------------------------ the L3 preconditions
#
# Whether Slack would have accepted a `chat.postMessage` at all, decided by `conversations.info`
# on the channel it names (#45). The document `_post_verdict` sees has been through `apply_read`
# like every other precondition read, and comes back exactly as Slack sent it: no write irimi
# models changes whether a channel exists, is archived or has the bot in it.
#
# Every rejection body is envelope-shaped, not fixture-shaped: they never go through
# `echo.slack_l1_body`, which always answers `ok: true`, and slack_sdk raises `SlackApiError` off
# `ok: false`.


def _post_probe(proposal: Proposal) -> Probe | None:
    """`conversations.info` on the channel this post names, when it is spelled as an id."""
    channel = proposal.posted.get("channel")
    # `conversations.info` answers with the channel's ID spelling only. `chat.postMessage` also
    # accepts `#general`, and probing with that name would come back `channel_not_found` for a
    # channel that exists - irimi reporting a rejection Slack would never have made. So a channel
    # that is not id-shaped is not probed at all, and the write is recorded `not_evaluable` (#45).
    # Same tri-state as `stripe._passes_filters`: "cannot tell" is its own answer. The id rule is
    # `_CHANNEL_ID`, the one `conversations.history` already uses.
    if not isinstance(channel, str) or not _CHANNEL_ID.fullmatch(channel):
        return None
    return Probe(
        operation="conversations.info",
        method="POST",
        path="/api/conversations.info",
        posted={"channel": channel},
    )


def _post_verdict(proposal: Proposal, document: Any) -> Rejection | NotEvaluable | None:
    """`channel_not_found`, `is_archived`, `not_in_channel`, `NOT_EVALUABLE`, or None."""
    if not isinstance(document, dict):
        return NOT_EVALUABLE
    if document.get("ok") is False:
        if document.get("error") == "channel_not_found":
            return _rejection("channel_not_found")
        # An error irimi does not model - `missing_scope`, `invalid_auth`, `ratelimited` - says
        # nothing about whether the post would have succeeded, and inventing a rejection from it
        # is the failure mode this check exists to avoid. It is not a pass either: #45 names a
        # missing scope as the `not_evaluable` case, and Slack sends one at HTTP 200, so the
        # policy's own non-200 rule can never catch it (#45).
        return NOT_EVALUABLE
    channel = document.get("channel")
    if not isinstance(channel, dict):
        # A success envelope with no channel object is not one this check can read either.
        return NOT_EVALUABLE
    if channel.get("is_archived") is True:
        return _rejection("is_archived")
    # `is_member` absent is not evidence of anything, and a DM or MPIM has no membership to be
    # outside of - so only an explicit `false` on a channel or a private group rejects.
    if channel.get("is_member") is False and (
        channel.get("is_channel") is True or channel.get("is_group") is True
    ):
        return _rejection("not_in_channel")
    return None


def _rejection(error: str) -> Rejection:
    """Slack's own refusal: HTTP 200 and an `ok: false` envelope naming the error."""
    return Rejection(status=200, body={"ok": False, "error": error}, code=error)


CHANNEL_POSTABLE = Check(probe=_post_probe, verdict=_post_verdict)
