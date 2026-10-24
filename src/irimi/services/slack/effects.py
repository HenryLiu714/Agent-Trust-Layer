"""What a faked Slack `chat.postMessage` does to later live reads.

    top-level post      at the head of page 1 of `conversations.history`
    threaded reply      at the tail of the last page of `conversations.replies`, and moves its
                        parent's `reply_count`, `latest_reply` and `reply_users` wherever it appears
    broadcast reply     both, and the history read is `partial`

Guarantees:
- No field the live object lacks is added, except Slack's own `THREAD_FIELDS`.
- What cannot be applied whole leaves the body as Slack sent it and marks the read `partial`.
- Pure functions over plain data (no clock, I/O or module state), so a replay reuses them.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from irimi.core.bodies import is_int
from irimi.services.model import Applied, Read, Write

# The body parameters each read understands; any other makes the read `partial`. Slack's reads are
# POSTs, so parameters come from `read.posted`, never the query string.
HISTORY_PARAMS = frozenset(
    {"token", "channel", "cursor", "limit", "oldest", "latest", "inclusive", "include_all_metadata"}
)
REPLIES_PARAMS = HISTORY_PARAMS | {"ts"}
# Parameters selecting part of a thread, which `_minted_thread` cannot honour: it builds it whole.
PAGE_SHAPING_PARAMS = frozenset({"oldest", "latest", "inclusive", "limit", "cursor"})
# The fields Slack puts on a message once it has replies, in the order they are added. They may be
# added to a parent that had none: a reply shown under a parent with no `reply_count` is a body
# Slack never sends. `subscribed` and `last_read` are per viewer and stay out.
THREAD_FIELDS = ("thread_ts", "reply_count", "reply_users_count", "latest_reply", "reply_users")
# A channel id as `conversations.history` requires it; `#general` is not one.
CHANNEL_ID = re.compile(r"[CDG][A-Z0-9]+")
_TS = re.compile(r"(\d{1,12})(?:\.(\d{1,6}))?")


@dataclass(frozen=True)
class _Post:
    """One `chat.postMessage` from the write log, as the effects need it."""

    channel: str | None
    thread_ts: str | None  # None for a top-level post
    broadcast: bool
    message: dict[str, Any] | None  # a copy, as effects edit it; None when the answer had none
    ts: str | None  # the message's `ts`, else the envelope's: placeable, not always showable


def apply_read(read: Read, document: Any, writes: Sequence[Write]) -> Applied:
    """Apply the run's faked Slack writes to one live read's parsed body."""
    if not isinstance(document, dict):
        return Applied(document)
    posts = _posts(writes)
    if document.get("ok") is not True:
        # `thread_not_found` for a thread irimi minted is answered from the write log. Any other
        # error stays as Slack sent it, `partial` if the run holds a post this read would show.
        if document.get("error") == "thread_not_found":
            minted = _minted_thread(read, posts)
            if minted is not None:
                return minted
        return Applied(document, partial=any(_would_apply(read, post) for post in posts))
    if read.operation == "conversations.history":
        return _history(read, document, posts)
    if read.operation == "conversations.replies":
        return _replies(read, document, posts)
    return Applied(document)


def _history(read: Read, document: dict[str, Any], posts: Sequence[_Post]) -> Applied:
    """Put faked top-level posts at the head of page 1 and move parents' thread fields."""
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
    # A faked post belongs on page 1 alone; later pages hold exactly Slack's real messages.
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
                partial = True
                continue
            if post.broadcast:
                # Slack marks a broadcast `subtype: thread_broadcast`; irimi's minted one has none.
                partial = True
            if post.ts not in shown:
                head.append(post)
        if head:
            # Newest first. The page is never truncated to `limit`: Slack's cursor is opaque, so
            # dropping a real message would hide it from the agent for good.
            messages = [p.message for p in sorted(head, key=_order, reverse=True)] + messages
            document["messages"] = messages
            changed = True
    # A threaded parent can sit on any page, including the posts just inserted.
    moved, unsure = _threads(messages, posts, channel)
    return Applied(document, changed=changed or moved, partial=partial or unsure)


def _replies(read: Read, document: dict[str, Any], posts: Sequence[_Post]) -> Applied:
    """Append faked replies to a thread's last page and move the parent's thread fields."""
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
    # Replies come oldest-first, so only the last page gets them. The parent's counts move on
    # every page, as Slack reports the true `reply_count` on each.
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


def _minted_thread(read: Read, posts: Sequence[_Post]) -> Applied | None:
    """Build a minted parent's whole thread, which Slack answered `thread_not_found`, or None.

    None unless the read asks for the whole thread and every message is showable and surely in
    the asked channel.
    """
    if read.operation != "conversations.replies":
        return None
    posted = read.posted
    thread = posted.get("ts")
    if not isinstance(thread, str):
        return None
    if any(name not in REPLIES_PARAMS for name in posted):
        return None
    if any(name in PAGE_SHAPING_PARAMS for name in posted):
        return None
    channel = posted.get("channel")
    parent = next((p for p in posts if p.thread_ts is None and p.ts == thread), None)
    if parent is None or parent.message is None:
        return None
    if _in_channel(parent.channel, channel) is not True:
        return None
    replies = [p for p in posts if p.thread_ts == thread]
    if any(p.message is None or _in_channel(p.channel, channel) is not True for p in replies):
        return None
    messages: list[Any] = [
        dict(parent.message),
        *(p.message for p in sorted(replies, key=_order)),
    ]
    _, unsure = _threads(messages, posts, channel)
    return Applied(
        {"ok": True, "messages": messages, "has_more": False}, changed=True, partial=unsure
    )


def _threads(messages: list[Any], posts: Sequence[_Post], channel: Any) -> tuple[bool, bool]:
    """Apply `_thread_fields` to each message a faked reply answers; return `(changed, partial)`."""
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
    """Move a parent's thread fields; return whether any moved, or None if they are unreadable."""
    newest = max(replies, key=_order).ts
    users = [p.message.get("user") for p in replies if p.message is not None]
    values: dict[str, Any]
    if "reply_count" in parent:
        # A parent that already has a thread gets only the fields Slack sent moved.
        count = parent["reply_count"]
        if not is_int(count):
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
            # Slack caps `reply_users` but not the count, so the count grows by the users newly
            # added; a replier already past the cap is counted twice.
            if not is_int(parent["reply_users_count"]) or added is None:
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


def _posts(writes: Sequence[Write]) -> list[_Post]:
    """Return one record per `chat.postMessage` in the run, the only Slack write with an effect."""
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
            usable = False
            thread_ts = None
        own = dict(message) if usable and isinstance(message, dict) else None
        if own is not None and thread_ts is not None:
            # Real Slack puts `thread_ts` on every reply; set it on this copy in case the fixture
            # lacks it. `parent_user_id` is left out: irimi cannot always know it.
            own["thread_ts"] = thread_ts
        out.append(
            _Post(
                channel=channel if isinstance(channel, str) and channel else None,
                thread_ts=thread_ts,
                # Slack ignores `reply_broadcast` on a top-level post, and a form body sends it as
                # the text `false`, which is truthy.
                broadcast=thread_ts is not None
                and _flag(write.posted.get("reply_broadcast")) is True,
                message=own,
                ts=_own_ts(message, write.answer.get("ts")),
            )
        )
    return out


def _own_ts(message: Any, envelope_ts: Any) -> str | None:
    """Return a post's id: its message's `ts`, else the envelope's, each only when it parses."""
    for ts in (message.get("ts") if isinstance(message, dict) else None, envelope_ts):
        if isinstance(ts, str) and _ts(ts) is not None:
            return ts
    return None


def _would_apply(read: Read, post: _Post) -> bool:
    """Return whether a post would have touched this read, had Slack answered it with a success."""
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
    """Return whether a post belongs on this read's page; None for "cannot tell", unless a no."""
    verdicts = (_in_channel(post.channel, posted.get("channel")), _in_window(post.ts, posted))
    if False in verdicts:
        return False
    if None in verdicts:
        return None
    return True


def _in_channel(mine: str | None, asked: Any) -> bool | None:
    """Return whether a post's channel is the one this read asks about, or None for "cannot tell".

    A post to `#general` cannot be matched to an id: irimi echoes the posted name where real Slack
    answers with the id.
    """
    if not isinstance(asked, str) or not asked or mine is None:
        return None
    if mine == asked:
        return True
    if CHANNEL_ID.fullmatch(mine) and CHANNEL_ID.fullmatch(asked):
        return False
    return None


def _in_window(ts: str | None, posted: dict[str, Any]) -> bool | None:
    """Return whether `ts` falls inside the read's `oldest` / `latest` bounds: True, False, or None.

    An absent bound is no constraint; `inclusive` (default false) decides only exact equality.
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
    """Return a `ts` as the `(seconds, microseconds)` pair it sorts by, or None if unparseable.

    Strings sort wrongly and floats lose the microsecond. A form body may deliver an int; a JSON
    float is read through its shortest `repr`, the digits `json.dumps` sent.
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
    """Return a form or JSON boolean parameter, False when absent, None when unreadable."""
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
    """Return a new list of `known`, then each new user id in order."""
    merged = list(known)
    for user in users:
        if isinstance(user, str) and user not in merged:
            merged.append(user)
    return merged
