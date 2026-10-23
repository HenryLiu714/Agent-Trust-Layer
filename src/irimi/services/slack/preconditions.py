"""Slack's L3 precondition: whether Slack would have accepted a `chat.postMessage` at all.

It is decided by `conversations.info` on the channel the post names. The document `_post_verdict`
sees has been through `effects.apply_read` like every other precondition read, and comes back
exactly as Slack sent it: no write irimi models changes whether a channel exists, is archived or
has the bot in it.

Every rejection body is envelope-shaped, not fixture-shaped: they never go through
`answers.slack_l1_body`, which always answers `ok: true`, and slack_sdk raises `SlackApiError` off
`ok: false`.
"""

from typing import Any

from irimi.services.model import NOT_EVALUABLE, Check, NotEvaluable, Probe, Proposal, Rejection
from irimi.services.slack.effects import CHANNEL_ID


def _post_probe(proposal: Proposal) -> Probe | None:
    """`conversations.info` on the channel this post names, when it is spelled as an id."""
    channel = proposal.posted.get("channel")
    # `conversations.info` answers with the channel's ID spelling only. `chat.postMessage` also
    # accepts `#general`, and probing with that name would come back `channel_not_found` for a
    # channel that exists - irimi reporting a rejection Slack would never have made. So a channel
    # that is not id-shaped is not probed at all, and the write is recorded `not_evaluable`.
    # Same tri-state as Stripe's `_passes_filters`: "cannot tell" is its own answer. The id rule is
    # `CHANNEL_ID`, the one `conversations.history` already uses.
    if not isinstance(channel, str) or not CHANNEL_ID.fullmatch(channel):
        return None
    return Probe(operation="conversations.info", posted={"channel": channel})


def _post_verdict(proposal: Proposal, document: Any) -> Rejection | NotEvaluable | None:
    """`channel_not_found`, `is_archived`, `not_in_channel`, `NOT_EVALUABLE`, or None."""
    if not isinstance(document, dict):
        return NOT_EVALUABLE
    if document.get("ok") is False:
        if document.get("error") == "channel_not_found":
            return _rejection("channel_not_found")
        # An error irimi does not model - `missing_scope`, `invalid_auth`, `ratelimited` - says
        # nothing about whether the post would have succeeded, and inventing a rejection from it
        # is the failure mode this check exists to avoid. It is not a pass either: a missing
        # scope is the `not_evaluable` case, and Slack sends one at HTTP 200, so the policy's own
        # non-200 rule can never catch it.
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
