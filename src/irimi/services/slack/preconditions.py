"""Slack's `chat.postMessage` check; irimi's check read is `conversations.info` on the channel."""

from typing import Any

from irimi.services.model import NOT_EVALUABLE, Check, NotEvaluable, Probe, Proposal, Rejection
from irimi.services.slack.effects import CHANNEL_ID


def _post_probe(proposal: Proposal) -> Probe | None:
    """Return `conversations.info` on the posted channel id; a name such as `#general` is not."""
    channel = proposal.posted.get("channel")
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
        # Other errors (`missing_scope`) say nothing about the post, and come at HTTP 200.
        return NOT_EVALUABLE
    channel = document.get("channel")
    if not isinstance(channel, dict):
        return NOT_EVALUABLE
    if channel.get("is_archived") is True:
        return _rejection("is_archived")
    # A DM or MPIM has no membership, so only an explicit `false` on a channel or group rejects.
    if channel.get("is_member") is False and (
        channel.get("is_channel") is True or channel.get("is_group") is True
    ):
        return _rejection("not_in_channel")
    return None


def _rejection(error: str) -> Rejection:
    """Return Slack's refusal: HTTP 200 and an `ok: false` envelope naming the error."""
    return Rejection(status=200, body={"ok": False, "error": error}, code=error)


CHANNEL_POSTABLE = Check(probe=_post_probe, verdict=_post_verdict)
