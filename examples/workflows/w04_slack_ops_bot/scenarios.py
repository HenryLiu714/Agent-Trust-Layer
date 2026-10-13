"""W4's scenarios: one Slack workspace, seeded per scenario, and a scripted model."""

from __future__ import annotations

from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import LlmCall, LlmTurn, World
from examples.workflows.w04_slack_ops_bot.agent import (
    ALICE,
    ARCHIVED,
    MENTION_TS,
    OPS,
    THREAD_TS,
)

ANSWER = "Payouts are on schedule."


def _script(call: LlmCall) -> LlmTurn:
    if "incident" in call.last_user_text():
        return LlmTurn(text="Payouts are delayed; investigating.")
    return LlmTurn(text=ANSWER)


def _workspace(world: World) -> None:
    world.slack.add_channel(OPS, "ops")
    world.slack.add_channel(ARCHIVED, "old-ops", is_archived=True)
    world.slack.add_user(ALICE, "alice")
    for channel in (OPS, ARCHIVED):
        world.slack.add_message(channel, THREAD_TS, "payouts look slow today", user=ALICE)
        world.slack.add_message(
            channel, MENTION_TS, "<@U0BOT> are payouts on schedule?", ALICE, thread_ts=THREAD_TS
        )
    world.llm.script = _script


def _no_chat_write(world: World) -> None:
    _workspace(world)
    world.slack.scopes.discard("chat:write")


WORKFLOW = Workflow(
    name="w04_slack_ops_bot",
    summary="A Slack Events API bot: signed inbound events, threaded replies, reactions, a file, "
    "an incoming webhook, and a read-back of its own reply.",
    scenarios={
        "url_verification": Scenario(
            ("url_verification",), setup=_workspace, doc="the challenge handshake: no calls at all"
        ),
        "mention_in_thread": Scenario(
            ("mention_in_thread",),
            setup=_workspace,
            doc="reply in a live thread; the read-back sees it through the overlay",
        ),
        "own_thread": Scenario(
            ("own_thread",),
            setup=_workspace,
            doc="open a top-level message, reply under its minted ts, read that thread (#52)",
        ),
        "channel_by_name": Scenario(
            ("channel_by_name",),
            env={"SLACK_POST_CHANNEL": "#ops"},
            setup=_workspace,
            doc="post to #ops by name: not probed by L3, and read back by the echoed name (#44)",
        ),
        "missing_scope": Scenario(
            ("missing_scope",),
            setup=_no_chat_write,
            diverges=True,
            doc="no chat:write: Slack refuses the post, shadow fakes it (L3 cannot see scopes)",
        ),
        "archived_channel": Scenario(
            ("archived_channel",),
            setup=_workspace,
            doc="an archived channel: L3's is_archived agrees with Slack's own refusal",
        ),
        "duplicate_delivery": Scenario(
            ("duplicate_delivery",),
            setup=_workspace,
            doc="the same event twice (X-Slack-Retry-Num: 1): one reply, not two",
        ),
    },
)
