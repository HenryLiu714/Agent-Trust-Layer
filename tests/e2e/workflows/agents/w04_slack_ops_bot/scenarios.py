"""W4's scenarios: one Slack workspace, seeded per scenario, and a scripted model."""

from __future__ import annotations

from collections.abc import Callable

from irimi.servicemaps import loader
from tests.e2e.workflows.agents.w04_slack_ops_bot.agent import (
    ALICE,
    ARCHIVED,
    MENTION_TS,
    OPS,
    THREAD_TS,
)
from tests.e2e.workflows.harness.scenario import Scenario, Workflow
from tests.e2e.workflows.harness.services import LlmCall, LlmTurn, World

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


# Two answers that print alike: one holds a newline, the other a backslash and an `n`.
TWO_LINES = "Payouts are on schedule.\nNext run at 09:00."
TYPED_ESCAPE = "Payouts are on schedule.\\nNext run at 09:00."


def _answering(text: str) -> Callable[[World], None]:
    """`_workspace` with a model that answers `text` to every mention."""

    def setup(world: World) -> None:
        _workspace(world)
        world.llm.script = lambda call: LlmTurn(text=text)

    return setup


def _no_chat_write(world: World) -> None:
    _workspace(world)
    world.slack.scopes.discard("chat:write")


# The shipped Slack map with `chat.postMessage`'s fixture renamed to one `slack.json` lacks. irimi
# refuses a map whose `fixture:` names nothing, so it never starts.
NO_POST_FIXTURE_MAP = (
    (loader.shipped_dir() / "slack.yaml")
    .read_text()
    .replace("fixture: message", "fixture: no_such_message")
)


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
            doc="open a top-level message, reply under its minted ts, read that thread",
        ),
        "post_fixture_missing": Scenario(
            ("own_thread",),
            setup=_workspace,
            extra_maps={"slack.yaml": NO_POST_FIXTURE_MAP},
            diverges=True,
            doc="the post's `fixture:` names an object slack.json lacks: irimi refuses the map "
            "and never starts",
        ),
        "channel_by_name": Scenario(
            ("channel_by_name",),
            env={"SLACK_POST_CHANNEL": "#ops"},
            setup=_workspace,
            doc="post to #ops by name: not checked, and read back by the echoed name",
        ),
        "missing_scope": Scenario(
            ("missing_scope",),
            setup=_no_chat_write,
            diverges=True,
            doc="no chat:write: Slack refuses the post, shadow fakes it (no check sees scopes)",
        ),
        "archived_channel": Scenario(
            ("archived_channel",),
            setup=_workspace,
            doc="an archived channel: the precondition check agrees with Slack's own refusal",
        ),
        "two_line_answer": Scenario(
            ("mention_in_thread",),
            setup=_answering(TWO_LINES),
            doc="mention_in_thread with a two-line answer: the post's newline, escaped wherever "
            "irimi prints it",
        ),
        "typed_escape_answer": Scenario(
            ("mention_in_thread",),
            setup=_answering(TYPED_ESCAPE),
            doc="mention_in_thread with an answer holding a typed backslash-n, which prints as "
            "two_line_answer's newline does, and is not the same post",
        ),
        "duplicate_delivery": Scenario(
            ("duplicate_delivery",),
            setup=_workspace,
            doc="the same event twice (X-Slack-Retry-Num: 1): one reply, not two",
        ),
    },
)
