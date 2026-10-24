"""The Slack `Service` spec; its map is `maps/slack.yaml` and fixtures `fixtures/slack.json`."""

from irimi.services.model import Answer, Service
from irimi.services.slack import answers, effects, preconditions

# Slack has no idempotency mechanism: a repeated `chat.postMessage` is two messages. With no
# `scope_headers`, a run has one Slack scope.
SERVICE = Service(
    name="slack",
    operations=frozenset(
        {
            "chat.postMessage",
            "conversations.history",
            "conversations.info",
            "conversations.replies",
            "files.upload",
            "incoming_webhook",
            "reactions.add",
        }
    ),
    answer=Answer(body=answers.body, literal=answers.LITERAL_BODIES),
    observe_read=answers.observe_read,
    effects=effects.apply_read,
    preconditions={"channel_postable": preconditions.CHANNEL_POSTABLE},
)
