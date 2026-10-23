"""Slack: everything irimi's Python knows about it, as one `Service` spec.

Its map is `irimi/maps/slack.yaml` and its fixture objects are `irimi/fixtures/slack.json`.
`answers` builds the envelopes its faked writes get and the `ts` sequence they take their identity
from, `effects` applies those writes to later live reads, and `preconditions` holds the post check.
"""

from irimi.services.model import Answer, Service
from irimi.services.slack import answers, effects, preconditions

# Slack ships no idempotency mechanism, and `chat.postMessage` sent twice really is two messages,
# so it has no `idempotency` and no store.
#
# Slack has no `scope_headers`, so a run has exactly one Slack scope, which is the right answer
# today. If the token's team is wanted later, `scope_headers=("authorization",)` is the whole
# change: the header is already on each write's own request, and it is only ever compared, never
# stored.
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
