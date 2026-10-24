"""Every synthetic credential the sample workflows use, and none of them real.

Each is its prefix joined to a body, so no file holds a whole key for a secret scanner to flag;
`.gitleaks.toml` allowlists the joined values. `tests/support/credentials.py` is the tests' copy.
"""

# Given to every workflow's agent: fake, recognisable, and greppable. The fake services accept
# them by prefix, and one found on disk under irimi's home is a redaction leak.
STRIPE_API_KEY = "sk_test_" + "CANARYstripe0000000000000000"
SLACK_BOT_TOKEN = "xoxb-" + "CANARY-slack-0000000000"
SLACK_SIGNING_SECRET = "CANARY" + "slacksigning00000000000000"
STRIPE_WEBHOOK_SECRET = "whsec_" + "CANARYwebhook000000000000000"
ANTHROPIC_API_KEY = "sk-ant-" + "CANARYanthropic0000000000000"
OPENAI_API_KEY = "sk-" + "CANARYopenai000000000000000000000"
SLACK_WEBHOOK_PATH = "/services/T0CANARY/B0CANARY/" + "CANARYwebhookpath000000"

# A key W10's agent raises with as its whole error message, longer than any cut irimi makes of one.
LONG_ERROR_KEY = "sk_live_" + "CANARYlongerror" + "0" * 4000

# Handed back by a fake service in a response body, never given to the agent.
CLIENT_SECRET = "CANARY" + "clientsecret000000000000"
BOMB_SECRET = "CANARY" + "bombsecret00000000000000"

# A live key W7's user pastes into a question, which the stream must carry redacted.
STREAM_SECRET = "sk_live_" + "CANARYstreamsecret0000000000"

# The secret W5's `bad_signature` scenario signs a webhook with: not the one the handler holds.
WRONG_WEBHOOK_SECRET = "whsec_" + "not_the_real_one"
