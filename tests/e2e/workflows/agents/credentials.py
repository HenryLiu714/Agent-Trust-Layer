"""Every synthetic credential the sample workflows use, none of them real.

Each joins a prefix to a body, so no file holds a whole key; `.gitleaks.toml` allows the values."""

# Given to every workflow's agent: fake, recognisable, and greppable. The fake services accept
# them by prefix, and one found on disk under irimi's home is a redaction leak.
STRIPE_API_KEY = "sk_test_" + "CANARYstripe0000000000000000"
SLACK_BOT_TOKEN = "xoxb-" + "CANARY-slack-0000000000"
SLACK_SIGNING_SECRET = "CANARY" + "slacksigning00000000000000"
STRIPE_WEBHOOK_SECRET = "whsec_" + "CANARYwebhook000000000000000"
ANTHROPIC_API_KEY = "sk-ant-" + "CANARYanthropic0000000000000"
OPENAI_API_KEY = "sk-" + "CANARYopenai000000000000000000000"
SLACK_WEBHOOK_PATH = "/services/T0CANARY/B0CANARY/" + "CANARYwebhookpath000000"

# Handed back by a fake service in a response body, never given to the agent.
CLIENT_SECRET = "CANARY" + "clientsecret000000000000"
BOMB_SECRET = "CANARY" + "bombsecret00000000000000"

# A live key W7's user pastes into a question, which the stream must carry redacted.
STREAM_SECRET = "sk_live_" + "CANARYstreamsecret0000000000"

# The secret W5's `bad_signature` scenario signs a webhook with: not the one the handler holds.
WRONG_WEBHOOK_SECRET = "whsec_" + "not_the_real_one"
