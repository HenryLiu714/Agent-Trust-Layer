"""Every synthetic credential the tests use, and none of them real.

Each joins a prefix to a body, so no file holds a whole key; `.gitleaks.toml` allowlists them.
"""

# The prefix each kind of key starts with. `irimi.core.secret_shapes` recognises every one.
STRIPE_LIVE = "sk_live_"
STRIPE_TEST = "sk_test_"
STRIPE_RESTRICTED = "rk_live_"
STRIPE_WEBHOOK = "whsec_"
SLACK_BOT = "xoxb-"
SLACK_APP = "xapp-"
GITHUB = "ghp_"
GITHUB_PAT = "github_pat_"
AWS = "AKIA"
ANTHROPIC = "sk-ant-"
OPENAI_PROJECT = "sk-proj-"
OPENAI = "sk-"
SLACK_WEBHOOKS = "https://hooks.slack.com/services/"

# The live Stripe key most redaction tests hide, and a shorter one.
LIVE_KEY = STRIPE_LIVE + "abc123"
LIVE_KEY_SHORT = STRIPE_LIVE + "abc"
TEST_KEY = STRIPE_TEST + "abc"
# The test-mode keys an agent under test sends; the loopback Stripe accepts any of them.
AGENT_STRIPE_KEY = STRIPE_TEST + "notreal"
HERMETIC_STRIPE_KEY = STRIPE_TEST + "hermetic"
HERMETIC_ANTHROPIC_KEY = ANTHROPIC + "hermetic"
STRIPE_TEST_X = STRIPE_TEST + "x"
# A live key an agent must refuse to send.
REFUSED_LIVE_KEY = STRIPE_LIVE + "nope"

# Keys a test plants in a body, a header or an error message, and then looks for on disk.
BEARER_SECRET = STRIPE_LIVE + "SECRET"
MESSAGE_SECRET = STRIPE_LIVE + "secret"
UNREADABLE_SECRET = STRIPE_LIVE + "CANARY0000000000000000"
STORE_SECRET = STRIPE_LIVE + "StoreUnitSecret"
STORE_SECRET_SHORT = STRIPE_LIVE + "A"
TORN_STREAM_SECRET = STRIPE_LIVE + "CutMidCharacter"
PROMPT_SECRET = STRIPE_LIVE + "REPLAYSECRET123"
OTHER_PROMPT_SECRET = STRIPE_LIVE + "ANOTHERSECRET456"
RECORDED_SECRET = STRIPE_LIVE + "ABCDEF123456"
OTHER_RECORDED_SECRET = STRIPE_LIVE + "OTHER9876"
E2E_STRIPE_KEY = STRIPE_TEST + "RedactE2e"
E2E_LLM_KEY = ANTHROPIC + "api03-RedactE2eHeaderKey"
E2E_NESTED_KEY = STRIPE_LIVE + "NestedThreeDeep"
E2E_SSE_PLAIN_KEY = STRIPE_LIVE + "StreamedPlain"
E2E_SSE_ESCAPED_KEY = STRIPE_LIVE + "AfterAnEscape"
