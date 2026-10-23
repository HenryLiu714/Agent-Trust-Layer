"""Every `Irimi-` header name, lower-case, as irimi stores header names and compares them."""

# The run a request belongs to. The SDK puts it on every request a run makes; irimi reads it and
# strips it before forwarding.
RUN_HEADER = "irimi-run"
# Stamped on every response irimi decided rather than forwarded, carrying the `answered_by` value
# itself. A live forward carries none (`wire.answered_by_header`).
ANSWERED_BY_HEADER = "irimi-answered-by"
# A name in irimi's own header vocabulary that irimi never sends. The engine strips one an agent
# sends, as it strips every `Irimi-` header that is not the agent's to set.
REWROTE_HEADER = "irimi-rewrote"
# The control protocol version the SDK speaks, on every post, and the endpoint, on every answer.
PROTOCOL_HEADER = "irimi-protocol"
