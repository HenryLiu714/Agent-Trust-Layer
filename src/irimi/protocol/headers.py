"""Every `Irimi-` header name, lower-case, as irimi stores and compares header names."""

# The run a request belongs to; irimi strips it before forwarding.
RUN_HEADER = "irimi-run"
# The answered-by header (see `wire.answered_by_header`).
ANSWERED_BY_HEADER = "irimi-answered-by"
# A reserved name irimi never sends; the engine strips it from agent requests.
REWROTE_HEADER = "irimi-rewrote"
# The control protocol version, on every SDK post and every endpoint answer.
PROTOCOL_HEADER = "irimi-protocol"
