"""Every size bound irimi enforces, one line each, so how they relate is read in one place.

Sizes are bytes unless the comment says characters. `tests/test_limits.py` holds the
relationships between them.
"""

KiB = 1024
MiB = 1024 * KiB

# Bodies. A body is decoded no further than it is redacted, and stored no larger than that.
MAX_REDACTED_BODY = 64 * MiB  # a larger body is stored empty, never redacted
MAX_DECODED_BODY = MAX_REDACTED_BODY  # a compressed body decodes to at most this; the rest is cut
MAX_STORED_BODY = 8 * MiB  # a larger body keeps its start, flagged `body-truncated`
MAX_QUEUED_BYTES = 256 * MiB  # body bytes the store holds for its writer before it drops
MAX_BODY_BYTES = 2 * MiB  # the largest live JSON body irimi parses to apply or check a write

# The control endpoint and the SDK that posts to it.
MAX_CONTROL_BODY = 2 * MiB  # the largest body a control request may post
MAX_CAPTURED_BYTES = 1 * MiB  # a trigger's captured args, so a start fits the control body
MAX_ANSWER = 16 * MAX_CAPTURED_BYTES  # the largest control answer the SDK reads
MAX_NAME = 1000  # characters of a run's or tool call's name, cut outside any secret's shape

# Error labels, in characters. The store redacts each whole and only then cuts it.
MAX_ERROR_MESSAGE = 1000  # an error message a trace keeps
MAX_ERROR_TYPE = 300  # an error type a trace keeps
MAX_POSTED_MESSAGE = 64 * KiB  # the most of a message the SDK posts for the store to cut
MAX_POSTED_TYPE = 4 * MAX_ERROR_TYPE  # the most of a type the SDK posts
MAX_QUEUED_ERROR = 4 * MAX_ERROR_MESSAGE  # the most of an end's error that waits for the writer

# Runs. The ended runs a write log and a key store remember, so a write sent after its run ended
# is never filed under it again: a serve of many runs holds this many ids and nothing more.
MAX_ENDED_RUNS = 4096
