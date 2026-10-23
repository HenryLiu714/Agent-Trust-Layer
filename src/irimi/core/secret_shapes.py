"""The shapes a secret is found by in text, and the one cut made before redaction.

`redact` replaces every match of SECRET_SHAPE with its placeholder before anything reaches disk.
This module says only where the shapes are, so that a cut made before redaction - by the SDK,
which posts what it cuts and has no key, or by the store as an end waits for its writer - can be
made where no shape crosses it. Pure and layer 0: it imports nothing of irimi's.
"""

import re

# Credential shapes, replaced wherever they appear in text irimi can read: header values, the
# path, query and form values, JSON strings, and a UTF-8 body that is neither JSON nor a form.
SECRET_PATTERNS: tuple[str, ...] = (
    r"sk_(live|test)_[A-Za-z0-9]+",
    r"rk_(live|test)_[A-Za-z0-9]+",
    r"whsec_[A-Za-z0-9]+",
    r"xox[abprs]-[A-Za-z0-9-]+",
    r"xapp-[A-Za-z0-9-]+",
    r"gh[pousr]_[A-Za-z0-9]{36,}",
    r"github_pat_[A-Za-z0-9_]+",
    r"AKIA[0-9A-Z]{16}",
    r"sk-ant-[A-Za-z0-9_-]+",
    r"sk-(proj-)?[A-Za-z0-9_-]{20,}",
)
# One pass over the text, the patterns tried in the order above at each position, so `sk-ant-` is
# replaced whole before the wider `sk-` could claim it. A shape must not follow a letter or a
# digit: without that, `task_test_runner` would be stored as `ta<redacted:…>` and any slug holding
# `risk-` followed by 20 more characters would lose its tail. A real key follows a space, a
# quote, `=`, `/`, `_` or the start of the value, and still matches.
#
# Unless that letter or digit ends an escape. Text stored raw spells a separator as one: a JSON
# string inside an SSE line as `\n` or `\u0020`, a path, a header or a form field's name as `%20`,
# `%3D` or `%2F`. Each ends in a letter or a digit, so `key:\nsk_live_…` and
# `/Bearer%20sk_live_…` would otherwise be stored whole.
_BOUNDARY = r"(?:(?<![A-Za-z0-9])|(?<=\\[bfnrt])|(?<=\\u[0-9A-Fa-f]{4})|(?<=%[0-9A-Fa-f]{2}))"
SECRET_SHAPE = re.compile(_BOUNDARY + "(?:" + "|".join(SECRET_PATTERNS) + ")")


def cut_outside_secrets(text: str, limit: int) -> str:
    """`text` cut to at most `limit` characters, never inside a secret's shape: when a shape in
    `text` runs across the cut, the cut is made where that shape starts instead.

    For a cut made before redaction. Cut inside a key, the part kept is no longer the key's
    shape, so redaction stores it as it is, or, for a shape whose prefix still matches
    (`sk_test_…`), as a placeholder of its own, not the key's. Cut here, what is kept,
    redacted, is a prefix of the whole redacted: no shape crosses the cut, and every shape that
    ends by it is matched in the cut text as it is in the whole. Only a key across the cut is
    lost, never the text around it. Raises TypeError when `text` is not a string."""
    if len(text) <= limit:
        return text
    for shape in SECRET_SHAPE.finditer(text):
        if shape.end() > limit:
            return text[: min(shape.start(), limit)]
    return text[:limit]
