"""Credential shapes in text, and a length cut made before redaction that never splits one."""

import re

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
# `sk-ant-` precedes the wider `sk-`. A shape may not follow a letter or digit (`task_test_runner`
# survives) unless it ends an escape stored text uses as a separator: `\n`, ` `, `%20`.
_BOUNDARY = r"(?:(?<![A-Za-z0-9])|(?<=\\[bfnrt])|(?<=\\u[0-9A-Fa-f]{4})|(?<=%[0-9A-Fa-f]{2}))"
SECRET_SHAPE = re.compile(_BOUNDARY + "(?:" + "|".join(SECRET_PATTERNS) + ")")


def cut_outside_secrets(text: str, limit: int) -> str:
    """Return `text` cut to at most `limit` chars, moved back to the start of any shape it splits.

    A key cut in half may not match and could reach disk unredacted. Raises TypeError for non-str.
    """
    if len(text) <= limit:
        return text
    for shape in SECRET_SHAPE.finditer(text):
        if shape.end() > limit:
            return text[: min(shape.start(), limit)]
    return text[:limit]
