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


# What a cut made with no redaction key, the SDK's, puts where a secret ran across it: dropping the
# secret instead would post a value that is one long key as empty.
CUT_MARK = "<redacted>"


def cut_marking_secrets(text: str, limit: int) -> str:
    """Return `cut_outside_secrets(text, limit)` with a secret the cut crosses marked CUT_MARK.

    For a cut made before redaction with no key (the SDK's); a limit too small for the mark drops
    the secret unmarked. The store, which holds a key, puts the placeholder there instead.
    """
    cut = cut_outside_secrets(text, limit)
    if len(cut) == len(text) or limit < len(CUT_MARK):
        return cut
    if SECRET_SHAPE.match(text, len(cut)) is None:
        return cut
    return cut_outside_secrets(text, max(0, limit - len(CUT_MARK))) + CUT_MARK


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
