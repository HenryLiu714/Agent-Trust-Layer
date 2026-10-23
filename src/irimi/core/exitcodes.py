"""The exit codes every irimi command shares, and the table their `--help` prints.

CI jobs key on these. `compare` and `replay` exit as diff(1) does, any failure exits `ERROR`, and
the rest follow the shell's convention (126/127 for a command that cannot start, 128 + signal).
"""

from collections.abc import Sequence

OK = 0
DIFFERENT = 1
ERROR = 2
CANNOT_EXEC = 126
NOT_FOUND = 127
SIGINT = 130
BROKEN_PIPE = 141
SIGTERM = 143

MEANINGS = {
    OK: "every write is the same",
    DIFFERENT: "a write differs",
    ERROR: "irimi could not do it, or was invoked wrongly",
    CANNOT_EXEC: "the command was found but could not be run",
    NOT_FOUND: "the command was not found",
    SIGINT: "stopped by SIGINT (Ctrl-C)",
    BROKEN_PIPE: "whatever read irimi's output went away",
    SIGTERM: "stopped by SIGTERM",
}


def help_table(codes: Sequence[int], lead: str | None = None) -> str:
    """Return the `exit status:` block of a `--help`: `lead` if given, then a line per code."""
    lines = ["exit status:"]
    if lead is not None:
        lines.append(f"  {lead}")
    lines.extend(f"  {code:<5}{MEANINGS[code]}" for code in codes)
    return "\n".join(lines)
