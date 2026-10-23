"""The `irimi` command: the parser, assembled from each command module's `register`, and the
dispatch to the command typed.

Each command lives in a module of its own (`init`, `serve`, `shadow`, `maps`, `runs`,
`replay_cmd`), which imports what it needs at its top. `main` imports only the module of the
command typed, so a command loads only what it runs; `options` holds what their parsers share,
and `session` the composition root that the commands which start a proxy share.
"""

import argparse
import contextlib
import importlib
import os
import sys
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from irimi import __version__, exitcodes

if TYPE_CHECKING:  # the quoted annotation below; no runtime import
    from irimi.cli.session import EngineFactory

# Each command's module, in the order `irimi --help` lists the commands.
COMMANDS = {
    "init": "irimi.cli.init",
    "shadow": "irimi.cli.shadow",
    "serve": "irimi.cli.serve",
    "replay": "irimi.cli.replay_cmd",
    "runs": "irimi.cli.runs",
    "compare": "irimi.cli.runs",
    "maps": "irimi.cli.maps",
}
# The subcommand a command with subcommands runs when none is typed: `irimi runs` lists the runs.
DEFAULT_SUBCOMMANDS = {"runs": "list", "maps": "list"}

DESCRIPTION = """\
Shadow mode for AI agents: reads are real, writes are faked.

  faked write  a write irimi answered itself; the real service never saw it
  target       a URL you name (--target) that answers a service in irimi's place"""
EPILOG = """\
examples:
  irimi init
  irimi shadow -- python3 agent.py
  irimi runs show last"""


class UsageError(Exception):
    """A command invoked in a way its parser could not refuse by itself: `main` answers it as
    argparse answers a bad invocation, with the command's usage and exit 2."""


class CliError(Exception):
    """A command that cannot go on: `main` prints `error: <message>` and exits `code`."""

    def __init__(self, message: str, code: int) -> None:
        super().__init__(message)
        self.message = message
        self.code = code


def os_error(subject: str, exc: OSError) -> str:
    """An OS error named by what irimi was doing, `<subject>: not a directory`, rather than by
    Python's `[Errno 20] Not a directory: '...'`. One irimi raised with a message of its own
    already names its subject, and is said as it is."""
    if not exc.strerror:
        return str(exc)
    return f"{subject}: {exc.strerror[:1].lower()}{exc.strerror[1:]}"


def build_parser(command: str | None = None) -> argparse.ArgumentParser:
    """The `irimi` parser, with every command, or with `command`'s alone when it names one."""
    return _parser(command)[0]


def _parser(
    command: str | None,
) -> tuple[argparse.ArgumentParser, dict[str, argparse.ArgumentParser]]:
    """The parser and its commands' own parsers, by name."""
    parser = argparse.ArgumentParser(
        prog="irimi",
        allow_abbrev=False,
        description=DESCRIPTION,
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"irimi {__version__}")
    subparsers = parser.add_subparsers(dest="command", metavar="<command>")
    modules: Iterable[str] = COMMANDS.values()
    if command in COMMANDS:
        modules = [COMMANDS[command]]
    for name in dict.fromkeys(modules):
        importlib.import_module(name).register(subparsers)
    return parser, subparsers.choices


def _command_typed(argv: list[str]) -> str | None:
    """The command `argv` names, its first word, or None when that is an option: a root option
    before the command (`-h`, `--version`) needs every command's parser."""
    if not argv or argv[0].startswith("-"):
        return None
    return argv[0]


def main(argv: list[str] | None = None, *, engine_factory: "EngineFactory | None" = None) -> int:
    """The `irimi` command. A reader of its output that goes away (`irimi runs show X | head -1`,
    or `2>&1 | head -1`, where the write that fails may be stderr's) ends it quietly with
    `exitcodes.BROKEN_PIPE`, wherever it was writing, `--help` included. stdout and stderr
    are flushed here, not at exit, so a reader gone by the last write is caught too, and so is a
    warning the store's logger could not write, which the logger swallows and leaves buffered; a
    stream closed before irimi started is None, which `print` writes nothing to.

    `engine_factory` builds the engine in place of `session.build_engine`, for the commands that
    start a proxy (`shadow`, `serve` and `replay`): a harness's way to watch or swap the engine."""
    try:
        try:
            return _main(argv, engine_factory)
        finally:
            for stream in (sys.stdout, sys.stderr):
                if stream is not None:
                    stream.flush()
    except BrokenPipeError:
        _discard_unread_output()
        return exitcodes.BROKEN_PIPE


def parse_args(argv: list[str]) -> argparse.Namespace:
    """`argv` parsed as `irimi` parses it, for a caller that runs a command's function itself."""
    parser, commands = _parser(_command_typed(argv))
    return _parse(parser, commands, argv)


def _parse(
    parser: argparse.ArgumentParser,
    commands: dict[str, argparse.ArgumentParser],
    argv: list[str],
) -> argparse.Namespace:
    """`argv` split at its first `--`: argparse parses what comes before, and what comes after is
    the command `shadow` and `replay` run, `args.cmd`. Every other command takes nothing there."""
    split = argv.index("--") if "--" in argv else len(argv)
    before, after = argv[:split], argv[split + 1 :]
    if before and before[0] in DEFAULT_SUBCOMMANDS:
        rest = before[1:]
        if not rest or (rest[0].startswith("-") and rest[0] not in ("-h", "--help")):
            before = [before[0], DEFAULT_SUBCOMMANDS[before[0]], *rest]
    args = parser.parse_args(before)
    if "cmd" in vars(args):
        args.cmd = after
    elif split < len(argv):
        (commands.get(args.command) or parser).error(
            f"unrecognized arguments: {' '.join(argv[split:])}"
        )
    return args


def _main(argv: list[str] | None, engine_factory: "EngineFactory | None") -> int:
    argv = sys.argv[1:] if argv is None else argv
    parser, commands = _parser(_command_typed(argv))
    args = _parse(parser, commands, argv)
    if args.command is None:
        parser.print_help(sys.stderr)
        return exitcodes.ERROR
    command: Callable[..., int] = args.func
    try:
        if engine_factory is None:
            return command(args)
        return command(args, engine_factory=engine_factory)
    except UsageError as exc:
        commands[args.command].error(str(exc))
    except CliError as exc:
        print(f"error: {exc.message}", file=sys.stderr)
        return exc.code


def _discard_unread_output() -> None:
    """Point stdout, and stderr, at /dev/null when its reader has gone: one that still holds
    what it could not write fails to flush, and the interpreter's flush of it at exit would raise
    BrokenPipeError again and exit 120. A stream that flushes is still read and is left as
    it is; one that is None (closed before irimi started) or no file (a test's) has nothing to
    repoint."""
    for stream in (sys.stdout, sys.stderr):
        if stream is None:
            continue
        try:
            stream.flush()
            continue
        except OSError:
            pass
        devnull = os.open(os.devnull, os.O_WRONLY)
        try:
            with contextlib.suppress(OSError):  # io.UnsupportedOperation is an OSError
                os.dup2(devnull, stream.fileno())
        finally:
            os.close(devnull)
