"""The `irimi` command: the parser, built from each command module's `register`, and dispatch.

`main` imports only the typed command's module, so a command loads only what it runs. `options`
holds shared parser options; `compose` is the composition root for commands that start a proxy.
"""

import argparse
import contextlib
import importlib
import os
import sys
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from irimi import __version__
from irimi.core import exitcodes

if TYPE_CHECKING:  # the quoted annotation below; no runtime import
    from irimi.cli.compose import EngineFactory

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
# The subcommand run when none is typed.
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
    """A bad invocation argparse could not catch; `main` prints usage and exits 2."""


class CliError(Exception):
    """A command that cannot continue; `main` prints `error: <message>` and exits `code`."""

    def __init__(self, message: str, code: int) -> None:
        super().__init__(message)
        self.message = message
        self.code = code


def os_error(subject: str, exc: OSError) -> str:
    """Return `<subject>: <strerror>` for `exc`, or its own message when it has no strerror."""
    if not exc.strerror:
        return str(exc)
    return f"{subject}: {exc.strerror[:1].lower()}{exc.strerror[1:]}"


def build_parser(command: str | None = None) -> argparse.ArgumentParser:
    """Return the `irimi` parser, with only `command`'s subparser when it names one."""
    return _parser(command)[0]


def _parser(
    command: str | None,
) -> tuple[argparse.ArgumentParser, dict[str, argparse.ArgumentParser]]:
    """Return the parser and its commands' parsers by name."""
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
    """Return `argv`'s first word, or None when it is an option, which needs every parser."""
    if not argv or argv[0].startswith("-"):
        return None
    return argv[0]


def main(argv: list[str] | None = None, *, engine_factory: "EngineFactory | None" = None) -> int:
    """Run the `irimi` command and return its exit code.

    A closed output pipe (`| head -1`) exits quietly with `exitcodes.BROKEN_PIPE`; both streams are
    flushed here so that is caught before interpreter exit. `engine_factory` replaces
    `compose.build_engine` for `shadow`, `serve` and `replay`.
    """
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
    """Return `argv` parsed as `irimi` parses it."""
    parser, commands = _parser(_command_typed(argv))
    return _parse(parser, commands, argv)


def _parse(
    parser: argparse.ArgumentParser,
    commands: dict[str, argparse.ArgumentParser],
    argv: list[str],
) -> argparse.Namespace:
    """Parse `argv` before its first `--`; what follows becomes `args.cmd` (shadow and replay)."""
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
    """Point each stream whose reader has gone at /dev/null.

    Otherwise the interpreter's flush at exit raises BrokenPipeError again and exits 120.
    """
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
