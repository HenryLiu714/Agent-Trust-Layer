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
# Flags irimi 0.1 spelled otherwise. They are refused like any unknown flag, with a hint.
RENAMED_FLAGS = {
    "--listen-host": "--bind",
    "--allow-host": "--base-url-host",
    "--allow-target-host": "--allow-remote-target",
    "--call": "--entrypoint",
}

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
    return _parse(build_parser(_command_typed(argv)), argv)


def _parse(parser: argparse.ArgumentParser, argv: list[str]) -> argparse.Namespace:
    """Parse `argv` before its first `--`; what follows becomes `args.cmd` (shadow and replay).

    An argument no parser takes is refused by the command it was typed to, with its usage.
    """
    split = argv.index("--") if "--" in argv else len(argv)
    before, after = argv[:split], argv[split + 1 :]
    if before and before[0] in DEFAULT_SUBCOMMANDS:
        rest = before[1:]
        if not rest or (rest[0].startswith("-") and rest[0] not in ("-h", "--help")):
            before = [before[0], DEFAULT_SUBCOMMANDS[before[0]], *rest]
    args, extras = parser.parse_known_args(before)
    if "cmd" in vars(args):
        args.cmd = after
    else:
        extras += argv[split:]
    if extras:
        refusal = [f"unrecognized arguments: {' '.join(extras)}", *_hints(args, extras)]
        _innermost(parser, args).error("\n".join(refusal))
    return args


def _hints(args: argparse.Namespace, extras: list[str]) -> list[str]:
    """Return what to type instead of `extras`: a renamed flag, `irimi serve`, or a `--`."""
    hints = []
    for extra in extras:
        flag = extra.partition("=")[0]
        if flag in RENAMED_FLAGS:
            hints.append(f"hint: {flag} is spelled {RENAMED_FLAGS[flag]}")
        elif flag == "--serve":
            hints.append("hint: serve mode is its own command: irimi serve")
    if "cmd" in vars(args) and not extras[0].startswith("-"):
        hints.append("hint: put the command to run after --")
    return hints


def _innermost(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> argparse.ArgumentParser:
    """Return the parser of the command `args` names, down to a subcommand's (`runs show`)."""
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            chosen = action.choices.get(getattr(args, action.dest, None) or "")
            if chosen is not None:
                return _innermost(chosen, args)
    return parser


def _main(argv: list[str] | None, engine_factory: "EngineFactory | None") -> int:
    argv = sys.argv[1:] if argv is None else argv
    parser = build_parser(_command_typed(argv))
    args = _parse(parser, argv)
    if args.command is None:
        parser.print_help(sys.stderr)
        return exitcodes.ERROR
    command: Callable[..., int] = args.func
    try:
        if engine_factory is None:
            return command(args)
        return command(args, engine_factory=engine_factory)
    except UsageError as exc:
        _innermost(parser, args).error(str(exc))
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
