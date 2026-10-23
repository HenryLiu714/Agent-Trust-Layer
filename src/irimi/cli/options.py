"""Shared parser pieces: argparse types, and the proxy, map and storage options."""

import argparse
import json
from pathlib import Path
from typing import Any

from irimi.cli import os_error
from irimi.core import exitcodes
from irimi.protocol.trace import UNATTRIBUTED, JSONValue

LISTEN_HOST = "127.0.0.1"
DEFAULT_PORT = 4000
# What `runs show`, `compare` and `replay` take for the newest stored run.
LAST_RUN = "last"
RUN_ID_HELP = (
    f"a run id from `irimi runs list`, a prefix only one run id starts with, {LAST_RUN} for the "
    f"newest, or {UNATTRIBUTED} for traffic no run claimed"
)


def help_formatted(**kwargs: Any) -> dict[str, Any]:
    """Return `add_parser` kwargs for hand-laid-out help with abbreviated flags refused."""
    return {
        "allow_abbrev": False,
        "formatter_class": argparse.RawDescriptionHelpFormatter,
        **kwargs,
    }


def epilog(examples: list[str], exit_codes: str | None = None) -> str:
    """Return a `--help` epilog: the examples, then the exit status table if given."""
    text = "examples:\n" + "\n".join(f"  {example}" for example in examples)
    return text if exit_codes is None else f"{text}\n\n{exit_codes}"


# What `shadow` exits with, beyond its command's own code.
SHADOW_EXIT_CODES = exitcodes.help_table(
    [
        exitcodes.ERROR,
        exitcodes.CANNOT_EXEC,
        exitcodes.NOT_FOUND,
        exitcodes.SIGINT,
        exitcodes.BROKEN_PIPE,
        exitcodes.SIGTERM,
    ],
    lead="the command's own exit code, or:",
)


def _host_arg(value: str) -> str:
    """Parse a bare, lower-cased host name; a scheme, port or path would never match."""
    host = value.strip().lower()
    if not host or "/" in host or ":" in host:
        raise argparse.ArgumentTypeError(
            f"{value!r}: give a bare host name with no scheme, port or path "
            "(the port goes on the request path, e.g. /127.0.0.1:8443/...)"
        )
    return host


def _target_arg(value: str) -> tuple[str, str, str]:
    """Parse --target '<host>[<path>]=<url>' into (host, path, url).

    A bare host, or a host with only a trailing `/`, targets the whole service. The map loader
    validates the URL.
    """
    spec, sep, url = value.partition("=")
    host, slash, path = spec.strip().partition("/")
    host = host.lower()
    if not sep or not url.strip() or not host or ":" in host:
        raise argparse.ArgumentTypeError(
            f"{value!r}: use --target '<host>[<path>]=<url>', e.g. "
            "--target 'api.stripe.com/v1/refunds=http://127.0.0.1:3000/refund' "
            "(a bare host sets the whole service's target)"
        )
    return host, f"{slash}{path}" if slash and path else "", url.strip()


def add_proxy_options(parser: argparse.ArgumentParser, port: int) -> None:
    """Add the proxy, map and store options `serve`, `shadow` and `replay` share.

    `port` is the default `--port`; 0 picks any free port.
    """
    proxy = parser.add_argument_group("proxy options")
    default = f"{port}; 0 picks any free port" if port else "0, any free port"
    proxy.add_argument(
        "--port", type=int, default=port, help=f"port to listen on (default {default})"
    )
    proxy.add_argument(
        "--bind",
        default=LISTEN_HOST,
        metavar="HOST",
        help=f"address to bind (default {LISTEN_HOST}); anything else exposes the proxy to the "
        "network",
    )
    proxy.add_argument(
        "--base-url-host",
        action="append",
        default=[],
        type=_host_arg,
        metavar="HOST",
        help=f"also allow HOST in base-URL mode, http://{LISTEN_HOST}:PORT/HOST/PATH, for SDKs "
        "that ignore proxy settings (mapped hosts are always allowed; repeatable)",
    )
    proxy.add_argument(
        "--quiet",
        action="store_true",
        help="print only the banner and the summary, not a line per exchange and tool call",
    )
    add_map_options(parser)
    add_store_option(parser, "where to record runs, redacted")


def add_map_options(parser: argparse.ArgumentParser) -> None:
    """Add the target options, which change what the loaded maps say."""
    maps = parser.add_argument_group("map options")
    maps.add_argument(
        "--target",
        action="append",
        default=[],
        type=_target_arg,
        metavar="SPEC",
        dest="target",
        help="answer a route from a URL you control instead of faking it: "
        "'<host>[<path>]=<url>', e.g. 'api.stripe.com/v1/refunds=http://127.0.0.1:3000/refund'; "
        "a bare host sets the whole service's target (repeatable; overrides ./irimi.maps.yaml)",
    )
    maps.add_argument(
        "--target-reads",
        action="append",
        default=[],
        type=_host_arg,
        metavar="HOST",
        help="send HOST's reads to its --target too, instead of the real service (repeatable)",
    )
    maps.add_argument(
        "--allow-remote-target",
        action="append",
        default=[],
        type=_host_arg,
        metavar="HOST",
        help="allow a --target on HOST, off this machine (repeatable)",
    )


def add_store_option(
    parser: argparse.ArgumentParser, what: str = "the trace store to read"
) -> None:
    storage = parser.add_argument_group("storage")
    storage.add_argument(
        "--store",
        type=Path,
        default=None,
        metavar="DIR",
        help=f"{what} (default $IRIMI_STORE, else ~/.irimi/store or $IRIMI_HOME/store)",
    )


def entrypoint_arg(value: str) -> str:
    """Parse --entrypoint: a function's `module:qualname`."""
    module, sep, qualname = value.partition(":")
    if not sep or not module.strip() or not qualname.strip():
        raise argparse.ArgumentTypeError(f"{value!r}: give the function as module:qualname")
    return value


def args_arg(value: str) -> dict[str, JSONValue]:
    """Parse --args: a JSON object of `{parameter: captured value}`."""
    try:
        parsed = json.loads(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--args is not JSON: {exc}") from None
    except RecursionError:
        # Otherwise argparse would show a traceback instead of a usage error.
        raise argparse.ArgumentTypeError("--args is nested deeper than it can be read") from None
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("--args is a JSON object of {parameter: value}")
    return parsed


def args_file_arg(value: str) -> dict[str, JSONValue]:
    """Parse --args-file: `--args` read from the file `value`."""
    try:
        text = Path(value).read_text(encoding="utf-8")
    except OSError as exc:
        raise argparse.ArgumentTypeError(os_error(f"--args-file {value}", exc)) from None
    return args_arg(text)


def positive_int(value: str) -> int:
    """Parse --limit: a whole number of runs, at least one."""
    try:
        number = int(value)
    except ValueError:
        number = 0
    if number < 1:
        raise argparse.ArgumentTypeError(f"{value!r}: give a whole number of runs, at least 1")
    return number
