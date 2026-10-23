"""What the commands' parsers share: the argparse types, the proxy options and `--store`."""

import argparse
import json
from pathlib import Path

from irimi.cli import os_error
from irimi.trace import JSONValue

LISTEN_HOST = "127.0.0.1"
DEFAULT_PORT = 4000
# What `runs show`, `compare` and `replay` take for the newest stored run.
LAST_RUN = "last"
RUN_ID_HELP = f"a run id, a prefix only one run id starts with, {LAST_RUN}, or unattributed"


def _host_arg(value: str) -> str:
    """argparse type for --allow-host: a bare host name, lower-cased. The port belongs on the
    request path (/127.0.0.1:8443/...), so a value with a scheme, port or path is a mistake that
    would otherwise never match anything."""
    host = value.strip().lower()
    if not host or "/" in host or ":" in host:
        raise argparse.ArgumentTypeError(
            f"{value!r}: give a bare host name with no scheme, port or path "
            "(the port goes on the request path, e.g. /127.0.0.1:8443/...)"
        )
    return host


def _target_arg(value: str) -> tuple[str, str, str]:
    """argparse type for --target '<host>[<path>]=<url>'.

    `api.stripe.com=http://127.0.0.1:3000` sets the service target;
    `api.stripe.com/v1/refunds=http://127.0.0.1:3000/refund` sets one route's. The URL is
    validated by the map loader, which owns every target rule.

    A trailing bare slash is the service too: `api.stripe.com/=<url>` is how a reader used to
    `proxy_pass` spells "the whole service", and taken as the route path `/` it would match no
    map's route.
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


def add_proxy_options(parser: argparse.ArgumentParser, port: int | None) -> None:
    """The options `serve`, `shadow` and `replay` share. `port` is the default `--port`: 0 for any
    free port, which the command irimi runs is told in its environment, or None for `shadow`,
    whose default depends on `--serve`."""
    if port:
        default = f"{port}; 0 picks any free port"
    else:
        default = "0, any free port, which the command is told"
        default += f"; {DEFAULT_PORT} with --serve" if port is None else ""
    parser.add_argument(
        "--port", type=int, default=port, help=f"port to listen on (default {default})"
    )
    parser.add_argument(
        "--listen-host",
        default=LISTEN_HOST,
        metavar="HOST",
        help=f"address to listen on (default {LISTEN_HOST}). Anything but loopback lets "
        "every machine that reaches the port send traffic through irimi and call its control "
        "endpoint; the reverse door stays loopback-only",
    )
    parser.add_argument(
        "--allow-host",
        action="append",
        default=[],
        type=_host_arg,
        metavar="HOST",
        help=f"also let the reverse door http://{LISTEN_HOST}:<port>/<host>/<path> relay "
        "to HOST, a bare host name (repeatable; every host in a loaded map is already allowed)",
    )
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        type=_target_arg,
        metavar="SPEC",
        dest="target",
        help="answer a route from an address you control instead of faking it: "
        "'<host>[<path>]=<url>', e.g. 'api.stripe.com/v1/refunds=http://127.0.0.1:3000/refund'. "
        "A bare host sets the whole service's target (repeatable; beats the overrides file)",
    )
    parser.add_argument(
        "--target-reads",
        action="append",
        default=[],
        type=_host_arg,
        metavar="HOST",
        help="also send this service's reads to its target, making it a delegated service - "
        "its reads are then answered by the target, not by the real service (repeatable)",
    )
    parser.add_argument(
        "--allow-target-host",
        action="append",
        default=[],
        type=_host_arg,
        metavar="HOST",
        help="let an answer target name HOST instead of loopback. This sends the agent's "
        "requests off this machine; targets are loopback-only without it (repeatable)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="print only the banner and the summary, not a line per exchange and tool call",
    )
    add_store_option(parser, "where to record runs, redacted")


def add_store_option(
    parser: argparse.ArgumentParser, what: str = "the trace store to read"
) -> None:
    parser.add_argument(
        "--store",
        type=Path,
        default=None,
        metavar="DIR",
        help=f"{what} (default: $IRIMI_HOME/store)",
    )


def call_arg(value: str) -> str:
    """argparse type for --call: a function's `module:qualname`, as a trigger records it."""
    module, sep, qualname = value.partition(":")
    if not sep or not module.strip() or not qualname.strip():
        raise argparse.ArgumentTypeError(f"{value!r}: give the function as module:qualname")
    return value


def args_arg(value: str) -> dict[str, JSONValue]:
    """argparse type for --args: a JSON object, `{parameter: captured value}`, the shape a trigger
    records its args in."""
    try:
        parsed = json.loads(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--args is not JSON: {exc}") from None
    except RecursionError:
        # argparse would let this through as a traceback where a usage error belongs.
        raise argparse.ArgumentTypeError("--args is nested deeper than it can be read") from None
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("--args is a JSON object of {parameter: value}")
    return parsed


def args_file_arg(value: str) -> dict[str, JSONValue]:
    """argparse type for --args-file: `--args`, read from the file `value` names."""
    try:
        text = Path(value).read_text(encoding="utf-8")
    except OSError as exc:
        raise argparse.ArgumentTypeError(os_error(f"--args-file {value}", exc)) from None
    return args_arg(text)


def positive_int(value: str) -> int:
    """argparse type for --limit: a whole number of runs, at least one."""
    try:
        number = int(value)
    except ValueError:
        number = 0
    if number < 1:
        raise argparse.ArgumentTypeError(f"{value!r}: give a whole number of runs, at least 1")
    return number
