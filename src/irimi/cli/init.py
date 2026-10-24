"""`irimi init`: generate the local CA."""

import argparse
import sys
from typing import Any

from irimi.cli import os_error
from irimi.cli.options import epilog, help_formatted
from irimi.config import ca
from irimi.core import exitcodes

# A `@sdk.write_tool` runs only its fake under irimi, so only unlabelled non-HTTP writes are real.
NON_HTTP_NOTICE = (
    "irimi shadows HTTP(S) only: database writes, files, gRPC and WebSockets happen for real, "
    "unless the agent labels the function that makes them with @sdk.write_tool"
)
NEXT_STEP = "next: irimi shadow -- python3 your_agent.py"


def register(subparsers: "argparse._SubParsersAction[Any]") -> None:
    init = subparsers.add_parser(
        "init",
        **help_formatted(
            help="create the local CA in ~/.irimi (or $IRIMI_HOME)",
            description="Create the local certificate authority irimi signs HTTPS traffic with, "
            "in ~/.irimi/ca\n(or $IRIMI_HOME/ca). Only the processes irimi starts are told to "
            "trust it.",
            epilog=epilog(["irimi init", "irimi init --force"]),
        ),
    )
    init.add_argument(
        "--force", action="store_true", help="make a new CA even if one already exists"
    )
    init.set_defaults(func=cmd_init)


def cmd_init(args: argparse.Namespace) -> int:
    p = ca.ca_paths()
    match ca.state(p):
        case "present" if not args.force:
            print(
                f"hint: CA already exists at {p.key.parent}; use --force to regenerate",
                file=sys.stderr,
            )
            return exitcodes.OK
        case "partial" if not args.force:
            print(
                f"error: CA directory {p.key.parent} is incomplete "
                f"(expected both {p.key.name} and {p.cert.name}); run `irimi init --force`",
                file=sys.stderr,
            )
            return exitcodes.ERROR
    try:
        ca.generate_ca(p)
    except OSError as exc:
        why = os_error(f"could not write the CA to {p.key.parent}", exc)
        print(f"error: {why}", file=sys.stderr)
        return exitcodes.ERROR
    print("Generated CA:")
    print(f"  key:  {p.key}")
    print(f"  cert: {p.cert}")
    print(NON_HTTP_NOTICE)
    print(NEXT_STEP)
    return exitcodes.OK
