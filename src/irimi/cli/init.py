"""`irimi init`: generate the local CA."""

import argparse
import sys
from typing import Any

from irimi import ca
from irimi.cli import os_error

# A non-HTTP write the agent labels with `@sdk.write_tool` runs only its fake under irimi, so the
# notice says the rest happen for real, not that all of them do.
NON_HTTP_NOTICE = (
    "irimi shadows HTTP(S) only: database writes, files, gRPC and WebSockets happen for real, "
    "unless the agent labels the function that makes them with @sdk.write_tool"
)
NEXT_STEP = "next: irimi shadow -- python3 your_agent.py"


def register(subparsers: "argparse._SubParsersAction[Any]") -> None:
    init = subparsers.add_parser(
        "init", allow_abbrev=False, help="generate the local CA under $IRIMI_HOME/ca"
    )
    init.add_argument(
        "--force", action="store_true", help="regenerate the CA even if one already exists"
    )
    init.set_defaults(func=cmd_init)


def cmd_init(args: argparse.Namespace) -> int:
    p = ca.ca_paths()
    match ca.state(p):
        case "present" if not args.force:
            print(f"CA already exists at {p.key.parent}; use --force to regenerate")
            return 0
        case "partial" if not args.force:
            print(
                f"error: CA directory {p.key.parent} is incomplete "
                f"(expected both {p.key.name} and {p.cert.name}); run `irimi init --force`",
                file=sys.stderr,
            )
            return 1
    try:
        ca.generate_ca(p)
    except OSError as exc:
        why = os_error(f"could not write the CA to {p.key.parent}", exc)
        print(f"error: {why}", file=sys.stderr)
        return 1
    print("Generated CA:")
    print(f"  key:  {p.key}")
    print(f"  cert: {p.cert}")
    print(NON_HTTP_NOTICE)
    print(NEXT_STEP)
    return 0
