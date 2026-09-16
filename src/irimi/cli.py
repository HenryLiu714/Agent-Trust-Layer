import argparse
import sys

from irimi import __version__

NON_HTTP_NOTICE = (
    "Note: irimi only sees HTTP(S). Side effects that are not HTTP "
    "(database writes, files, gRPC, WebSockets) are not virtualized and happen for real."
)


def cmd_init(args: argparse.Namespace) -> int:
    from irimi import ca  # deferred: keeps --version and --help free of the cryptography import

    p = ca.ca_paths()
    if ca.ca_exists(p) and not args.force:
        print(f"CA already exists at {p.key.parent} (use --force to regenerate).")
        print(NON_HTTP_NOTICE)
        return 0
    if ca.ca_partial(p) and not args.force:
        print(
            f"error: CA directory {p.key.parent} is incomplete "
            f"(expected both {p.key.name} and {p.cert.name}). Run `irimi init --force`.",
            file=sys.stderr,
        )
        return 1
    try:
        ca.generate_ca(p)
    except OSError as exc:
        print(f"error: could not write CA to {p.key.parent}: {exc}", file=sys.stderr)
        return 1
    print("Generated CA:")
    print(f"  key:  {p.key}")
    print(f"  cert: {p.cert}")
    print(NON_HTTP_NOTICE)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="irimi",
        description="Shadow mode for AI agents: reads are real, writes are virtual.",
    )
    parser.add_argument("--version", action="version", version=f"irimi {__version__}")
    subparsers = parser.add_subparsers(dest="command", metavar="<command>")

    init = subparsers.add_parser("init", help="generate the local CA under $IRIMI_HOME/ca")
    init.add_argument(
        "--force", action="store_true", help="regenerate the CA even if one already exists"
    )
    init.set_defaults(func=cmd_init)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
