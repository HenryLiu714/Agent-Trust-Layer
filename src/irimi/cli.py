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


def cmd_serve(args: argparse.Namespace) -> int:
    import asyncio
    import secrets

    from irimi import ca, paths
    from irimi.engine import EngineConfig
    from irimi.engine.mitm import MitmEngine
    from irimi.exchange import Exchange
    from irimi.overlay import NoOverlay
    from irimi.policy import ShadowPolicy
    from irimi.store import NullStore

    p = ca.ca_paths()
    if not ca.ca_exists(p):
        print(f"error: no CA at {p.key.parent}. Run `irimi init` first.", file=sys.stderr)
        return 1
    run_id = secrets.token_hex(2)
    cfg = EngineConfig(
        run_id=run_id,
        ca=p,
        confdir=paths.mitm_dir(),
        listen_host=paths.LISTEN_HOST,
        listen_port=args.port,
    )

    def on_exchange(ex: Exchange) -> None:
        status = ex.response.status if ex.response else "-"
        flags = f"  [{', '.join(ex.flags)}]" if ex.flags else ""
        print(
            f"{ex.answered_by:<8} {ex.kind:<8} {ex.request.method} "
            f"{ex.request.host}{ex.request.path} -> {status}{flags}",
            flush=True,
        )

    engine = MitmEngine(cfg, ShadowPolicy(), NullStore(), NoOverlay(), on_exchange=on_exchange)

    async def _main() -> None:
        task = asyncio.ensure_future(engine.run())
        ready = asyncio.ensure_future(engine.wait_ready())
        await asyncio.wait({task, ready}, return_when=asyncio.FIRST_COMPLETED)
        if task.done():
            ready.cancel()
            task.result()  # re-raise if run() raised
            raise RuntimeError(
                f"proxy did not start on {cfg.listen_host}:{cfg.listen_port} (port in use?)"
            )
        print(
            f"irimi serve · run {run_id} · listening on "
            f"{cfg.listen_host}:{engine.listen_port()} · ca {p.cert}"
        )
        print("hosts not routed through the proxy are NOT virtualized. backstop: none (Phase 4)")
        await task

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
    except (RuntimeError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except SystemExit:  # mitmproxy's ErrorCheck calls sys.exit(1) when startup logged an error
        print(
            f"error: proxy did not start on {cfg.listen_host}:{cfg.listen_port} (port in use?)",
            file=sys.stderr,
        )
        return 1
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

    serve = subparsers.add_parser("serve", help="run the shadow proxy in the foreground (dev)")
    serve.add_argument("--port", type=int, default=4000, help="port on 127.0.0.1 (default 4000)")
    serve.set_defaults(func=cmd_serve)
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
