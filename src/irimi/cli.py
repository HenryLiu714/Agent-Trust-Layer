import argparse
import sys
from typing import TYPE_CHECKING

from irimi import __version__, paths

if TYPE_CHECKING:  # the quoted annotation on _wait_for_child; no runtime import
    import subprocess

SIGINT_EXIT_CODE = 130  # 128 + SIGINT, the shell convention for a Ctrl-C'd command

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

    from irimi import ca, runner
    from irimi.engine import EngineConfig, EngineStartError
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
        print(runner.exchange_line(ex), flush=True)

    engine = MitmEngine(cfg, ShadowPolicy(), NullStore(), NoOverlay(), on_exchange=on_exchange)

    async def _main() -> None:
        task = asyncio.ensure_future(engine.run())
        try:
            await engine.wait_ready()
        except EngineStartError:
            await asyncio.gather(task, return_exceptions=True)  # let mitmproxy finish stopping
            raise
        for line in runner.banner_lines(
            "serve", run_id, cfg.listen_host, engine.listen_port(), p.cert
        ):
            print(line)
        await task

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
    except (RuntimeError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def cmd_shadow(args: argparse.Namespace) -> int:
    import os
    import secrets
    import subprocess

    from irimi import ca, runner
    from irimi.engine import EngineConfig, EngineStartError
    from irimi.engine.mitm import MitmEngine
    from irimi.exchange import Exchange
    from irimi.overlay import NoOverlay
    from irimi.policy import ShadowPolicy
    from irimi.store import NullStore

    cmd = list(args.cmd)
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        print("error: nothing to run. Usage: irimi shadow -- <command> [args...]", file=sys.stderr)
        return 1

    p = ca.ca_paths()
    if not ca.ca_exists(p):
        print(f"error: no CA at {p.key.parent}. Run `irimi init` first.", file=sys.stderr)
        return 1

    run_id = secrets.token_hex(2)
    exchanges: list[Exchange] = []

    def on_exchange(ex: Exchange) -> None:
        exchanges.append(ex)
        print(runner.exchange_line(ex), flush=True)

    engine = MitmEngine(
        EngineConfig(
            run_id=run_id,
            ca=p,
            confdir=paths.mitm_dir(),
            listen_host=paths.LISTEN_HOST,
            listen_port=args.port,
        ),
        ShadowPolicy(),
        NullStore(),
        NoOverlay(),
        on_exchange=on_exchange,
    )

    try:
        handle = runner.start_engine(engine)
    except (EngineStartError, TimeoutError) as exc:
        # A timeout from the ready future carries no message of its own.
        reason = str(exc) or f"proxy did not start within {runner.READY_TIMEOUT_S:.0f}s"
        print(f"error: {reason}", file=sys.stderr)
        return 1  # fail closed: the child is never spawned without the proxy
    except KeyboardInterrupt:
        print("error: interrupted before the proxy was ready.", file=sys.stderr)
        return SIGINT_EXIT_CODE

    try:
        for line in runner.banner_lines("shadow", run_id, paths.LISTEN_HOST, handle.port(), p.cert):
            print(line, flush=True)
        env = runner.child_env(dict(os.environ), paths.LISTEN_HOST, handle.port(), p.cert, run_id)
        try:
            proc = subprocess.Popen(cmd, env=env)
        except FileNotFoundError:
            print(f"error: command not found: {cmd[0]}", file=sys.stderr)
            return 127
        except OSError as exc:
            print(f"error: could not run {cmd[0]}: {exc}", file=sys.stderr)
            return 126
        code = runner.exit_code_for(_wait_for_child(proc))
    except KeyboardInterrupt:  # Ctrl-C outside _wait_for_child's own handling
        code = SIGINT_EXIT_CODE
    finally:
        # An impatient second Ctrl-C lands here, while the engine thread is being joined.
        try:
            handle.stop()
        except KeyboardInterrupt:
            pass

    for line in runner.summary_lines(run_id, exchanges):
        print(line, flush=True)
    return code


def _wait_for_child(proc: "subprocess.Popen[bytes]") -> int:
    """Wait for the child, surviving Ctrl-C. Returns Popen's returncode (negative if signalled).

    Ctrl-C in a terminal reaches the whole foreground process group, so the child already got its
    own SIGINT; give it time to exit, then escalate.
    """
    import signal

    from irimi import runner

    try:
        return proc.wait()
    except KeyboardInterrupt:
        pass
    try:
        return proc.wait(timeout=runner.CHILD_GRACE_S)
    except KeyboardInterrupt:
        pass
    except Exception:
        pass
    proc.terminate()
    try:
        return proc.wait(timeout=runner.CHILD_GRACE_S)
    except Exception:
        pass
    proc.kill()
    try:
        return proc.wait(timeout=runner.CHILD_GRACE_S)
    except Exception:
        # SIGKILL did not reap it (uninterruptible I/O). Don't block the CLI forever.
        return -signal.SIGKILL


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
    serve.add_argument(
        "--port",
        type=int,
        default=paths.DEFAULT_PORT,
        help=f"port on {paths.LISTEN_HOST} (default {paths.DEFAULT_PORT})",
    )
    serve.set_defaults(func=cmd_serve)

    shadow = subparsers.add_parser(
        "shadow", help="run a command with its HTTP(S) traffic in shadow mode"
    )
    shadow.add_argument(
        "--port",
        type=int,
        default=paths.DEFAULT_PORT,
        help=f"port on {paths.LISTEN_HOST} (default {paths.DEFAULT_PORT})",
    )
    shadow.add_argument(
        "cmd",
        nargs=argparse.REMAINDER,
        metavar="-- <command> [args...]",
        help="the command to run under the proxy",
    )
    shadow.set_defaults(func=cmd_shadow)
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
