import argparse
import sys
from typing import TYPE_CHECKING

from irimi import __version__, paths

if TYPE_CHECKING:  # the quoted annotations below; no runtime import
    import subprocess
    from pathlib import Path

    from irimi.ca import CAPaths
    from irimi.engine import EngineConfig
    from irimi.servicemap import MapIndex

SIGINT_EXIT_CODE = 130  # 128 + SIGINT, the shell convention for a Ctrl-C'd command

NON_HTTP_NOTICE = (
    "Note: irimi only sees HTTP(S). Side effects that are not HTTP "
    "(database writes, files, gRPC, WebSockets) are not virtualized and happen for real."
)


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
    return host, f"{slash}{path}" if slash else "", url.strip()


def _add_engine_args(parser: argparse.ArgumentParser) -> None:
    """The options `serve` and `shadow` share."""
    parser.add_argument(
        "--port",
        type=int,
        default=paths.DEFAULT_PORT,
        help=f"port on {paths.LISTEN_HOST} (default {paths.DEFAULT_PORT})",
    )
    parser.add_argument(
        "--allow-host",
        action="append",
        default=[],
        type=_host_arg,
        metavar="HOST",
        help=f"also let the reverse door http://{paths.LISTEN_HOST}:<port>/<host>/<path> relay "
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


def _reverse_hosts(args: argparse.Namespace, index: "MapIndex") -> frozenset[str]:
    """Every host in a loaded service map, plus every --allow-host, lower-cased and stripped."""
    extra = frozenset(h.strip().lower() for h in args.allow_host if h.strip())
    return index.hosts | extra


def _engine_config(
    args: argparse.Namespace, index: "MapIndex", run_id: str, ca_paths: "CAPaths"
) -> "EngineConfig":
    """The EngineConfig `serve` and `shadow` share.

    `maps=index` is what makes the classifier use the maps: without it the engine falls back to the
    verb rule and every mapped write on a POST-only service reads as `unknown`.
    """
    from irimi.engine import EngineConfig

    return EngineConfig(
        run_id=run_id,
        ca=ca_paths,
        confdir=paths.mitm_dir(),
        listen_host=paths.LISTEN_HOST,
        listen_port=args.port,
        reverse_hosts=_reverse_hosts(args, index),
        maps=index,
    )


def _load_maps(args: argparse.Namespace | None = None) -> "MapIndex | None":
    """The loaded service maps, or None after printing why they were refused.

    Fail closed: a map, overrides file or `--target` the loader rejects must not start a proxy
    that would then classify and answer with half a policy. `args` is None for `maps list`, which
    has no engine flags of its own.
    """
    from irimi import pipeline, servicemap

    try:
        index = servicemap.load(
            allow_target_hosts=frozenset(getattr(args, "allow_target_host", ()) or ()),
            targets=getattr(args, "target", ()) or (),
            target_reads=getattr(args, "target_reads", ()) or (),
        )
        # The loader keeps targets on loopback, where the only thing separating a stub from our
        # own listener is the port - and dialling ourselves is a loop, not a delegation. The
        # addon repeats this per request against the port it actually bound, which is the
        # authoritative check; this one is here so `--port 4000` with a target on 4000 fails
        # before anything starts rather than on the first request.
        port = getattr(args, "port", 0) or 0
        if port:
            for sm in index.services:
                for target in (sm.target, *(r.target for r in sm.routes)):
                    if target != servicemap.SELF_TARGET:
                        pipeline.refuse_self_target(target, port)
        return index
    except (servicemap.MapError, pipeline.TargetRefused) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None


def _escape_hatch_warning(args: argparse.Namespace) -> str | None:
    """The one line `--allow-target-host` has to print, or None when it was not used.

    Targets are loopback-only so that a delegated request cannot leave the machine; this flag is
    the deliberate way out of that, so it says so out loud. The delegated-service banner and the
    summary's own accounting are issue #20, not this one.
    """
    hosts = getattr(args, "allow_target_host", None)
    if not hosts:
        return None
    return (
        f"WARNING: --allow-target-host {', '.join(sorted(hosts))} - an answer target may now "
        "leave this machine. Requests to a targeted route go to a host you named, which is "
        "neither the real service nor loopback."
    )


def print_startup(
    command: str,
    args: argparse.Namespace,
    index: "MapIndex",
    run_id: str,
    host: str,
    port: int,
    ca_cert: "Path",
) -> None:
    """Everything a run says before it starts: the escape-hatch warning, the banner, and one line
    per delegated service. `command` is "serve" or "shadow".

    Every line is flushed. Python block-buffers stdout when it is a pipe rather than a terminal,
    and `serve` is a foreground process that may then print nothing for minutes, so
    `irimi serve > log`, `irimi serve | tee` and any supervisor capturing the process showed none
    of this until an exchange came along and flushed it - and a `serve` nobody talks to showed
    nothing at all. What was withheld is the port it bound, the CA path a client needs,
    `backstop: none`, and the red `NOT loopback` line saying the agent's requests are leaving
    this machine. `shadow` flushed already; `serve` did not, and the two are one function now so
    they cannot drift again.
    """
    from irimi import runner  # deferred like every other heavy import in this module

    warning = _escape_hatch_warning(args)
    if warning is not None:
        print(warning, file=sys.stderr, flush=True)
    for line in runner.banner_lines(command, run_id, host, port, ca_cert):
        print(line, flush=True)
    for line in runner.delegated_lines(index, color=sys.stdout.isatty()):
        print(line, flush=True)


def cmd_maps_list(args: argparse.Namespace) -> int:
    from irimi import servicemap

    index = _load_maps()
    if index is None:
        return 1
    # Exact hosts first, then the wildcard patterns. A pattern is printed because a map whose
    # hosts are all wildcards would otherwise not appear here at all; it is still not a
    # reverse-door allow-list entry (see MapIndex.hosts).
    hosts = sorted(index.hosts)
    patterns = index.patterns
    rows = [(host, index.by_host[host]) for host in hosts]
    rows += [(pattern, index.by_suffix[pattern[1:]]) for pattern in patterns]
    width = max((len(name) for name, _ in rows), default=0)
    service_width = max((len(sm.service) for _, sm in rows), default=0)
    routes = sum(len(sm.routes) for sm in index.services)
    # Hosts and patterns are counted separately: only the exact hosts are the reverse door's
    # allow-list, and one number covering both told an operator a pattern host was reachable
    # through the door when it 403s (#29).
    print(
        f"irimi maps · {len(index.services)} service(s) · {len(hosts)} host(s) · "
        f"{len(patterns)} pattern(s) · {routes} route(s)"
    )
    for name, sm in rows:
        reads = " + reads" if sm.target_reads else ""
        print(
            f"  {name:<{width}}  {sm.service:<{service_width}}  {len(sm.routes):>3} routes  "
            f"target: {sm.target}{reads}"
        )
        # The service target alone printed `target: self` for a service whose *routes* carry
        # targets, so a partly delegated service looked untouched in the listing and contradicted
        # the banner printed two seconds later (#20). Route targets get a line of their own under
        # the host, because the column above is the service's answer and this is the route's.
        for route in sm.routes:
            if route.target != servicemap.SELF_TARGET:
                print(f"  {'':<{width}}  route {route.method} {route.path} → {route.target}")
    override = servicemap.override_path()
    if override is not None:
        print(f"  overrides from {override}")
    return 0


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
    from irimi.engine import EngineStartError
    from irimi.engine.mitm import MitmEngine
    from irimi.exchange import Exchange
    from irimi.overlay import NoOverlay
    from irimi.policy import ShadowPolicy
    from irimi.store import NullStore

    index = _load_maps(args)
    if index is None:
        return 1
    p = ca.ca_paths()
    if not ca.ca_exists(p):
        print(f"error: no CA at {p.key.parent}. Run `irimi init` first.", file=sys.stderr)
        return 1
    run_id = secrets.token_hex(2)
    cfg = _engine_config(args, index, run_id, p)

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
        port = engine.listen_port()
        assert port is not None  # wait_ready() returned, so the listener is bound
        print_startup("serve", args, index, run_id, cfg.listen_host, port, p.cert)
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
    import time

    from irimi import ca, runner
    from irimi.engine import EngineStartError
    from irimi.engine.mitm import MitmEngine
    from irimi.exchange import Exchange
    from irimi.overlay import NoOverlay
    from irimi.policy import ShadowPolicy
    from irimi.store import NullStore

    index = _load_maps(args)
    if index is None:
        return 1

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
    elapsed = 0.0

    def on_exchange(ex: Exchange) -> None:
        exchanges.append(ex)
        print(runner.exchange_line(ex), flush=True)

    engine = MitmEngine(
        _engine_config(args, index, run_id, p),
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
        print_startup("shadow", args, index, run_id, paths.LISTEN_HOST, handle.port(), p.cert)
        env = runner.child_env(dict(os.environ), paths.LISTEN_HOST, handle.port(), p.cert, run_id)
        try:
            proc = subprocess.Popen(cmd, env=env)
        except FileNotFoundError:
            print(f"error: command not found: {cmd[0]}", file=sys.stderr)
            return 127
        except OSError as exc:
            print(f"error: could not run {cmd[0]}: {exc}", file=sys.stderr)
            return 126
        # The summary's duration is the child's, not the proxy's: what the reader wants to know is
        # how long the agent ran. monotonic, so a clock change mid-run cannot make it negative.
        started = time.monotonic()
        code = runner.exit_code_for(_wait_for_child(proc))
        elapsed = time.monotonic() - started
    except KeyboardInterrupt:  # Ctrl-C outside _wait_for_child's own handling
        code = SIGINT_EXIT_CODE
    finally:
        # An impatient second Ctrl-C lands here, while the engine thread is being joined.
        try:
            handle.stop()
        except KeyboardInterrupt:
            pass

    for line in runner.summary_lines(run_id, exchanges, elapsed, index):
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
    _add_engine_args(serve)
    serve.set_defaults(func=cmd_serve)

    shadow = subparsers.add_parser(
        "shadow", help="run a command with its HTTP(S) traffic in shadow mode"
    )
    _add_engine_args(shadow)
    shadow.add_argument(
        "cmd",
        nargs=argparse.REMAINDER,
        metavar="-- <command> [args...]",
        help="the command to run under the proxy",
    )
    shadow.set_defaults(func=cmd_shadow)

    maps = subparsers.add_parser("maps", help="show the loaded service maps")
    maps_sub = maps.add_subparsers(dest="maps_command", metavar="<subcommand>", required=True)
    maps_list = maps_sub.add_parser(
        "list", help="print each mapped host with its service, route count and answer target"
    )
    maps_list.set_defaults(func=cmd_maps_list)
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
