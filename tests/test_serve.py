"""Serve mode: `irimi shadow --serve`, a sidecar with no child process.

The engine half first: a listener bound wider than loopback is this machine at every address it
was reached at, so a request naming that address, or a name that resolves to it, is never
forwarded to irimi itself; and the reverse door stays loopback-only whatever the bind. Then the
store's end-of-run callback, and then the CLI, driven as a subprocess where a signal is the point.
"""

import asyncio
import contextlib
import dataclasses
import json
import socket

import pytest

from irimi import ca, paths, servicemap
from irimi.engine import EngineConfig, Serve
from irimi.engine.mitm import IrimiAddon, MitmEngine
from irimi.overlay import NoOverlay
from irimi.policy import ShadowPolicy
from irimi.protocol.env import CONTROL_ENV, ENGINE_ACTIVE_ENV
from tests.doubles import NullStore


def _config(tmp_path, monkeypatch, listen_host="127.0.0.1"):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    p = ca.ca_paths()
    ca.generate_ca(p)
    return EngineConfig(
        run_id="s3rv",
        ca=p,
        confdir=paths.mitm_dir(),
        listen_host=listen_host,
        listen_port=0,
        reverse_hosts=frozenset({"api.stripe.com"}),
        maps=servicemap.MapIndex(),
    )


def _own_address() -> str | None:
    """This machine's non-loopback IPv4 address: the one a connected UDP socket would send from.
    Connecting a UDP socket sends nothing; it only asks the routing table. None with no route."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            sock.connect(("192.0.2.1", 9))  # TEST-NET-1: routed like any address, never answered
        except OSError:
            return None
        address = sock.getsockname()[0]
    return None if address.startswith("127.") or address == "0.0.0.0" else address


# -- the reverse door stays loopback-only ---------------------------------------------------------


def test_the_reverse_door_refuses_a_peer_that_is_not_loopback(tmp_path, monkeypatch):
    """A wider bind lets other machines reach the listener, but never through the reverse door,
    which relays plain HTTP with the agent's credentials to the real service: the peer check in
    `ingress.admit` is unchanged. Driven through the addon, because a
    connection from another machine cannot be staged here."""
    from mitmproxy.test import tflow, tutils

    seen = []
    addon = IrimiAddon(
        _config(tmp_path, monkeypatch, listen_host="0.0.0.0"),
        ShadowPolicy(),
        NullStore(),
        NoOverlay(),
        seen.append,
        lambda port, error: None,
    )
    flow = tflow.tflow(
        req=tutils.treq(method=b"GET", host="10.0.0.5", port=4000, path=b"/api.stripe.com/v1/x")
    )
    flow.client_conn.sockname = ("10.0.0.5", 4000)
    flow.client_conn.peername = ("10.0.0.9", 51515)
    asyncio.run(addon.request(flow))

    assert flow.response.status_code == 403
    assert flow.response.headers["irimi-answered-by"] == "irimi"
    assert (
        "base-URL requests come from loopback only; refusing '10.0.0.9'"
        in json.loads(flow.response.content)["error"]["message"]
    )
    assert (flow.request.host, flow.request.path) == ("10.0.0.5", "/api.stripe.com/v1/x")
    (ex,) = seen
    assert (ex.answered_by, ex.door, ex.kind, ex.flags) == (
        "irimi",
        "reverse",
        "read",
        ("refused",),
    )


# -- the self-loop guard, widened ------------------------------------------------------------------


def _counting_requests(monkeypatch) -> list[tuple[str, int]]:
    """Every request the addon's hook is called with, as the (address, port) its connection
    arrived on. A request irimi forwarded to itself arrives a second time."""
    calls: list[tuple[str, int]] = []
    hook = IrimiAddon.request

    async def counted(self, flow):
        calls.append(tuple(flow.client_conn.sockname[:2]))
        await hook(self, flow)

    monkeypatch.setattr(IrimiAddon, "request", counted)
    return calls


def _absolute_get(address: str, port: int, url: str) -> tuple[int, bytes]:
    """A forward-proxy request for `url`, sent to irimi at `address:port`."""
    import http.client

    conn = http.client.HTTPConnection(address, port, timeout=10)
    try:
        conn.request("GET", url, headers={"host": url.split("/")[2]})
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


@pytest.mark.parametrize("named", [False, True], ids=["address", "name"])
def test_a_request_naming_the_address_irimi_was_reached_at_is_never_forwarded_to_it(
    tmp_path, monkeypatch, named
):
    """Bound to 0.0.0.0, irimi is this machine at its non-loopback address too, which is not a
    loopback literal and does not resolve to one. A forward-proxy request for that address on
    irimi's port - or for a name that resolves to it, as `irimi` does in compose - is irimi's own
    listener: answered by irimi, here the reverse door's refusal of a peer that is not loopback,
    and never forwarded, which would open a connection to the listener and arrive a second time
    in a loop. The name is resolved by a `getaddrinfo` patch, in the process that runs irimi."""
    import os

    address = _own_address()
    if address is None:
        # The one proof of the non-loopback case: CI must run it, never skip it.
        assert not os.environ.get("CI"), "a CI runner with no route has no address to test"
        pytest.skip("this machine has no route, so no non-loopback address")
    if named:
        real = socket.getaddrinfo

        def resolving(host, *args, **kwargs):
            return real(address if host == "irimi.test" else host, *args, **kwargs)

        monkeypatch.setattr(socket, "getaddrinfo", resolving)
    calls = _counting_requests(monkeypatch)
    cfg = _config(tmp_path, monkeypatch, listen_host="0.0.0.0")
    engine = MitmEngine(cfg, ShadowPolicy(), NullStore(), NoOverlay())
    from irimi import runner

    handle = runner.start_engine(engine)
    try:
        port = handle.port()
        host = "irimi.test" if named else address
        status, body = _absolute_get(address, port, f"http://{host}:{port}/api.stripe.com/v1/x")
    finally:
        handle.stop()
    assert status == 403
    assert f"base-URL requests come from loopback only; refusing '{address}'".encode() in body
    assert calls == [(address, port)]


TARGETED_MAP = """
version: 1
service: demo
hosts:
  - api.demo.test
routes:
  - match:
      method: POST
      path: /things
    operation: things.create
    kind: write
    human: create a thing
"""


def test_an_answer_target_naming_the_address_irimi_was_reached_at_is_never_dialled(
    tmp_path, monkeypatch
):
    """An answer target is irimi's own listener at the address a connection reached it at, as a
    request is: with `--listen-host 0.0.0.0` and `--allow-target-host` naming this machine's
    own address, a write the target would answer is refused by irimi as its own listener, with
    `irimi_target_failed`, and no second connection to the listener is opened. Before, the target
    was dialled and the reverse door refused the looped request, as from a peer not on loopback."""
    import json
    import os

    address = _own_address()
    if address is None:
        assert not os.environ.get("CI"), "a CI runner with no route has no address to test"
        pytest.skip("this machine has no route, so no non-loopback address")
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir()
    (maps_dir / "demo.yaml").write_text(TARGETED_MAP)
    # The target has to name the port the engine bound, so the engine binds port 0 and the target
    # is loaded once the port is known, into the index the engine and the policy were built with,
    # which both read per request. A port reserved by binding a socket and closing it could be
    # taken by another test before the engine bound it.
    maps = servicemap.MapIndex()
    cfg = dataclasses.replace(_config(tmp_path, monkeypatch, listen_host="0.0.0.0"), maps=maps)
    calls = _counting_requests(monkeypatch)
    engine = MitmEngine(cfg, ShadowPolicy(maps=maps), NullStore(), NoOverlay())
    from irimi import runner

    handle = runner.start_engine(engine)
    try:
        port = handle.port()
        loaded = servicemap.load(
            allow_target_hosts=frozenset({address}),
            cwd=tmp_path,
            maps_dir=maps_dir,
            targets=[("api.demo.test", "/things", f"http://{address}:{port}/w")],
        )
        vars(maps).update(vars(loaded))
        import http.client

        conn = http.client.HTTPConnection(address, port, timeout=10)
        try:
            conn.request("POST", "http://api.demo.test/things", body=b"{}")
            resp = conn.getresponse()
            status, body = resp.status, resp.read()
        finally:
            conn.close()
    finally:
        handle.stop()
    assert status == 502
    error = json.loads(body)["error"]
    assert error["type"] == "irimi_target_failed" and "own listener" in error["message"]
    assert calls == [(address, port)]


def test_a_door_that_cannot_be_told_is_refused_never_forwarded(tmp_path, monkeypatch):
    """Telling the doors apart parses the address a connection arrived on and may resolve a
    name. If that raises, the hook would forward the flow, and the request it was asking
    about may be one addressed to irimi itself: it is answered 400 by irimi instead."""
    from mitmproxy.test import tflow, tutils

    from irimi import ingress

    def broken(*args):
        raise ValueError("no address")

    monkeypatch.setattr(ingress, "detect_door", broken)
    seen = []
    addon = IrimiAddon(
        _config(tmp_path, monkeypatch, listen_host="0.0.0.0"),
        ShadowPolicy(),
        NullStore(),
        NoOverlay(),
        seen.append,
        lambda port, error: None,
    )
    flow = tflow.tflow(req=tutils.treq(method=b"POST", host="irimi", port=4000, path=b"/x"))
    flow.client_conn.sockname = ("10.0.0.5", 4000)
    asyncio.run(addon.request(flow))
    assert flow.response.status_code == 400
    assert json.loads(flow.response.content) == {
        "error": {
            "type": "irimi_refused",
            "message": "irimi refused this request: could not route this request",
        }
    }
    (ex,) = seen
    assert (ex.answered_by, ex.flags) == ("irimi", ("unclassified", "refused"))


def test_a_failed_decision_under_serve_mode_is_unattributed(tmp_path, monkeypatch):
    """Serve mode's engine run is `trace.UNATTRIBUTED`, and the failed-decision path reads
    the same fallback as every other: irimi's own 502, filed under no run, not
    under a process run that does not exist."""
    from mitmproxy.test import tflow, tutils

    from irimi.trace import UNATTRIBUTED

    class _Broken(ShadowPolicy):
        def decide(self, *args, **kwargs):
            raise RuntimeError("the decision failed")

    class _Kept(NullStore):
        def __init__(self):
            self.recorded = []

        def record(self, exchange):
            self.recorded.append(exchange)

    store = _Kept()
    cfg = dataclasses.replace(_config(tmp_path, monkeypatch), run_id=UNATTRIBUTED, mode=Serve())
    addon = IrimiAddon(cfg, _Broken(), store, NoOverlay(), None, lambda port, error: None)
    flow = tflow.tflow(
        req=tutils.treq(method=b"POST", host="api.example.com", port=443, path=b"/things")
    )
    flow.client_conn.sockname = ("127.0.0.1", 4000)
    asyncio.run(addon.request(flow))
    assert flow.response.status_code == 502
    [ex] = store.recorded
    assert ex.run_id == UNATTRIBUTED
    assert "decision-failed" in ex.flags


def test_a_loopback_bind_still_finds_its_own_loopback_spellings(tmp_path, monkeypatch):
    """The default bind is unchanged: `127.1` on irimi's port is irimi, through the forward door
    as through the reverse one, and goes no further than the reverse door's allow-list."""
    calls = _counting_requests(monkeypatch)
    cfg = dataclasses.replace(_config(tmp_path, monkeypatch), reverse_hosts=frozenset())
    engine = MitmEngine(cfg, ShadowPolicy(), NullStore(), NoOverlay())
    from irimi import runner

    handle = runner.start_engine(engine)
    try:
        port = handle.port()
        status, body = _absolute_get("127.0.0.1", port, f"http://127.1:{port}/nope.example/x")
    finally:
        handle.stop()
    assert status == 403
    assert b"not in a loaded map" in body
    assert calls == [("127.0.0.1", port)]


# -- the store tells serve when a run's end is on disk ---------------------------------------------


def test_the_store_calls_back_on_its_writer_once_a_runs_end_is_on_disk(tmp_path):
    """`on_run_ended` is called with the run's id by the writer thread after it wrote the end, so
    a reader opened in the callback finds the run whole: every event queued before the end, and
    the end itself. A run that has not ended is not called back for."""
    import threading

    from irimi.store.reader import StoreReader
    from irimi.store.writer import DirectoryStore
    from tests.test_store import _exchange, _record

    root = tmp_path / "store"
    seen = []

    def ended(run_id):
        stored = StoreReader(root).load_run(run_id)
        seen.append(
            (run_id, threading.current_thread().name, stored.record.end.outcome, stored.events)
        )

    store = DirectoryStore(root, b"k" * 32, ended)
    store.start_run(_record("r1"))
    store.record(_exchange("r1"))
    store.record(_exchange("r2"))
    store.end_run("r1", 20.0, "ok")
    store.close()
    [(run_id, thread, outcome, events)] = seen
    assert (run_id, thread, outcome) == ("r1", "irimi-store", "ok")
    assert [e.run_id for e in events] == ["r1"]


def test_a_run_s_writes_and_keys_are_dropped_once_its_end_is_on_disk(tmp_path, monkeypatch):
    """The composition root drops a run's writes from the engine's write log, and its idempotency
    keys from the policy's store, as the store reports the run ended, before serve prints its
    summary, so a long serve holds open runs' alone. Another run's stay."""
    from irimi import cli, idempotency
    from tests.test_idempotency import PARAMS, _stored
    from tests.test_store import _exchange, _record

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)
    ca.generate_ca(ca.ca_paths())
    args = cli.build_parser().parse_args(["serve", "--store", str(tmp_path / "store")])
    left_at_end = []
    slot_of = {run_id: idempotency.slot_id(run_id, "stripe", (), "k-1") for run_id in ("r1", "r2")}

    def on_run_ended(index, run_id):
        assert run.keys is not None
        left_at_end.append((run.write_log.for_run(run_id), run.keys.get(slot_of[run_id], PARAMS)))

    run = cli._prepare_run(args, serve=True, on_run_ended=on_run_ended)
    assert run is not None and run.write_log is not None and run.keys is not None
    ended, still_open = _exchange("r1"), _exchange("r2")
    run.write_log.add(ended)
    run.write_log.add(still_open)
    for slot in slot_of.values():
        run.keys.put(slot, PARAMS, _stored())
    try:
        run.store.start_run(_record("r1"))
        run.store.end_run("r1", 20.0, "ok")
    finally:
        run.store.close()
        run.lock.release()
    assert left_at_end == [((), (None, False))]
    assert run.write_log.for_run("r2") == (still_open,)
    assert run.keys.get(slot_of["r2"], PARAMS) == (_stored(), False)
    engine = cli._build_engine(run, lambda ex: None)
    assert engine.write_log is run.write_log
    assert engine.policy.idempotency is run.keys


def test_a_callback_that_raises_is_logged_and_the_store_carries_on(tmp_path, caplog):
    """What `on_run_ended` raises is the caller's problem, never the run's: one WARNING naming the
    exception's type, and the next event is still written."""
    from irimi.store.reader import StoreReader
    from irimi.store.writer import DirectoryStore
    from tests.test_store import _exchange, _record

    root = tmp_path / "store"

    def ended(run_id):
        raise ValueError("cannot print sk_live_secret")

    store = DirectoryStore(root, b"k" * 32, ended)
    store.start_run(_record("r1"))
    store.end_run("r1", 20.0, "ok")
    store.record(_exchange("r2"))
    store.close()
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings == ["irimi: the end-of-run callback raised ValueError"]
    assert StoreReader(root).load_run("r1").record.end.outcome == "ok"
    assert len(StoreReader(root).load_run("r2").events) == 1
    assert store.stats().dropped == 0


def test_one_serve_per_store_root(tmp_path):
    """A second holder of a root's `serve.lock` is refused while the first holds it, and the lock
    is free again once the first releases it, which it may do twice."""
    import os

    from irimi.store.lock import ServeLock, StoreLocked

    first = ServeLock.hold(tmp_path / "store")
    try:
        with pytest.raises(StoreLocked, match="another irimi serve is writing the trace store"):
            ServeLock.hold(tmp_path / "store")
        assert (os.stat(tmp_path / "store" / "serve.lock").st_mode & 0o777) == 0o600
    finally:
        first.release()
    first.release()
    ServeLock.hold(tmp_path / "store").release()


# -- the banner ------------------------------------------------------------------------------------


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "10.0.0.5", "irimi"])
def test_a_bind_that_is_not_loopback_adds_one_banner_line(host):
    from pathlib import Path

    from irimi import report

    lines = report.banner_lines("shadow --serve", None, host, 4000, Path("/ca.pem"))
    assert lines[0].startswith(f"irimi shadow --serve · {report.SERVE_RUNS} · listening on ")
    assert lines[2:] == [
        f"listening on {host}: any machine that can reach this port can send traffic through "
        "irimi and call its control endpoint"
    ]


@pytest.mark.parametrize("host", ["127.0.0.1", "127.0.0.9", "::1"])
def test_a_loopback_bind_adds_none(host):
    from pathlib import Path

    from irimi import report

    lines = report.banner_lines("shadow", "7f3a", host, 4000, Path("/ca.pem"))
    assert len(lines) == 2


def test_an_ipv6_listener_is_bracketed_where_it_is_a_url(tmp_path):
    """`::1:4000` reads as an address with one more group; the banner and the child's proxy and
    control variables say `[::1]:4000`."""
    from pathlib import Path

    from irimi import report, runner

    [first, *_] = report.banner_lines("shadow", "7f3a", "::1", 4000, Path("/ca.pem"))
    assert "listening on [::1]:4000 ·" in first
    env = runner.child_env({}, "::1", 4000, Path("/ca.pem"), "7f3a")
    assert env["HTTPS_PROXY"] == "http://[::1]:4000"
    assert env[CONTROL_ENV] == "http://[::1]:4000/_irimi"


# -- the command -----------------------------------------------------------------------------------


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    monkeypatch.chdir(tmp_path)
    ca.generate_ca(ca.ca_paths())
    return tmp_path


def test_serve_with_a_command_is_a_usage_error(home, monkeypatch, capsys):
    """Serve mode launches nothing, so `-- echo hi` with it is answered as argparse answers a bad
    invocation: usage on stderr, exit 2, and nothing started or stored."""
    from irimi import cli
    from irimi.cli import main

    monkeypatch.setattr(cli, "cmd_serve", lambda *args: pytest.fail("serve was started"))
    with pytest.raises(SystemExit) as exc:
        main(["shadow", "--serve", "--", "echo", "hi"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert err.startswith("usage: irimi shadow")
    assert "error: --serve runs no command" in err
    assert not (home / "store").exists()


def test_serve_with_a_command_exits_2_from_the_command_line(home):
    """As a shell runs it: `irimi shadow --serve -- echo hi` exits 2 with argparse's usage, and
    starts and stores nothing."""
    import os
    import subprocess
    import sys

    env = {**os.environ, paths.IRIMI_HOME_ENV: str(home)}
    done = subprocess.run(
        [sys.executable, "-m", "irimi.cli", "shadow", "--serve", "--", "echo", "hi"],
        cwd=home,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert done.returncode == 2
    assert done.stderr.startswith("usage: irimi shadow") and done.stdout == ""
    assert not (home / "store").exists()


def test_a_second_serve_on_one_store_is_refused_before_it_starts(home, capsys):
    """Two serves on one root would share `unattributed/events.jsonl`, repeating each other's
    `seq` and cutting each other's lines as a crash's. The second is refused with one line
    and exit 1, its store closed again, and the first serves on. Once the first stops, the
    root is free again. The second is given a `stop` already set, so a serve that is wrongly let
    through returns at once rather than serving for ever."""
    import threading

    from irimi import cli
    from irimi.store.lock import ServeLock

    def writers() -> int:
        return sum(thread.name == "irimi-store" for thread in threading.enumerate())

    stop = threading.Event()
    args = cli.build_parser().parse_args(["shadow", "--serve", "--port", "0"])
    codes: list[int] = []
    first = threading.Thread(target=lambda: codes.append(cli.cmd_shadow(args, stop)))
    first.start()
    try:
        _wait_for(lambda: "listening on" in capsys.readouterr().out)
        running = writers()
        second = cli.build_parser().parse_args(["serve", "--port", "0"])
        stopped = threading.Event()
        stopped.set()
        assert second.func(second, stopped) == 1
        err = capsys.readouterr().err.strip().splitlines()
        assert err == [f"error: another irimi serve is writing the trace store {home / 'store'}"]
        assert writers() == running
        assert first.is_alive()
    finally:
        stop.set()
        first.join(timeout=30)
    assert codes == [0]
    ServeLock.hold(home / "store").release()


@contextlib.contextmanager
def _signals_caught_here():
    """SIGINT and SIGTERM recorded by the test's own handlers for the length of the block, and
    the handlers before it put back after: a signal serve failed to catch is a list entry, not the
    death of the test process."""
    import signal

    outside: list[int] = []

    def record(signum, frame):
        outside.append(signum)

    saved = {signum: signal.signal(signum, record) for signum in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield outside, record
    finally:
        for signum, handler in saved.items():
            signal.signal(signum, handler)


def test_a_signal_during_serve_only_records_itself():
    """Serve's handlers take no lock: one that ran inside `Event.wait` on the main thread and set
    the event would wait on itself for ever. Each signal is recorded and nothing else; once
    one has arrived, serve is stopping and both are ignored on the way out, so a second during the
    interpreter's exit cannot end it. With none, the handlers found are put back."""
    import signal

    from irimi import cli

    with _signals_caught_here() as (outside, record):
        with cli._stopping_on_signals() as quiet:
            pass
        after_none = {
            signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
        }
        with cli._stopping_on_signals() as signalled:
            signal.raise_signal(signal.SIGTERM)
            signal.raise_signal(signal.SIGINT)
        after_two = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    assert quiet == [] and after_none == dict.fromkeys(after_none, record)
    assert signalled == [signal.SIGTERM, signal.SIGINT]
    assert after_two == dict.fromkeys(after_two, signal.SIG_IGN)
    assert outside == []


def test_a_sigint_ignored_when_serve_starts_stays_ignored():
    """`irimi serve &` from a script inherits an ignored SIGINT, and a Ctrl-C meant for the script
    must not stop the serve. SIGTERM is still serve's."""
    import signal

    from irimi import cli

    with _signals_caught_here() as (outside, record):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        with cli._stopping_on_signals() as signalled:
            during = signal.getsignal(signal.SIGINT)
            signal.raise_signal(signal.SIGTERM)
    assert during is signal.SIG_IGN
    assert signalled == [signal.SIGTERM] and outside == []


@pytest.mark.parametrize("when", ["preparing", "starting"])
def test_a_signal_before_the_proxy_is_ready_stops_serve_with_no_banner(
    home, monkeypatch, capsys, when
):
    """A signal that arrives while serve loads its maps and opens its store, or while its engine
    binds, is held until serve can act on it: it prints no banner that would say it serves, stops
    whatever it started, closes the store, releases the lock and exits 0."""
    import signal

    from irimi import cli, runner
    from irimi.store.lock import ServeLock

    prepare, start = cli._prepare_run, runner.start_engine
    started = []

    def signalled_while_preparing(*args, **kwargs):
        run = prepare(*args, **kwargs)
        if when == "preparing":
            signal.raise_signal(signal.SIGTERM)
        return run

    def signalled_while_starting(engine):
        started.append(start(engine))
        if when == "starting":
            signal.raise_signal(signal.SIGTERM)
        return started[-1]

    monkeypatch.setattr(cli, "_prepare_run", signalled_while_preparing)
    monkeypatch.setattr(runner, "start_engine", signalled_while_starting)
    with _signals_caught_here() as (outside, record):
        assert cli.main(["serve", "--port", "0"]) == 0
    assert len(started) == (when == "starting") and outside == []
    assert "listening on" not in capsys.readouterr().out
    assert all(not handle.thread.is_alive() for handle in started)
    ServeLock.hold(home / "store").release()


def test_an_engine_that_cannot_be_built_leaves_the_store_closed_and_unlocked(home, monkeypatch):
    """What `_build_engine` raises reaches the caller, after the store is closed and its lock
    released: nothing it opened outlives it."""
    import threading

    from irimi import cli
    from irimi.store.lock import ServeLock

    def broken(*args, **kwargs):
        raise RuntimeError("no engine")

    writers = sum(thread.name == "irimi-store" for thread in threading.enumerate())
    monkeypatch.setattr(cli, "_build_engine", broken)
    with pytest.raises(RuntimeError, match="no engine"):
        cli.main(["serve", "--port", "0"])
    assert sum(thread.name == "irimi-store" for thread in threading.enumerate()) == writers
    ServeLock.hold(home / "store").release()


def test_two_blocks_printed_at_once_never_interleave(tmp_path, monkeypatch):
    """The store's writer prints a run's summary while the engine's thread prints exchange lines:
    `_ServeOutput` prints each call's lines under one lock, so a block comes out whole. The
    stdout here yields between writes, which is where two unlocked writers would interleave."""
    import sys
    import threading
    import time

    from irimi import cli

    class _Slow:
        def __init__(self):
            self.lines: list[str] = []

        def write(self, text):
            time.sleep(0.001)
            self.lines.extend(line for line in text.split("\n") if line)
            return len(text)

        def flush(self):
            pass

    out = _Slow()
    monkeypatch.setattr(sys, "stdout", out)
    output = cli._ServeOutput(tmp_path / "store")
    blocks = {name: [f"{name} {i}" for i in range(20)] for name in ("a", "b")}
    threads = [threading.Thread(target=output.say, args=(block,)) for block in blocks.values()]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert out.lines in ([*blocks["a"], *blocks["b"]], [*blocks["b"], *blocks["a"]])


def test_a_run_that_cannot_be_read_back_is_one_warning_naming_only_its_type(
    tmp_path, monkeypatch, capsys
):
    """A run whose end is on disk but which cannot be read back prints one line on stderr instead
    of its summary, naming the exception's type and never its message, which may quote what it
    choked on, as the store's own warnings never do."""
    from irimi import cli
    from irimi.store.reader import StoreReader
    from irimi.trace import TraceFormatError

    def unreadable(self, run_id, *, bodies=True):
        raise TraceFormatError("cannot read sk_live_CANARY0000000000000000")

    monkeypatch.setattr(StoreReader, "load_run", unreadable)
    cli._ServeOutput(tmp_path / "store").run_ended(servicemap.MapIndex(), "r1")
    out, err = capsys.readouterr()
    assert out == ""
    assert err == "warning: run r1 ended but could not be read back: TraceFormatError\n"


def test_an_engine_that_stops_on_its_own_ends_serve_as_a_failure(home, monkeypatch, capsys):
    """A sidecar whose proxy is gone must not look alive to its supervisor: serve exits 1 and
    says why. Run on the main thread, it installed its signal handlers, and puts back the
    ones it found when it returns."""
    import signal

    from irimi import cli

    engines = []
    build = cli._build_engine
    monkeypatch.setattr(
        cli, "_build_engine", lambda *a, **kw: engines.append(build(*a, **kw)) or engines[-1]
    )
    printed = cli.print_startup

    def startup(*args):
        printed(*args)
        engines[0].shutdown()

    monkeypatch.setattr(cli, "print_startup", startup)
    before = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    assert cli.main(["serve", "--port", "0"]) == 1
    assert capsys.readouterr().err.strip() == "error: the proxy stopped on its own"
    assert {signum: signal.getsignal(signum) for signum in before} == before


def _wait_for(condition, timeout: float = 30.0) -> None:
    import time

    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("timed out")
        time.sleep(0.02)


class _Served:
    """`irimi <argv>` as its own process, on a free port, with this test's `$IRIMI_HOME`. Its
    stdout is read line by line on a thread, so a test waits for a line with a deadline rather
    than blocking on a pipe, and its stderr goes to a file. `launcher` is what Python runs irimi
    as: its own module, or a `-c` bootstrap that calls `irimi.cli.main`."""

    def __init__(self, home, *argv: str, launcher: tuple[str, ...] = ("-m", "irimi.cli")) -> None:
        import os
        import subprocess
        import sys
        import threading

        env = {**os.environ, paths.IRIMI_HOME_ENV: str(home)}
        env.pop("STRIPE_API_KEY", None)
        self.err = home / "irimi.err"
        with open(self.err, "w") as err:
            self.proc = subprocess.Popen(
                [sys.executable, *launcher, *argv, "--port", "0"],
                cwd=home,
                env=env,
                stdout=subprocess.PIPE,
                stderr=err,
                text=True,
            )
        self.lines: list[str] = []
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            self.lines.append(line.rstrip("\n"))

    def wait_for(self, condition) -> None:
        _wait_for(lambda: condition(self.lines))

    def banner(self) -> list[str]:
        self.wait_for(lambda lines: len(lines) >= 2)
        return self.lines[:2]

    def stop(self, signame: str = "SIGTERM") -> tuple[int, list[str], str]:
        """Send `signame`, and return the exit code, every line printed and stderr."""
        import signal

        self.proc.send_signal(getattr(signal, signame))
        code = self.proc.wait(timeout=60)
        self._reader.join(timeout=10)
        return code, self.lines, self.err.read_text()

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)


def _port_of(banner: list[str]) -> int:
    import re

    match = re.search(r" · listening on 127\.0\.0\.1:(\d+) · ", banner[0])
    assert match, banner
    return int(match.group(1))


@pytest.mark.parametrize(
    ("argv", "signame"),
    [(["serve"], "SIGTERM"), (["shadow", "--serve"], "SIGTERM"), (["shadow", "--serve"], "SIGINT")],
    ids=["serve-SIGTERM", "shadow-serve-SIGTERM", "shadow-serve-SIGINT"],
)
def test_serve_and_its_alias_print_one_banner_and_stop_cleanly_on_a_signal(home, argv, signame):
    """`irimi serve` is `irimi shadow --serve`: one banner, which names no run of its own,
    and both stop on SIGTERM, as a container runtime stops a sidecar, and on Ctrl-C's SIGINT, by
    closing the store and exiting 0 with nothing more to say."""
    from irimi import report

    served = _Served(home, *argv)
    try:
        banner = served.banner()
        port = _port_of(banner)
        assert banner == report.banner_lines(
            "shadow --serve", None, "127.0.0.1", port, ca.ca_paths().cert
        )
        code, lines, err = served.stop(signame)
    finally:
        served.kill()
    assert code == 0, err
    assert lines == banner
    assert "Traceback" not in err


def test_a_wide_bind_says_so_in_the_banner(home):
    """`--listen-host 0.0.0.0` adds the banner line saying who can now reach irimi, as a real
    serve prints it; the loopback default does not, which the test above pins."""
    import re

    from irimi import report

    served = _Served(home, "serve", "--listen-host", "0.0.0.0")
    try:
        served.wait_for(lambda lines: len(lines) >= 3)
        banner = served.lines[:3]
        match = re.search(r" · listening on 0\.0\.0\.0:(\d+) · ", banner[0])
        assert match, banner
        port = int(match.group(1))
        assert banner == report.banner_lines(
            "shadow --serve", None, "0.0.0.0", port, ca.ca_paths().cert
        )
        assert banner[2] == (
            "listening on 0.0.0.0: any machine that can reach this port can send traffic through "
            "irimi and call its control endpoint"
        )
        code, lines, err = served.stop()
    finally:
        served.kill()
    assert code == 0, err
    assert lines == banner


AGENT = """
import os, sys, urllib.request
from irimi import sdk

UP = sys.argv[1]


def call(method, path):
    data = b"x=1" if method == "POST" else None
    request = urllib.request.Request(UP + path, method=method, data=data)
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


@sdk.trigger
def handle(n):
    call("GET", f"/items/{n}")
    call("POST", f"/items/{n}")


handle(1)
handle(2)
call("GET", "/bare")
with sdk.run("run", inputs={"n": 3}):
    call("GET", "/items/3")
    os._exit(0)
"""


def test_a_served_sdk_agent_gets_one_summary_block_per_run_it_ends(home, tmp_path_factory):
    """Serve mode end to end: `irimi shadow --serve` as its own process, and an SDK agent in
    another, pointed at it by the variables the README's "Serve mode" names.

    The agent runs two triggers, each a read and a write, then a read in no run, then a third run
    it never ends: it exits mid-run. Each ended run prints a `--- run <id> ---` block equal to
    `stored_summary_lines` for that run, read from the store; the read in no run lands in
    `unattributed/`, which builds no run; and after SIGTERM, which exits 0, the run the agent never
    ended loads as `incomplete`. The health answer says serve mode."""
    import json
    import threading
    import urllib.request
    from http.server import BaseHTTPRequestHandler, HTTPServer

    from irimi import report
    from irimi.store.reader import StoreReader
    from irimi.trace import UNATTRIBUTED

    class _Items(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        def do_POST(self):  # never reached: irimi answers a write itself
            self.send_response(500)
            self.end_headers()

        def log_message(self, *args):
            pass

    upstream = HTTPServer(("127.0.0.1", 0), _Items)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    served = _Served(home, "shadow", "--serve")
    try:
        port = _port_of(served.banner())
        listener = f"http://127.0.0.1:{port}"
        # The control endpoint says it serves: `EngineConfig.serve`, which the CLI sets.
        direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with direct.open(f"{listener}/_irimi/health", timeout=10) as health:
            assert json.load(health)["serve"] is True
        up = f"http://127.0.0.1:{upstream.server_address[1]}"
        _run_agent(tmp_path_factory, AGENT, listener, up)
        # Each block is printed when its run's end is on disk, while serve runs on: both are out
        # before serve is told to stop.
        served.wait_for(lambda lines: sum(line.startswith("--- run ") for line in lines) == 2)
        code, lines, err = served.stop("SIGTERM")
    finally:
        served.kill()
        upstream.shutdown()
    assert code == 0, err
    reader = StoreReader(home / "store")
    records = {r.run_id: r for r in reader.list_runs()}
    ended = sorted(run_id for run_id, r in records.items() if r.end and r.end.outcome == "ok")
    [open_run] = [run_id for run_id, r in records.items() if r.end is None]
    assert len(records) == 3 and len(ended) == 2
    assert all(r.attribution == "sdk" for r in records.values())
    assert report.run_status(records[open_run]) == "incomplete"

    assert sorted(_blocks(lines, reader)) == ended
    # The read in no run is stored as no run's: in `unattributed/`, never as a run of its own.
    [bare] = reader.load_run(UNATTRIBUTED).events
    assert bare.request.path == "/bare"
    paths_by_run = {
        run_id: [e.request.path for e in reader.load_run(run_id).events] for run_id in records
    }
    assert sorted(paths_by_run.values()) == [
        ["/items/1", "/items/1"],
        ["/items/2", "/items/2"],
        ["/items/3"],
    ]


def _run_agent(tmp_path_factory, source: str, listener: str, *argv: str) -> None:
    """`source` as an agent of its own, in its own process, pointed at the serve on `listener` by
    the variables the README's "Serve mode" names, and nothing else of the test's proxies."""
    import os
    import subprocess
    import sys

    agent = tmp_path_factory.mktemp("agent") / "agent.py"
    agent.write_text(source)
    env = {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}
    env.pop("STRIPE_API_KEY", None)
    env.update(
        {
            "HTTP_PROXY": listener,
            "HTTPS_PROXY": listener,
            "SSL_CERT_FILE": str(ca.ca_paths().cert),
            "REQUESTS_CA_BUNDLE": str(ca.ca_paths().cert),
            ENGINE_ACTIVE_ENV: "1",
            CONTROL_ENV: f"{listener}/_irimi",
        }
    )
    done = subprocess.run(
        [sys.executable, str(agent), *argv], env=env, capture_output=True, text=True, timeout=120
    )
    assert done.returncode == 0, done.stderr
    assert "irimi.sdk" not in done.stderr, done.stderr  # every post reached irimi


def _blocks(lines: list[str], reader) -> list[str]:
    """The run ids of the `--- run <id> ---` blocks in `lines`, each checked to be followed at once
    by `stored_summary_lines` for that run, as the store holds it now: printed under one lock, a
    block is never split by another line."""
    from irimi import report

    maps = servicemap.load()
    ids = []
    for at, line in enumerate(lines):
        if not line.startswith("--- run "):
            continue
        run_id = line.removeprefix("--- run ").removesuffix(" ---")
        expected = report.stored_summary_lines(reader.load_run(run_id), maps)
        assert lines[at + 1 : at + 1 + len(expected)] == expected, run_id
        ids.append(run_id)
    return ids


STREAMING_AGENT = """
import http.client, os, sys, urllib.parse
from irimi import sdk

UP = sys.argv[1]
PROXY = urllib.parse.urlsplit(os.environ["HTTP_PROXY"])


@sdk.trigger
def handle(n):
    conn = http.client.HTTPConnection(PROXY.hostname, PROXY.port, timeout=10)
    try:
        conn.request("GET", f"{UP}/stream/{n}")
        conn.getresponse().read()
    finally:
        conn.close()


for n in range(10):
    handle(n)
"""


def test_a_stream_read_to_its_end_is_in_its_runs_block(home, tmp_path_factory):
    """A streamed answer is recorded once its last chunk has gone through irimi, and the run's
    block is printed when the SDK's end is on disk. For a stream the agent reads to its
    end, the record comes first: mitmproxy sends the agent the stream's end only after the
    `response` hook has queued the exchange, and the agent posts its run's end after that. So the
    block of each run holds its stream. (A stream the agent leaves open past its run's end is
    recorded when it closes, after the block: the README says so.)"""
    import threading
    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    from irimi.store.reader import StoreReader

    class _Events(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            self.send_response(200)
            self.send_header("content-type", "text/event-stream")
            self.send_header("transfer-encoding", "chunked")
            self.end_headers()
            for i in range(3):
                event = f"data: {i}\n\n".encode()
                self.wfile.write(b"%x\r\n%s\r\n" % (len(event), event))
                self.wfile.flush()
                time.sleep(0.01)
            self.wfile.write(b"0\r\n\r\n")

        def log_message(self, *args):
            pass

    upstream = ThreadingHTTPServer(("127.0.0.1", 0), _Events)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    served = _Served(home, "serve")
    try:
        listener = f"http://127.0.0.1:{_port_of(served.banner())}"
        _run_agent(
            tmp_path_factory,
            STREAMING_AGENT,
            listener,
            f"http://127.0.0.1:{upstream.server_address[1]}",
        )
        served.wait_for(lambda lines: sum(line.startswith("--- run ") for line in lines) == 10)
        code, lines, err = served.stop()
    finally:
        served.kill()
        upstream.shutdown()
    assert code == 0, err
    reader = StoreReader(home / "store")
    ended = _blocks(lines, reader)
    assert len(ended) == 10
    for run_id in ended:
        [stream] = reader.load_run(run_id).events
        assert stream.stream_chunks, run_id
