"""Serve mode: `irimi serve`, a sidecar with no child process.

The engine under a wide bind, the store's end-of-run callback, then the command itself."""

import asyncio
import contextlib
import dataclasses
import json
import socket
import sys
from typing import cast

import pytest

from irimi import cli, servicemaps
from irimi.cli import compose, main
from irimi.config import ca, paths
from irimi.core import exitcodes
from irimi.core.exchange import Exchange
from irimi.decide.overlay import NoOverlay
from irimi.decide.policy import ShadowPolicy
from irimi.engine import EngineConfig, Serve
from irimi.engine.mitm import IrimiAddon, MitmEngine
from irimi.protocol.env import CONTROL_ENV, ENGINE_ACTIVE_ENV
from tests.support import builders
from tests.support.credentials import MESSAGE_SECRET, UNREADABLE_SECRET
from tests.support.doubles import NullStore
from tests.support.engine import ReaderGoneWhen, Served, port_of, serve_blocks, spy_on_the_store
from tests.support.servers import stop_server
from tests.support.waiting import eventually


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
        maps=servicemaps.MapIndex(),
    )


def _own_address() -> str | None:
    """Return this machine's non-loopback IPv4 address, or None with no route.

    Connecting a UDP socket sends nothing; it only asks the routing table."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            sock.connect(("192.0.2.1", 9))  # TEST-NET-1: routed like any address, never answered
        except OSError:
            return None
        address = sock.getsockname()[0]
    return None if address.startswith("127.") or address == "0.0.0.0" else address


# -- base-URL requests stay loopback-only ---------------------------------------------------------


def test_the_reverse_door_refuses_a_peer_that_is_not_loopback(tmp_path, monkeypatch):
    """Under a wide bind, a base-URL request from a peer that is not loopback is refused.

    Base-URL mode relays the agent's credentials, so `ingress.admit` checks the peer. Driven
    through the addon, since a connection from another machine cannot be staged here."""
    from mitmproxy.test import tflow, tutils

    seen: list[Exchange] = []
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

    assert flow.response is not None and flow.response.content is not None
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
    """Record the (address, port) each request hook call arrived on; a self-loop arrives twice."""
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
    """Bound to 0.0.0.0, a request for this machine's own address on irimi's port is answered.

    So is one for a name resolving to it, as `irimi` does in compose (patched `getaddrinfo`).
    Forwarding either would loop back into the listener."""
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
    from irimi.cli import runner

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
    """A target at this machine's own address and irimi's port is refused, never dialled.

    The write gets `irimi_target_failed`, and no second connection reaches the listener."""
    import json
    import os

    address = _own_address()
    if address is None:
        assert not os.environ.get("CI"), "a CI runner with no route has no address to test"
        pytest.skip("this machine has no route, so no non-loopback address")
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir()
    (maps_dir / "demo.yaml").write_text(TARGETED_MAP)
    # The target names the port the engine bound, so it is loaded into the shared index once that
    # port is known; a port reserved in advance could be taken by another test.
    maps = servicemaps.MapIndex()
    cfg = dataclasses.replace(_config(tmp_path, monkeypatch, listen_host="0.0.0.0"), maps=maps)
    calls = _counting_requests(monkeypatch)
    engine = MitmEngine(cfg, ShadowPolicy(maps=maps), NullStore(), NoOverlay())
    from irimi.cli import runner

    handle = runner.start_engine(engine)
    try:
        port = handle.port()
        loaded = servicemaps.load(
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
    """If telling proxy from base URL raises, irimi answers 400 instead of forwarding the flow."""
    from mitmproxy.test import tflow, tutils

    from irimi.engine import ingress

    def broken(*args):
        raise ValueError("no address")

    monkeypatch.setattr(ingress, "detect_door", broken)
    seen: list[Exchange] = []
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
    assert flow.response is not None and flow.response.content is not None
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
    """Under serve, irimi's 502 for a failed decision is filed under `trace.UNATTRIBUTED`."""
    from mitmproxy.test import tflow, tutils

    from irimi.protocol.trace import UNATTRIBUTED

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
    assert flow.response is not None
    assert flow.response.status_code == 502
    [ex] = store.recorded
    assert ex.run_id == UNATTRIBUTED
    assert "decision-failed" in ex.flags


def test_a_loopback_bind_still_finds_its_own_loopback_spellings(tmp_path, monkeypatch):
    """Under the loopback bind, `127.1` on irimi's port is irimi, via the proxy as via base URL."""
    calls = _counting_requests(monkeypatch)
    cfg = dataclasses.replace(_config(tmp_path, monkeypatch), reverse_hosts=frozenset())
    engine = MitmEngine(cfg, ShadowPolicy(), NullStore(), NoOverlay())
    from irimi.cli import runner

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
    """`on_run_ended` runs on the writer thread once the run is whole on disk, end included.

    A run that has not ended is not called back for."""
    import threading

    from irimi.record.store.reader import StoreReader
    from irimi.record.store.writer import DirectoryStore

    root = tmp_path / "store"
    seen = []

    def ended(run_id):
        stored = StoreReader(root).load_run(run_id)
        assert stored.record.end is not None
        seen.append(
            (run_id, threading.current_thread().name, stored.record.end.outcome, stored.events)
        )

    store = DirectoryStore(root, b"k" * 32, ended)
    store.start_run(builders.run_record("r1"))
    store.record(builders.exchange(run_id="r1"))
    store.record(builders.exchange(run_id="r2"))
    store.end_run("r1", 20.0, "ok")
    store.close()
    [(run_id, thread, outcome, events)] = seen
    assert (run_id, thread, outcome) == ("r1", "irimi-store", "ok")
    assert [e.run_id for e in events] == ["r1"]


def test_a_run_s_writes_and_keys_are_dropped_once_its_end_is_on_disk(tmp_path, monkeypatch):
    """A run's write log and idempotency keys are dropped when its end is on disk; others stay."""
    from irimi import cli
    from irimi.cli import compose
    from irimi.decide import idempotency

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)
    ca.generate_ca(ca.ca_paths())
    args = cli.parse_args(["serve", "--store", str(tmp_path / "store")])
    left_at_end: list[tuple] = []
    slot_of = {run_id: idempotency.slot_id(run_id, "stripe", (), "k-1") for run_id in ("r1", "r2")}

    def on_run_ended(index, run_id):
        assert run is not None and run.write_log is not None and run.keys is not None
        left_at_end.append(
            (run.write_log.for_run(run_id), run.keys.get(slot_of[run_id], builders.REFUND_PARAMS))
        )

    run = compose.prepare_run(args, serve=True, on_run_ended=on_run_ended)
    assert run is not None and run.write_log is not None and run.keys is not None
    assert run.lock is not None
    ended, still_open = builders.exchange(run_id="r1"), builders.exchange(run_id="r2")
    run.write_log.add(ended)
    run.write_log.add(still_open)
    for slot in slot_of.values():
        run.keys.put(slot, builders.REFUND_PARAMS, builders.first_answer())
    try:
        run.store.start_run(builders.run_record("r1"))
        run.store.end_run("r1", 20.0, "ok")
    finally:
        run.store.close()
        run.lock.release()
    assert left_at_end == [((), (None, False))]
    assert run.write_log.for_run("r2") == (still_open,)
    assert run.keys.get(slot_of["r2"], builders.REFUND_PARAMS) == (builders.first_answer(), False)
    engine = compose.build_engine(run, lambda ex: None)
    assert isinstance(engine, MitmEngine) and isinstance(engine.policy, ShadowPolicy)
    assert engine.write_log is run.write_log
    assert engine.policy.idempotency is run.keys


def test_a_callback_that_raises_is_logged_and_the_store_carries_on(tmp_path, caplog):
    """A raising `on_run_ended` logs one warning naming its type, and the store writes on."""
    from irimi.record.store.reader import StoreReader
    from irimi.record.store.writer import DirectoryStore

    root = tmp_path / "store"

    def ended(run_id):
        raise ValueError(f"cannot print {MESSAGE_SECRET}")

    store = DirectoryStore(root, b"k" * 32, ended)
    store.start_run(builders.run_record("r1"))
    store.end_run("r1", 20.0, "ok")
    store.record(builders.exchange(run_id="r2"))
    store.close()
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings == ["irimi: the end-of-run callback raised ValueError"]
    end = StoreReader(root).load_run("r1").record.end
    assert end is not None
    assert end.outcome == "ok"
    assert len(StoreReader(root).load_run("r2").events) == 1
    assert store.stats().dropped == 0


def test_one_serve_per_store_root(tmp_path):
    """A second `serve.lock` holder is refused until the first releases, which may happen twice."""
    import os

    from irimi.record.store.lock import ServeLock, StoreLocked

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

    from irimi.report import terminal

    lines = terminal.banner_lines("serve", None, host, 4000, Path("/ca.pem"))
    assert lines[0].startswith(f"irimi serve · {terminal.SERVE_RUNS} · listening on ")
    assert lines[2:] == [
        f"listening on {host}: any machine that can reach this port can send traffic through "
        "irimi and call its control endpoint"
    ]


@pytest.mark.parametrize("host", ["127.0.0.1", "127.0.0.9", "::1"])
def test_a_loopback_bind_adds_none(host):
    from pathlib import Path

    from irimi.report import terminal

    lines = terminal.banner_lines("shadow", "7f3a", host, 4000, Path("/ca.pem"))
    assert len(lines) == 2


def test_an_ipv6_listener_is_bracketed_where_it_is_a_url(tmp_path):
    """The banner and the child's proxy and control variables bracket IPv6: `[::1]:4000`."""
    from pathlib import Path

    from irimi.cli import runner
    from irimi.report import terminal

    [first, *_] = terminal.banner_lines("shadow", "7f3a", "::1", 4000, Path("/ca.pem"))
    assert "listening on [::1]:4000 ·" in first
    env = runner.child_env({}, "::1", 4000, Path("/ca.pem"), "7f3a")
    assert env["HTTPS_PROXY"] == "http://[::1]:4000"
    assert env[CONTROL_ENV] == "http://[::1]:4000/_irimi"


# -- the command -----------------------------------------------------------------------------------


@pytest.fixture
def home(home, monkeypatch):
    """The shared `home`, which is also the working directory here."""
    monkeypatch.chdir(home)
    return home


def test_serve_with_a_command_is_a_usage_error(home, monkeypatch, capsys):
    """`serve -- echo hi` is a usage error: exit 2, nothing started or stored."""
    from irimi.cli import main, serve

    monkeypatch.setattr(
        serve, "cmd_serve", lambda *args, **kwargs: pytest.fail("serve was started")
    )
    with pytest.raises(SystemExit) as exc:
        main(["serve", "--", "echo", "hi"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert err.startswith("usage: irimi serve")
    assert "error: unrecognized arguments: -- echo hi" in err
    assert not (home / "store").exists()


def test_serve_has_one_spelling(home, capsys):
    """`irimi serve` is serve mode; `shadow` has no `--serve`."""
    from irimi.cli import main

    with pytest.raises(SystemExit) as exc:
        main(["shadow", "--serve"])
    assert exc.value.code == 2
    assert "unrecognized arguments: --serve" in capsys.readouterr().err
    assert not (home / "store").exists()


def test_serve_with_a_command_exits_2_from_the_command_line(home):
    """Run from a shell, `irimi serve -- echo hi` exits 2 with usage and stores nothing."""
    import os
    import subprocess
    import sys

    env = {**os.environ, paths.IRIMI_HOME_ENV: str(home)}
    done = subprocess.run(
        [sys.executable, "-m", "irimi", "serve", "--", "echo", "hi"],
        cwd=home,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert done.returncode == 2
    assert done.stderr.startswith("usage: irimi serve") and done.stdout == ""
    assert not (home / "store").exists()


def test_a_second_serve_on_one_store_is_refused_before_it_starts(home, capsys):
    """A second serve on one store exits 2 with one line, and the first serves on.

    Two would share `unattributed/events.jsonl`. The second gets a `stop` already set, so if it
    were wrongly let through it would return at once."""
    import threading

    from irimi import cli
    from irimi.cli import serve
    from irimi.record.store.lock import ServeLock

    def writers() -> int:
        return sum(thread.name == "irimi-store" for thread in threading.enumerate())

    stop = threading.Event()
    args = cli.parse_args(["serve", "--port", "0"])
    codes: list[int] = []
    first = threading.Thread(target=lambda: codes.append(serve.cmd_serve(args, stop)))
    first.start()
    try:
        eventually(lambda: "listening on" in capsys.readouterr().out, timeout=30)
        running = writers()
        second = cli.parse_args(["serve", "--port", "0"])
        stopped = threading.Event()
        stopped.set()
        assert second.func(second, stopped) == 2
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
    """Record SIGINT and SIGTERM in a list for the block, so an uncaught one cannot kill pytest."""
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
    """Serve's signal handlers only record the signal, and both are ignored once one arrives.

    A handler that set an event inside `Event.wait` would deadlock. With no signal, the handlers
    found are put back."""
    import signal

    from irimi.cli import runner

    with _signals_caught_here() as (outside, record):
        with runner.stopping_on_signals() as quiet:
            pass
        after_none = {
            signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
        }
        with runner.stopping_on_signals() as signalled:
            signal.raise_signal(signal.SIGTERM)
            signal.raise_signal(signal.SIGINT)
        after_two = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    assert quiet == [] and after_none == dict.fromkeys(after_none, record)
    assert signalled == [signal.SIGTERM, signal.SIGINT]
    assert after_two == dict.fromkeys(after_two, signal.SIG_IGN)
    assert outside == []


def test_a_sigint_ignored_when_serve_starts_stays_ignored():
    """An inherited ignored SIGINT, as `irimi serve &` gets, stays ignored; SIGTERM still stops."""
    import signal

    from irimi.cli import runner

    with _signals_caught_here() as (outside, _record):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        with runner.stopping_on_signals() as signalled:
            during = signal.getsignal(signal.SIGINT)
            signal.raise_signal(signal.SIGTERM)
    assert during is signal.SIG_IGN
    assert signalled == [signal.SIGTERM] and outside == []


@pytest.mark.parametrize("when", ["preparing", "starting"])
def test_a_signal_before_the_proxy_is_ready_stops_serve_with_no_banner(
    home, monkeypatch, capsys, when
):
    """A signal during startup stops serve with no banner, the store closed and the lock freed."""
    import signal

    from irimi import cli
    from irimi.cli import compose, runner
    from irimi.record.store.lock import ServeLock

    prepare, start = compose.prepare_run, runner.start_engine
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

    monkeypatch.setattr(compose, "prepare_run", signalled_while_preparing)
    monkeypatch.setattr(runner, "start_engine", signalled_while_starting)
    with _signals_caught_here() as (outside, _record):
        assert cli.main(["serve", "--port", "0"]) == 0
    assert len(started) == (when == "starting") and outside == []
    assert "listening on" not in capsys.readouterr().out
    assert all(not handle.thread.is_alive() for handle in started)
    ServeLock.hold(home / "store").release()


def test_an_engine_that_cannot_be_built_leaves_the_store_closed_and_unlocked(home, monkeypatch):
    """An engine factory's error reaches the caller after the store is closed and unlocked."""
    import threading

    from irimi import cli
    from irimi.record.store.lock import ServeLock

    def broken(*args, **kwargs):
        raise RuntimeError("no engine")

    writers = sum(thread.name == "irimi-store" for thread in threading.enumerate())
    with pytest.raises(RuntimeError, match="no engine"):
        cli.main(["serve", "--port", "0"], engine_factory=broken)
    assert sum(thread.name == "irimi-store" for thread in threading.enumerate()) == writers
    ServeLock.hold(home / "store").release()


def test_two_blocks_printed_at_once_never_interleave(tmp_path, monkeypatch):
    """`_ServeOutput` prints each call's lines under one lock, so a block comes out whole.

    The stdout here yields between writes, where two unlocked writers would interleave."""
    import sys
    import threading
    import time

    from irimi.cli import serve

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
    output = serve._ServeOutput(tmp_path / "store")
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
    """An ended run that cannot be read back gets one warning naming the error's type only.

    The message may quote what the reader choked on."""
    from irimi.cli import serve
    from irimi.protocol.trace import TraceFormatError
    from irimi.record.store.reader import StoreReader

    def unreadable(self, run_id, *, bodies=True):
        raise TraceFormatError(f"cannot read {UNREADABLE_SECRET}")

    monkeypatch.setattr(StoreReader, "load_run", unreadable)
    serve._ServeOutput(tmp_path / "store").run_ended(servicemaps.MapIndex(), "r1")
    out, err = capsys.readouterr()
    assert out == ""
    assert err == "warning: run r1 ended but could not be read back: TraceFormatError\n"


def test_an_engine_that_stops_on_its_own_ends_serve_as_a_failure(home, monkeypatch, capsys):
    """Serve exits 2 and says why when its proxy stops, and puts back the signal handlers."""
    import signal

    from irimi import cli
    from irimi.cli import compose

    engines = []

    def build(*a, **kw):
        engines.append(compose.build_engine(*a, **kw))
        return engines[-1]

    printed = compose.print_startup

    def startup(*args):
        printed(*args)
        engines[0].shutdown()

    monkeypatch.setattr(compose, "print_startup", startup)
    before = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    assert cli.main(["serve", "--port", "0"], engine_factory=build) == 2
    assert capsys.readouterr().err.strip() == "error: the proxy stopped on its own"
    assert {signum: signal.getsignal(signum) for signum in before} == before


@pytest.mark.parametrize(
    ("argv", "signame"),
    [(["serve"], "SIGTERM"), (["serve"], "SIGINT")],
    ids=["serve-SIGTERM", "serve-SIGINT"],
)
def test_serve_prints_one_banner_and_stops_cleanly_on_a_signal(home, argv, signame):
    """`irimi serve` prints one banner naming no run, and exits 0 quietly on SIGTERM or SIGINT."""
    from irimi.report import terminal

    served = Served(home, *argv)
    try:
        banner = served.banner()
        port = port_of(banner)
        assert banner == terminal.banner_lines("serve", None, "127.0.0.1", port, ca.ca_paths().cert)
        code, lines, err = served.stop(signame)
    finally:
        served.kill()
    assert code == 0, err
    assert lines == banner
    assert "Traceback" not in err


def test_a_wide_bind_says_so_in_the_banner(home):
    """A real serve with `--bind 0.0.0.0` prints the banner line saying who can reach irimi."""
    import re

    from irimi.report import terminal

    served = Served(home, "serve", "--bind", "0.0.0.0")
    try:
        served.wait_for(lambda lines: len(lines) >= 3)
        banner = served.lines[:3]
        match = re.search(r" · listening on 0\.0\.0\.0:(\d+) · ", banner[0])
        assert match, banner
        port = int(match.group(1))
        assert banner == terminal.banner_lines("serve", None, "0.0.0.0", port, ca.ca_paths().cert)
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
    """Each run a served SDK agent ends prints a block equal to its stored summary.

    The agent runs two triggers, a read in no run (stored in `unattributed/`), and a third run it
    never ends, which loads as `incomplete`."""
    import json
    import threading
    import urllib.request
    from http.server import BaseHTTPRequestHandler, HTTPServer

    from irimi.protocol.trace import UNATTRIBUTED
    from irimi.record.store.reader import StoreReader
    from irimi.report.runs import run_status

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
    served = Served(home, "serve")
    try:
        port = port_of(served.banner())
        listener = f"http://127.0.0.1:{port}"
        # The control endpoint's health answer says serve mode.
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
        stop_server(upstream)
    assert code == 0, err
    reader = StoreReader(home / "store")
    records = {r.run_id: r for r in reader.list_runs()}
    ended = sorted(run_id for run_id, r in records.items() if r.end and r.end.outcome == "ok")
    [open_run] = [run_id for run_id, r in records.items() if r.end is None]
    assert len(records) == 3 and len(ended) == 2
    assert all(r.attribution == "sdk" for r in records.values())
    assert run_status(records[open_run]) == "incomplete"

    assert sorted(serve_blocks(lines, reader)) == ended
    # The read in no run is stored as no run's: in `unattributed/`, never as a run of its own.
    [bare] = reader.load_run(UNATTRIBUTED).events
    assert isinstance(bare, Exchange)
    assert bare.request.path == "/bare"
    paths_by_run = {
        run_id: [cast(Exchange, e).request.path for e in reader.load_run(run_id).events]
        for run_id in records
    }
    assert sorted(paths_by_run.values()) == [
        ["/items/1", "/items/1"],
        ["/items/2", "/items/2"],
        ["/items/3"],
    ]


def _run_agent(tmp_path_factory, source: str, listener: str, *argv: str) -> None:
    """Run `source` as its own process, pointed at `listener` by the serve-mode variables."""
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
        [sys.executable, str(agent), *argv],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    assert "irimi.sdk" not in done.stderr, done.stderr  # every post reached irimi


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
    """A stream the agent reads to its end is recorded before its run ends, so it is in the block.

    mitmproxy sends the stream's end only after the `response` hook queued the exchange."""
    import threading
    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    from irimi.record.store.reader import StoreReader

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
    served = Served(home, "serve")
    try:
        listener = f"http://127.0.0.1:{port_of(served.banner())}"
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
        stop_server(upstream)
    assert code == 0, err
    reader = StoreReader(home / "store")
    ended = serve_blocks(lines, reader)
    assert len(ended) == 10
    for run_id in ended:
        [stream] = reader.load_run(run_id).events
        assert isinstance(stream, Exchange)
        assert stream.stream_chunks, run_id


def test_serve_without_a_ca_is_refused(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    assert main(["serve"]) == exitcodes.ERROR
    assert "irimi init" in capsys.readouterr().err


def test_the_top_level_help_lists_serve(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    assert "serve" in capsys.readouterr().out


def test_serve_help_documents_port(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["serve", "--help"])
    assert exc.value.code == 0
    assert "--port" in capsys.readouterr().out


def test_serve_on_a_port_in_use_is_refused(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    ca.generate_ca(ca.ca_paths())
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        port = sock.getsockname()[1]
        assert main(["serve", "--port", str(port)]) == exitcodes.ERROR
        assert capsys.readouterr().err.splitlines() == [
            f"error: port {port} on 127.0.0.1 is in use; pick another with --port, or --port 0 "
            "for any free port"
        ]
    finally:
        sock.close()


def test_serve_help_has_base_url_host(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["serve", "--help"])
    assert exc.value.code == 0
    assert "--base-url-host" in capsys.readouterr().out


def test_a_reader_gone_by_serve_s_banner_closes_its_store_and_releases_its_lock(home, monkeypatch):
    """A reader gone before the banner exits 141 with the engine stopped and the store unlocked."""
    import threading

    from irimi.record.store.lock import ServeLock

    spy = spy_on_the_store(monkeypatch)
    out = ReaderGoneWhen()
    out.gone = True
    monkeypatch.setattr(sys, "stdout", out)
    before = set(threading.enumerate())
    assert main(["serve", "--port", "0"]) == exitcodes.BROKEN_PIPE
    assert spy.closed != []
    assert [t for t in threading.enumerate() if t not in before and t.is_alive()] == []
    ServeLock.hold(home / "store").release()


def test_serve_prints_a_line_for_each_tool_call(home, monkeypatch, capsys):
    """`serve` wires the control endpoint's tool calls to its stream, as it does exchanges.

    The engine refuses to start; its `run` waits for `shutdown`, so `wait_ready` raises first."""
    import asyncio
    import threading

    from irimi.engine import EngineStartError
    from irimi.protocol.trace import ToolCall

    wired = []
    stopped = threading.Event()

    class _Refuses:
        async def run(self) -> None:
            while not stopped.is_set():
                await asyncio.sleep(0.01)

        async def wait_ready(self) -> None:
            raise EngineStartError("not started")

        def shutdown(self) -> None:
            stopped.set()

    def build(run, on_exchange, on_tool_call):
        wired.append(on_tool_call)
        return _Refuses()

    factory = cast(compose.EngineFactory, build)
    assert main(["serve", "--port", "0"], engine_factory=factory) == exitcodes.ERROR
    assert "error: not started" in capsys.readouterr().err.splitlines()
    [on_tool_call] = wired
    on_tool_call(ToolCall("t", "r", "db.write", "write", "shadow", {}, None, None, 1.0, 2.0))
    assert "tool-fake     write     db.write -> ok" in capsys.readouterr().out.splitlines()


def test_serve_records_under_the_store_it_was_given(home, monkeypatch, capsys):
    """`serve --store` stores there, an exchange in no run under `unattributed/`, named at stop.

    Stopped by its `stop` event, as the corpus harness stops it, not by a signal."""
    import threading
    import urllib.request
    from http.server import BaseHTTPRequestHandler, HTTPServer

    from irimi.core.exchange import Exchange
    from irimi.protocol.trace import UNATTRIBUTED
    from irimi.record.store.reader import StoreReader

    class _Hello(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("content-length", "5")
            self.end_headers()
            self.wfile.write(b"hello")

        def log_message(self, *args):
            pass

    upstream = HTTPServer(("127.0.0.1", 0), _Hello)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    stop = threading.Event()
    got = []

    def drive(port):
        proxy = urllib.request.ProxyHandler({"http": f"http://127.0.0.1:{port}"})
        url = f"http://127.0.0.1:{upstream.server_address[1]}/hello"
        try:
            got.append(urllib.request.build_opener(proxy).open(url, timeout=10).read())
        finally:  # stop serve whatever happened, or the test would wait on it forever
            stop.set()

    def startup(mode, args, index, run_id, host, port, cert):
        threading.Thread(target=drive, args=(port,), daemon=True).start()

    monkeypatch.setattr(compose, "print_startup", startup)
    elsewhere = home / "elsewhere"
    args = cli.parse_args(["serve", "--port", "0", "--store", str(elsewhere)])
    try:
        assert args.func(args, stop) == 0
    finally:
        stop_server(upstream)
    assert got == [b"hello"]
    assert capsys.readouterr().out.splitlines()[-1] == (
        "stopped · 1 exchange not in any run: irimi runs show unattributed"
    )
    assert not (home / "store").exists()
    assert StoreReader(elsewhere).list_runs() == []
    stored = StoreReader(elsewhere).load_run(UNATTRIBUTED)
    seen = [
        (ex.request.path, ex.response.body)
        if isinstance(ex, Exchange) and ex.response is not None
        else ex
        for ex in stored.events
    ]
    assert seen == [("/hello", b"hello")]
