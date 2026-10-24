"""The sidecar example beside `irimi serve`: two concurrent refunds are two runs that never mix.

The agent gets no `NO_PROXY`, for the reason `tests/support/scenarios.py` gives."""

import contextlib
import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, wait
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import cast
from urllib.parse import parse_qs, quote

import pytest

from examples.sidecar import app
from irimi import servicemaps
from irimi.cli import main
from irimi.config import ca
from irimi.core.exchange import Exchange
from irimi.protocol.control import CONTROL_PREFIX
from irimi.protocol.env import CONTROL_ENV, ENGINE_ACTIVE_ENV, PROXY_ENVS, RUN_ENV
from irimi.protocol.trace import UNATTRIBUTED, RunEnd, ToolCall
from irimi.record.store.reader import StoreReader
from irimi.report.model import RunView
from irimi.report.summary import summary_lines
from tests.support.credentials import HERMETIC_STRIPE_KEY, REFUSED_LIVE_KEY
from tests.support.engine import Served, port_of, serve_blocks
from tests.support.maps import PRECONDITION_STRIPE_MAP
from tests.support.scenarios import base_url_stripe
from tests.support.upstreams import Answer, FakeStripe, serve
from tests.support.waiting import eventually

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[2]
CHARGES = ("ch_A", "ch_B")
REASON = "requested_by_customer"
TOOL_NAME = "db.mark_refunded"
# How long `ch_A` must stay unanswered, alone at the barrier, before `ch_B` is sent. Without the
# barrier it is answered in milliseconds; with it, not before `ch_B` arrives.
HELD_S = 0.5
# How long a read held at the barrier waits for the other, so a test that sent one fails.
BARRIER_TIMEOUT_S = 30.0
# The app as `python -m examples.sidecar.app` starts it, minus `server_bind`'s reverse DNS lookup
# (`socket.getfqdn`), which a slow CI resolver can hold past the test's deadline.
APP = """
import socket
import sys

socket.getfqdn = lambda name="": name or "localhost"

from examples.sidecar import app

sys.exit(app.main())
"""
# irimi as `irimi.cli.main`, with the shipped maps read from argv[1].
IRIMI = """
import sys
from pathlib import Path

from irimi import cli
from irimi.servicemaps import loader

maps = Path(sys.argv[1])
loader.shipped_dir = lambda: maps
sys.exit(cli.main(sys.argv[2:]))
"""


# Charges the stub answers with an error, keyed by the path segment the agent sends. The odd one
# fails only when the agent quotes it into one segment; unquoted, it names another path (200).
FAILING = {"ch_MISSING": 404, "ch_DOWN": 500, "ch_ODD/../refunds?x=1": 404}


def _two_at_once(answer: Callable[..., object]) -> Callable[..., object]:
    """Wrap `any_charge` to hold the first two charge reads until both arrive.

    Two refunds sent one after the other are then two runs open at once. Later reads, irimi's
    check reads among them, are answered at once."""
    barrier = threading.Barrier(2, timeout=BARRIER_TIMEOUT_S)
    lock = threading.Lock()
    arrived = [0]

    def any_charge(handler, charge_id):
        with lock:
            arrived[0] += 1
            hold = arrived[0] <= 2
        if hold:
            barrier.wait()
        return answer(handler, charge_id)

    return any_charge


@pytest.fixture
def stripe_stub():
    """Yield a Stripe stub: any charge is refundable but `FAILING`'s, behind `_two_at_once`.

    `stripe_stub.heard` keeps each request's headers, so a leaked `Irimi-Run` shows."""
    failing = {
        f"/v1/charges/{quote(charge, safe='')}": Answer(
            status, {"error": {"type": "invalid_request_error"}}
        )
        for charge, status in FAILING.items()
    }
    stripe = base_url_stripe()
    stripe["any_charge"] = _two_at_once(stripe["any_charge"])
    port = serve(FakeStripe, threaded=True, **stripe, answers=failing)
    yield port
    port.stop()


@pytest.fixture
def home(home, monkeypatch, home_with_maps):
    """Return the shared `home` as the working directory, with the loopback Stripe's map.

    This process reads the same maps when it rebuilds each run's summary."""
    monkeypatch.chdir(home)
    home_with_maps({"stripe.yaml": PRECONDITION_STRIPE_MAP})
    return home


def _agent_env(listener: str, stub_port: int, db: Path) -> dict[str, str]:
    """Return the README's serve-mode variables and the app's own, with no `NO_PROXY`."""
    env = {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}
    for name in ("STRIPE_API_KEY", RUN_ENV):
        env.pop(name, None)
    env.update(
        {
            **dict.fromkeys(PROXY_ENVS, listener),
            "SSL_CERT_FILE": str(ca.ca_paths().cert),
            "REQUESTS_CA_BUNDLE": str(ca.ca_paths().cert),
            ENGINE_ACTIVE_ENV: "1",
            CONTROL_ENV: listener + CONTROL_PREFIX.rstrip("/"),
            "STRIPE_API_BASE": f"http://127.0.0.1:{stub_port}",
            "STRIPE_API_KEY": HERMETIC_STRIPE_KEY,
            "REFUND_DB": str(db),
            "HOST": "127.0.0.1",  # never a `HOST` the runner exported
            "PORT": "0",
            "PYTHONFAULTHANDLER": "1",  # a stuck app dumps its stacks on SIGABRT
        }
    )
    return env


@contextlib.contextmanager
def _agent_beside_serve(home: Path, stub_port: int):
    """Yield (serve, app port) for `irimi serve` and the app beside it, each its own process.

    Both are killed on the way out, so a failed test leaves no process or port behind."""
    served = Served(
        home,
        "serve",
        "--store",
        str(home / "store"),
        launcher=("-c", IRIMI, str(home / "maps")),
    )
    agent = None
    try:
        listener = f"http://127.0.0.1:{port_of(served.banner())}"
        env = _agent_env(listener, stub_port, home / "refunds.sqlite3")
        assert not any(name.lower() == "no_proxy" for name in env), "the trap: see the module doc"
        with open(home / "app.err", "w") as err:
            agent = subprocess.Popen(
                [sys.executable, "-c", APP],
                cwd=ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=err,
                text=True,
            )
        # The app's first line says where it listens; read on a thread, so a silent app fails.
        out = agent.stdout
        assert out is not None
        said: list[str] = []
        threading.Thread(target=lambda: said.append(out.readline()), daemon=True).start()
        try:
            eventually(lambda: bool(said))
        except AssertionError:
            # On SIGABRT faulthandler dumps every thread's stack to stderr, showing where the
            # silent app is stuck.
            agent.send_signal(signal.SIGABRT)
            agent.wait(timeout=10)
            stuck = (home / "app.err").read_text()
            raise AssertionError(f"the app said nothing in 30 s; its stacks:\n{stuck}") from None
        assert said[0].startswith("listening on 127.0.0.1:"), (home / "app.err").read_text()
        yield served, int(said[0].rsplit(":", 1)[1])
    finally:
        try:
            served.kill()
        finally:
            if agent is not None:
                agent.kill()
                agent.wait(timeout=10)
                assert agent.stdout is not None
                agent.stdout.close()


def _call(port: int, method: str, path: str, body: bytes | None = None, **headers: str):
    """Send one request straight to the app and return (status, its JSON answer)."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
    try:
        conn.request(method, path, body=body, headers=headers)
        response = conn.getresponse()
        return response.status, json.loads(response.read())
    finally:
        conn.close()


def _post_refund(port: int, charge: str) -> tuple[int, dict]:
    """Send `POST /refund` for `charge`, as a caller of the agent does."""
    body = json.dumps({"charge": charge, "reason": REASON}).encode()
    return _call(port, "POST", "/refund", body, **{"content-type": "application/json"})


def _charge_named(ex: Exchange) -> str:
    """Return the charge an exchange names: in a read's path, or in a refund's form body."""
    if ex.request.path.startswith("/v1/charges/"):
        return ex.request.path.rsplit("/", 1)[1]
    return parse_qs(ex.request.body.decode())["charge"][0]


def _ended_runs(count: int):
    """Return a `Served.wait_for` condition: `count` run blocks printed."""
    return lambda lines: sum(line.startswith("--- run ") for line in lines) == count


def test_two_concurrent_requests_to_a_served_agent_are_two_runs_that_never_mix(
    home, stripe_stub, capsys
):
    """Two concurrent requests are stored as two runs, each with its own trigger and exchanges."""
    store = home / "store"
    db = home / "refunds.sqlite3"
    with (
        _agent_beside_serve(home, stripe_stub) as (served, port),
        ThreadPoolExecutor(2) as pool,
    ):
        first = pool.submit(_post_refund, port, "ch_A")
        # The stub holds `ch_A`'s read until `ch_B` arrives, so the two runs overlap by design
        # rather than by luck.
        eventually(lambda: ("GET", "/v1/charges/ch_A") in stripe_stub.seen)
        assert not wait([first], timeout=HELD_S).done, "ch_A was answered alone"
        second = pool.submit(_post_refund, port, "ch_B")
        answers = {"ch_A": first.result(), "ch_B": second.result()}
        served.wait_for(_ended_runs(2))
        code, lines, err = served.stop("SIGTERM")
    assert code == 0, err
    # Every SDK post reached irimi: a failed one is a warning on stderr, with no logger name.
    assert (home / "app.err").read_text() == ""

    # The agent was answered: a refund id from irimi for each charge, and not the same one.
    assert {c: status for c, (status, _) in answers.items()} == {"ch_A": 200, "ch_B": 200}
    refunds = {c: body["refund"] for c, (_, body) in answers.items()}
    assert all(r.startswith("re_") for r in refunds.values()) and len(set(refunds.values())) == 2

    # Exactly two runs, both the SDK's and both ended ok, each triggered by its own request.
    reader = StoreReader(store)
    records = reader.list_runs()
    assert (
        sorted((r.attribution, cast(RunEnd, r.end).outcome) for r in records) == [("sdk", "ok")] * 2
    )
    by_charge = {}
    for record in records:
        assert record.trigger is not None
        assert (record.trigger.name, record.trigger.entrypoint) == (
            "examples.sidecar.app:handle_refund",
            "examples.sidecar.app:handle_refund",
        )
        assert record.trigger.replayable
        args = record.trigger.args
        assert isinstance(args, dict)
        charge = args["charge"]
        assert isinstance(charge, str)
        assert record.trigger.args == {"charge": charge, "reason": REASON}
        by_charge[charge] = record.run_id
    assert sorted(by_charge) == list(CHARGES)
    # And they were open at once: each started before the other ended (the stub's barrier).
    earlier, later = sorted(records, key=lambda r: r.started_at or 0.0)
    assert earlier.end is not None and later.started_at is not None
    assert later.started_at < earlier.end.ended_at

    # Each run holds only its own: its charge read, irimi's check read, its faked refund and its
    # tool's fake, and nothing naming the other charge.
    live_charge_reads = 0
    agent_reads_ended, tools_started = [], []
    for charge, run_id in by_charge.items():
        events = reader.load_run(run_id).events
        exchanges = [e for e in events if isinstance(e, Exchange)]
        assert {_charge_named(e) for e in exchanges} == {charge}, (charge, exchanges)
        other = next(c for c in CHARGES if c != charge).encode()
        for e in exchanges:
            sent = (e.request.path.encode(), e.request.body)
            assert not any(other in part for part in sent), e
            assert e.response is not None and other not in e.response.body, e
        assert sorted(
            (e.issued_by, e.request.method, e.request.path, e.answered_by) for e in exchanges
        ) == [
            ("agent", "GET", f"/v1/charges/{charge}", "live"),
            ("agent", "POST", "/v1/refunds", "fake-L1"),
            ("engine", "GET", f"/v1/charges/{charge}", "live"),
        ]
        [refund] = [e for e in exchanges if e.request.method == "POST"]
        assert refund.precondition.outcome == "passed"
        assert refund.response is not None
        assert json.loads(refund.response.body)["id"] == refunds[charge]
        [tool] = [e for e in events if isinstance(e, ToolCall)]
        assert (tool.name, tool.kind, tool.ran) == (TOOL_NAME, "write", "shadow")
        # Called once Stripe had answered the refund, with the refund's id.
        assert (tool.args, tool.result, tool.error) == (
            {"charge": charge, "refund": refunds[charge]},
            {"recorded": False},
            None,
        )
        live_charge_reads += sum(e.request.method == "GET" for e in exchanges)
        agent_reads_ended += [
            e.ended_at for e in exchanges if (e.issued_by, e.request.method) == ("agent", "GET")
        ]
        tools_started.append(tool.started_at)
    # The barrier, from the store: both charge reads were answered before either tool was called.
    assert max(agent_reads_ended) < min(tools_started)
    # Nothing was left unlabelled.
    assert reader.load_run(UNATTRIBUTED).events == []
    # The real `mark_refunded` never ran.
    assert not db.exists()

    # The stub heard only charge reads, as many as irimi stored as live, so all went through
    # irimi. The `Irimi-Run` check is for a leak: irimi strips the header.
    heard = sorted(stripe_stub.seen)
    assert heard == sorted(("GET", f"/v1/charges/{c}") for c in CHARGES for _ in range(2))
    assert len(heard) == live_charge_reads
    assert not any("irimi-run" in {k.lower() for k, _ in pairs} for pairs in stripe_stub.heard)

    # Serve printed one block per run, equal to its stored summary, naming only its own writes.
    assert sorted(serve_blocks(lines, reader)) == sorted(by_charge.values())
    maps = servicemaps.load()
    for charge, run_id in by_charge.items():
        stored = reader.load_run(run_id)
        block = summary_lines(RunView(stored.record, stored.events), maps)
        # `?`: the refund names no amount, as Stripe refunds what is left (W5 prints the same).
        assert (
            f"  ○ stripe  refund ? on {charge}  faked; checked against live state: would succeed"
            in block
        )
        assert f"  ○ tool  {TOOL_NAME}  fake ran instead" in block
        other_charge = next(c for c in CHARGES if c != charge)
        assert not any(other_charge in line for line in block), block

    # `irimi runs list` over the store: a header row, then the two runs, one line each.
    capsys.readouterr()
    assert main(["runs", "list", "--store", str(store)]) == 0
    listed = capsys.readouterr().out.splitlines()
    assert sorted(line.split()[0] for line in listed[1:]) == sorted(by_charge.values())


def test_a_refund_stripe_refuses_is_a_502_and_a_run_ended_in_error_and_a_bad_body_no_run(
    home, stripe_stub
):
    """Each failure is answered and its run says what went wrong; a bad body starts no run.

    A charge Stripe answers 404 or 500 gets a 502, and its run ends `error` holding only its read,
    the charge quoted as one path segment. A bad body is a 400 and an unknown route a 404."""
    failing = FAILING
    bad = {
        "not JSON": b"refund ch_A",
        "a list": b"[]",
        "no reason": json.dumps({"charge": "ch_A"}).encode(),
        "a number": json.dumps({"charge": 7, "reason": REASON}).encode(),
        "an empty charge": json.dumps({"charge": "", "reason": REASON}).encode(),
    }
    with _agent_beside_serve(home, stripe_stub) as (served, port):
        assert _call(port, "GET", "/healthz") == (200, {"ok": True})
        assert _call(port, "GET", "/refund")[0] == 404
        assert _call(port, "POST", "/refunds", b"{}")[0] == 404
        for what, body in bad.items():
            status, answer = _call(port, "POST", "/refund", body)
            assert status == 400 and answer["error"].startswith("expected {"), what
        # A negative length would read to the end of a connection the caller holds open.
        status, answer = _call(port, "POST", "/refund", None, **{"content-length": "-1"})
        expected = "expected {'charge': ..., 'reason': ...}: content-length -1"
        assert (status, answer["error"]) == (400, expected)
        answers = {charge: _post_refund(port, charge) for charge in failing}
        served.wait_for(_ended_runs(len(failing)))
        code, lines, err = served.stop("SIGTERM")
    assert code == 0, err
    assert (home / "app.err").read_text() == ""
    reasons = {404: "Not Found", 500: "Internal Server Error"}
    assert answers == {
        charge: (502, {"error": f"HTTPError: HTTP Error {status}: {reasons[status]}"})
        for charge, status in failing.items()
    }

    # One run per refused refund, ended in error with what was raised; none for a bad body.
    reader = StoreReader(home / "store")
    records = reader.list_runs()
    assert sorted(cast(dict, r.trigger.args)["charge"] for r in records if r.trigger) == sorted(
        failing
    )
    for record in records:
        assert record.end is not None
        assert (record.attribution, record.end.outcome) == ("sdk", "error")
        charge = cast(str, cast(dict, record.trigger.args)["charge"] if record.trigger else None)
        assert record.end.error is not None
        assert (record.end.error.type, record.end.error.message) == (
            "urllib.error.HTTPError",
            f"HTTP Error {failing[charge]}: {reasons[failing[charge]]}",
        )
        # It read the charge, was refused, and went no further: no tool call and no refund.
        [read] = reader.load_run(record.run_id).events
        assert isinstance(read, Exchange) and read.response is not None
        assert (read.issued_by, read.request.path, read.response.status) == (
            "agent",
            f"/v1/charges/{quote(charge, safe='')}",
            failing[charge],
        )
    assert reader.load_run(UNATTRIBUTED).events == []
    assert sorted(serve_blocks(lines, reader)) == sorted(r.run_id for r in records)
    assert [m for m, _ in stripe_stub.seen if m != "GET"] == []
    assert not (home / "refunds.sqlite3").exists()


def test_a_caller_that_sends_less_body_than_it_named_is_cut_off_not_waited_on(monkeypatch):
    """A request whose body never arrives times out and is closed, rather than hold its thread."""
    assert app._Handler.timeout is not None and 0 < app._Handler.timeout <= 60
    monkeypatch.setattr(app._Handler, "timeout", 0.2)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), app._Handler)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    try:
        address = cast(tuple[str, int], srv.server_address[:2])
        with socket.create_connection(address, timeout=10) as conn:
            conn.sendall(b"POST /refund HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\n{}")
            started = time.monotonic()
            assert conn.recv(1024) == b""
            assert time.monotonic() - started < 10
    finally:
        srv.shutdown()
        srv.server_close()


def test_the_app_sends_no_stripe_key_that_is_not_test_mode(monkeypatch, stripe_stub):
    """A live key is refused before anything is sent, as every example refuses one."""
    monkeypatch.delenv(ENGINE_ACTIVE_ENV, raising=False)
    monkeypatch.setenv("STRIPE_API_KEY", REFUSED_LIVE_KEY)
    monkeypatch.setenv("STRIPE_API_BASE", f"http://127.0.0.1:{stripe_stub}")
    with pytest.raises(ValueError, match=r"STRIPE_API_KEY must be a test-mode key \(sk_test_"):
        app.handle_refund("ch_A", REASON)
    assert stripe_stub.seen == []
