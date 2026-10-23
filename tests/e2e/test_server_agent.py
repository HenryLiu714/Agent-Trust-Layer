"""The server agent beside `irimi serve`, end to end: two concurrent `POST /refund`s are two
attributed runs that never mix. The agent's environment holds no `NO_PROXY`, for the reason
`tests/support/scenarios.py` gives, and routing is proved from the store: the stub answered as
many charge reads as irimi stored as live."""

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
from concurrent.futures import ThreadPoolExecutor, wait
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import cast
from urllib.parse import parse_qs, quote

import pytest

from examples.server_agent import app
from examples.server_agent.app import BARRIER_ENV, TOOL_NAME
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
from tests.support.credentials import HERMETIC_STRIPE_KEY
from tests.support.engine import Served, port_of, serve_blocks
from tests.support.maps import PRECONDITION_STRIPE_MAP
from tests.support.scenarios import base_url_stripe
from tests.support.upstreams import Answer, FakeStripe, serve
from tests.support.waiting import eventually

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[2]
CHARGES = ("ch_A", "ch_B")
REASON = "requested_by_customer"
# How long `ch_A` must stay unanswered, alone at the barrier, before `ch_B` is sent. Without the
# barrier it is answered in milliseconds; with it, not before `ch_B` arrives.
HELD_S = 0.5
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


# Three charges the Stripe answers with an error instead, as Stripe answers ones it never heard
# of (404) and one it fails on (500), each named as the agent is given it. Each is matched by the
# path segment the agent sent, so a charge holding `/` and `?` fails only when the agent quoted it
# into its one segment: unquoted, it names another Stripe path, which answers 200.
FAILING = {"ch_MISSING": 404, "ch_DOWN": 500, "ch_ODD/../refunds?x=1": 404}


@pytest.fixture
def stripe_stub():
    """The refund agent test's Stripe, which answers any charge as refundable, but `FAILING`'s
    with their errors. `stripe_stub.heard` keeps each request's headers, so a leaked `Irimi-Run`
    shows."""
    failing = {
        f"/v1/charges/{quote(charge, safe='')}": Answer(
            status, {"error": {"type": "invalid_request_error"}}
        )
        for charge, status in FAILING.items()
    }
    port = serve(FakeStripe, threaded=True, **base_url_stripe(), answers=failing)
    yield port
    port.stop()


@pytest.fixture
def home(home, monkeypatch, home_with_maps):
    """The shared `home`, also the working directory, with the loopback Stripe's map in place of
    the shipped one: this process reads the same maps when it rebuilds each run's summary."""
    monkeypatch.chdir(home)
    home_with_maps({"stripe.yaml": PRECONDITION_STRIPE_MAP})
    return home


def _agent_env(listener: str, stub_port: int, db: Path, *, barrier: bool) -> dict[str, str]:
    """The README's "Serve mode" variables, and the app's own. No `NO_PROXY`: the stub is on
    loopback, and one naming it would send the agent past irimi (see the module doc)."""
    env = {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}
    for name in ("STRIPE_API_KEY", RUN_ENV, BARRIER_ENV):
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
            **({BARRIER_ENV: "2"} if barrier else {}),
        }
    )
    return env


@contextlib.contextmanager
def _agent_beside_serve(home: Path, stub_port: int, *, barrier: bool):
    """`irimi serve --store <home>/store`, and the app beside it as a process of its own,
    pointed at it; yields (the serve, the app's port). Both are killed on the way out, whatever
    failed, so a failed test leaves no process and no port behind."""
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
        env = _agent_env(listener, stub_port, home / "refunds.sqlite3", barrier=barrier)
        assert not any(name.lower() == "no_proxy" for name in env), "the trap: see the module doc"
        with open(home / "app.err", "w") as err:
            agent = subprocess.Popen(
                [sys.executable, "-m", "examples.server_agent.app"],
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
            # Say where a silent app is stuck: faulthandler dumps every thread's stack to its
            # stderr on SIGABRT, which shows where a slow runner holds it past the deadline.
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
    """One request to the app, straight, never through a proxy: (status, its JSON answer)."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
    try:
        conn.request(method, path, body=body, headers=headers)
        response = conn.getresponse()
        return response.status, json.loads(response.read())
    finally:
        conn.close()


def _post_refund(port: int, charge: str) -> tuple[int, dict]:
    """`POST /refund` for `charge`, as a caller of the agent sends it."""
    body = json.dumps({"charge": charge, "reason": REASON}).encode()
    return _call(port, "POST", "/refund", body, **{"content-type": "application/json"})


def _charge_named(ex: Exchange) -> str:
    """The charge a stored exchange is about: a charge read names it in its path, a refund in its
    form body."""
    if ex.request.path.startswith("/v1/charges/"):
        return ex.request.path.rsplit("/", 1)[1]
    return parse_qs(ex.request.body.decode())["charge"][0]


def _ended_runs(count: int):
    """A `_Served.wait_for` condition: `count` runs' blocks printed, each once its end is stored."""
    return lambda lines: sum(line.startswith("--- run ") for line in lines) == count


def test_two_concurrent_requests_to_a_served_agent_are_two_runs_that_never_mix(
    home, stripe_stub, capsys
):
    """A served agent's two concurrent requests are stored as two runs, each with its own
    trigger and its own exchanges."""
    store = home / "store"
    db = home / "refunds.sqlite3"
    with (
        _agent_beside_serve(home, stripe_stub, barrier=True) as (served, port),
        ThreadPoolExecutor(2) as pool,
    ):
        first = pool.submit(_post_refund, port, "ch_A")
        # `ch_A` is inside its run once Stripe has heard its read. The barrier holds it there
        # until `ch_B` arrives: without it, `ch_A` would be answered alone, and the two runs
        # would be open at once only by luck.
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
            "examples.server_agent.app:handle_refund",
            "examples.server_agent.app:handle_refund",
        )
        assert record.trigger.replayable
        args = record.trigger.args
        assert isinstance(args, dict)
        charge = args["charge"]
        assert isinstance(charge, str)
        assert record.trigger.args == {"charge": charge, "reason": REASON}
        by_charge[charge] = record.run_id
    assert sorted(by_charge) == list(CHARGES)
    # And they were open at once: each started before the other ended (the app's barrier).
    earlier, later = sorted(records, key=lambda r: r.started_at or 0.0)
    assert earlier.end is not None and later.started_at is not None
    assert later.started_at < earlier.end.ended_at

    # Each run holds exactly its own: its read of its charge, irimi's L3 read of it, its faked
    # refund, and its stood-in tool call, and nothing that names the other charge.
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
        assert (tool.args, tool.result, tool.error) == (
            {"charge": charge},
            {"stood_in": True},
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

    # The stub heard the charge reads and nothing else, no write and no run id; and as many as
    # irimi stored as live, so every one went through irimi. (No `Irimi-Run` is a leak check
    # only: irimi strips it, and the SDK never sets it on a request that bypasses irimi.)
    heard = sorted(stripe_stub.seen)
    assert heard == sorted(("GET", f"/v1/charges/{c}") for c in CHARGES for _ in range(2))
    assert len(heard) == live_charge_reads
    assert not any("irimi-run" in {k.lower() for k, _ in pairs} for pairs in stripe_stub.heard)

    # Serve printed one block per run, each equal to its stored summary (`serve_blocks`), naming its
    # own refund and its stood-in tool, and not the other run's.
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
    """The agent answers every request, never hangs one, and its runs say what went wrong: a
    charge Stripe has never heard of (404) or fails on (500) is answered 502 with the error, and
    its run ends `error` holding only its read, which names the charge as one quoted path segment
    whatever it holds; a body that is not `{"charge", "reason"}` is answered 400 before any run
    starts, and an unknown route 404."""
    failing = FAILING
    bad = {
        "not JSON": b"refund ch_A",
        "a list": b"[]",
        "no reason": json.dumps({"charge": "ch_A"}).encode(),
        "a number": json.dumps({"charge": 7, "reason": REASON}).encode(),
        "an empty charge": json.dumps({"charge": "", "reason": REASON}).encode(),
    }
    with _agent_beside_serve(home, stripe_stub, barrier=False) as (served, port):
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
    """A request whose body never arrives does not hold its thread forever: the handler's
    socket times out and the connection is closed. Here in-process, with the timeout shortened."""
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
