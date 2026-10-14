"""Phase 3's exit test, the hermetic half (#78): "a server-style agent in a container produces
attributed runs with their triggers, and two concurrent requests don't mix" (the Rough Phases
Roadmap).

Three processes, as in production: `irimi shadow --serve`, the server agent
`examples/server_agent` beside it, pointed at irimi by the variables the README's "Serve mode"
names, and a loopback Stripe on the other side. Two `POST /refund` requests at once, for `ch_A`
and `ch_B`, each held after its charge read until both have made it, so both runs are open at
once. The container half is `examples/server_agent/check.sh`, run by hand against Stripe test
mode; it is not in CI.

THE TRAP THIS TEST MUST NOT FALL INTO. The stub is on loopback, and a `NO_PROXY` naming
`127.0.0.1` or `localhost` sends `urllib` straight to it: irimi then sees nothing, and every
assertion about what irimi stored fails or, worse, the ones about what the stub never saw pass.
The agent's environment carries no `NO_PROXY` at all. And because irimi strips `Irimi-Run` from
everything it forwards, and the SDK labels only requests to irimi (#67, #75), "the stub saw no
`Irimi-Run`" proves nothing about routing: it is kept as a leak check. Routing is proved from the
store instead: each run holds its own agent-issued charge read, and the stub answered exactly as
many charge reads as irimi stored as live.

irimi runs as the real CLI in its own process, through a bootstrap that only points the shipped
maps at the loopback Stripe's (`tests.test_engine_mitm.PRECONDITION_STRIPE_MAP`, the Phase 2 exit
test's map), since a process of its own cannot be monkeypatched from here.
"""

import json
import os
import subprocess
import sys
import threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs

import pytest

from examples.server_agent.app import BARRIER_ENV, TOOL_NAME
from irimi import ca, paths, report, servicemap
from irimi.cli import main
from irimi.exchange import CONTROL_PREFIX, Exchange
from irimi.store import StoreReader
from irimi.trace import UNATTRIBUTED, ToolCall
from tests.test_engine_mitm import PRECONDITION_STRIPE_MAP
from tests.test_phase_exit import _Stripe
from tests.test_serve import _blocks, _port_of, _Served, _wait_for

ROOT = Path(__file__).resolve().parent.parent
CHARGES = ("ch_A", "ch_B")
REASON = "requested_by_customer"
# irimi as `irimi.cli.main`, with the shipped maps read from argv[1].
IRIMI = """
import sys
from pathlib import Path

from irimi import cli
from irimi.servicemap import loader

maps = Path(sys.argv[1])
loader.shipped_dir = lambda: maps
sys.exit(cli.main(sys.argv[2:]))
"""


class _HeardStripe(_Stripe):
    """The Phase 2 exit test's loopback Stripe, which answers any charge as refundable and records
    every request, also keeping each request's header names, so a leaked `Irimi-Run` shows."""

    heard: list[list[str]] = []

    def _record(self) -> bytes:
        _HeardStripe.heard.append([name.lower() for name in self.headers])
        return super()._record()


@pytest.fixture
def stripe_stub():
    _Stripe.seen = []
    _HeardStripe.heard = []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _HeardStripe)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    yield srv.server_address[1]
    srv.shutdown()
    srv.server_close()


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    monkeypatch.chdir(tmp_path)
    ca.generate_ca(ca.ca_paths())
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir()
    (maps_dir / "stripe.yaml").write_text(PRECONDITION_STRIPE_MAP)
    # This process reads the same maps when it rebuilds each run's summary (`_blocks`).
    monkeypatch.setattr("irimi.servicemap.loader.shipped_dir", lambda: maps_dir)
    return tmp_path


def _agent_env(listener: str, stub_port: int, db: Path) -> dict[str, str]:
    """The README's "Serve mode" variables, and the app's own. No `NO_PROXY`: the stub is on
    loopback, and one naming it would send the agent past irimi (see the module doc)."""
    env = {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}
    env.pop("STRIPE_API_KEY", None)
    env.pop(paths.RUN_ENV, None)
    env.update(
        {
            **dict.fromkeys(paths.PROXY_ENVS, listener),
            "SSL_CERT_FILE": str(ca.ca_paths().cert),
            "REQUESTS_CA_BUNDLE": str(ca.ca_paths().cert),
            paths.ENGINE_ACTIVE_ENV: "1",
            paths.CONTROL_ENV: listener + CONTROL_PREFIX.rstrip("/"),
            "STRIPE_API_BASE": f"http://127.0.0.1:{stub_port}",
            "STRIPE_API_KEY": "sk_test_hermetic",
            "REFUND_DB": str(db),
            BARRIER_ENV: "2",
            "PORT": "0",
        }
    )
    return env


def _refund(port: int, charge: str) -> tuple[int, dict]:
    """`POST /refund` to the app, straight, never through a proxy."""
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/refund",
        data=json.dumps({"charge": charge, "reason": REASON}).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )
    direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with direct.open(request, timeout=60) as response:
        return response.status, json.load(response)


def _charge_named(ex: Exchange) -> str:
    """The charge a stored exchange is about: a charge read names it in its path, a refund in its
    form body."""
    if ex.request.path.startswith("/v1/charges/"):
        return ex.request.path.rsplit("/", 1)[1]
    return parse_qs(ex.request.body.decode())["charge"][0]


def test_two_concurrent_requests_to_a_served_agent_are_two_runs_that_never_mix(
    home, stripe_stub, capsys
):
    """The Phase 3 exit criterion for use case 0 (#78)."""
    store = home / "store"
    db = home / "refunds.sqlite3"
    served = _Served(
        home,
        "shadow",
        "--serve",
        "--store",
        str(store),
        launcher=("-c", IRIMI, str(home / "maps")),
    )
    app = None
    try:
        listener = f"http://127.0.0.1:{_port_of(served.banner())}"
        with open(home / "app.err", "w") as err:
            app = subprocess.Popen(
                [sys.executable, "-m", "examples.server_agent"],
                cwd=ROOT,
                env=_agent_env(listener, stripe_stub, db),
                stdout=subprocess.PIPE,
                stderr=err,
                text=True,
            )
        # The app's first line says where it listens; read on a thread, so a silent app fails.
        assert app.stdout is not None
        said: list[str] = []
        threading.Thread(target=lambda: said.append(app.stdout.readline()), daemon=True).start()
        _wait_for(lambda: bool(said))
        assert said[0].startswith("listening on 127.0.0.1:"), (home / "app.err").read_text()
        port = int(said[0].rsplit(":", 1)[1])
        with ThreadPoolExecutor(2) as pool:
            answers = dict(zip(CHARGES, pool.map(lambda c: _refund(port, c), CHARGES), strict=True))
        # Each run's block is printed once its end is on disk, while serve runs on.
        served.wait_for(lambda lines: sum(line.startswith("--- run ") for line in lines) == 2)
        code, lines, err = served.stop("SIGTERM")
    finally:
        served.kill()
        if app is not None:
            app.kill()
            app.wait(timeout=10)
            assert app.stdout is not None
            app.stdout.close()
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
    assert sorted((r.attribution, r.outcome) for r in records) == [("sdk", "ok")] * 2
    by_charge = {}
    for record in records:
        assert record.trigger is not None
        assert (record.trigger.name, record.trigger.entrypoint) == (
            "handle_refund",
            "examples.server_agent.app:handle_refund",
        )
        assert record.trigger.replayable
        charge = record.trigger.args["charge"]
        assert record.trigger.args == {"charge": charge, "reason": REASON}
        by_charge[charge] = record.run_id
    assert sorted(by_charge) == list(CHARGES)
    # And they were open at once: each started before the other ended (the app's barrier).
    first, second = sorted(records, key=lambda r: r.started_at or 0.0)
    assert first.ended_at is not None and second.started_at is not None
    assert second.started_at < first.ended_at

    # Each run holds exactly its own: its read of its charge, irimi's L3 read of it, its faked
    # refund, and its stood-in tool call, and nothing that names the other charge.
    live_charge_reads = 0
    for charge, run_id in by_charge.items():
        events = reader.load_run(run_id).events
        exchanges = [e for e in events if isinstance(e, Exchange)]
        assert {_charge_named(e) for e in exchanges} == {charge}, (charge, exchanges)
        assert sorted(
            (e.issued_by, e.request.method, e.request.path, e.answered_by) for e in exchanges
        ) == [
            ("agent", "GET", f"/v1/charges/{charge}", "live"),
            ("agent", "POST", "/v1/refunds", "fake-L1"),
            ("engine", "GET", f"/v1/charges/{charge}", "live"),
        ]
        [refund] = [e for e in exchanges if e.request.method == "POST"]
        assert refund.precondition == "passed"
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
    # Nothing was left unlabelled.
    assert reader.load_run(UNATTRIBUTED).events == []
    # The real `mark_refunded` never ran.
    assert not db.exists()

    # The stub never heard a write, nor a run id: and it answered exactly the charge reads irimi
    # stored as live, so every one went through irimi.
    assert [m for m, _, _ in _Stripe.seen if m != "GET"] == []
    assert not any("irimi-run" in names for names in _HeardStripe.heard)
    charge_reads = [p for m, p, _ in _Stripe.seen if p.startswith("/v1/charges/")]
    assert sorted(charge_reads) == sorted(f"/v1/charges/{c}" for c in CHARGES for _ in range(2))
    assert len(charge_reads) == live_charge_reads

    # Serve printed one block per run, each equal to its stored summary (`_blocks`), naming its
    # own refund and its stood-in tool, and not the other run's.
    assert sorted(_blocks(lines, reader)) == sorted(by_charge.values())
    maps = servicemap.load()
    for charge, run_id in by_charge.items():
        block = report.stored_summary_lines(reader.load_run(run_id), maps)
        # `?`: the refund names no amount, as Stripe refunds what is left (W5 prints the same).
        assert f"  ○ refund ? on {charge}  unvalidated (L3 preconditions passed)" in block
        assert f"  ○ tool {TOOL_NAME}  shadow stand-in ran" in block
        other = next(c for c in CHARGES if c != charge)
        assert not any(other in line for line in block), block

    # `irimi runs list` over the store: the two runs, one line each.
    capsys.readouterr()
    assert main(["runs", "list", "--store", str(store)]) == 0
    listed = capsys.readouterr().out.splitlines()
    assert sorted(line.split("  ")[0] for line in listed) == sorted(by_charge.values())
