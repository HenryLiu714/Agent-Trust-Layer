"""`irimi replay` and `sdk.replay()` end to end (#84): a run recorded under the real `irimi shadow`
into a store, then replayed by the real `irimi replay` against the same Stripe-shaped stub and LLM
stub on loopback, each of which writes down every request it is given, so a test can prove what a
replay never sent anywhere.

The agent is a module this file writes into the test's working directory, `replay_agent`, started
by its name (`python -c "import replay_agent; ..."`) so its trigger's entrypoint is
`replay_agent:handle`, which `irimi.sdk.replay_child` imports, never `__main__:handle`. It sends
its calls through the forward proxy explicitly, because `irimi shadow` exempts loopback from it
through `NO_PROXY` and the stubs are on loopback. The maps are `tests/test_replay_engine.py`'s.
"""

import json
import sys
from dataclasses import replace

import pytest

from irimi import ca, paths
from irimi.cli import main
from irimi.sdk.replays import ReplayResult
from irimi.servicemap import loader
from irimi.store import StoreReader
from irimi.trace import ErrorInfo, ToolCall
from tests import test_replay_engine
from tests.test_replay_engine import LLM_MAP, STRIPE_MAP, _Llm, _Stripe

stubs = test_replay_engine.stubs

AGENT = '''
"""The agent `tests/test_replay_cli.py` records and replays (#84)."""
import http.client, json, os, urllib.parse
from dataclasses import dataclass

from irimi import sdk

STRIPE = f"http://127.0.0.1:{os.environ['STUB_STRIPE']}"
LLM = f"http://localhost:{os.environ['STUB_LLM']}"


@dataclass(frozen=True)
class Ticket:
    charge: str
    note: str


def call(method, url, body=None, content_type="application/json"):
    door = urllib.parse.urlparse(os.environ["HTTPS_PROXY"])
    conn = http.client.HTTPConnection(door.hostname, door.port, timeout=10)
    headers = {"host": url.split("/")[2]}
    if body is not None:
        headers["content-type"] = content_type
    conn.request(method, url, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def refund(ticket, amount):
    call("GET", f"{STRIPE}/v1/charges/{ticket.charge}")
    form = f"charge={ticket.charge}&amount={amount}"
    return call("POST", f"{STRIPE}/v1/refunds", form, "application/x-www-form-urlencoded")[0]


@sdk.trigger
def handle(ticket: Ticket) -> int:
    """Ask the model how much to refund, then refund it: `PROMPT` is the prompt edit."""
    system = os.environ.get("PROMPT", "Refund in full.")
    ask = {"model": "m", "system": system, "messages": [{"role": "user", "content": ticket.note}]}
    _, body = call("POST", f"{LLM}/v1/messages", json.dumps(ask))
    text = json.loads(body)["content"][0]["text"]
    return refund(ticket, 2450 if "half" in text else 4900)


@sdk.trigger
def refund_half(ticket: Ticket) -> int:
    return refund(ticket, 2450)


@sdk.trigger
def handle_with(ticket: Ticket, notify) -> int:
    """A trigger given a callable, which capture stores by its repr."""
    status = refund(ticket, 4900)
    if notify is not None:
        notify(status)
    return status


def plain(charge):
    """No trigger at all: the process run is the run."""
    return refund(Ticket(charge, "-"), 4900)


@sdk.tool(kind="read")
def lookup(customer: str) -> dict:
    return {"id": customer, "tier": os.environ.get("TIER", "gold")}


def _reply_stand_in(body: str) -> dict:
    return {"sent": True, "stood_in": True}


@sdk.tool(kind="write", shadow=_reply_stand_in)
def reply(body: str) -> dict:
    raise AssertionError("a write tool never runs for real under irimi")


class Desk:
    @sdk.trigger
    def handle(self, ticket: Ticket) -> int:
        return refund(ticket, 4900)


@sdk.trigger
def fails(ticket: Ticket) -> None:
    refund(ticket, 4900)
    raise KeyError(ticket.charge)


@sdk.tool(kind="read")
def charge_of(ticket: Ticket) -> int:
    """A read tool whose body makes HTTP of its own, which a replay that answers it never makes."""
    return call("GET", f"{STRIPE}/v1/charges/{ticket.charge}")[0]


@sdk.trigger
def checks(ticket: Ticket) -> int:
    return charge_of(ticket)


@sdk.trigger
def waits(ticket: Ticket) -> str:
    """Replayed with `WAIT_FOR` set, it says it started and waits, for a signal to stop it."""
    lookup(ticket.note)
    marker = os.environ.get("WAIT_FOR")
    if marker:
        open(marker, "w").close()
        import time

        time.sleep(60)
    return "done"


@sdk.trigger
def answer(ticket: Ticket) -> str:
    """Tools only, no HTTP: a read tool the replay answers from its recording, and a write tool."""
    tier = lookup(ticket.note)["tier"]
    reply(f"{ticket.charge}: {tier}")
    return tier
'''
TICKET = "replay_agent.Ticket('ch_REAL1', 'refund please')"


@pytest.fixture
def home(tmp_path, monkeypatch, stubs):
    """`$IRIMI_HOME` with a CA, the test maps in place of the shipped ones, the agent module in the
    working directory, and the stubs' ports in the environment the agent reads them from."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "home"))
    ca.generate_ca(ca.ca_paths())
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir()
    (maps_dir / "stripe.yaml").write_text(STRIPE_MAP)
    (maps_dir / "anthropic.yaml").write_text(LLM_MAP)
    monkeypatch.setattr(loader, "shipped_dir", lambda: maps_dir)
    (tmp_path / "replay_agent.py").write_text(AGENT)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("STUB_STRIPE", str(stubs[0]))
    monkeypatch.setenv("STUB_LLM", str(stubs[1]))
    monkeypatch.delenv("PROMPT", raising=False)
    return tmp_path / "home"


def _record(home, code: str) -> str:
    """Run `code` under `irimi shadow`, after `import replay_agent`, and return the one run it
    stored besides the process run: the SDK's, or the process run when there is none. Forgets every
    request the stubs saw while it ran."""
    before = {r.run_id for r in StoreReader(home / "store").list_runs()}
    command = [sys.executable, "-c", f"import replay_agent, sys; {code}"]
    assert main(["shadow", "--port", "0", "--", *command]) == 0
    runs = [r for r in StoreReader(home / "store").list_runs() if r.run_id not in before]
    _Stripe.seen, _Llm.seen = [], []
    sdk_runs = [r.run_id for r in runs if r.attribution == "sdk"]
    if sdk_runs:
        (run_id,) = sdk_runs
        return run_id
    (run_id,) = [r.run_id for r in runs if r.attribution == "process"]
    return run_id


def _replay(capfd, *argv: str) -> tuple[int, list[str], list[str]]:
    """`irimi replay <argv>`: its exit code, and what it printed to stdout and stderr."""
    code = main(["replay", *argv[:1], "--port", "0", *argv[1:]])
    out, err = capfd.readouterr()
    return code, out.splitlines(), err.splitlines()


def _result(lines: list[str]) -> list[str]:
    """The result block: from `irimi replay · ` on, the per-exchange stream left out."""
    start = next(
        i for i, line in enumerate(lines) if line.startswith("irimi replay · ") and "→" in line
    )
    return lines[start:]


def _replay_runs(home, source: str) -> list:
    return [r for r in StoreReader(home / "store").list_runs() if r.replay_of == source]


def test_an_unchanged_agent_replays_with_no_divergence_and_sends_nothing(home, capfd):
    source = _record(home, f"replay_agent.handle({TICKET})")
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert (code, err) == (0, [])
    [replay] = _replay_runs(home, source)
    assert (replay.mode, replay.attribution, replay.exit_code) == ("replay", "process", 0)
    assert out[:2] == [
        f"irimi replay · run {replay.run_id} · listening on " + out[0].split(" · listening on ")[1],
        f"replaying {source} as {replay.run_id}",
    ]
    assert _result(out) == [
        f"irimi replay · {source} → {replay.run_id} · - → -",
        "  agent exited 0",
        "  no divergence: every call was answered from the recording",
        "  3 from the recording · 0 missed",
        "",
        "  = ○ refund $49.00 on ch_REAL1  unvalidated (L3 preconditions passed)",
        "",
        "  1 same · 0 changed · 0 only in A · 0 only in B",
    ]
    assert _Stripe.seen == [] and _Llm.seen == []
    # The replay is one run: the SDK's trigger adopted it, so it holds every event (#84).
    events = StoreReader(home / "store").load_run(replay.run_id).events
    assert [e.request.path for e in events] == [
        "/v1/messages",
        "/v1/charges/ch_REAL1",
        "/v1/refunds",
    ]
    assert replay.trigger == StoreReader(home / "store").load_run(source).record.trigger


def test_a_changed_prompt_diverges_at_the_llm_call_and_refunds_another_amount(
    home, capfd, monkeypatch
):
    source = _record(home, f"replay_agent.handle({TICKET})")
    capfd.readouterr()
    monkeypatch.setenv("PROMPT", "Refund half of it.")
    code, out, err = _replay(capfd, source)
    assert (code, err) == (1, [])
    [replay] = _replay_runs(home, source)
    assert _result(out) == [
        f"irimi replay · {source} → {replay.run_id} · - → -",
        "  agent exited 0",
        "  diverged at #1: live      llm       POST localhost/v1/messages -> 200  [replay-miss]",
        "  1 from the recording · 2 missed",
        "",
        "  ~ A ○ refund $49.00 on ch_REAL1  unvalidated (L3 preconditions passed)",
        "    B ○ refund $24.50 on ch_REAL1  unvalidated (L3 preconditions passed)",
        "",
        "  0 same · 1 changed · 0 only in A · 0 only in B",
    ]
    assert len(_Llm.seen) == 1
    assert [m for m, _ in _Stripe.seen if m == "POST"] == []


def test_a_process_run_with_no_trigger_replays_by_running_its_argv_again(home, capfd):
    source = _record(home, "sys.exit(0 if replay_agent.plain('ch_REAL1') == 200 else 1)")
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert (code, err) == (0, [])
    [replay] = _replay_runs(home, source)
    recorded = StoreReader(home / "store").load_run(source).record
    assert replay.trigger == recorded.trigger
    assert "  no divergence: every call was answered from the recording" in out
    assert _Stripe.seen == []


def test_call_and_args_override_the_recorded_entrypoint_and_args(home, capfd):
    source = _record(home, f"replay_agent.handle({TICKET})")
    capfd.readouterr()
    ticket = {
        "ticket": {
            "__irimi_type__": "replay_agent:Ticket",
            "value": {"charge": "ch_REAL1", "note": "refund please"},
        }
    }
    code, out, err = _replay(
        capfd, source, "--call", "replay_agent:refund_half", "--args", json.dumps(ticket)
    )
    assert (code, err) == (1, [])
    [replay] = _replay_runs(home, source)
    assert (replay.trigger.name, replay.trigger.entrypoint) == (
        "handle",
        "replay_agent:refund_half",
    )
    result = _result(out)
    assert result[2:4] == [
        "  diverged at #3: fake-L1   write     POST 127.0.0.1/v1/refunds -> 200  "
        "[fidelity:L1, replay-miss]",
        "  1 from the recording · 1 missed",
    ]
    assert "    B ○ refund $24.50 on ch_REAL1  unvalidated (L3 preconditions passed)" in result
    # Other args: the model is asked something else, so its call misses, and the header says so.
    other = {"ticket": {**ticket["ticket"], "value": {"charge": "ch_REAL1", "note": "please"}}}
    code, out, err = _replay(capfd, source, "--args", json.dumps(other))
    assert (code, err) == (0, [])
    assert _result(out)[2] == "  (the triggers' args differ)"
    assert _result(out)[3].startswith("  diverged at #1: live      llm ")


def test_args_stored_by_their_description_are_refused_and_given_with_args_run(home, capfd):
    source = _record(home, f"replay_agent.handle_with({TICKET}, print)")
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert (code, out) == (2, [])
    assert err == [
        f"error: run {source}'s trigger arguments were not recorded replayably (a value was "
        "recorded as its description); pass --args"
    ]
    assert _replay_runs(home, source) == []
    recorded = StoreReader(home / "store").load_run(source).record.trigger
    args = {**recorded.args, "notify": None}
    code, out, err = _replay(capfd, source, "--args", json.dumps(args))
    assert (code, err) == (0, [])
    assert "  no divergence: every call was answered from the recording" in out


def test_a_run_with_no_entrypoint_and_no_argv_is_refused(home, capfd):
    source = _record(
        home, 'exec(\'with replay_agent.sdk.run(name="block"): replay_agent.plain("ch_REAL1")\')'
    )
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert (code, out) == (2, [])
    assert err == [f"error: run {source} has no entrypoint and no argv; pass --call or -- CMD"]


def test_sdk_replay_returns_the_result_the_cli_writes(home, capfd, monkeypatch, tmp_path):
    """`sdk.replay(run_id)` and `sdk.replay(run_id, fn, *args)`, from this test's own process, run
    `irimi replay` as a subprocess and return the result file it writes, equal to the CLI's but for
    the replay's new run id. The agent is tools-only: the maps `irimi replay` loads in a subprocess
    are the shipped ones, which the stubs are not on, and a replay of tools needs none."""
    from irimi import sdk

    source = _record(home, f"replay_agent.answer({TICKET})")
    capfd.readouterr()
    result_file = tmp_path / "result.json"
    code, out, _ = _replay(capfd, source, "--result-file", str(result_file))
    by_cli = ReplayResult.from_json(json.loads(result_file.read_text()))
    assert code == by_cli.exit_code == 0
    assert by_cli.lines == tuple(_result(out))
    assert by_cli.diverged_at is None
    monkeypatch.syspath_prepend(str(tmp_path))
    import replay_agent

    for result in (
        sdk.replay(source),
        sdk.replay(source, replay_agent.answer, replay_agent.Ticket("ch_REAL1", "refund please")),
    ):
        capfd.readouterr()
        assert result.replay_run_id not in (None, by_cli.replay_run_id)
        same = [line.replace(result.replay_run_id, by_cli.replay_run_id) for line in result.lines]
        assert replace(result, replay_run_id=by_cli.replay_run_id, lines=tuple(same)) == by_cli
    tools = [
        (e.name, e.ran)
        for r in _replay_runs(home, source)
        for e in StoreReader(home / "store").load_run(r.run_id).events
        if isinstance(e, ToolCall)
    ]
    assert tools == [("replay_agent.lookup", "recording"), ("replay_agent.reply", "shadow")] * 3
    # Args far past what one command-line argument may hold go in a file, never on the line.
    big = sdk.replay(source, replay_agent.answer, replay_agent.Ticket("ch_REAL1", "x" * 300_000))
    capfd.readouterr()
    assert (big.exit_code, big.lines[2]) == (0, "  (the triggers' args differ)")


def test_sdk_replay_of_a_run_the_store_does_not_hold_is_a_result_that_exits_2(home):
    from irimi import sdk

    result = sdk.replay("nosuchrun")
    assert result == ReplayResult(
        "nosuchrun",
        None,
        2,
        None,
        (f"error: no run nosuchrun in {home / 'store'}",),
    )


def test_a_trigger_it_cannot_call_is_a_replay_that_could_not_run(home, capfd):
    """A method's run, whose instance a run does not record, and args whose class does not revive:
    the child cannot call either, and the replay says so and exits 2, never an agent that ran and
    differed (#84)."""
    source = _record(home, f"replay_agent.Desk().handle({TICKET})")
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert code == 2
    assert err == [
        "error: cannot call replay_agent:Desk.handle: it is a method, and a run does not record "
        "the instance it was called on"
    ]
    assert _result(out)[1] == (
        "  agent could not be called: it is a method, and a run does not record the instance it "
        "was called on"
    )
    [replay] = _replay_runs(home, source)
    assert (replay.outcome, replay.exit_code, replay.error.type) == ("error", 126, "cannot-call")
    other = _record(home, f"replay_agent.handle({TICKET})")
    missing = {"ticket": {"__irimi_type__": "replay_agent:NoSuchTicket", "value": {}}}
    capfd.readouterr()
    code, out, _ = _replay(capfd, other, "--args", json.dumps(missing))
    assert code == 2
    assert _result(out)[1] == (
        "  agent could not be called: its args do not revive: replay_agent:NoSuchTicket is not in "
        "its module"
    )


def test_a_trigger_recorded_in_main_is_refused(home, capfd):
    """An agent run as `python agent.py` records `__main__:handle`, and `__main__` is the child's
    own module when it is called (#89): refused, exit 2, before anything runs."""
    (home.parent / "script_agent.py").write_text(
        "import replay_agent\nfrom irimi import sdk\n\n@sdk.trigger\ndef main():\n    "
        "replay_agent.plain('ch_REAL1')\n\nmain()\n"
    )
    assert main(["shadow", "--port", "0", "--", sys.executable, "script_agent.py"]) == 0
    [run] = [r for r in StoreReader(home / "store").list_runs() if r.attribution == "sdk"]
    capfd.readouterr()
    code, out, err = _replay(capfd, run.run_id)
    assert (code, out) == (2, [])
    assert err == [
        f"error: run {run.run_id}'s entrypoint __main__:main is in __main__, which is irimi's own "
        "child when it is called; start the agent by its module name, or pass --call or -- CMD"
    ]


def test_a_replay_that_ends_on_what_its_trigger_raised_records_it_as_its_recording_did(home, capfd):
    """The adopted run posts no end, so the child says what the call raised and `irimi replay`
    ends the run with it: `builtins.KeyError`, as the recorded run ended (#83, #84)."""
    command = [sys.executable, "-c", f"import replay_agent; replay_agent.fails({TICKET})"]
    assert main(["shadow", "--port", "0", "--", *command]) == 1
    [source] = [r for r in StoreReader(home / "store").list_runs() if r.attribution == "sdk"]
    capfd.readouterr()
    code, out, _ = _replay(capfd, source.run_id)
    assert code == 0
    assert _result(out)[1] == "  agent exited 1"
    [replay] = _replay_runs(home, source.run_id)
    assert (replay.outcome, replay.exit_code) == ("error", 1)
    assert replay.error == source.error == ErrorInfo("builtins.KeyError", "'ch_REAL1'")


def test_args_from_a_file_and_a_command_with_a_call_is_a_usage_error(home, capfd, tmp_path):
    source = _record(home, f"replay_agent.handle({TICKET})")
    capfd.readouterr()
    args = tmp_path / "args.json"
    args.write_text(json.dumps(StoreReader(home / "store").load_run(source).record.trigger.args))
    code, out, err = _replay(capfd, source, "--args-file", str(args))
    assert (code, err) == (0, [])
    assert "  no divergence: every call was answered from the recording" in out
    with pytest.raises(SystemExit) as usage:
        main(["replay", source, "--call", "replay_agent:handle", "--", "true"])
    assert usage.value.code == 2
    assert "-- CMD runs a command, not a call" in capfd.readouterr().err


def test_a_refused_replay_s_result_file_holds_the_reason_irimi_gave(home, capfd, tmp_path):
    """`sdk.replay`'s caller gets the reason, not only the one who read stderr (#84)."""
    source = _record(home, f"replay_agent.handle({TICKET})")
    for name in ("ca.pem", "ca.key"):
        (home / "ca" / name).unlink()
    capfd.readouterr()
    written = tmp_path / "result.json"
    code, out, err = _replay(capfd, source, "--result-file", str(written))
    assert (code, out) == (2, [])
    result = ReplayResult.from_json(json.loads(written.read_text()))
    assert result == ReplayResult(source, None, 2, None, tuple(err))
    assert err == [f"error: no CA at {home / 'ca'}. Run `irimi init` first."]


def test_a_replay_that_called_a_trigger_can_itself_be_replayed(home, capfd):
    """A replay's run is a process run that keeps the trigger it replayed, entrypoint and args, so
    replaying it calls that trigger again, as the issue's "Decided" says (#84)."""
    source = _record(home, f"replay_agent.handle({TICKET})")
    capfd.readouterr()
    assert _replay(capfd, source)[0] == 0
    [first] = _replay_runs(home, source)
    code, out, err = _replay(capfd, first.run_id)
    assert (code, err) == (0, [])
    assert "  no divergence: every call was answered from the recording" in out
    [second] = _replay_runs(home, first.run_id)
    assert second.trigger == first.trigger


def test_args_that_do_not_bind_and_a_module_that_exits_as_it_loads_cannot_be_called(home, capfd):
    """Never an agent that ran and raised (#84): args with a parameter the function does not take,
    and a module that exits as it is imported, are calls the child could not make: exit 2."""
    source = _record(home, f"replay_agent.handle({TICKET})")
    capfd.readouterr()
    code, out, _ = _replay(capfd, source, "--args", json.dumps({"tikket": 1}))
    assert code == 2
    assert _result(out)[1].startswith("  agent could not be called: its args do not bind: ")
    (home.parent / "exits_agent.py").write_text("import sys\nsys.exit(4)\n")
    code, out, _ = _replay(capfd, source, "--call", "exits_agent:handle", "--args", "{}")
    assert code == 2
    assert _result(out)[1] == (
        "  agent could not be called: importing exits_agent raised SystemExit: 4"
    )


def test_a_read_tool_answered_with_the_http_its_body_made_is_no_divergence(home, capfd):
    """The tool's GET was recorded within its span, its body's: a replay that answers the tool
    never makes it, and that is no call the replay failed to make (#83, #84)."""
    source = _record(home, f"replay_agent.checks({TICKET})")
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert (code, err) == (0, [])
    assert _result(out)[2:4] == [
        "  no divergence: every call was answered from the recording",
        "  1 from the recording · 0 missed",
    ]
    assert _Stripe.seen == []


def test_a_replay_sigterm_stops_while_its_agent_runs_exits_143_and_says_so(home, capfd, tmp_path):
    """SIGTERM to `irimi replay`, as a supervisor sends it, is passed to the agent (#77), and the
    replay is one it stopped: exit 143, said in its result, never a replay that came out the same
    (#84). `irimi replay` runs as its own process here, so the signal reaches it alone."""
    import os
    import signal
    import subprocess
    import time

    source = _record(home, f"replay_agent.waits({TICKET})")
    capfd.readouterr()
    marker, written = tmp_path / "started", tmp_path / "result.json"
    command = [sys.executable, "-m", "irimi", "replay", "--port", "0"]
    command += ["--result-file", str(written), "--", source]
    proc = subprocess.Popen(
        command,
        env={**os.environ, "WAIT_FOR": str(marker), "PYTHONPATH": str(tmp_path)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 30
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert marker.exists()
    proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=30) == 143
    result = ReplayResult.from_json(json.loads(written.read_text()))
    assert result.exit_code == 143
    assert "  stopped: irimi replay was interrupted before the agent ended" in result.lines


def test_a_replay_interrupted_before_its_run_is_stored_exits_130_with_no_run(
    home, capfd, monkeypatch, tmp_path
):
    """A Ctrl-C while the replay's engine starts, before its run is stored: no agent ran and no
    run exists, so the result names none, and the replay exits 130 as `irimi shadow` would."""
    from irimi import cli

    source = _record(home, f"replay_agent.handle({TICKET})")

    def interrupted(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "print_startup", interrupted)
    written = tmp_path / "result.json"
    capfd.readouterr()
    code, _, _ = _replay(capfd, source, "--result-file", str(written))
    assert code == 130
    result = ReplayResult.from_json(json.loads(written.read_text()))
    assert (result.replay_run_id, result.exit_code) == (None, 130)
    assert _replay_runs(home, source) == []
