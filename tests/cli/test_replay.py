"""`irimi replay` and `testing.replay()`: runs recorded by the real `irimi shadow`, replayed.

The agent names the proxy explicitly, for the `NO_PROXY` reason `support/scenarios.py` gives."""

import json
import re
import shlex
import sys
from dataclasses import replace

import pytest

from irimi import __version__, cli
from irimi.cli import main
from irimi.config import ca, paths
from irimi.core.exchange import Exchange
from irimi.protocol.replay_files import ReplayResult, replay_result_from_json
from irimi.protocol.trace import ErrorInfo, ToolCall
from irimi.record.store.reader import StoreReader
from tests.support.maps import REPLAY_LLM_MAP, REPLAY_STRIPE_MAP
from tests.support.waiting import eventually

AGENT = '''
"""The agent `tests/cli/test_replay.py` records and replays (#84)."""
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


@sdk.read_tool
def lookup(customer: str) -> dict:
    return {"id": customer, "tier": os.environ.get("TIER", "gold")}


def _reply_stand_in(body: str) -> dict:
    return {"sent": True, "stood_in": True}


@sdk.write_tool(fake=_reply_stand_in)
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


@sdk.read_tool
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
def home(tmp_path, monkeypatch, replay_stubs, home_with_maps):
    """Return an `$IRIMI_HOME` with a CA and the test maps, the agent module and stub ports set."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "home"))
    ca.generate_ca(ca.ca_paths())
    home_with_maps({"stripe.yaml": REPLAY_STRIPE_MAP, "anthropic.yaml": REPLAY_LLM_MAP})
    (tmp_path / "replay_agent.py").write_text(AGENT)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("STUB_STRIPE", str(replay_stubs[0]))
    monkeypatch.setenv("STUB_LLM", str(replay_stubs[1]))
    monkeypatch.delenv("PROMPT", raising=False)
    return tmp_path / "home"


def _record_run(home, code: str, stubs) -> str:
    """Run `code` under `irimi shadow` and return the SDK run it stored, else the process run.

    The stubs forget every request they saw while it ran."""
    before = {r.run_id for r in StoreReader(home / "store").list_runs()}
    command = [sys.executable, "-c", f"import replay_agent, sys; {code}"]
    assert main(["shadow", "--port", "0", "--", *command]) == 0
    runs = [r for r in StoreReader(home / "store").list_runs() if r.run_id not in before]
    stubs.forget()
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
        i for i, line in enumerate(lines) if line.startswith("irimi replay · ") and "->" in line
    )
    return lines[start:]


def _no_result(lines: list[str]) -> bool:
    """Whether `irimi replay` printed no result block: an agent it could not call is no replay."""
    return not any(line.startswith("irimi replay · ") and "->" in line for line in lines)


def _replay_runs(home, source: str) -> list:
    return [r for r in StoreReader(home / "store").list_runs() if r.replay_of == source]


def test_an_unchanged_agent_replays_with_no_divergence_and_sends_nothing(home, capfd, replay_stubs):
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert (code, err) == (0, [])
    [replay] = _replay_runs(home, source)
    assert (replay.mode, replay.attribution, replay.end.exit_code) == ("replay", "process", 0)
    assert out[:2] == [
        f"irimi replay · run {replay.run_id} · listening on " + out[0].split(" · listening on ")[1],
        f"replaying {source} as {replay.run_id}",
    ]
    assert _result(out) == [
        f"irimi replay · {source} -> {replay.run_id}",
        "  agent exited 0",
        "  no divergence: every call was answered from the recording",
        "  3 from the recording · 0 missed",
        "",
        "  = ○ stripe  refund $49.00 on ch_REAL1  faked; checked against live state: would succeed",
        "",
        "  1 same · 0 changed · 0 only in A · 0 only in B",
    ]
    assert replay_stubs.stripe.seen == [] and replay_stubs.llm.seen == []
    # The replay is one run: the SDK's trigger adopted it, so it holds every event.
    events = StoreReader(home / "store").load_run(replay.run_id).events
    assert [e.request.path if isinstance(e, Exchange) else e for e in events] == [
        "/v1/messages",
        "/v1/charges/ch_REAL1",
        "/v1/refunds",
    ]
    assert replay.trigger == StoreReader(home / "store").load_run(source).record.trigger


def test_a_changed_prompt_diverges_at_the_llm_call_and_refunds_another_amount(
    home, capfd, monkeypatch, replay_stubs
):
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    capfd.readouterr()
    monkeypatch.setenv("PROMPT", "Refund half of it.")
    code, out, err = _replay(capfd, source)
    assert (code, err) == (1, [])
    [replay] = _replay_runs(home, source)
    assert _result(out) == [
        f"irimi replay · {source} -> {replay.run_id}",
        "  agent exited 0",
        "  diverged at the agent's 1st call: real          llm       "
        "POST localhost/v1/messages -> 200  [replay-miss]",
        "  1 from the recording · 2 missed",
        "",
        "  ~ A ○ stripe  refund $49.00 on ch_REAL1  faked; checked against live state: "
        "would succeed",
        "    B ○ stripe  refund $24.50 on ch_REAL1  faked; checked against live state: "
        "would succeed",
        "",
        "  0 same · 1 changed · 0 only in A · 0 only in B",
    ]
    assert len(replay_stubs.llm.seen) == 1
    assert [m for m, _ in replay_stubs.stripe.seen if m == "POST"] == []


def test_a_process_run_with_no_trigger_replays_by_running_its_argv_again(home, capfd, replay_stubs):
    source = _record_run(
        home, "sys.exit(0 if replay_agent.plain('ch_REAL1') == 200 else 1)", replay_stubs
    )
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert (code, err) == (0, [])
    [replay] = _replay_runs(home, source)
    recorded = StoreReader(home / "store").load_run(source).record
    assert replay.trigger == recorded.trigger
    assert "  no divergence: every call was answered from the recording" in out
    assert replay_stubs.stripe.seen == []
    # The header says only the argv ran again: nothing else of the process was recorded.
    assert recorded.trigger is not None and recorded.trigger.argv is not None
    argv = shlex.join(recorded.trigger.argv)
    assert _result(out)[1] == (
        f"  re-ran {argv} (argv only: environment, cwd and stdin are not recorded)"
    )


def test_call_and_args_override_the_recorded_entrypoint_and_args(home, capfd, replay_stubs):
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    capfd.readouterr()
    ticket = {
        "ticket": {
            "__irimi_type__": "replay_agent:Ticket",
            "value": {"charge": "ch_REAL1", "note": "refund please"},
        }
    }
    code, out, err = _replay(
        capfd, source, "--entrypoint", "replay_agent:refund_half", "--args", json.dumps(ticket)
    )
    assert (code, err) == (1, [])
    [replay] = _replay_runs(home, source)
    # Named after the function it called, as `@sdk.trigger` names a run.
    assert (replay.trigger.name, replay.trigger.entrypoint) == (
        "replay_agent:refund_half",
        "replay_agent:refund_half",
    )
    result = _result(out)
    assert result[2:4] == [
        "  diverged at the agent's 2nd call: faked         write     "
        "POST 127.0.0.1/v1/refunds -> 200  "
        "[replay-miss]",
        "  1 from the recording · 1 missed",
    ]
    assert (
        "    B ○ stripe  refund $24.50 on ch_REAL1  faked; checked against live state: "
        "would succeed" in result
    )
    # The agent's 2nd call, as the totals count two: irimi's check read of the charge, stored
    # between the two, is not the agent's and is not counted.
    events = StoreReader(home / "store").load_run(replay.run_id).events
    made = [
        (e.issued_by, e.request.method, e.request.path) if isinstance(e, Exchange) else e
        for e in events
    ]
    assert made == [
        ("agent", "GET", "/v1/charges/ch_REAL1"),
        ("engine", "GET", "/v1/charges/ch_REAL1"),
        ("agent", "POST", "/v1/refunds"),
    ]
    # Other args: the model is asked something else, so its call misses, and the header says so.
    other = {"ticket": {**ticket["ticket"], "value": {"charge": "ch_REAL1", "note": "please"}}}
    code, out, err = _replay(capfd, source, "--args", json.dumps(other))
    assert (code, err) == (0, [])
    assert _result(out)[2] == "  (the triggers' args differ)"
    assert _result(out)[3].startswith("  diverged at the agent's 1st call: real          llm ")


def test_args_stored_by_their_description_are_refused_and_given_with_args_run(
    home, capfd, replay_stubs
):
    source = _record_run(home, f"replay_agent.handle_with({TICKET}, print)", replay_stubs)
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert (code, out) == (2, [])
    assert err == [
        f"error: run {source}'s trigger arguments were not recorded replayably (a value was "
        "recorded as its description); pass --args"
    ]
    assert _replay_runs(home, source) == []
    recorded = StoreReader(home / "store").load_run(source).record.trigger
    assert recorded is not None and isinstance(recorded.args, dict)
    args = {**recorded.args, "notify": None}
    code, out, err = _replay(capfd, source, "--args", json.dumps(args))
    assert (code, err) == (0, [])
    assert "  no divergence: every call was answered from the recording" in out


def test_a_run_with_no_entrypoint_and_no_argv_is_refused(home, capfd, replay_stubs):
    source = _record_run(
        home,
        'exec(\'with replay_agent.sdk.run("block"): replay_agent.plain("ch_REAL1")\')',
        replay_stubs,
    )
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert (code, out) == (2, [])
    assert err == [
        f"error: run {source} has no entrypoint and no argv; pass --entrypoint or -- CMD"
    ]


def test_sdk_replay_returns_the_result_the_cli_writes(
    home, capfd, monkeypatch, tmp_path, replay_stubs
):
    """`testing.replay` returns the result the CLI writes, but for the replay's new run id.

    The agent is tools-only: a subprocess `irimi replay` loads the shipped maps, not the stubs'."""
    from irimi import testing

    source = _record_run(home, f"replay_agent.answer({TICKET})", replay_stubs)
    capfd.readouterr()
    result_file = tmp_path / "result.json"
    code, out, _ = _replay(capfd, source, "--result-file", str(result_file))
    by_cli = replay_result_from_json(json.loads(result_file.read_text()))
    assert code == by_cli.exit_code == 0
    assert by_cli.lines == tuple(_result(out))
    assert by_cli.diverged_at is None
    monkeypatch.syspath_prepend(str(tmp_path))
    import replay_agent  # type: ignore[import-not-found]

    for result in (
        testing.replay(source),
        testing.replay(
            source, replay_agent.answer, args=(replay_agent.Ticket("ch_REAL1", "refund please"),)
        ),
    ):
        capfd.readouterr()
        assert result.replay_run_id not in (None, by_cli.replay_run_id)
        assert by_cli.replay_run_id is not None
        same = [line.replace(result.replay_run_id, by_cli.replay_run_id) for line in result.lines]
        assert replace(result, replay_run_id=by_cli.replay_run_id, lines=tuple(same)) == by_cli
    tools = [
        (e.name, e.ran)
        for r in _replay_runs(home, source)
        for e in StoreReader(home / "store").load_run(r.run_id).events
        if isinstance(e, ToolCall)
    ]
    assert tools == [("replay_agent:lookup", "recording"), ("replay_agent:reply", "shadow")] * 3
    # Args far past what one command-line argument may hold go in a file, never on the line.
    big = testing.replay(
        source, replay_agent.answer, args=(replay_agent.Ticket("ch_REAL1", "x" * 300_000),)
    )
    capfd.readouterr()
    assert (big.exit_code, big.lines[2]) == (0, "  (the triggers' args differ)")


def test_sdk_replay_of_a_run_the_store_does_not_hold_is_a_result_that_exits_2(home):
    from irimi import testing

    result = testing.replay("nosuchrun")
    assert result == ReplayResult(
        "nosuchrun",
        None,
        2,
        None,
        (f"error: no run nosuchrun in {home / 'store'}",),
    )


def test_a_replay_takes_a_prefix_of_the_run_id_and_names_the_whole_one(
    home, capfd, tmp_path, replay_stubs
):
    """A unique run-id prefix names its run, and the result and messages name the whole id."""
    source = _record_run(home, f"replay_agent.Desk().handle({TICKET})", replay_stubs)
    capfd.readouterr()
    result_file = tmp_path / "result.json"
    code, _, err = _replay(capfd, source[:8], "--result-file", str(result_file))
    assert code == 2
    assert err[0].startswith("error: cannot call replay_agent:Desk.handle: ")
    result = replay_result_from_json(json.loads(result_file.read_text()))
    assert result.source_run_id == source


def test_a_trigger_it_cannot_call_is_a_replay_that_could_not_run(
    home, capfd, tmp_path, replay_stubs
):
    """A method trigger, or args that do not revive, exits 2 with one reason and no comparison."""
    source = _record_run(home, f"replay_agent.Desk().handle({TICKET})", replay_stubs)
    capfd.readouterr()
    result_file = tmp_path / "result.json"
    code, out, err = _replay(capfd, source, "--result-file", str(result_file))
    assert code == 2
    why = (
        "error: cannot call replay_agent:Desk.handle: it is a method, and a run does not record "
        "the instance it was called on"
    )
    assert err == [why]
    assert _no_result(out)
    [replay] = _replay_runs(home, source)
    assert (replay.end.outcome, replay.end.exit_code, replay.end.error.type) == (
        "error",
        126,
        "cannot-call",
    )
    result = replay_result_from_json(json.loads(result_file.read_text()))
    assert result == ReplayResult(source, replay.run_id, 2, None, (why,))
    other = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    missing = {"ticket": {"__irimi_type__": "replay_agent:NoSuchTicket", "value": {}}}
    capfd.readouterr()
    code, out, err = _replay(capfd, other, "--args", json.dumps(missing))
    assert code == 2
    assert err == [
        "error: cannot call replay_agent:handle: its args do not revive: "
        "replay_agent:NoSuchTicket is not in its module"
    ]
    assert _no_result(out)


def test_a_trigger_recorded_in_main_is_refused(home, capfd):
    """A `__main__` trigger is refused with exit 2 before anything runs, naming the command to use.

    An SDK run records no command line, so the module is left as `<module>`."""
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
        f"error: run {run.run_id} was started as a script, so irimi cannot import its trigger "
        f"__main__:main; replay it with: irimi replay {run.run_id} --entrypoint <module>:main "
        "--python <your venv python>"
    ]


def _python(tmp_path, name: str, setup: str, flags: str = "") -> str:
    """Return a `--python` script that runs `setup`, then this interpreter with `flags`."""
    path = tmp_path / name
    path.write_text(f'#!/bin/sh\n{setup}\nexec {shlex.quote(sys.executable)} {flags} "$@"\n')
    path.chmod(0o755)
    return str(path)


def test_a_trigger_only_the_agent_s_python_can_import_is_called_under_python(
    home, capfd, tmp_path, replay_stubs
):
    """A trigger irimi's own Python cannot import exits 2 with a hint, and `--python` calls it.

    The agent's module leaves the working directory once its run is recorded."""
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    site = tmp_path / "agent-site"
    site.mkdir()
    (tmp_path / "replay_agent.py").rename(site / "replay_agent.py")
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert code == 2
    why = "importing replay_agent raised ModuleNotFoundError: No module named 'replay_agent'"
    assert _no_result(out)
    # The error and the hint, each said once.
    assert err == [
        f"error: cannot call replay_agent:handle: {why}",
        f"hint: the trigger was called under irimi's own Python, {sys.executable}, which may not "
        "have the agent's packages; name the agent's Python with --python PATH",
    ]
    # Named with --python, a Python is the agent's: irimi says nothing of its own.
    code, out, err = _replay(capfd, source, "--python", sys.executable)
    assert (code, err) == (2, [f"error: cannot call replay_agent:handle: {why}"])
    _python(tmp_path, "agent-python", f"export PYTHONPATH={shlex.quote(str(site))}")
    # A relative path, as `--python .venv/bin/python` is typed, from the working directory.
    code, out, err = _replay(capfd, source, "--python", "./agent-python")
    assert (code, err) == (0, [])
    assert _result(out)[1:] == [
        "  agent exited 0",
        "  no divergence: every call was answered from the recording",
        "  3 from the recording · 0 missed",
        "",
        "  = ○ stripe  refund $49.00 on ch_REAL1  faked; checked against live state: would succeed",
        "",
        "  1 same · 0 changed · 0 only in A · 0 only in B",
    ]
    assert replay_stubs.stripe.seen == [] and replay_stubs.llm.seen == []


def test_a_python_that_cannot_run_irimi_s_replay_child_is_refused_before_anything_runs(
    home, capfd, tmp_path, monkeypatch, replay_stubs
):
    """A `--python` that cannot run irimi's replay child exits 2 and stores no run.

    Covered: no irimi, no such program, a venv directory, a slow Python and a null byte."""
    from irimi.cli import runner

    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    bare = _python(tmp_path, "bare-python", "unset PYTHONPATH", "-S")
    missing = str(tmp_path / "no-such-python")
    slow = _python(tmp_path, "slow-python", "exec sleep 5")
    monkeypatch.setattr(runner, "PYTHON_PROBE_TIMEOUT_S", 1.0)
    capfd.readouterr()
    code, out, err = _replay(capfd, source, "--python", bare)
    assert (code, out) == (2, [])
    assert err == [
        f"error: --python {bare} cannot import irimi's replay child (ModuleNotFoundError: No "
        "module named 'irimi'); install irimi in that Python's environment, or replay with -- CMD"
    ]
    code, out, err = _replay(capfd, source, "--python", missing)
    assert (code, out) == (2, [])
    assert err == [f"error: --python {missing} could not be run: No such file or directory"]
    # `--python .venv`, as uv takes it: relative or absolute, the error names the directory.
    (tmp_path / "venv" / "bin").mkdir(parents=True)
    for venv in ("venv", str(tmp_path / "venv")):
        code, out, err = _replay(capfd, source, "--python", venv)
        assert (code, out) == (2, [])
        assert err == [
            f"error: --python {venv} is a directory; name the Python in it, as --python "
            f"{venv}/bin/python"
        ]
    code, out, err = _replay(capfd, source, "--python", slow)
    assert (code, out) == (2, [])
    assert err == [f"error: --python {slow} did not answer within 1s"]
    code, out, err = _replay(capfd, source, "--python", "python\0")
    assert (code, out) == (2, [])
    assert err == ["error: --python python\\x00 could not be run: embedded null byte"]
    assert _replay_runs(home, source) == []


def test_a_python_with_another_irimi_is_warned_of_and_called_under(
    home, capfd, tmp_path, replay_stubs
):
    """A `--python` whose irimi is another version gets a warning, and the replay runs."""
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    older = tmp_path / "older-irimi"
    (older / "irimi-0.0.1.dist-info").mkdir(parents=True)
    (older / "irimi-0.0.1.dist-info" / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: irimi\nVersion: 0.0.1\n"
    )
    python = _python(tmp_path, "older-python", f"export PYTHONPATH={shlex.quote(str(older))}")
    capfd.readouterr()
    code, out, err = _replay(capfd, source, "--python", python)
    assert (code, err) == (
        0,
        [
            f"warning: --python {python} has irimi 0.0.1, and this is irimi {__version__}; the "
            "trigger is called by that irimi, so install the same version in both"
        ],
    )
    assert _result(out)[2] == "  no divergence: every call was answered from the recording"


def test_a_sigterm_while_python_is_asked_stops_the_replay_with_143(
    home, capfd, tmp_path, replay_stubs
):
    """A SIGTERM while `--python` is probed exits 143, not 2, and stores no run.

    `irimi replay` runs as its own process, so the signal reaches it alone."""
    import os
    import subprocess

    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    python = _python(tmp_path, "stopping-python", "kill -TERM $PPID\nexit 1")
    written = tmp_path / "result.json"
    command = [sys.executable, "-m", "irimi", "replay", source, "--port", "0"]
    command += ["--python", python, "--result-file", str(written)]
    proc = subprocess.run(
        command, env=dict(os.environ), capture_output=True, timeout=60, check=False
    )
    assert proc.returncode == 143
    assert replay_result_from_json(json.loads(written.read_text())).exit_code == 143
    assert _replay_runs(home, source) == []


def test_python_with_a_command_is_a_usage_error_and_for_a_process_run_refused(
    home, capfd, replay_stubs
):
    """`--python` with `-- CMD` is a usage error, and with a process run it exits 2."""
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    capfd.readouterr()
    with pytest.raises(SystemExit) as usage:
        main(["replay", source, "--python", sys.executable, "--", "true"])
    assert usage.value.code == 2
    assert "-- CMD names its own Python: drop --python, or the command" in capfd.readouterr().err
    process = _record_run(home, "replay_agent.plain('ch_REAL1')", replay_stubs)
    capfd.readouterr()
    code, out, err = _replay(capfd, process, "--python", sys.executable)
    assert (code, out) == (2, [])
    assert err == [
        f"error: run {process} is replayed by its recorded command, which names its own Python; "
        "drop --python"
    ]


def test_a_replay_that_ends_on_what_its_trigger_raised_records_it_as_its_recording_did(home, capfd):
    """A replay ends its run with the error the trigger raised, as the recorded run ended."""
    command = [sys.executable, "-c", f"import replay_agent; replay_agent.fails({TICKET})"]
    assert main(["shadow", "--port", "0", "--", *command]) == 1
    [source] = [r for r in StoreReader(home / "store").list_runs() if r.attribution == "sdk"]
    capfd.readouterr()
    code, out, _ = _replay(capfd, source.run_id)
    assert code == 0
    assert _result(out)[1] == "  agent exited 1"
    [replay] = _replay_runs(home, source.run_id)
    assert (replay.end.outcome, replay.end.exit_code) == ("error", 1)
    assert source.end is not None
    assert replay.end.error == source.end.error == ErrorInfo("builtins.KeyError", "'ch_REAL1'")


def test_args_from_a_file_and_a_command_with_a_call_is_a_usage_error(
    home, capfd, tmp_path, replay_stubs
):
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    capfd.readouterr()
    args = tmp_path / "args.json"
    trigger = StoreReader(home / "store").load_run(source).record.trigger
    assert trigger is not None
    args.write_text(json.dumps(trigger.args))
    code, out, err = _replay(capfd, source, "--args-file", str(args))
    assert (code, err) == (0, [])
    assert "  no divergence: every call was answered from the recording" in out
    with pytest.raises(SystemExit) as usage:
        main(["replay", source, "--entrypoint", "replay_agent:handle", "--", "true"])
    assert usage.value.code == 2
    assert "-- CMD runs a command, not a call" in capfd.readouterr().err


@pytest.mark.parametrize("flag", ["--args", "--args-file"])
def test_args_nested_deeper_than_the_stack_are_a_usage_error(capfd, tmp_path, flag):
    """`--args` JSON nested deeper than the stack is a usage error, not a traceback."""
    deep = "[" * 200_000 + "]" * 200_000
    (tmp_path / "args.json").write_text(deep)
    value = deep if flag == "--args" else str(tmp_path / "args.json")
    with pytest.raises(SystemExit) as usage:
        main(["replay", "RUN", flag, value])
    assert usage.value.code == 2
    assert capfd.readouterr().err.splitlines()[-1] == (
        f"irimi replay: error: argument {flag}: --args is nested deeper than it can be read"
    )


def test_a_refused_replay_s_result_file_holds_the_reason_irimi_gave(
    home, capfd, tmp_path, replay_stubs
):
    """`testing.replay`'s caller gets the reason, not only the one who read stderr."""
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    for name in ("ca.pem", "ca.key"):
        (home / "ca" / name).unlink()
    capfd.readouterr()
    written = tmp_path / "result.json"
    code, out, err = _replay(capfd, source, "--result-file", str(written))
    assert (code, out) == (2, [])
    result = replay_result_from_json(json.loads(written.read_text()))
    assert result == ReplayResult(source, None, 2, None, tuple(err))
    assert err == [f"error: no CA at {home / 'ca'}; run `irimi init` first"]


def test_a_replay_that_called_a_trigger_can_itself_be_replayed(home, capfd, replay_stubs):
    """A replay's run keeps the trigger it replayed, so replaying it calls that trigger again."""
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    capfd.readouterr()
    assert _replay(capfd, source)[0] == 0
    [first] = _replay_runs(home, source)
    code, out, err = _replay(capfd, first.run_id)
    assert (code, err) == (0, [])
    assert "  no divergence: every call was answered from the recording" in out
    [second] = _replay_runs(home, first.run_id)
    assert second.trigger == first.trigger


def test_args_that_do_not_bind_and_a_module_that_exits_as_it_loads_cannot_be_called(
    home, capfd, replay_stubs
):
    """Args that do not bind, or a module that exits on import, are calls not made: exit 2."""
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    capfd.readouterr()
    code, out, err = _replay(capfd, source, "--args", json.dumps({"tikket": 1}))
    assert (code, _no_result(out)) == (2, True)
    assert err[0].startswith("error: cannot call replay_agent:handle: its args do not bind: ")
    (home.parent / "exits_agent.py").write_text("import sys\nsys.exit(4)\n")
    code, out, err = _replay(capfd, source, "--entrypoint", "exits_agent:handle", "--args", "{}")
    assert (code, _no_result(out)) == (2, True)
    assert err == [
        "error: cannot call exits_agent:handle: importing exits_agent raised SystemExit: 4"
    ]


def test_a_read_tool_answered_with_the_http_its_body_made_is_no_divergence(
    home, capfd, replay_stubs
):
    """HTTP a read tool's body made is not missed when the replay answers the tool."""
    source = _record_run(home, f"replay_agent.checks({TICKET})", replay_stubs)
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert (code, err) == (0, [])
    assert _result(out)[2:4] == [
        "  no divergence: every call was answered from the recording",
        "  1 from the recording · 0 missed",
    ]
    assert replay_stubs.stripe.seen == []


def test_a_replay_sigterm_stops_while_its_agent_runs_exits_143_and_says_so(
    home, capfd, tmp_path, replay_stubs
):
    """A SIGTERM reaches the agent, and the replay exits 143 and says it was stopped.

    `irimi replay` runs as its own process here, so the signal reaches it alone."""
    import os
    import signal
    import subprocess

    source = _record_run(home, f"replay_agent.waits({TICKET})", replay_stubs)
    capfd.readouterr()
    marker, written = tmp_path / "started", tmp_path / "result.json"
    command = [sys.executable, "-m", "irimi", "replay", "--port", "0"]
    command += ["--result-file", str(written), f"--run={source}"]
    proc = subprocess.Popen(
        command,
        env={**os.environ, "WAIT_FOR": str(marker), "PYTHONPATH": str(tmp_path)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    eventually(marker.exists, timeout=30, interval=0.05)
    proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=30) == 143
    result = replay_result_from_json(json.loads(written.read_text()))
    assert result.exit_code == 143
    assert "  stopped: irimi replay was interrupted before the agent ended" in result.lines


def test_a_replay_interrupted_before_its_run_is_stored_exits_130_with_no_run(
    home, capfd, monkeypatch, tmp_path, replay_stubs
):
    """A Ctrl-C before the replay's run is stored exits 130, and the result names no run."""
    from irimi.cli import compose

    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)

    def interrupted(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(compose, "print_startup", interrupted)
    written = tmp_path / "result.json"
    capfd.readouterr()
    code, _, _ = _replay(capfd, source, "--result-file", str(written))
    assert code == 130
    result = replay_result_from_json(json.loads(written.read_text()))
    assert (result.replay_run_id, result.exit_code) == (None, 130)
    assert _replay_runs(home, source) == []


def test_a_replay_interrupted_after_its_run_is_stored_names_the_run_and_exits_130(
    home, capfd, monkeypatch, tmp_path, replay_stubs
):
    """A Ctrl-C while the runs are compared exits 130, and the result names the ended run."""
    from irimi.replay import report as replay_report

    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)

    def interrupted(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(replay_report, "outcome", interrupted)
    written = tmp_path / "result.json"
    capfd.readouterr()
    code, _, err = _replay(capfd, source, "--result-file", str(written))
    [run] = _replay_runs(home, source)
    assert (code, err) == (130, ["error: interrupted"])
    result = replay_result_from_json(json.loads(written.read_text()))
    assert result == ReplayResult(source, run.run_id, 130, None, ("error: interrupted",))
    assert (run.end.outcome, run.end.exit_code) == ("ok", 0)


def test_a_run_that_did_not_end_is_replayed_after_one_warning(home, capfd, replay_stubs):
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    run_json = home / "store" / "runs" / source / "run.json"
    record = json.loads(run_json.read_text())
    record.update(ended_at=None, outcome=None, exit_code=None, error=None)
    run_json.write_text(json.dumps(record))
    capfd.readouterr()
    code, out, err = _replay(capfd, source)
    assert (code, err) == (
        0,
        [f"warning: run {source} did not end; it is replayed as far as it was recorded"],
    )
    assert _result(out)[2] == "  no divergence: every call was answered from the recording"


def test_a_command_that_does_not_start_is_a_replay_that_could_not_run(
    home, capfd, tmp_path, replay_stubs
):
    """A command that does not resolve exits 2 before the proxy starts, its result saying why."""
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    written = tmp_path / "result.json"
    capfd.readouterr()
    code, _, err = _replay(capfd, source, "--result-file", str(written), "--", "no-such-agent")
    assert _replay_runs(home, source) == []
    assert (code, err) == (2, ["error: command not found: no-such-agent"])
    assert replay_result_from_json(json.loads(written.read_text())) == ReplayResult(
        source, None, 2, None, ("error: command not found: no-such-agent",)
    )


def test_an_agent_that_names_the_recorded_run_never_rewrites_it(
    home, capfd, monkeypatch, replay_stubs
):
    """Starting or labelling the recorded run goes to `unattributed/`, flagged `bad-run-id`.

    The recording stays byte for byte as it was."""
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    store = home / "store"
    before = {p.name: p.read_bytes() for p in (store / "runs" / source).iterdir()}
    (home.parent / "names_it.py").write_text(
        "import http.client, json, os, urllib.parse, urllib.request\n"
        "import replay_agent\n"
        "src = os.environ['SOURCE']\n"
        "start = open(os.path.join(os.environ['STORE'], 'runs', src, 'run.json'), 'rb').read()\n"
        "opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))\n"
        "url = os.environ['IRIMI_CONTROL'] + f'/runs/{src}/start'\n"
        "opener.open(urllib.request.Request(url, data=start, method='POST'))\n"
        "door = urllib.parse.urlparse(os.environ['HTTPS_PROXY'])\n"
        "conn = http.client.HTTPConnection(door.hostname, door.port, timeout=10)\n"
        "host = replay_agent.STRIPE.split('/')[2]\n"
        "url = replay_agent.STRIPE + '/v1/charges/ch_REAL1'\n"
        "conn.request('GET', url, headers={'host': host, 'irimi-run': src})\n"
        "conn.getresponse().read()\n"
    )
    monkeypatch.setenv("SOURCE", source)
    monkeypatch.setenv("STORE", str(store))
    capfd.readouterr()
    code, _, err = _replay(capfd, source, "--", sys.executable, "names_it.py")
    assert (code, err) == (1, [])
    assert {p.name: p.read_bytes() for p in (store / "runs" / source).iterdir()} == before
    [stray] = StoreReader(store).load_run("unattributed").events
    assert isinstance(stray, Exchange)
    assert (stray.run_id, stray.flags) == (
        source,
        ("from-recording", "bad-run-id"),
    )


def test_runs_list_and_show_mark_a_run_a_label_made_in_a_replay(home, capfd, replay_stubs):
    """An `Irimi-Run` label made in a replay is a header run listed and shown as a replay's."""
    source = _record_run(home, f"replay_agent.handle({TICKET})", replay_stubs)
    (home.parent / "labels.py").write_text(
        "import http.client, os, urllib.parse\n"
        "import replay_agent\n"
        "door = urllib.parse.urlparse(os.environ['HTTPS_PROXY'])\n"
        "conn = http.client.HTTPConnection(door.hostname, door.port, timeout=10)\n"
        "host = replay_agent.STRIPE.split('/')[2]\n"
        "url = replay_agent.STRIPE + '/v1/charges/ch_REAL1'\n"
        "conn.request('GET', url, headers={'host': host, 'irimi-run': 'labelled-in-replay'})\n"
        "conn.getresponse().read()\n"
    )
    capfd.readouterr()
    code, _, err = _replay(capfd, source, "--", sys.executable, "labels.py")
    # Exit 1: the recorded refund is only in A, since the command makes none.
    assert (code, err) == (1, [])
    replays = {r.attribution: r.run_id for r in _replay_runs(home, source)}
    assert replays == {"process": replays["process"], "header": "labelled-in-replay"}
    assert main(["runs", "list"]) == 0
    # A header row, then the runs, their columns padded to two spaces or more.
    rows = capfd.readouterr().out.splitlines()[1:]
    listed = [re.split(r"  +", line) for line in rows]
    attribution = {columns[0]: columns[4] for columns in listed}
    assert attribution[source] == "sdk"
    assert attribution[replays["process"]] == f"replay of {source}"
    assert attribution["labelled-in-replay"] == f"header, replay of {source}"
    assert sorted(attribution.values()) == sorted(
        ["process", "sdk", f"replay of {source}", f"header, replay of {source}"]
    )
    assert main(["runs", "show", "labelled-in-replay"]) == 0
    shown = capfd.readouterr().out.splitlines()
    assert shown[:5] == [
        "run: labelled-in-replay",
        "attribution: header",
        "mode: replay",
        f"replay of: {source}",
        "trigger: -",
    ]
    assert [line for line in shown if line.startswith("irimi ")] == [
        "irimi replay · run labelled-in-replay · 1 exchange · 0.0s"
    ]


@pytest.mark.parametrize(
    ("typed", "said"),
    [
        (["--", sys.executable, "-c", "pass"], "give the run to replay: RUN, or --run=RUN"),
        (["r1", "--run=r1"], "give the run once: RUN, or --run=RUN"),
    ],
)
def test_a_replay_names_its_run_once_before_any_command(home, capfd, typed, said):
    """A replay names its run exactly once before `--`, as RUN or `--run=RUN`."""
    capfd.readouterr()
    with pytest.raises(SystemExit) as usage:
        main(["replay", "--port", "0", *typed])
    assert usage.value.code == 2
    err = capfd.readouterr().err
    assert err.startswith("usage: irimi replay RUN [options] [-- COMMAND [ARGS...]]\n")
    assert f"irimi replay: error: {said}" in err


def test_a_run_id_that_starts_with_a_dash_is_given_as_run_equals(home, capfd, replay_stubs):
    """`--run=RUN` spells an id argparse would take as a flag; a command still follows `--`."""
    source = _record_run(
        home, "sys.exit(0 if replay_agent.plain('ch_REAL1') == 200 else 1)", replay_stubs
    )
    capfd.readouterr()
    command = [sys.executable, "-c", "import replay_agent; replay_agent.plain('ch_REAL1')"]
    code = main(["replay", f"--run={source}", "--port", "0", "--", *command])
    out = capfd.readouterr().out.splitlines()
    assert code == 0
    assert _result(out)[2] == "  no divergence: every call was answered from the recording"
    assert main(["replay", "--port", "0", f"--run={source}"]) == 0
    assert cli.parse_args(["replay", "--run=-r1"]).run == "-r1"


@pytest.mark.parametrize(
    ("typed", "command"),
    [
        (["replay", "RUN"], []),
        (["replay", "RUN", "--", "pytest", "--", "-k", "x"], ["pytest", "--", "-k", "x"]),
        (["replay", "--store", "s", "RUN", "--", "a", "--"], ["a", "--"]),
        (["replay", "--run=-RUN", "--", "pytest", "--", "-k"], ["pytest", "--", "-k"]),
    ],
)
def test_the_command_is_every_word_after_the_first_double_dash(typed, command):
    assert cli.parse_args(typed).cmd == command


def test_an_outcome_file_the_agent_nested_past_the_stack_is_no_outcome(home, capfd, tmp_path):
    """An outcome file nested past the stack is ignored; the agent's exit ends the run."""
    (home.parent / "deep_agent.py").write_text(
        "import os\nfrom irimi import sdk\n\n\n@sdk.trigger\ndef handle(n):\n"
        "    outcome = os.environ.get('IRIMI_REPLAY_OUTCOME_FILE')\n"
        "    if outcome:\n"
        "        with open(outcome, 'w') as fh:\n"
        "            fh.write('[' * 200000 + ']' * 200000)\n"
        "        os._exit(0)\n"
    )
    command = [sys.executable, "-c", "import deep_agent; deep_agent.handle(1)"]
    assert main(["shadow", "--port", "0", "--", *command]) == 0
    [source] = [r for r in StoreReader(home / "store").list_runs() if r.attribution == "sdk"]
    written = tmp_path / "result.json"
    capfd.readouterr()
    code, out, err = _replay(capfd, source.run_id, "--result-file", str(written))
    assert (code, err) == (0, [])
    assert _result(out)[1] == "  agent exited 0"
    [run] = _replay_runs(home, source.run_id)
    assert replay_result_from_json(json.loads(written.read_text())) == ReplayResult(
        source.run_id, run.run_id, 0, None, tuple(_result(out))
    )
    assert (run.end.outcome, run.end.exit_code) == ("ok", 0)


def test_a_replay_of_a_run_the_store_does_not_hold_starts_nothing(tmp_path, monkeypatch, capsys):
    """An unknown run exits 2 before any command runs or any store is made."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    monkeypatch.chdir(tmp_path)
    ca.generate_ca(ca.ca_paths())
    marker = tmp_path / "ran"
    command = [sys.executable, "-c", f"open({str(marker)!r}, 'w')"]
    assert main(["replay", "nosuchrun", "--port", "0", "--", *command]) == 2
    assert capsys.readouterr().err == f"error: no run nosuchrun in {tmp_path / 'store'}\n"
    assert not marker.exists()
    assert not (tmp_path / "store").exists()


@pytest.mark.parametrize("damage", ["events", "run.json", "newer", "blob"])
def test_a_replay_of_a_damaged_run_starts_nothing(tmp_path, monkeypatch, capsys, damage):
    """A recording `StoreReader` cannot read exits 2 with one line, no command and no new run."""
    import json

    from irimi.core.exchange import Exchange, Request, Response
    from irimi.record import redact
    from irimi.record.store.writer import DirectoryStore

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    monkeypatch.chdir(tmp_path)
    ca.generate_ca(ca.ca_paths())
    store = tmp_path / "store"
    recorded = DirectoryStore(store, redact.load_key(tmp_path))
    recorded.record(
        Exchange(
            request=Request("GET", "https", "api.stripe.com", 443, "/v1/charges/ch_1", "", (), b""),
            response=Response(200, (), b"{}"),
            service="stripe",
            operation="charges.retrieve",
            kind="read",
            answered_by="live",
            run_id="rec1",
        )
    )
    recorded.close()
    run = store / "runs" / "rec1"
    if damage == "events":
        (run / "events.jsonl").write_text("{\n" + (run / "events.jsonl").read_text())
    elif damage == "run.json":
        (run / "run.json").write_text("{")
    elif damage == "newer":
        record = json.loads((run / "run.json").read_text())
        (run / "run.json").write_text(json.dumps(record | {"schema_version": 4}))
    else:
        for blob in (store / "blobs").iterdir():
            blob.unlink()
    marker = tmp_path / "ran"
    command = [sys.executable, "-c", f"open({str(marker)!r}, 'w')"]
    assert main(["replay", "rec1", "--port", "0", "--", *command]) == 2
    err = capsys.readouterr().err
    assert err.startswith("error: run rec1 could not be read: ")
    assert err.count("\n") == 1
    assert not marker.exists()
    assert [p.name for p in (store / "runs").iterdir()] == ["rec1"]


def test_the_unattributed_pool_is_not_a_run_to_replay(tmp_path, monkeypatch, capsys):
    """`unattributed` holds no single run to answer from, so replaying it exits 2."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    monkeypatch.chdir(tmp_path)
    ca.generate_ca(ca.ca_paths())
    command = [sys.executable, "-c", "pass"]
    assert main(["replay", "unattributed", "--port", "0", "--", *command]) == 2
    assert capsys.readouterr().err == "error: unattributed is not a run to replay\n"
    assert not (tmp_path / "store").exists()
