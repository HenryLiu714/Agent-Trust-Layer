"""The replay example recorded, its prompt edited and replayed: the replay shows what changed.

Stubs use names only irimi resolves, for the `NO_PROXY` reason in `tests/support/scenarios.py`."""

import contextlib
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import stripe

from examples.replay import agent, seed
from examples.replay.agent import Ticket, handle_ticket
from irimi import testing
from irimi.cli import main, runner
from irimi.config import paths
from irimi.core.exchange import Exchange
from irimi.protocol.env import ENGINE_ACTIVE_ENV
from irimi.protocol.replay_files import replay_result_from_json
from irimi.protocol.trace import ToolCall, Trigger
from irimi.record.store.reader import StoreReader
from tests.e2e.workflows.harness.internet import resolving_only
from tests.support.credentials import (
    HERMETIC_ANTHROPIC_KEY,
    HERMETIC_STRIPE_KEY,
    REFUSED_LIVE_KEY,
)
from tests.support.scenarios import base_url_stripe
from tests.support.upstreams import FakeLlm, FakeStripe, ReplayStubs, serve

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[2]
AGENT = agent.__name__
PROMPT = agent.DEFAULT_PROMPT
PROMPT_HALF = agent.DEFAULT_PROMPT.with_name("prompt_half.txt")
TICKET = ("T1", "ch_A", "double charged")
CUSTOMER = "Ada"
FULL = "○ stripe  refund $49.00 on ch_A  faked; checked against live state: would succeed"
HALF = "○ stripe  refund $24.50 on ch_A  faked; checked against live state: would succeed"
RECORDED = "○ tool  db.record_decision  fake ran instead"
# The variables the agent and its two SDKs read.
STRIPE_KEY_ENV = "STRIPE_API_KEY"
STRIPE_BASE_ENV = "STRIPE_API_BASE"
ANTHROPIC_KEY_ENV = "ANTHROPIC_API_KEY"
ANTHROPIC_BASE_ENV = "ANTHROPIC_BASE_URL"
MODEL_ENV = "ANTHROPIC_MODEL"
PROMPT_FILE_ENV = "PROMPT_FILE"
DB_ENV = "REFUND_DB"
# The names the stubs answer as, which only irimi's process resolves, to loopback.
SERVED = ("api.stripe.com", "api.anthropic.com")
# A hang guard for one irimi process. irimi dumps every thread's stack to stderr before the test
# gives up, so a hang says where it was.
TIMEOUT_S = 120
# `irimi.cli.main` in a process where only `SERVED` resolves, to loopback. `NO_PROXY=*` keeps
# irimi's own urllib calls off the shell's proxy; irimi sets its child's `NO_PROXY` itself.
IRIMI = f"""
import faulthandler
import os
import sys

from tests.e2e.workflows.harness.internet import resolving_only
from irimi import cli

faulthandler.dump_traceback_later({TIMEOUT_S - 20}, exit=True)
os.environ["NO_PROXY"] = os.environ["no_proxy"] = "*"
with resolving_only({SERVED!r}):
    sys.exit(cli.main(sys.argv[1:]))
"""


def _model(handler, body):
    """Answer Anthropic's `POST /v1/messages`: refund all for `prompt.txt`, half for the other.

    A request the real API would refuse, or any other prompt, is a 400. The answer opens with an
    empty `thinking` block, as a thinking model's does."""
    prompts = {PROMPT.read_text(): 1, PROMPT_HALF.read_text(): 2}
    output = body.get("output_config", {}).get("format", {})
    key = handler.headers.get("x-api-key") or handler.headers.get("authorization", "")
    well_formed = (
        handler.path == "/v1/messages"
        and key
        and handler.headers.get("anthropic-version")
        and body.get("model")
        and body.get("max_tokens")
        and body.get("messages")
        and output.get("type") == "json_schema"
        # Structured outputs refuse an object schema that leaves `additionalProperties` open.
        and output.get("schema", {}).get("additionalProperties") is False
        and body.get("system") in prompts
    )
    if not well_formed:
        return 400, {"type": "error", "error": {"type": "invalid_request_error"}}
    facts = json.loads(body["messages"][0]["content"])
    left = facts["charge"]["amount"] - facts["charge"]["amount_refunded"]
    amount = left // prompts[body["system"]]
    decision = {"refund_amount": amount, "reply": f"Refunded {amount}."}
    return 200, {
        "id": "msg_hermetic",
        "type": "message",
        "role": "assistant",
        "model": body["model"],
        "content": [
            {"type": "thinking", "thinking": "", "signature": "hermetic"},
            {"type": "text", "text": json.dumps(decision)},
        ],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }


@pytest.fixture
def stubs():
    """The loopback Stripe and Anthropic: `(stripe, llm)`, which log every request each heard."""
    stripe = serve(FakeStripe, threaded=True, **base_url_stripe())
    llm = serve(FakeLlm, threaded=True, reply=_model)
    yield ReplayStubs(stripe, llm)
    stripe.stop()
    llm.stop()


@pytest.fixture
def direct(monkeypatch):
    """Send the agent's in-process calls straight to the stubs, whatever proxy the shell names."""
    for name in [n for n in os.environ if n.lower().endswith("_proxy")]:
        monkeypatch.delenv(name)
    monkeypatch.setenv("NO_PROXY", "*")
    monkeypatch.setenv("no_proxy", "*")


@pytest.fixture
def home(home, monkeypatch):
    """The shared `home`, which is also the working directory here."""
    monkeypatch.chdir(home)
    return home


def _agent_env(home: Path, stripe_port: int, model_port: int) -> dict[str, str]:
    """Return the environment for irimi and the agent: stubs by real names, test keys, a database.

    It drops the runner's own proxy variables: irimi sets its child's."""
    env = {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}
    for name in (STRIPE_KEY_ENV, ANTHROPIC_KEY_ENV, MODEL_ENV, PROMPT_FILE_ENV):
        env.pop(name, None)
    env.update(
        {
            paths.IRIMI_HOME_ENV: str(home),
            STRIPE_BASE_ENV: f"http://api.stripe.com:{stripe_port}",
            ANTHROPIC_BASE_ENV: f"http://api.anthropic.com:{model_port}",
            STRIPE_KEY_ENV: HERMETIC_STRIPE_KEY,
            ANTHROPIC_KEY_ENV: HERMETIC_ANTHROPIC_KEY,
            DB_ENV: str(home / "agent.sqlite3"),
            "PYTHONFAULTHANDLER": "1",
        }
    )
    return env


def _irimi(env: dict[str, str], *argv: str) -> subprocess.CompletedProcess[str]:
    """Run `irimi <argv>` through `IRIMI` from the repository root, as the example's user would.

    A hang fails the test with irimi's stderr, its threads' stacks included."""
    try:
        return subprocess.run(
            [sys.executable, "-c", IRIMI, *argv],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=TIMEOUT_S,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        said = exc.stderr.decode(errors="replace") if isinstance(exc.stderr, bytes) else exc.stderr
        pytest.fail(f"irimi {argv[0]} did not end in {TIMEOUT_S}s; its stderr:\n{said}")


def _replay(
    env: dict[str, str], store: Path, run_id: str, result_file: Path
) -> tuple[subprocess.CompletedProcess[str], testing.ReplayResult]:
    """`irimi replay <run_id>`, and the result it wrote."""
    argv = ["replay", "--port", "0", "--store", str(store), "--result-file", str(result_file)]
    done = _irimi(env, *argv, run_id)
    assert result_file.exists(), done.stderr
    return done, replay_result_from_json(json.loads(result_file.read_text()))


def _decisions(db: Path) -> list[tuple[Any, ...]]:
    with contextlib.closing(sqlite3.connect(db)) as conn:
        return conn.execute("SELECT * FROM decisions").fetchall()


def _event(event: Exchange | ToolCall | Any) -> tuple[Any, ...]:
    if isinstance(event, ToolCall):
        return ("tool", event.name, event.kind, event.ran)
    assert isinstance(event, Exchange), event
    request = event.request
    return (
        event.issued_by,
        event.kind,
        request.method,
        request.host + request.path,
        event.answered_by,
    )


def _tool_calls(reader: StoreReader, run_id: str) -> dict[str, ToolCall]:
    """The run's tool calls by name; each tool is called once a run, so a second call fails."""
    calls = [e for e in reader.load_run(run_id).events if isinstance(e, ToolCall)]
    named = {e.name: e for e in calls}
    assert len(named) == len(calls), [e.name for e in calls]
    return named


def _anonymous(result: testing.ReplayResult) -> tuple[Any, ...]:
    """What a replay said, its own run's id aside: two replays of one run store two runs."""
    assert result.replay_run_id is not None
    lines = tuple(line.replace(result.replay_run_id, "<replay>") for line in result.lines)
    return (result.source_run_id, result.exit_code, result.diverged_at, lines)


def _heard_stripe(stubs) -> list[tuple[str, str]]:
    return stubs.stripe.seen


def test_an_llm_agent_recorded_then_replayed_with_its_prompt_edited_shows_what_changed(
    home, stubs, monkeypatch, capsys
):
    """A replay after a prompt edit shows what changed, and no write runs for real."""
    assert set(SERVED).isdisjoint(runner.NO_PROXY_VALUE.split(",")), "the trap: see the module doc"
    # Each served name resolves to loopback, so nothing irimi sends can leave the machine. Checked
    # first, so a wrong address fails here rather than hanging on a real host.
    with resolving_only(SERVED):
        for name in SERVED:
            assert socket.gethostbyname(name) == "127.0.0.1"
            assert {a[4][0] for a in socket.getaddrinfo(name, 443)} == {"127.0.0.1"}
    env = _agent_env(home, *stubs)
    store = home / "store"
    db = home / "agent.sqlite3"
    seed.seed(str(db), TICKET[1], CUSTOMER)

    # --- Record: the agent under `irimi shadow`. The SDK's run is stored with its trigger and args.
    command = [sys.executable, "-m", "examples.replay.agent", *TICKET]
    shadow = _irimi(env, "shadow", "--port", "0", "--store", str(store), "--", *command)
    assert shadow.returncode == 0, shadow.stderr
    assert "Refunded 4900.\n" in shadow.stdout
    reader = StoreReader(store)
    records = reader.list_runs()
    assert sorted(r.attribution for r in records) == ["process", "sdk"]
    [run] = [r for r in records if r.attribution == "sdk"]
    source = run.run_id
    assert run.end is not None
    assert run.end.outcome == "ok"
    # The ticket is the trigger's argument, stored as the dataclass it is, so a replay rebuilds it.
    assert run.trigger == Trigger(
        f"{AGENT}:handle_ticket",
        f"{AGENT}:handle_ticket",
        {
            "ticket": {
                "__irimi_type__": f"{AGENT}:Ticket",
                "value": {"ticket_id": "T1", "charge": "ch_A", "message": "double charged"},
            }
        },
        True,
    )
    events = reader.load_run(source).events
    assert [_event(e) for e in events] == [
        ("tool", "db.lookup_customer", "read", "real"),
        ("agent", "read", "GET", "api.stripe.com/v1/charges/ch_A", "live"),
        ("agent", "llm", "POST", "api.anthropic.com/v1/messages", "live"),
        ("engine", "read", "GET", "api.stripe.com/v1/charges/ch_A", "live"),
        ("agent", "write", "POST", "api.stripe.com/v1/refunds", "fake-L1"),
        ("tool", "db.record_decision", "write", "shadow"),
    ]
    [refund] = [e for e in events if isinstance(e, Exchange) and e.kind == "write"]
    assert (refund.request.body, refund.precondition.outcome) == (
        b"charge=ch_A&amount=4900",
        "passed",
    )
    # The read tool ran for real, and the write tool's fake ran in place of its body.
    tools = _tool_calls(reader, source)
    assert tools["db.lookup_customer"].result == {"charge": "ch_A", "name": CUSTOMER}
    decided = {"refund_amount": 4900, "reply": "Refunded 4900."}
    assert tools["db.record_decision"].args == {"ticket_id": "T1", "decision": decided}
    assert tools["db.record_decision"].result == {"recorded": False}
    # Neither write happened: Stripe heard no refund, and the database holds no decision.
    assert _decisions(db) == []
    # Each stub heard exactly what irimi stored as live: the agent's charge read, irimi's check
    # read of it, and the one model call.
    live = [e for e in events if isinstance(e, Exchange) and e.answered_by == "live"]
    assert _heard_stripe(stubs) == [("GET", "/v1/charges/ch_A")] * 2
    assert len(stubs.stripe.seen) == sum(e.request.host == "api.stripe.com" for e in live)
    assert [m["system"] for m in stubs.llm.seen] == [PROMPT.read_text()]
    assert len(stubs.llm.seen) == sum(e.kind == "llm" for e in live)

    # --- Replay, unchanged. The database is gone, so a real `lookup_customer` would raise: it is
    # answered from the recording.
    db.unlink()
    stubs.forget()
    done, same = _replay(env, store, source, home / "same.json")
    assert done.returncode == same.exit_code == 0, done.stderr
    assert same.lines == (
        f"irimi replay · {source} -> {same.replay_run_id}",
        "  agent exited 0",
        "  no divergence: every call was answered from the recording",
        "  4 from the recording · 0 missed",
        "",
        f"  = {FULL}",
        f"  = {RECORDED}",
        "",
        "  2 same · 0 changed · 0 only in A · 0 only in B",
    )
    assert same.diverged_at is None
    # What it printed last is what its result file says.
    assert done.stdout.endswith("\n".join(same.lines) + "\n")
    # Neither service heard anything; the read tool answered from the recording, and the write
    # tool's fake ran, never its body.
    assert (stubs.stripe.seen, stubs.llm.seen) == ([], [])
    assert same.replay_run_id is not None
    tools = _tool_calls(reader, same.replay_run_id)
    assert {name: call.ran for name, call in tools.items()} == {
        "db.lookup_customer": "recording",
        "db.record_decision": "shadow",
    }
    assert tools["db.lookup_customer"].result == {"charge": "ch_A", "name": CUSTOMER}
    assert not db.exists()

    # --- Replay with the prompt edited: the model is asked again, and decides half.
    edited_env = {**env, PROMPT_FILE_ENV: str(PROMPT_HALF)}
    done, edited = _replay(edited_env, store, source, home / "edited.json")
    assert done.returncode == edited.exit_code == 1, done.stderr
    diverged = (
        "the agent's 3rd call: real          llm       POST api.anthropic.com/v1/messages -> 200  "
        "[replay-miss]"
    )
    assert edited.diverged_at == diverged
    assert edited.lines == (
        f"irimi replay · {source} -> {edited.replay_run_id}",
        "  agent exited 0",
        f"  diverged at {diverged}",
        "  2 from the recording · 2 missed",
        "",
        f"  ~ A {FULL}",
        f"    B {HALF}",
        # The decision the fake was given, which its line does not print, changed too.
        f"  ~ A {RECORDED}",
        f"    B {RECORDED}",
        "      args.decision.refund_amount: 4900 -> 2450",
        '      args.decision.reply: "Refunded 4900." -> "Refunded 2450."',
        "",
        "  0 same · 2 changed · 0 only in A · 0 only in B",
    )
    # The model heard the edited prompt once. Stripe heard nothing: the charge read and the check
    # read for the changed refund both came from the recording.
    assert [m["system"] for m in stubs.llm.seen] == [PROMPT_HALF.read_text()]
    assert stubs.stripe.seen == []
    assert edited.replay_run_id is not None
    tools = _tool_calls(reader, edited.replay_run_id)
    halved = {"refund_amount": 2450, "reply": "Refunded 2450."}
    assert tools["db.record_decision"].args == {"ticket_id": "T1", "decision": halved}
    assert tools["db.record_decision"].ran == "shadow"
    assert not db.exists()

    # --- `irimi.testing.replay`: the same two replays, the same results.
    def run_irimi(argv: list[str], env: dict[str, str]) -> int:
        command = [sys.executable, "-c", IRIMI, *argv]
        return subprocess.run(command, env=dict(env), timeout=TIMEOUT_S, check=False).returncode

    monkeypatch.setattr(testing, "_run_irimi", run_irimi)
    for name in [n for n in os.environ if n.lower().endswith("_proxy")]:
        monkeypatch.delenv(name)
    # Clear what `_agent_env` leaves out, the shell's values included, so the replay asks nothing
    # the recording did not.
    for name in (MODEL_ENV, PROMPT_FILE_ENV):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    stubs.forget()
    by_function = testing.replay(source, store=store)
    assert _anonymous(by_function) == _anonymous(same)
    assert (stubs.stripe.seen, stubs.llm.seen) == ([], [])
    monkeypatch.setenv(PROMPT_FILE_ENV, str(PROMPT_HALF))
    by_function_edited = testing.replay(source, handle_ticket, args=(Ticket(*TICKET),), store=store)
    assert _anonymous(by_function_edited) == _anonymous(edited)
    assert [m["system"] for m in stubs.llm.seen] == [PROMPT_HALF.read_text()]
    assert stubs.stripe.seen == []
    assert not db.exists()

    # --- `irimi runs list`: the recording, its process run, and each replay of it, marked.
    capsys.readouterr()
    assert main(["runs", "list", "--store", str(store)]) == 0
    # Its columns are padded, so two spaces or more separate them, and a header row comes first.
    rows = capsys.readouterr().out.splitlines()[1:]
    columns = [re.split(r"  +", line) for line in rows]
    listed = {column[0]: column[4] for column in columns}
    assert len(listed) == len(columns)
    replayed = [same, edited, by_function, by_function_edited]
    expected: dict[str | None, str] = {
        **{r.run_id: r.attribution for r in records},
        **{r.replay_run_id: f"replay of {source}" for r in replayed},
    }
    assert listed == expected


@pytest.fixture
def stripe_settings(monkeypatch):
    """stripe-python's module-wide settings, put back after the test: `connect` sets them."""
    for name in ("api_key", "api_base", "proxy", "ca_bundle_path"):
        monkeypatch.setattr(stripe, name, getattr(stripe, name))


@pytest.fixture
def model_answering(monkeypatch, direct):
    """Yield `answering(answer)`, which points the agent at a model that always answers `answer`."""
    ports = []

    def answering(answer: dict[str, Any]) -> None:
        message = {
            "id": "msg_hermetic",
            "type": "message",
            "role": "assistant",
            "model": agent.DEFAULT_MODEL,
            "usage": {"input_tokens": 1, "output_tokens": 1},
            **answer,
        }
        ports.append(serve(FakeLlm, reply=lambda handler, body: (200, message)))
        monkeypatch.setenv(ANTHROPIC_BASE_ENV, f"http://127.0.0.1:{ports[-1]}")
        monkeypatch.setenv(ANTHROPIC_KEY_ENV, HERMETIC_ANTHROPIC_KEY)

    yield answering
    for port in ports:
        port.stop()


def test_the_agent_sends_no_stripe_key_that_is_not_test_mode(monkeypatch, stubs, stripe_settings):
    """The trigger refuses a live key before sending anything, so a replay checks the key too."""
    monkeypatch.setenv(STRIPE_KEY_ENV, REFUSED_LIVE_KEY)
    monkeypatch.setenv(STRIPE_BASE_ENV, f"http://127.0.0.1:{stubs[0]}")
    with pytest.raises(ValueError, match=r"STRIPE_API_KEY must be a test-mode key \(sk_test_"):
        handle_ticket(Ticket(*TICKET))
    assert stubs.stripe.seen == []


@pytest.mark.parametrize("stop", ["refusal", "max_tokens"])
def test_the_agent_takes_no_decision_from_an_answer_that_stopped_short(model_answering, stop):
    """A refused answer, or one cut at `max_tokens`, holds no decision, whatever its text says."""
    text = json.dumps({"refund_amount": 1, "reply": ""})
    model_answering({"stop_reason": stop, "content": [{"type": "text", "text": text}]})
    with pytest.raises(ValueError, match=f"stopped without a decision: {stop}"):
        agent.ask_model(PROMPT.read_text(), "{}")


@pytest.mark.parametrize(
    "answer",
    [
        # `pause_turn`: a turn the API paused, with no text yet. This agent uses no server tools,
        # so it should never see one; if it does, there is no decision in it.
        {"stop_reason": "pause_turn", "content": [{"type": "thinking", "thinking": ""}]},
        {"stop_reason": "end_turn", "content": [{"type": "text", "text": "Refund it all."}]},
        {"stop_reason": "end_turn", "content": [{"type": "text", "text": "[4900]"}]},
        {"stop_reason": "end_turn", "content": [{"type": "text", "text": '{"refund_amount": 1}'}]},
        {
            "stop_reason": "end_turn",
            "content": [{"type": "text", "text": '{"refund_amount": true, "reply": ""}'}],
        },
        {
            "stop_reason": "end_turn",
            "content": [{"type": "text", "text": '{"refund_amount": "4900", "reply": ""}'}],
        },
    ],
)
def test_the_agent_takes_no_decision_from_an_answer_that_holds_none(model_answering, answer):
    """Only text matching `DECISION_SCHEMA` is a decision: an integer amount and a reply.

    `true` is not an amount, though Python counts it as 1."""
    model_answering(answer)
    with pytest.raises(ValueError, match="the model did not answer with a decision: "):
        agent.ask_model(PROMPT.read_text(), "{}")


@pytest.mark.parametrize("amount", [4900, 0])
def test_outside_irimi_the_agent_refunds_what_the_model_decided_and_records_it(
    tmp_path, monkeypatch, stripe_settings, amount
):
    """Without irimi the tools run for real and the refund is sent, the control for the tests above.

    No refund is sent when the model decides nothing."""
    monkeypatch.delenv(ENGINE_ACTIVE_ENV, raising=False)
    monkeypatch.setenv(STRIPE_KEY_ENV, HERMETIC_STRIPE_KEY)
    db = tmp_path / "agent.sqlite3"
    seed.seed(str(db), TICKET[1], CUSTOMER)
    monkeypatch.setenv(DB_ENV, str(db))
    charge = {"id": "ch_A", "amount": 4900, "amount_refunded": 0, "currency": "usd"}
    read: list[str] = []
    refunded: list[dict[str, Any]] = []

    def retrieve(charge_id: str) -> SimpleNamespace:
        read.append(charge_id)
        return SimpleNamespace(**charge)

    told: list[dict[str, Any]] = []

    def ask_model(system: str, content: str) -> dict[str, Any]:
        told.append(json.loads(content))
        return {"refund_amount": amount, "reply": "Done."}

    monkeypatch.setattr(stripe.Charge, "retrieve", retrieve)
    monkeypatch.setattr(stripe.Refund, "create", lambda **params: refunded.append(params))
    monkeypatch.setattr(agent, "ask_model", ask_model)
    assert handle_ticket(Ticket(*TICKET)) == "Done."
    assert read == ["ch_A"]
    assert refunded == ([{"charge": "ch_A", "amount": amount}] if amount else [])
    assert told == [
        {
            "ticket": {"ticket_id": "T1", "charge": "ch_A", "message": "double charged"},
            "customer": {"charge": "ch_A", "name": CUSTOMER},
            "charge": charge,
        }
    ]
    assert _decisions(db) == [("T1", amount, "Done.")]


def test_the_read_tool_raises_for_a_database_that_is_not_there_and_makes_none(
    tmp_path, monkeypatch
):
    """The real `lookup_customer` raises without a database rather than create an empty one.

    So a replay that answers it with the database gone answered from the recording."""
    monkeypatch.delenv(ENGINE_ACTIVE_ENV, raising=False)
    db = tmp_path / "agent.sqlite3"
    monkeypatch.setenv(DB_ENV, str(db))
    with pytest.raises(sqlite3.OperationalError):
        agent.lookup_customer("ch_A")
    assert not db.exists()
    seed.seed(str(db), "ch_A", CUSTOMER)
    assert agent.lookup_customer("ch_A") == {"charge": "ch_A", "name": CUSTOMER}
    assert agent.lookup_customer("ch_B") == {"charge": "ch_B", "name": None}
