"""The tool decorators under replay, against a real engine replaying a stored run.

Each test records a run first, through a shadow engine and a real `DirectoryStore`, then starts a
second engine answering from that run - a `ReplayPolicy` over its `ReplayIndex`, which answers the
control endpoint's lookups too, as `cli.compose.build_engine` builds it - with this process set
up as `irimi shadow` sets up a replay's child: `IRIMI_REPLAY_RUN` names the engine's run. Every
claim about what the replay stored is read back off disk once its engine has stopped.

A read tool's real function raises `_RealReadRan` for the accounts in `_FORBIDDEN`, and a write
tool's always raises `RealWriteRan`, so a test that let either run where it must not fails on it.
"""

import asyncio
import contextlib
import copy
import dataclasses
import http.client
import http.server
import json
import pickle
import threading
import time
import traceback
from collections.abc import Callable, Coroutine, Iterator
from decimal import Decimal
from typing import Any

import pytest

from irimi import sdk
from irimi.cli import runner
from irimi.decide.policy import ShadowPolicy
from irimi.protocol.control import RECORDED_ACTION, RecordedOutcome
from irimi.protocol.env import (
    AGENT_VERSION_ENV,
    CONTROL_ENV,
    ENGINE_ACTIVE_ENV,
    REPLAY_RUN_ENV,
    RUN_ENV,
)
from irimi.protocol.headers import RUN_HEADER
from irimi.protocol.trace import ErrorInfo, RunRecord, ToolCall, new_run_id
from irimi.record import redact
from irimi.record.store.reader import StoreReader
from irimi.record.store.writer import DirectoryStore
from irimi.replay.index import ReplayIndex
from irimi.replay.policy import ReplayPolicy
from irimi.sdk import _context as context
from irimi.sdk import _control_client as client
from irimi.sdk import _runs as runs
from irimi.sdk import _tools as tools
from irimi.sdk._errors import recorded_error
from irimi.sdk._state import state
from tests.support.engine import RealWriteRan, engine_config, start_engine

# The accounts whose real read must not run in this test: the replay must answer them.
_FORBIDDEN: set[str] = set()
# What the real `db.balance` answers, account by account, and how often it has run.
_BALANCES: dict[str, int] = {}
_REAL_READS: list[str] = []
SECRET = "sk_test_" + "a" * 24


class _RealReadRan(AssertionError):
    """A read tool's real function ran where the recording should have answered it."""


@dataclasses.dataclass(frozen=True)
class Statement:
    account: str
    lines: list[int]


def _real(account: str) -> None:
    if account in _FORBIDDEN:
        raise _RealReadRan(account)
    _REAL_READS.append(account)


@sdk.read_tool(name="db.balance")
def balance(account: str) -> dict[str, Any]:
    _real(account)
    if account == "acc_missing":
        raise KeyError(account)
    if account == "acc_mystery":
        raise LookupError("no such account")
    return {"account": account, "balance": _BALANCES.get(account, 100)}


@sdk.read_tool(name="db.balance_async")
async def balance_async(account: str) -> dict[str, Any]:
    _real(account)
    await asyncio.sleep(0)
    return {"account": account, "balance": 100}


@sdk.read_tool(name="db.score")
def score(account: str) -> dict[str, Any]:
    """A result JSON cannot hold: it is recorded as `__irimi_repr__`, which does not revive."""
    _real(account)
    return {"score": Decimal("0.87")}


@sdk.read_tool(name="db.statement")
def statement(account: str) -> Statement:
    _real(account)
    return Statement(account, [1, 2])


@sdk.read_tool(name="db.api_key")
def api_key(account: str) -> dict[str, Any]:
    _real(account)
    return {"account": account, "key": SECRET}


async def _history(account: str) -> list[int]:
    _real(account)
    await asyncio.sleep(0)
    return [1, 2, 3]


@sdk.read_tool(name="db.history")
def history(account: str) -> Coroutine[Any, Any, list[int]]:
    """A sync read that returns a coroutine: recorded with what the coroutine returned."""
    return _history(account)


def _debit_stand_in(account: str, amount: int) -> dict[str, Any]:
    return {"stood_in": True, "account": account, "amount": amount}


@sdk.write_tool(fake=_debit_stand_in, name="db.debit")
def debit(account: str, amount: int) -> dict[str, Any]:
    raise RealWriteRan(account)


@pytest.fixture(autouse=True)
def _a_fresh_world() -> Iterator[None]:
    _FORBIDDEN.clear()
    _BALANCES.clear()
    _REAL_READS.clear()
    yield
    _FORBIDDEN.clear()


class _Irimi:
    """Records a run through one engine, then replays it through another, both on one store."""

    def __init__(self, tmp_path, monkeypatch) -> None:
        self.tmp_path = tmp_path
        self.monkeypatch = monkeypatch
        self.cfg = engine_config(tmp_path, monkeypatch)
        self.root = tmp_path / "store"
        self.key = redact.load_key(tmp_path)
        monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
        monkeypatch.delenv(RUN_ENV, raising=False)
        monkeypatch.delenv(REPLAY_RUN_ENV, raising=False)
        monkeypatch.delenv(AGENT_VERSION_ENV, raising=False)
        monkeypatch.setattr(state, "reporter", client.ControlClient())

    def record(self, script: Callable[[], Any]) -> str:
        """`script` run in one SDK run under a shadow engine: the run's id."""
        self._serve(ShadowPolicy(), DirectoryStore(self.root, self.key), script)
        return self._only_sdk_run(StoreReader(self.root), set())

    def replay(self, run_id: str, script: Callable[[], Any]) -> tuple[Any, list[ToolCall]]:
        """`script` run again in one SDK run under an engine answering from `run_id`, which adopts
        the engine's run as `irimi replay`'s child does: what it returned, and the replay
        run's tool calls as stored."""
        before = {r.run_id for r in StoreReader(self.root).list_runs()}
        events = StoreReader(self.root).load_run(run_id).events
        recording = ReplayIndex.from_events(events, self.key, self.cfg.maps)
        # Each replay is a run of its own, the engine's, as each `irimi replay` mints one.
        self.cfg = dataclasses.replace(self.cfg, run_id=new_run_id())
        store = DirectoryStore(self.root, self.key, replay_of=run_id)
        # Stored before the script runs, as `irimi replay` stores its run before its child exists,
        # so the run is read back whatever reached the engine.
        store.start_run(
            RunRecord.process(self.cfg.run_id, ["script"], None, time.time(), replay_of=run_id)
        )
        self.monkeypatch.setenv(RUN_ENV, self.cfg.run_id)
        self.monkeypatch.setenv(REPLAY_RUN_ENV, self.cfg.run_id)
        policy = ReplayPolicy(recording, maps=self.cfg.maps)
        result = self._serve(policy, store, script)
        reader = StoreReader(self.root)
        # The script's `sdk.run` adopted the replay's run, the engine's own, and posted no start.
        replay_run = self.cfg.run_id
        assert replay_run not in before
        assert reader.load_run(replay_run).record.replay_of == run_id
        calls = [e for e in reader.load_run(replay_run).events if isinstance(e, ToolCall)]
        return result, calls

    def _serve(self, policy, store, script) -> Any:
        eng, _seen, stop = start_engine(self.cfg, policy=policy, store=store)
        self.port = eng.listen_port()
        self.monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{self.port}/_irimi")
        try:
            with sdk.run("script", inputs={"test": True}):
                return script()
        finally:
            stop()

    @staticmethod
    def _only_sdk_run(reader: StoreReader, before: set[str]) -> str:
        [run] = [r for r in reader.list_runs() if r.attribution == "sdk" and r.run_id not in before]
        return run.run_id


@pytest.fixture
def irimi(tmp_path, monkeypatch) -> _Irimi:
    return _Irimi(tmp_path, monkeypatch)


def _post(port: int, path: str, body: object, method: str = "POST") -> tuple[int, dict[str, Any]]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request(method, path, body=json.dumps(body).encode())
    resp = conn.getresponse()
    data = json.loads(resp.read() or b"null")
    conn.close()
    return resp.status, data


# -- tools answered from the recording ------------------------------------------------------------


def test_a_read_tool_the_recording_holds_returns_its_recorded_result_and_never_runs(irimi):
    _BALANCES["acc_1"] = 100
    run_id = irimi.record(lambda: balance("acc_1"))
    # The world moved on since the recording, and the real read may not run at all.
    _BALANCES["acc_1"] = 5
    _FORBIDDEN.add("acc_1")
    result, [call] = irimi.replay(run_id, lambda: balance("acc_1"))
    assert result == {"account": "acc_1", "balance": 100}
    assert (call.name, call.kind, call.ran) == ("db.balance", "read", "recording")
    assert (call.args, call.result, call.error) == ({"account": "acc_1"}, result, None)


def test_the_same_read_tool_with_other_args_runs_the_real_function(irimi):
    run_id = irimi.record(lambda: balance("acc_1"))
    result, [call] = irimi.replay(run_id, lambda: balance("acc_2"))
    assert result == {"account": "acc_2", "balance": 100}
    assert _REAL_READS == ["acc_1", "acc_2"]
    assert (call.ran, call.args) == ("real", {"account": "acc_2"})


def test_a_recorded_key_error_comes_back_as_recorded_tool_error_catchable_as_key_error(irimi):
    def look() -> Any:
        try:
            balance("acc_missing")
        except KeyError as exc:
            return exc

    run_id = irimi.record(look)
    _FORBIDDEN.add("acc_missing")
    raised, [call] = irimi.replay(run_id, look)
    assert isinstance(raised, sdk.RecordedToolError) and isinstance(raised, KeyError)
    assert (raised.type_name, raised.message) == ("builtins.KeyError", "'acc_missing'")
    # Its `str` is the recorded message, not KeyError's quoting of it again.
    assert str(raised) == "'acc_missing'"
    # Named as recorded, module included, so a traceback says `KeyError: 'acc_missing'`.
    assert (type(raised).__module__, type(raised).__qualname__) == ("builtins", "KeyError")
    assert traceback.format_exception_only(raised) == ["KeyError: 'acc_missing'\n"]
    assert (call.ran, call.result) == ("recording", None)
    # Recorded as it was recorded, not as the class it was raised as.
    assert call.error == ErrorInfo("builtins.KeyError", "'acc_missing'")


def test_a_recorded_result_held_as_its_repr_runs_the_real_function(irimi):
    run_id = irimi.record(lambda: score("acc_1"))
    [recorded] = [
        e for e in StoreReader(irimi.root).load_run(run_id).events if isinstance(e, ToolCall)
    ]
    assert recorded.result == {"score": {"__irimi_repr__": "Decimal('0.87')"}}
    result, [call] = irimi.replay(run_id, lambda: score("acc_1"))
    assert result == {"score": Decimal("0.87")}
    assert _REAL_READS == ["acc_1", "acc_1"]
    assert call.ran == "real"


def test_a_write_tool_runs_its_stand_in_in_a_replay_and_never_the_real_function(irimi):
    run_id = irimi.record(lambda: debit("acc_1", 5))
    result, [call] = irimi.replay(run_id, lambda: debit("acc_1", 5))
    assert result == {"stood_in": True, "account": "acc_1", "amount": 5}
    assert (call.kind, call.ran) == ("write", "shadow")


def test_the_lookup_route_answers_404_under_an_engine_that_is_not_replaying(tmp_path, monkeypatch):
    cfg = engine_config(tmp_path, monkeypatch)
    eng, _seen, stop = start_engine(
        cfg, store=DirectoryStore(tmp_path / "store", redact.load_key(tmp_path))
    )
    try:
        status, body = _post(
            eng.listen_port(),
            f"/_irimi/runs/abcd1234abcd1234/{RECORDED_ACTION}",
            {"name": "db.balance", "args": {"account": "acc_1"}},
        )
    finally:
        stop()
    assert (status, body) == (
        404,
        {"error": {"type": "irimi_control_refused", "message": "no recorded call"}},
    )


# -- the rest of the replay branch ---------------------------------------------------------------


def test_two_identical_calls_take_the_two_recorded_results_in_order_and_a_third_runs(irimi):
    def twice() -> list[Any]:
        first = balance("acc_1")
        _BALANCES["acc_1"] = 200
        return [first, balance("acc_1")]

    run_id = irimi.record(twice)
    _FORBIDDEN.add("acc_1")
    result, calls = irimi.replay(run_id, twice)
    assert [r["balance"] for r in result] == [100, 200]
    assert [c.ran for c in calls] == ["recording", "recording"]
    _FORBIDDEN.clear()
    _, calls = irimi.replay(run_id, lambda: [balance("acc_1") for _ in range(3)])
    assert [c.ran for c in calls] == ["recording", "recording", "real"]


def test_an_async_read_tool_is_answered_from_the_recording(irimi):
    run_id = irimi.record(lambda: asyncio.run(balance_async("acc_1")))
    _FORBIDDEN.add("acc_1")
    result, [call] = irimi.replay(run_id, lambda: asyncio.run(balance_async("acc_1")))
    assert result == {"account": "acc_1", "balance": 100}
    assert call.ran == "recording"


def test_a_sync_tool_that_returned_a_coroutine_is_answered_with_a_coroutine_again(irimi):
    """The agent awaits what the tool returns: a replay that handed it the recorded list would
    raise TypeError at its `await`."""

    async def awaits() -> list[int]:
        return await history("acc_1")

    run_id = irimi.record(lambda: asyncio.run(awaits()))
    [recorded] = [
        e for e in StoreReader(irimi.root).load_run(run_id).events if isinstance(e, ToolCall)
    ]
    assert (recorded.result, recorded.awaited) == ([1, 2, 3], True)
    _FORBIDDEN.add("acc_1")
    result, [call] = irimi.replay(run_id, lambda: asyncio.run(awaits()))
    assert result == [1, 2, 3]
    assert (call.ran, call.awaited) == ("recording", True)


def test_a_dataclass_result_is_revived_as_the_dataclass(irimi):
    run_id = irimi.record(lambda: statement("acc_1"))
    _FORBIDDEN.add("acc_1")
    result, [call] = irimi.replay(run_id, lambda: statement("acc_1"))
    assert result == Statement("acc_1", [1, 2])
    assert call.ran == "recording"


def test_a_secret_in_a_recorded_result_comes_back_as_its_placeholder(irimi):
    """The store redacted the result before it wrote it, so the recording holds no secret
    to hand back, and README says so."""
    run_id = irimi.record(lambda: api_key("acc_1"))
    _FORBIDDEN.add("acc_1")
    result, [call] = irimi.replay(run_id, lambda: api_key("acc_1"))
    assert result["key"] != SECRET and result["key"].startswith("<redacted:")
    assert call.ran == "recording"


def test_a_recorded_error_that_is_not_a_builtin_is_a_plain_recorded_tool_error(irimi, monkeypatch):
    run_id = irimi.record(lambda: _raised(lambda: balance("acc_mystery")))
    _FORBIDDEN.add("acc_mystery")
    raised, [call] = irimi.replay(run_id, lambda: _raised(lambda: balance("acc_mystery")))
    # LookupError is a builtin: it is caught as one.
    assert isinstance(raised, LookupError) and isinstance(raised, sdk.RecordedToolError)
    assert call.error == ErrorInfo("builtins.LookupError", "no such account")


def _raised(fn: Callable[[], Any]) -> BaseException | None:
    try:
        fn()
    except Exception as exc:
        return exc
    return None


def test_a_lookup_that_fails_runs_the_tool_for_real_and_warns_once(
    irimi, monkeypatch, warnings, refused_port
):
    run_id = irimi.record(lambda: balance("acc_1"))

    def unreachable() -> Any:
        monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{refused_port}/_irimi")
        return [balance("acc_1"), balance("acc_1")]

    result, calls = irimi.replay(run_id, unreachable)
    assert [r["balance"] for r in result] == [100, 100]
    assert calls == []  # their posts could not reach the engine either
    assert _REAL_READS == ["acc_1", "acc_1", "acc_1"]
    said = [w.getMessage() for w in warnings()]
    [lookup] = [w for w in said if "look up a recorded call of tool db.balance" in w]
    # The failure paused every send, so the posts after it were skipped, and not warned of.
    assert "irimi is not asked again for 30s" in lookup
    assert not [w for w in said if "could not record a tool call" in w]


def test_a_miss_is_said_by_no_warning(irimi, warnings):
    """The endpoint's 404 is a miss, not a failure: the tool runs, and nothing is logged."""
    run_id = irimi.record(lambda: balance("acc_1"))
    irimi.replay(run_id, lambda: balance("acc_2"))
    assert warnings() == []


def test_an_inactive_sdk_never_asks_the_recording(monkeypatch):
    monkeypatch.delenv(ENGINE_ACTIVE_ENV, raising=False)
    monkeypatch.setenv(RUN_ENV, "abcd1234abcd1234")
    monkeypatch.setenv(REPLAY_RUN_ENV, "abcd1234abcd1234")

    class _Refuses:
        def post(self, *args: Any) -> None:
            raise AssertionError("an inactive SDK posts nothing")

        def recorded(self, *args: Any) -> None:
            raise AssertionError("an inactive SDK asks nothing")

    monkeypatch.setattr(state, "reporter", _Refuses())
    assert balance("acc_1") == {"account": "acc_1", "balance": 100}


@pytest.mark.parametrize("run_env", [None, "another0another0"])
def test_a_replay_id_irimi_did_not_set_asks_no_recording(monkeypatch, run_env):
    """An `IRIMI_REPLAY_RUN` beside no `IRIMI_RUN`, or another, is a stale export, not irimi's
    word (`context.replay_run_id`): the read tool runs for real and only posts its call."""
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    monkeypatch.setenv(REPLAY_RUN_ENV, "abcd1234abcd1234")
    if run_env is None:
        monkeypatch.delenv(RUN_ENV, raising=False)
    else:
        monkeypatch.setenv(RUN_ENV, run_env)
    posted: list[str] = []

    class _Writes:
        """Writes down each post and each lookup; a lookup that raised would be swallowed as a
        failed one (`_replay_answer` never raises), so it is counted instead."""

        def post(self, run_id: str, kind: str, payload: Any) -> None:
            posted.append(kind)

        def recorded(self, *args: Any) -> None:
            posted.append("recorded")

    monkeypatch.setattr(state, "reporter", _Writes())
    assert balance("acc_1") == {"account": "acc_1", "balance": 100}
    assert (posted, _REAL_READS) == (["tool-calls"], ["acc_1"])


def test_a_hit_is_used_up_and_is_not_itself_a_tool_call(irimi):
    run_id = irimi.record(lambda: balance("acc_1"))
    events = StoreReader(irimi.root).load_run(run_id).events
    recording = ReplayIndex.from_events(events, irimi.key, irimi.cfg.maps)
    store = DirectoryStore(irimi.root, irimi.key, replay_of=run_id)
    eng, _seen, stop = start_engine(irimi.cfg, policy=ReplayPolicy(recording), store=store)
    route = f"/_irimi/runs/abcd1234abcd1234/{RECORDED_ACTION}"
    posted = {"name": "db.balance", "args": {"account": "acc_1"}}
    try:
        first = _post(eng.listen_port(), route, posted)
        second = _post(eng.listen_port(), route, posted)
    finally:
        stop()
    answer = {"result": {"account": "acc_1", "balance": 100}, "error": None, "awaited": False}
    assert first == (200, answer)
    assert second == (
        404,
        {"error": {"type": "irimi_control_refused", "message": "no recorded call"}},
    )
    assert "abcd1234abcd1234" not in {r.run_id for r in StoreReader(irimi.root).list_runs()}


@pytest.mark.parametrize(
    ("path", "body", "method", "status", "said"),
    [
        ("runs/abcd1234abcd1234/tool-calls/recorded", {"name": "x"}, "POST", 400, "'args'"),
        (
            "runs/abcd1234abcd1234/tool-calls/recorded",
            {"name": 1, "args": {}},
            "POST",
            400,
            "'name'",
        ),
        (
            "runs/abcd1234abcd1234/tool-calls/recorded",
            {"name": "x", "args": {}},
            "GET",
            405,
            "POST",
        ),
        (
            "runs/../tool-calls/recorded",
            {"name": "x", "args": {}},
            "POST",
            400,
            "cannot name a run",
        ),
        ("runs/tool-calls/recorded", {"name": "x", "args": {}}, "POST", 404, "no control route"),
        (
            "runs/abcd1234abcd1234/recorded",
            {"name": "x", "args": {}},
            "POST",
            404,
            "no control route",
        ),
    ],
)
def test_the_lookup_route_is_validated_like_every_control_route(
    tmp_path, monkeypatch, path, body, method, status, said
):
    cfg = engine_config(tmp_path, monkeypatch)
    eng, _seen, stop = start_engine(cfg)
    try:
        got, answer = _post(eng.listen_port(), f"/_irimi/{path}", body, method)
    finally:
        stop()
    assert got == status and said in answer["error"]["message"]


@pytest.mark.parametrize("run_id", ["unattributed", "t3st"])
def test_a_tool_called_in_no_run_or_the_process_run_may_ask_the_recording(irimi, run_id):
    """A tool outside every SDK run is filed in the process run, or in `unattributed` when there
    is none, and asks its recording from there."""
    recorded = irimi.record(lambda: balance("acc_1"))
    events = StoreReader(irimi.root).load_run(recorded).events
    recording = ReplayIndex.from_events(events, irimi.key, irimi.cfg.maps)
    store = DirectoryStore(irimi.root, irimi.key, replay_of=recorded)
    eng, _seen, stop = start_engine(irimi.cfg, policy=ReplayPolicy(recording), store=store)
    try:
        status, _ = _post(
            eng.listen_port(),
            f"/_irimi/runs/{run_id}/{RECORDED_ACTION}",
            {"name": "db.balance", "args": {"account": "acc_1"}},
        )
    finally:
        stop()
    assert status == 200


def test_a_replay_s_child_is_told_it_replays_and_a_shadow_run_s_never_is(tmp_path):
    base = {REPLAY_RUN_ENV: "stale"}
    shadow = runner.child_env(base, "127.0.0.1", 4000, tmp_path / "ca.pem", "abcd1234abcd1234")
    replay = runner.child_env(
        base, "127.0.0.1", 4000, tmp_path / "ca.pem", "abcd1234abcd1234", replaying=True
    )
    assert REPLAY_RUN_ENV not in shadow
    assert replay[REPLAY_RUN_ENV] == "abcd1234abcd1234"
    assert base == {REPLAY_RUN_ENV: "stale"}


def test_a_run_that_ends_on_a_recorded_error_records_the_recorded_type(irimi):
    """The recorded run ends on `builtins.KeyError`. Its replay raises the same, as
    `RecordedToolError`, out of a run that adopted the replay's and so posts no end of its own:
    `irimi replay` ends the replay's run with how its child exited."""

    def ends_on_it() -> None:
        balance("acc_missing")

    with pytest.raises(KeyError):
        irimi.record(ends_on_it)
    [recorded] = [r for r in StoreReader(irimi.root).list_runs() if r.attribution == "sdk"]
    assert recorded.end is not None
    assert recorded.end.error == ErrorInfo("builtins.KeyError", "'acc_missing'")
    _FORBIDDEN.add("acc_missing")
    with pytest.raises(sdk.RecordedToolError) as raised:
        irimi.replay(recorded.run_id, ends_on_it)
    assert isinstance(raised.value, KeyError)
    replayed = StoreReader(irimi.root).load_run(irimi.cfg.run_id).record
    assert replayed.end is None


@pytest.mark.parametrize("kind", ["builtins.KeyboardInterrupt", "builtins.SystemExit"])
def test_a_recorded_builtin_that_is_no_exception_is_never_raised_as_itself(kind):
    """A recorded Ctrl-C is the recording's interruption, not what the tool does: it comes back
    as a plain RecordedToolError, which an agent's `except Exception` may catch."""
    raised = recorded_error(ErrorInfo(kind, ""))
    assert type(raised) is sdk.RecordedToolError


@pytest.mark.parametrize("kind", ["builtins.KeyError", "myapp.errors.NotFound"])
def test_a_recorded_tool_error_pickles_and_copies_as_itself(kind):
    """An agent that hands exceptions across processes (a process pool, a task queue) must not
    break in a replay where it worked in the recording."""
    raised = recorded_error(ErrorInfo(kind, "'acc_1'"))
    for again in (pickle.loads(pickle.dumps(raised)), copy.copy(raised)):
        assert type(again) is type(raised)
        assert (again.type_name, again.message, str(again)) == (kind, "'acc_1'", "'acc_1'")


def test_an_async_tool_outside_a_replay_runs_its_body_without_a_hop_off_the_loop(
    irimi, monkeypatch
):
    """Only a read in a replay waits on the endpoint: shadow mode's async tools go straight to
    their bodies, so an agent whose executor is busy is never held up by one."""
    hops: list[str] = []
    real = runs.off_the_loop

    async def counted(fn: Any, *args: Any) -> Any:
        hops.append(getattr(fn, "__name__", repr(fn)))
        return await real(fn, *args)

    monkeypatch.setattr(tools, "off_the_loop", counted)
    irimi.record(lambda: asyncio.run(balance_async("acc_1")))
    assert "_replay_answer" not in hops


# -- found on the verification pass --------------------------------------------------------------


@sdk.read_tool(name="db.slow")
async def slow(account: str) -> dict[str, Any]:
    _REAL_READS.append(account)
    await asyncio.sleep(30)  # longer than any test waits: the agent's timeout always cancels it
    return {"account": account}


def _times_out() -> str:
    async def main() -> str:
        try:
            await asyncio.wait_for(slow("acc_1"), 0.05)
        except TimeoutError:
            return "timed out"
        return "answered"

    return asyncio.run(main())


def test_a_recorded_call_that_was_cancelled_is_a_miss_and_the_tool_runs(irimi):
    """A call the agent's own timeout cancelled recorded `CancelledError`, which the tool never
    raised. Answered with it, the agent's `except TimeoutError` would meet a RecordedToolError and
    the replay would end where the recording went on; it runs again instead, and is cancelled
    again."""
    run_id = irimi.record(_times_out)
    [recorded] = [
        e for e in StoreReader(irimi.root).load_run(run_id).events if isinstance(e, ToolCall)
    ]
    assert recorded.error == ErrorInfo("asyncio.exceptions.CancelledError", "")
    result, [call] = irimi.replay(run_id, _times_out)
    assert result == "timed out"
    assert _REAL_READS == ["acc_1", "acc_1"]
    assert (call.ran, call.error) == ("real", recorded.error)


def test_a_recorded_call_that_was_closed_unawaited_is_a_miss(monkeypatch):
    """`GeneratorExit` is what a coroutine closed before it finished records: no outcome of the
    tool's own either."""
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    monkeypatch.setenv(RUN_ENV, "abcd1234abcd1234")
    monkeypatch.setenv(REPLAY_RUN_ENV, "abcd1234abcd1234")
    posted: list[Any] = []

    class _Closed:
        def post(self, run_id: str, action: str, doc: Any) -> None:
            posted.append(doc)

        def recorded(self, *args: Any) -> RecordedOutcome:
            return RecordedOutcome(None, ErrorInfo("builtins.GeneratorExit", ""), awaited=False)

    monkeypatch.setattr(state, "reporter", _Closed())
    assert balance("acc_1") == {"account": "acc_1", "balance": 100}
    assert [doc["ran"] for doc in posted] == ["real"]


def test_an_inactive_sdk_is_never_told_it_replays(monkeypatch):
    """`replay_run_id` is the one rule for "this is a replay", which `RunScope` asks too: a
    variable left in an environment irimi is not in front of says nothing."""
    monkeypatch.setenv(RUN_ENV, "abcd1234abcd1234")
    monkeypatch.setenv(REPLAY_RUN_ENV, "abcd1234abcd1234")
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    assert context.replay_run_id() == "abcd1234abcd1234"
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "0")
    assert context.replay_run_id() is None


class _Answers(http.server.BaseHTTPRequestHandler):
    """Answers every request `status` with `body`, and keeps each one's path and headers."""

    status = 200
    body = b""
    seen: list[tuple[str, dict[str, str]]]

    def do_POST(self) -> None:
        self.seen.append((self.path, {k.lower(): v for k, v in self.headers.items()}))
        self.rfile.read(int(self.headers["content-length"]))
        self.send_response(self.status)
        self.send_header("content-length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *args: Any) -> None:
        pass


@contextlib.contextmanager
def _answering(monkeypatch, status: int, body: bytes) -> Iterator[list[tuple[str, dict[str, str]]]]:
    """A control endpoint that answers every request `status` with `body`, for a replay's child."""
    seen: list[tuple[str, dict[str, str]]] = []
    handler = type("Handler", (_Answers,), {"status": status, "body": body, "seen": seen})
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}).start()
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    monkeypatch.setenv(RUN_ENV, "abcd1234abcd1234")
    monkeypatch.setenv(REPLAY_RUN_ENV, "abcd1234abcd1234")
    monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{server.server_address[1]}/_irimi")
    monkeypatch.setattr(state, "reporter", client.ControlClient())
    try:
        yield seen
    finally:
        server.shutdown()
        server.server_close()


_HIT = {"result": {"account": "acc_1", "balance": 1}, "error": None, "awaited": False}


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"not json",
        b"\xff\xfe",
        b"[1]",
        b'{"result": 1}',
        json.dumps({**_HIT, "error": {"type": 1, "message": "x"}}).encode(),
        json.dumps({**_HIT, "awaited": "yes"}).encode(),
        b"[" * 100_000 + b"]" * 100_000,
    ],
)
def test_an_answer_the_sdk_cannot_read_is_a_miss_warned_once(monkeypatch, warnings, body):
    with _answering(monkeypatch, 200, body):
        assert [balance("acc_1"), balance("acc_1")] == [{"account": "acc_1", "balance": 100}] * 2
    said = [w.getMessage() for w in warnings()]
    assert len([w for w in said if "look up a recorded call of tool db.balance" in w]) == 1


def test_an_answer_past_the_bound_is_a_miss_and_is_never_read_whole(monkeypatch, warnings):
    """irimi's own answer is at most a recorded call; one larger is from something else on the
    port, and is not read into the agent's memory to the end."""
    monkeypatch.setattr(client, "MAX_ANSWER", 1000, raising=False)
    body = json.dumps({**_HIT, "result": {"account": "acc_1", "pad": "x" * 2000}}).encode()
    with _answering(monkeypatch, 200, body):
        assert balance("acc_1") == {"account": "acc_1", "balance": 100}
    [said] = [w.getMessage() for w in warnings() if "look up" in w.getMessage()]
    assert "the answer is past 1000 bytes" in said


def test_a_lookup_carries_no_run_header_and_never_goes_through_a_proxy(monkeypatch, refused_port):
    """A control request, as the posts are: unlabelled under `instrument()`, and to the
    endpoint itself whatever `HTTP_PROXY` says."""
    monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{refused_port}")
    with (
        _answering(monkeypatch, 200, json.dumps(_HIT).encode()) as seen,
        sdk.run("labelled", inputs={}),  # which instruments the HTTP clients
    ):
        assert balance("acc_1") == {"account": "acc_1", "balance": 1}
    [lookup] = [h for path, h in seen if path.endswith(RECORDED_ACTION)]
    assert RUN_HEADER not in lookup


class Ledger:
    """Read tools on a method, a static method and a class method, as W3's ledger has."""

    def __init__(self, owner: str) -> None:
        self.owner = owner

    @sdk.read_tool(name="ledger.entry")
    def entry(self, account: str) -> dict[str, Any]:
        _real(account)
        return {"owner": self.owner, "account": account}

    @staticmethod
    @sdk.read_tool(name="ledger.rate")
    def rate(account: str) -> int:
        _real(account)
        return len(_REAL_READS)

    @sdk.read_tool(name="ledger.kind")
    @classmethod
    def kind(cls, account: str) -> str:
        _real(account)
        return f"{cls.__name__}:{len(_REAL_READS)}"


def test_method_tools_and_a_tool_on_a_thread_are_answered_from_the_recording(irimi):
    """The receiver is no argument, so a method's call matches on what it was given; a thread the
    agent hands the run to (`sdk.propagate`) asks from that run."""

    def script() -> list[Any]:
        got: list[Any] = []
        ledger = Ledger("first")

        def ask() -> None:
            got.append(balance("acc_t"))

        worker = threading.Thread(target=sdk.propagate(ask))
        worker.start()
        worker.join()
        return [ledger.entry("acc_m"), Ledger.rate("acc_s"), Ledger.kind("acc_c"), *got]

    run_id = irimi.record(script)
    _FORBIDDEN.update({"acc_m", "acc_s", "acc_c", "acc_t"})
    result, calls = irimi.replay(run_id, script)
    assert result == [
        {"owner": "first", "account": "acc_m"},
        3,
        "Ledger:4",
        {"account": "acc_t", "balance": 100},
    ]
    assert sorted((c.name, c.ran) for c in calls) == [
        ("db.balance", "recording"),
        ("ledger.entry", "recording"),
        ("ledger.kind", "recording"),
        ("ledger.rate", "recording"),
    ]
    assert {c.name: c.args for c in calls}["ledger.entry"] == {"account": "acc_m"}
