"""`@sdk.tool` under replay (#83), against a real engine replaying a stored run.

Each test records a run first, through a shadow engine and a real `DirectoryStore`, then starts a
second engine answering from that run - a `ReplayPolicy` over its `Recording`, which answers the
control endpoint's lookups too, as `cli._build_engine` builds it - with this process set up as
`irimi shadow` sets up a replay's child: `IRIMI_REPLAY_RUN` names the engine's run. Every claim
about what the replay stored is read back off disk once its engine has stopped.

A read tool's real function raises `_RealReadRan` for the accounts in `_FORBIDDEN`, and a write
tool's always raises `_RealWriteRan`, so a test that let either run where it must not fails on it.
"""

import asyncio
import copy
import dataclasses
import http.client
import json
import pickle
from collections.abc import Callable, Coroutine, Iterator
from decimal import Decimal
from typing import Any

import pytest

from irimi import exchange, paths, redact, runner, sdk
from irimi.policy import ReplayPolicy, ShadowPolicy
from irimi.recording import Recording
from irimi.sdk import client, runs
from irimi.sdk.errors import recorded_error
from irimi.store import DirectoryStore, StoreReader
from irimi.trace import ErrorInfo, ToolCall
from tests import test_sdk
from tests.test_engine_mitm import _config, _start

warnings = test_sdk.warnings  # bound here so pytest finds the fixture

# The accounts whose real read must not run in this test: the replay must answer them.
_FORBIDDEN: set[str] = set()
# What the real `db.balance` answers, account by account, and how often it has run.
_BALANCES: dict[str, int] = {}
_REAL_READS: list[str] = []
SECRET = "sk_test_" + "a" * 24


class _RealReadRan(AssertionError):
    """A read tool's real function ran where the recording should have answered it."""


class _RealWriteRan(AssertionError):
    """A write tool's real function ran where only its stand-in may."""


@dataclasses.dataclass(frozen=True)
class Statement:
    account: str
    lines: list[int]


def _real(account: str) -> None:
    if account in _FORBIDDEN:
        raise _RealReadRan(account)
    _REAL_READS.append(account)


@sdk.tool(kind="read", name="db.balance")
def balance(account: str) -> dict[str, Any]:
    _real(account)
    if account == "acc_missing":
        raise KeyError(account)
    if account == "acc_mystery":
        raise LookupError("no such account")
    return {"account": account, "balance": _BALANCES.get(account, 100)}


@sdk.tool(kind="read", name="db.balance_async")
async def balance_async(account: str) -> dict[str, Any]:
    _real(account)
    await asyncio.sleep(0)
    return {"account": account, "balance": 100}


@sdk.tool(kind="read", name="db.score")
def score(account: str) -> dict[str, Any]:
    """A result JSON cannot hold: it is recorded as `__irimi_repr__`, which does not revive."""
    _real(account)
    return {"score": Decimal("0.87")}


@sdk.tool(kind="read", name="db.statement")
def statement(account: str) -> Statement:
    _real(account)
    return Statement(account, [1, 2])


@sdk.tool(kind="read", name="db.api_key")
def api_key(account: str) -> dict[str, Any]:
    _real(account)
    return {"account": account, "key": SECRET}


async def _history(account: str) -> list[int]:
    _real(account)
    await asyncio.sleep(0)
    return [1, 2, 3]


@sdk.tool(kind="read", name="db.history")
def history(account: str) -> Coroutine[Any, Any, list[int]]:
    """A sync read that returns a coroutine: recorded with what the coroutine returned (#76)."""
    return _history(account)


def _debit_stand_in(account: str, amount: int) -> dict[str, Any]:
    return {"stood_in": True, "account": account, "amount": amount}


@sdk.tool(kind="write", shadow=_debit_stand_in, name="db.debit")
def debit(account: str, amount: int) -> dict[str, Any]:
    raise _RealWriteRan(account)


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
        self.cfg = _config(tmp_path, monkeypatch)
        self.root = tmp_path / "store"
        self.key = redact.load_key(tmp_path)
        monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
        monkeypatch.delenv(paths.RUN_ENV, raising=False)
        monkeypatch.delenv(paths.REPLAY_RUN_ENV, raising=False)
        monkeypatch.delenv(paths.AGENT_VERSION_ENV, raising=False)
        monkeypatch.setattr(runs, "reporter", client.ControlClient())

    def record(self, script: Callable[[], Any]) -> str:
        """`script` run in one SDK run under a shadow engine: the run's id."""
        self._serve(ShadowPolicy(), DirectoryStore(self.root, self.key), script)
        return self._only_sdk_run(StoreReader(self.root), set())

    def replay(self, run_id: str, script: Callable[[], Any]) -> tuple[Any, list[ToolCall]]:
        """`script` run again in one SDK run under an engine answering from `run_id`: what it
        returned, and the replay run's tool calls as stored."""
        before = {r.run_id for r in StoreReader(self.root).list_runs()}
        events = StoreReader(self.root).load_run(run_id).events
        recording = Recording.from_events(events, self.key, self.cfg.maps)
        store = DirectoryStore(self.root, self.key, replay_of=run_id)
        self.monkeypatch.setenv(paths.REPLAY_RUN_ENV, self.cfg.run_id)
        policy = ReplayPolicy(recording, maps=self.cfg.maps)
        result = self._serve(policy, store, script)
        reader = StoreReader(self.root)
        replay_run = self._only_sdk_run(reader, before)
        assert reader.load_run(replay_run).record.replay_of == run_id
        calls = [e for e in reader.load_run(replay_run).events if isinstance(e, ToolCall)]
        return result, calls

    def _serve(self, policy, store, script) -> Any:
        eng, _seen, stop = _start(self.cfg, policy=policy, store=store)
        self.port = eng.listen_port()
        self.monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{self.port}/_irimi")
        try:
            with sdk.run(trigger={"test": True}, name="script"):
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


# -- the issue's "Done when" -----------------------------------------------------------------------


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
    assert (raised.type, raised.message) == ("builtins.KeyError", "'acc_missing'")
    # Its `str` is the recorded message, not KeyError's quoting of it again.
    assert str(raised) == "'acc_missing'"
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
    cfg = _config(tmp_path, monkeypatch)
    eng, _seen, stop = _start(
        cfg, store=DirectoryStore(tmp_path / "store", redact.load_key(tmp_path))
    )
    try:
        status, body = _post(
            eng.listen_port(),
            f"/_irimi/runs/abcd1234abcd1234/{exchange.RECORDED_ACTION}",
            {"name": "db.balance", "args": {"account": "acc_1"}},
        )
    finally:
        stop()
    assert (status, body) == (404, {"error": "no recorded call"})


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
    raise TypeError at its `await` (#83)."""

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
    """The store redacted the result before it wrote it (#69), so the recording holds no secret
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


def test_a_lookup_that_fails_runs_the_tool_for_real_and_warns_once(irimi, monkeypatch, warnings):
    run_id = irimi.record(lambda: balance("acc_1"))

    def unreachable() -> Any:
        monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{test_sdk._closed_port()}/_irimi")
        return [balance("acc_1"), balance("acc_1")]

    result, calls = irimi.replay(run_id, unreachable)
    assert [r["balance"] for r in result] == [100, 100]
    assert calls == []  # their posts could not reach the engine either
    assert _REAL_READS == ["acc_1", "acc_1", "acc_1"]
    said = [w.getMessage() for w in warnings()]
    assert len([w for w in said if "look up a recorded call of tool db.balance" in w]) == 1
    # A failed lookup never silences a post's warning of the same kind.
    assert len([w for w in said if "could not record a tool call" in w]) == 1


def test_a_miss_is_said_by_no_warning(irimi, warnings):
    """The endpoint's 404 is a miss, not a failure: the tool runs, and nothing is logged."""
    run_id = irimi.record(lambda: balance("acc_1"))
    irimi.replay(run_id, lambda: balance("acc_2"))
    assert warnings() == []


def test_an_inactive_sdk_never_asks_the_recording(monkeypatch):
    monkeypatch.delenv(paths.ENGINE_ACTIVE_ENV, raising=False)
    monkeypatch.setenv(paths.REPLAY_RUN_ENV, "abcd1234abcd1234")

    class _Refuses:
        def post(self, *args: Any) -> None:
            raise AssertionError("an inactive SDK posts nothing")

        def recorded(self, *args: Any) -> None:
            raise AssertionError("an inactive SDK asks nothing")

    monkeypatch.setattr(runs, "reporter", _Refuses())
    assert balance("acc_1") == {"account": "acc_1", "balance": 100}


def test_a_hit_is_used_up_and_is_not_itself_a_tool_call(irimi):
    run_id = irimi.record(lambda: balance("acc_1"))
    events = StoreReader(irimi.root).load_run(run_id).events
    recording = Recording.from_events(events, irimi.key, irimi.cfg.maps)
    store = DirectoryStore(irimi.root, irimi.key, replay_of=run_id)
    eng, _seen, stop = _start(irimi.cfg, policy=ReplayPolicy(recording), store=store)
    route = f"/_irimi/runs/abcd1234abcd1234/{exchange.RECORDED_ACTION}"
    posted = {"name": "db.balance", "args": {"account": "acc_1"}}
    try:
        first = _post(eng.listen_port(), route, posted)
        second = _post(eng.listen_port(), route, posted)
    finally:
        stop()
    answer = {"result": {"account": "acc_1", "balance": 100}, "error": None, "awaited": False}
    assert first == (200, answer)
    assert second == (404, {"error": "no recorded call"})
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
            {"name": "x", "args": {}, "run_id": "y"},
            "POST",
            400,
            "unknown field 'run_id'",
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
    cfg = _config(tmp_path, monkeypatch)
    eng, _seen, stop = _start(cfg)
    try:
        got, answer = _post(eng.listen_port(), f"/_irimi/{path}", body, method)
    finally:
        stop()
    assert got == status and said in answer["error"]


@pytest.mark.parametrize("run_id", ["unattributed", "t3st"])
def test_a_tool_called_in_no_run_or_the_process_run_may_ask_the_recording(irimi, run_id):
    """A tool outside every SDK run is filed in the process run, or in `unattributed` when there
    is none (#76), and asks its recording from there."""
    recorded = irimi.record(lambda: balance("acc_1"))
    events = StoreReader(irimi.root).load_run(recorded).events
    recording = Recording.from_events(events, irimi.key, irimi.cfg.maps)
    store = DirectoryStore(irimi.root, irimi.key, replay_of=recorded)
    eng, _seen, stop = _start(irimi.cfg, policy=ReplayPolicy(recording), store=store)
    try:
        status, _ = _post(
            eng.listen_port(),
            f"/_irimi/runs/{run_id}/{exchange.RECORDED_ACTION}",
            {"name": "db.balance", "args": {"account": "acc_1"}},
        )
    finally:
        stop()
    assert status == 200


def test_a_replay_s_child_is_told_it_replays_and_a_shadow_run_s_never_is(tmp_path):
    base = {paths.REPLAY_RUN_ENV: "stale"}
    shadow = runner.child_env(base, "127.0.0.1", 4000, tmp_path / "ca.pem", "abcd1234abcd1234")
    replay = runner.child_env(
        base, "127.0.0.1", 4000, tmp_path / "ca.pem", "abcd1234abcd1234", replaying=True
    )
    assert paths.REPLAY_RUN_ENV not in shadow
    assert replay[paths.REPLAY_RUN_ENV] == "abcd1234abcd1234"
    assert base == {paths.REPLAY_RUN_ENV: "stale"}


def test_a_run_that_ends_on_a_recorded_error_records_the_recorded_type(irimi):
    """The replay's run ends where its recording's did, on the same `builtins.KeyError`, so
    `irimi runs show` and a comparison say one thing of both (#83)."""

    def ends_on_it() -> None:
        balance("acc_missing")

    with pytest.raises(KeyError):
        irimi.record(ends_on_it)
    [recorded] = [r for r in StoreReader(irimi.root).list_runs() if r.attribution == "sdk"]
    _FORBIDDEN.add("acc_missing")
    before = {r.run_id for r in StoreReader(irimi.root).list_runs()}
    with pytest.raises(sdk.RecordedToolError):
        irimi.replay(recorded.run_id, ends_on_it)
    [replayed] = [
        r
        for r in StoreReader(irimi.root).list_runs()
        if r.attribution == "sdk" and r.run_id not in before
    ]
    assert replayed.error == recorded.error == ErrorInfo("builtins.KeyError", "'acc_missing'")


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
        assert (again.type, again.message, str(again)) == (kind, "'acc_1'", "'acc_1'")


def test_an_async_tool_outside_a_replay_runs_its_body_without_a_hop_off_the_loop(
    irimi, monkeypatch
):
    """Only a read in a replay waits on the endpoint: shadow mode's async tools go straight to
    their bodies, so an agent whose executor is busy is never held up by one (#83)."""
    hops: list[str] = []
    real = runs.off_the_loop

    async def counted(fn: Any, *args: Any) -> Any:
        hops.append(getattr(fn, "__name__", repr(fn)))
        return await real(fn, *args)

    monkeypatch.setattr(runs, "off_the_loop", counted)
    irimi.record(lambda: asyncio.run(balance_async("acc_1")))
    assert "_replay_answer" not in hops
