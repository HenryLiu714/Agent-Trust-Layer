"""`@sdk.read_tool` and `@sdk.write_tool` against a real engine and store, read back off disk.

A write tool's real function raises `RealWriteRan` wherever a test says it must not run."""

import asyncio
import functools
import inspect
import json
import re
import sys
import threading
from collections.abc import Callable
from typing import Any

import pytest

from irimi import sdk, servicemaps
from irimi.cli import main
from irimi.config import ca, paths
from irimi.core import limits
from irimi.protocol import trace
from irimi.protocol.env import CONTROL_ENV, ENGINE_ACTIVE_ENV, RUN_ENV
from irimi.protocol.trace import ErrorInfo, ToolCall
from irimi.record.store.reader import StoreReader
from irimi.report.model import RunView
from irimi.report.summary import summary_lines
from irimi.sdk import _capture as capture
from irimi.sdk import _control_client as client
from irimi.sdk import _tools as tools
from irimi.sdk._state import state
from tests.support.engine import RealWriteRan

ENGINE_RUN = "t3st"  # the engine's own run, `engine_config`'s: the process run's place


def _stand_in(order: int, *, note: str = "") -> dict[str, Any]:
    return {"stood_in": order, "note": note}


async def _stand_in_async(order: int, *, note: str = "") -> dict[str, Any]:
    await asyncio.sleep(0)
    return {"stood_in": order, "note": note}


@sdk.write_tool(fake=_stand_in, name="db.mark_refunded")
def mark_refunded(order: int, *, note: str = "") -> dict[str, Any]:
    raise RealWriteRan("the real write ran")


@sdk.write_tool(fake=_stand_in_async, name="db.mark_refunded_async")
async def mark_refunded_async(order: int, *, note: str = "") -> dict[str, Any]:
    raise RealWriteRan("the real write ran")


@sdk.read_tool
def look_up(order: int) -> dict[str, Any]:
    return {"order": order, "status": "paid"}


def _tool_calls(reader: StoreReader, run_id: str) -> list[ToolCall]:
    return [e for e in reader.load_run(run_id).events if isinstance(e, ToolCall)]


def _only_sdk_run(reader: StoreReader) -> str:
    [run] = [r for r in reader.list_runs() if r.attribution == "sdk"]
    return run.run_id


class _Posted:
    """A `client.Reporter` that keeps every post it is given."""

    def __init__(self) -> None:
        self.posts: list[tuple[str, str, dict[str, Any]]] = []

    def post(self, run_id: str, action: str, doc: Any) -> None:
        self.posts.append((run_id, action, dict(doc)))


@pytest.fixture
def posted(monkeypatch) -> _Posted:
    """The SDK active, with no engine: what it would post, kept in order."""
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    monkeypatch.delenv(RUN_ENV, raising=False)
    reporter = _Posted()
    monkeypatch.setattr(state, "reporter", reporter)
    return reporter


@pytest.fixture
def no_warning_yet(monkeypatch) -> None:
    """The once-per-process WARNING not yet given, as in a fresh agent process."""
    monkeypatch.setattr(state, "warned", set())


# -- decoration time: the scope rule's shape, applied to the SDK -----------------------------------


def _sync(order: int) -> int:
    return order


async def _async(order: int) -> int:
    return order


def _gen(order: int) -> Any:
    yield order


async def _agen(order: int) -> Any:
    yield order


class _Klass:
    pass


MISTAKES: dict[str, tuple[Callable[[], Any], str]] = {
    "write_without_fake": (
        lambda: sdk.write_tool()(_sync),  # type: ignore[call-arg]
        "missing 1 required keyword-only argument: 'fake'",
    ),
    "read_with_fake": (
        lambda: sdk.read_tool(fake=_sync)(_sync),  # type: ignore[call-overload]
        "unexpected keyword argument 'fake'",
    ),
    "bare_write_tool": (lambda: sdk.write_tool(_sync), "positional"),  # type: ignore[call-arg]
    "name_positional": (lambda: sdk.read_tool("lookup"), "takes its name as name="),  # type: ignore[call-overload]
    "fake_none": (lambda: sdk.write_tool(fake=None)(_sync), "must be callable"),  # type: ignore[arg-type]
    "stand_in_not_callable": (
        lambda: sdk.write_tool(fake="mock_database_write")(_sync),  # type: ignore[arg-type]
        "must be callable",
    ),
    "sync_stand_in_for_async": (
        lambda: sdk.write_tool(fake=_sync)(_async),
        "must be an async def",
    ),
    "async_stand_in_for_sync": (
        lambda: sdk.write_tool(fake=_async)(_sync),
        "must be a sync function",
    ),
    "generator": (lambda: sdk.read_tool(_gen), "generator function"),
    "async_generator": (lambda: sdk.read_tool(_agen), "generator function"),
    "generator_stand_in": (
        lambda: sdk.write_tool(fake=_gen)(_sync),
        "cannot be the generator function",
    ),
    "name_not_a_string": (lambda: sdk.read_tool(name=7)(_sync), "must be a string"),  # type: ignore[call-overload]
    "write_name_not_a_string": (
        lambda: sdk.write_tool(fake=_sync, name=7)(_sync),  # type: ignore[arg-type]
        "@sdk.write_tool(name=...) must be a string",
    ),
    "partial": (lambda: sdk.read_tool(functools.partial(_sync, 1)), "@sdk.read_tool wraps"),
    "named_partial": (
        lambda: sdk.write_tool(fake=_sync, name="db.write")(functools.partial(_sync, 1)),
        "@sdk.write_tool wraps",
    ),
    "class": (lambda: sdk.read_tool(_Klass), "@sdk.read_tool wraps a function or a method"),
}


@pytest.mark.parametrize("case", MISTAKES)
def test_each_decoration_time_mistake_raises_type_error_before_any_call(case, posted):
    attempt, message = MISTAKES[case]
    with pytest.raises(TypeError, match=re.escape(message)):
        attempt()
    assert posted.posts == []


def test_a_bare_read_tool_is_a_read_tool():
    lookup = sdk.read_tool(_sync)
    assert lookup(3) == 3
    assert lookup.__name__ == "_sync"


def test_the_old_tool_decorator_is_gone():
    assert not hasattr(sdk, "tool")


def test_a_tool_keeps_its_functions_name_signature_and_nature():
    assert mark_refunded.__name__ == "mark_refunded"
    assert list(inspect.signature(mark_refunded).parameters) == ["order", "note"]
    assert inspect.iscoroutinefunction(mark_refunded_async)
    assert not inspect.iscoroutinefunction(mark_refunded)


# -- active: a write runs only its fake, a read runs for real, and both are recorded ---------------


def test_an_active_write_runs_only_its_stand_in_and_is_stored_in_its_run(irimi):
    @sdk.trigger
    def refund(order: int) -> Any:
        return mark_refunded(order, note="dup")

    assert refund(7) == {"stood_in": 7, "note": "dup"}
    reader = irimi.finish()
    run_id = _only_sdk_run(reader)
    [call] = _tool_calls(reader, run_id)
    assert (call.name, call.kind, call.ran) == ("db.mark_refunded", "write", "shadow")
    # Bound against the real function's signature, with no defaults applied.
    assert call.args == {"order": 7, "note": "dup"}
    assert call.result == {"stood_in": 7, "note": "dup"}
    assert call.error is None
    assert call.run_id == run_id
    assert re.fullmatch(r"[0-9a-f]{16}", call.tool_call_id)
    assert call.started_at <= call.ended_at


def test_an_active_read_runs_for_real_and_is_stored_as_real(irimi):
    @sdk.trigger
    def check(order: int) -> Any:
        return look_up(order)

    assert check(3) == {"order": 3, "status": "paid"}
    reader = irimi.finish()
    [call] = _tool_calls(reader, _only_sdk_run(reader))
    assert (call.name, call.kind, call.ran) == (f"{__name__}:look_up", "read", "real")
    assert (call.args, call.result) == ({"order": 3}, {"order": 3, "status": "paid"})


def test_an_async_tool_runs_its_async_stand_in_and_is_stored(irimi):
    @sdk.trigger
    async def refund(order: int) -> Any:
        return await mark_refunded_async(order)

    assert asyncio.run(refund(5)) == {"stood_in": 5, "note": ""}
    reader = irimi.finish()
    [call] = _tool_calls(reader, _only_sdk_run(reader))
    assert (call.name, call.ran, call.args, call.result) == (
        "db.mark_refunded_async",
        "shadow",
        {"order": 5},
        {"stood_in": 5, "note": ""},
    )


def test_a_stand_in_that_raises_re_raises_the_same_object_and_records_its_error(irimi):
    raised = KeyError("acc_missing")

    def failing_stand_in(order: int) -> Any:
        raise raised

    @sdk.write_tool(fake=failing_stand_in, name="db.fails")
    def fails(order: int) -> Any:
        raise RealWriteRan("the real write ran")

    @sdk.trigger
    def refund() -> None:
        fails(1)

    with pytest.raises(KeyError) as caught:
        refund()
    assert caught.value is raised
    reader = irimi.finish()
    [call] = _tool_calls(reader, _only_sdk_run(reader))
    assert call.error == ErrorInfo("builtins.KeyError", "'acc_missing'")
    assert call.result is None


def test_a_tool_called_under_irimi_shadow_with_no_trigger_lands_in_the_process_run(
    irimi, monkeypatch
):
    """Under `irimi shadow` a tool call made in no SDK run lands in the process run."""
    monkeypatch.setenv(RUN_ENV, ENGINE_RUN)
    assert mark_refunded(9) == {"stood_in": 9, "note": ""}
    reader = irimi.finish()
    [call] = _tool_calls(reader, ENGINE_RUN)
    assert (call.run_id, call.ran) == (ENGINE_RUN, "shadow")
    assert [r.attribution for r in reader.list_runs()] == ["header"]


def test_a_tool_called_in_no_run_with_no_process_run_is_stored_unattributed(irimi, monkeypatch):
    """Under `irimi serve`, which sets no `IRIMI_RUN`, a call in no run is stored unattributed."""
    monkeypatch.delenv(RUN_ENV, raising=False)
    assert look_up(4) == {"order": 4, "status": "paid"}
    reader = irimi.finish()
    [call] = _tool_calls(reader, trace.UNATTRIBUTED)
    assert (call.run_id, call.name, call.ran) == (trace.UNATTRIBUTED, f"{__name__}:look_up", "real")
    assert reader.list_runs() == []


def test_a_tool_call_is_posted_to_its_runs_route_with_no_run_id_in_the_body(posted):
    with sdk.run("r"):
        run_id = sdk.current_run_id()
        mark_refunded(1)
    [(posted_run, action, doc)] = [p for p in posted.posts if p[1] == "tool-calls"]
    assert (posted_run, action) == (run_id, "tool-calls")
    assert "run_id" not in doc
    # What is posted decodes as a ToolCall once the route's run id is added, as the endpoint does.
    call = trace.tool_call_from_json({**doc, "run_id": posted_run})
    assert (call.name, call.kind, call.ran) == ("db.mark_refunded", "write", "shadow")
    assert [a for _, a, _ in posted.posts] == ["start", "tool-calls", "end"]


def test_the_args_are_bound_against_the_real_function_not_the_stand_in(posted):
    """Args are bound by the real function's parameter names, which a replay binds against.

    A fake that takes anything would otherwise record `{"args": [...], "kwargs": {...}}`."""

    def anything(*args: Any, **kwargs: Any) -> str:
        return "stood in"

    @sdk.write_tool(fake=anything, name="db.charge")
    def charge(customer: str, amount: int) -> str:
        raise RealWriteRan("the real write ran")

    assert charge("cus_1", amount=4900) == "stood in"
    [(_, _, doc)] = posted.posts
    assert doc["args"] == {"customer": "cus_1", "amount": 4900}


def test_the_args_are_captured_before_the_body_runs(posted):
    def appending_stand_in(rows: list[int]) -> int:
        rows.append(99)
        return len(rows)

    @sdk.write_tool(fake=appending_stand_in, name="db.append")
    def append(rows: list[int]) -> int:
        raise RealWriteRan("the real write ran")

    assert append([1, 2]) == 3
    [(_, _, doc)] = posted.posts
    assert (doc["args"], doc["result"]) == ({"rows": [1, 2]}, 3)


def test_args_and_result_together_stay_inside_one_capture_budget(posted):
    """A result that takes args and result past one capture budget becomes the truncation marker.

    The post must stay under the control endpoint's 2 MiB; the call is still recorded."""
    half = "x" * (limits.MAX_CAPTURED_BYTES // 2 + 10)

    @sdk.read_tool(name="db.echo")
    def echo(text: str) -> str:
        return text

    assert echo(half) == half
    [(_, _, doc)] = posted.posts
    assert doc["args"] == {"text": half}
    assert doc["result"] == {trace.TRUNCATED_KEY: True, "bytes": len(half) + 2}
    assert len(capture.serialized(doc)) < 2 * limits.MAX_CAPTURED_BYTES


def test_a_sync_tool_that_returns_a_coroutine_is_recorded_when_the_coroutine_ends(posted):
    async def body(order: int) -> int:
        await asyncio.sleep(0)
        return order * 2

    @sdk.read_tool(name="db.later")
    def later(order: int) -> Any:
        return body(order)

    coroutine = later(4)
    assert posted.posts == []  # nothing has run yet
    assert asyncio.run(coroutine) == 8
    [(_, _, doc)] = posted.posts
    assert (doc["args"], doc["result"]) == ({"order": 4}, 8)


class _Ledger:
    def _mark_stand_in(self, order: int) -> str:
        return f"stood in for {order}"

    @sdk.write_tool(fake=_mark_stand_in, name="ledger.mark")
    def mark(self, order: int) -> str:
        raise RealWriteRan("the real write ran")

    @sdk.write_tool(fake=lambda order: f"static {order}", name="ledger.static_below")
    @staticmethod
    def static_below(order: int) -> str:
        raise RealWriteRan("the real write ran")

    @staticmethod
    @sdk.write_tool(fake=lambda order: f"static {order}", name="ledger.static_above")
    def static_above(order: int) -> str:
        raise RealWriteRan("the real write ran")

    @sdk.read_tool(name="ledger.kind")
    @classmethod
    def kind(cls, order: int) -> str:
        return f"{cls.__name__} {order}"


def test_a_method_tool_leaves_self_out_of_its_args_and_keeps_static_and_class_methods(posted):
    ledger = _Ledger()
    assert ledger.mark(1) == "stood in for 1"
    assert ledger.static_below(2) == _Ledger.static_below(2) == "static 2"
    assert ledger.static_above(3) == "static 3"
    assert _Ledger.kind(4) == "_Ledger 4"
    assert [(doc["name"], doc["args"]) for _, _, doc in posted.posts] == [
        ("ledger.mark", {"order": 1}),
        ("ledger.static_below", {"order": 2}),
        ("ledger.static_below", {"order": 2}),
        ("ledger.static_above", {"order": 3}),
        ("ledger.kind", {"order": 4}),
    ]


def test_an_async_stand_in_that_raises_re_raises_the_same_object_and_records_its_error(posted):
    raised = ValueError("card declined")

    async def failing(order: int) -> Any:
        await asyncio.sleep(0)
        raise raised

    @sdk.write_tool(fake=failing, name="db.fails_async")
    async def fails(order: int) -> Any:
        raise RealWriteRan("the real write ran")

    with pytest.raises(ValueError) as caught:  # noqa: PT011  # checked by identity below
        asyncio.run(fails(1))
    assert caught.value is raised
    [(_, _, doc)] = posted.posts
    assert (doc["result"], doc["error"]) == (
        None,
        {"type": "builtins.ValueError", "message": "card declined"},
    )


def test_an_async_tool_called_with_the_wrong_arguments_is_recorded_with_the_type_error(posted):
    with pytest.raises(TypeError):
        asyncio.run(mark_refunded_async(1, 2))  # type: ignore[call-arg, arg-type]
    [(_, _, doc)] = posted.posts
    assert doc["error"]["type"] == "builtins.TypeError"
    assert doc["args"] == {"args": [1, 2], "kwargs": {}}  # it did not bind (`capture_call`)


def test_a_cancelled_async_tool_is_recorded_as_cancelled_and_stays_cancelled(posted):
    async def slow(order: int) -> str:
        await asyncio.sleep(10)
        return "never"

    @sdk.write_tool(fake=slow, name="db.slow")
    async def write(order: int) -> str:
        raise RealWriteRan("the real write ran")

    async def main() -> None:
        await asyncio.wait_for(write(1), 0.05)

    with pytest.raises(TimeoutError):
        asyncio.run(main())
    [(_, _, doc)] = posted.posts
    assert doc["error"]["type"] == "asyncio.exceptions.CancelledError"


class _Pause:
    """Suspends a coroutine once, with no event loop: what a coroutine driven by hand awaits."""

    def __await__(self) -> Any:
        yield


def test_an_async_tool_closed_mid_call_records_the_close_and_raises_nothing(posted):
    async def waits(order: int) -> str:
        await _Pause()
        return "never"

    @sdk.write_tool(fake=waits, name="db.waits")
    async def write(order: int) -> str:
        raise RealWriteRan("the real write ran")

    coroutine = write(1)
    coroutine.send(None)  # to its first suspension: the fake is waiting
    coroutine.close()
    [(_, _, doc)] = posted.posts
    assert doc["error"]["type"] == "builtins.GeneratorExit"


def test_an_async_tools_result_is_captured_on_the_callers_thread_as_it_was_returned(posted):
    """An async tool's result is captured where it returned, before another task can change it."""
    shared = {"status": "as returned"}
    seen: list[str] = []

    class _Repr:
        def __repr__(self) -> str:
            seen.append(threading.current_thread().name)
            return "<thing>"

    @sdk.read_tool(name="db.shared")
    async def read() -> list[Any]:
        return [_Repr(), shared]

    async def main() -> None:
        async def mutate() -> None:
            await asyncio.sleep(0)
            shared["status"] = "changed after the return"

        await asyncio.gather(read(), mutate())

    asyncio.run(main())
    [(_, _, doc)] = posted.posts
    assert doc["result"] == [{trace.REPR_KEY: "<thing>"}, {"status": "as returned"}]
    assert seen == [threading.current_thread().name]


def test_a_record_that_cannot_be_built_costs_the_agent_nothing(posted, monkeypatch, warnings):
    def broken(value: object) -> Any:
        raise RecursionError("too deep to record")

    monkeypatch.setattr(tools, "capture", broken)
    monkeypatch.setattr(state, "warned", set())
    assert look_up(1) == {"order": 1, "status": "paid"}
    assert look_up(2) == {"order": 2, "status": "paid"}
    assert posted.posts == []
    assert len(warnings()) == 1


def test_a_stand_in_that_is_the_function_itself_is_refused_when_the_agent_loads(posted):
    """`fake=` naming the write itself is refused: it would run the real write under irimi."""

    def database_write(order: int) -> None:
        raise RealWriteRan("the real write ran")

    with pytest.raises(TypeError, match="is the function itself"):
        sdk.write_tool(fake=database_write)(database_write)
    with pytest.raises(TypeError, match="is the function itself"):
        sdk.write_tool(fake=database_write)(staticmethod(database_write))
    assert posted.posts == []


class _Repository:
    """Stand-ins written as static methods in the class body of the tools they stand in for."""

    @staticmethod
    def _save_stand_in(row: int) -> str:
        return f"stood in for {row}"

    @staticmethod
    async def _save_async_stand_in(row: int) -> str:
        return f"stood in async for {row}"

    @sdk.write_tool(fake=_save_stand_in, name="repo.save")
    @staticmethod
    def save(row: int) -> str:
        raise RealWriteRan("the real write ran")

    @sdk.write_tool(fake=_save_async_stand_in, name="repo.save_async")
    @staticmethod
    async def save_async(row: int) -> str:
        raise RealWriteRan("the real write ran")


def test_a_static_method_stand_in_is_judged_and_called_as_its_function(posted):
    """A `staticmethod` fake is judged by its function, which `inspect` cannot see through it.

    Judged as the wrapper, an async one would be refused as sync and a generator let through."""
    assert _Repository.save(1) == "stood in for 1"
    assert asyncio.run(_Repository.save_async(2)) == "stood in async for 2"
    assert [doc["name"] for _, _, doc in posted.posts] == ["repo.save", "repo.save_async"]
    with pytest.raises(TypeError, match="cannot be the generator function"):
        sdk.write_tool(fake=staticmethod(_gen))(_sync)


def test_an_object_with_an_async_call_is_an_async_stand_in(posted):
    class _Mock:
        async def __call__(self, order: int) -> str:
            return "mocked"

    @sdk.write_tool(fake=_Mock(), name="db.mocked")
    async def write(order: int) -> str:
        raise RealWriteRan("the real write ran")

    assert asyncio.run(write(1)) == "mocked"
    with pytest.raises(TypeError, match="must be a sync function"):
        sdk.write_tool(fake=_Mock())(_sync)


def test_a_failed_post_never_reaches_the_agent(irimi, monkeypatch, warnings):
    monkeypatch.setenv(CONTROL_ENV, "http://127.0.0.1:1/_irimi")
    with sdk.run("r"):
        assert mark_refunded(1) == {"stood_in": 1, "note": ""}
    assert [r.levelname for r in warnings()] == ["WARNING"]
    assert "could not record the start of run" in warnings()[0].getMessage()
    monkeypatch.setattr(state, "reporter", client.ControlClient())
    mark_refunded(2)
    assert "could not record a tool call of run" in warnings()[-1].getMessage()


# -- inactive: the real function, and nothing recorded --------------------------------------------


def test_an_inactive_write_runs_for_real_posts_nothing_and_warns_once_when_a_proxy_is_set(
    monkeypatch, warnings, no_warning_yet
):
    monkeypatch.delenv(ENGINE_ACTIVE_ENV, raising=False)
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    reporter = _Posted()
    monkeypatch.setattr(state, "reporter", reporter)
    ran: list[int] = []

    @sdk.write_tool(fake=lambda order: "stood in", name="db.inactive")
    def write(order: int) -> str:
        ran.append(order)
        return "real"

    assert write(1) == write(2) == "real"
    assert ran == [1, 2]
    assert reporter.posts == []
    # Another write tool in the same process runs for real too, and is not warned of again.
    with pytest.raises(RealWriteRan):
        mark_refunded(3)
    assert [r.getMessage() for r in warnings()] == [
        "irimi: write tool db.inactive ran for real: HTTPS_PROXY is set but "
        "IRIMI_ENGINE_ACTIVE is not"
    ]


@pytest.mark.parametrize("variable", ["HTTPS_PROXY", "https_proxy"])
def test_either_spelling_of_the_proxy_variable_is_the_hint(
    monkeypatch, warnings, no_warning_yet, variable
):
    monkeypatch.delenv(ENGINE_ACTIVE_ENV, raising=False)
    for name in tools.PROXY_HINTS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(variable, "http://127.0.0.1:9")
    with pytest.raises(RealWriteRan):
        mark_refunded(1)
    assert len(warnings()) == 1


def test_an_inactive_write_with_no_proxy_and_an_inactive_read_say_nothing(
    monkeypatch, warnings, no_warning_yet
):
    monkeypatch.delenv(ENGINE_ACTIVE_ENV, raising=False)
    for name in tools.PROXY_HINTS:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RealWriteRan):
        mark_refunded(1)
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    assert look_up(1) == {"order": 1, "status": "paid"}
    assert warnings() == []


def test_an_inactive_async_write_runs_for_real(monkeypatch, no_warning_yet):
    monkeypatch.delenv(ENGINE_ACTIVE_ENV, raising=False)
    with pytest.raises(RealWriteRan):
        asyncio.run(mark_refunded_async(1))


# -- the real `irimi shadow` ---------------------------------------------------------------------

AGENT = """
import urllib.request

from irimi import sdk


def _stand_in(order: int) -> str:
    return "stood in"


@sdk.write_tool(fake=_stand_in, name="db.mark_refunded")
def mark_refunded(order: int) -> str:
    raise SystemExit("the real write ran")


request = urllib.request.Request(
    "http://api.stripe.com/v1/customers/cus_1", data=b"metadata[order]=7", method="POST"
)
with urllib.request.urlopen(request, timeout=10) as response:
    response.read()
print("tool said:", mark_refunded(7))
"""
ELAPSED = re.compile(r" · \d+\.\ds$")


def _summary(lines: list[str]) -> list[str]:
    """irimi's closing block, from its header on, the elapsed seconds normalized."""
    start = next(i for i, line in enumerate(lines) if re.search(r" · \d+ exchanges? · ", line))
    return [ELAPSED.sub(" · <elapsed>", line) for line in lines[start:]]


def test_irimi_shadow_prints_a_tool_call_in_the_stream_and_the_summary_and_stores_it(
    tmp_path, monkeypatch, capfd
):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "home"))
    ca.generate_ca(ca.ca_paths())
    agent = tmp_path / "agent.py"
    agent.write_text(AGENT)
    assert main(["shadow", "--port", "0", "--", sys.executable, str(agent)]) == 0
    lines = capfd.readouterr().out.splitlines()
    assert "tool said: stood in" in lines
    assert "tool-fake     write     db.mark_refunded -> ok" in lines
    live = _summary(lines)
    assert live[live.index("  ○ stripe  update customer cus_1  faked; not checked") + 1] == (
        "  ○ tool  db.mark_refunded  fake ran instead"
    )
    # Stored in the process run, which then prints the summary `irimi shadow` printed.
    reader = StoreReader(tmp_path / "home" / "store")
    [process] = [r for r in reader.list_runs() if r.attribution == "process"]
    run = reader.load_run(process.run_id)
    [call] = [e for e in run.events if isinstance(e, ToolCall)]
    assert (call.name, call.ran, call.args, call.result) == (
        "db.mark_refunded",
        "shadow",
        {"order": 7},
        "stood in",
    )
    stored = summary_lines(RunView(run.record, run.events), servicemaps.load())
    assert [ELAPSED.sub(" · <elapsed>", line) for line in stored] == live


def test_the_posted_tool_call_is_valid_json_the_endpoint_reads(posted):
    """A NaN result is captured as a marker, never posted as a bare NaN."""

    @sdk.read_tool(name="db.nan")
    def nan() -> float:
        return float("nan")

    assert nan() != nan()
    assert len(posted.posts) == 2
    for _, _, doc in posted.posts:
        json.loads(capture.serialized(doc))
        assert doc["result"] == {trace.REPR_KEY: "nan"}


def test_a_tool_name_is_cut_to_the_run_name_limit(posted):
    @sdk.read_tool(name="n" * (limits.MAX_NAME + 5))
    def long_named() -> None:
        return None

    long_named()
    [(_, _, doc)] = posted.posts
    assert doc["name"] == "n" * limits.MAX_NAME


def test_a_long_tool_name_is_cut_outside_secrets_so_it_keeps_no_part_of_a_key(posted):
    """A long tool name is cut before redaction, so a key straddling the cut is left out whole.

    Kept in part, it would be too short for its shape to be redacted."""
    head = "n" * (limits.MAX_NAME - 10) + " "

    @sdk.read_tool(name=head + "ghp_" + "k" * 36)
    def keyed() -> None:
        return None

    keyed()
    [(_, _, doc)] = posted.posts
    assert doc["name"] == head


def test_an_active_tool_logs_nothing(posted, warnings, monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    mark_refunded(1)
    look_up(1)
    assert warnings() == []
