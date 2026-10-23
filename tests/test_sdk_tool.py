"""`@sdk.tool`: the tool calls the proxy cannot see, against a real engine and store.

Each recorded call is posted over a socket to the engine's control endpoint and read back
off disk with `StoreReader` once the engine has stopped. A write tool's real function raises
`_RealWriteRan` wherever a test says it must not run, so a test that let it run fails on it rather
than on a missing record. The last test runs the real `irimi shadow` over a child that makes one
Stripe write through the proxy and calls one write tool.
"""

import asyncio
import functools
import inspect
import json
import re
import sys
import threading
from collections.abc import Callable
from typing import Any, get_args

import pytest

from irimi import ca, exchange, paths, report, sdk, servicemap, trace
from irimi.cli import main
from irimi.sdk import capture, client, runs, tools
from irimi.store import StoreReader
from irimi.trace import ErrorInfo, ToolCall
from tests import test_sdk

irimi = test_sdk.irimi  # bound here so pytest finds the fixtures
warnings = test_sdk.warnings

ENGINE_RUN = "t3st"  # the engine's own run, `test_engine_mitm._config`'s: the process run's place


class _RealWriteRan(AssertionError):
    """A write tool's real function ran where only its stand-in may."""


def _stand_in(order: int, *, note: str = "") -> dict[str, Any]:
    return {"stood_in": order, "note": note}


async def _stand_in_async(order: int, *, note: str = "") -> dict[str, Any]:
    await asyncio.sleep(0)
    return {"stood_in": order, "note": note}


@sdk.tool(kind="write", shadow=_stand_in, name="db.mark_refunded")
def mark_refunded(order: int, *, note: str = "") -> dict[str, Any]:
    raise _RealWriteRan("the real write ran")


@sdk.tool(kind="write", shadow=_stand_in_async, name="db.mark_refunded_async")
async def mark_refunded_async(order: int, *, note: str = "") -> dict[str, Any]:
    raise _RealWriteRan("the real write ran")


@sdk.tool(kind="read")
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
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    monkeypatch.delenv(paths.RUN_ENV, raising=False)
    reporter = _Posted()
    monkeypatch.setattr(runs, "reporter", reporter)
    return reporter


@pytest.fixture
def no_warning_yet(monkeypatch) -> None:
    """The once-per-process WARNING not yet given, as in a fresh agent process."""
    monkeypatch.setattr(tools, "_warned_real_write", False)


# -- decoration time: THE SCOPE RULE's shape, applied to the SDK -----------------------------------


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
    "bad_kind": (lambda: sdk.tool(kind="delete", shadow=_sync)(_sync), "'read' or 'write'"),  # type: ignore[arg-type]
    "write_without_shadow": (lambda: sdk.tool(kind="write")(_sync), "must name its stand-in"),
    "read_with_shadow": (lambda: sdk.tool(kind="read", shadow=_sync)(_sync), "takes no shadow="),
    "stand_in_not_callable": (
        lambda: sdk.tool(kind="write", shadow="mock_database_write")(_sync),  # type: ignore[arg-type]
        "must be callable",
    ),
    "sync_stand_in_for_async": (
        lambda: sdk.tool(kind="write", shadow=_sync)(_async),
        "must be an async def",
    ),
    "async_stand_in_for_sync": (
        lambda: sdk.tool(kind="write", shadow=_async)(_sync),
        "must be a sync function",
    ),
    "generator": (lambda: sdk.tool(kind="read")(_gen), "generator function"),
    "async_generator": (lambda: sdk.tool(kind="read")(_agen), "generator function"),
    "generator_stand_in": (
        lambda: sdk.tool(kind="write", shadow=_gen)(_sync),
        "cannot be the generator function",
    ),
    "name_not_a_string": (lambda: sdk.tool(kind="read", name=7)(_sync), "must be a string"),  # type: ignore[arg-type]
    "partial": (lambda: sdk.tool(kind="read")(functools.partial(_sync, 1)), "@sdk.tool wraps"),
    "class": (lambda: sdk.tool(kind="read")(_Klass), "@sdk.tool wraps a function or a method"),
}


@pytest.mark.parametrize("case", MISTAKES)
def test_each_decoration_time_mistake_raises_type_error_before_any_call(case, posted):
    attempt, message = MISTAKES[case]
    with pytest.raises(TypeError, match=re.escape(message)):
        attempt()
    assert posted.posts == []


def test_bare_sdk_tool_without_its_kind_is_refused_when_the_agent_loads():
    with pytest.raises(TypeError):
        sdk.tool(_sync)  # type: ignore[call-arg]


def test_a_tool_keeps_its_functions_name_signature_and_nature():
    assert mark_refunded.__name__ == "mark_refunded"
    assert list(inspect.signature(mark_refunded).parameters) == ["order", "note"]
    assert inspect.iscoroutinefunction(mark_refunded_async)
    assert not inspect.iscoroutinefunction(mark_refunded)


# -- active: a write runs only its stand-in, a read runs for real, and both are recorded -----------


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
    assert (call.name, call.kind, call.ran) == (f"{__name__}.look_up", "read", "real")
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

    @sdk.tool(kind="write", shadow=failing_stand_in, name="db.fails")
    def fails(order: int) -> Any:
        raise _RealWriteRan("the real write ran")

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
    """`irimi shadow` names its process run in `IRIMI_RUN`; a call made in no SDK run is the
    process's, which may hold tool calls though not an SDK start or end."""
    monkeypatch.setenv(paths.RUN_ENV, ENGINE_RUN)
    assert mark_refunded(9) == {"stood_in": 9, "note": ""}
    reader = irimi.finish()
    [call] = _tool_calls(reader, ENGINE_RUN)
    assert (call.run_id, call.ran) == (ENGINE_RUN, "shadow")
    assert [r.attribution for r in reader.list_runs()] == ["header"]


def test_a_tool_called_in_no_run_with_no_process_run_is_stored_unattributed(irimi, monkeypatch):
    """`irimi serve` gives no `IRIMI_RUN`: a call outside every run is filed under none, as an
    exchange that names none is, and the control endpoint takes it there."""
    monkeypatch.delenv(paths.RUN_ENV, raising=False)
    assert look_up(4) == {"order": 4, "status": "paid"}
    reader = irimi.finish()
    [call] = _tool_calls(reader, trace.UNATTRIBUTED)
    assert (call.run_id, call.name, call.ran) == (trace.UNATTRIBUTED, f"{__name__}.look_up", "real")
    assert reader.list_runs() == []


def test_a_tool_call_is_posted_to_its_runs_route_with_no_run_id_in_the_body(posted):
    with sdk.run(name="r"):
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
    """The agent called the real function, so its parameter names are what a replay binds
    against; a stand-in that takes anything would record `{"args": [...], "kwargs": {...}}`."""

    def anything(*args: Any, **kwargs: Any) -> str:
        return "stood in"

    @sdk.tool(kind="write", shadow=anything, name="db.charge")
    def charge(customer: str, amount: int) -> str:
        raise _RealWriteRan("the real write ran")

    assert charge("cus_1", amount=4900) == "stood in"
    [(_, _, doc)] = posted.posts
    assert doc["args"] == {"customer": "cus_1", "amount": 4900}


def test_the_args_are_captured_before_the_body_runs(posted):
    def appending_stand_in(rows: list[int]) -> int:
        rows.append(99)
        return len(rows)

    @sdk.tool(kind="write", shadow=appending_stand_in, name="db.append")
    def append(rows: list[int]) -> int:
        raise _RealWriteRan("the real write ran")

    assert append([1, 2]) == 3
    [(_, _, doc)] = posted.posts
    assert (doc["args"], doc["result"]) == ({"rows": [1, 2]}, 3)


def test_args_and_result_together_stay_inside_one_capture_budget(posted):
    """Each is bounded alone (`capture.MAX_CAPTURED_BYTES`); the post must stay under the
    control endpoint's 2 MiB, so a result that would take the pair past one budget is replaced by
    the truncation marker, and the call is still recorded."""
    half = "x" * (capture.MAX_CAPTURED_BYTES // 2 + 10)

    @sdk.tool(kind="read", name="db.echo")
    def echo(text: str) -> str:
        return text

    assert echo(half) == half
    [(_, _, doc)] = posted.posts
    assert doc["args"] == {"text": half}
    assert doc["result"] == {capture.TRUNCATED_KEY: True, "bytes": len(half) + 2}
    assert len(capture.serialized(doc)) < 2 * capture.MAX_CAPTURED_BYTES


def test_a_sync_tool_that_returns_a_coroutine_is_recorded_when_the_coroutine_ends(posted):
    async def body(order: int) -> int:
        await asyncio.sleep(0)
        return order * 2

    @sdk.tool(kind="read", name="db.later")
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

    @sdk.tool(kind="write", shadow=_mark_stand_in, name="ledger.mark")
    def mark(self, order: int) -> str:
        raise _RealWriteRan("the real write ran")

    @sdk.tool(kind="write", shadow=lambda order: f"static {order}", name="ledger.static_below")
    @staticmethod
    def static_below(order: int) -> str:
        raise _RealWriteRan("the real write ran")

    @staticmethod
    @sdk.tool(kind="write", shadow=lambda order: f"static {order}", name="ledger.static_above")
    def static_above(order: int) -> str:
        raise _RealWriteRan("the real write ran")

    @sdk.tool(kind="read", name="ledger.kind")
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

    @sdk.tool(kind="write", shadow=failing, name="db.fails_async")
    async def fails(order: int) -> Any:
        raise _RealWriteRan("the real write ran")

    with pytest.raises(ValueError) as caught:
        asyncio.run(fails(1))
    assert caught.value is raised
    [(_, _, doc)] = posted.posts
    assert (doc["result"], doc["error"]) == (
        None,
        {"type": "builtins.ValueError", "message": "card declined"},
    )


def test_an_async_tool_called_with_the_wrong_arguments_is_recorded_with_the_type_error(posted):
    with pytest.raises(TypeError):
        asyncio.run(mark_refunded_async(1, 2))  # type: ignore[call-arg]
    [(_, _, doc)] = posted.posts
    assert doc["error"]["type"] == "builtins.TypeError"
    assert doc["args"] == {"args": [1, 2], "kwargs": {}}  # it did not bind (`capture_call`)


def test_a_cancelled_async_tool_is_recorded_as_cancelled_and_stays_cancelled(posted):
    async def slow(order: int) -> str:
        await asyncio.sleep(10)
        return "never"

    @sdk.tool(kind="write", shadow=slow, name="db.slow")
    async def write(order: int) -> str:
        raise _RealWriteRan("the real write ran")

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

    @sdk.tool(kind="write", shadow=waits, name="db.waits")
    async def write(order: int) -> str:
        raise _RealWriteRan("the real write ran")

    coroutine = write(1)
    coroutine.send(None)  # to its first suspension: the stand-in is waiting
    coroutine.close()
    [(_, _, doc)] = posted.posts
    assert doc["error"]["type"] == "builtins.GeneratorExit"


def test_an_async_tools_result_is_captured_on_the_callers_thread_as_it_was_returned(posted):
    """Only the post waits off the event loop: the result's `repr` runs where the tool returned,
    before any other task can change what it returned."""
    shared = {"status": "as returned"}
    seen: list[str] = []

    class _Repr:
        def __repr__(self) -> str:
            seen.append(threading.current_thread().name)
            return "<thing>"

    @sdk.tool(kind="read", name="db.shared")
    async def read() -> list[Any]:
        return [_Repr(), shared]

    async def main() -> None:
        async def mutate() -> None:
            await asyncio.sleep(0)
            shared["status"] = "changed after the return"

        await asyncio.gather(read(), mutate())

    asyncio.run(main())
    [(_, _, doc)] = posted.posts
    assert doc["result"] == [{capture.REPR_KEY: "<thing>"}, {"status": "as returned"}]
    assert seen == [threading.current_thread().name]


def test_a_record_that_cannot_be_built_costs_the_agent_nothing(posted, monkeypatch, warnings):
    def broken(value: object) -> Any:
        raise RecursionError("too deep to record")

    monkeypatch.setattr(tools, "capture", broken)
    monkeypatch.setattr(tools, "_record_failed", False)
    assert look_up(1) == {"order": 1, "status": "paid"}
    assert look_up(2) == {"order": 2, "status": "paid"}
    assert posted.posts == []
    assert len(warnings()) == 1


def test_a_stand_in_that_is_the_function_itself_is_refused_when_the_agent_loads(posted):
    """`shadow=` names what runs IN PLACE of the write. Naming the write itself - reading `shadow`
    as "the function to shadow" - would run the real write under irimi, so it is refused, as every
    other way of leaving a write unnamed is."""

    def database_write(order: int) -> None:
        raise _RealWriteRan("the real write ran")

    with pytest.raises(TypeError, match="is the function itself"):
        sdk.tool(kind="write", shadow=database_write)(database_write)
    with pytest.raises(TypeError, match="is the function itself"):
        sdk.tool(kind="write", shadow=database_write)(staticmethod(database_write))
    assert posted.posts == []


class _Repository:
    """Stand-ins written as static methods in the class body of the tools they stand in for."""

    @staticmethod
    def _save_stand_in(row: int) -> str:
        return f"stood in for {row}"

    @staticmethod
    async def _save_async_stand_in(row: int) -> str:
        return f"stood in async for {row}"

    @sdk.tool(kind="write", shadow=_save_stand_in, name="repo.save")
    @staticmethod
    def save(row: int) -> str:
        raise _RealWriteRan("the real write ran")

    @sdk.tool(kind="write", shadow=_save_async_stand_in, name="repo.save_async")
    @staticmethod
    async def save_async(row: int) -> str:
        raise _RealWriteRan("the real write ran")


def test_a_static_method_stand_in_is_judged_and_called_as_its_function(posted):
    """A `staticmethod` in a class body is callable, but `inspect` sees neither its nature nor a
    generator through it: an async one was refused as sync, and a generator one slipped through.
    """
    assert _Repository.save(1) == "stood in for 1"
    assert asyncio.run(_Repository.save_async(2)) == "stood in async for 2"
    assert [doc["name"] for _, _, doc in posted.posts] == ["repo.save", "repo.save_async"]
    with pytest.raises(TypeError, match="cannot be the generator function"):
        sdk.tool(kind="write", shadow=staticmethod(_gen))(_sync)


def test_an_object_with_an_async_call_is_an_async_stand_in(posted):
    class _Mock:
        async def __call__(self, order: int) -> str:
            return "mocked"

    @sdk.tool(kind="write", shadow=_Mock(), name="db.mocked")
    async def write(order: int) -> str:
        raise _RealWriteRan("the real write ran")

    assert asyncio.run(write(1)) == "mocked"
    with pytest.raises(TypeError, match="must be a sync function"):
        sdk.tool(kind="write", shadow=_Mock())(_sync)


def test_a_failed_post_never_reaches_the_agent(irimi, monkeypatch, warnings):
    monkeypatch.setenv(paths.CONTROL_ENV, "http://127.0.0.1:1/_irimi")
    with sdk.run(name="r"):
        assert mark_refunded(1) == {"stood_in": 1, "note": ""}
    assert [r.levelname for r in warnings()] == ["WARNING"]
    assert "could not record the start of run" in warnings()[0].getMessage()
    monkeypatch.setattr(runs, "reporter", client.ControlClient())
    mark_refunded(2)
    assert "could not record a tool call of run" in warnings()[-1].getMessage()


# -- inactive: the real function, and nothing recorded --------------------------------------------


def test_an_inactive_write_runs_for_real_posts_nothing_and_warns_once_when_a_proxy_is_set(
    monkeypatch, warnings, no_warning_yet
):
    monkeypatch.delenv(paths.ENGINE_ACTIVE_ENV, raising=False)
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    reporter = _Posted()
    monkeypatch.setattr(runs, "reporter", reporter)
    ran: list[int] = []

    @sdk.tool(kind="write", shadow=lambda order: "stood in", name="db.inactive")
    def write(order: int) -> str:
        ran.append(order)
        return "real"

    assert write(1) == write(2) == "real"
    assert ran == [1, 2]
    assert reporter.posts == []
    # Another write tool in the same process runs for real too, and is not warned of again.
    with pytest.raises(_RealWriteRan):
        mark_refunded(3)
    assert [r.getMessage() for r in warnings()] == [
        "irimi: write tool db.inactive ran for real: HTTPS_PROXY is set but "
        "IRIMI_ENGINE_ACTIVE is not"
    ]


@pytest.mark.parametrize("variable", ["HTTPS_PROXY", "https_proxy"])
def test_either_spelling_of_the_proxy_variable_is_the_hint(
    monkeypatch, warnings, no_warning_yet, variable
):
    monkeypatch.delenv(paths.ENGINE_ACTIVE_ENV, raising=False)
    for name in tools.PROXY_HINTS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(variable, "http://127.0.0.1:9")
    with pytest.raises(_RealWriteRan):
        mark_refunded(1)
    assert len(warnings()) == 1


def test_an_inactive_write_with_no_proxy_and_an_inactive_read_say_nothing(
    monkeypatch, warnings, no_warning_yet
):
    monkeypatch.delenv(paths.ENGINE_ACTIVE_ENV, raising=False)
    for name in tools.PROXY_HINTS:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(_RealWriteRan):
        mark_refunded(1)
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    assert look_up(1) == {"order": 1, "status": "paid"}
    assert warnings() == []


def test_an_inactive_async_write_runs_for_real(monkeypatch, no_warning_yet):
    monkeypatch.delenv(paths.ENGINE_ACTIVE_ENV, raising=False)
    with pytest.raises(_RealWriteRan):
        asyncio.run(mark_refunded_async(1))


# -- the real `irimi shadow` ---------------------------------------------------------------------

AGENT = """
import urllib.request

from irimi import sdk


def _stand_in(order: int) -> str:
    return "stood in"


@sdk.tool(kind="write", shadow=_stand_in, name="db.mark_refunded")
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
    stored = report.stored_summary_lines(run, servicemap.load())
    assert [ELAPSED.sub(" · <elapsed>", line) for line in stored] == live


def test_the_posted_tool_call_is_valid_json_the_endpoint_reads(posted):
    """`allow_nan=False` is how both the SDK and the store write: a NaN result is
    captured as a marker, never posted as a bare NaN."""

    @sdk.tool(kind="read", name="db.nan")
    def nan() -> float:
        return float("nan")

    assert nan() != nan()
    assert len(posted.posts) == 2
    for _, _, doc in posted.posts:
        json.loads(capture.serialized(doc))
        assert doc["result"] == {capture.REPR_KEY: "nan"}


def test_the_client_posts_exactly_the_endpoints_run_actions():
    """`client.Action` is a `Literal`, which cannot be built from `exchange.RUN_ACTIONS`: this is
    what keeps the two one list."""
    assert get_args(client.Action) == exchange.RUN_ACTIONS


def test_a_tool_name_is_cut_to_the_run_name_limit(posted):
    @sdk.tool(kind="read", name="n" * (runs.MAX_NAME + 5))
    def long_named() -> None:
        return None

    long_named()
    [(_, _, doc)] = posted.posts
    assert doc["name"] == "n" * runs.MAX_NAME


def test_a_long_tool_name_is_cut_outside_secrets_so_it_keeps_no_part_of_a_key(posted):
    """A tool's name is cut before irimi redacts it, as a run's is: a key straddling the cut is
    left out whole rather than kept in part, too short for its shape to be redacted."""
    head = "n" * (runs.MAX_NAME - 10) + " "

    @sdk.tool(kind="read", name=head + "ghp_" + "k" * 36)
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
