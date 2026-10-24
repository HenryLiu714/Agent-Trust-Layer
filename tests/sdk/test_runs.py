"""The SDK's runs and triggers against a real engine and store, read back off disk."""

import asyncio
import contextlib
import contextvars
import functools
import gc
import http.server
import inspect
import json
import logging
import os
import socket
import subprocess
import sys
import threading
import time
import typing
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from irimi import __version__, sdk
from irimi.core import limits, secret_shapes
from irimi.core.exchange import Exchange
from irimi.core.limits import MAX_ERROR_TYPE
from irimi.protocol.control import RUN_ACTIONS, RunAction
from irimi.protocol.env import AGENT_VERSION_ENV, CONTROL_ENV, ENGINE_ACTIVE_ENV, PROXY_ENVS
from irimi.protocol.trace import ErrorInfo, Trigger
from irimi.sdk import _capture as capture
from irimi.sdk import _control_client as client
from irimi.sdk import _runs as runs
from irimi.sdk._state import state
from tests.support.engine import via_proxy

HERE = __name__  # the module a trigger defined in this file records in its entrypoint


# -- the agent's code, as the tests decorate it --------------------------------------------------


@sdk.trigger
def handle(ticket_id: int, *, note: str = "") -> str | None:
    return sdk.current_run_id()


@sdk.trigger(name="refund")
async def handle_async(charge: str) -> str | None:
    await asyncio.sleep(0)
    return sdk.current_run_id()


class Desk:
    @sdk.trigger
    def answer(self, ticket_id: int) -> str | None:
        return sdk.current_run_id()


@sdk.trigger
def echo(value: object) -> object:
    return value


@sdk.trigger
async def echo_async(value: object) -> object:
    return value


@sdk.trigger(name="outer")
def outer() -> tuple[str | None, str | None]:
    return sdk.current_run_id(), inner()


@sdk.trigger(name="inner")
def inner() -> str | None:
    return sdk.current_run_id()


# -- one run per entry, each the right shape -----------------------------------------------------


def test_a_sync_trigger_is_one_stored_sdk_run_with_its_entrypoint_and_args(irimi):
    run_id = handle(7, note="hi")
    assert run_id is not None and sdk.current_run_id() is None
    [run] = irimi.sdk_runs()
    assert run.run_id == run_id
    assert run.trigger == Trigger(
        f"{HERE}:handle", f"{HERE}:handle", {"ticket_id": 7, "note": "hi"}, True
    )
    assert (run.end.outcome, run.end.error, run.end.exit_code) == ("ok", None, None)
    assert (run.sdk_version, run.engine_version, run.agent_version) == (
        __version__,
        __version__,
        None,
    )
    assert run.started_at is not None and run.end.ended_at is not None
    assert run.started_at <= run.end.ended_at


def test_an_async_trigger_is_one_stored_sdk_run_under_the_name_it_was_given(irimi):
    run_id = asyncio.run(handle_async("ch_1"))
    [run] = irimi.sdk_runs()
    assert run.run_id == run_id
    assert run.trigger == Trigger("refund", f"{HERE}:handle_async", {"charge": "ch_1"}, True)
    assert run.end.outcome == "ok"


def test_a_decorated_method_records_its_class_and_leaves_self_out_of_the_args(irimi):
    run_id = Desk().answer(42)
    [run] = irimi.sdk_runs()
    assert run.run_id == run_id
    assert run.trigger == Trigger(
        f"{HERE}:Desk.answer", f"{HERE}:Desk.answer", {"ticket_id": 42}, True
    )
    assert run.end.outcome == "ok"


def test_sdk_run_with_and_async_with_are_one_run_each_with_no_entrypoint(irimi):
    with sdk.run("nightly", inputs={"date": "2026-09-29"}) as nightly:
        sync_id = sdk.current_run_id()

    async def block() -> str | None:
        async with sdk.run("run", inputs=["a", 1]) as entered:
            assert entered is not None
            return entered.id

    async_id = asyncio.run(block())
    assert None not in (sync_id, async_id) and sync_id != async_id
    assert sync_id is not None
    assert nightly == sdk.Run(sync_id, "nightly")
    found = {r.run_id: r for r in irimi.sdk_runs()}
    assert set(found) == {sync_id, async_id}
    assert found[sync_id].trigger == Trigger("nightly", None, {"date": "2026-09-29"}, True)
    assert found[async_id].trigger == Trigger("run", None, ["a", 1], True)
    assert {r.end.outcome for r in found.values()} == {"ok"}


def test_a_run_records_the_agents_version_when_its_deployment_names_one(irimi, monkeypatch):
    monkeypatch.setenv(AGENT_VERSION_ENV, "agent-3.1")
    handle(1)
    [run] = irimi.sdk_runs()
    assert run.agent_version == "agent-3.1"


def test_a_run_id_is_16_lower_case_hex_characters(irimi):
    ids = {handle(n) for n in range(5)}
    assert len(ids) == 5
    assert all(i is not None and len(i) == 16 and i == i.lower() for i in ids)
    assert all(int(i, 16) >= 0 for i in ids if i is not None)


def test_an_exchange_the_run_labels_lands_in_the_run_the_sdk_started(irimi, upstream):
    """A labelled exchange lands in the SDK's run, never a `header` run: the start comes first."""

    @sdk.trigger
    def fetch() -> int:
        status, _ = via_proxy(
            irimi.port,
            "GET",
            f"http://127.0.0.1:{upstream}/hello",
            extra_headers={"Irimi-Run": sdk.current_run_id() or ""},
        )
        return status

    assert fetch() == 200
    reader = irimi.finish()
    [run] = [r for r in reader.list_runs() if r.attribution != "process"]
    assert (run.attribution, run.end.outcome) == ("sdk", "ok")
    [exchange] = reader.load_run(run.run_id).events
    assert isinstance(exchange, Exchange) and exchange.request.path == "/hello"


class _Posts:
    """A `client.Reporter` that keeps what it is given, in order, with what the agent did."""

    def __init__(self, log: list[str]) -> None:
        self.log = log

    def post(self, run_id: str, action: str, doc: Any) -> None:
        self.log.append(action)


def test_the_start_is_posted_before_the_body_runs_and_the_end_after_it(monkeypatch):
    """The start reaches irimi before the run's first request, and the end after its last.

    The store makes a late start's run `sdk` all the same, so only the order shows it."""
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    log: list[str] = []
    monkeypatch.setattr(state, "reporter", _Posts(log))

    @sdk.trigger
    def body() -> None:
        log.append("body")

    @sdk.trigger
    async def async_body() -> None:
        log.append("async body")

    body()
    asyncio.run(async_body())
    with sdk.run("run"):
        log.append("block")
    assert log == ["start", "body", "end", "start", "async body", "end", "start", "block", "end"]


def test_the_sdk_never_posts_through_the_agents_proxy(irimi, monkeypatch):
    """The SDK's control posts never use the agent's proxy variables, which name irimi too.

    The client is built after the variables are set, as in an agent `irimi shadow` started."""
    with _recorder() as (port, hits):
        for name in PROXY_ENVS:
            monkeypatch.setenv(name, f"http://127.0.0.1:{port}")
        for name in ("NO_PROXY", "no_proxy"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setattr(state, "reporter", client.ControlClient())
        run_id = handle(1)
    assert hits == []
    [run] = irimi.sdk_runs()
    assert (run.run_id, run.end.outcome) == (run_id, "ok")


def test_an_async_trigger_waits_for_its_posts_off_the_event_loop(monkeypatch):
    """An async trigger posts in a worker thread, so a ticker task runs during each post."""
    with _recorder(status=204, delay=0.5) as (port, hits):
        monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
        monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
        monkeypatch.setattr(state, "reporter", client.ControlClient())

        async def main() -> tuple[int, int]:
            ticks = 0

            async def tick() -> None:
                nonlocal ticks
                while True:
                    await asyncio.sleep(0.01)
                    ticks += 1

            @sdk.trigger
            async def body() -> int:
                return ticks  # every tick so far came while the start was posted

            ticker = asyncio.create_task(tick())
            during_start = await body()
            during_end = ticks - during_start
            ticker.cancel()
            return during_start, during_end

        during_start, during_end = asyncio.run(main())
    assert [hit.rsplit("/", 1)[1] for hit in hits] == ["start", "end"]
    # 0.5 s each, so about 50 ticks apiece; a loop blocked by a post would barely tick.
    assert during_start >= 10 and during_end >= 10, (during_start, during_end)


# -- errors ------------------------------------------------------------------------------------


def test_a_trigger_that_raises_re_raises_the_same_object_and_its_run_ends_in_error(irimi):
    boom = ValueError("x")

    @sdk.trigger
    def fails() -> None:
        raise boom

    with pytest.raises(ValueError) as caught:  # noqa: PT011  # checked by identity below
        fails()
    assert caught.value is boom
    [run] = irimi.sdk_runs()
    assert (run.end.outcome, run.end.error) == ("error", ErrorInfo("builtins.ValueError", "x"))


def test_an_exception_named_past_what_the_control_endpoint_takes_still_ends_its_run(irimi):
    """An exception type named past 2 MiB is cut to MAX_POSTED_TYPE, so the run's end still posts.

    The store keeps MAX_ERROR_TYPE of it, redacted."""
    name = "H" * (3 * 1024 * 1024)
    huge = type(name, (Exception,), {})
    with pytest.raises(huge), sdk.run("huge"):
        raise huge("x")
    [run] = irimi.sdk_runs()
    kept = f"{__name__}.{name}"[:MAX_ERROR_TYPE]
    assert (run.end.outcome, run.end.error) == ("error", ErrorInfo(kept, "x"))


def test_a_keyboard_interrupt_ends_the_run_in_error_and_still_propagates(irimi):
    stop = KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt) as caught, sdk.run("interrupted"):
        raise stop
    assert caught.value is stop
    [run] = irimi.sdk_runs()
    assert (run.end.outcome, run.end.error) == (
        "error",
        ErrorInfo("builtins.KeyboardInterrupt", ""),
    )


def test_a_cancelled_async_trigger_ends_its_run_in_error_and_stays_cancelled(irimi):
    started = asyncio.Event()

    @sdk.trigger
    async def waits() -> None:
        started.set()
        await asyncio.sleep(60)

    async def cancel_it() -> None:
        task = asyncio.create_task(waits())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_it())
    [run] = irimi.sdk_runs()
    assert run.end.outcome == "error"
    assert run.end.error is not None and run.end.error.type == "asyncio.exceptions.CancelledError"


def test_a_trigger_cancelled_while_its_start_is_posted_leaves_no_run_current(monkeypatch):
    """A task cancelled while its start is posted leaves the run itself.

    `async with` calls no `__aexit__` when `__aenter__` raises."""
    with _recorder(status=204, delay=0.5) as (port, _hits):
        monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
        monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
        monkeypatch.setattr(state, "reporter", client.ControlClient())

        async def caller() -> tuple[bool, str | None]:
            try:
                await handle_async("ch_1")
            except asyncio.CancelledError:
                return True, sdk.current_run_id()
            return False, sdk.current_run_id()

        async def main() -> tuple[bool, str | None]:
            task = asyncio.create_task(caller())
            await asyncio.sleep(0.1)
            task.cancel()
            return await task

        assert asyncio.run(main()) == (True, None)


def test_an_interruption_while_the_end_is_posted_still_leaves_no_run_current(monkeypatch):
    """An interrupt or cancellation while the end is posted propagates, and the run is left.

    The agent's own exception is its `__context__`."""
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    boom = ValueError("x")

    class InterruptedEnd:
        def post(self, run_id: str, action: str, doc: Any) -> None:
            if action == "end":
                raise KeyboardInterrupt

    monkeypatch.setattr(state, "reporter", InterruptedEnd())

    @sdk.trigger
    def fails() -> None:
        raise boom

    with pytest.raises(KeyboardInterrupt) as caught:
        fails()
    assert caught.value.__context__ is boom and sdk.current_run_id() is None

    posting = threading.Event()
    release = threading.Event()

    class SlowEnd:
        def post(self, run_id: str, action: str, doc: Any) -> None:
            if action == "end":
                posting.set()
                release.wait(5)

    monkeypatch.setattr(state, "reporter", SlowEnd())

    @sdk.trigger
    async def fails_async() -> None:
        raise boom

    async def main() -> tuple[BaseException | None, str | None]:
        async def caller() -> tuple[BaseException | None, str | None]:
            try:
                await fails_async()
            except BaseException as exc:
                return exc.__context__, sdk.current_run_id()
            return None, sdk.current_run_id()

        task = asyncio.create_task(caller())
        await asyncio.to_thread(posting.wait, 5)
        task.cancel()
        try:
            return await task
        finally:
            release.set()

    assert asyncio.run(main()) == (boom, None)


def test_an_async_trigger_closed_mid_run_ends_its_run_and_raises_nothing(monkeypatch):
    """A coroutine closed mid-run posts its end inline and leaves the run, raising nothing.

    Nothing may be awaited during GeneratorExit, and the collector may close it from another
    context. Neither the closer nor `sys.unraisablehook` sees an error of the SDK's."""
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    log: list[str] = []
    monkeypatch.setattr(state, "reporter", _Posts(log))
    unraisable: list[BaseException | None] = []
    monkeypatch.setattr(sys, "unraisablehook", lambda u: unraisable.append(u.exc_value))

    @sdk.trigger
    async def waits() -> None:
        log.append("body")
        await asyncio.get_running_loop().create_future()  # nothing ever resolves it

    async def by_hand() -> None:
        coroutine = waits()
        blocked = coroutine.send(None)
        while "body" not in log:  # the start is posted in a worker thread, which may be done first
            while not blocked.done():
                await asyncio.sleep(0.01)
            blocked = coroutine.send(None)
        coroutine.close()  # suspended in the body

    async def collected() -> None:
        task = asyncio.create_task(waits())
        while log.count("body") < 2:
            await asyncio.sleep(0.01)
        del task
        gc.collect()

    asyncio.run(by_hand())
    assert log == ["start", "body", "end"] and sdk.current_run_id() is None
    asyncio.run(collected())
    assert log[3:] == ["start", "body", "end"]
    assert unraisable == []


# -- nesting and concurrency ---------------------------------------------------------------------


def test_a_nested_trigger_joins_the_run_it_is_called_in(irimi):
    outer_id, inner_id = outer()
    assert outer_id is not None and outer_id == inner_id
    [run] = irimi.sdk_runs()
    assert run.run_id == outer_id and run.trigger is not None and run.trigger.name == "outer"


def test_a_trigger_inside_sdk_run_joins_it(irimi):
    with sdk.run("batch"):
        block_id = sdk.current_run_id()
        assert inner() == block_id
    [run] = irimi.sdk_runs()
    assert run.run_id == block_id


def test_two_async_triggers_gathered_are_two_runs(irimi):
    async def both() -> list[str | None]:
        return list(await asyncio.gather(handle_async("ch_1"), handle_async("ch_2")))

    first, second = asyncio.run(both())
    assert None not in (first, second) and first != second
    found = {r.run_id: r.trigger.args for r in irimi.sdk_runs() if r.trigger is not None}
    assert found == {first: {"charge": "ch_1"}, second: {"charge": "ch_2"}}


def test_a_thread_loses_the_run_unless_its_work_is_propagated(irimi):
    seen: dict[str, str | None] = {}

    def bare_thread() -> None:
        seen["thread"] = sdk.current_run_id()

    with sdk.run("fan-out"):
        run_id = sdk.current_run_id()
        worker = threading.Thread(target=bare_thread)
        worker.start()
        worker.join()
        with ThreadPoolExecutor(1) as pool:
            seen["submit"] = pool.submit(sdk.current_run_id).result()
            seen["propagated"] = pool.submit(sdk.propagate(sdk.current_run_id)).result()
        seen["to_thread"] = asyncio.run(asyncio.to_thread(sdk.current_run_id))
    # A free-threaded build of 3.14 starts a thread in a copy of its creator's context by default
    # (`sys.flags.thread_inherit_context`); every other build starts it in an empty one.
    inherited = run_id if getattr(sys.flags, "thread_inherit_context", 0) else None
    assert seen == {
        "thread": inherited,
        "submit": inherited,
        "propagated": run_id,
        "to_thread": run_id,
    }


def test_propagate_takes_the_context_when_it_is_called_not_when_its_wrapper_runs(irimi):
    with sdk.run("first"):
        first = sdk.current_run_id()
        work = sdk.propagate(sdk.current_run_id)
    with sdk.run("second"):
        assert work() == first != sdk.current_run_id()


def test_one_propagated_wrapper_may_run_on_two_threads_at_once(irimi):
    barrier = threading.Barrier(2, timeout=5)

    def meet() -> str | None:
        barrier.wait()
        return sdk.current_run_id()

    with sdk.run("shared"):
        run_id = sdk.current_run_id()
        work = sdk.propagate(meet)
        with ThreadPoolExecutor(2) as pool:
            assert [f.result() for f in [pool.submit(work), pool.submit(work)]] == [run_id] * 2


JOB = sdk.run("job")  # one object, entered from everywhere, as a module constant is


def test_one_sdk_run_object_may_be_entered_nested_on_threads_and_in_tasks_at_once(irimi):
    """Each entry of one `sdk.run` is its own scope, kept per context.

    Nested in itself it joins its own run; two threads or tasks inside it are two runs."""
    with JOB:
        outer_id = sdk.current_run_id()
        with JOB:
            assert sdk.current_run_id() == outer_id
        assert sdk.current_run_id() == outer_id
    assert sdk.current_run_id() is None

    barrier = threading.Barrier(2, timeout=5)

    def on_a_thread() -> str | None:
        with JOB:
            barrier.wait()  # both threads are inside the one object at once
            return sdk.current_run_id()

    with ThreadPoolExecutor(2) as pool:
        thread_ids = [f.result() for f in [pool.submit(on_a_thread), pool.submit(on_a_thread)]]

    async def in_a_task(started: asyncio.Event, go: asyncio.Event) -> str | None:
        async with JOB:
            started.set()
            await go.wait()
            return sdk.current_run_id()

    async def two_tasks() -> list[str | None]:
        first, second, go = asyncio.Event(), asyncio.Event(), asyncio.Event()
        tasks = [asyncio.create_task(in_a_task(e, go)) for e in (first, second)]
        await first.wait()
        await second.wait()
        go.set()
        return list(await asyncio.gather(*tasks))

    task_ids = asyncio.run(two_tasks())
    ids = [outer_id, *thread_ids, *task_ids]
    assert None not in ids and len(set(ids)) == 5
    found = {r.run_id: r for r in irimi.sdk_runs()}
    assert set(found) == set(ids)
    assert {(r.trigger.name, r.end.outcome) for r in found.values() if r.trigger} == {("job", "ok")}


def test_an_exit_in_a_context_that_made_no_entry_ends_nothing_and_does_not_raise(irimi):
    """An exit from a context that made no entry ends nothing; the run is stored incomplete.

    An async generator closed by another task exits in a context other than the one it entered."""
    block = sdk.run("moved")
    entered = contextvars.copy_context()
    run_id = entered.run(lambda: (block.__enter__(), sdk.current_run_id())[1])
    contextvars.copy_context().run(block.__exit__, None, None, None)
    assert sdk.current_run_id() is None
    [run] = irimi.sdk_runs()
    assert (run.run_id, run.end) == (run_id, None)


def test_an_exit_in_a_context_that_only_inherited_an_entry_leaves_it_to_its_maker(irimi):
    """A propagated thread's stray exit ends nothing; the block that made the entry ends it once."""
    with JOB:
        run_id = sdk.current_run_id()
        with ThreadPoolExecutor(1) as pool:
            pool.submit(sdk.propagate(JOB.__exit__), None, None, None).result()
        assert sdk.current_run_id() == run_id
    assert sdk.current_run_id() is None
    [run] = irimi.sdk_runs()
    assert (run.run_id, run.end.outcome) == (run_id, "ok")


def test_a_stray_exit_after_the_block_has_ended_ends_nothing_and_does_not_raise(irimi):
    """The same stray exit after the block's own, when its token is spent, still raises nothing.

    Resetting a spent token is a RuntimeError, not the ValueError of an unspent one."""
    with JOB:
        run_id = sdk.current_run_id()
        stray = sdk.propagate(JOB.__exit__)
    with ThreadPoolExecutor(1) as pool:
        pool.submit(stray, None, None, None).result()
    assert sdk.current_run_id() is None
    [run] = irimi.sdk_runs()
    assert (run.run_id, run.end.outcome) == (run_id, "ok")


def test_instrument_is_called_once_per_new_run_and_never_for_a_joined_one(irimi, monkeypatch):
    calls: list[str | None] = []
    monkeypatch.setattr(runs, "instrument", lambda: calls.append(sdk.current_run_id()))
    outer_id, _ = outer()
    async_id = asyncio.run(handle_async("ch_9"))
    # Inside the run it starts, posted inline or from a worker thread alike.
    assert calls == [outer_id, async_id]


def test_a_start_interrupted_while_posted_leaves_no_run_current(irimi, monkeypatch):
    """An interrupt while a sync start is posted leaves the run, so the next trigger is its own.

    `with` calls no `__exit__` here, so the scope leaves the run itself."""

    class Interrupted:
        def __init__(self) -> None:
            self.raised = False

        def post(self, run_id: str, action: str, doc: Any) -> None:
            if not self.raised:
                self.raised = True
                raise KeyboardInterrupt

    monkeypatch.setattr(state, "reporter", Interrupted())
    with pytest.raises(KeyboardInterrupt):
        handle(1)
    assert sdk.current_run_id() is None
    first, second = handle(2), handle(3)
    assert None not in (first, second) and first != second


def test_an_instrument_that_raises_costs_one_warning_and_never_the_agents_call(
    irimi, monkeypatch, warnings
):
    def broken() -> None:
        raise RuntimeError("patching failed")

    monkeypatch.setattr(runs, "instrument", broken)
    monkeypatch.setattr(state, "warned", set())
    assert echo(1) == 1 and echo(2) == 2
    [warning] = warnings()
    assert "could not label" in warning.getMessage() and warning.exc_info is not None
    assert [r.end.outcome for r in irimi.sdk_runs()] == ["ok", "ok"]


def test_an_async_trigger_driven_with_no_asyncio_loop_posts_inline(irimi):
    """With no asyncio loop, as under trio or by hand, an async trigger posts inline."""

    @sdk.trigger
    async def no_loop() -> str | None:
        return sdk.current_run_id()

    coroutine = no_loop()
    with pytest.raises(StopIteration) as done:
        coroutine.send(None)
    run_id = done.value.value
    [run] = irimi.sdk_runs()
    assert run_id is not None and (run.run_id, run.end.outcome) == (run_id, "ok")


def test_an_async_trigger_whose_loop_takes_no_more_threads_posts_inline(irimi):
    """When the loop's executor is shut down, an async trigger posts inline instead of raising.

    That happens at the end of `asyncio.run` and while the interpreter exits."""

    async def late() -> str | None:
        await asyncio.get_running_loop().shutdown_default_executor()
        return await handle_async("ch_1")

    run_id = asyncio.run(late())
    [run] = irimi.sdk_runs()
    assert run_id is not None and (run.run_id, run.end.outcome) == (run_id, "ok")


def plain_decorator(fn: Any) -> Any:
    """A decorator that does not mark its wrapper as a coroutine function, as many do not."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return fn(*args, **kwargs)

    return wrapper


@sdk.trigger
@plain_decorator
async def wrapped_handler(charge: str) -> str | None:
    await asyncio.sleep(0)
    return sdk.current_run_id()


def test_an_async_function_behind_a_plain_decorator_runs_inside_its_run(irimi):
    run_id = asyncio.run(wrapped_handler("ch_1"))
    [run] = irimi.sdk_runs()
    assert run_id is not None and (run.run_id, run.end.outcome) == (run_id, "ok")
    assert run.trigger is not None and run.trigger.args == {"charge": "ch_1"}
    # The wrapper keeps the nature of what it wraps: the plain decorator's is a sync function.
    assert not inspect.iscoroutinefunction(wrapped_handler)


def runs_to_completion(fn: Any) -> Any:
    """A decorator that gives an async function a sync entrypoint, driving it to completion."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


@sdk.trigger
@runs_to_completion
async def synchronous_handler(charge: str) -> tuple[str, str | None]:
    await asyncio.sleep(0)
    return charge, sdk.current_run_id()


@pytest.mark.parametrize("active", [False, True])
def test_a_sync_entrypoint_to_an_async_function_still_returns_its_value(irimi, monkeypatch, active):
    """A sync wrapper around an async function is triggered as sync and returns its value.

    `functools.wraps` makes it look async to `inspect.unwrap`."""
    if not active:
        monkeypatch.delenv(ENGINE_ACTIVE_ENV)
    charge, run_id = synchronous_handler("ch_1")
    assert charge == "ch_1" and (run_id is not None) is active
    found = irimi.sdk_runs()
    assert [(r.run_id, r.end.outcome) for r in found] == ([(run_id, "ok")] if active else [])


async def _settle(charge: str) -> str | None:
    await asyncio.sleep(0)
    if charge == "bad":
        raise _BOOM
    return sdk.current_run_id()


_BOOM = ValueError("bad charge")


@sdk.trigger
def settle(charge: str) -> Any:
    return _settle(charge)  # a sync function that hands back a coroutine for its caller to await


def test_a_sync_trigger_that_returns_a_coroutine_keeps_its_run_until_the_coroutine_ends(irimi):
    """The run lasts until the returned coroutine ends, in whatever task awaits it.

    The run's outcome is the coroutine's."""
    log: list[str] = []
    inner = state.reporter

    class Order:
        def post(self, run_id: str, action: RunAction, doc: Any) -> None:
            log.append(action)
            inner.post(run_id, action, doc)

    # Restored by the fixture's monkeypatch; no tool runs, so `recorded` is never asked for.
    state.reporter = typing.cast(client.Reporter, Order())

    async def main() -> tuple[str | None, str | None]:
        made = settle("ch_1")
        assert log == ["start"] and sdk.current_run_id() is None  # its body has not run yet
        ran_in = await asyncio.create_task(made)
        with pytest.raises(ValueError) as caught:  # noqa: PT011  # checked by identity below
            await settle("bad")
        assert caught.value is _BOOM
        return ran_in, sdk.current_run_id()

    ran_in, after = asyncio.run(main())
    assert ran_in is not None and after is None and log == ["start", "end", "start", "end"]
    found = {r.run_id: r for r in irimi.sdk_runs()}
    assert found[ran_in].end.outcome == "ok"
    [failed] = [r for r in found.values() if r.run_id != ran_in]
    assert (failed.end.outcome, failed.end.error) == (
        "error",
        ErrorInfo("builtins.ValueError", "bad charge"),
    )


class Handlers:
    @sdk.trigger
    @staticmethod
    def static_above(charge: str) -> str | None:
        return sdk.current_run_id()

    @staticmethod
    @sdk.trigger
    def static_below(charge: str) -> str | None:
        return sdk.current_run_id()

    @sdk.trigger
    @classmethod
    def class_above(cls, charge: str) -> str | None:
        return sdk.current_run_id()

    @classmethod
    @sdk.trigger
    def class_below(cls, charge: str) -> str | None:
        return sdk.current_run_id()


def test_a_static_or_class_method_stays_one_in_either_decorator_order(irimi):
    """A trigger above `@staticmethod` or `@classmethod` keeps it one, even with the SDK inactive.

    A static method is never given the instance, and a class method's `cls` is not taken for a
    name passed positionally."""
    ids = [
        call("ch_1")
        for handlers in (Handlers, Handlers())
        for call in (
            handlers.static_above,
            handlers.static_below,
            handlers.class_above,
            handlers.class_below,
        )
    ]
    assert None not in ids and len(set(ids)) == 8
    found = sorted((r.trigger.entrypoint, r.trigger.args) for r in irimi.sdk_runs() if r.trigger)
    assert found == sorted(
        (f"{HERE}:Handlers.{name}", {"charge": "ch_1"})
        for name in ("static_above", "static_below", "class_above", "class_below")
        for _ in range(2)
    )


class Handler:
    def __call__(self, charge: str) -> None: ...


@pytest.mark.parametrize(
    "target",
    [functools.partial(echo, 1), Handler(), Desk],
    ids=["partial", "callable instance", "class"],
)
def test_what_has_no_importable_function_to_name_is_refused_at_decoration(target):
    """A partial, an instance or a class is refused: replay imports `module:qualname`.

    A decorated class would become a function, breaking `isinstance` in production too."""
    with pytest.raises(TypeError, match="function or a method"):
        sdk.trigger(target)


def test_a_function_defined_with_no_module_is_refused_by_its_name():
    """A function with `__module__` None is refused by its name, not as "not function"."""
    namespace: dict[str, Any] = {}
    exec("def on_message(body): ...", namespace)
    with pytest.raises(TypeError, match="on_message, which names no module"):
        sdk.trigger(namespace["on_message"])


def test_a_name_that_is_not_a_string_is_refused_and_a_long_one_is_cut(irimi):
    with pytest.raises(TypeError, match="name="):
        sdk.trigger("refund")  # type: ignore[call-overload]
    with pytest.raises(TypeError, match="must be a string"):
        sdk.trigger(name=7)  # type: ignore[call-overload]
    with pytest.raises(TypeError, match="must be a string"):
        sdk.run(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be a string"):
        sdk.run(7)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        sdk.run()  # type: ignore[call-arg]
    with sdk.run("name " * 1000):
        pass
    names = sorted(r.trigger.name for r in irimi.sdk_runs() if r.trigger is not None)
    assert names == ["name " * (limits.MAX_NAME // 5)]


def test_a_long_name_is_cut_outside_secrets_so_it_keeps_no_part_of_a_key(irimi):
    """A long name is cut outside secrets, so a key across the cut is marked whole.

    A part kept would be too short for the key's shape, and reach disk unredacted."""
    head = "n" * (limits.MAX_NAME - 10) + " "
    with sdk.run(head + "ghp_" + "k" * 36):
        pass
    cut = "n" * (limits.MAX_NAME - 10) + secret_shapes.CUT_MARK
    assert [r.trigger.name for r in irimi.sdk_runs() if r.trigger is not None] == [cut]


def test_a_trigger_annotated_with_a_type_checking_only_import_still_binds_by_name():
    """Arguments bind by name even when an annotation names a `TYPE_CHECKING`-only import.

    `inspect.signature` would evaluate the lazy annotations and raise NameError. On 3.12 the
    module needs `from __future__ import annotations` to load."""
    source = "def refund(charge: str, amount: Decimal) -> None: ...\n"
    if sys.version_info < (3, 14):
        source = "from __future__ import annotations\n" + source
    namespace: dict[str, Any] = {}
    exec(source, namespace)
    assert capture.capture_call(namespace["refund"], ("ch_1", 5), {}) == (
        {"charge": "ch_1", "amount": 5},
        True,
    )


# -- inactive: inert ---------------------------------------------------------------------------


class _Recorder(http.server.BaseHTTPRequestHandler):
    """Record each request's target and answer `status` after `delay`, to `location` if set.

    A target is absolute-form when the request was sent to it as a proxy."""

    hits: list[str]
    status = 500
    delay = 0.0
    location = ""

    def do_POST(self) -> None:
        self.hits.append(self.path)
        time.sleep(self.delay)
        self.send_response(self.status)
        if self.location:
            self.send_header("location", self.location)
        self.send_header("content-length", "0")
        self.end_headers()

    do_GET = do_POST

    def log_message(self, *args: Any) -> None:
        pass


@contextlib.contextmanager
def _recorder(
    status: int = 500, delay: float = 0.0, location: str = ""
) -> Iterator[tuple[int, list[str]]]:
    """A server on a loopback port that records what reaches it: `(port, hits)`."""
    hits: list[str] = []
    fields = {"hits": hits, "status": status, "delay": delay, "location": location}
    handler = type("Handler", (_Recorder,), fields)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}).start()
    try:
        yield server.server_address[1], hits
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def tripwire(monkeypatch) -> Iterator[list[str]]:
    """A control endpoint that counts every request it gets, which an inactive SDK must not make."""
    with _recorder() as (port, hits):
        monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
        monkeypatch.setattr(state, "reporter", client.ControlClient())
        yield hits


@pytest.mark.parametrize("value", [None, "", "0", "true"])
def test_an_inactive_sdk_calls_straight_through(tripwire, monkeypatch, value):
    if value is None:
        monkeypatch.delenv(ENGINE_ACTIVE_ENV, raising=False)
    else:
        monkeypatch.setenv(ENGINE_ACTIVE_ENV, value)
    calls: list[object] = []
    monkeypatch.setattr(runs, "instrument", lambda: calls.append("instrument"))
    sentinel = object()
    assert echo(sentinel) is sentinel
    assert asyncio.run(echo_async(sentinel)) is sentinel
    assert handle(7) is None and asyncio.run(handle_async("ch_1")) is None
    assert outer() == (None, None)
    outside = len(contextvars.copy_context())
    with JOB, JOB:  # nested in itself, inactive: nothing to join and nothing to refuse
        assert sdk.current_run_id() is None
        # Inactive, it sets no context variable, not even the object's own record of its entries.
        assert len(contextvars.copy_context()) == outside
    assert not sdk.is_active()
    assert tripwire == [] and calls == []


def test_active_is_read_at_each_call(tripwire, monkeypatch):
    monkeypatch.delenv(ENGINE_ACTIVE_ENV, raising=False)
    assert handle(1) is None
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    assert sdk.is_active() and handle(1) is not None
    assert [path.rsplit("/", 1)[1] for path in tripwire] == ["start", "end"]


# -- the SDK never raises into the agent ---------------------------------------------------------


def test_a_closed_control_port_costs_one_warning_and_no_time(monkeypatch, warnings, refused_port):
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{refused_port}/_irimi")
    monkeypatch.setattr(state, "reporter", client.ControlClient())
    # A post that waited at all would wait its whole timeout, so a long one shows.
    monkeypatch.setattr(client, "TIMEOUT_S", 30.0)
    began = time.monotonic()
    first = handle(1)
    second = asyncio.run(handle_async("ch_1"))
    assert time.monotonic() - began < 10
    assert None not in (first, second)
    [warning] = warnings()
    assert warning.name == "irimi.sdk" and warning.levelno == logging.WARNING
    assert "unreachable" in warning.getMessage()


def test_an_unset_irimi_control_is_one_warning_and_the_run_goes_on(monkeypatch, warnings):
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    monkeypatch.delenv(CONTROL_ENV, raising=False)
    monkeypatch.setattr(state, "reporter", client.ControlClient())
    assert handle(1) is not None and handle(2) is not None
    [warning] = warnings()
    assert "unset" in warning.getMessage() and CONTROL_ENV in warning.getMessage()


def test_a_refused_post_is_one_warning_quoting_irimis_answer(irimi, monkeypatch, warnings):
    monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{irimi.port}/_irimi/nope")
    assert handle(1) is not None
    [warning] = warnings()
    assert "rejected: HTTP 404" in warning.getMessage()
    assert irimi.sdk_runs() == []


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_a_redirect_is_a_refused_post_never_followed(monkeypatch, warnings, status):
    """A redirect is refused like any answer outside 2xx: irimi's endpoint never redirects."""
    with (
        _recorder(status=200) as (elsewhere, landed),
        _recorder(status=status, location=f"http://127.0.0.1:{elsewhere}/x") as (port, hits),
    ):
        monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
        client.ControlClient().post("r1", "start", {})
    assert len(hits) == 1 and landed == []
    [warning] = warnings()
    assert f"rejected: HTTP {status}" in warning.getMessage()


def test_irimi_control_with_a_trailing_slash_names_the_same_endpoint(irimi, monkeypatch, warnings):
    """An `IRIMI_CONTROL` set by hand with a trailing slash posts to the same routes."""
    monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{irimi.port}/_irimi/")
    run_id = handle(1)
    assert warnings() == []
    [run] = irimi.sdk_runs()
    assert (run.run_id, run.end.outcome) == (run_id, "ok")


def test_every_action_the_sdk_posts_is_one_the_control_endpoint_answers():
    """Every run action the SDK posts is a route the control endpoint answers."""
    assert typing.get_type_hints(client.Reporter.post)["action"] is RunAction
    assert set(typing.get_args(RunAction)) == set(RUN_ACTIONS)


def test_a_control_endpoint_that_never_answers_times_out(monkeypatch, warnings):
    silent = socket.socket()
    silent.bind(("127.0.0.1", 0))
    silent.listen(8)  # accepts into the backlog and never answers
    try:
        monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
        monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{silent.getsockname()[1]}")
        monkeypatch.setattr(state, "reporter", client.ControlClient())
        monkeypatch.setattr(client, "TIMEOUT_S", 0.2)
        began = time.monotonic()
        assert handle(1) is not None
        assert time.monotonic() - began < 10
    finally:
        silent.close()
    [warning] = warnings()
    assert "timeout" in warning.getMessage()


def test_a_fault_of_the_clients_own_is_a_warning_not_an_exception(monkeypatch, warnings):
    monkeypatch.setenv(CONTROL_ENV, "http://127.0.0.1:1/_irimi")
    client.ControlClient().post("r1", "start", {"trigger": object()})
    [warning] = warnings()
    assert "failed: TypeError" in warning.getMessage()


def test_an_unreachable_endpoint_pauses_every_send_for_30s_then_irimi_is_asked_again(
    monkeypatch, warnings, refused_port
):
    """A stalled irimi costs one timeout: the breaker skips every send for 30 s, then retries."""
    now = [100.0]
    reporter = client.ControlClient(clock=lambda: now[0])
    monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{refused_port}/_irimi")
    reporter.post("r1", "start", {})
    [warning] = warnings()
    assert "unreachable" in warning.getMessage()
    assert "irimi is not asked again for 30s" in warning.getMessage()
    with _recorder(status=204) as (port, hits):
        monkeypatch.setenv(CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
        now[0] += client.PAUSE_S - 1
        reporter.post("r1", "end", {})
        assert reporter.recorded("r1", "db.read", {}) is None
        assert hits == []
        now[0] += 2
        reporter.post("r1", "end", {})
        assert len(hits) == 1
    assert len(warnings()) == 1


def test_a_proxy_speaking_another_protocol_is_one_warning_naming_both_versions(
    irimi, monkeypatch, warnings
):
    """A 426 from a proxy on another protocol major gives one warning naming both versions."""
    monkeypatch.setattr(client, "PROTOCOL_VERSION", "2")
    assert handle(1) is not None and handle(2) is not None
    assert state.reporter.recorded("r1", "db.read", {}) is None
    [warning] = warnings()
    said = warning.getMessage()
    assert said == (
        f"irimi: this SDK speaks control protocol 2, but the proxy at "
        f"http://127.0.0.1:{irimi.port}/_irimi speaks 1; runs are not recorded until the two match"
    )
    assert irimi.sdk_runs() == []


# -- decoration ----------------------------------------------------------------------------------


_SCRIPT_AGENT = """\
import dataclasses, json
from irimi import sdk
from irimi.protocol import entrypoints
from irimi.sdk import _capture, _runs

@dataclasses.dataclass
class Ticket:
    n: int

@sdk.trigger
def handle(ticket: Ticket) -> None: ...

@sdk.trigger
def other() -> None: ...

@sdk.read_tool
def look() -> None: ...

if __name__ == "__main__":
    print(json.dumps([
        entrypoints.entrypoint(handle.__wrapped__),
        _runs.label(look.__wrapped__, None, "@sdk.read_tool"),
        _capture.capture(Ticket(1)),
    ]))
"""


def _run_agent(tmp_path, argv: list[str], *, active: bool) -> subprocess.CompletedProcess[str]:
    package = tmp_path / "pkg"
    package.mkdir(exist_ok=True)
    (package / "__init__.py").write_text("")
    (package / "agent.py").write_text(_SCRIPT_AGENT)
    env = {k: v for k, v in os.environ.items() if not k.startswith("IRIMI_")}
    if active:
        env[ENGINE_ACTIVE_ENV] = "1"
    return subprocess.run(
        [sys.executable, *argv], cwd=tmp_path, env=env, capture_output=True, text=True, check=True
    )


def test_a_trigger_in_a_module_run_with_python_m_records_its_importable_name(tmp_path):
    done = _run_agent(tmp_path, ["-m", "pkg.agent"], active=True)
    entry, tool_name, ticket = json.loads(done.stdout)
    assert entry == "pkg.agent:handle" and tool_name == "pkg.agent:look"
    assert ticket == [{"__irimi_type__": "pkg.agent:Ticket", "value": {"n": 1}}, True]
    assert done.stderr == ""


def test_a_trigger_in_a_script_run_by_path_is_warned_of_once_under_irimi(tmp_path):
    done = _run_agent(tmp_path, ["pkg/agent.py"], active=True)
    entry, _, ticket = json.loads(done.stdout)
    assert entry == "__main__:handle"
    assert ticket[1] is False  # its class cannot be imported by another process either
    assert done.stderr == (
        "irimi: trigger handle is defined in __main__, which a replay cannot import, so its runs "
        "cannot be replayed by entrypoint; define it in a module and start the agent with "
        "python -m <module>\n"
    )
    # Production stays quiet.
    assert _run_agent(tmp_path, ["pkg/agent.py"], active=False).stderr == ""


def test_a_generator_function_is_refused_at_decoration():
    def gen() -> Iterator[int]:
        yield 1

    async def agen() -> Any:
        yield 1

    with pytest.raises(TypeError, match=r"generator function \S*\.gen:"):
        sdk.trigger(gen)
    with pytest.raises(TypeError, match=r"generator function \S*\.agen:"):
        sdk.trigger(name="stream")(agen)


def test_a_trigger_keeps_its_functions_name_signature_and_nature():
    assert handle.__name__ == "handle" and handle.__wrapped__.__qualname__ == "handle"  # type: ignore[attr-defined]
    assert list(inspect.signature(handle).parameters) == ["ticket_id", "note"]
    assert inspect.iscoroutinefunction(handle_async)
    assert not inspect.iscoroutinefunction(handle)


def test_the_public_api_is_exactly_the_documented_names():
    assert sorted(sdk.__all__) == [
        "RecordedToolError",
        "Run",
        "current_run_id",
        "instrument",
        "is_active",
        "propagate",
        "read_tool",
        "run",
        "trigger",
        "write_tool",
    ]
    assert sdk.instrument() is None  # type: ignore[func-returns-value]


def test_importing_the_sdk_imports_no_mitmproxy():
    code = "import irimi.sdk, sys; assert not [m for m in sys.modules if m.startswith('mitmproxy')]"
    subprocess.run([sys.executable, "-c", code], check=True)
