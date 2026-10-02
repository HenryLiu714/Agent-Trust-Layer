"""The SDK, `irimi.sdk` (#74), against a real engine and a real `DirectoryStore`.

Every run here is posted over a socket to the engine's control endpoint (#73), and every claim
about it is read back off disk with `StoreReader` once the engine has stopped. The capture rules,
which are pure, are tested row by row at the end, and so is the promise that `import irimi.sdk`
pulls in no mitmproxy.
"""

import asyncio
import contextlib
import contextvars
import dataclasses
import enum
import functools
import gc
import http.server
import inspect
import logging
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from irimi import __version__, paths, redact, sdk
from irimi.exchange import Exchange
from irimi.sdk import capture, client, runs
from irimi.store import DirectoryStore, StoreReader
from irimi.trace import ErrorInfo, RunRecord, Trigger
from tests import test_engine_mitm
from tests.test_engine_mitm import _config, _start, _via_proxy

upstream = test_engine_mitm.upstream  # bound here so pytest finds the fixture

HERE = __name__  # the module a trigger defined in this file records in its entrypoint


@dataclasses.dataclass
class _Irimi:
    port: int
    store: DirectoryStore
    stop: Any

    def finish(self) -> StoreReader:
        """Stop the engine, which closes the store, and read the store back."""
        self.stop()
        return StoreReader(self.store.layout.root)

    def sdk_runs(self) -> list[RunRecord]:
        return [r for r in self.finish().list_runs() if r.attribution == "sdk"]


@pytest.fixture
def irimi(tmp_path, monkeypatch) -> Iterator[_Irimi]:
    """A running engine with a real store, and this process set up as `irimi shadow` sets up its
    child: `IRIMI_ENGINE_ACTIVE=1` and `IRIMI_CONTROL` naming the engine's listener. The SDK's
    client is a fresh one, so no warning a test expects was already given by another."""
    cfg = _config(tmp_path, monkeypatch)
    store = DirectoryStore(tmp_path / "store", redact.load_key(tmp_path))
    eng, _seen, stop = _start(cfg, store=store)
    stopped = False

    def stop_once() -> None:
        nonlocal stopped
        if not stopped:
            stopped = True
            stop()

    port = eng.listen_port()
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
    monkeypatch.delenv(paths.AGENT_VERSION_ENV, raising=False)
    monkeypatch.setattr(runs, "reporter", client.ControlClient())
    yield _Irimi(port, store, stop_once)
    stop_once()


@pytest.fixture
def warnings(caplog) -> Callable[[], list[logging.LogRecord]]:
    """`warnings()`: what the SDK has logged so far at WARNING or above on `irimi.sdk`."""
    caplog.set_level(logging.WARNING, logger=client.LOGGER_NAME)
    return lambda: [r for r in caplog.records if r.name == client.LOGGER_NAME]


def _closed_port() -> int:
    """A loopback port nothing listens on: bound once, then closed."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


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
    assert run.trigger == Trigger("handle", f"{HERE}:handle", {"ticket_id": 7, "note": "hi"}, True)
    assert (run.outcome, run.error, run.exit_code) == ("ok", None, None)
    assert (run.sdk_version, run.engine_version, run.agent_version) == (
        __version__,
        __version__,
        None,
    )
    assert run.started_at is not None and run.ended_at is not None
    assert run.started_at <= run.ended_at


def test_an_async_trigger_is_one_stored_sdk_run_under_the_name_it_was_given(irimi):
    run_id = asyncio.run(handle_async("ch_1"))
    [run] = irimi.sdk_runs()
    assert run.run_id == run_id
    assert run.trigger == Trigger("refund", f"{HERE}:handle_async", {"charge": "ch_1"}, True)
    assert run.outcome == "ok"


def test_a_decorated_method_records_its_class_and_leaves_self_out_of_the_args(irimi):
    run_id = Desk().answer(42)
    [run] = irimi.sdk_runs()
    assert run.run_id == run_id
    assert run.trigger == Trigger("Desk.answer", f"{HERE}:Desk.answer", {"ticket_id": 42}, True)
    assert run.outcome == "ok"


def test_sdk_run_with_and_async_with_are_one_run_each_with_no_entrypoint(irimi):
    with sdk.run(trigger={"date": "2026-09-29"}, name="nightly"):
        sync_id = sdk.current_run_id()

    async def block() -> str | None:
        async with sdk.run(trigger=["a", 1]):
            return sdk.current_run_id()

    async_id = asyncio.run(block())
    assert None not in (sync_id, async_id) and sync_id != async_id
    found = {r.run_id: r for r in irimi.sdk_runs()}
    assert set(found) == {sync_id, async_id}
    assert found[sync_id].trigger == Trigger("nightly", None, {"date": "2026-09-29"}, True)
    assert found[async_id].trigger == Trigger("run", None, ["a", 1], True)
    assert {r.outcome for r in found.values()} == {"ok"}


def test_a_run_records_the_agents_version_when_its_deployment_names_one(irimi, monkeypatch):
    monkeypatch.setenv(paths.AGENT_VERSION_ENV, "agent-3.1")
    handle(1)
    [run] = irimi.sdk_runs()
    assert run.agent_version == "agent-3.1"


def test_a_run_id_is_16_lower_case_hex_characters(irimi):
    ids = {handle(n) for n in range(5)}
    assert len(ids) == 5
    assert all(i is not None and len(i) == 16 and i == i.lower() for i in ids)
    assert all(int(i, 16) >= 0 for i in ids if i is not None)


def test_an_exchange_the_run_labels_lands_in_the_run_the_sdk_started(irimi, upstream):
    """The start is posted before the trigger's body runs, so an exchange labelled with the run
    reaches a run that already has its record: `sdk`, never a `header` run (#70, #74)."""

    @sdk.trigger
    def fetch() -> int:
        status, _ = _via_proxy(
            irimi.port,
            "GET",
            f"http://127.0.0.1:{upstream}/hello",
            extra_headers={"Irimi-Run": sdk.current_run_id() or ""},
        )
        return status

    assert fetch() == 200
    reader = irimi.finish()
    [run] = [r for r in reader.list_runs() if r.attribution != "process"]
    assert (run.attribution, run.outcome) == ("sdk", "ok")
    [exchange] = reader.load_run(run.run_id).events
    assert isinstance(exchange, Exchange) and exchange.request.path == "/hello"


class _Posts:
    """A `client.Reporter` that keeps what it is given, in order, with what the agent did."""

    def __init__(self, log: list[str]) -> None:
        self.log = log

    def post(self, run_id: str, action: str, doc: Any) -> None:
        self.log.append(action)


def test_the_start_is_posted_before_the_body_runs_and_the_end_after_it(monkeypatch):
    """So the start reaches irimi before the run's first request and the end after its last. The
    store would make a late start's run `sdk` all the same (#70), so only the order shows it."""
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    log: list[str] = []
    monkeypatch.setattr(runs, "reporter", _Posts(log))

    @sdk.trigger
    def body() -> None:
        log.append("body")

    @sdk.trigger
    async def async_body() -> None:
        log.append("async body")

    body()
    asyncio.run(async_body())
    with sdk.run():
        log.append("block")
    assert log == ["start", "body", "end", "start", "async body", "end", "start", "block", "end"]


def test_the_sdk_never_posts_through_the_agents_proxy(irimi, monkeypatch):
    """The agent's `HTTP_PROXY` names irimi's listener too, and urllib's default opener would send
    the post there as a proxied request whenever NO_PROXY does not cover the endpoint's host. The
    SDK's opener has no proxies at all. The client is built after the variables are set, as the
    SDK's is in an agent `irimi shadow` started: urllib reads them when an opener is built."""
    with _recorder() as (port, hits):
        for name in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
            monkeypatch.setenv(name, f"http://127.0.0.1:{port}")
        for name in ("NO_PROXY", "no_proxy"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setattr(runs, "reporter", client.ControlClient())
        run_id = handle(1)
    assert hits == []
    [run] = irimi.sdk_runs()
    assert (run.run_id, run.outcome) == (run_id, "ok")


def test_an_async_trigger_waits_for_its_posts_off_the_event_loop(monkeypatch):
    """A post takes as long as irimi takes to answer. An async trigger waits for each of its two
    in a worker thread (`asyncio.to_thread`), so the agent's other tasks run meanwhile: here a
    ticker, counted while the start is posted and again while the end is."""
    with _recorder(status=204, delay=0.5) as (port, hits):
        monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
        monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
        monkeypatch.setattr(runs, "reporter", client.ControlClient())

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

    with pytest.raises(ValueError) as caught:
        fails()
    assert caught.value is boom
    [run] = irimi.sdk_runs()
    assert (run.outcome, run.error) == ("error", ErrorInfo("builtins.ValueError", "x"))


def test_a_keyboard_interrupt_ends_the_run_in_error_and_still_propagates(irimi):
    stop = KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt) as caught, sdk.run(name="interrupted"):
        raise stop
    assert caught.value is stop
    [run] = irimi.sdk_runs()
    assert (run.outcome, run.error) == ("error", ErrorInfo("builtins.KeyboardInterrupt", ""))


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
    assert run.outcome == "error"
    assert run.error is not None and run.error.type == "asyncio.exceptions.CancelledError"


def test_a_trigger_cancelled_while_its_start_is_posted_leaves_no_run_current(monkeypatch):
    """`async with` calls no `__aexit__` when `__aenter__` raises, so a task cancelled while its
    start is posted must leave the run itself, or a caller that catches the cancellation and goes
    on would still be inside it."""
    with _recorder(status=204, delay=0.5) as (port, _hits):
        monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
        monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
        monkeypatch.setattr(runs, "reporter", client.ControlClient())

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
    """A KeyboardInterrupt (sync) or a cancellation (async) that arrives while the end is posted
    propagates, as it would from any other line, with the agent's own exception as its
    `__context__`. Either way the run is left: the context variable is always reset."""
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    boom = ValueError("x")

    class InterruptedEnd:
        def post(self, run_id: str, action: str, doc: Any) -> None:
            if action == "end":
                raise KeyboardInterrupt

    monkeypatch.setattr(runs, "reporter", InterruptedEnd())

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

    monkeypatch.setattr(runs, "reporter", SlowEnd())

    @sdk.trigger
    async def fails_async() -> None:
        raise boom

    async def main() -> tuple[BaseException | None, str | None]:
        async def caller() -> tuple[BaseException | None, str | None]:
            try:
                await fails_async()
            except BaseException as exc:  # noqa: BLE001 - what the agent's caller would see
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
    """A coroutine closed rather than awaited - by hand, or a pending task the garbage collector
    reclaims - is closed with GeneratorExit, during which nothing may be awaited: an await there
    is "coroutine ignored GeneratorExit", raised into whoever closed it. So the end is posted
    inline, and the run left even from the collector's context, which is not the one that entered
    it. Neither the closer nor `sys.unraisablehook` sees an error of the SDK's."""
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    log: list[str] = []
    monkeypatch.setattr(runs, "reporter", _Posts(log))
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
    with sdk.run(name="batch"):
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

    with sdk.run(name="fan-out"):
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
    with sdk.run(name="first"):
        first = sdk.current_run_id()
        work = sdk.propagate(sdk.current_run_id)
    with sdk.run(name="second"):
        assert work() == first != sdk.current_run_id()


def test_one_propagated_wrapper_may_run_on_two_threads_at_once(irimi):
    barrier = threading.Barrier(2, timeout=5)

    def meet() -> str | None:
        barrier.wait()
        return sdk.current_run_id()

    with sdk.run(name="shared"):
        run_id = sdk.current_run_id()
        work = sdk.propagate(meet)
        with ThreadPoolExecutor(2) as pool:
            assert [f.result() for f in [pool.submit(work), pool.submit(work)]] == [run_id] * 2


JOB = sdk.run(name="job")  # one object, entered from everywhere, as a module constant is


def test_one_sdk_run_object_may_be_entered_nested_on_threads_and_in_tasks_at_once(irimi):
    """Each entry is a scope of its own, kept per context: nested in itself it joins its own run,
    and two threads or two tasks inside it at once are two runs that end separately."""
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
    assert {(r.trigger.name, r.outcome) for r in found.values() if r.trigger} == {("job", "ok")}


def test_an_exit_in_a_context_that_made_no_entry_ends_nothing_and_does_not_raise(irimi):
    """An async generator closed by another task leaves its `async with sdk.run()` in a context
    other than the one it entered in. That exit has no entry of its own to end, so it ends none,
    and the run is stored incomplete rather than ended by the wrong caller."""
    block = sdk.run(name="moved")
    entered = contextvars.copy_context()
    run_id = entered.run(lambda: (block.__enter__(), sdk.current_run_id())[1])
    contextvars.copy_context().run(block.__exit__, None, None, None)
    assert sdk.current_run_id() is None
    [run] = irimi.sdk_runs()
    assert (run.run_id, run.outcome) == (run_id, None)


def test_an_exit_in_a_context_that_only_inherited_an_entry_leaves_it_to_its_maker(irimi):
    """A thread handed the block's context by `sdk.propagate` holds the block's entry too, but
    did not make it: its stray exit ends nothing and raises nothing, and the block that made the
    entry still ends its own run, once."""
    with JOB:
        run_id = sdk.current_run_id()
        with ThreadPoolExecutor(1) as pool:
            pool.submit(sdk.propagate(JOB.__exit__), None, None, None).result()
        assert sdk.current_run_id() == run_id
    assert sdk.current_run_id() is None
    [run] = irimi.sdk_runs()
    assert (run.run_id, run.outcome) == (run_id, "ok")


def test_a_stray_exit_after_the_block_has_ended_ends_nothing_and_does_not_raise(irimi):
    """The same stray exit as above, made after the block's own: the entry it inherited has been
    taken off by its maker, whose token is spent. Resetting a spent token is a RuntimeError, which
    reached the agent, where one not yet spent is a ValueError, which did not (#74)."""
    with JOB:
        run_id = sdk.current_run_id()
        stray = sdk.propagate(JOB.__exit__)
    with ThreadPoolExecutor(1) as pool:
        pool.submit(stray, None, None, None).result()
    assert sdk.current_run_id() is None
    [run] = irimi.sdk_runs()
    assert (run.run_id, run.outcome) == (run_id, "ok")


def test_instrument_is_called_once_per_new_run_and_never_for_a_joined_one(irimi, monkeypatch):
    calls: list[str | None] = []
    monkeypatch.setattr(runs, "instrument", lambda: calls.append(sdk.current_run_id()))
    outer_id, _ = outer()
    async_id = asyncio.run(handle_async("ch_9"))
    # Inside the run it starts, posted inline or from a worker thread alike.
    assert calls == [outer_id, async_id]


def test_a_start_interrupted_while_posted_leaves_no_run_current(irimi, monkeypatch):
    """A KeyboardInterrupt while a sync start is posted: `with` calls no `__exit__`, so the scope
    leaves the run itself. The next trigger is a run of its own, not a silent join."""

    class Interrupted:
        def __init__(self) -> None:
            self.raised = False

        def post(self, run_id: str, action: str, doc: Any) -> None:
            if not self.raised:
                self.raised = True
                raise KeyboardInterrupt

    monkeypatch.setattr(runs, "reporter", Interrupted())
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
    monkeypatch.setattr(runs, "_instrument_failed", False)
    assert echo(1) == 1 and echo(2) == 2
    [warning] = warnings()
    assert "could not label" in warning.getMessage() and warning.exc_info is not None
    assert [r.outcome for r in irimi.sdk_runs()] == ["ok", "ok"]


def test_an_async_trigger_driven_with_no_asyncio_loop_posts_inline(irimi):
    """A coroutine driven by trio, or by hand, has no asyncio loop for `asyncio.to_thread`: the
    posts are made inline instead of raising into the agent."""

    @sdk.trigger
    async def no_loop() -> str | None:
        return sdk.current_run_id()

    coroutine = no_loop()
    with pytest.raises(StopIteration) as done:
        coroutine.send(None)
    run_id = done.value.value
    [run] = irimi.sdk_runs()
    assert run_id is not None and (run.run_id, run.outcome) == (run_id, "ok")


def test_an_async_trigger_whose_loop_takes_no_more_threads_posts_inline(irimi):
    """A loop whose default executor has been shut down - at the end of `asyncio.run`, or once
    the interpreter is exiting - refuses `asyncio.to_thread` with RuntimeError, which reached the
    agent. The posts are made inline instead (#74)."""

    async def late() -> str | None:
        await asyncio.get_running_loop().shutdown_default_executor()
        return await handle_async("ch_1")

    run_id = asyncio.run(late())
    [run] = irimi.sdk_runs()
    assert run_id is not None and (run.run_id, run.outcome) == (run_id, "ok")


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
    assert run_id is not None and (run.run_id, run.outcome) == (run_id, "ok")
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
    """`functools.wraps` makes the wrapper look async to `inspect.unwrap`, but it returns the
    value, not a coroutine. Wrapped as async, a call returned an unawaited coroutine and the body
    never ran - in production too, with the SDK inactive (#74)."""
    if not active:
        monkeypatch.delenv(paths.ENGINE_ACTIVE_ENV)
    charge, run_id = synchronous_handler("ch_1")
    assert charge == "ch_1" and (run_id is not None) is active
    found = irimi.sdk_runs()
    assert [(r.run_id, r.outcome) for r in found] == ([(run_id, "ok")] if active else [])


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
    """The body is in the coroutine, so the run lasts until it is awaited to its end, in whatever
    task awaits it, and its outcome is the coroutine's. Before, the run ended when the coroutine
    was made, and the body ran outside every run (#74)."""
    log: list[str] = []
    inner = runs.reporter

    class Order:
        def post(self, run_id: str, action: str, doc: Any) -> None:
            log.append(action)
            inner.post(run_id, action, doc)

    runs.reporter = Order()  # restored by the fixture's monkeypatch

    async def main() -> tuple[str | None, str | None]:
        made = settle("ch_1")
        assert log == ["start"] and sdk.current_run_id() is None  # its body has not run yet
        ran_in = await asyncio.create_task(made)
        with pytest.raises(ValueError) as caught:
            await settle("bad")
        assert caught.value is _BOOM
        return ran_in, sdk.current_run_id()

    ran_in, after = asyncio.run(main())
    assert ran_in is not None and after is None and log == ["start", "end", "start", "end"]
    found = {r.run_id: r for r in irimi.sdk_runs()}
    assert found[ran_in].outcome == "ok"
    [failed] = [r for r in found.values() if r.run_id != ran_in]
    assert (failed.outcome, failed.error) == (
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
    """Above `@staticmethod`, the trigger used to return a plain function, which Python binds:
    called on an instance, the static method was given the instance as its first argument, even
    with the SDK inactive. Above `@classmethod`, it was refused as a name passed positionally."""
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
    """A trigger records `module:qualname`, which replay (#84) imports. A partial or an instance
    has none (decorating one used to raise AttributeError), and a class decorated would be
    replaced by a function, so `isinstance` against it would break in production too."""
    with pytest.raises(TypeError, match="function or a method"):
        sdk.trigger(target)


def test_a_function_defined_with_no_module_is_refused_by_its_name():
    """A function `exec` defines in a namespace with no `__name__` has `__module__` None, so no
    `module:qualname` to import. It is refused as the others are, but by its name: the refusal
    said it was "not function", which reads as a contradiction (#74)."""
    namespace: dict[str, Any] = {}
    exec("def on_message(body): ...", namespace)
    with pytest.raises(TypeError, match="on_message, which names no module"):
        sdk.trigger(namespace["on_message"])


def test_a_name_that_is_not_a_string_is_refused_at_decoration_or_kept_as_a_label(irimi):
    with pytest.raises(TypeError, match="name="):
        sdk.trigger("refund")  # type: ignore[call-overload]
    with pytest.raises(TypeError, match="must be a string"):
        sdk.trigger(name=7)  # type: ignore[call-overload]
    with sdk.run(name=None):  # type: ignore[arg-type]
        pass
    with sdk.run(name="n" * 5000):
        pass
    names = sorted(r.trigger.name for r in irimi.sdk_runs() if r.trigger is not None)
    assert names == ["None", "n" * runs.MAX_NAME]


def test_a_trigger_annotated_with_a_type_checking_only_import_still_binds_by_name():
    """Python 3.14 evaluates annotations lazily, and `inspect.signature` would evaluate them: a
    name imported only under `TYPE_CHECKING` raised NameError, and every call was captured as one
    that did not bind. On 3.12 the module needs `from __future__ import annotations` to load."""
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
    """Records the target of every request it gets - absolute-form when it was sent to it as a
    proxy - and answers `status` after `delay` seconds, pointing at `location` when it is set."""

    hits: list[str]
    status = 500
    delay = 0.0
    location = ""

    def do_POST(self) -> None:  # noqa: N802 - the stdlib's spelling
        self.hits.append(self.path)
        time.sleep(self.delay)
        self.send_response(self.status)
        if self.location:
            self.send_header("location", self.location)
        self.send_header("content-length", "0")
        self.end_headers()

    do_GET = do_POST  # noqa: N815

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
        monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
        monkeypatch.setattr(runs, "reporter", client.ControlClient())
        yield hits


@pytest.mark.parametrize("value", [None, "", "0", "true"])
def test_an_inactive_sdk_calls_straight_through(tripwire, monkeypatch, value):
    if value is None:
        monkeypatch.delenv(paths.ENGINE_ACTIVE_ENV, raising=False)
    else:
        monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, value)
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
        # Not even the object's own record of its entries: the issue's "set no context variable".
        assert len(contextvars.copy_context()) == outside
    assert not sdk.active()
    assert tripwire == [] and calls == []


def test_active_is_read_at_each_call(tripwire, monkeypatch):
    monkeypatch.delenv(paths.ENGINE_ACTIVE_ENV, raising=False)
    assert handle(1) is None
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    assert sdk.active() and handle(1) is not None
    assert [path.rsplit("/", 1)[1] for path in tripwire] == ["start", "end"]


# -- the SDK never raises into the agent ---------------------------------------------------------


def test_a_closed_control_port_costs_one_warning_and_no_time(monkeypatch, warnings):
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{_closed_port()}/_irimi")
    monkeypatch.setattr(runs, "reporter", client.ControlClient())
    began = time.monotonic()
    first = handle(1)
    second = asyncio.run(handle_async("ch_1"))
    assert time.monotonic() - began < 3
    assert None not in (first, second)
    [warning] = warnings()
    assert warning.name == "irimi.sdk" and warning.levelno == logging.WARNING
    assert "unreachable" in warning.getMessage()


def test_an_unset_irimi_control_is_one_warning_and_the_run_goes_on(monkeypatch, warnings):
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    monkeypatch.delenv(paths.CONTROL_ENV, raising=False)
    monkeypatch.setattr(runs, "reporter", client.ControlClient())
    assert handle(1) is not None and handle(2) is not None
    [warning] = warnings()
    assert "unset" in warning.getMessage() and paths.CONTROL_ENV in warning.getMessage()


def test_a_refused_post_is_one_warning_quoting_irimis_answer(irimi, monkeypatch, warnings):
    monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{irimi.port}/_irimi/nope")
    assert handle(1) is not None
    [warning] = warnings()
    assert "rejected: HTTP 404" in warning.getMessage()
    assert irimi.sdk_runs() == []


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_a_redirect_is_a_refused_post_never_followed(monkeypatch, warnings, status):
    """irimi's endpoint never redirects, so a redirect is an answer from something else. urllib
    followed a 301, 302 or 303 as a GET to wherever it pointed, outside the proxy, and called the
    run recorded when that answered 200; it is refused like any answer outside 2xx (#74)."""
    with _recorder(status=200) as (elsewhere, landed):
        with _recorder(status=status, location=f"http://127.0.0.1:{elsewhere}/x") as (port, hits):
            monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{port}/_irimi")
            client.ControlClient().post("r1", "start", {})
    assert len(hits) == 1 and landed == []
    [warning] = warnings()
    assert f"rejected: HTTP {status}" in warning.getMessage()


def test_irimi_control_with_a_trailing_slash_names_the_same_endpoint(irimi, monkeypatch, warnings):
    """irimi sets `IRIMI_CONTROL` with no trailing slash (#73), but serve mode (#77) has the agent's
    deployment set it by hand. One typed with a slash posted to `/_irimi//runs/...`, a 404, and
    every run was lost."""
    monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{irimi.port}/_irimi/")
    run_id = handle(1)
    assert warnings() == []
    [run] = irimi.sdk_runs()
    assert (run.run_id, run.outcome) == (run_id, "ok")


def test_a_control_endpoint_that_never_answers_times_out(monkeypatch, warnings):
    silent = socket.socket()
    silent.bind(("127.0.0.1", 0))
    silent.listen(8)  # accepts into the backlog and never answers
    try:
        monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
        monkeypatch.setenv(paths.CONTROL_ENV, f"http://127.0.0.1:{silent.getsockname()[1]}")
        monkeypatch.setattr(runs, "reporter", client.ControlClient())
        monkeypatch.setattr(client, "TIMEOUT_S", 0.2)
        began = time.monotonic()
        assert handle(1) is not None
        assert time.monotonic() - began < 2
    finally:
        silent.close()
    [warning] = warnings()
    assert "timeout" in warning.getMessage()


def test_a_fault_of_the_clients_own_is_a_warning_not_an_exception(monkeypatch, warnings):
    monkeypatch.setenv(paths.CONTROL_ENV, "http://127.0.0.1:1/_irimi")
    client.ControlClient().post("r1", "start", {"trigger": object()})
    [warning] = warnings()
    assert "failed: TypeError" in warning.getMessage()


# -- decoration ----------------------------------------------------------------------------------


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


def test_the_public_api_is_the_issues():
    assert sorted(sdk.__all__) == [
        "active",
        "current_run_id",
        "instrument",
        "propagate",
        "run",
        "trigger",
    ]
    assert sdk.instrument() is None


def test_importing_the_sdk_imports_no_mitmproxy():
    code = "import irimi.sdk, sys; assert not [m for m in sys.modules if m.startswith('mitmproxy')]"
    subprocess.run([sys.executable, "-c", code], check=True)


# -- capture: one test per row of the table (sdk/capture.py, docs/trace-format.md) ---------------


@dataclasses.dataclass
class Ticket:
    ticket_id: str
    amount: int
    note: str = dataclasses.field(default="", init=False)


class Model:
    """Shaped like a pydantic v2 model: a callable `model_dump(mode="json")`."""

    def __init__(self, **fields: Any) -> None:
        self.fields = fields

    def model_dump(self, mode: str = "python") -> dict[str, Any]:
        assert mode == "json"
        return dict(self.fields)


class Color(enum.IntEnum):
    RED = 1


class Unprintable:
    def __repr__(self) -> str:
        raise RuntimeError("no")


@pytest.mark.parametrize(
    ("value", "encoded"),
    [(None, None), (True, True), (0, 0), (-12, -12), ("é", "é"), (1.5, 1.5), (0.0, 0.0)],
)
def test_a_json_scalar_is_itself_and_replayable(value, encoded):
    assert capture.capture(value) == (encoded, True)


@pytest.mark.parametrize(("value", "text"), [(float("nan"), "nan"), (float("inf"), "inf"),
                                             (float("-inf"), "-inf")])  # fmt: skip
def test_a_non_finite_float_is_its_repr_and_not_replayable(value, text):
    assert capture.capture(value) == ({"__irimi_repr__": text}, False)


def test_a_list_and_a_tuple_are_json_lists_replayable_when_every_item_is():
    assert capture.capture([1, (2, "x")]) == ([1, [2, "x"]], True)
    assert capture.capture((1, float("nan"))) == ([1, {"__irimi_repr__": "nan"}], False)


def test_a_dict_with_string_keys_is_an_object_replayable_when_every_value_is():
    assert capture.capture({"a": 1, "b": [None]}) == ({"a": 1, "b": [None]}, True)
    assert capture.capture({"a": b"\x00"}) == ({"a": {"__irimi_bytes__": "AA=="}}, True)
    assert capture.capture({"a": object})[1] is False


def test_a_dict_with_any_other_key_is_its_repr_and_not_replayable():
    assert capture.capture({1: "a"}) == ({"__irimi_repr__": "{1: 'a'}"}, False)
    assert capture.capture({"a": 1, ("t",): 2})[1] is False


def test_a_dict_holding_a_key_capture_writes_is_its_repr_so_a_marker_is_never_ambiguous():
    value = {"__irimi_bytes__": "AA=="}
    assert capture.capture(value) == ({"__irimi_repr__": repr(value)}, False)


def test_a_dataclass_is_its_importable_class_and_its_init_fields():
    assert capture.capture(Ticket("T-1", 500)) == (
        {"__irimi_type__": f"{HERE}:Ticket", "value": {"ticket_id": "T-1", "amount": 500}},
        True,
    )


def test_a_dataclass_is_replayable_only_when_its_fields_are_and_its_class_can_be_imported():
    @dataclasses.dataclass
    class Local:
        x: int

    encoded, replayable = capture.capture(Local(1))
    assert encoded == {"__irimi_type__": f"{HERE}:{Local.__qualname__}", "value": {"x": 1}}
    assert "<locals>" in Local.__qualname__ and replayable is False
    assert capture.capture(Ticket("T-1", float("nan")))[1] is False  # type: ignore[arg-type]


def test_an_object_with_model_dump_is_its_class_and_its_json_dump():
    assert capture.capture(Model(id="cus_1", tags=["a"])) == (
        {"__irimi_type__": f"{HERE}:Model", "value": {"id": "cus_1", "tags": ["a"]}},
        True,
    )


def test_bytes_are_base64_and_replayable():
    assert capture.capture(b"hi\xff") == ({"__irimi_bytes__": "aGn/"}, True)


@pytest.mark.parametrize(
    "value",
    [object(), Color.RED, bytearray(b"x"), {1, 2}, lambda: 1],
    ids=["object", "IntEnum", "bytearray", "set", "function"],
)
def test_anything_else_is_its_repr_and_not_replayable(value):
    assert capture.capture(value) == ({"__irimi_repr__": repr(value)[:1000]}, False)


def test_a_repr_is_cut_to_1000_characters():
    class Long:
        def __repr__(self) -> str:
            return "x" * 5000

    assert capture.capture(Long()) == ({"__irimi_repr__": "x" * 1000}, False)


def test_a_repr_that_raises_is_named_unrepresentable():
    assert capture.capture(Unprintable()) == (
        {"__irimi_repr__": "<unrepresentable Unprintable>"},
        False,
    )


def test_a_model_dump_that_raises_is_its_repr_and_never_the_agents_error():
    class Broken:
        def model_dump(self, mode: str) -> dict[str, Any]:
            raise ValueError("no")

        def __repr__(self) -> str:
            return "<Broken>"

    assert capture.capture(Broken()) == ({"__irimi_repr__": "<Broken>"}, False)


def test_an_int_too_long_to_write_is_unrepresentable():
    assert capture.capture(10**5000) == ({"__irimi_repr__": "<unrepresentable int>"}, False)


def test_nesting_deeper_than_32_levels_becomes_too_deep():
    value: list[Any] = []
    innermost = value
    for _ in range(39):  # 40 lists, one inside the next
        innermost.append([])
        innermost = innermost[0]
    encoded, replayable = capture.capture(value)
    depth = 0
    while isinstance(encoded, list):
        [encoded] = encoded
        depth += 1
    assert (depth, encoded, replayable) == (33, {"__irimi_repr__": "<too deep>"}, False)
    ok: list[Any] = [1]
    for _ in range(31):
        ok = [ok]
    assert capture.capture(ok)[1] is True  # 32 lists deep, every item reached


def test_args_past_1_mib_are_replaced_by_their_size():
    big = "x" * (2 * 1024 * 1024)
    assert capture.capture(big) == ({"__irimi_truncated__": True, "bytes": len(big) + 2}, False)
    under = "x" * (1024 * 1024 - 2)
    assert capture.capture(under) == (under, True)


def test_args_that_grow_past_1_mib_when_written_are_replaced_by_their_exact_size():
    escaped = "\x00" * (300 * 1024)  # 300 KiB that JSON writes as six bytes each
    encoded, replayable = capture.capture(escaped)
    assert encoded == {"__irimi_truncated__": True, "bytes": len(capture.serialized(escaped))}
    assert replayable is False


def test_a_value_that_shares_itself_is_bounded_in_time():
    shared: list[Any] = []
    shared.extend([shared, shared])
    began = time.monotonic()
    encoded, replayable = capture.capture(shared)
    assert time.monotonic() - began < 2
    assert isinstance(encoded, dict) and encoded["__irimi_truncated__"] is True
    assert encoded["bytes"] > capture.MAX_CAPTURED_BYTES and replayable is False


def test_capture_call_binds_arguments_by_parameter_name():
    def f(a: int, b: int = 2, *rest: int, c: str = "", **more: Any) -> None: ...

    assert capture.capture_call(f, (1, 3, 4), {"c": "x", "d": True}) == (
        {"a": 1, "b": 3, "rest": [4], "c": "x", "more": {"d": True}},
        True,
    )
    assert capture.capture_call(f, (1,), {}) == ({"a": 1}, True)  # defaults are not applied


def test_capture_call_leaves_out_a_receiver_named_self_or_cls():
    class Thing:
        def method(self, x: int) -> None: ...

        @classmethod
        def build(cls, x: int) -> None: ...

    assert capture.capture_call(Thing.method, (Thing(), 1), {}) == ({"x": 1}, True)
    assert capture.capture_call(Thing.build.__func__, (Thing, 1), {}) == ({"x": 1}, True)

    def plain(other: Any, x: int) -> None: ...

    assert capture.capture_call(plain, (object(), 1), {})[1] is False


def test_a_call_that_does_not_bind_is_captured_as_args_and_kwargs_and_not_replayable():
    def f(a: int) -> None: ...

    assert capture.capture_call(f, (1, 2), {"z": 3}) == (
        {"args": [1, 2], "kwargs": {"z": 3}},
        False,
    )


def test_a_dataclass_defined_in_main_is_not_replayable():
    """Replay imports the class by name in another process, where `__main__` is another module."""
    ticket = dataclasses.make_dataclass("Ticket", ["x"], module="__main__")
    assert capture.capture(ticket(1)) == (
        {"__irimi_type__": "__main__:Ticket", "value": {"x": 1}},
        False,
    )


def test_a_trigger_parameter_may_nest_as_deep_as_a_run_trigger():
    deep: list[Any] = [1]
    for _ in range(31):
        deep = [deep]  # 32 lists deep

    def f(value: Any) -> None: ...

    assert capture.capture(deep)[1] is True
    assert capture.capture_call(f, (deep,), {})[1] is True
    assert capture.capture_call(f, ([deep],), {})[1] is False
