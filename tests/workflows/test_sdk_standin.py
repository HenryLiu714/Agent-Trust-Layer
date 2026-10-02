"""The SDK stand-in, `examples/workflows/sdk.py`, against the `irimi.sdk` that replaces it (#89).

`irimi.sdk` does not exist yet (#74, #76, #84), so each test here reloads the stand-in over a fake
one put in `sys.modules`, and reloads it again afterwards over whatever is really installed. The
stand-in must: resolve each name on its own (#74 ships before #76's `tool`), log `run.start` and
`run.end` with the REAL run id, leave the real `tool`'s decoration-time checks to it, fall back
only when `irimi.sdk` is absent (never when it is broken), and forward names it does not define.

The last test is the fake internet's resolver: no lookup of an unserved name resolves.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import importlib
import importlib.abc
import importlib.machinery
import inspect
import itertools
import json
import socket
import sys
import types
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from examples.workflows import agentkit
from examples.workflows import sdk as sdk_module
from examples.workflows.harness.internet import FakeInternet, Req, Resp

AGENTS = tuple(
    f"examples.workflows.{name}.agent"
    for name in (
        "w01_ticket_triage",
        "w02_nightly_reconcile",
        "w03_queue_worker",
        "w04_slack_ops_bot",
        "w05_dispute_responder",
        "w06_crm_db_agent",
        "w07_streaming_assistant",
        "w08_orchestrator",
        "w09_scope_gauntlet",
        "w10_flaky_upstream",
        "w11_leaky_agent",
    )
)
W6 = "examples.workflows.w06_crm_db_agent.agent"
REAL_TYPE_ERROR = "irimi.sdk refuses"


# -- a fake irimi.sdk ----------------------------------------------------------------------------


def fake_sdk(*, active: bool = True, with_tool: bool = False, **extra: Any) -> types.ModuleType:
    """A module shaped like #74's `irimi.sdk` (and #76's `tool` with `with_tool`): a context
    variable run id that nested triggers join, minted from a counter so a test can tell it from
    the stand-in's `token_hex`. Every decoration is recorded in `mod.decorated`."""
    mod = types.ModuleType("irimi.sdk")
    run_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("fake_run", default=None)
    ids = (f"real{n:012d}" for n in itertools.count(1))
    mod.decorated = []  # type: ignore[attr-defined]
    mod.minted = []  # type: ignore[attr-defined]

    @contextlib.contextmanager
    def entered() -> Iterator[None]:
        if not active or run_var.get() is not None:
            yield
            return
        run_id = next(ids)
        mod.minted.append(run_id)  # type: ignore[attr-defined]
        token = run_var.set(run_id)
        try:
            yield
        finally:
            run_var.reset(token)

    def trigger(fn: Callable[..., Any] | None = None, *, name: str | None = None) -> Any:
        def decorate(f: Callable[..., Any]) -> Callable[..., Any]:
            if inspect.isgeneratorfunction(f) or inspect.isasyncgenfunction(f):
                raise TypeError(f"{REAL_TYPE_ERROR}: a generator trigger")
            mod.decorated.append(("trigger", name, f))  # type: ignore[attr-defined]
            if inspect.iscoroutinefunction(f):

                @functools.wraps(f)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    with entered():
                        return await f(*args, **kwargs)

                return async_wrapper

            @functools.wraps(f)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                with entered():
                    return f(*args, **kwargs)

            return wrapper

        return decorate(fn) if fn is not None else decorate

    class run:  # noqa: N801
        def __init__(self, trigger: Any = None, name: str = "run") -> None:
            self._cm = entered()

        def __enter__(self) -> None:
            self._cm.__enter__()

        def __exit__(self, *exc: Any) -> Any:
            return self._cm.__exit__(*exc)

        async def __aenter__(self) -> None:
            self._cm.__enter__()

        async def __aexit__(self, *exc: Any) -> Any:
            return self._cm.__exit__(*exc)

    def propagate(fn: Callable[..., Any]) -> Callable[..., Any]:
        ctx = contextvars.copy_context()
        return lambda *a, **kw: ctx.copy().run(fn, *a, **kw)

    mod.trigger = trigger  # type: ignore[attr-defined]
    mod.run = run  # type: ignore[attr-defined]
    mod.current_run_id = run_var.get  # type: ignore[attr-defined]
    mod.propagate = propagate  # type: ignore[attr-defined]
    mod.active = lambda: active  # type: ignore[attr-defined]

    if with_tool:

        def tool(*, kind: str, shadow: Any = None, name: str | None = None) -> Any:
            def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
                mod.decorated.append(("tool", kind, shadow, fn))  # type: ignore[attr-defined]
                if kind not in ("read", "write"):
                    raise TypeError(f"{REAL_TYPE_ERROR}: kind {kind!r}")
                if (kind == "write") != (shadow is not None):
                    raise TypeError(f"{REAL_TYPE_ERROR}: shadow for {kind}")
                if shadow is not None and not callable(shadow):
                    raise TypeError(f"{REAL_TYPE_ERROR}: shadow not callable")
                if shadow is not None and (
                    inspect.iscoroutinefunction(shadow) != inspect.iscoroutinefunction(fn)
                ):
                    raise TypeError(f"{REAL_TYPE_ERROR}: sync/async mismatch")
                if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
                    raise TypeError(f"{REAL_TYPE_ERROR}: a generator tool")
                return shadow if kind == "write" and active else fn

            return decorate

        mod.tool = tool  # type: ignore[attr-defined]

    for key, value in extra.items():
        setattr(mod, key, value)
    return mod


class _BrokenSDK(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    """Finds `irimi.sdk` and fails to execute it, as a broken real SDK would."""

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc

    def find_spec(self, fullname: str, path: Any, target: Any = None) -> Any:
        if fullname == "irimi.sdk":
            return importlib.machinery.ModuleSpec(fullname, self)
        return None

    def create_module(self, spec: Any) -> None:
        return None

    def exec_module(self, module: types.ModuleType) -> None:
        raise self.exc


# -- fixtures ------------------------------------------------------------------------------------


@pytest.fixture
def load_sdk() -> Iterator[Callable[..., types.ModuleType]]:
    """`load(fake)` reloads the stand-in with `fake` as `irimi.sdk`; `load(None)` as if there is
    none; `load(broken=exc)` over an `irimi.sdk` whose import raises `exc`. Afterwards everything
    is put back and the stand-in reloaded over what is really installed, so no other test sees a
    fake."""
    with pytest.MonkeyPatch.context() as mp:

        def load(fake: Any = None, *, broken: BaseException | None = None) -> types.ModuleType:
            if broken is not None:
                mp.delitem(sys.modules, "irimi.sdk", raising=False)
                mp.setattr(sys, "meta_path", [_BrokenSDK(broken), *sys.meta_path])
            else:
                mp.setitem(sys.modules, "irimi.sdk", fake)
            return importlib.reload(sdk_module)

        try:
            yield load
        finally:
            mp.undo()
            sys.modules[sdk_module.__name__] = sdk_module  # a failed reload may have dropped it
            importlib.reload(sdk_module)


@pytest.fixture
def fresh_import() -> Iterator[Callable[[str], types.ModuleType]]:
    """`fresh_import(name)` imports an agent module anew, under whatever `irimi.sdk` the stand-in
    was loaded with, and puts the previously imported one back afterwards."""
    with pytest.MonkeyPatch.context() as mp:

        def imp(name: str) -> types.ModuleType:
            parent, _, leaf = name.rpartition(".")
            package = importlib.import_module(parent)
            mp.setattr(package, leaf, getattr(package, leaf, None), raising=False)
            mp.setitem(sys.modules, name, None)  # records the entry to restore
            del sys.modules[name]
            return importlib.import_module(name)

        yield imp


@pytest.fixture
def obs(tmp_path, monkeypatch) -> Callable[[str], list[dict[str, Any]]]:
    """The observation log an agent writes, read back by event, without timestamps."""
    path = tmp_path / "obs.jsonl"
    monkeypatch.setenv(agentkit.OBS_ENV, str(path))
    monkeypatch.setenv(agentkit.STATE_ENV, str(tmp_path / "state"))
    monkeypatch.delenv(sdk_module.ENGINE_ACTIVE_ENV, raising=False)

    def read(event: str) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        lines = [json.loads(line) for line in path.read_text().splitlines()]
        return [{k: v for k, v in e.items() if k != "t"} for e in lines if e["event"] == event]

    return read


# -- (a) #74 without #76: per-name resolution ----------------------------------------------------


def test_with_only_74s_names_every_agent_imports_and_tools_are_the_stand_ins(
    load_sdk, fresh_import, obs
):
    fake = fake_sdk(active=True)
    sdk = load_sdk(fake)
    assert sdk.trigger is not fake.trigger  # wrapped, for the log
    assert sdk._from_real("tool") is None
    for name in AGENTS:
        fresh_import(name)  # an AttributeError on `tool` would fail here
    w6 = sys.modules[W6]

    @sdk.trigger(name="enrich")
    def enrich() -> Any:
        return w6.upsert_account({"id": "acc_1"})

    # The fake says active (IRIMI_ENGINE_ACTIVE is unset): the stand-in `tool` asks the real
    # `active()`, so the write tool's stand-in runs, inside the real run.
    assert enrich() == {"stood_in": True, "id": "acc_1"}
    [minted] = fake.minted
    assert obs("tool") == [
        {"event": "tool", "name": "crm.upsert_account", "kind": "write", "ran": "shadow",
         "run": minted},
    ]  # fmt: skip
    assert w6.connect_with("postgres://u:p@db.internal/crm") == {"connected": "db.internal/crm"}
    assert obs("tool")[-1]["ran"] == "real"


def test_with_only_74s_names_the_stand_in_tool_still_refuses_at_decoration(load_sdk):
    sdk = load_sdk(fake_sdk())

    def real(x: int) -> int:
        return x

    with pytest.raises(TypeError, match="must name a shadow stand-in"):
        sdk.tool(kind="write")(real)


# -- (b) run.start / run.end carry the real run id -----------------------------------------------


def test_a_real_trigger_logs_one_run_with_the_real_id_and_nested_triggers_join(load_sdk, obs):
    fake = fake_sdk(active=True)
    sdk = load_sdk(fake)

    @sdk.trigger
    def inner() -> str | None:
        return sdk.current_run_id()

    @sdk.trigger(name="outer")
    def outer(x: int, *, y: str = "a") -> tuple[str | None, str | None]:
        return sdk.current_run_id(), inner()

    outer_id, inner_id = outer(1, y="b")
    assert outer_id == inner_id == "real000000000001" == fake.minted[0]
    assert obs("run.start") == [{"event": "run.start", "name": "outer", "run": outer_id}]
    assert obs("run.end") == [
        {"event": "run.end", "name": "outer", "run": outer_id, "outcome": "ok"}
    ]
    # The real decorator got the name as given and a function with the agent's signature, which
    # #74 binds the captured args against and names the entrypoint after.
    [(_, inner_name, inner_fn), (_, outer_name, outer_fn)] = fake.decorated
    assert (inner_name, outer_name) == (None, "outer")
    assert list(inspect.signature(outer_fn).parameters) == ["x", "y"]
    assert (outer_fn.__module__, outer_fn.__qualname__) == (__name__, outer.__qualname__)


def test_a_real_trigger_that_raises_logs_the_error_and_re_raises_the_same_object(load_sdk, obs):
    sdk = load_sdk(fake_sdk(active=True))
    boom = ValueError("x")

    @sdk.trigger
    def fails() -> None:
        raise boom

    with pytest.raises(ValueError) as caught:
        fails()
    assert caught.value is boom
    [end] = obs("run.end")
    assert (end["outcome"], end["error"], end["run"]) == ("error", "ValueError", "real000000000001")


def test_real_run_and_async_trigger_log_the_real_id(load_sdk, obs):
    fake = fake_sdk(active=True)
    sdk = load_sdk(fake)

    with sdk.run(name="batch", trigger={"date": "2026-09-29"}):
        sync_id = sdk.current_run_id()

    @sdk.trigger(name="handle_async")
    async def handle() -> str | None:
        return sdk.current_run_id()

    async def both() -> list[str | None]:
        async with sdk.run(name="stream"):
            in_run = sdk.current_run_id()
        return [in_run, *await asyncio.gather(handle(), handle())]

    ids = [sync_id, *asyncio.run(both())]
    assert ids == fake.minted and len(set(ids)) == 4
    assert [(e["name"], e["run"]) for e in obs("run.start")] == list(
        zip(["batch", "stream", "handle_async", "handle_async"], ids, strict=True)
    )
    assert sorted(e["run"] for e in obs("run.end")) == sorted(ids)
    assert sdk.current_run_id() is None


def test_an_inactive_real_sdk_logs_no_run(load_sdk, obs):
    sdk = load_sdk(fake_sdk(active=False))

    @sdk.trigger
    def work() -> int:
        return 7

    with sdk.run(name="batch"):
        pass
    assert work() == 7
    assert obs("run.start") == obs("run.end") == []


def test_a_generator_trigger_is_refused_by_the_real_sdk(load_sdk):
    sdk = load_sdk(fake_sdk())

    def gen() -> Any:
        yield 1

    with pytest.raises(TypeError, match=REAL_TYPE_ERROR):
        sdk.trigger(gen)


@pytest.mark.parametrize("real", [True, False], ids=["real", "absent"])
def test_a_trigger_above_a_static_or_class_method_keeps_it_one(load_sdk, obs, monkeypatch, real):
    """`irimi.sdk`'s trigger keeps a `staticmethod` or `classmethod` one above which it is written
    (#74). The stand-in's own wrapper, put between, must too: it called the descriptor itself, so
    a class method was not callable, and a static method made a plain function would be bound and
    given the instance as an argument."""
    fake = fake_sdk(active=True)
    sdk = load_sdk(fake if real else None)
    monkeypatch.setenv(sdk.ENGINE_ACTIVE_ENV, "1")

    class Handlers:
        @sdk.trigger
        @staticmethod
        def static(charge: str) -> tuple[str, str | None]:
            return charge, sdk.current_run_id()

        @sdk.trigger(name="by_class")
        @classmethod
        def by_class(cls, charge: str) -> tuple[str, str | None]:
            return f"{cls.__name__}:{charge}", sdk.current_run_id()

    calls = [(owner.static("ch_1"), owner.by_class("ch_2")) for owner in (Handlers, Handlers())]
    results = [result for pair in calls for result in pair]
    assert [value for value, _ in results] == ["ch_1", "Handlers:ch_2"] * 2
    ids = [run_id for _, run_id in results]
    assert None not in ids and len(set(ids)) == 4
    static = Handlers.static.__qualname__
    assert [e["name"] for e in obs("run.start")] == [static, "by_class"] * 2
    if real:
        assert ids == fake.minted


# -- (c) the real tool's decoration-time checks decide -------------------------------------------


def test_w6_decoration_errors_are_the_real_tools_own(load_sdk, fresh_import, obs):
    fake = fake_sdk(active=True, with_tool=True)
    load_sdk(fake)
    w6 = fresh_import(W6)
    fake.decorated.clear()

    raised = w6.decoration_errors()

    assert len(raised) == 7
    messages = [e["message"] for e in obs("decoration_error")]
    assert len(messages) == 7 and all(m.startswith(REAL_TYPE_ERROR) for m in messages), messages
    # What #76 checks reached the real decorator as the agent wrote it.
    seen = {(kind, shadow if not callable(shadow) else "fn", fn.__name__) for _, kind, shadow, fn
            in fake.decorated}  # fmt: skip
    assert ("write", "mock_database_write", "real") in seen
    assert any(inspect.isgeneratorfunction(fn) for *_, fn in fake.decorated)


def test_the_stand_in_does_not_pre_empt_a_real_tool_that_accepts(load_sdk, obs):
    fake = fake_sdk(active=True, with_tool=True)

    def lenient(*, kind: str, shadow: Any = None, name: str | None = None) -> Any:
        return lambda fn: fn

    fake.tool = lenient  # type: ignore[attr-defined]
    sdk = load_sdk(fake)

    def real(x: int) -> int:
        return x

    assert sdk.tool(kind="delete")(real)(3) == 3  # the stand-in alone would raise TypeError


def test_a_real_write_tool_logs_which_function_ran(load_sdk, obs):
    sdk = load_sdk(fake_sdk(active=True, with_tool=True))

    def stand_in(x: int) -> str:
        return "stood in"

    @sdk.tool(kind="write", shadow=stand_in, name="t.write")
    def write(x: int) -> str:
        raise AssertionError("the real write ran")

    assert write(1) == "stood in"
    assert [(e["name"], e["ran"]) for e in obs("tool")] == [("t.write", "shadow")]


# -- (d) only absence falls back ------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        ImportError("cannot import name 'ToolCall' from 'irimi.trace'"),
        ModuleNotFoundError("No module named 'some_dependency'", name="some_dependency"),
        ModuleNotFoundError("No module named 'irimi.sdk.capture'", name="irimi.sdk.capture"),
    ],
    ids=["import-error", "missing-dependency", "missing-submodule"],
)
def test_an_import_error_inside_irimi_sdk_propagates(load_sdk, exc):
    with pytest.raises(ImportError) as caught:
        load_sdk(broken=exc)
    assert caught.value is exc


def test_an_absent_irimi_sdk_falls_back_to_the_stand_in(load_sdk, obs, monkeypatch):
    sdk = load_sdk(None)  # None in sys.modules: ModuleNotFoundError named 'irimi.sdk'
    assert sdk._real is None
    monkeypatch.setenv(sdk.ENGINE_ACTIVE_ENV, "1")

    @sdk.trigger
    def work() -> str | None:
        return sdk.current_run_id()

    run_id = work()
    assert run_id is not None and len(run_id) == 16 and not run_id.startswith("real")
    assert [(e["name"], e["run"]) for e in obs("run.start")] == [(work.__qualname__, run_id)]


# -- (e) names the stand-in does not define ------------------------------------------------------


def test_unknown_public_names_forward_to_the_real_sdk(load_sdk):
    class ReplayResult:
        pass

    def replay(run_id: str, fn: Any = None, /, *args: Any, **kwargs: Any) -> ReplayResult:
        return ReplayResult()

    fake = fake_sdk(replay=replay, ReplayResult=ReplayResult, _private=object())
    sdk = load_sdk(fake)
    assert sdk.replay is replay
    assert isinstance(sdk.replay("abc"), ReplayResult)
    from examples.workflows.sdk import ReplayResult as imported  # noqa: PLC0415

    assert imported is ReplayResult
    with pytest.raises(AttributeError):
        _ = sdk._private
    with pytest.raises(AttributeError):
        _ = sdk.no_such_name


def test_without_the_real_sdk_unknown_names_are_attribute_errors(load_sdk):
    sdk = load_sdk(None)
    with pytest.raises(AttributeError, match="replay"):
        _ = sdk.replay


# -- the fake internet's resolver ----------------------------------------------------------------


class _Stripe:
    hosts = ("api.stripe.com",)

    def handle(self, req: Req) -> Resp:
        return Resp()

    def is_write(self, req: Req) -> bool:
        return False


def test_resolving_refuses_every_lookup_of_a_name_it_does_not_serve():
    originals = (socket.getaddrinfo, socket.gethostbyname, socket.gethostbyname_ex)
    internet = FakeInternet([_Stripe()])
    lookups: dict[str, Callable[[str], Any]] = {
        "getaddrinfo": lambda host: socket.getaddrinfo(host, 443),
        "gethostbyname": lambda host: socket.gethostbyname(host),
        "gethostbyname_ex": lambda host: socket.gethostbyname_ex(host),
    }
    with internet.resolving():
        for lookup in lookups.values():
            for name in ("example.com", "8.8.8.8"):
                with pytest.raises(socket.gaierror, match="not on the fake internet"):
                    lookup(name)
        assert socket.gethostbyname("API.Stripe.com.") == "127.0.0.1"
        assert socket.gethostbyname_ex("api.stripe.com")[2] == ["127.0.0.1"]
        assert {ai[4][0] for ai in socket.getaddrinfo("api.stripe.com", 443)} == {"127.0.0.1"}
        assert socket.gethostbyname("127.0.0.1") == "127.0.0.1"
    assert (socket.getaddrinfo, socket.gethostbyname, socket.gethostbyname_ex) == originals
