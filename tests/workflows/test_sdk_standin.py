"""The SDK stand-in, `examples/workflows/sdk.py`, against the `irimi.sdk` that replaces it.

Each test here reloads the stand-in over a fake `irimi.sdk` put in `sys.modules`, and reloads it
again afterwards over whatever is really installed. The stand-in must: resolve each name on its
own (an `irimi.sdk` without `tool` still serves the rest), log `run.start` and
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
import threading
import types
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from examples.workflows import agentkit
from examples.workflows import sdk as sdk_module
from examples.workflows.harness.internet import FakeInternet, Req, Resp, resolving_only
from irimi.protocol.control import RecordedOutcome
from irimi.protocol.env import REPLAY_RUN_ENV, RUN_ENV

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
    """A module shaped like `irimi.sdk` (with `read_tool` and `write_tool` when `with_tool`): a
    context variable run id that nested triggers join, minted from a counter so a test can tell
    it from the stand-in's `token_hex`. Every decoration is recorded in `mod.decorated`."""
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

    class run:
        def __init__(self, name: str, *, inputs: Any = None) -> None:
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
    mod.is_active = lambda: active  # type: ignore[attr-defined]

    if with_tool:

        def tool(kind: str, shadow: Any) -> Any:
            def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
                mod.decorated.append(("tool", kind, shadow, fn))  # type: ignore[attr-defined]
                if shadow is not None and not callable(shadow):
                    raise TypeError(f"{REAL_TYPE_ERROR}: shadow not callable")
                if shadow is fn:
                    raise TypeError(f"{REAL_TYPE_ERROR}: shadow is the function")
                if inspect.isgeneratorfunction(shadow):
                    raise TypeError(f"{REAL_TYPE_ERROR}: a generator shadow")
                if shadow is not None and (
                    inspect.iscoroutinefunction(shadow) != inspect.iscoroutinefunction(fn)
                ):
                    raise TypeError(f"{REAL_TYPE_ERROR}: sync/async mismatch")
                if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
                    raise TypeError(f"{REAL_TYPE_ERROR}: a generator tool")
                return shadow if kind == "write" and active else fn

            return decorate

        def read_tool(fn: Any = None, /, *, name: str | None = None) -> Any:
            return tool("read", None) if fn is None else tool("read", None)(fn)

        def write_tool(*, fake: Any, name: str | None = None) -> Any:
            return tool("write", fake)

        mod.read_tool = read_tool  # type: ignore[attr-defined]
        mod.write_tool = write_tool  # type: ignore[attr-defined]

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


# -- (a) an SDK without `tool`: per-name resolution ----------------------------------------------


def test_with_only_74s_names_every_agent_imports_and_tools_are_the_stand_ins(
    load_sdk, fresh_import, obs
):
    fake = fake_sdk(active=True)
    sdk = load_sdk(fake)
    assert sdk.trigger is not fake.trigger  # wrapped, for the log
    assert sdk._from_real("write_tool") is None
    for name in AGENTS:
        fresh_import(name)  # an AttributeError on `write_tool` would fail here
    w6 = sys.modules[W6]

    @sdk.trigger(name="enrich")
    def enrich() -> Any:
        return w6.upsert_account({"id": "acc_1"})

    # The fake says active (IRIMI_ENGINE_ACTIVE is unset): the stand-in `write_tool` asks the
    # real `is_active()`, so the write tool's fake runs, inside the real run.
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

    with pytest.raises(TypeError, match="real is the function itself"):
        sdk.write_tool(fake=real)(real)


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
    # The run's start says the type of each argument the trigger was called with, and its end
    # what the trigger returned.
    assert obs("run.start") == [
        {
            "event": "run.start",
            "name": "outer",
            "run": outer_id,
            "called_with": {"x": "int", "y": "str"},
        }
    ]
    assert obs("run.end") == [
        {
            "event": "run.end",
            "name": "outer",
            "run": outer_id,
            "outcome": "ok",
            "returned": [outer_id, inner_id],
        }
    ]
    # The real decorator got the name as given and a function with the agent's signature, which
    # the SDK binds the captured args against and names the entrypoint after.
    [(_, inner_name, inner_fn), (_, outer_name, outer_fn)] = fake.decorated
    assert (inner_name, outer_name) == (None, "outer")
    assert list(inspect.signature(outer_fn).parameters) == ["x", "y"]
    assert (outer_fn.__module__, outer_fn.__qualname__) == (__name__, outer.__qualname__)


@pytest.mark.skipif(sys.version_info < (3, 14), reason="3.12 evaluates annotations at definition")
def test_a_real_trigger_annotated_with_a_name_never_defined_runs_and_logs_its_types(load_sdk, obs):
    """The real SDK leaves a trigger's annotations unevaluated (`capture._signature`), so a name
    imported only for type checkers never raises into the agent; nor does the log of its types.
    This module's annotations are strings (`from __future__`), so the trigger is compiled
    without its flags."""
    sdk = load_sdk(fake_sdk(active=True))
    namespace: dict[str, Any] = {"__name__": __name__}
    source = "def handle(ticket: OnlyForTypeCheckers) -> str:\n    return 'ok'\n"
    exec(compile(source, "<agent>", "exec", dont_inherit=True), namespace)
    handle = sdk.trigger(namespace["handle"])
    assert handle("T1") == "ok"
    assert obs("run.start") == [
        {
            "event": "run.start",
            "name": f"{__name__}:handle",
            "run": "real000000000001",
            "called_with": {"ticket": "str"},
        }
    ]


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

    with sdk.run("batch", inputs={"date": "2026-09-29"}):
        sync_id = sdk.current_run_id()

    @sdk.trigger(name="handle_async")
    async def handle() -> str | None:
        return sdk.current_run_id()

    async def both() -> list[str | None]:
        async with sdk.run("stream"):
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

    with sdk.run("batch"):
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
    """`irimi.sdk`'s trigger keeps a `staticmethod` or `classmethod` one above which it is written.
    The stand-in's own wrapper, put between, must too: it called the descriptor itself, so
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
    assert [e["name"] for e in obs("run.start")] == [f"{__name__}:{static}", "by_class"] * 2
    if real:
        assert ids == fake.minted


@pytest.mark.parametrize("real", [True, False], ids=["real", "absent"])
def test_a_tool_above_a_static_or_class_method_keeps_it_one(load_sdk, obs, monkeypatch, real):
    """`irimi.sdk`'s tool keeps a `staticmethod` or `classmethod` it is written above. The
    stand-in's logging wrapper goes inside the descriptor, so a static method is not handed the
    instance, a class method's stand-in is handed the class, and the real tool is given the
    descriptor itself, with the agent's own `name` (None), so the name it records is its own
    default and equals the one this module logs. W3's `ledger_worker` runs it end to end."""
    import irimi.sdk as real_sdk
    from irimi.sdk import runs

    posted: list[tuple[str, str, str]] = []

    class Collect:
        def post(self, run_id: str, action: str, doc: dict[str, Any]) -> None:
            posted.append((action, doc["name"], doc["ran"]))

    monkeypatch.setattr(runs, "reporter", Collect())
    sdk = load_sdk(real_sdk if real else None)
    monkeypatch.setenv(sdk.ENGINE_ACTIVE_ENV, "1")

    def journal_stand_in(cls: type, charge: str) -> str:
        return f"stood in for {cls.__name__}:{charge}"

    class Ledger:
        @sdk.read_tool
        @staticmethod
        def fee(amount: int) -> int:
            return amount // 10

        @sdk.write_tool(fake=journal_stand_in)
        @classmethod
        def journal(cls, charge: str) -> str:
            raise AssertionError("the real write ran")

    assert isinstance(Ledger.__dict__["fee"], staticmethod)
    assert isinstance(Ledger.__dict__["journal"], classmethod)
    for owner in (Ledger, Ledger()):
        assert owner.fee(100) == 10
        assert owner.journal("ch_1") == "stood in for Ledger:ch_1"
    logged = [(e["name"], e["ran"]) for e in obs("tool")]
    names = [f"{__name__}:{Ledger.__qualname__}.{fn}" for fn in ("fee", "journal")]
    assert logged == [(names[0], "real"), (names[1], "shadow")] * 2
    assert posted == ([("tool-calls", name, ran) for name, ran in logged] if real else [])


@pytest.mark.parametrize("real", [True, False], ids=["real", "absent"])
def test_a_stand_in_reaches_the_real_tool_as_the_agent_wrote_it(load_sdk, obs, monkeypatch, real):
    """The real `tool` judges a `staticmethod` stand-in by the function inside, and refuses a
    stand-in that is the function itself. This module's logging wrapper must not hide
    either: wrapped as a plain callable, an async static stand-in, or an object whose `__call__`
    is async, read as sync and was refused, a generator one passed, and the function and its
    stand-in became two wrappers, so the real write ran under irimi, logged as the stand-in. The
    fallback judges them as the real `tool` does."""
    import irimi.sdk as real_sdk
    from irimi.sdk import runs

    monkeypatch.setattr(runs, "reporter", types.SimpleNamespace(post=lambda *a: None))
    sdk = load_sdk(real_sdk if real else None)
    monkeypatch.setenv(sdk.ENGINE_ACTIVE_ENV, "1")

    class Repository:
        @staticmethod
        async def _save_stand_in(row: int) -> str:
            return f"stood in for {row}"

        @sdk.write_tool(fake=_save_stand_in, name="repo.save")
        @staticmethod
        async def save(row: int) -> str:
            raise AssertionError("the real write ran")

    class Mailer:
        """A stand-in object whose `__call__` is async, which the real `tool` takes as async."""

        async def __call__(self, row: int) -> str:
            return f"mailed {row}"

    @sdk.write_tool(fake=Mailer(), name="repo.mail")
    async def mail(row: int) -> str:
        raise AssertionError("the real write ran")

    assert asyncio.run(Repository.save(1)) == "stood in for 1"
    assert asyncio.run(mail(2)) == "mailed 2"
    assert [(e["name"], e["ran"]) for e in obs("tool")] == [
        ("repo.save", "shadow"),
        ("repo.mail", "shadow"),
    ]

    def write(row: int) -> str:
        raise AssertionError("the real write ran")

    def generator(row: int) -> Any:
        yield row

    with pytest.raises(TypeError):
        sdk.write_tool(fake=write)(write)
    with pytest.raises(TypeError):
        sdk.write_tool(fake=staticmethod(write))(staticmethod(write))
    for stand_in in (generator, staticmethod(generator)):
        with pytest.raises(TypeError):
            sdk.write_tool(fake=stand_in)(write)


# -- (c) the real tool's decoration-time checks decide -------------------------------------------


def test_w6_decoration_errors_are_the_real_tools_own(load_sdk, fresh_import, obs):
    fake = fake_sdk(active=True, with_tool=True)
    load_sdk(fake)
    w6 = fresh_import(W6)
    fake.decorated.clear()

    raised = w6.decoration_errors()

    assert len(raised) == 9
    # The three the signature refuses are Python's own, as the real decorators' signatures are.
    by_signature = {"write_without_fake", "read_with_fake", "bare_write_tool"}
    messages = [e["message"] for e in obs("decoration_error") if e["case"] not in by_signature]
    assert len(messages) == 6 and all(m.startswith(REAL_TYPE_ERROR) for m in messages), messages
    # What the real `tool` checks reached it as the agent wrote it.
    seen = {(kind, shadow if not callable(shadow) else "fn", fn.__name__) for _, kind, shadow, fn
            in fake.decorated}  # fmt: skip
    assert ("write", "mock_database_write", "real") in seen
    assert any(inspect.isgeneratorfunction(fn) for *_, fn in fake.decorated)


def test_the_stand_in_does_not_pre_empt_a_real_tool_that_accepts(load_sdk, obs):
    fake = fake_sdk(active=True, with_tool=True)

    def lenient(*, fake: Any, name: str | None = None) -> Any:
        return lambda fn: fn

    fake.write_tool = lenient  # type: ignore[attr-defined]
    sdk = load_sdk(fake)

    def real(x: int) -> int:
        return x

    # The stand-in alone would refuse a fake that is the function itself.
    assert sdk.write_tool(fake=real)(real)(3) == 3


def test_a_real_write_tool_logs_which_function_ran(load_sdk, obs):
    sdk = load_sdk(fake_sdk(active=True, with_tool=True))

    def stand_in(x: int) -> str:
        return "stood in"

    @sdk.write_tool(fake=stand_in, name="t.write")
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
    assert [(e["name"], e["run"]) for e in obs("run.start")] == [
        (f"{__name__}:{work.__qualname__}", run_id)
    ]


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
    from examples.workflows.sdk import ReplayResult as imported

    assert imported is ReplayResult
    with pytest.raises(AttributeError):
        _ = sdk._private
    with pytest.raises(AttributeError):
        _ = sdk.no_such_name


def test_without_the_real_sdk_unknown_names_are_attribute_errors(load_sdk):
    sdk = load_sdk(None)
    with pytest.raises(AttributeError, match="replay"):
        _ = sdk.replay


# -- (f) a read tool a replay answers from its recording -----------------------------------------

REPLAY_RUN = "abcd1234abcd1234"


class _Recording:
    """A reporter, as `runs.reporter` is, over a recording that answers each lookup with
    `outcome` (None: a miss), keeping each lookup's tool name and each posted call's `ran`.
    `gate`, when given, holds every lookup until it is set."""

    def __init__(self, outcome: Any = None, gate: threading.Event | None = None) -> None:
        self.outcome = outcome
        self.gate = gate
        self.asked: list[str] = []
        self.posted: list[tuple[str, str]] = []

    def post(self, run_id: str, action: str, doc: dict[str, Any]) -> None:
        self.posted.append((doc["name"], doc["ran"]))

    def recorded(self, run_id: str, name: str, args: Any) -> Any:
        self.asked.append(name)
        if self.gate is not None:
            self.gate.wait(5)
        return self.outcome


@pytest.fixture
def replaying(load_sdk, monkeypatch) -> Callable[..., tuple[types.ModuleType, _Recording]]:
    """`replaying(outcome, active=, gate=)`: this module over the real `irimi.sdk`, as a replay's
    child has it: active, told it replays (`IRIMI_REPLAY_RUN`, naming its `IRIMI_RUN`), and its
    reporter a `_Recording`."""
    import irimi.sdk as real_sdk
    from irimi.sdk import runs

    def get(
        outcome: Any = None, *, active: bool = True, gate: threading.Event | None = None
    ) -> tuple[types.ModuleType, _Recording]:
        recording = _Recording(outcome, gate)
        monkeypatch.setattr(runs, "reporter", recording)
        sdk = load_sdk(real_sdk)
        monkeypatch.setenv(sdk.ENGINE_ACTIVE_ENV, "1" if active else "0")
        monkeypatch.setenv(RUN_ENV, REPLAY_RUN)
        monkeypatch.setenv(REPLAY_RUN_ENV, REPLAY_RUN)
        return sdk, recording

    return get


def _outcome(result: Any = None, error: tuple[str, str] | None = None, awaited: bool = False):
    from irimi.trace import ErrorInfo

    return RecordedOutcome(result, None if error is None else ErrorInfo(*error), awaited)


def _called(sdk: types.ModuleType, form: str) -> Any:
    """A read tool `t.read` in `form`, whose real body fails the test, called as an agent calls
    it: what the call gave the agent."""

    def real(account: str) -> Any:
        raise AssertionError("the real read ran")

    async def real_async(account: str) -> Any:
        raise AssertionError("the real read ran")

    if form == "sync":
        return sdk.read_tool(name="t.read")(real)("acc_1")
    if form == "async":
        return asyncio.run(sdk.read_tool(name="t.read")(real_async)("acc_1"))
    if form == "returns_coroutine":
        # A sync read that returns its work as a coroutine (W3's `ledger_history`): the recording
        # says it was awaited, so the answer is a coroutine too.
        returned = sdk.read_tool(name="t.read")(real)("acc_1")
        assert inspect.iscoroutine(returned)
        return asyncio.run(returned)

    class Ledger:
        @sdk.read_tool(name="t.read")
        @staticmethod
        def static(account: str) -> Any:
            raise AssertionError("the real read ran")

        @sdk.read_tool(name="t.read")
        @classmethod
        def by_class(cls, account: str) -> Any:
            raise AssertionError("the real read ran")

    return getattr(Ledger(), form)("acc_1")


FORMS = ("sync", "async", "returns_coroutine", "static", "by_class")


@pytest.mark.parametrize("form", FORMS)
def test_a_read_tool_a_replay_answers_logs_recording_and_runs_neither_body(replaying, obs, form):
    """Neither the function nor a stand-in runs, so neither logs; the outer wrapper logs
    `ran="recording"`, once, as irimi records the call. Every form a tool takes in the corpus."""
    sdk, recording = replaying(_outcome({"balance": 7}, awaited=form == "returns_coroutine"))
    assert _called(sdk, form) == {"balance": 7}
    assert recording.asked == ["t.read"]
    assert (
        [(e["name"], e["ran"]) for e in obs("tool")]
        == recording.posted
        == [("t.read", "recording")]
    )


@pytest.mark.parametrize("form", FORMS)
def test_a_recorded_error_raised_again_logs_recording(replaying, obs, form):
    """A recorded call that raised raises `sdk.RecordedToolError` again, catchable as the recorded
    builtin, and was answered from the recording all the same (W6 `tool_raises`)."""
    sdk, recording = replaying(_outcome(error=("builtins.KeyError", "'acc_1'")))
    with pytest.raises(KeyError) as caught:
        _called(sdk, form)
    assert isinstance(caught.value, sdk.RecordedToolError)
    assert (
        [(e["name"], e["ran"]) for e in obs("tool")]
        == recording.posted
        == [("t.read", "recording")]
    )


def test_a_miss_runs_the_real_read_and_logs_it_once(replaying, obs):
    sdk, recording = replaying(None)

    @sdk.read_tool(name="t.read")
    def read(account: str) -> str:
        return f"live {account}"

    assert read("acc_1") == "live acc_1"
    assert recording.asked == ["t.read"]
    assert [(e["name"], e["ran"]) for e in obs("tool")] == recording.posted == [("t.read", "real")]


def test_a_write_in_a_replay_runs_its_stand_in_and_asks_no_recording(replaying, obs):
    """Even one the recording holds: a write tool never asks."""
    sdk, recording = replaying(_outcome({"written": True}))

    def stand_in(row: int) -> str:
        return "stood in"

    @sdk.write_tool(fake=stand_in, name="t.write")
    def write(row: int) -> str:
        raise AssertionError("the real write ran")

    assert write(1) == "stood in"
    assert recording.asked == []
    assert (
        [(e["name"], e["ran"]) for e in obs("tool")] == recording.posted == [("t.write", "shadow")]
    )


def test_an_inactive_sdk_told_it_replays_asks_nothing_and_logs_the_real_read(replaying, obs):
    """`IRIMI_REPLAY_RUN` with no irimi in front, as a bare agent run from a shell that exported
    it has it: the SDK is inert, so the real read runs, nothing is asked or posted, and the log
    says `real` and never `recording`."""
    sdk, recording = replaying(_outcome({"balance": 7}), active=False)

    @sdk.read_tool(name="t.read")
    def read(account: str) -> str:
        return f"live {account}"

    assert read("acc_1") == "live acc_1"
    assert recording.asked == recording.posted == []
    assert [(e["name"], e["ran"]) for e in obs("tool")] == [("t.read", "real")]


def test_a_read_cancelled_while_it_asks_the_recording_logs_nothing(replaying, obs):
    """Cancelled while its lookup waits off the loop, the call ran no body and was answered by
    nothing, and irimi stores no call of it: the log says so too, not `recording`."""
    gate = threading.Event()
    sdk, recording = replaying(_outcome({"balance": 7}), gate=gate)

    @sdk.read_tool(name="t.read")
    async def read(account: str) -> Any:
        raise AssertionError("the real read ran")

    async def agent() -> str:
        try:
            await asyncio.wait_for(read("acc_1"), 0.05)
        except TimeoutError:
            return "timed out"
        finally:
            gate.set()
        return "answered"

    assert asyncio.run(agent()) == "timed out"
    assert recording.asked == ["t.read"]
    assert recording.posted == obs("tool") == []


# What `_Kept` drops of a posted call: its id and its clock, new at every call.
_UNKEPT = ("tool_call_id", "started_at", "ended_at")


class _Kept(_Recording):
    """A `_Recording` that keeps each posted call whole, but for its id and its clock."""

    def __init__(self, outcome: Any = None) -> None:
        super().__init__(outcome)
        self.docs: list[dict[str, Any]] = []

    def post(self, run_id: str, action: str, doc: dict[str, Any]) -> None:
        super().post(run_id, action, doc)
        self.docs.append({k: v for k, v in doc.items() if k not in _UNKEPT})


def _through(sdk: Any, kind: str, form: str) -> tuple[Any, list[str]]:
    """A tool `t.tool` of `kind` in `form`, made with `sdk`'s tool decorators, called as an agent
    calls it: what the call gave the agent (returned, or raised: its name, its message and the two
    classes an agent catches it as), and which bodies ran."""
    from irimi.sdk import RecordedToolError

    ran: list[str] = []

    async def value(v: Any) -> Any:
        return v

    def real(account: str) -> Any:
        ran.append("real")
        return value({"real": account}) if form == "returns_coroutine" else {"real": account}

    def stand_in(account: str) -> Any:
        ran.append("shadow")
        return value({"shadow": account}) if form == "returns_coroutine" else {"shadow": account}

    async def real_async(account: str) -> Any:
        ran.append("real")
        return {"real": account}

    async def stand_in_async(account: str) -> Any:
        ran.append("shadow")
        return {"shadow": account}

    def made(fn: Any, fake: Any) -> Any:
        if kind == "write":
            return sdk.write_tool(fake=fake, name="t.tool")(fn)
        return sdk.read_tool(name="t.tool")(fn)

    class Ledger:
        static = made(staticmethod(real), staticmethod(stand_in))
        by_class = made(classmethod(lambda cls, account: real(account)), lambda cls, a: stand_in(a))

    try:
        if form == "async":
            got = asyncio.run(made(real_async, stand_in_async)("acc_1"))
        elif form in ("sync", "returns_coroutine"):
            got = made(real, stand_in)("acc_1")
            if form == "returns_coroutine":
                assert inspect.iscoroutine(got)
                got = asyncio.run(got)
        else:
            got = getattr(Ledger, form)("acc_1")
    except Exception as exc:
        caught = (isinstance(exc, KeyError), isinstance(exc, RecordedToolError))
        return ("raised", type(exc).__name__, str(exc), *caught), ran
    return ("returned", got), ran


@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize("kind", ("read", "write"))
@pytest.mark.parametrize("mode", ("bare", "shadow", "miss", "hit", "raised"))
def test_a_tool_through_this_module_does_what_the_real_sdk_s_does(
    replaying, monkeypatch, form, kind, mode
):
    """The agents write `sdk.read_tool` and `sdk.write_tool` from this module, and the corpus
    must test the SDK that ships: in every mode a tool runs in - bare, shadow, and a replay whose
    recording misses, answers or raises - the same tool made with this module's decorators and
    with `irimi.sdk`'s gives the agent the same thing, runs the same bodies and posts the same
    records."""
    import irimi.sdk as real_sdk
    from irimi.sdk import runs

    outcome = {
        "hit": _outcome({"balance": 7}, awaited=form == "returns_coroutine"),
        "raised": _outcome(error=("builtins.KeyError", "'acc_1'")),
    }.get(mode)
    sdk, _ = replaying(outcome, active=mode != "bare")
    if mode == "shadow":
        monkeypatch.delenv(REPLAY_RUN_ENV)
    seen = []
    for module in (sdk, real_sdk):
        kept = _Kept(outcome)
        monkeypatch.setattr(runs, "reporter", kept)
        got, ran = _through(module, kind, form)
        seen.append((got, ran, kept.asked, kept.docs))
    shim, real = seen
    assert shim == real
    if mode == "bare":
        assert real[1] == ["real"] and real[3] == []
    elif kind == "write" or mode in ("shadow", "miss"):
        assert real[1] == ["shadow" if kind == "write" else "real"]
    else:
        assert real[1] == [] and [d["ran"] for d in real[3]] == ["recording"]


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


def test_resolving_only_serves_the_names_it_is_given_whatever_their_case():
    """`resolving_only`, the fake internet's resolver taken out of it for a test whose irimi
    reaches loopback stubs by their real names (`tests/test_phase3_replay_exit.py`): a name
    given in any case resolves to loopback in any case and with a trailing dot, every other name is
    refused, and the socket module is restored even when the block raises."""
    originals = (socket.getaddrinfo, socket.gethostbyname, socket.gethostbyname_ex)
    with pytest.raises(RuntimeError, match="block raised"):
        with resolving_only(["Stub.Example"]):
            assert socket.gethostbyname("stub.example.") == "127.0.0.1"
            assert socket.gethostbyname(b"STUB.EXAMPLE") == "127.0.0.1"
            assert {ai[4][0] for ai in socket.getaddrinfo("stub.example", 443)} == {"127.0.0.1"}
            with pytest.raises(socket.gaierror, match="not on the fake internet"):
                socket.gethostbyname("api.stripe.com")
            raise RuntimeError("block raised")
    assert (socket.getaddrinfo, socket.gethostbyname, socket.gethostbyname_ex) == originals
