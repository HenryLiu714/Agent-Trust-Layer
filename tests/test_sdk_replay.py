"""The SDK under `irimi replay`: a top-level run adopts the replay's run and posts nothing, a
trigger is called again from its entrypoint and captured args, and `testing.replay`'s result codec.

`tests/test_replay_cli.py` drives all of it through the real command; these are its parts, each
alone: `RunScope`'s adopting branch against a reporter that writes down every post,
`calls.call_arguments` and `entrypoints.resolve`, `irimi.replay.child` as the subprocess it
is, and `ReplayResult`'s JSON.
"""

import asyncio
import json
import os
import signal
import stat
import subprocess
import sys
import threading
import types
from typing import Any

import pytest

from irimi import sdk, testing
from irimi.core import calls
from irimi.protocol import entrypoints
from irimi.protocol.env import (
    ENGINE_ACTIVE_ENV,
    REPLAY_ARGS_FILE_ENV,
    REPLAY_CALL_ENV,
    REPLAY_OUTCOME_FILE_ENV,
    REPLAY_RUN_ENV,
    RUN_ENV,
)
from irimi.protocol.replay_files import (
    CallResult,
    ReplayResult,
    read_call_outcome,
    replay_result_from_json,
    replay_result_to_json,
)
from irimi.protocol.trace import ErrorInfo
from irimi.sdk import _capture as capture
from irimi.sdk import _runs as runs
from irimi.sdk._state import state

REPLAY = "abcd1234abcd1234"


class _Posts:
    """A `Reporter` that writes down each post, and answers no recorded tool call."""

    def __init__(self) -> None:
        self.posts: list[tuple[str, str]] = []

    def post(self, run_id: str, kind: str, payload: Any) -> None:
        self.posts.append((run_id, kind))

    def recorded(self, run_id: str, name: str, args: Any) -> None:
        return None


@pytest.fixture
def posts(monkeypatch) -> _Posts:
    """This process as `irimi replay` sets up its child: active, and told the run it replays as."""
    reporter = _Posts()
    monkeypatch.setattr(state, "reporter", reporter)
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "1")
    monkeypatch.setenv(RUN_ENV, REPLAY)
    monkeypatch.setenv(REPLAY_RUN_ENV, REPLAY)
    return reporter


@pytest.fixture
def instrumented(monkeypatch) -> list[int]:
    """Each call `runs` makes of `instrument()`, which an adopted run makes too."""
    calls: list[int] = []
    monkeypatch.setattr(runs, "instrument", lambda: calls.append(1))
    return calls


@sdk.trigger
def handle(ticket: str) -> str | None:
    return sdk.current_run_id()


@sdk.trigger
def outer(ticket: str) -> tuple[str | None, str | None]:
    return sdk.current_run_id(), handle(ticket)


@sdk.trigger
async def handle_async(ticket: str) -> str | None:
    await asyncio.sleep(0)
    return sdk.current_run_id()


@sdk.trigger
def hands_off(ticket: str) -> Any:
    return handle_async.__wrapped__(ticket)  # type: ignore[attr-defined]


def test_a_top_level_trigger_adopts_the_replay_s_run_and_posts_nothing(posts, instrumented):
    assert handle("T1") == REPLAY
    assert sdk.current_run_id() is None
    assert posts.posts == []
    assert instrumented == [1]


def test_a_nested_trigger_joins_the_adopted_run(posts, instrumented):
    assert outer("T1") == (REPLAY, REPLAY)
    assert posts.posts == []


def test_every_top_level_run_of_the_process_adopts_the_one_replay_run(posts, instrumented):
    """Two triggers one after the other, and a block: one run, the replay's."""
    with sdk.run("block", inputs={"n": 1}):
        in_block = sdk.current_run_id()
    assert [handle("T1"), handle("T2"), in_block] == [REPLAY] * 3
    assert posts.posts == []


def test_an_async_trigger_and_a_handed_off_coroutine_adopt_it_too(posts, instrumented):
    assert asyncio.run(handle_async("T1")) == REPLAY
    assert asyncio.run(hands_off("T1")) == REPLAY
    assert posts.posts == []


def test_the_adopted_run_is_current_when_it_is_instrumented(posts, monkeypatch):
    """The patch reads the current run as each request is sent, so the run is current before
    `instrument()` runs, and so before the run's first request."""
    seen: list[str | None] = []
    monkeypatch.setattr(runs, "instrument", lambda: seen.append(sdk.current_run_id()))
    handle("T1")
    asyncio.run(handle_async("T2"))
    assert seen == [REPLAY, REPLAY]


@sdk.trigger
def hands_to_workers(ticket: str) -> list[str | None]:
    """The run's id as a thread handed the run (`sdk.propagate`), an asyncio task, and a thread
    that was not handed it but calls a trigger of its own see it."""
    seen: list[str | None] = []
    for target in (
        sdk.propagate(lambda: seen.append(sdk.current_run_id())),
        lambda: seen.append(handle(ticket)),
    ):
        worker = threading.Thread(target=target)
        worker.start()
        worker.join()

    async def in_a_task() -> str | None:
        return await asyncio.create_task(handle_async(ticket))

    seen.append(asyncio.run(in_a_task()))
    return seen


def test_threads_and_tasks_of_an_adopted_run_stay_in_it_and_a_thread_s_own_trigger_adopts_it(
    posts, instrumented
):
    assert hands_to_workers("T1") == [REPLAY, REPLAY, REPLAY]
    assert posts.posts == []


@types.coroutine
def _suspend() -> Any:
    yield


@sdk.trigger
async def suspends(ticket: str) -> None:
    await _suspend()


def test_an_adopted_async_run_closed_unawaited_posts_nothing_and_leaves_the_run(
    posts, instrumented
):
    """`GeneratorExit` is never awaited through, and an adopted run has no end to post."""
    coroutine = suspends("T1")
    coroutine.send(None)
    coroutine.close()
    assert posts.posts == []
    assert sdk.current_run_id() is None


def test_a_tool_call_in_an_adopted_run_is_posted_to_the_replay_s_run(posts, instrumented):
    @sdk.write_tool(fake=lambda body: "stood in")
    def reply(body: str) -> str:
        raise AssertionError("never for real")

    @sdk.trigger
    def answer() -> str:
        return reply("hi")

    assert answer() == "stood in"
    assert posts.posts == [(REPLAY, "tool-calls")]


def test_outside_a_replay_a_trigger_starts_its_own_run(posts, instrumented, monkeypatch):
    monkeypatch.delenv(REPLAY_RUN_ENV)
    run_id = handle("T1")
    assert run_id not in (None, REPLAY)
    assert posts.posts == [(run_id, "start"), (run_id, "end")]


def test_a_replay_id_irimi_did_not_set_adopts_nothing(posts, instrumented, monkeypatch):
    """A stale `IRIMI_REPLAY_RUN` left in a serve agent's environment, which names no `IRIMI_RUN`,
    or another, is not irimi's word: the trigger starts its own run, as it would."""
    for run_env in (None, "another0another0"):
        if run_env is None:
            monkeypatch.delenv(RUN_ENV)
        else:
            monkeypatch.setenv(RUN_ENV, run_env)
        posts.posts.clear()
        run_id = handle("T1")
        assert run_id not in (None, REPLAY)
        assert posts.posts == [(run_id, "start"), (run_id, "end")]


def test_an_inactive_sdk_adopts_nothing(posts, instrumented, monkeypatch):
    monkeypatch.setenv(ENGINE_ACTIVE_ENV, "0")
    assert handle("T1") is None
    assert posts.posts == []
    assert instrumented == []


# -- calling a trigger again ----------------------------------------------------------------------


def _f(a, b=2, c=3):
    return (a, b, c)


def _g(a, *rest, k=0, **kw):
    return (a, rest, k, kw)


def _h(x, /, y):
    return (x, y)


def _p(a, /, **kw):
    return (a, kw)


@pytest.mark.parametrize(
    ("fn", "args", "kwargs"),
    [
        (_f, (1,), {"c": 5}),
        (_f, (1, 2, 3), {}),
        (_g, (1, 2, 3), {"k": 4, "z": 9}),
        (_h, (1,), {"y": 2}),
        (_p, (1,), {"a": 2}),  # a positional-only parameter's name is free for `**kwargs`
        (_g, (1,), {"rest": 2, "kw": 3}),  # so are `*args`' and `**kwargs`' own
    ],
)
def test_call_arguments_undoes_capture_call_s_binding(fn, args, kwargs):
    """A parameter left to its default stays so, `*args` spread back out, `**kwargs` back in, and
    a positional-only parameter in its place: the call the run recorded, made again."""
    captured, replayable = capture.capture_call(fn, args, kwargs)
    assert replayable
    again, named = calls.call_arguments(fn, capture.revive(captured))
    assert fn(*again, **named) == fn(*args, **kwargs)


def _r(a, b=2, *rest):
    return (a, b, rest)


@pytest.mark.parametrize(
    ("fn", "arguments", "error"),
    [
        (_r, {"a": 1, "rest": [9]}, "rest takes its values after b, which was left to its default"),
        (_g, {"a": 1, "rest": "ab"}, "rest must be a list, not str"),
        (_g, {"a": 1, "kw": "ab"}, "kw must be an object, not str"),
        (_g, {"a": 1, "k": 1, "kw": {"k": 2}}, "kw names k, which is a parameter of its own"),
    ],
)
def test_call_arguments_refuses_args_that_would_bind_otherwise_than_they_say(fn, arguments, error):
    """Args given by hand (`--args`) can say what no call binds: `*args` with a positional
    parameter before it left out, which would fill that parameter instead; a `*args` that is no
    list or a `**kwargs` that is no object, which would be spread as characters or keys; and a
    `**kwargs` naming a parameter, which would replace that parameter's value."""
    with pytest.raises(TypeError) as raised:
        calls.call_arguments(fn, arguments)
    assert str(raised.value) == error


class Desk:
    @sdk.trigger
    def answer(self, ticket: str) -> str:
        return ticket

    @staticmethod
    @sdk.trigger
    def static(ticket: str) -> str:
        return ticket

    @classmethod
    @sdk.trigger
    def by_class(cls, ticket: str) -> str:
        return ticket

    @staticmethod
    @sdk.trigger
    def static_self(self: str, ticket: str) -> tuple[str, str]:
        return (self, ticket)


@sdk.trigger
def plain_cls(cls: str, ticket: str) -> tuple[str, str]:
    return (cls, ticket)


@pytest.mark.parametrize("fn", [Desk.static_self, plain_cls])
def test_a_trigger_receiving_no_instance_is_called_again_with_its_first_parameter(fn):
    """A static method's or a plain function's first parameter is an argument, even named `self`
    or `cls`: captured, it calls the trigger again."""
    captured, replayable = capture.capture_call(fn.__wrapped__, ("desk", "T1"), {})
    assert replayable
    trigger = entrypoints.resolve(entrypoints.entrypoint(fn))
    again, named = calls.call_arguments(trigger, capture.revive(captured))
    assert fn(*again, **named) == ("desk", "T1")


def test_resolve_finds_a_function_a_staticmethod_and_a_classmethod_by_name():
    assert entrypoints.resolve(f"{__name__}:handle") is handle
    assert entrypoints.resolve(f"{__name__}:Desk.static") is Desk.static
    assert entrypoints.resolve(f"{__name__}:Desk.by_class") == Desk.by_class


@pytest.mark.parametrize(
    ("entrypoint", "reason"),
    [
        (f"{__name__}:Desk.answer", "it is a method"),
        (f"{__name__}:nothing_here", f"{__name__} has no nothing_here"),
        ("no_such_module_84:handle", "importing no_such_module_84 raised ModuleNotFoundError"),
        (f"{__name__}:REPLAY", "REPLAY is not callable"),
        ("handle", "it is not module:qualname"),
        ("__main__:main", "it is in __main__, which in a replay's child is the child itself"),
    ],
)
def test_resolve_refuses_what_it_cannot_call(entrypoint, reason):
    with pytest.raises(entrypoints.NotCallable, match=reason):
        entrypoints.resolve(entrypoint)


CHILD_MODULE = """
import asyncio
from dataclasses import dataclass

from irimi import sdk, testing
from irimi.core import calls
from irimi.protocol import entrypoints


@dataclass(frozen=True)
class Ticket:
    charge: str


def ok(ticket, *, note=""):
    assert isinstance(ticket, Ticket), ticket
    print("called", ticket.charge, note)


def fails(ticket):
    raise KeyError(ticket.charge)


def interrupts(ticket):
    raise KeyboardInterrupt


def exits(ticket):
    raise SystemExit(3)


async def waits(ticket):
    await asyncio.sleep(0)
    print("awaited", ticket.charge)


def spreads(**more):
    print("spread", more)


def __getattr__(name):
    if name == "lazy":
        raise RuntimeError("the lazy import broke")
    raise AttributeError(name)


@sdk.trigger
def handled(ticket):
    return _later(ticket)


async def _later(ticket):
    await asyncio.sleep(0)
    print("later", ticket.charge, sdk.current_run_id())


@sdk.trigger
async def adopted(ticket):
    await asyncio.sleep(0)
    print("adopted", ticket.charge, sdk.current_run_id())
"""


def _child(
    tmp_path, entrypoint: str, args: dict, env: dict[str, str] | None = None
) -> tuple[subprocess.CompletedProcess[str], Any]:
    """`replay_child` run as `irimi replay` runs it, with `env` besides: what it did, and the
    outcome it wrote."""

    (tmp_path / "child_agent.py").write_text(CHILD_MODULE)
    (tmp_path / "interrupted_agent.py").write_text("raise KeyboardInterrupt\n")
    args_file, outcome_file = tmp_path / "args.json", tmp_path / "outcome.json"
    args_file.write_text(json.dumps(args))
    env = {
        "PATH": "",
        "PYTHONPATH": str(tmp_path),
        REPLAY_CALL_ENV: entrypoint,
        REPLAY_ARGS_FILE_ENV: str(args_file),
        REPLAY_OUTCOME_FILE_ENV: str(outcome_file),
        **(env or {}),
    }
    done = subprocess.run(
        [sys.executable, "-m", "irimi.replay.child"],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    return done, read_call_outcome(outcome_file)


TICKET = {"ticket": {"__irimi_type__": "child_agent:Ticket", "value": {"charge": "ch_1"}}}


def test_the_child_calls_the_trigger_with_its_revived_args_and_exits_0(tmp_path):

    done, outcome = _child(tmp_path, "child_agent:ok", {**TICKET, "note": "hi"})
    assert (done.returncode, done.stdout, done.stderr) == (0, "called ch_1 hi\n", "")
    assert outcome == CallResult(True, None)


def test_the_child_runs_a_coroutine_to_its_end(tmp_path):
    done, _ = _child(tmp_path, "child_agent:waits", TICKET)
    assert (done.returncode, done.stdout) == (0, "awaited ch_1\n")


@pytest.mark.parametrize(
    ("entrypoint", "printed"),
    [
        ("child_agent:handled", "later ch_1 abcd1234abcd1234\n"),
        ("child_agent:adopted", "adopted ch_1 abcd1234abcd1234\n"),
    ],
)
def test_the_child_s_trigger_adopts_the_replay_s_run_and_its_handed_off_coroutine_runs(
    tmp_path, entrypoint, printed
):
    """A sync trigger that returns a coroutine hands its run to it: the child runs that
    coroutine to its end, in the replay's run, as it runs an `async def` trigger's. Nothing
    is posted, so no control endpoint is needed."""

    replaying = {ENGINE_ACTIVE_ENV: "1", RUN_ENV: REPLAY, REPLAY_RUN_ENV: REPLAY}
    done, outcome = _child(tmp_path, entrypoint, TICKET, replaying)
    assert (done.returncode, done.stdout, done.stderr) == (0, printed, "")
    assert outcome == CallResult(True, None)


def test_a_trigger_that_raises_exits_1_with_its_traceback_and_says_what_it_raised(tmp_path):
    """The outcome is what a run records an exception as (`_errors.error_info`), so the replay's
    run ends on `builtins.KeyError` as its recording's did."""

    done, outcome = _child(tmp_path, "child_agent:fails", TICKET)
    assert done.returncode == 1
    assert done.stderr.startswith("Traceback (most recent call last):")
    assert done.stderr.endswith("KeyError: 'ch_1'\n")
    assert outcome == CallResult(True, ErrorInfo("builtins.KeyError", "'ch_1'"))


@pytest.mark.parametrize(
    ("entrypoint", "args", "error"),
    [
        ("child_agent:gone", TICKET, "child_agent has no gone"),
        (
            "child_agent:ok",
            {"ticket": {"__irimi_repr__": "<Ticket>"}},
            "its args do not revive: the value was recorded as its description, not as itself",
        ),
        (
            "child_agent:ok",
            {"ticket": {"__irimi_type__": "child_agent:NoSuchTicket", "value": {}}},
            "its args do not revive: child_agent:NoSuchTicket is not in its module",
        ),
        (
            "child_agent:lazy",
            {},
            "looking up lazy in child_agent raised RuntimeError: the lazy import broke",
        ),
        (
            "child_agent:spreads",
            {"more": "ab"},
            "its args do not bind: more must be an object, not str",
        ),
        ("__main__:main", {}, "it is in __main__, which in a replay's child is the child itself"),
    ],
)
def test_a_call_the_child_cannot_make_exits_cannot_call_and_says_why(
    tmp_path, entrypoint, args, error
):
    """Never an agent that ran and raised: the child exits `CANNOT_CALL`, with one `error:` line,
    and its outcome says it never called."""
    from irimi.replay.child import CANNOT_CALL

    done, outcome = _child(tmp_path, entrypoint, args)
    assert (done.returncode, done.stderr) == (
        CANNOT_CALL,
        f"error: cannot call {entrypoint}: {error}\n",
    )
    assert outcome == CallResult(False, ErrorInfo("cannot-call", error))


@pytest.mark.parametrize(
    ("entrypoint", "code", "error"),
    [
        ("child_agent:interrupts", -signal.SIGINT, ErrorInfo("builtins.KeyboardInterrupt", "")),
        ("child_agent:exits", 3, ErrorInfo("builtins.SystemExit", "3")),
    ],
)
def test_a_call_ended_by_an_interrupt_or_an_exit_says_so_and_ends_the_child_as_it_would(
    tmp_path, entrypoint, code, error
):
    """What a run records them as, and then the process ends as they would have ended it, never
    as a trigger that raised."""

    done, outcome = _child(tmp_path, entrypoint, TICKET)
    assert done.returncode == code
    assert outcome == CallResult(True, error)


def test_a_child_interrupted_before_its_call_says_it_never_called_and_stops_as_interrupted(
    tmp_path,
):
    """Whatever ends the child before the call, a KeyboardInterrupt as its module loads too, is a
    call never made, never an agent that ran; the interruption then ends the process."""

    done, outcome = _child(tmp_path, "interrupted_agent:main", {})
    assert done.returncode == -signal.SIGINT
    error = "KeyboardInterrupt before the call"
    assert done.stderr.startswith(f"error: cannot call interrupted_agent:main: {error}\n")
    assert outcome == CallResult(False, ErrorInfo("cannot-call", error))


@pytest.mark.parametrize(
    "written",
    [
        "",
        "{",
        "[]",
        '{"called": 1, "error": null}',
        '{"called": true}',
        '{"called": true, "error": {"type": 1, "message": "m"}}',
        # The agent can write this file too, its env names it: deeper than the stack.
        pytest.param("[" * 200000 + "]" * 200000, id="deeper-than-the-stack"),
        pytest.param(b"\xff\xfe not utf-8", id="not-utf-8"),
    ],
)
def test_an_outcome_the_child_did_not_write_reads_as_none(tmp_path, written):

    path = tmp_path / "outcome.json"
    assert read_call_outcome(path) is None
    if isinstance(written, bytes):
        path.write_bytes(written)
    else:
        path.write_text(written)
    assert read_call_outcome(path) is None


# -- the result's codec ---------------------------------------------------------------------------


def test_a_replay_result_round_trips_through_its_json():
    result = ReplayResult("src", "new", 1, "#1: live llm ...", ("a", "b"))
    assert replay_result_from_json(json.loads(json.dumps(replay_result_to_json(result)))) == result
    refused = ReplayResult("src", None, 2, None, ("error: no run src in /s",))
    assert replay_result_from_json(replay_result_to_json(refused)) == refused


def test_a_replay_result_says_whether_it_is_the_same_and_prints_what_irimi_said():
    assert ReplayResult("s", "n", 0, None, ("no divergence",)).same
    changed = ReplayResult("s", "n", 1, None, ("irimi replay · s -> n", "  ~ A ..."))
    assert not changed.same and not ReplayResult("s", None, 2, None, ()).same
    assert str(changed) == "irimi replay · s -> n\n  ~ A ..."


@pytest.mark.parametrize(
    "broken",
    [
        {"source_run_id": "s"},
        {**replay_result_to_json(ReplayResult("s", "n", 0, None, ())), "extra": 1},
        {**replay_result_to_json(ReplayResult("s", "n", 0, None, ())), "exit_code": True},
        {**replay_result_to_json(ReplayResult("s", "n", 0, None, ())), "lines": "one"},
        {**replay_result_to_json(ReplayResult("s", "n", 0, None, ())), "replay_run_id": 3},
    ],
)
def test_a_result_it_did_not_write_is_refused(broken):
    with pytest.raises(ValueError):
        replay_result_from_json(broken)


def test_sdk_replay_refuses_a_function_from_main_or_one_its_name_does_not_find(monkeypatch):
    def local(ticket: str) -> str:
        return ticket

    def in_main(ticket: str) -> str:
        return ticket

    in_main.__module__, in_main.__qualname__ = "__main__", "in_main"
    monkeypatch.setattr(sys.modules["__main__"], "in_main", in_main, raising=False)
    with pytest.raises(testing.ReplayError) as raised:
        testing.replay("r", in_main, args=("T1",))
    assert str(raised.value) == (
        "__main__:in_main cannot be called by its name: it is in __main__, which in a replay's "
        "child is the child itself"
    )

    with pytest.raises(testing.ReplayError, match="cannot be called by its name"):
        testing.replay("r", local, args=("T1",))

    def impostor(ticket: str) -> str:
        return ticket

    impostor.__qualname__ = "handle"  # its name finds `handle`, another function
    with pytest.raises(testing.ReplayError) as raised:
        testing.replay("r", impostor, args=("T1",))
    assert str(raised.value) == f"{__name__}:handle names another function than the one given"
    with pytest.raises(testing.ReplayError, match="args and kwargs only for a call it is given"):
        testing.replay("r", None, args=("T1",))
    with pytest.raises(testing.ReplayError, match="args and kwargs only for a call it is given"):
        testing.replay("r", kwargs={"ticket": "T1"})
    with pytest.raises(testing.ReplayError, match="it is a method"):
        testing.replay("r", Desk.answer, args=("T1",))


@pytest.mark.parametrize(
    ("args", "error"),
    [
        (("T1", "T2"), "the arguments do not bind to handle: too many positional arguments"),
        ((), "the arguments do not bind to handle: missing a required argument: 'ticket'"),
        ((object(),), "the argument ticket to handle does not capture replayably"),
    ],
)
def test_sdk_replay_says_why_the_arguments_it_was_given_cannot_be_replayed(args, error):
    with pytest.raises(testing.ReplayError) as raised:
        testing.replay("r", handle, args=args)
    assert str(raised.value) == error


@sdk.trigger
def pair(a: str, b: str) -> None: ...


def test_sdk_replay_refuses_arguments_each_of_which_fits_but_not_together():
    half = "x" * (capture.MAX_CAPTURED_BYTES // 2)
    with pytest.raises(testing.ReplayError) as raised:
        testing.replay("r", pair, args=(half,), kwargs={"b": half})
    assert str(raised.value) == (
        "the arguments to pair are larger than a run records (1048576 bytes)"
    )


def _answering(argv: list[str], seen: dict[str, Any]) -> int:
    """A stand-in for `irimi`, through `testing.replay`'s one seam: writes down what it was given
    and how the directory holding its files was, and writes a result."""
    tmp = os.path.dirname(argv[argv.index("--result-file") + 1])
    seen["argv"], seen["tmp"] = argv, tmp
    seen["mode"] = stat.S_IMODE(os.stat(tmp).st_mode)
    if "--args-file" in argv:
        seen["args"] = json.loads(open(argv[argv.index("--args-file") + 1]).read())
    run_id = next(word for word in argv if word.startswith("--run="))[len("--run=") :]
    result = ReplayResult(run_id, "new0new0new0new0", 1, None, ("a",))
    with open(argv[argv.index("--result-file") + 1], "w") as fh:
        json.dump(replay_result_to_json(result), fh)
    return 1


def test_sdk_replay_passes_its_call_in_a_private_directory_it_removes(monkeypatch):
    """The args in a file in a directory only this user can enter, never on the command line;
    the run id as `--run=`, so one starting with `-` is no flag; this process's `sys.path` as the
    child's; and the directory gone when `testing.replay` returns, raises, or finds no result."""
    seen: dict[str, Any] = {}

    def run_irimi(argv: list[str], env: Any) -> int:
        seen["pythonpath"] = env["PYTHONPATH"]
        return _answering(argv, seen)

    monkeypatch.setattr(testing, "_run_irimi", run_irimi)
    result = testing.replay("-r", handle, kwargs={"ticket": "T1"}, store="/s")
    assert result == ReplayResult("-r", "new0new0new0new0", 1, None, ("a",))
    tmp = seen["tmp"]
    assert seen["argv"] == [
        "replay",
        "--run=-r",
        "--port",
        "0",
        "--result-file",
        f"{tmp}/result.json",
        "--entrypoint",
        f"{__name__}:handle",
        "--args-file",
        f"{tmp}/args.json",
        "--store",
        "/s",
    ]
    assert (seen["mode"], seen["args"], seen["pythonpath"]) == (
        0o700,
        {"ticket": "T1"},
        os.pathsep.join(sys.path),
    )
    assert not os.path.exists(tmp)

    def interrupted(argv: list[str], env: Any) -> int:
        _answering(argv, seen)
        raise KeyboardInterrupt

    monkeypatch.setattr(testing, "_run_irimi", interrupted)
    with pytest.raises(KeyboardInterrupt):
        testing.replay("r")
    assert not os.path.exists(seen["tmp"])
    monkeypatch.setattr(
        testing, "_run_irimi", lambda argv, env: seen.update(tmp=os.path.dirname(argv[4])) or 3
    )
    with pytest.raises(testing.ReplayError, match="^irimi replay exited 3 and wrote no result: "):
        testing.replay("r")
    assert not os.path.exists(seen["tmp"])


def test_a_replay_s_child_is_told_which_trigger_to_call_and_no_other_child_is(tmp_path):
    """`runner.child_env(..., call=)` names the trigger and its args file for `replay_child`, and
    drops both from every other child, as `IRIMI_REPLAY_RUN` is dropped."""
    from irimi.cli import runner

    stale = {
        REPLAY_CALL_ENV: "m:f",
        REPLAY_ARGS_FILE_ENV: "/stale.json",
        REPLAY_OUTCOME_FILE_ENV: "/stale-outcome.json",
    }
    plain = runner.child_env(stale, "127.0.0.1", 4000, tmp_path / "ca.pem", REPLAY)
    assert not set(stale) & set(plain)
    call = runner.ChildCall("agent:handle", tmp_path / "args.json", tmp_path / "outcome.json")
    child = runner.child_env(
        stale, "127.0.0.1", 4000, tmp_path / "ca.pem", REPLAY, replaying=True, call=call
    )
    named = (REPLAY_CALL_ENV, REPLAY_ARGS_FILE_ENV, REPLAY_OUTCOME_FILE_ENV)
    assert [child[name] for name in named] == [
        "agent:handle",
        str(tmp_path / "args.json"),
        str(tmp_path / "outcome.json"),
    ]
    assert child[REPLAY_RUN_ENV] == child[RUN_ENV] == REPLAY
    assert stale[REPLAY_CALL_ENV] == "m:f"
