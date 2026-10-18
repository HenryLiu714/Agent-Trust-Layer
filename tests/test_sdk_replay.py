"""The SDK under `irimi replay` (#84): a top-level run adopts the replay's run and posts nothing, a
trigger is called again from its entrypoint and captured args, and `sdk.replay`'s result codec.

`tests/test_replay_cli.py` drives all of it through the real command; these are its parts, each
alone: `RunScope`'s adopting branch against a reporter that writes down every post,
`capture.call_arguments` and `api.resolve`, `irimi.sdk.replay_child` as the subprocess it is, and
`ReplayResult`'s JSON.
"""

import asyncio
import json
import subprocess
import sys
from typing import Any

import pytest

from irimi import paths, sdk
from irimi.sdk import api, capture, runs
from irimi.sdk.replays import ReplayResult

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
    monkeypatch.setattr(runs, "reporter", reporter)
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "1")
    monkeypatch.setenv(paths.RUN_ENV, REPLAY)
    monkeypatch.setenv(paths.REPLAY_RUN_ENV, REPLAY)
    return reporter


@pytest.fixture
def instrumented(monkeypatch) -> list[int]:
    """Each call `runs` makes of `instrument()`, which an adopted run makes too (#75, #84)."""
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
    """Two triggers one after the other, and a block: one run, the replay's (#84)."""
    with sdk.run(trigger={"n": 1}, name="block"):
        in_block = sdk.current_run_id()
    assert [handle("T1"), handle("T2"), in_block] == [REPLAY] * 3
    assert posts.posts == []


def test_an_async_trigger_and_a_handed_off_coroutine_adopt_it_too(posts, instrumented):
    assert asyncio.run(handle_async("T1")) == REPLAY
    assert asyncio.run(hands_off("T1")) == REPLAY
    assert posts.posts == []


def test_a_tool_call_in_an_adopted_run_is_posted_to_the_replay_s_run(posts, instrumented):
    @sdk.tool(kind="write", shadow=lambda body: "stood in")
    def reply(body: str) -> str:
        raise AssertionError("never for real")

    @sdk.trigger
    def answer() -> str:
        return reply("hi")

    assert answer() == "stood in"
    assert posts.posts == [(REPLAY, "tool-calls")]


def test_outside_a_replay_a_trigger_starts_its_own_run(posts, instrumented, monkeypatch):
    monkeypatch.delenv(paths.REPLAY_RUN_ENV)
    run_id = handle("T1")
    assert run_id not in (None, REPLAY)
    assert posts.posts == [(run_id, "start"), (run_id, "end")]


def test_a_replay_id_irimi_did_not_set_adopts_nothing(posts, instrumented, monkeypatch):
    """A stale `IRIMI_REPLAY_RUN` left in a serve agent's environment, which names no `IRIMI_RUN`,
    or another, is not irimi's word: the trigger starts its own run, as it would (#84)."""
    for run_env in (None, "another0another0"):
        if run_env is None:
            monkeypatch.delenv(paths.RUN_ENV)
        else:
            monkeypatch.setenv(paths.RUN_ENV, run_env)
        posts.posts.clear()
        run_id = handle("T1")
        assert run_id not in (None, REPLAY)
        assert posts.posts == [(run_id, "start"), (run_id, "end")]


def test_an_inactive_sdk_adopts_nothing(posts, instrumented, monkeypatch):
    monkeypatch.setenv(paths.ENGINE_ACTIVE_ENV, "0")
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


@pytest.mark.parametrize(
    ("fn", "args", "kwargs"),
    [
        (_f, (1,), {"c": 5}),
        (_f, (1, 2, 3), {}),
        (_g, (1, 2, 3), {"k": 4, "z": 9}),
        (_h, (1,), {"y": 2}),
    ],
)
def test_call_arguments_undoes_capture_call_s_binding(fn, args, kwargs):
    """A parameter left to its default stays so, `*args` spread back out, `**kwargs` back in, and
    a positional-only parameter in its place: the call the run recorded, made again (#84)."""
    captured, replayable = capture.capture_call(fn, args, kwargs)
    assert replayable
    again, named = capture.call_arguments(fn, capture.revive(captured))
    assert fn(*again, **named) == fn(*args, **kwargs)


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


def test_resolve_finds_a_function_a_staticmethod_and_a_classmethod_by_name():
    assert api.resolve(f"{__name__}:handle") is handle
    assert api.resolve(f"{__name__}:Desk.static") is Desk.static
    assert api.resolve(f"{__name__}:Desk.by_class") == Desk.by_class


@pytest.mark.parametrize(
    ("entrypoint", "reason"),
    [
        (f"{__name__}:Desk.answer", "it is a method"),
        (f"{__name__}:nothing_here", f"{__name__} has no nothing_here"),
        ("no_such_module_84:handle", "importing no_such_module_84 raised ModuleNotFoundError"),
        (f"{__name__}:REPLAY", "REPLAY is not callable"),
        ("handle", "it is not module:qualname"),
    ],
)
def test_resolve_refuses_what_it_cannot_call(entrypoint, reason):
    with pytest.raises(api.NotCallable, match=reason):
        api.resolve(entrypoint)


CHILD_MODULE = """
import asyncio
from dataclasses import dataclass


@dataclass(frozen=True)
class Ticket:
    charge: str


def ok(ticket, *, note=""):
    assert isinstance(ticket, Ticket), ticket
    print("called", ticket.charge, note)


def fails(ticket):
    raise KeyError(ticket.charge)


async def waits(ticket):
    await asyncio.sleep(0)
    print("awaited", ticket.charge)
"""


def _child(tmp_path, entrypoint: str, args: dict) -> tuple[subprocess.CompletedProcess[str], Any]:
    """`replay_child` run as `irimi replay` runs it: what it did, and the outcome it wrote."""
    from irimi.sdk.replay_child import CallOutcome

    (tmp_path / "child_agent.py").write_text(CHILD_MODULE)
    args_file, outcome_file = tmp_path / "args.json", tmp_path / "outcome.json"
    args_file.write_text(json.dumps(args))
    env = {
        "PATH": "",
        "PYTHONPATH": str(tmp_path),
        paths.REPLAY_CALL_ENV: entrypoint,
        paths.REPLAY_ARGS_FILE_ENV: str(args_file),
        paths.REPLAY_OUTCOME_FILE_ENV: str(outcome_file),
    }
    done = subprocess.run(
        [sys.executable, "-m", "irimi.sdk.replay_child"],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    return done, CallOutcome.read(outcome_file)


TICKET = {"ticket": {"__irimi_type__": "child_agent:Ticket", "value": {"charge": "ch_1"}}}


def test_the_child_calls_the_trigger_with_its_revived_args_and_exits_0(tmp_path):
    from irimi.sdk.replay_child import CallOutcome

    done, outcome = _child(tmp_path, "child_agent:ok", {**TICKET, "note": "hi"})
    assert (done.returncode, done.stdout, done.stderr) == (0, "called ch_1 hi\n", "")
    assert outcome == CallOutcome(True, None)


def test_the_child_runs_a_coroutine_to_its_end(tmp_path):
    done, _ = _child(tmp_path, "child_agent:waits", TICKET)
    assert (done.returncode, done.stdout) == (0, "awaited ch_1\n")


def test_a_trigger_that_raises_exits_1_with_its_traceback_and_says_what_it_raised(tmp_path):
    """The outcome is what a run records an exception as (`errors.error_info`), so the replay's
    run ends on `builtins.KeyError` as its recording's did (#83, #84)."""
    from irimi.sdk.replay_child import CallOutcome
    from irimi.trace import ErrorInfo

    done, outcome = _child(tmp_path, "child_agent:fails", TICKET)
    assert done.returncode == 1
    assert done.stderr.startswith("Traceback (most recent call last):")
    assert done.stderr.endswith("KeyError: 'ch_1'\n")
    assert outcome == CallOutcome(True, ErrorInfo("builtins.KeyError", "'ch_1'"))


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
    ],
)
def test_a_call_the_child_cannot_make_exits_cannot_call_and_says_why(
    tmp_path, entrypoint, args, error
):
    """Never an agent that ran and raised: the child exits `CANNOT_CALL`, with one `error:` line,
    and its outcome says it never called (#84)."""
    from irimi.sdk.replay_child import CANNOT_CALL, CallOutcome
    from irimi.trace import ErrorInfo

    done, outcome = _child(tmp_path, entrypoint, args)
    assert (done.returncode, done.stderr) == (
        CANNOT_CALL,
        f"error: cannot call {entrypoint}: {error}\n",
    )
    assert outcome == CallOutcome(False, ErrorInfo("cannot-call", error))


@pytest.mark.parametrize(
    "written",
    [
        "",
        "{",
        "[]",
        '{"called": 1, "error": null}',
        '{"called": true}',
        '{"called": true, "error": {"type": 1, "message": "m"}}',
    ],
)
def test_an_outcome_the_child_did_not_write_reads_as_none(tmp_path, written):
    from irimi.sdk.replay_child import CallOutcome

    path = tmp_path / "outcome.json"
    assert CallOutcome.read(path) is None
    path.write_text(written)
    assert CallOutcome.read(path) is None


# -- the result's codec ---------------------------------------------------------------------------


def test_a_replay_result_round_trips_through_its_json():
    result = ReplayResult("src", "new", 1, "#1: live llm ...", ("a", "b"))
    assert ReplayResult.from_json(json.loads(json.dumps(result.to_json()))) == result
    refused = ReplayResult("src", None, 2, None, ("error: no run src in /s",))
    assert ReplayResult.from_json(refused.to_json()) == refused


@pytest.mark.parametrize(
    "broken",
    [
        {"source_run_id": "s"},
        {**ReplayResult("s", "n", 0, None, ()).to_json(), "extra": 1},
        {**ReplayResult("s", "n", 0, None, ()).to_json(), "exit_code": True},
        {**ReplayResult("s", "n", 0, None, ()).to_json(), "lines": "one"},
        {**ReplayResult("s", "n", 0, None, ()).to_json(), "replay_run_id": 3},
    ],
)
def test_a_result_it_did_not_write_is_refused(broken):
    with pytest.raises(ValueError):
        ReplayResult.from_json(broken)


def test_sdk_replay_refuses_a_function_from_main_or_one_its_name_does_not_find():
    def local(ticket: str) -> str:
        return ticket

    with pytest.raises(ValueError, match="cannot be called by its name"):
        sdk.replay("r", local, "T1")
    with pytest.raises(ValueError, match="arguments only for a function"):
        sdk.replay("r", None, "T1")
    with pytest.raises(ValueError, match="do not capture replayably"):
        sdk.replay("r", handle, object())
    with pytest.raises(ValueError, match="it is a method"):
        sdk.replay("r", Desk.answer, "T1")


def test_a_replay_s_child_is_told_which_trigger_to_call_and_no_other_child_is(tmp_path):
    """`runner.child_env(..., call=)` names the trigger and its args file for `replay_child`, and
    drops both from every other child, as `IRIMI_REPLAY_RUN` is dropped (#83, #84)."""
    from irimi import runner

    stale = {
        paths.REPLAY_CALL_ENV: "m:f",
        paths.REPLAY_ARGS_FILE_ENV: "/stale.json",
        paths.REPLAY_OUTCOME_FILE_ENV: "/stale-outcome.json",
    }
    plain = runner.child_env(stale, "127.0.0.1", 4000, tmp_path / "ca.pem", REPLAY)
    assert not set(stale) & set(plain)
    call = runner.ChildCall("agent:handle", tmp_path / "args.json", tmp_path / "outcome.json")
    child = runner.child_env(
        stale, "127.0.0.1", 4000, tmp_path / "ca.pem", REPLAY, replaying=True, call=call
    )
    named = (paths.REPLAY_CALL_ENV, paths.REPLAY_ARGS_FILE_ENV, paths.REPLAY_OUTCOME_FILE_ENV)
    assert [child[name] for name in named] == [
        "agent:handle",
        str(tmp_path / "args.json"),
        str(tmp_path / "outcome.json"),
    ]
    assert child[paths.REPLAY_RUN_ENV] == child[paths.RUN_ENV] == REPLAY
    assert stale[paths.REPLAY_CALL_ENV] == "m:f"
