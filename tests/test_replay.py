"""`irimi.replay`, the pure part of `irimi replay`: what a replay runs for a stored run
(`plan`), why it refuses one (`not_replayable`), and what it says once it has run (`outcome`),
over records and events built by hand. `tests/test_replay_cli.py` runs the command itself."""

from dataclasses import replace

import pytest

from irimi import compare, redact, replay, runner
from irimi.exchange import Exchange, Request, Response
from irimi.sdk.replay_child import CallOutcome
from irimi.sdk.replays import ReplayResult
from irimi.store import StoredRun
from irimi.trace import ErrorInfo, RunRecord, ToolCall, Trigger

TICKET = {"ticket": {"__irimi_type__": "agent:Ticket", "value": {"charge": "ch_1"}}}
TRIGGER = Trigger("handle", "agent:handle", TICKET, True)


def _record(trigger: Trigger | None = TRIGGER, attribution: str = "sdk") -> RunRecord:
    record = runner.process_run("src", ["python", "agent.py"], None, 1.0)
    return replace(record, attribution=attribution, trigger=trigger)


def test_a_command_is_run_whatever_the_run_recorded():
    plan = replay.plan(_record(None, "header"), ["python", "x.py"], None, None)
    assert (plan.call, plan.argv, plan.trigger) == (None, ("python", "x.py"), None)


def test_a_trigger_with_an_entrypoint_is_called_with_its_recorded_args():
    plan = replay.plan(_record(), [], None, None)
    assert plan.call == replay.Call("agent:handle", TICKET)
    assert (plan.argv, plan.trigger) == ((), TRIGGER)


def test_call_and_args_replace_the_entrypoint_and_the_args_and_the_trigger_says_so():
    other = {"ticket": None}
    plan = replay.plan(_record(), [], "agent:other", other)
    assert plan.call == replay.Call("agent:other", other)
    assert plan.trigger == Trigger("other", "agent:other", other, True)
    # A run with no trigger at all is called by `--call`'s name.
    bare = replay.plan(_record(None, "header"), [], "agent:other", other)
    assert bare.trigger == Trigger("other", "agent:other", other, True)


def test_a_call_of_another_function_names_the_trigger_after_it_as_the_sdk_names_one():
    """The replay's run names the function it called, by its qualname, as `@sdk.trigger` names a
    run, never the recorded trigger that did not run. The recorded entrypoint called again
    keeps the recorded name, which `@sdk.trigger(name=...)` may have given."""
    named = _record(replace(TRIGGER, name="refund"))
    plan = replay.plan(named, [], "agent:Desk.lookup", {"customer": "c"})
    assert plan.trigger == Trigger("Desk.lookup", "agent:Desk.lookup", {"customer": "c"}, True)
    again = replay.plan(named, [], "agent:handle", None)
    assert again.trigger == replace(TRIGGER, name="refund")


@pytest.mark.parametrize("call", [None, "__main__:handle"])
def test_an_entrypoint_in_main_is_refused_for_the_child_s_own_main_it_would_call(call):
    record = _record(replace(TRIGGER, entrypoint="__main__:main"))
    with pytest.raises(replay.Refused) as refused:
        replay.plan(record, [], call, None)
    entrypoint = call or "__main__:main"
    qualname = entrypoint.partition(":")[2]
    assert str(refused.value) == (
        f"run src was started as a script, so irimi cannot import its trigger {entrypoint}; "
        f"replay it with: irimi replay src --call <module>:{qualname} --python <your venv python>"
    )


@pytest.mark.parametrize(
    ("argv", "shown", "module"),
    [
        (
            ["/usr/bin/python3", "bots/refunder.py", "--live"],
            "python3 bots/refunder.py --live",
            "refunder",
        ),
        (["python", "-m", "bots.refunder"], "python -m bots.refunder", "bots.refunder"),
        (["./refunder.py"], "refunder.py", "refunder"),
    ],
)
def test_a_trigger_in_main_is_refused_with_the_command_that_replays_it(argv, shown, module):
    """A process run's command line names the module a trigger in `__main__` lives in."""
    record = runner.process_run("src", argv, None, 1.0)
    with pytest.raises(replay.Refused) as refused:
        replay.plan(record, [], "__main__:handle", None)
    assert str(refused.value) == (
        f"run src was started as a script ({shown}), so irimi cannot import its trigger; replay "
        f"it with: irimi replay src --call {module}:handle --python <your venv python>"
    )


def test_a_process_run_s_argv_is_no_function_s_parameters():
    record = runner.process_run("src", ["python", "agent.py"], None, 1.0)
    with pytest.raises(replay.Refused) as refused:
        replay.plan(record, [], "agent:handle", None)
    assert str(refused.value) == (
        "run src recorded a command line, not a call's arguments; pass --args with --call"
    )
    assert replay.plan(record, [], "agent:handle", {"n": 1}).call == replay.Call(
        "agent:handle", {"n": 1}
    )


def test_a_replay_s_run_of_a_trigger_taking_argv_is_called_with_its_args_not_run_as_argv():
    """A replay's run is a process run that keeps the trigger it called: its entrypoint says its
    args are that function's, even a parameter named `argv`."""
    called = Trigger("main", "agent:main", {"argv": ["agent", "-v"]}, True)
    record = replace(runner.process_run("src", ["python"], None, 1.0), trigger=called)
    plan = replay.plan(record, [], "agent:other", None)
    assert plan.call == replay.Call("agent:other", {"argv": ["agent", "-v"]})


def test_args_with_no_function_to_give_them_to_are_refused():
    record = runner.process_run("src", ["python", "agent.py"], None, 1.0)
    with pytest.raises(replay.Refused) as refused:
        replay.plan(record, [], None, {"n": 1})
    assert str(refused.value) == "run src has no entrypoint to give --args to; pass --call too"


def test_a_process_run_is_its_argv_run_again():
    record = runner.process_run("src", ["python", "agent.py", "-v"], None, 1.0)
    plan = replay.plan(record, [], None, None)
    assert (plan.call, plan.argv, plan.trigger, plan.rerun) == (
        None,
        ("python", "agent.py", "-v"),
        record.trigger,
        True,
    )
    # A command given in its place is not the recorded one.
    assert not replay.plan(record, ["python", "other.py"], None, None).rerun


def test_a_process_run_s_replay_says_it_reran_the_argv_alone():
    """The environment, cwd and stdin the run had are not recorded, so a replay that refunds
    another amount may only have lost an environment variable."""
    record = runner.process_run("src", ["python", "agent.py", "-v x"], None, 1.0)
    replayed = _run(replace(record, run_id="new", replay_of="src"), [])
    rerun = ("python", "agent.py", "-v x")
    lines = replay.outcome(_run(record, []), replayed, replay.Ended(0), None, rerun).lines()
    assert lines[:3] == [
        "irimi replay · src -> new",
        "  re-ran python agent.py '-v x' (argv only: environment, cwd and stdin are not recorded)",
        "  agent exited 0",
    ]


@pytest.mark.parametrize(
    ("n", "spelled"),
    [(1, "1st"), (2, "2nd"), (3, "3rd"), (4, "4th"), (11, "11th"), (12, "12th"), (13, "13th")]
    + [(21, "21st"), (22, "22nd"), (101, "101st"), (111, "111th")],
)
def test_ordinal_spells_a_call_s_place(n, spelled):
    assert replay.ordinal(n) == spelled


@pytest.mark.parametrize(
    "record",
    [
        _record(None, "header"),
        _record(Trigger("block", None, {"date": "d"}, True)),
        _record(Trigger("python", None, {"argv": []}, True), "process"),
        _record(Trigger("python", None, {"argv": ["python", 3]}, True), "process"),
        # Only a process run's args are its argv: an `sdk.run` may record any args.
        _record(Trigger("block", None, {"argv": ["python", "x.py"]}, True)),
    ],
)
def test_a_run_with_nothing_to_call_or_run_is_refused(record):
    with pytest.raises(replay.Refused) as refused:
        replay.plan(record, [], None, None)
    assert str(refused.value) == "run src has no entrypoint and no argv; pass --call or -- CMD"


@pytest.mark.parametrize(
    ("trigger", "reason"),
    [
        (None, "the run has no trigger"),
        (replace(TRIGGER, args=redact.REDACTION_FAILED), "the store could not redact them"),
        (
            replace(TRIGGER, args={"send": {"__irimi_repr__": "<fn>"}}, replayable=False),
            "a value was recorded as its description",
        ),
        (
            replace(TRIGGER, args={"n": {"__irimi_truncated__": True, "bytes": 9}}),
            "a value was recorded as its description",
        ),
        (replace(TRIGGER, replayable=False), "the SDK could not capture them as values"),
        (replace(TRIGGER, args=[1, 2]), "they are not an object of parameters"),
        (TRIGGER, None),
    ],
)
def test_not_replayable_says_why_recorded_args_cannot_be_given_again(trigger, reason):
    assert replay.not_replayable(trigger) == reason


def test_args_not_recorded_replayably_are_refused_unless_args_are_given():
    record = _record(replace(TRIGGER, replayable=False))
    with pytest.raises(replay.Refused) as refused:
        replay.plan(record, [], None, None)
    assert str(refused.value) == (
        "run src's trigger arguments were not recorded replayably (the SDK could not capture "
        "them as values); pass --args"
    )
    assert replay.plan(record, [], None, {"ticket": None}).call == replay.Call(
        "agent:handle", {"ticket": None}
    )
    with pytest.raises(replay.Refused, match="--args holds a value's description"):
        replay.plan(record, [], None, {"ticket": {"__irimi_repr__": "x"}})


def _exchange(
    path: str, flags: tuple[str, ...], issued_by: str = "agent", answered_by: str | None = None
) -> Exchange:
    if answered_by is None:
        answered_by = "recording" if "from-recording" in flags else "live"
    return Exchange(
        request=Request("GET", "https", "api.stripe.com", 443, path, "", (), b""),
        response=Response(200, (), b"{}"),
        service="stripe",
        operation="charges.retrieve",
        kind="read",
        answered_by=answered_by,  # type: ignore[arg-type]
        validation="unvalidated",
        flags=flags,
        issued_by=issued_by,  # type: ignore[arg-type]
        run_id="new",
    )


def _tool(kind: str, ran: str) -> ToolCall:
    return ToolCall("t", "new", f"db.{kind}", kind, ran, {}, None, None, 1.0, 1.0)  # type: ignore[arg-type]


def _run(record: RunRecord, events: list) -> StoredRun:
    return StoredRun(record, events)


def test_the_divergence_is_the_first_agent_exchange_or_read_tool_the_recording_missed():
    """A precondition read irimi issued and a write tool's stand-in are never one, and are not
    counted, so the divergence is numbered among the calls the totals count; a read tool that ran
    for real is one."""
    events = [
        _exchange("/v1/charges/a", ("fidelity:recording", "from-recording")),
        _exchange("/v1/charges/p", ("replay-miss",), issued_by="engine"),
        _tool("write", "shadow"),
        _tool("read", "recording"),
        _tool("read", "real"),
        _exchange("/v1/charges/b", ("replay-miss",)),
    ]
    source = _run(_record(), [])
    replayed = _run(replace(_record(), run_id="new", replay_of="src"), events)
    outcome = replay.outcome(source, replayed, replay.Ended(0), None)
    assert outcome.diverged_at == "the agent's 3rd call: tool-real     read      db.read -> ok"
    assert (outcome.answered, outcome.missed) == (2, 2)
    assert outcome.lines() == [
        "irimi replay · src -> new",
        "  agent exited 0",
        "  diverged at the agent's 3rd call: tool-real     read      db.read -> ok",
        "  2 from the recording · 2 missed",
        "",
        "  + ○ tool  db.write  fake ran instead",
        "",
        "  0 same · 0 changed · 0 only in A · 1 only in B",
    ]
    assert outcome.exit_code == 1
    assert outcome.result() == ReplayResult(
        "src",
        "new",
        1,
        "the agent's 3rd call: tool-real     read      db.read -> ok",
        tuple(outcome.lines()),
    )


def test_a_replay_that_missed_nothing_says_so_and_names_both_versions_and_other_args():
    source = _run(replace(_record(), agent_version="v1"), [])
    other = replace(TRIGGER, args={"ticket": None})
    replayed = _run(replace(_record(other), run_id="new", agent_version="v2\n"), [])
    lines = replay.outcome(source, replayed, replay.Ended(3), None).lines()
    assert lines[:4] == [
        "irimi replay · src -> new · agent v1 -> v2\\n",
        "  agent exited 3",
        "  (the triggers' args differ)",
        f"  {replay.NO_DIVERGENCE}",
    ]


def test_a_run_with_no_trigger_replayed_by_a_command_says_no_args_differ():
    """A `header` run recorded no trigger, and its replay by `-- CMD` records the command line, as
    a process run does: no args were given, so none are said to differ."""
    source = _run(_record(None, "header"), [])
    record = runner.process_run("new", ["python", "agent.py"], None, 1.0)
    replayed = _run(replace(record, replay_of="src"), [])
    lines = replay.outcome(source, replayed, replay.Ended(0), None).lines()
    assert lines[:3] == [
        "irimi replay · src -> new",
        "  agent exited 0",
        f"  {replay.NO_DIVERGENCE}",
    ]


def _new(events: list) -> StoredRun:
    return _run(replace(_record(), run_id="new", replay_of="src"), events)


def test_a_replay_that_made_fewer_calls_than_its_recording_says_how_many_it_never_made():
    """A child that stopped early is no replay that answered every call. A write cut before
    any policy was asked carries no replay flag, and was made all the same."""
    recorded = [_exchange("/v1/charges/a", ()), _tool("read", "real"), _tool("write", "shadow")]
    hit = _exchange("/v1/charges/a", ("fidelity:recording", "from-recording"))
    outcome = replay.outcome(_run(_record(), recorded), _new([hit]), replay.Ended(1), None)
    assert outcome.lines()[2:4] == [
        "  no divergence, but 1 of the recording's calls was never made",
        "  1 from the recording · 0 missed",
    ]
    # A delegated answer is the target's, never looked up in the recording: not a call missed.
    delegated = _exchange("/v1/charges/d", (), answered_by="delegated")
    outcome = replay.outcome(_run(_record(), [delegated]), _new([]), replay.Ended(0), None)
    assert outcome.lines()[2] == f"  {replay.NO_DIVERGENCE}"
    cut = _exchange("/v1/refunds", ("decision-failed", "body-truncated"))
    outcome = replay.outcome(_run(_record(), [cut]), _new([cut]), replay.Ended(0), None)
    assert outcome.lines()[2] == f"  {replay.NO_DIVERGENCE}"


def test_a_trigger_that_raised_is_an_agent_that_ran():
    """A call the child could not make has no outcome: `irimi replay` stops before one
    (`test_replay_cli`)."""
    raised = CallOutcome(True, ErrorInfo("builtins.KeyError", "'k'"))
    ended = replay.outcome(_run(_record(), []), _new([]), replay.Ended(1, raised), None)
    assert (ended.lines()[1], ended.exit_code) == ("  agent exited 1", compare.SAME)


def test_a_stopped_replay_exits_as_the_signal_did_and_says_so():
    ended = replay.Ended(143, stopped=143)
    outcome = replay.outcome(_run(_record(), []), _new([]), ended, None)
    assert outcome.lines()[1:3] == [
        "  agent exited 143",
        "  stopped: irimi replay was interrupted before the agent ended",
    ]
    assert outcome.exit_code == 143


def test_a_run_the_store_dropped_events_of_is_never_called_the_same():
    source = _run(replace(_record(), dropped_events=2), [])
    outcome = replay.outcome(source, _new([]), replay.Ended(0), None)
    assert "  the store dropped 2 of the recording's events, which are not compared" in (
        outcome.lines()
    )
    assert outcome.exit_code == compare.DIFFERENT


def test_a_result_is_the_outcome_s_and_a_refusal_holds_its_reason():
    outcome = replay.outcome(_run(_record(), []), _new([]), replay.Ended(0), None)
    assert outcome.result() == ReplayResult("src", "new", 0, None, tuple(outcome.lines()))
    assert replay.refused("src", ["error: no run src in /s"]) == ReplayResult(
        "src", None, compare.TROUBLE, None, ("error: no run src in /s",)
    )
