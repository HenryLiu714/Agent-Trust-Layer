"""Every scenario's recording, replayed unchanged, run by run.

`test_invariants.py` holds the replays the workflows pin to invariants 1 to 4. This holds EVERY
scenario's: each run its shadow run stored (`replay_targets`) - the SDK's runs, a run W9 posted by
hand, a `header` run, or the process run when there is no other - replayed by the real `irimi
replay <run id>`, in a copy of the recording's home, against a fresh `World` and fake internet. A
process that held several runs (W3, W5, W8, W9's control scenarios) is replayed once per run.

`irimi replay` runs what it runs for that run: the run's trigger, called alone by
`irimi.sdk.replay_child` from its entrypoint and its recorded args, or a process run's argv. A run
it cannot call - an `sdk.run` block or a `header` run with no entrypoint, args stored by their
description - it refuses, exit 2, and those scenarios are replayed by their command instead
(`-- <cmd>`, `REFUSED`), which runs the agent's whole process, every run of it adopted into the
replay's one run. Either way a replay is ONE run, so another run of the process no longer takes the
replayed run's answers.

A replay of a recording its agent makes again owes:

1. invariants 1 to 4, and no write reaching a fake service but an escape irimi never saw;
2. the exit code its recording had: a trigger called alone exits 0 for a run that ended `ok` and 1
   for one that raised, and a command exits as the recorded process did; and a trigger called
   alone, or a process run's argv, ends the replay's run as the recorded run ended, a raised
   builtin stored under its recorded type;
3. every exchange the policy decided flagged `from-recording` or `replay-miss`, never both;
4. every answer irimi decided reaching the agent stamped `Irimi-Answered-By`, as irimi recorded it;
5. `irimi replay`'s comparison of the recorded run with its replay printing `=` rows, or
   a `?` pair for a write whose body was cut, and exiting 0, or 1 for the cut; its result never
   saying the triggers' args differ, since the replay was given the recorded ones, and its
   result file saying what it printed and exited (`result_problems`);
6. nothing reaching the fake internet but what the replay could not answer from that run, each
   one a read or llm call it forwarded as a miss, telemetry, or an escape irimi never saw - and
   nothing at all for a scenario whose one run the recording holds whole;
7. and for that scenario, no miss at all: an unchanged replay of a whole recording never diverges,
   and says so;
8. and the agent making the calls its run made when it was recorded - the run's own, for a trigger
   called alone, and the whole process's for a command - each answered with the status it was
   answered with then, in any order, so a replay that never ran its agent passes nothing;
9. and the tool calls the proxy cannot see made again: the replay's run makes the recorded
   run's, each read tool answered from the recording and each write tool through its stand-in,
   and the corpus's own log of which body ran says what irimi stored; and an agent whose `main()`
   ran told it replays, `IRIMI_REPLAY_RUN` naming the replay's one run.

What a scenario cannot owe, it names in a table below with the reason, and each entry must still be
needed, so a fix that makes one unnecessary fails here until the entry goes.
"""

from collections import Counter
from dataclasses import dataclass

import pytest

from examples.workflows.harness.run import (
    Result,
    check_invariants,
    counterpart,
    replay_targets,
    workflows,
)
from irimi.compare import ARGS_DIFFER
from irimi.exchange import LIVE_KINDS, Flag
from irimi.replay import plan as replay_plan
from irimi.replay import report as replay_report
from irimi.runner import NO_PROXY_VALUE
from irimi.trace import ToolCall

CASES = [(name, scenario) for name, wf in workflows().items() for scenario in wf.scenarios]
W2, W3, W5, W6 = (
    "w02_nightly_reconcile",
    "w03_queue_worker",
    "w05_dispute_responder",
    "w06_crm_db_agent",
)
W7, W8, W9 = "w07_streaming_assistant", "w08_orchestrator", "w09_scope_gauntlet"
W10, W11 = "w10_flaky_upstream", "w11_leaky_agent"

# The runs `irimi replay` refuses to call alone, by scenario, and why (`irimi.replay.plan.plan`):
# each is replayed by its scenario's command instead. Read off each run's record, so this table
# only says which scenarios hold one, and
# `test_the_runs_replayed_by_their_command_are_the_ones_irimi_refuses` holds it to what irimi
# refuses.
_NO_ENTRYPOINT = "an `sdk.run` block or a `header` run: no entrypoint to call"
_DESCRIBED = "its trigger's args hold a value stored by its description, a callable"
_BY_HAND = "its run was posted by hand, args not captured as values"
REFUSED: dict[tuple[str, str], str] = {
    **{
        (W2, s): _NO_ENTRYPOINT
        for s in (
            "clean",
            "forty_mismatches",
            "two_mismatches",
            "datetime_trigger",
            "same_date_twice",
            "refund_then_page",
        )
    },
    (W3, "async_run"): _NO_ENTRYPOINT,
    **{
        (W7, s): _DESCRIBED
        for s in (
            "normal",
            "httpx_streams",
            "secret_in_stream",
            "caller_disconnects",
            "upstream_reset",
            "reset_before_stream",
            "tool_call_across_deltas",
            "secret_split_across_writes",
            "gzip_bomb_stream",
            "gzip_stream",
            "two_callers",
        )
    },
    (W9, "control_runs"): _BY_HAND,
    (W9, "control_hazards"): _BY_HAND,
    **{
        (W10, s): _NO_ENTRYPOINT
        for s in ("control_unset", "control_unreachable", "control_refused")
    },
    **{
        (W11, s): _NO_ENTRYPOINT
        for s in (
            "proxied",
            "proxyless_client",
            "no_proxy_star",
            "raw_socket",
            "redirected",
            "loopback_service",
            "labelled_sidecar",
        )
    },
}
# The runs whose trigger, called alone, does not do what it did in its process, and why: what it
# depended on is the agent's `main()`'s, or another run's. Each replay of one is pinned to
# how it ended instead of rules 2, 4, 5, 7, 8 and 9: `(the agent's line, the replay's exit, the
# comparison's totals)`, one per distinct ending among the scenario's runs, or, for a trigger the
# replay could not call, why and no totals: it compares nothing. Rules 1, 3 and 6 hold.
_SAME_ALONE = "  agent exited {}"
_METHOD = "it is a method, and a run does not record the instance it was called on"
_TOTALS = "  {} same · {} changed · {} only in A · {} only in B"


@dataclass(frozen=True)
class Alone:
    reason: str
    ends: frozenset[tuple[str, int, str]]


def _alone(reason: str, *ends: tuple[str, int, tuple[int, int, int, int] | None]) -> Alone:
    return Alone(
        reason,
        frozenset((agent, code, _TOTALS.format(*t) if t else "") for agent, code, t in ends),
    )


_LOOP = "the trigger schedules its task on the event loop `main()` runs, and raises without it"
_THREAD = (
    "the thread the trigger starts without `propagate` made its calls in the process run, which "
    "the run's recording lacks (#75), so its refund misses and is faked again"
)
_ACCOUNTS = "a tool reads the accounts table the agent's `main()` creates, and raises without it"
_RECORD = (
    "`--record`, which makes the trigger call its tools, is `main()`'s flag, so the replay makes "
    "no tool call"
)
ALONE: dict[tuple[str, str], Alone] = {
    (W3, "worker_methods"): _alone(
        "a method: a run does not record its instance, so `replay_child` cannot call it",
        (_METHOD, 2, None),
    ),
    (W3, "ledger_worker"): _alone(
        "a method: a run does not record its instance, so `replay_child` cannot call it",
        (_METHOD, 2, None),
    ),
    (W3, "task_outlives_trigger"): _alone(_LOOP, (_SAME_ALONE.format(1), 1, (0, 0, 1, 0))),
    (W3, "unpropagated_thread"): _alone(_THREAD, (_SAME_ALONE.format(0), 1, (0, 0, 0, 1))),
    (W3, "ledger_unpropagated"): _alone(_THREAD, (_SAME_ALONE.format(0), 1, (0, 0, 0, 2))),
    (W5, "replayed_event"): _alone(
        "the second delivery's run did nothing because the process remembered the first; its "
        "trigger alone does not, so it handles the dispute again, missing every call",
        (_SAME_ALONE.format(0), 0, (3, 0, 0, 0)),
        (_SAME_ALONE.format(0), 1, (0, 0, 0, 3)),
    ),
    (W6, "merge_retry"): _alone(_ACCOUNTS, (_SAME_ALONE.format(1), 1, (0, 1, 1, 0))),
    (W6, "merge_once"): _alone(_ACCOUNTS, (_SAME_ALONE.format(1), 1, (0, 1, 0, 0))),
    (W6, "read_after_write"): _alone(_ACCOUNTS, (_SAME_ALONE.format(1), 0, (1, 0, 0, 0))),
    (W6, "unlabeled_write"): _alone(_ACCOUNTS, (_SAME_ALONE.format(1), 0, (0, 0, 0, 0))),
    (W8, "nested_tools"): _alone(_RECORD, (_SAME_ALONE.format(0), 1, (1, 0, 1, 0))),
    (W8, "target_down_tools"): _alone(_RECORD, (_SAME_ALONE.format(0), 1, (1, 0, 1, 0))),
}

# Rule 6's exceptions: the scenarios a replay of which reaches the fake internet, and why. Every
# request there must still be one the replay forwarded as a miss, telemetry, or an escape.
_ALL_RUNS = (
    "replayed by its command, the replay's one run holds every run of the process, and the "
    "recording one of them"
)
_UNSERVABLE = "the recorded answer is not whole (`UNSERVABLE_FLAGS`), so it misses and goes live"
_ESCAPE = "the escape goes around irimi, in the replay as in the recording (`leaks`)"
REACHES_THE_INTERNET: dict[tuple[str, str], str] = {
    (W3, "async_run"): _ALL_RUNS,
    (W5, "replayed_event"): ALONE[(W5, "replayed_event")].reason,
    (W7, "normal"): "the LangSmith trace is telemetry, never looked up, forwarded as in shadow",
    (W7, "caller_disconnects"): _UNSERVABLE + ": `stream-truncated`, `upstream-error`",
    (W7, "upstream_reset"): _UNSERVABLE + ": `stream-truncated`, `upstream-error`",
    (W7, "reset_before_stream"): _UNSERVABLE + ": `upstream-error`",
    (W7, "gzip_bomb_stream"): _UNSERVABLE + ": `stream-truncated` at irimi's cap",
    (W7, "two_callers"): _ALL_RUNS,
    (W9, "control_runs"): _ALL_RUNS,
    (W9, "bombs"): _UNSERVABLE + ": `body-truncated` request bodies, an llm call and a read",
    (W10, "read_timeout"): _UNSERVABLE + ": `upstream-error`",
    (W10, "reset_on_read"): _UNSERVABLE + ": `upstream-error`",
    (W10, "gzip_bomb_read"): _UNSERVABLE + ": `body-truncated` at irimi's cap",
    (W10, "br_bomb_read"): _UNSERVABLE + ": `body-truncated` at irimi's cap",
    (W10, "zstd_bomb_read"): _UNSERVABLE + ": `body-truncated` at irimi's cap",
    (W11, "proxyless_client"): _ESCAPE,
    (W11, "no_proxy_star"): _ESCAPE,
    (W11, "raw_socket"): _ESCAPE,
    (W11, "loopback_service"): _ESCAPE,
    (W11, "redirected"): "each hop's query names the fake internet's port, new in every run",
}
# Rule 3's exception: a write whose body decoded past irimi's cap is refused by the engine before
# any policy is asked, in a replay as when it was recorded, so no replay flag is due.
CUT_BEFORE_THE_POLICY = {(W9, "bombs"), (W9, "cut_refund"), (W9, "cut_update")}
# Rule 2's exception: (recorded exit, replay's agent exit, why).
EXIT_DIFFERS = {
    (W7, "two_callers"): (
        0,
        4,
        "the fake streams the two chats in lockstep; replaying the process answers one chat's "
        "stream from the recording, so the other's live stream waits for a partner that never "
        "comes, and fails",
    ),
}
# Rule 5's exception: the comparison differs, and why.
COMPARE_DIFFERS = {(W3, "async_run"): _ALL_RUNS}
# Rule 2's end's exception: a trigger called alone that ends otherwise than its recorded run, but
# exits as it did, by scenario: (the recorded run's error type, the replay's, and why).
ENDS_DIFFER = {
    (W3, "cancelled_on_shutdown"): (
        "asyncio.exceptions.CancelledError",
        "builtins.AssertionError",
        "the stuck message's trigger waits on the event `main()`'s shutdown creates and then "
        "cancels; called alone it finds none, and raises",
    ),
}
# Rule 9 cannot hold for an agent that names its runs itself and posts its tool calls itself too
# (W9's control scenarios, `test_w09_scope_gauntlet.py`): its posts name the recorded runs again,
# which a replay's store files as `unattributed`, and it logs none; nor rule 7's "no
# divergence", since a tool it posts says it ran for real. A serve that refused its map recorded
# nothing to replay (W8 `map_refused`).
NAMES_ITS_OWN_RUNS = {(W9, "control_runs"), (W9, "control_hazards")}
NOTHING_RECORDED = {(W8, "map_refused"), ("w04_slack_ops_bot", "post_fixture_missing")}
# Rule 8's exception: the scenarios whose agent makes fewer calls in a replay, and why.
CALLS_FEWER = {
    (W6, "enrich"): (
        "`crm.fetch_enrichment` makes its GET inside a read tool the recording answers, so the GET "
        "is never made"
    ),
}
# Rule 9's exceptions. The read tools a replay runs for real, by scenario, and why:
_NOT_REVIVED = "its recorded result holds `Decimal` and `datetime` as `__irimi_repr__`"
TOOLS_RUN_FOR_REAL: dict[tuple[str, str], dict[str, str]] = {
    (W6, "enrich"): {"crm.score_account": _NOT_REVIVED},
}


@dataclass
class Replayed:
    run_id: str
    replay: Result
    by_command: bool  # replayed by the scenario's command, since `irimi replay` refuses the run
    counterpart: str | None  # the replay's run, which stands for `run_id` (`counterpart`)
    # The comparison's exit and rows; None with no run, or an agent the replay could not call.
    compared: tuple[int, list[str]] | None


def refused(recorded: Result, run_id: str) -> str | None:
    """Why `irimi replay` refuses to call the recorded run `run_id` alone, or None when it calls
    it: irimi's own rule, `irimi.replay.plan.plan`."""
    record = recorded.stored().load_run(run_id).record
    try:
        replay_plan.plan(record, [], None, None)
    except replay_plan.Refused as exc:
        return str(exc)
    return None


def replays_of(run_workflow, replayed, name: str, scenario: str) -> list[Replayed]:
    """Each run of the scenario's shadow recording, replayed, with its comparison."""
    recorded = run_workflow(name, scenario, "shadow")
    if not recorded.stored().list_runs():
        return []
    found = []
    for run_id in replay_targets(recorded):
        by_command = refused(recorded, run_id) is not None
        _, replay = replayed(name, scenario, run_id=run_id, command=by_command)
        other = counterpart(recorded, replay, run_id)
        compared = None
        result = replay.replay_result()
        if other is not None and "" in result:
            compared = (replay.exit_code, result[result.index("") + 1 :])
        found.append(Replayed(run_id, replay, by_command, other, compared))
    return found


def ending(r: Replayed) -> tuple[str, int, str]:
    """How a replay ended, as `ALONE` pins it: the agent's line, the exit and the totals; or why
    the replay could not call the trigger (`error: cannot call <entrypoint>: <why>`), the exit,
    and no totals."""
    lines = r.replay.replay_result()
    if r.compared is None:
        return (lines[0].split(": ", 2)[-1], r.replay.exit_code, "")
    agent = next(line for line in lines if line.startswith("  agent "))
    return (agent, r.replay.exit_code, lines[-1])


def tool_calls(result: Result, run_ids: set[str]) -> list[ToolCall]:
    """The tool calls `result`'s store holds in the runs `run_ids`, run by run in `seq` order."""
    reader = result.stored()
    return [
        event
        for run_id in sorted(run_ids)
        for event in reader.load_run(run_id).events
        if isinstance(event, ToolCall)
    ]


def tool_problems(recorded: Result, r: Replayed, for_real: dict[str, str], logs: bool) -> list[str]:
    """Rule 9: how the replay's tool calls differ from what it owes. Its run made the
    recorded run's tool calls, by name and kind, in any order; each read ran from the recording,
    but those `for_real` names, and each write its stand-in. And when the agent `logs` its tools
    through `examples/workflows/sdk.py`, the log of which body ran agrees with every tool call the
    replay stored."""
    problems = []
    if r.counterpart is not None:
        mine = tool_calls(recorded, {r.run_id})
        theirs = tool_calls(r.replay, {r.counterpart})
        made = Counter((c.name, c.kind) for c in theirs)
        if made != Counter((c.name, c.kind) for c in mine):
            problems.append(f"made {sorted(made.elements())}, recorded {mine}")
        for call in theirs:
            owed = "shadow" if call.kind == "write" else "real" if call.name in for_real else None
            if call.ran != (owed or "recording"):
                problems.append(f"{call.name} ran {call.ran}")
    if not logs:
        return problems
    before = {run.run_id for run in recorded.stored().list_runs()}
    replayed_runs = {run.run_id for run in r.replay.stored().list_runs()} - before
    stored = Counter((c.name, c.ran) for c in tool_calls(r.replay, replayed_runs))
    logged = Counter((t["name"], t["ran"]) for t in r.replay.events("tool"))
    if stored != logged:
        problems.append(f"stored {sorted(stored.elements())}, logged {sorted(logged.elements())}")
    return problems


def result_problems(r: Replayed) -> list[str]:
    """Rule 5's result: what `irimi replay --result-file` wrote names the run replayed and the
    replay's one run, exits as `irimi replay` did, and holds the divergence point its lines name,
    or none when they name none; and no replay given the recorded args says they differ."""
    result, replay = r.replay.replay, r.replay
    assert result is not None
    problems = []
    if (result.source_run_id, result.replay_run_id) != (r.run_id, replay.process_run_id()):
        problems.append(f"names {result.source_run_id} -> {result.replay_run_id}")
    if result.exit_code != replay.exit_code:
        problems.append(f"result exit {result.exit_code}, irimi replay exited {replay.exit_code}")
    said = [line for line in result.lines if line.startswith("  diverged at ")]
    owed = [] if result.diverged_at is None else [f"  diverged at {result.diverged_at}"]
    if said != owed:
        problems.append(f"diverged_at {result.diverged_at!r}, lines {said}")
    if f"  {ARGS_DIFFER}" in result.lines:
        problems.append(ARGS_DIFFER)
    return problems


def end_problems(recorded: Result, r: Replayed) -> list[str]:
    """Rule 2's end: a trigger called alone, or a process run's argv, ends the replay's run as the
    recorded run ended: `ok`, or `error` with what it raised, under the type the recording
    stored, `builtins.KeyError` for a `KeyError`, and a process run with its exit. A run the
    recording never saw end (a kill) owes nothing here."""
    source = recorded.stored().load_run(r.run_id).record
    if r.by_command or source.end is None:
        return []
    mine = r.replay.stored().load_run(r.replay.process_run_id()).record
    ours, theirs = mine.end, source.end
    if (ours.outcome, ours.error) != (theirs.outcome, theirs.error):
        recorded_type = theirs.error.type if theirs.error is not None else None
        replayed_type = ours.error.type if ours.error is not None else None
        return [f"ended {ours.outcome} {replayed_type}, recorded {theirs.outcome} {recorded_type}"]
    return []


def flag_problems(replay: Result) -> list[str]:
    """Each exchange a policy decided flagged neither `from-recording` nor `replay-miss`, or both:
    every one but telemetry and a delegated answer, which a replay never looks up, whether its
    target answered or irimi answered for it with `target-failed`."""
    return [
        line(ex)
        for ex in replay.reported
        if ex.kind != "telemetry"
        and ex.answered_by != "delegated"
        and Flag.TARGET_FAILED not in ex.flags
        and len({Flag.FROM_RECORDING, Flag.REPLAY_MISS} & set(ex.flags)) != 1
    ]


def calls(result: Result, run_id: str | None = None) -> Counter:
    """The agent's calls, as `(label or URL, status)`, in any order - concurrent runs (W3) make
    theirs in another order each time - all of them, or those of the run `run_id`."""
    return Counter(
        (c.get("label") or c["url"], c.get("status"))
        for c in result.calls()
        if run_id is None or c.get("run") == run_id
    )


def owed_calls(recorded: Result, r: Replayed) -> Counter:
    """Rule 8: the calls a replay's agent owes. A trigger called alone makes its run's own; a
    command, or a process run's argv, the whole process's."""
    record = recorded.stored().load_run(r.run_id).record
    whole = r.by_command or record.attribution == "process"
    return calls(recorded) if whole else calls(recorded, r.run_id)


def owed_exit(recorded: Result, r: Replayed) -> int:
    """Rule 2: the exit code a replay's agent owes. A trigger called alone exits 0 when its run
    ended `ok` and 1 when it raised (`replay_child`); a command or an argv, as the process did, and
    so does a trigger whose process a signal ended (W10's), since the signal ends it again."""
    record = recorded.stored().load_run(r.run_id).record
    if r.by_command or record.attribution == "process" or recorded.exit_code >= 128:
        return recorded.exit_code
    return 0 if record.end.outcome == "ok" else 1


def line(ex) -> str:
    return f"{ex.answered_by} {ex.request.method} {ex.request.host}{ex.request.path} {ex.flags}"


def stamp_problems(replay: Result) -> Counter:
    """The `Irimi-Answered-By` values the agent saw, less those irimi decided, and the reverse: a
    decided answer that reached the agent unstamped, or stamped otherwise. By value, since a call
    the agent logs by its first URL may be answered at a redirect's (W11) or through the reverse
    door (W9). `control` is the control endpoint's, which no exchange is."""
    seen = Counter(c["answered_by"] for c in replay.calls() if c.get("answered_by"))
    seen.pop("control", None)
    decided = Counter(
        ex.answered_by
        for ex in replay.reported
        if ex.issued_by == "agent" and ex.answered_by != "live" and ex.kind != "telemetry"
    )
    return (seen - decided) + Counter({f"unseen {k}": v for k, v in (decided - seen).items()})


def unaccounted(replay: Result, leaks: bool) -> list[str]:
    """Each request that reached the fake internet and is not one irimi forwarded live in this
    replay - a read or llm call that missed, its body the service's or the overlay's, or telemetry
    - nor one that went around irimi: to loopback, which `NO_PROXY` keeps off the proxy (W11's
    redirects and sidecar), or, in a `leaks` scenario, any write irimi never saw."""
    forwarded = Counter(
        (ex.request.method, ex.request.host, ex.request.path)
        for ex in replay.reported
        if ex.answered_by in ("live", "overlay")
        and ex.kind in LIVE_KINDS
        and (ex.kind == "telemetry" or Flag.REPLAY_MISS in ex.flags)
    )
    around = set(NO_PROXY_VALUE.split(","))
    left = []
    for r in replay.internet.requests():
        key = (r.method, r.host, r.path)
        if forwarded[key]:
            forwarded[key] -= 1
        elif r.host not in around and not (leaks and r in replay.internet.writes()):
            left.append(f"{r.method} {r.host}{r.path}")
    return left


@pytest.mark.parametrize(("name", "scenario"), CASES, ids=[f"{n}:{s}" for n, s in CASES])
def test_every_run_of_every_scenario_replays_from_its_recording(
    run_workflow, replayed, name, scenario
):
    case = (name, scenario)
    recorded = run_workflow(name, scenario, "shadow")
    spec = recorded.workflow.scenarios[scenario]
    replays = replays_of(run_workflow, replayed, name, scenario)
    assert (replays == []) == (case in NOTHING_RECORDED)
    alone_differs = case in ALONE
    for r in replays:
        replay = r.replay
        # 1. The invariants, and no write forwarded, matched or missed: the one write that may
        # reach a fake service is an escape irimi never saw (rule 6).
        assert check_invariants(replay) == [], r.run_id
        assert [
            line(ex)
            for ex in replay.reported
            if ex.kind not in LIVE_KINDS and (ex.answered_by == "live")
        ] == [], r.run_id
        # 2. The agent exits as it did when it was recorded.
        owed = (recorded.exit_code, owed_exit(recorded, r))
        expected = EXIT_DIFFERS.get(case, (*owed, ""))[:2]
        if not alone_differs:
            assert (recorded.exit_code, replay.agent_exit_code()) == expected, r.run_id
            ended = end_problems(recorded, r)
            if case in ENDS_DIFFER:
                was, now, _ = ENDS_DIFFER[case]
                ended = [p for p in ended if p != f"ended error {now}, recorded error {was}"]
            assert ended == [], r.run_id
        # 3. Every looked-up exchange says whether the recording answered it.
        lost = flag_problems(replay)
        if case in CUT_BEFORE_THE_POLICY:
            lost = [
                x for x in lost if Flag.DECISION_FAILED not in x or Flag.BODY_TRUNCATED not in x
            ]
        assert lost == [], r.run_id
        # 4. Every answer irimi decided reached the agent stamped as irimi recorded it.
        if not alone_differs:
            assert stamp_problems(replay) == Counter(), r.run_id
        # 5. The comparison: the replay made the writes its recording did. A trigger the replay
        # could not call compares nothing, which only `ALONE` may pin.
        if r.compared is None:
            assert (alone_differs, r.replay.exit_code) == (True, 2), r.run_id
        else:
            code, rows = r.compared
            cut = any(row.startswith("  ? A ") for row in rows)
            same = all(row.startswith(("  = ", "  ? A ", "    B ")) for row in rows[:-2])
            totals = " · 0 changed · 0 only in A · 0 only in B" in rows[-1]
            if case not in COMPARE_DIFFERS and not alone_differs:
                assert (code, same, totals) == (1 if cut else 0, True, True), (r.run_id, rows)
        assert result_problems(r) == [], r.run_id
        # 6. Nothing else reaches the fake internet.
        assert unaccounted(replay, spec.leaks) == [], r.run_id
        if case not in REACHES_THE_INTERNET:
            assert replay.internet.requests() == [], r.run_id
            # 7. And a recording its replay could answer whole is answered whole: nothing misses,
            # and the replay says so.
            missed = [line(ex) for ex in replay.reported if Flag.REPLAY_MISS in ex.flags]
            if not alone_differs:
                assert missed == [], r.run_id
            # A read tool run for real is a miss too, which a scenario's table names.
            whole = not (alone_differs or case in TOOLS_RUN_FOR_REAL or case in NAMES_ITS_OWN_RUNS)
            if whole:
                assert f"  {replay_report.NO_DIVERGENCE}" in replay.replay_result(), r.run_id
        if alone_differs:
            continue
        # 8. The agent made the calls its run made when it was recorded, and was answered alike:
        # the rules above hold of a replay whose agent never ran, too.
        if case in CALLS_FEWER:
            assert calls(replay) < owed_calls(recorded, r), r.run_id
        elif case not in EXIT_DIFFERS:
            assert calls(replay) == owed_calls(recorded, r), r.run_id
        # 9. The tool calls the proxy cannot see were made again, and answered as replay answers
        # them, by an agent told it replays, in the replay's one run.
        told = {start["replay"] for start in replay.events("start")}
        ran_main = (
            r.by_command or recorded.stored().load_run(r.run_id).record.attribution == "process"
        )
        assert told == ({replay.process_run_id()} if ran_main else set()), r.run_id
        for_real = TOOLS_RUN_FOR_REAL.get(case, {})
        # An agent that names its runs itself posts its tool calls itself too (W9), and logs none.
        logs = case not in NAMES_ITS_OWN_RUNS
        if case not in COMPARE_DIFFERS and case not in NAMES_ITS_OWN_RUNS:
            problems = tool_problems(recorded, r, for_real, logs)
            assert problems == [], r.run_id
    # Each exception is still needed.
    if case in REFUSED:
        assert any(r.by_command for r in replays)
    else:
        assert not any(r.by_command for r in replays)
    if alone_differs:
        assert {ending(r) for r in replays} == ALONE[case].ends
    if case in REACHES_THE_INTERNET:
        assert any(r.replay.internet.requests() for r in replays)
    if case in CUT_BEFORE_THE_POLICY:
        assert any(flag_problems(r.replay) for r in replays)
    if case in COMPARE_DIFFERS:
        assert any(r.compared and r.compared[0] == 1 for r in replays)
    if case in CALLS_FEWER:
        assert any(calls(r.replay) != owed_calls(recorded, r) for r in replays)
    if case in ENDS_DIFFER:
        assert any(end_problems(recorded, r) for r in replays)
    ran = [
        (c.name, c.kind, c.ran)
        for r in replays
        for c in tool_calls(r.replay, {r.counterpart} if r.counterpart else set())
    ]
    for name in TOOLS_RUN_FOR_REAL.get(case, {}):
        assert (name, "read", "real") in ran


def test_the_runs_replayed_by_their_command_are_the_ones_irimi_refuses(run_workflow):
    """`REFUSED` names the scenarios holding a run `irimi replay` will not call alone, each with
    irimi's reason in its own words; every other scenario's runs it calls. Read off the
    records, with no replay."""
    found = {}
    for case in CASES:
        recorded = run_workflow(*case, "shadow")
        if not recorded.stored().list_runs():
            continue
        reasons = {refused(recorded, run_id) for run_id in replay_targets(recorded)} - {None}
        if reasons:
            found[case] = reasons
    assert set(found) == set(REFUSED)
    for case, reasons in found.items():
        assert all(
            ("no entrypoint and no argv" in why) == (REFUSED[case] == _NO_ENTRYPOINT)
            for why in reasons
        ), (case, reasons)


def test_the_tables_name_only_scenarios_that_exist():
    named = (
        set(REFUSED)
        | set(ALONE)
        | set(REACHES_THE_INTERNET)
        | CUT_BEFORE_THE_POLICY
        | set(EXIT_DIFFERS)
        | set(COMPARE_DIFFERS)
        | set(ENDS_DIFFER)
        | NAMES_ITS_OWN_RUNS
        | NOTHING_RECORDED
        | set(CALLS_FEWER)
        | set(TOOLS_RUN_FOR_REAL)
    )
    assert named <= set(CASES)


def test_rule_9_catches_a_replay_whose_tools_ran_as_they_did_under_shadow(run_workflow):
    """Rule 9 has something to catch: W6 `enrich`'s shadow run held up as its own replay, every
    read tool run for real and no tool call stored by a run it started, is flagged for each read
    the recording would have answered and for its log, but not for `score_account`, which a
    replay runs for real too, nor for a write, whose stand-in ran as a replay's does."""
    recorded = run_workflow(W6, "enrich", "shadow")
    [run] = recorded.sdk_runs()
    as_replay = Replayed(run.run_id, recorded, False, run.run_id, None)
    problems = tool_problems(recorded, as_replay, TOOLS_RUN_FOR_REAL[(W6, "enrich")], True)
    ran_for_real = [p for p in problems if p.endswith(" ran real")]
    assert sorted(ran_for_real) == [
        "crm.connect_with ran real",
        "crm.fetch_enrichment ran real",
        "crm.fetch_enrichment ran real",
        "crm.find_accounts ran real",
    ]
    assert [p for p in problems if p not in ran_for_real] == [
        p for p in problems if p.startswith("stored [], logged ")
    ]
    assert len(problems) == len(ran_for_real) + 1
