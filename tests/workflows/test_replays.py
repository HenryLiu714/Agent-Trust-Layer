"""Every scenario's recording, replayed unchanged, run by run (#82).

`test_invariants.py` holds the replays the workflows pin to invariants 1 to 4. This holds EVERY
scenario's: each run its shadow run stored (`replay_targets`) - the SDK's runs, a run W9 posted by
hand, a `header` run, or the process run when there is no other - replayed through the real
`irimi shadow` with `EngineConfig.replay_of` naming it, in a copy of the recording's home, against
a fresh `World` and fake internet. A process that held several runs (W3, W5, W8, W9's control
scenarios) is replayed once per run, each replay answering from that one run alone, as the issue's
"Replay which run?" says.

A replay of a recording its agent makes again owes:

1. invariants 1 to 4, and no write reaching a fake service but an escape irimi never saw;
2. the exit code its recording had;
3. every exchange the policy decided flagged `from-recording` or `replay-miss`, never both;
4. every answer irimi decided reaching the agent stamped `Irimi-Answered-By`, as irimi recorded it;
5. `irimi compare <recorded run> <its replay>` printing `=` rows, or a `?` pair for a write whose
   body was cut, and exiting 0, or 1 for the cut;
6. nothing reaching the fake internet but what the replay could not answer from that run, each
   one a read or llm call it forwarded as a miss, telemetry, or an escape irimi never saw - and
   nothing at all for a scenario whose one run the recording holds whole;
7. and for that scenario, no miss at all: an unchanged replay of a whole recording never diverges.

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
from irimi.exchange import (
    BODY_TRUNCATED_FLAG,
    DECISION_FAILED_FLAG,
    FROM_RECORDING_FLAG,
    LIVE_KINDS,
    REPLAY_MISS_FLAG,
)
from irimi.runner import NO_PROXY_VALUE

CASES = [(name, scenario) for name, wf in workflows().items() for scenario in wf.scenarios]
W3, W5, W7 = "w03_queue_worker", "w05_dispute_responder", "w07_streaming_assistant"
W8, W9, W10, W11 = "w08_orchestrator", "w09_scope_gauntlet", "w10_flaky_upstream", "w11_leaky_agent"

# Rule 6's exceptions: the scenarios a replay of which reaches the fake internet, and why. Every
# request there must still be one the replay forwarded as a miss, telemetry, or an escape.
_OTHER_RUNS = "the process holds other runs, whose requests the replayed run's recording lacks"
_UNSERVABLE = "the recorded answer is not whole (`UNSERVABLE_FLAGS`), so it misses and goes live"
_ESCAPE = "the escape goes around irimi, in the replay as in the recording (`leaks`)"
REACHES_THE_INTERNET: dict[tuple[str, str], str] = {
    **{
        (W3, s): _OTHER_RUNS
        for s in (
            "threads_8",
            "asyncio_8",
            "unpropagated_thread",
            "nested_trigger",
            "one_message_fails",
            "shared_charge",
            "async_run",
            "cancelled_on_shutdown",
            "worker_methods",
            "coroutine_handoff",
            "one_pool_8",
            "ledger_threads_8",
            "ledger_asyncio_8",
            "ledger_unpropagated",
            "ledger_worker",
            "task_outlives_trigger",
        )
    },
    (W5, "cascade"): _OTHER_RUNS + " (the event its write fires is a run with no exchanges)",
    (W5, "replayed_event"): _OTHER_RUNS + " (the second delivery's run has no exchanges)",
    (W7, "normal"): "the LangSmith trace is telemetry, never looked up, forwarded as in shadow",
    (W7, "caller_disconnects"): _UNSERVABLE + ": `stream-truncated`, `upstream-error`",
    (W7, "upstream_reset"): _UNSERVABLE + ": `stream-truncated`, `upstream-error`",
    (W7, "reset_before_stream"): _UNSERVABLE + ": `upstream-error`",
    (W7, "gzip_bomb_stream"): _UNSERVABLE + ": `stream-truncated` at irimi's cap",
    (W7, "two_callers"): _OTHER_RUNS,
    (W7, "hand_off"): "the models list is made after the run ended, in the process run",
    (W8, "nested_runs"): _OTHER_RUNS,
    (W8, "nested_tools"): _OTHER_RUNS,
    (W8, "nested_write"): _OTHER_RUNS,
    (W9, "control_runs"): _OTHER_RUNS,
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
# any policy is asked (#94), in a replay as when it was recorded, so no replay flag is due.
CUT_BEFORE_THE_POLICY = {(W9, "bombs"), (W9, "cut_refund"), (W9, "cut_update")}
# Rule 2's exception: (recorded exit, replay exit, why).
EXIT_DIFFERS = {
    (W7, "two_callers"): (
        0,
        4,
        "the fake streams the two chats in lockstep; replaying one chat answers its stream from "
        "the recording, so the other's live stream waits for a partner that never comes, and fails",
    ),
}
# Rule 5's exception. LOOKS WRONG: a replay answers every run of the process from the one run's
# recording, so another run's identical request takes its answers first. W3 `shared_charge`'s
# second run recorded a refund refused `amount_too_large`; replayed, the first run's identical
# refund takes that refusal, and the second run's misses and passes L3 (`test_w03_queue_worker.py`).
COMPARE_DIFFERS = {(W3, "shared_charge")}
# Rule 5 has no replay to compare with: an agent that names its runs itself names the recorded
# runs again (W9's control scenarios, `test_w09_scope_gauntlet.py`), and a serve that refused its
# map recorded nothing (W8 `map_refused`).
NAMES_ITS_OWN_RUNS = {(W9, "control_runs"), (W9, "control_hazards")}
NOTHING_RECORDED = {(W8, "map_refused")}


@dataclass
class Replayed:
    run_id: str
    replay: Result
    compared: tuple[int, list[str]] | None  # irimi compare's exit and rows, None if no counterpart


def replays_of(run_workflow, replayed, name: str, scenario: str) -> list[Replayed]:
    """Each run of the scenario's shadow recording, replayed, with its `irimi compare`."""
    recorded = run_workflow(name, scenario, "shadow")
    if not recorded.stored().list_runs():
        return []
    found = []
    for run_id in replay_targets(recorded):
        _, replay = replayed(name, scenario, run_id=run_id)
        other = counterpart(recorded, replay, run_id)
        compared = None
        if other is not None:
            code, out, err = replay.compare_runs(run_id, other)
            assert err == [], err
            compared = (code, out[out.index("") + 1 :])
        found.append(Replayed(run_id, replay, compared))
    return found


def flag_problems(replay: Result) -> list[str]:
    """Each exchange a policy decided flagged neither `from-recording` nor `replay-miss`, or both:
    every one but telemetry and a delegated answer, which a replay never looks up."""
    return [
        line(ex)
        for ex in replay.reported
        if ex.kind != "telemetry"
        and ex.answered_by != "delegated"
        and len({FROM_RECORDING_FLAG, REPLAY_MISS_FLAG} & set(ex.flags)) != 1
    ]


def line(ex) -> str:
    return f"{ex.answered_by} {ex.request.method} {ex.request.host}{ex.request.path} {ex.flags}"


def stamp_problems(replay: Result) -> Counter:
    """The `Irimi-Answered-By` values the agent saw, less those irimi decided, and the reverse: a
    decided answer that reached the agent unstamped, or stamped otherwise. By value, since a call
    the agent logs by its first URL may be answered at a redirect's (W11) or through the reverse
    door (W9). `control` is the control endpoint's, which no exchange is (#73)."""
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
        and (ex.kind == "telemetry" or REPLAY_MISS_FLAG in ex.flags)
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
        expected = EXIT_DIFFERS.get(case, (recorded.exit_code, recorded.exit_code, ""))[:2]
        assert (recorded.exit_code, replay.exit_code) == expected, r.run_id
        # 3. Every looked-up exchange says whether the recording answered it.
        lost = flag_problems(replay)
        if case in CUT_BEFORE_THE_POLICY:
            lost = [
                x for x in lost if DECISION_FAILED_FLAG not in x or BODY_TRUNCATED_FLAG not in x
            ]
        assert lost == [], r.run_id
        # 4. Every answer irimi decided reached the agent stamped as irimi recorded it.
        assert stamp_problems(replay) == Counter(), r.run_id
        # 5. irimi compare: the replay made the writes its recording did.
        if case in NAMES_ITS_OWN_RUNS:
            assert r.compared is None
        else:
            assert r.compared is not None, r.run_id
            code, rows = r.compared
            cut = any(row.startswith("  ? A ") for row in rows)
            same = all(row.startswith(("  = ", "  ? A ", "    B ")) for row in rows[:-2])
            totals = " · 0 changed · 0 only in A · 0 only in B" in rows[-1]
            if case not in COMPARE_DIFFERS:
                assert (code, same, totals) == (1 if cut else 0, True, True), (r.run_id, rows)
        # 6. Nothing else reaches the fake internet.
        assert unaccounted(replay, spec.leaks) == [], r.run_id
        if case not in REACHES_THE_INTERNET:
            assert replay.internet.requests() == [], r.run_id
            # 7. And a recording its replay could answer whole is answered whole: nothing misses.
            missed = [line(ex) for ex in replay.reported if REPLAY_MISS_FLAG in ex.flags]
            assert missed == [], r.run_id
    # Each exception is still needed.
    if case in REACHES_THE_INTERNET:
        assert any(r.replay.internet.requests() for r in replays)
    if case in CUT_BEFORE_THE_POLICY:
        assert any(flag_problems(r.replay) for r in replays)
    if case in COMPARE_DIFFERS:
        assert any(r.compared and " · 1 changed · " in r.compared[1][-1] for r in replays)


def test_the_tables_name_only_scenarios_that_exist():
    named = (
        set(REACHES_THE_INTERNET)
        | CUT_BEFORE_THE_POLICY
        | set(EXIT_DIFFERS)
        | COMPARE_DIFFERS
        | NAMES_ITS_OWN_RUNS
        | NOTHING_RECORDED
    )
    assert named <= set(CASES)
