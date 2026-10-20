"""W8 `orchestrator`: nested sub-agent triggers, and an internal service irimi may or may not map.

The labels are the calls in `examples/workflows/w08_orchestrator/agent.py`.
"""

import pytest
import yaml

from examples.workflows.harness.run import check_invariants, compare, rows_not_shown
from examples.workflows.w08_orchestrator.scenarios import DOWN_TARGET, SUBAGENT_MAP, UNJUSTIFIED_MAP
from irimi.exchange import Exchange
from irimi.servicemap.loader import MapError, parse_service
from irimi.trace import UNATTRIBUTED, ToolCall

W = "w08_orchestrator"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi (#77).
MODES = ("shadow", "serve")
AGENT = "examples.workflows.w08_orchestrator.agent"
HOST = "subagent.internal"


def decision(result):
    return {k: v for k, v in result.one("decision").items() if k not in ("event", "t")}


@pytest.mark.parametrize("mode", MODES)
def test_an_unmapped_services_post_read_is_faked_and_the_agent_takes_another_branch(
    run_workflow, mode
):
    """No map claims the internal service, so its GET forwards and every POST is answered with the
    L0 echo - including `/quote`, which is a read. The echo has no `total`, so the orchestrator
    cannot price the basket and never reaches its reservation: the shadow run exercised a
    different path from production, and nothing in irimi's output says so beyond `unclassified`."""
    shadow = run_workflow(W, "no_map", mode)
    bare = run_workflow(W, "no_map", "bare")
    assert shadow.by_label() == {
        "inventory": (200, None),
        "quote": (200, "fake-L0"),
        "charges": (200, None),
    }
    quote_line = "fake-L0   unknown   POST subagent.internal/quote -> 200"
    assert f"{quote_line}  [unclassified, fidelity:L0]" in shadow.exchange_lines()
    # The echo reflects what was posted and mints a timestamp; it prices nothing.
    quote = shadow.events("quote")[0]["doc"]
    assert quote["skus"] == ["SKU-A", "SKU-B"] and "total" not in quote
    assert decision(shadow) == {"reserved": False, "reason": "no_price"}
    # Bare, the same agent priced the basket and reserved it.
    assert decision(bare) == {
        "reserved": True,
        "reason": "ok",
        "total": 2000,
        "reservation": "rsv_0001",
    }
    assert [r.path for r in shadow.internet.requests(HOST)] == ["/inventory"]


@pytest.mark.parametrize("mode", MODES)
def test_a_map_naming_the_post_read_forwards_it_and_fakes_the_reservation(run_workflow, mode):
    shadow = run_workflow(W, "with_map", mode)
    assert shadow.by_label() == {
        "inventory": (200, None),
        "quote": (200, None),
        "charges": (200, None),
        "reserve": (200, "fake-L0"),
    }
    lines = shadow.exchange_lines()
    assert "live      read      POST subagent.internal/quote -> 200" in lines
    assert "fake-L0   write     POST subagent.internal/reservations -> 200  [fidelity:L0]" in lines
    # The route's `ids:` mints the reservation id the agent then holds on to.
    reservation = decision(shadow)["reservation"]
    assert reservation.startswith("rsv_") and reservation != "rsv_0001"
    assert decision(shadow)["reserved"] is True
    assert "  ○ POST subagent.internal/reservations  unvalidated (L0)" in shadow.summaries()
    # The quote reached the service; the reservation did not.
    assert [(r.method, r.path) for r in shadow.internet.requests(HOST)] == [
        ("GET", "/inventory"),
        ("POST", "/quote"),
    ]


def test_a_post_read_without_persists_false_is_refused_and_nothing_runs(run_workflow):
    """THE SCOPE RULE at load time: in an `honest` map, `kind: read` on POST forwards an unsafe
    method, so the route must say `persists: false` and why. Without it irimi refuses the map,
    starts no proxy and runs no agent (it prints the refusal on stderr)."""
    # The refusal is THE SCOPE RULE's, and not some other fault in the map: the same map with the
    # justification loads.
    parse_service(yaml.safe_load(SUBAGENT_MAP), "subagent.yaml")
    with pytest.raises(MapError, match="unsafe method downgrades a write.*persists: false"):
        parse_service(yaml.safe_load(UNJUSTIFIED_MAP), "subagent.yaml")
    # Under serve mode the same refusal ends serve before it listens (#77): exit 1, no banner, and
    # so no agent is started beside it.
    for mode in MODES:
        shadow = run_workflow(W, "map_refused", mode)
        assert shadow.exit_code == 1
        assert shadow.irimi == []
        assert shadow.obs == []
        assert shadow.internet.requests() == []
        # Refused before the store is opened (#70): no run, no store, not even a redaction key.
        # The CA is the harness's own, made before irimi starts.
        assert sorted(path.name for path in shadow.home.iterdir()) == ["ca"]
    assert run_workflow(W, "map_refused", "bare").exit_code == 0


@pytest.mark.parametrize("mode", MODES)
def test_irimi_strips_its_run_header_at_the_hop_and_only_the_agents_own_header_survives(
    run_workflow, mode
):
    """Attribution across a service hop is an open design question: irimi strips `Irimi-Run`
    from everything it forwards (#67), so the internal service cannot tell which run called it
    unless the agent carries the id itself. This pins both halves, under `irimi shadow -- <cmd>`
    and under serve mode (#77), where the internal service is the callee a sidecar sits beside."""
    shadow = run_workflow(W, "run_header_across_hop", mode)
    run_id = shadow.events("run.start")[0]["run"]
    forwarded = shadow.internet.requests(HOST)
    assert [r.path for r in forwarded] == ["/inventory", "/quote"]
    for req in forwarded:
        assert "irimi-run" not in req.headers
        assert req.headers["x-parent-run"] == run_id
    # And yet each call reached irimi labelled: the SDK put `Irimi-Run` on it (#75), so irimi
    # stored every one in the run: the two reads it forwarded to the internal service, and the
    # reservation it faked.
    assert {c["run"] for c in shadow.calls()} == {run_id}
    stored = [
        (e.request.host, e.request.path)
        for e in shadow.stored().load_run(run_id).events
        if isinstance(e, Exchange)
    ]
    assert [path for host, path in stored if host == HOST] == [
        "/inventory",
        "/quote",
        "/reservations",
    ]
    if mode == "shadow":
        assert shadow.stored().load_run(shadow.process_run_id()).events == []
        return
    # Serve mode has no process run, and nothing the agent sent is left unattributed: every call
    # was its run's, and the run's summary names the reservation it faked.
    assert [r.run_id for r in shadow.stored().list_runs()] == [run_id]
    assert shadow.stored().load_run(UNATTRIBUTED).events == []
    assert f"  ○ POST {HOST}/reservations  unvalidated (L0)" in shadow.run_blocks()[run_id]


@pytest.mark.parametrize("mode", MODES)
def test_every_sub_agent_call_joins_its_parents_run(run_workflow, mode):
    shadow = run_workflow(W, "nested_runs", mode)
    starts = shadow.events("run.start")
    # Two orchestrator runs; the sub-agent triggers started none of their own.
    assert [e["name"] for e in starts] == ["plan_quarter_close", "plan_quarter_close"]
    runs = {e["run"] for e in starts}
    assert len(runs) == 2
    per_run = shadow.by_run()
    assert set(per_run) == runs
    for labels in per_run.values():
        assert sorted(labels) == ["charges", "inventory", "quote", "reserve"]
    assert {e["outcome"] for e in shadow.events("run.end")} == {"ok"}
    assert shadow.result()["reserved"] == 2
    # Stored the same way (#70, #74): two runs the SDK started, not four, each holding its own
    # four calls, the sub-agents' included, and the process run holds none of them. Under serve
    # mode (#77) there is no process run, nothing is left `unattributed`, and each run's block
    # names its own reservation alone.
    reader = shadow.stored()
    records = {r.run_id: r.attribution for r in reader.list_runs()}
    process = ["process"] if mode == "shadow" else []
    assert sorted(records.values()) == [*process, "sdk", "sdk"]
    assert reader.load_run(shadow.engine_run_id()).events == []
    if mode == "serve":
        assert set(shadow.run_blocks()) == runs
        for block in shadow.run_blocks().values():
            assert [line for line in block if line.startswith("  ○ ")] == [
                f"  ○ POST {HOST}/reservations  unvalidated (L0)"
            ]
    assert {run_id for run_id, a in records.items() if a == "sdk"} == runs
    entrypoint = "examples.workflows.w08_orchestrator.agent:plan_quarter_close"
    assert {(r.trigger.name, r.trigger.entrypoint) for r in shadow.sdk_runs() if r.trigger} == {
        ("plan_quarter_close", entrypoint)
    }
    for run_id, attribution in records.items():
        paths = [
            (e.request.method, e.request.host, e.request.path)
            for e in reader.load_run(run_id).events
            if isinstance(e, Exchange)
        ]
        if attribution == "process":
            assert paths == []
            continue
        assert sorted(paths) == [
            ("GET", "api.stripe.com", "/v1/charges"),
            ("GET", HOST, "/inventory"),
            ("POST", HOST, "/quote"),
            ("POST", HOST, "/reservations"),
        ]


@pytest.mark.parametrize("mode", MODES)
def test_a_tool_called_in_a_nested_trigger_is_a_call_of_its_parent_s_run(run_workflow, mode):
    """`nested_runs` with tools (#76): `inventory_agent`, a nested trigger that joins its parent's
    run, reads the planning file with a read tool, and the orchestrator records its decision with
    a write tool. Two orchestrator runs, and each holds its own two calls, the sub-agent's
    included: the run a nested trigger joined is the one its tools' calls are filed in. Bare, the
    second run's read sees the first run's recorded decision; under shadow, whose stand-in wrote
    nothing, it does not."""
    shadow = run_workflow(W, "nested_tools", mode)
    bare = run_workflow(W, "nested_tools", "bare")
    starts = shadow.events("run.start")
    assert [e["name"] for e in starts] == ["plan_quarter_close", "plan_quarter_close"]
    runs = [e["run"] for e in starts]
    assert [(t["name"].rsplit(".", 1)[1], t["ran"], t["run"]) for t in shadow.events("tool")] == [
        ("load_reorder_policy", "real", runs[0]),
        ("record_decision", "shadow", runs[0]),
        ("load_reorder_policy", "real", runs[1]),
        ("record_decision", "shadow", runs[1]),
    ]
    reader = shadow.stored()
    decisions = [
        {k: v for k, v in e.items() if k not in ("event", "t")} for e in shadow.events("decision")
    ]
    for run_id, decision in zip(runs, decisions, strict=True):
        calls = [e for e in reader.load_run(run_id).events if isinstance(e, ToolCall)]
        assert [(c.name, c.ran, c.args, c.result) for c in calls] == [
            (f"{AGENT}.load_reorder_policy", "real", {}, {"decisions_so_far": 0}),
            (f"{AGENT}.record_decision", "shadow", {"decision": decision}, {"recorded": False}),
        ]
    assert reader.load_run(shadow.engine_run_id()).events == []
    assert [line for line in shadow.summaries() if "○ tool" in line] == [
        f"  ○ tool {AGENT}.record_decision  shadow stand-in ran"
    ] * 2
    assert [e["decisions_so_far"] for e in shadow.events("policy")] == [0, 0]
    assert [e["decisions_so_far"] for e in bare.events("policy")] == [0, 1]
    assert not (shadow.state / "decisions.jsonl").exists()
    assert len((bare.state / "decisions.jsonl").read_text().splitlines()) == 2


# ---------------------------------------------------------------------- irimi compare (#79)

NOT_A_VERDICT = "a comparison, not a verdict: nothing was replayed"
PLAN = f"plan_quarter_close  {AGENT}:plan_quarter_close  agent -"
RESERVATION = f"○ POST {HOST}/reservations  unvalidated (L0)"
# What each nested scenario's two orchestrator runs both wrote: the reservation, made by the
# orchestrator or, with `--nested-reserve`, by `reservation_agent`, a nested trigger; and with
# `--record` the decision's write tool. The sub-agent's read tool is no write.
NESTED = {
    "nested_runs": [RESERVATION],
    "nested_tools": [RESERVATION, f"○ tool {AGENT}.record_decision  shadow stand-in ran"],
    "nested_write": [RESERVATION],
}


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", NESTED)
def test_irimi_compare_of_two_orchestrator_runs_holds_their_nested_triggers_writes(
    run_workflow, scenario, mode
):
    """A nested trigger joins its parent's run and starts none (#74), so there is no inner run to
    compare: what a sub-agent wrote is its parent's write (`nested_write`), as a tool it called is
    (#76). The two orchestrator runs, given the same args, are the same, write for write, and
    neither shows the other's: each holds its own reservation and no more (#79). The engine's own
    run holds none of them."""
    shadow = run_workflow(W, scenario, mode)
    a, b = (e["run"] for e in shadow.events("run.start"))
    rows = [f"  = {text}" for text in NESTED[scenario]]
    code, out, err = shadow.compare_runs(a, b)
    assert (code, err) == (0, [])
    assert out == [
        f"irimi compare · {a} vs {b} · {NOT_A_VERDICT}",
        f"  A  {PLAN}",
        f"  B  {PLAN}",
        "",
        *rows,
        "",
        f"  {len(rows)} same · 0 changed · 0 only in A · 0 only in B",
    ]
    assert rows_not_shown(shadow, a, b, out) == []
    code, out, err = shadow.compare_runs(a, shadow.engine_run_id())
    assert (code, err) == (1, [])
    assert out[-1] == f"  0 same · 0 changed · {len(rows)} only in A · 0 only in B"


@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_shows_what_a_map_changed_about_the_runs_writes(shared_store_run, mode):
    """`with_map` names the internal service's `POST /quote` a read and forwards it, so the
    orchestrator prices the basket and reserves it; `no_map` fakes the quote at L0, and the agent,
    given no price, reserves nothing. Recorded into one store, B under shadow or serve, the map is
    the whole difference: A's reservation against B's faked quote, two writes on one host by
    different routes, so not a pair (#79)."""
    mapped = shared_store_run(W, "with_map")
    unmapped = shared_store_run(W, "no_map", mode)
    assert check_invariants(unmapped) == []
    code, out, err = compare(mapped, unmapped)
    a, b = (result.one("run.start")["run"] for result in (mapped, unmapped))
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b} · {NOT_A_VERDICT}",
        f"  A  {PLAN}",
        f"  B  {PLAN}",
        "",
        f"  - {RESERVATION}",
        f"  + ○ POST {HOST}/quote  unclassified (L0)",
        "",
        "  0 same · 0 changed · 1 only in A · 1 only in B",
    ]
    assert rows_not_shown(unmapped, a, b, out) == []


@pytest.mark.parametrize("mode", MODES)
def test_a_reservation_whose_answer_target_is_down_is_answered_502_and_compared_as_printed(
    shared_store_run, run_workflow, mode
):
    """`target_down` delegates the reservation to an answer target nothing listens on (#16): irimi
    cannot reach it, the agent gets a 502 marked `delegated`, and the summary says the write went
    unanswered. Bare, the service reserves. Recorded into one store with `with_map`, B under
    shadow or serve, compare pairs the two reservations and shows the unanswered one as its
    summary printed it: a write whose target failed is still a write the agent attempted, which
    the issue's own set of actions would have dropped (#79, decision 1)."""
    bare = run_workflow(W, "target_down", "bare")
    down = run_workflow(W, "target_down", mode)
    assert bare.by_label()["reserve"] == (200, None)
    assert down.by_label()["reserve"] == (502, "delegated")
    assert decision(down) == {
        "reserved": False,
        "reason": "http_502",
        "total": 2000,
        "reservation": None,
    }
    mapped = shared_store_run(W, "with_map")
    unreached = shared_store_run(W, "target_down", mode)
    assert check_invariants(unreached) == []
    code, out, err = compare(mapped, unreached)
    a, b = (result.one("run.start")["run"] for result in (mapped, unreached))
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b} · {NOT_A_VERDICT}",
        f"  A  {PLAN}",
        f"  B  {PLAN}",
        "",
        f"  ~ A {RESERVATION}",
        f"    B ○ POST {HOST}/reservations → {DOWN_TARGET}/reservations  unanswered (target "
        "unreachable)",
        "",
        "  0 same · 1 changed · 0 only in A · 0 only in B",
    ]
    assert rows_not_shown(unreached, a, b, out) == []


@pytest.mark.parametrize("mode", MODES)
def test_a_down_target_s_closing_lines_say_the_tool_write_did_not_happen_either(run_workflow, mode):
    """`target_down` with `--record`: the reservation goes unanswered, its target down, and the
    orchestrator records its decision through the write tool's stand-in. The closing lines say the
    reservation did not reach the internal service, and now that the tool writes did not happen
    either, which before #121 only the tool's own `○ tool` line said. Under serve, the run's block,
    printed as it ends, says the same. Bare, the decision is really recorded."""
    down = run_workflow(W, "target_down_tools", mode)
    bare = run_workflow(W, "target_down_tools", "bare")
    assert down.by_label()["reserve"] == (502, "delegated")
    summary = down.summaries()
    assert [line for line in summary if line.startswith("  ○ ")] == [
        f"  ○ POST {HOST}/reservations → {DOWN_TARGET}/reservations  unanswered (target "
        "unreachable)",
        f"  ○ tool {AGENT}.record_decision  shadow stand-in ran",
    ]
    assert summary[-2:] == [
        "  These writes did not reach subagent. The tool writes did not happen.",
        f"  1 was not answered at all: {DOWN_TARGET}/reservations could not be reached, and the "
        "agent got a 502.",
    ]
    assert not (down.state / "decisions.jsonl").exists()
    assert len((bare.state / "decisions.jsonl").read_text().splitlines()) == 1


# ------------------------------------------------------------------------------- replay (#82)


@pytest.mark.parametrize("scenario", ["nested_runs", "nested_write"])
def test_replaying_the_second_of_two_identical_runs_replays_it_alone(
    run_workflow, replayed, scenario
):
    """The two orchestrator runs make the same requests. Replaying the second calls its trigger
    alone (#84): one run, the replay's, which its nested trigger joins, every read and the
    reservation answered from the second run's recording, and nothing reaching the fake internet.
    The first run is not in the replay to take those answers, as it did when a replay re-ran the
    process (#155)."""
    recorded = run_workflow(W, scenario, "shadow")
    _, second = sorted(recorded.sdk_runs(), key=lambda r: r.started_at or 0.0)
    _, replay = replayed(W, scenario, run_id=second.run_id)
    starts = [e["run"] for e in replay.events("run.start") if e["name"] == "plan_quarter_close"]
    assert starts == [replay.process_run_id()]
    assert {ex.run_id for ex in replay.reported} == {replay.process_run_id()}
    assert {"from-recording" in ex.flags for ex in replay.reported} == {True}
    assert [answered for _, _, answered in replay.answered()] == [
        "recording",
        "recording",
        "recording",
        "fake-L0",
    ]
    assert replay.internet.requests() == []
    assert replay.exit_code == 0
