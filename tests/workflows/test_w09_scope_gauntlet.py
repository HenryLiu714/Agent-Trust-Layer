"""W9 `scope_gauntlet`: what irimi answers for each classification edge, pinned per call.

The labels are the calls in `examples/workflows/w09_scope_gauntlet/agent.py`.
"""

import dataclasses
import json
import re
import sys

import pytest

import irimi
from examples.workflows.w09_scope_gauntlet.agent import (
    AGENT_VERSION,
    BIG_CHARGE,
    CHARGE,
    CUSTOMER,
    ENTRYPOINT,
    LONG_MESSAGE_CHARS,
    OVERSIZED_BODY_BYTES,
)
from examples.workflows.w09_scope_gauntlet.scenarios import BIG_DESCRIPTION_BYTES, WORKFLOW
from irimi import control, paths, report, trace
from irimi.bodies import MAX_BODY_BYTES
from irimi.exchange import BODY_TRUNCATED_FLAG, Exchange
from irimi.store import MAX_STORED_BODY
from irimi.trace import MAX_ERROR_MESSAGE, ErrorInfo, RunRecord, ToolCall, Trigger

W = "w09_scope_gauntlet"


def test_the_scope_rule_fakes_every_method_no_route_names(run_workflow):
    shadow = run_workflow(W, "verbs", "shadow")
    assert shadow.by_label() == {
        "get_charge": (200, None),
        "head_charges": (200, None),
        "options_charges": (404, None),
        "delete_customer": (200, "fake-L0"),
        "patch_customer": (200, "fake-L0"),
        "put_charge": (200, "fake-L0"),
        "unrouted_post": (200, "fake-L0"),
        "cancel_empty_body": (200, "fake-L1"),
        "reaction_no_fixture": (200, "fake-L0"),
    }
    lines = shadow.exchange_lines()
    for verb, path in [
        ("DELETE", "/v1/customers/cus_GAUNTLET"),
        ("PATCH", "/v1/customers/cus_GAUNTLET"),
        ("PUT", "/v1/charges/ch_GAUNTLET"),
        ("POST", "/v1/subscriptions"),
    ]:
        assert (
            f"fake-L0   unknown   {verb} api.stripe.com{path} -> 200  [unclassified, fidelity:L0]"
            in lines
        )
    # The L0 floor: a mapped write whose route ships no `fixture:` is still a write, and faked.
    assert "fake-L0   write     POST slack.com/api/reactions.add -> 200  [fidelity:L0]" in lines
    assert "  ○ add :eyes: to a message in C0GAUNT  unvalidated (L0)" in shadow.summary()
    # Bare, the same calls really reach Stripe; the fake refuses the ones it does not know.
    bare = run_workflow(W, "verbs", "bare")
    assert [(r.method, r.path) for r in bare.internet.writes()] == [
        ("DELETE", "/v1/customers/cus_GAUNTLET"),
        ("PATCH", "/v1/customers/cus_GAUNTLET"),
        ("PUT", "/v1/charges/ch_GAUNTLET"),
        ("POST", "/v1/subscriptions"),
        ("POST", "/v1/payment_intents/pi_GAUNTLET/cancel"),
        ("POST", "/api/reactions.add"),
    ]


def test_one_idempotency_key_is_one_write_and_a_reused_key_is_stripes_own_error(run_workflow):
    shadow = run_workflow(W, "idempotency", "shadow")
    bare = run_workflow(W, "idempotency", "bare")
    # The agent sees the same statuses either way: irimi's idempotency is Stripe's.
    assert [s for s, _ in shadow.by_label().values()] == [s for s, _ in bare.by_label().values()]
    assert shadow.by_label()["refund_key_reused"] == (400, "fake-L1")
    # A replayed key hands back the first answer, minted id and all, and says it is a replay
    # (#46); a keyless retry mints a new id.
    ids = {e["label"]: (e["id"], e["replayed"]) for e in shadow.events("refund")}
    first_id = ids["refund_first"][0]
    assert first_id and first_id.startswith("re_")
    assert ids["refund_first"] == (first_id, None)
    assert ids["refund_same_key"] == (first_id, "true")
    assert ids["refund_key_reused"] == (None, None)
    assert len({ids["refund_first"][0], ids["refund_nokey_1"][0], ids["refund_nokey_2"][0]}) == 3
    refund = "fake-L1   write     POST api.stripe.com/v1/refunds"
    lines = shadow.exchange_lines()
    assert f"{refund} -> 200  [fidelity:L1, idempotent-replay]" in lines
    assert f"{refund} -> 400  [fidelity:L1, idempotency-conflict]" in lines
    # A replayed key issues no second L3 read; each keyless refund issues its own.
    engine_reads = [r for r in shadow.internet.requests() if r.path == "/v1/charges/ch_GAUNTLET"]
    assert len(engine_reads) == 3
    # The summary counts the two keyless refunds as two writes: the duplicate Phase 4 must flag.
    summary = "\n".join(shadow.summary())
    assert summary.count("refund $0.50 on ch_GAUNTLET") == 2
    assert "✗ refund 200 on ch_GAUNTLET  would fail: idempotency_error" in summary


def test_gzip_chunked_and_oversized_bodies_are_still_faked(run_workflow):
    shadow = run_workflow(W, "bodies", "shadow")
    assert shadow.by_label() == {
        "refund_gzip": (200, "fake-L1"),
        "refund_chunked": (200, "fake-L1"),
        "customer_3mb": (200, "fake-L1"),
    }
    summary = "\n".join(shadow.summary())
    # irimi read the refund amount out of the gzip body and out of the chunked one.
    assert "refund $3.00 on ch_GAUNTLET" in summary
    assert "refund $4.00 on ch_GAUNTLET" in summary
    # The 3 MB update and irimi's 3 MB fake of the customer are under MAX_STORED_BODY, so both
    # are stored whole, as blobs, and not flagged cut (#70).
    [update] = [
        e
        for e in shadow.stored_events()
        if isinstance(e, Exchange) and e.request.path.startswith("/v1/customers/")
    ]
    assert update.response is not None
    assert 3_000_000 < len(update.request.body) < MAX_STORED_BODY
    assert 3_000_000 < len(update.response.body) < MAX_STORED_BODY
    assert BODY_TRUNCATED_FLAG not in update.flags


def test_a_read_past_the_body_limit_leaves_the_check_and_the_overlay_unable_to_say(run_workflow):
    """A charge too big to parse (`MAX_BODY_BYTES`): irimi cannot check the refund against it (L2)
    or show the refund in it when the agent reads it back, and the summary says both."""
    bare = run_workflow(W, "big_reads", "bare")
    [charge_read] = [r for r in bare.internet.requests() if r.method == "GET"]
    assert charge_read.path == f"/v1/charges/{BIG_CHARGE}"
    shadow = run_workflow(W, "big_reads", "shadow")
    assert shadow.by_label() == {"refund_big": (200, "fake-L1"), "get_big": (200, None)}
    # Bare, the read-back shows the refund; under shadow the overlay could not add it.
    assert [e["amount_refunded"] for e in bare.events("big_charge")] == [700]
    assert [e["amount_refunded"] for e in shadow.events("big_charge")] == [0]
    assert shadow.exchange_lines() == [
        # LOOKS WRONG: the L3 read was answered 200, but irimi refused the body for its size and
        # records the read with no response and no flag, the same line as a read that got nothing.
        f"live      read      GET api.stripe.com/v1/charges/{BIG_CHARGE} -> -",
        "fake-L1   write     POST api.stripe.com/v1/refunds -> 200  [fidelity:L1]",
        f"live      read      GET api.stripe.com/v1/charges/{BIG_CHARGE} -> 200",
    ]
    summary = shadow.summary()
    assert f"  ○ refund 700 on {BIG_CHARGE}  unvalidated (L2)" in summary
    assert f"    ↳ GET /v1/charges/{BIG_CHARGE} did not show it  live (partial)" in summary
    # LOOKS WRONG: stored the same way (#70) - an engine read with no response and no flag, so a
    # stored run cannot tell "too big" apart from "no answer" either.
    assert engine_reads(shadow) == [(None, ())]


def engine_reads(result) -> list[tuple[object, tuple[str, ...]]]:
    """`(response, flags)` of each engine-issued read the store holds."""
    return [
        (e.response, e.flags)
        for e in result.stored_events()
        if isinstance(e, Exchange) and e.issued_by == "engine"
    ]


def test_the_big_charge_is_past_irimis_body_limit():
    """The scenario above means something only while its charge is past the limit."""
    assert BIG_DESCRIPTION_BYTES > MAX_BODY_BYTES


@pytest.mark.parametrize("scenario", sorted(WORKFLOW.scenarios))
def test_no_decision_ever_fails(run_workflow, scenario):
    """The never-raise hook: a decision that raised would answer 502 `decision-failed`. No edge
    in the gauntlet may reach that path, and every agent call got an HTTP answer."""
    shadow = run_workflow(W, scenario, "shadow")
    assert shadow.exchange_lines()
    assert not [line for line in shadow.exchange_lines() if "decision-failed" in line]
    assert all(isinstance(c.get("status"), int) for c in shadow.calls())


def test_irimis_own_headers_sent_by_the_agent_never_leave(run_workflow):
    # Bare, the agent's forged headers reach the service: the agent really sends them.
    bare = run_workflow(W, "headers", "bare")
    assert [
        sorted(h for h in r.headers if h.startswith("irimi-")) for r in bare.internet.requests()
    ] == [
        ["irimi-rewrote"],
        ["irimi-run"],
    ]
    shadow = run_workflow(W, "headers", "shadow")
    assert shadow.by_label() == {"forged_rewrote": (200, None), "bad_run_id": (200, None)}
    assert [r.path for r in shadow.internet.requests()] == [
        "/v1/refunds",
        "/v1/charges/ch_GAUNTLET",
    ]
    for req in shadow.internet.requests():
        assert "irimi-rewrote" not in req.headers
        assert "irimi-run" not in req.headers


def test_every_spelling_of_the_proxys_own_address_is_the_reverse_door(run_workflow):
    """`127.0.0.1` (sent direct, as irimi's NO_PROXY says), `127.1` and `0.0.0.0` (sent through the
    proxy to itself) all name irimi's listener, so each refund is taken through the reverse door
    and faked. Missing a spelling would forward the request to the proxy itself, or past it."""
    shadow = run_workflow(W, "self_addressed", "shadow")
    assert {k: v for k, v in shadow.by_label().items() if k.startswith("door_")} == {
        "door_127.0.0.1": (200, "fake-L1"),
        "door_127.1": (200, "fake-L1"),
        "door_0.0.0.0": (200, "fake-L1"),
    }
    assert shadow.internet.requests() == []
    # The door relays over https, and the fake internet speaks only http, so irimi's L3 read of
    # the charge gets no answer here and each refund is faked at L2. The failed engine read prints
    # with no response and no flag, as the oversized one does in `big_reads`.
    read = "live      read      GET api.stripe.com/v1/charges/ch_GAUNTLET -> -"
    write = "fake-L1   write     POST api.stripe.com/v1/refunds -> 200  [fidelity:L1]"
    assert shadow.exchange_lines() == [read, write] * 3
    refunds = [line for line in shadow.summary() if "○" in line]
    assert refunds == ["  ○ refund 600 on ch_GAUNTLET  unvalidated (L2)"] * 3
    assert engine_reads(shadow) == [(None, ())] * 3  # stored as `big_reads` stores its one (#70)


def test_the_control_endpoint_answers_at_every_spelling_and_is_never_an_exchange(run_workflow):
    """The same three spellings reach irimi's control endpoint (#73), direct and through the proxy
    to itself: health answers 200 and a path no route names 404, both stamped `control`. Neither
    is forwarded, decided or recorded, so the exchange lines above are the refunds' alone."""
    shadow = run_workflow(W, "self_addressed", "shadow")
    spellings = ("127.0.0.1", "127.1", "0.0.0.0")
    assert {k: v for k, v in shadow.by_label().items() if not k.startswith("door_")} == {
        **{f"health_{s}": (200, "control") for s in spellings},
        **{f"nope_{s}": (404, "control") for s in spellings},
    }
    assert not [line for line in shadow.exchange_lines() if "/_irimi/" in line]
    assert shadow.internet.requests() == []
    # Bare, there is no irimi: the same calls reach the fake internet's port, which serves no such
    # host, and the agent carries on to the same exit code.
    bare = run_workflow(W, "self_addressed", "bare")
    assert {k: v for k, v in bare.by_label().items() if not k.startswith("door_")} == {
        **{f"health_{s}": (502, None) for s in spellings},
        **{f"nope_{s}": (502, None) for s in spellings},
    }
    assert bare.exit_code == shadow.exit_code == 0


# -- the control endpoint, called as the SDK will call it (#73) -----------------------------------
#
# `control_runs` and `control_hazards` do the SDK's job by hand, with posts the test controls
# exactly: what `irimi shadow` stores for a run reported over the control endpoint, and what each
# refusal answers. The SDK's own posts (#74) are pinned by every other workflow.

# The runs `control_runs` posts in full: run id, the tag its labels start with, the path its one
# labelled read GETs.
CONTROL_RUNS = {
    "ctl-direct": ("direct", f"/v1/charges/{CHARGE}"),
    "ctl-proxied": ("proxied", f"/v1/charges/{CHARGE}"),
    "ctl-thread-1": ("thread_1", f"/v1/charges/{CHARGE}"),
    "ctl-thread-2": ("thread_2", f"/v1/customers/{CUSTOMER}"),
}
STEPS = ("start", "tool_1", "read", "tool_2", "end")
REFUSALS = {
    "refuse_get": (405, "/_irimi/runs/ctl-refused-get/start takes POST, not GET"),
    "refuse_route": (404, "no control route '/_irimi/runs/ctl-refused-route/nope'"),
    "refuse_dotdot": (400, "'../x' cannot name a run"),
    "refuse_unattributed": (400, "'unattributed' cannot name a run"),
    "refuse_nan": (400, "the body is not JSON: NaN is not a finite number"),
    "refuse_missing": (400, "missing required field 'trigger'"),
    "refuse_run_id": (400, "'run_id' comes from the path, not the body"),
    "refuse_big": (
        413,
        f"the body is {OVERSIZED_BODY_BYTES + 11} bytes, over the "
        f"{control.MAX_CONTROL_BODY} allowed",
    ),
}
ELAPSED = re.compile(r" · \d+\.\ds · ")


def posted(result, label: str) -> dict:
    """The body the agent posted to the control endpoint under `label`."""
    [event] = [e for e in result.events("posted") if e["label"] == label]
    return event["body"]


def sdk_record(result, run_id: str, start: str, end: str | None) -> RunRecord:
    """The record a run started by the post labelled `start` and ended by the one labelled `end`
    is stored as: every field the SDK may say, as it said it, and the rest irimi's."""
    began = posted(result, start)
    ended = posted(result, end) if end else {"ended_at": None, "outcome": None, "error": None}
    trigger = began["trigger"]
    error = ended["error"]
    return RunRecord(
        schema_version=trace.SCHEMA_VERSION,
        run_id=run_id,
        mode="shadow",
        attribution="sdk",
        trigger=Trigger(
            trigger["name"], trigger["entrypoint"], trigger["args"], trigger["replayable"]
        ),
        agent_version=began["agent_version"],
        engine_version=irimi.__version__,
        sdk_version=began["sdk_version"],
        started_at=began["started_at"],
        ended_at=ended["ended_at"],
        outcome=ended["outcome"],
        error=None if error is None else ErrorInfo(error["type"], error["message"]),
        exit_code=None,
    )


def stored_tool_call(result, run_id: str, label: str) -> ToolCall:
    """The tool call posted under `label`, as the store is to hold it: its run id from the path."""
    body = posted(result, label)
    error = body["error"]
    return ToolCall(
        tool_call_id=body["tool_call_id"],
        run_id=run_id,
        name=body["name"],
        kind=body["kind"],
        ran=body["ran"],
        args=body["args"],
        result=body["result"],
        error=None if error is None else ErrorInfo(error["type"], error["message"]),
        started_at=body["started_at"],
        ended_at=body["ended_at"],
    )


def read_line(path: str) -> str:
    return f"live      read      GET api.stripe.com{path} -> 200"


def listed(result) -> list[list[str]]:
    """`irimi runs list`, each line's fields, its start time and duration masked (#72)."""
    code, out, err = result.runs("list")
    assert (code, err) == (0, [])
    return sorted([f[0], "<started>", "<elapsed>", *f[3:]] for f in (o.split("  ") for o in out))


def shown(result, run_id: str) -> list[str]:
    """`irimi runs show <run_id>`, its elapsed seconds masked (#72)."""
    code, out, err = result.runs("show", run_id)
    assert (code, err) == (0, [])
    return [ELAPSED.sub(" · <elapsed> · ", line) for line in out]


def test_the_sdks_posts_are_answered_by_irimi_and_its_reads_go_live(run_workflow):
    """Every control post is answered `control`, a refusal with its documented status; every read
    labelled `Irimi-Run` is forwarded live. Bare, there is no irimi and no `IRIMI_CONTROL`: the
    agent posts to the same URL shape at its proxy, the fake internet, which serves no such host,
    and exits as it does under shadow."""
    shadow = run_workflow(W, "control_runs", "shadow")
    bare = run_workflow(W, "control_runs", "bare")
    posts = [f"{tag}_{step}" for tag, _ in CONTROL_RUNS.values() for step in STEPS]
    reads = [label for label in posts if label.endswith("_read")]
    errors = ["error_start", "error_tool", "error_end"]
    controls = [label for label in posts if label not in reads] + errors
    assert shadow.by_label() == {
        "health_before": (200, "control"),
        **dict.fromkeys(controls, (204, "control")),
        **dict.fromkeys(reads, (200, None)),
        **{label: (status, "control") for label, (status, _) in REFUSALS.items()},
        "health_after": (200, "control"),
    }
    assert bare.by_label() == {
        **dict.fromkeys(["health_before", *controls, *REFUSALS], (502, None)),
        **dict.fromkeys(reads, (200, None)),
        "health_after": (502, None),
    }
    assert bare.exit_code == shadow.exit_code == 0
    assert [e["given"] for e in shadow.events("control_base")] == [True]
    assert [e["given"] for e in bare.events("control_base")] == [False]
    # irimi printed a line for each labelled read and for nothing it answered itself, and the fake
    # internet saw those reads and nothing else, none of them carrying the label.
    assert sorted(shadow.exchange_lines()) == sorted(
        read_line(path) for _, path in CONTROL_RUNS.values()
    )
    assert sorted((r.method, r.path) for r in shadow.internet.requests()) == sorted(
        ("GET", path) for _, path in CONTROL_RUNS.values()
    )
    assert [r for r in shadow.internet.requests() if "irimi-run" in r.headers] == []


def test_a_run_posted_over_the_control_endpoint_is_stored_as_an_sdk_run(run_workflow):
    """#73's headline, through the real `irimi shadow`: each run is stored `attribution: "sdk"`
    with the trigger, versions and times it posted and `outcome: ok`, and holds its two tool calls
    with its labelled read between them - whether it posted direct (`ctl-direct`) or through the
    proxy to itself (`ctl-proxied`). `irimi runs show` prints it so."""
    shadow = run_workflow(W, "control_runs", "shadow")
    reader = shadow.stored()
    for run_id, (tag, path) in CONTROL_RUNS.items():
        stored = reader.load_run(run_id)
        assert stored.record == sdk_record(shadow, run_id, f"{tag}_start", f"{tag}_end")
        before, read, after = stored.events
        assert before == stored_tool_call(shadow, run_id, f"{tag}_tool_1")
        assert after == stored_tool_call(shadow, run_id, f"{tag}_tool_2")
        assert isinstance(read, Exchange)
        assert (read.run_id, report.exchange_line(read)) == (run_id, read_line(path))
        # In time as well as in `seq` (#73): the run's start, its first tool call, the read, its
        # second tool call and its end, each clock read after the last.
        times = [stored.record.started_at, before.ended_at, read.started_at, read.ended_at]
        times += [after.started_at, stored.record.ended_at]
        assert times == sorted(times)
        assert shown(shadow, run_id) == [
            f"run: {run_id}",
            "attribution: sdk",
            f"trigger: {run_id}",
            f"entrypoint: {ENTRYPOINT}:{run_id}",
            f'args: {{"run":"{run_id}"}}',
            f"agent version: {AGENT_VERSION}",
            "outcome: ok",
            "",
            "real      read      tool look_up -> ok",
            read_line(path),
            "real      read      tool summarise -> ok",
            "",
            f"irimi shadow · run {run_id} · 1 exchange · <elapsed> · backstop: none (Phase 4)",
            "",
            "  api.stripe.com  1 read",
            "",
            "  1 exchange · 1 live · 0 delegated · 0 virtualized",
        ]


def test_two_runs_posted_at_once_do_not_mix(run_workflow):
    """The Phase 3 exit test, "two concurrent requests don't mix", on the control endpoint. Two
    threads in lockstep: both start, both post a tool call, both read, both post another, both end,
    so each step of one run is between steps of the other on the wire and in the store's queue.
    Each run still holds only its own: its own tool calls, and the read it labelled, which the
    two runs make of different objects."""
    shadow = run_workflow(W, "control_runs", "shadow")
    threads = [label for label in (c["label"] for c in shadow.calls()) if "thread" in label]
    assert [label.split("_", 2)[2] for label in threads] == [s for s in STEPS for _ in (1, 2)]
    handed = [e.run_id for e in shadow.handed_events if e.run_id.startswith("ctl-thread-")]
    assert sorted(handed[:2]) == sorted(handed[2:4]) == sorted(handed[4:])
    assert sorted(handed[:2]) == ["ctl-thread-1", "ctl-thread-2"]
    reader = shadow.stored()
    for run_id in ("ctl-thread-1", "ctl-thread-2"):
        tag, path = CONTROL_RUNS[run_id]
        assert [e.run_id for e in reader.load_run(run_id).events] == [run_id] * 3
        assert shown(shadow, run_id)[8:11] == [
            "real      read      tool look_up -> ok",
            read_line(path),
            "real      read      tool summarise -> ok",
        ]


def test_a_run_that_ends_in_error_keeps_its_error_cut_to_what_a_trace_keeps(run_workflow):
    """A run posts a tool call that raised and an end in error, each message past the 1000
    characters `ErrorInfo` promises (#68): both are stored, cut to that length, not refused."""
    shadow = run_workflow(W, "control_runs", "shadow")
    stored = shadow.stored().load_run("ctl-error")
    tool = stored_tool_call(shadow, "ctl-error", "error_tool")
    run_message = posted(shadow, "error_end")["error"]["message"]
    tool_message = tool.error.message
    assert (len(run_message), len(tool_message)) == (LONG_MESSAGE_CHARS + 4, LONG_MESSAGE_CHARS + 5)
    cut = ErrorInfo("builtins.RuntimeError", run_message[:MAX_ERROR_MESSAGE])
    assert stored.record == dataclasses.replace(
        sdk_record(shadow, "ctl-error", "error_start", "error_end"), error=cut
    )
    assert stored.events == [
        dataclasses.replace(
            tool, error=ErrorInfo("builtins.ValueError", tool_message[:MAX_ERROR_MESSAGE])
        )
    ]
    assert shown(shadow, "ctl-error") == [
        "run: ctl-error",
        "attribution: sdk",
        "trigger: ctl-error",
        f"entrypoint: {ENTRYPOINT}:ctl-error",
        'args: {"run":"ctl-error"}',
        f"agent version: {AGENT_VERSION}",
        "outcome: error",
        f"error: builtins.RuntimeError: run {'y' * (MAX_ERROR_MESSAGE - 4)}",
        "",
        "real      read      tool charge_card -> raised builtins.ValueError",
        "",
        "irimi shadow · run ctl-error · 0 exchanges · <elapsed> · backstop: none (Phase 4)",
        "",
        "  0 exchanges · 0 live · 0 delegated · 0 virtualized",
    ]


def test_each_refusal_answers_its_status_and_line_and_makes_no_run(run_workflow):
    """Every refusal #73 documents, through the real proxy: the wrong method (405), no such route
    (404), a run id the store cannot use (400 for `../x` and for `unattributed`), `NaN`, a missing
    field, a tool call naming its own run (400) and a body past 2 MiB (413). Each names its own
    run, and none of them is a directory, or an unattributed event, afterwards."""
    shadow = run_workflow(W, "control_runs", "shadow")
    assert {e["label"]: e["body"] for e in shadow.events("refusal")} == {
        label: {"error": line} for label, (_, line) in REFUSALS.items()
    }
    root = shadow.home / paths.STORE_DIR_NAME
    assert sorted(p.name for p in root.iterdir()) == ["blobs", "runs"]
    assert sorted(p.name for p in (root / "runs").iterdir()) == sorted(
        [shadow.process_run_id(), *CONTROL_RUNS, "ctl-error"]
    )
    assert shadow.stored().load_run(trace.UNATTRIBUTED).events == []


def test_health_reports_the_engine_and_what_the_store_wrote(run_workflow):
    """`GET /_irimi/health` before the first run and after the last, each once the store had
    nothing queued: the engine's version, `serve: false` (no `--serve` until #77), and the store's
    counters, `written` grown from nothing to every event line the runs hold."""
    shadow = run_workflow(W, "control_runs", "shadow")
    before, after = (e["body"] for e in shadow.events("health"))
    health = {"engine_version": irimi.__version__, "serve": False}
    assert before == {**health, "store": {"queued": 0, "written": 0, "dropped": 0}}
    assert after == {**health, "store": {"queued": 0, "written": 13, "dropped": 0}}
    assert len(shadow.stored_events()) == 13


def test_a_run_ended_over_the_control_endpoint_no_longer_lists_as_incomplete(run_workflow):
    """#72 listed every run but the process run `incomplete`, because nothing could end one. Each
    run here posted its end, so `irimi runs list` says how it ended. A tool call counts as no
    exchange."""
    shadow = run_workflow(W, "control_runs", "shadow")
    process = shadow.process_run_id()
    argv0 = shadow.stored().load_run(process).record.trigger.name
    assert listed(shadow) == sorted(
        [
            *(
                [run_id, "<started>", "<elapsed>", "ok", "sdk", run_id, "1 exchanges", "0 writes"]
                for run_id in CONTROL_RUNS
            ),
            ["ctl-error", "<started>", "<elapsed>", "error", "sdk", "ctl-error"]
            + ["0 exchanges", "0 writes"],
            [process, "<started>", "<elapsed>", "ok", "process", argv0, "0 exchanges", "0 writes"],
        ]
    )


def test_a_start_re_posted_after_its_end_reopens_the_run(run_workflow):
    """The SDK must never re-post a start. Made anyway, it is accepted with a 204."""
    shadow = run_workflow(W, "control_hazards", "shadow")
    # LOOKS WRONG (#97): the second start is accepted, and replaces the ended run's record
    # whole, so its end, its outcome and its first trigger are gone and the run lists as
    # `incomplete` again, though its read is kept. A start for a run that has already ended could
    # be refused instead, as a start for the process run is.
    assert shadow.by_label() == {
        "restarted_start": (204, "control"),
        "restarted_read": (200, None),
        "restarted_end": (204, "control"),
        "restarted_again": (204, "control"),
        "process_start": (400, "control"),
        "process_tool": (204, "control"),
        "process_end": (400, "control"),
    }
    stored = shadow.stored().load_run("ctl-restarted")
    assert stored.record == sdk_record(shadow, "ctl-restarted", "restarted_again", None)
    assert [report.event_line(e) for e in stored.events] == [read_line(f"/v1/charges/{CHARGE}")]
    [line] = [f for f in listed(shadow) if f[0] == "ctl-restarted"]
    assert line[3:] == ["incomplete", "sdk", "ctl-restarted again", "1 exchanges", "0 writes"]


def test_the_process_runs_start_and_end_are_irimis_and_only_its_tool_calls_are_taken(
    run_workflow,
):
    """`IRIMI_RUN` names the run irimi starts and ends for the agent's process. A start or an end
    posted for it is refused, so its record stays irimi's: `process`, the agent's argv and its
    exit. A tool call posted to it is taken, and the process run holds it."""
    shadow = run_workflow(W, "control_hazards", "shadow")
    process = shadow.process_run_id()
    assert [e["id"] for e in shadow.events("process_run")] == [process]
    refused = {"error": f"{process!r} is irimi's own run, which the SDK may not start or end"}
    assert {e["label"]: e["body"] for e in shadow.events("answer")} == {
        "process_start": refused,
        "process_tool": None,
        "process_end": refused,
    }
    stored = shadow.stored().load_run(process)
    assert stored.events == [stored_tool_call(shadow, process, "process_tool")]
    assert shown(shadow, process) == [
        f"run: {process}",
        "attribution: process",
        f"trigger: {sys.executable}",
        "entrypoint: -",
        f'args: {{"argv":{json.dumps(argv(shadow), separators=(",", ":"))}}}',
        "agent version: -",
        "outcome: ok",
        "exit code: 0",
        "",
        "real      read      tool look_up -> ok",
        "",
        f"irimi shadow · run {process} · 0 exchanges · <elapsed> · backstop: none (Phase 4)",
        "",
        "  0 exchanges · 0 live · 0 delegated · 0 virtualized",
    ]
    # Bare, there is no process run: the agent posts for a stand-in id, to the fake internet.
    bare = run_workflow(W, "control_hazards", "bare")
    assert [e["id"] for e in bare.events("process_run")] == ["no-process-run"]


def argv(result) -> list[str]:
    """The argv the harness started the agent with: the process run's trigger args (#70)."""
    workflow = WORKFLOW.scenarios[result.scenario]
    return [sys.executable, "-m", "examples.workflows.launch", WORKFLOW.module, *workflow.argv]


def test_an_unmapped_hosts_posts_are_faked_even_when_they_are_reads(run_workflow):
    shadow = run_workflow(W, "unmapped_hosts", "shadow")
    assert shadow.by_label() == {
        # A GraphQL query is a read, but irimi cannot tell it from a mutation: both are faked.
        "graphql_query": (200, "fake-L0"),
        "graphql_mutation": (200, "fake-L0"),
        "unmapped_get": (200, None),
        "slack_unrouted": (200, "fake-L0"),
    }
    assert (
        "fake-L0   unknown   POST slack.com/api/chat.delete -> 200  [unclassified, fidelity:L0]"
        in shadow.exchange_lines()
    )
    assert [r.path for r in shadow.internet.requests()] == ["/health"]


def test_a_get_that_writes_escapes_and_the_scenario_says_so(run_workflow):
    shadow = run_workflow(W, "get_that_writes", "shadow")
    assert [(r.method, r.host, r.path) for r in shadow.internet.writes()] == [
        ("GET", "legacy.internal", "/api/delete_user")
    ]
    assert (
        "live      read      GET legacy.internal/api/delete_user -> 200" in shadow.exchange_lines()
    )
