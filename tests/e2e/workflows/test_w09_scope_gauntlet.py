"""W9 `scope_gauntlet`: what irimi answers for each classification edge, pinned per call.

The labels are the calls in `tests/e2e/workflows/agents/w09_scope_gauntlet/agent.py`."""

import dataclasses
import hashlib
import json
import re
import shlex
import sys

import pytest

import irimi
from irimi.config import paths
from irimi.core.exchange import Exchange, Flag
from irimi.core.limits import (
    MAX_BODY_BYTES,
    MAX_CONTROL_BODY,
    MAX_ERROR_MESSAGE,
    MAX_ERROR_TYPE,
    MAX_STORED_BODY,
)
from irimi.protocol import trace
from irimi.protocol.trace import ErrorInfo, RunEnd, RunRecord, ToolCall, Trigger
from irimi.record import redact
from irimi.record.store.layout import RUNS_DIR, SERVE_LOCK_FILE
from irimi.report.runs import MAX_TRIGGER_SHOWN, RUN_LIST_HEADER, event_line
from irimi.report.terminal import WIDE_BIND_NOTICE, exchange_line
from tests.e2e.workflows.agents.w09_scope_gauntlet.agent import (
    AGENT_VERSION,
    BIG_CHARGE,
    CHARGE,
    CUSTOMER,
    ENTRYPOINT,
    ERROR_KEY,
    LONG_MESSAGE_CHARS,
    OVERSIZED_BODY_BYTES,
)
from tests.e2e.workflows.agents.w09_scope_gauntlet.scenarios import BIG_DESCRIPTION_BYTES, WORKFLOW
from tests.e2e.workflows.harness.compare import rows_not_shown
from tests.e2e.workflows.harness.invariants import check_invariants

W = "w09_scope_gauntlet"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
MODES = ("shadow", "serve")


@pytest.mark.parametrize("mode", MODES)
def test_the_scope_rule_fakes_every_method_no_route_names(run_workflow, mode):
    shadow = run_workflow(W, "verbs", mode)
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
            f"faked         unknown   {verb} api.stripe.com{path} -> 200  [unclassified]" in lines
        )
    # A mapped write whose route ships no `fixture:` is still a write, faked with an echo.
    assert "faked         write     POST slack.com/api/reactions.add -> 200" in lines
    assert "  ○ slack  add :eyes: to a message in C0GAUNT  faked; not checked" in summary_of(shadow)
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


@pytest.mark.parametrize("mode", MODES)
def test_one_idempotency_key_is_one_write_and_a_reused_key_is_stripes_own_error(run_workflow, mode):
    shadow = run_workflow(W, "idempotency", mode)
    bare = run_workflow(W, "idempotency", "bare")
    # The agent sees the same statuses either way: irimi's idempotency is Stripe's.
    assert [s for s, _ in shadow.by_label().values()] == [s for s, _ in bare.by_label().values()]
    assert shadow.by_label()["refund_key_reused"] == (400, "fake-L1")
    # A retry with the same key hands back the first answer, minted id and all, and says so; a
    # keyless retry mints a new id.
    ids = {e["label"]: (e["id"], e["replayed"]) for e in shadow.events("refund")}
    first_id = ids["refund_first"][0]
    assert first_id and first_id.startswith("re_")
    assert ids["refund_first"] == (first_id, None)
    assert ids["refund_same_key"] == (first_id, "true")
    assert ids["refund_key_reused"] == (None, None)
    assert len({ids["refund_first"][0], ids["refund_nokey_1"][0], ids["refund_nokey_2"][0]}) == 3
    refund = "faked         write     POST api.stripe.com/v1/refunds"
    lines = shadow.exchange_lines()
    assert f"{refund} -> 200  [idempotent-replay]" in lines
    assert f"{refund} -> 400  [idempotency-conflict]" in lines
    # A retried key issues no second check read; each keyless refund issues its own.
    engine_reads = [r for r in shadow.internet.requests() if r.path == "/v1/charges/ch_GAUNTLET"]
    assert len(engine_reads) == 3
    # The summary counts the two keyless refunds as two writes: a duplicate it does not flag.
    summary = "\n".join(summary_of(shadow))
    assert summary.count("refund $0.50 on ch_GAUNTLET") == 2
    assert REUSED_KEY in summary


# The refund that reused an idempotency key with other parameters, as its summary line prints it.
REUSED_KEY = (
    "✗ stripe  refund 200 (minor units, currency unknown) on ch_GAUNTLET  faked; would fail: "
    "idempotency_error"
)


@pytest.mark.parametrize("mode", MODES)
def test_gzip_chunked_and_oversized_bodies_are_still_faked(run_workflow, mode):
    shadow = run_workflow(W, "bodies", mode)
    assert shadow.by_label() == {
        "refund_gzip": (200, "fake-L1"),
        "refund_chunked": (200, "fake-L1"),
        "customer_3mb": (200, "fake-L1"),
    }
    summary = "\n".join(summary_of(shadow))
    # irimi read the refund amount out of the gzip body and out of the chunked one.
    assert "refund $3.00 on ch_GAUNTLET" in summary
    assert "refund $4.00 on ch_GAUNTLET" in summary
    # The 3 MB update and its 3 MB fake are under `MAX_STORED_BODY`, so both are stored whole.
    [update] = [
        e
        for e in shadow.stored_events()
        if isinstance(e, Exchange) and e.request.path.startswith("/v1/customers/")
    ]
    assert update.response is not None
    assert 3_000_000 < len(update.request.body) < MAX_STORED_BODY
    assert 3_000_000 < len(update.response.body) < MAX_STORED_BODY
    assert Flag.BODY_TRUNCATED not in update.flags


@pytest.mark.parametrize("mode", MODES)
def test_a_read_past_the_body_limit_leaves_the_check_and_the_overlay_unable_to_say(
    run_workflow, mode
):
    """A charge past `MAX_BODY_BYTES` can be neither checked nor overlaid, as the summary says."""
    bare = run_workflow(W, "big_reads", "bare")
    [charge_read] = [r for r in bare.internet.requests() if r.method == "GET"]
    assert charge_read.path == f"/v1/charges/{BIG_CHARGE}"
    shadow = run_workflow(W, "big_reads", mode)
    assert shadow.by_label() == {"refund_big": (200, "fake-L1"), "get_big": (200, None)}
    # Bare, the read-back shows the refund; under shadow the overlay could not add it.
    assert [e["amount_refunded"] for e in bare.events("big_charge")] == [700]
    assert [e["amount_refunded"] for e in shadow.events("big_charge")] == [0]
    assert shadow.exchange_lines() == [
        # KNOWN BUG: a check read answered 200 with a body too big to read is recorded with no
        # response and no flag, the same line as a read that got nothing.
        f"irimi-check   read      GET api.stripe.com/v1/charges/{BIG_CHARGE} -> -",
        "faked         write     POST api.stripe.com/v1/refunds -> 200",
        f"real          read      GET api.stripe.com/v1/charges/{BIG_CHARGE} -> 200",
    ]
    summary = summary_of(shadow)
    assert (
        f"  ○ stripe  refund 700 (minor units, currency unknown) on {BIG_CHARGE}  faked; could not "
        "check: irimi's check read of stripe got no usable answer"
    ) in summary
    assert f"    ↳ GET /v1/charges/{BIG_CHARGE} did not show it  real (partial)" in summary
    # KNOWN BUG: the check read is stored the same way, with no response and no flag, so a stored
    # run cannot tell "too big" from "no answer" either.
    assert engine_reads(shadow) == [(None, ())]


def summary_of(result) -> list[str]:
    """Return irimi's summary of the script's writes: shadow's, or serve's unattributed one.

    A plain script names no run, so under serve its requests are `unattributed`."""
    if result.mode == "shadow":
        return result.summary()
    code, out, err = result.runs("show", trace.UNATTRIBUTED)
    assert (code, err) == (0, [])
    return out[out.index("") + 1 :]


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


# The scenarios that send one write whose body decodes past irimi's cap.
CUT_WRITES = ("bombs", "cut_refund", "cut_update")


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("scenario", sorted(WORKFLOW.scenarios))
def test_no_decision_ever_fails(run_workflow, scenario, mode):
    """No gauntlet edge makes a decision fail, except a write cut at the cap, refused by design.

    A failed decision answers 502 `decision-failed` (fail-closed hook rule). Each `CUT_WRITES`
    scenario reaches that path once, and every agent call gets an HTTP answer."""
    shadow = run_workflow(W, scenario, mode)
    assert shadow.exchange_lines()
    failed = [line for line in shadow.exchange_lines() if "decision-failed" in line]
    assert [line for line in failed if not line.endswith("decision-failed, body-truncated]")] == []
    assert len(failed) == (scenario in CUT_WRITES)
    assert all(isinstance(c.get("status"), int) for c in shadow.calls())


@pytest.mark.parametrize("mode", MODES)
def test_irimis_own_headers_sent_by_the_agent_never_leave(run_workflow, mode):
    # Bare, the agent's forged headers reach the service: the agent really sends them.
    bare = run_workflow(W, "headers", "bare")
    assert [
        sorted(h for h in r.headers if h.startswith("irimi-")) for r in bare.internet.requests()
    ] == [
        ["irimi-rewrote"],
        ["irimi-run"],
    ]
    shadow = run_workflow(W, "headers", mode)
    assert shadow.by_label() == {"forged_rewrote": (200, None), "bad_run_id": (200, None)}
    assert [r.path for r in shadow.internet.requests()] == [
        "/v1/refunds",
        "/v1/charges/ch_GAUNTLET",
    ]
    for req in shadow.internet.requests():
        assert "irimi-rewrote" not in req.headers
        assert "irimi-run" not in req.headers


@pytest.mark.parametrize("mode", MODES)
def test_every_spelling_of_the_proxys_own_address_is_the_reverse_door(run_workflow, mode):
    """Every spelling of irimi's own address is taken in base-URL mode, and each refund faked.

    `127.0.0.1` goes direct (irimi's `NO_PROXY`); `127.1` and `0.0.0.0` go through the proxy to
    itself. Missing a spelling would forward the request. Serve binds 0.0.0.0 here; the agent still
    comes from loopback, so the base-URL peer check lets it through. The non-loopback address is
    `tests/cli/test_serve.py`'s case."""
    shadow = run_workflow(W, "self_addressed", mode)
    assert {k: v for k, v in shadow.by_label().items() if k.startswith("door_")} == {
        "door_127.0.0.1": (200, "fake-L1"),
        "door_127.1": (200, "fake-L1"),
        "door_0.0.0.0": (200, "fake-L1"),
    }
    assert shadow.internet.requests() == []
    # Base-URL mode relays over https and the fake internet speaks only http, so the check read
    # gets no answer; it prints with no response and no flag, as in `big_reads`.
    read = "irimi-check   read      GET api.stripe.com/v1/charges/ch_GAUNTLET -> -"
    write = "faked         write     POST api.stripe.com/v1/refunds -> 200"
    assert shadow.exchange_lines() == [read, write] * 3
    assert engine_reads(shadow) == [(None, ())] * 3  # stored as `big_reads` stores its one
    if mode == "shadow":
        refunds = [line for line in shadow.summary() if "○" in line]
        refund = (
            "  ○ stripe  refund 600 (minor units, currency unknown) on ch_GAUNTLET  faked; could "
            "not check: irimi's check read of stripe got no usable answer"
        )
        assert refunds == [refund] * 3
        return
    # A plain script starts no run, so under serve every refund is `unattributed` and no run's
    # summary prints. The bind wider than loopback is in the banner, once.
    assert shadow.stored().list_runs() == [] and shadow.run_blocks() == {}
    assert [line for line in shadow.irimi if line.startswith("listening on 0.0.0.0: ")] == [
        f"listening on 0.0.0.0: {WIDE_BIND_NOTICE}"
    ]


@pytest.mark.parametrize("mode", MODES)
def test_the_control_endpoint_answers_at_every_spelling_and_is_never_an_exchange(
    run_workflow, mode
):
    """All three spellings reach the control endpoint, whose answers are never exchanges.

    Health answers 200 and an unknown path 404, both stamped `control`, and neither is recorded."""
    shadow = run_workflow(W, "self_addressed", mode)
    spellings = ("127.0.0.1", "127.1", "0.0.0.0")
    assert {k: v for k, v in shadow.by_label().items() if not k.startswith("door_")} == {
        **{f"health_{s}": (200, "control") for s in spellings},
        **{f"nope_{s}": (404, "control") for s in spellings},
    }
    assert not [line for line in shadow.exchange_lines() if "/_irimi/" in line]
    assert shadow.internet.requests() == []
    # Bare, the calls reach the fake internet, which serves no such host, and the agent carries on.
    bare = run_workflow(W, "self_addressed", "bare")
    assert {k: v for k, v in bare.by_label().items() if not k.startswith("door_")} == {
        **{f"health_{s}": (502, None) for s in spellings},
        **{f"nope_{s}": (502, None) for s in spellings},
    }
    assert bare.exit_code == shadow.exit_code == 0


# -- the control endpoint, called as the SDK will call it -----------------------------------------
#
# `control_runs` and `control_hazards` post by hand what the SDK would, so each post is exact. The
# SDK's own posts are pinned by every other workflow.

# Each run `control_runs` posts: run id -> (its labels' tag, the path its labelled read GETs).
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
        f"the body is {OVERSIZED_BODY_BYTES + 11} bytes, over the {MAX_CONTROL_BODY} allowed",
    ),
    # Named by its type, never quoted: the same line on every Python.
    "refuse_deep": (400, "'name' is a list, not a string"),
}
ELAPSED = re.compile(r" · \d+\.\ds$")


def posted(result, label: str) -> dict:
    """The body the agent posted to the control endpoint under `label`."""
    [event] = [e for e in result.events("posted") if e["label"] == label]
    return event["body"]


def sdk_record(result, run_id: str, start: str, end: str | None) -> RunRecord:
    """Return the record stored for the run posted under labels `start` and `end`.

    Every field the SDK may set is as it posted it, and the rest are irimi's."""
    began = posted(result, start)
    ended: dict = posted(result, end) if end else {"ended_at": None, "outcome": None, "error": None}
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
        end=None
        if end is None
        else RunEnd(
            ended["ended_at"],
            ended["outcome"],
            None if error is None else ErrorInfo(error["type"], error["message"]),
            None,
        ),
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
    return f"real          read      GET api.stripe.com{path} -> 200"


def listed(result) -> list[list[str]]:
    """Return `irimi runs list`'s rows as fields, start time and duration masked.

    Columns are padded, so two or more spaces separate two fields."""
    code, out, err = result.runs("list")
    assert (code, err) == (0, [])
    assert out[0].split() == list(RUN_LIST_HEADER)
    rows = (re.split(r"  +", line.rstrip()) for line in out[1:])
    return sorted([f[0], "<started>", "<elapsed>", *f[3:]] for f in rows)


def listed_command(argv: list[str]) -> str:
    """Return a process run's trigger column: its command without the program's directory, cut."""
    command = shlex.join([argv[0].rsplit("/", 1)[-1], *argv[1:]])
    if len(command) <= MAX_TRIGGER_SHOWN:
        return command
    return command[: MAX_TRIGGER_SHOWN - 1] + "…"


def shown(result, run_id: str) -> list[str]:
    """`irimi runs show <run_id>`, its elapsed seconds masked."""
    code, out, err = result.runs("show", run_id)
    assert (code, err) == (0, [])
    return [ELAPSED.sub(" · <elapsed>", line) for line in out]


@pytest.mark.parametrize("mode", MODES)
def test_the_sdks_posts_are_answered_by_irimi_and_its_reads_go_live(run_workflow, mode):
    """Every control post is answered `control`, and every read labelled `Irimi-Run` goes live.

    A refusal has its documented status. Bare, the posts reach the fake internet, which serves no
    such host, and the agent exits as under shadow."""
    shadow = run_workflow(W, "control_runs", mode)
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
    # irimi prints only the labelled reads, and the fake internet sees only those, unlabelled.
    assert sorted(shadow.exchange_lines()) == sorted(
        read_line(path) for _, path in CONTROL_RUNS.values()
    )
    assert sorted((r.method, r.path) for r in shadow.internet.requests()) == sorted(
        ("GET", path) for _, path in CONTROL_RUNS.values()
    )
    assert [r for r in shadow.internet.requests() if "irimi-run" in r.headers] == []


@pytest.mark.parametrize("mode", MODES)
def test_a_run_posted_over_the_control_endpoint_is_stored_as_an_sdk_run(run_workflow, mode):
    """A run posted over the control endpoint is stored as an `sdk` run with what it posted.

    It holds its two tool calls with its labelled read between them, whether it posted direct
    (`ctl-direct`) or through the proxy (`ctl-proxied`), and `irimi runs show` prints it so."""
    shadow = run_workflow(W, "control_runs", mode)
    reader = shadow.stored()
    for run_id, (tag, path) in CONTROL_RUNS.items():
        stored = reader.load_run(run_id)
        assert stored.record == sdk_record(shadow, run_id, f"{tag}_start", f"{tag}_end")
        before, read, after = stored.events
        assert before == stored_tool_call(shadow, run_id, f"{tag}_tool_1")
        assert after == stored_tool_call(shadow, run_id, f"{tag}_tool_2")
        assert isinstance(read, Exchange)
        assert (read.run_id, exchange_line(read)) == (run_id, read_line(path))
        # In time as well as in `seq`: each clock read is after the last.
        times = [stored.record.started_at, before.ended_at, read.started_at, read.ended_at]
        times += [after.started_at, stored.record.end.ended_at]
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
            "tool-real     read      look_up -> ok",
            read_line(path),
            "tool-real     read      summarise -> ok",
            "",
            f"irimi shadow · run {run_id} · 1 exchange · <elapsed>",
            "",
            "  api.stripe.com  1 read",
            "",
            "  1 exchange · 1 real · 0 answered by irimi (0 faked writes) · 0 sent to targets",
        ]


@pytest.mark.parametrize("mode", MODES)
def test_two_runs_posted_at_once_do_not_mix(run_workflow, mode):
    """Two runs posted in lockstep on the control endpoint each hold only their own events.

    Each step of one run falls between steps of the other, on the wire and in the store's queue."""
    shadow = run_workflow(W, "control_runs", mode)
    threads = [label for label in (c["label"] for c in shadow.calls()) if "thread" in label]
    assert [label.split("_", 2)[2] for label in threads] == [s for s in STEPS for _ in (1, 2)]
    handed = [e.run_id for e in shadow.handed_events if e.run_id.startswith("ctl-thread-")]
    assert sorted(handed[:2]) == sorted(handed[2:4]) == sorted(handed[4:])
    assert sorted(handed[:2]) == ["ctl-thread-1", "ctl-thread-2"]
    reader = shadow.stored()
    for run_id in ("ctl-thread-1", "ctl-thread-2"):
        _tag, path = CONTROL_RUNS[run_id]
        assert [e.run_id for e in reader.load_run(run_id).events] == [run_id] * 3
        assert shown(shadow, run_id)[8:11] == [
            "tool-real     read      look_up -> ok",
            read_line(path),
            "tool-real     read      summarise -> ok",
        ]


@pytest.mark.parametrize("mode", MODES)
def test_a_run_that_ends_in_error_keeps_its_error_redacted_then_cut_to_what_a_trace_keeps(
    run_workflow, mode
):
    """A run's long errors are stored redacted whole, then cut to what a trace keeps.

    The tool's and the end's messages pass 1000 characters and the end's type 300, with one key
    across all three cuts. Each keeps the key's placeholder and no part of the key."""
    shadow = run_workflow(W, "control_runs", mode)
    stored = shadow.stored().load_run("ctl-error")
    tool = stored_tool_call(shadow, "ctl-error", "error_tool")
    run_type = posted(shadow, "error_end")["error"]["type"]
    run_message = posted(shadow, "error_end")["error"]["message"]
    assert tool.error is not None
    tool_message = tool.error.message
    assert (len(run_type), len(run_message), len(tool_message)) == (657, *[LONG_MESSAGE_CHARS] * 2)
    hidden = redact.placeholder(redact.load_key(shadow.home), ERROR_KEY)

    def kept(posted_text: str, limit: int) -> str:
        """Return what a trace keeps of `posted_text`: redacted whole, then cut."""
        assert posted_text.index(ERROR_KEY) < limit < posted_text.index(ERROR_KEY) + len(ERROR_KEY)
        return posted_text.replace(ERROR_KEY, hidden)[:limit]

    cut = ErrorInfo(kept(run_type, MAX_ERROR_TYPE), kept(run_message, MAX_ERROR_MESSAGE))
    tool_cut = ErrorInfo("builtins.ValueError", kept(tool_message, MAX_ERROR_MESSAGE))
    assert all(hidden in text for text in (cut.type, cut.message, tool_cut.message))
    expected = sdk_record(shadow, "ctl-error", "error_start", "error_end")
    assert expected.end is not None
    assert stored.record == dataclasses.replace(
        expected, end=dataclasses.replace(expected.end, error=cut)
    )
    assert stored.events == [dataclasses.replace(tool, error=tool_cut)]
    on_disk = [p.read_bytes() for p in shadow.store_root().rglob("*") if p.is_file()]
    assert not any(ERROR_KEY[:8].encode() in data for data in on_disk)
    assert shown(shadow, "ctl-error") == [
        "run: ctl-error",
        "attribution: sdk",
        "trigger: ctl-error",
        f"entrypoint: {ENTRYPOINT}:ctl-error",
        'args: {"run":"ctl-error"}',
        f"agent version: {AGENT_VERSION}",
        "outcome: error",
        f"error: {cut.type}: {cut.message}",
        "",
        "tool-real     read      charge_card -> raised builtins.ValueError",
        "",
        "irimi shadow · run ctl-error · 0 exchanges · <elapsed>",
        "",
        "  0 exchanges · 0 real · 0 answered by irimi (0 faked writes) · 0 sent to targets",
    ]


@pytest.mark.parametrize("mode", MODES)
def test_each_refusal_answers_its_status_and_line_and_makes_no_run(run_workflow, mode):
    """Each documented control refusal answers its status and line, and leaves no run behind.

    `REFUSALS` lists them. None leaves a run directory or an unattributed event; under serve the
    store also holds `serve.lock` and no process run."""
    shadow = run_workflow(W, "control_runs", mode)
    assert {e["label"]: e["body"] for e in shadow.events("refusal")} == {
        label: control_refusal(line) for label, (_, line) in REFUSALS.items()
    }
    root = shadow.home / paths.STORE_DIR_NAME
    lock = [SERVE_LOCK_FILE] if mode == "serve" else []
    assert sorted(p.name for p in root.iterdir()) == ["blobs", "runs", *lock]
    process = [shadow.process_run_id()] if mode == "shadow" else []
    assert sorted(p.name for p in (root / "runs").iterdir()) == sorted(
        [*process, *CONTROL_RUNS, "ctl-error"]
    )
    assert shadow.stored().load_run(trace.UNATTRIBUTED).events == []


@pytest.mark.parametrize("mode", MODES)
def test_health_reports_the_engine_and_what_the_store_wrote(run_workflow, mode):
    """Health reports the engine's version, whether it serves, and the store's counters.

    Read before the first run and after the last, `written` grows from 0 to every event stored."""
    shadow = run_workflow(W, "control_runs", mode)
    before, after = (e["body"] for e in shadow.events("health"))
    health = {"engine_version": irimi.__version__, "serve": mode == "serve"}
    assert before == {**health, "store": {"queued": 0, "written": 0, "dropped": 0}}
    assert after == {**health, "store": {"queued": 0, "written": 13, "dropped": 0}}
    assert len(shadow.stored_events()) == 13


@pytest.mark.parametrize("mode", MODES)
def test_a_run_ended_over_the_control_endpoint_no_longer_lists_as_incomplete(run_workflow, mode):
    """A run that posts its end lists with how it ended, not as `incomplete`.

    A tool call counts as no exchange. Under serve each run prints, as its end is taken, the block
    `irimi runs show` ends in."""
    shadow = run_workflow(W, "control_runs", mode)
    ran = [
        *(
            [run_id, "<started>", "<elapsed>", "ok", "sdk", run_id, "1 exchange", "0 writes"]
            for run_id in CONTROL_RUNS
        ),
        ["ctl-error", "<started>", "<elapsed>", "error", "sdk", "ctl-error"]
        + ["0 exchanges", "0 writes"],
    ]
    if mode == "serve":
        assert listed(shadow) == sorted(ran)
        blocks = shadow.run_blocks()
        assert sorted(blocks) == sorted([*CONTROL_RUNS, "ctl-error"])
        for run_id, block in blocks.items():
            masked = [ELAPSED.sub(" · <elapsed>", line) for line in block]
            assert shown(shadow, run_id)[-len(block) :] == masked
        return
    process = shadow.process_run_id()
    trigger = shadow.stored().load_run(process).record.trigger
    assert trigger is not None and trigger.argv is not None
    command = listed_command(list(trigger.argv))
    assert listed(shadow) == sorted(
        [
            *ran,
            [
                process,
                "<started>",
                "<elapsed>",
                "ok",
                "process",
                command,
                "0 exchanges",
                "0 writes",
            ],
        ]
    )


@pytest.mark.parametrize("mode", MODES)
def test_a_start_re_posted_after_its_end_is_dropped_and_the_run_kept_whole(run_workflow, mode):
    """A start re-posted after its run ended is answered 204 and dropped by the store.

    The run keeps its first trigger, end and read. It lost nothing, so the drop counts in health's
    `dropped`, not in the run's `dropped_events`."""
    shadow = run_workflow(W, "control_hazards", mode)
    assert shadow.by_label() == {
        "restarted_start": (204, "control"),
        "restarted_read": (200, None),
        "restarted_end": (204, "control"),
        "restarted_again": (204, "control"),
        "process_start": (400, "control"),
        "process_tool": (204, "control"),
        "process_end": (400, "control"),
        "unattributed_tool": (204, "control"),
        "health_after": (200, "control"),
    }
    stored = shadow.stored().load_run("ctl-restarted")
    assert stored.record == sdk_record(shadow, "ctl-restarted", "restarted_start", "restarted_end")
    assert [event_line(e) for e in stored.events] == [read_line(f"/v1/charges/{CHARGE}")]
    [line] = [f for f in listed(shadow) if f[0] == "ctl-restarted"]
    assert line[3:] == ["ok", "sdk", "ctl-restarted", "1 exchange", "0 writes"]
    assert shown(shadow, "ctl-restarted")[:8] == [
        "run: ctl-restarted",
        "attribution: sdk",
        "trigger: ctl-restarted",
        f"entrypoint: {ENTRYPOINT}:ctl-restarted",
        'args: {"run":"ctl-restarted"}',
        f"agent version: {AGENT_VERSION}",
        "outcome: ok",
        "",
    ]
    # An empty queue means the writer is done with the start; `written` is not pinned, since the
    # last tool call's line may not be written yet.
    [health] = (e["body"] for e in shadow.events("health"))
    assert (health["store"]["queued"], health["store"]["dropped"]) == (0, 1)


def test_the_process_runs_start_and_end_are_irimis_and_only_its_tool_calls_are_taken(
    run_workflow,
):
    """The SDK may post tool calls to the process run, but not its start or end.

    So its record stays irimi's: `process`, the agent's argv and its exit."""
    shadow = run_workflow(W, "control_hazards", "shadow")
    process = shadow.process_run_id()
    assert [e["id"] for e in shadow.events("process_run")] == [process]
    refused = control_refusal(f"{process!r} is irimi's own run, which the SDK may not start or end")
    assert {e["label"]: e["body"] for e in shadow.events("answer")} == {
        "process_start": refused,
        "process_tool": None,
        "process_end": refused,
        "unattributed_tool": None,
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
        "tool-real     read      look_up -> ok",
        "",
        f"irimi shadow · run {process} · 0 exchanges · <elapsed>",
        "",
        "  0 exchanges · 0 real · 0 answered by irimi (0 faked writes) · 0 sent to targets",
    ]
    # A tool call posted for no run, as under `irimi serve`, is filed in `unattributed/`: the one
    # post that may name it.
    unattributed = shadow.stored().load_run(trace.UNATTRIBUTED).events
    assert unattributed == [stored_tool_call(shadow, trace.UNATTRIBUTED, "unattributed_tool")]
    # Both print the same line, so they are counted.
    assert shadow.tool_call_lines() == ["tool-real     read      look_up -> ok"] * 2
    # Bare, there is no process run: the agent posts for a placeholder id, to the fake internet.
    bare = run_workflow(W, "control_hazards", "bare")
    assert [e["id"] for e in bare.events("process_run")] == ["no-process-run"]


def test_under_serve_irimis_own_run_is_unattributed_and_only_its_tool_calls_are_taken(
    run_workflow,
):
    """Under serve, irimi's own run is `unattributed`: tool calls may be posted to it, not its ends.

    A start or end for it is refused 400 by the run-id check. The re-posted start is dropped, so
    the run's printed block is the one `irimi runs show` ends in."""
    serve = run_workflow(W, "control_hazards", "serve")
    assert [e["id"] for e in serve.events("process_run")] == [trace.UNATTRIBUTED]
    refused = control_refusal(f"{trace.UNATTRIBUTED!r} cannot name a run")
    assert {e["label"]: e["body"] for e in serve.events("answer")} == {
        "process_start": refused,
        "process_tool": None,
        "process_end": refused,
        "unattributed_tool": None,
    }
    unattributed = serve.stored().load_run(trace.UNATTRIBUTED).events
    assert unattributed == [
        stored_tool_call(serve, trace.UNATTRIBUTED, "process_tool"),
        stored_tool_call(serve, trace.UNATTRIBUTED, "unattributed_tool"),
    ]
    assert [r.run_id for r in serve.stored().list_runs()] == ["ctl-restarted"]
    assert [line for line in serve.irimi if line.startswith("--- run ")] == [
        "--- run ctl-restarted ---"
    ]
    [block] = serve.run_blocks().values()
    assert [ELAPSED.sub(" · <elapsed>", line) for line in block] == [
        "irimi shadow · run ctl-restarted · 1 exchange · <elapsed>",
        "",
        "  api.stripe.com  1 read",
        "",
        "  1 exchange · 1 real · 0 answered by irimi (0 faked writes) · 0 sent to targets",
    ]
    [line] = [f for f in listed(serve) if f[0] == "ctl-restarted"]
    assert line[3] == "ok"
    assert shown(serve, "ctl-restarted")[-len(block) :] == [
        ELAPSED.sub(" · <elapsed>", line) for line in block
    ]


def argv(result) -> list[str]:
    """Return the argv the harness started the agent with: the process run's trigger args."""
    workflow = WORKFLOW.scenarios[result.scenario]
    return [sys.executable, "-m", WORKFLOW.module, *workflow.argv]


@pytest.mark.parametrize("mode", MODES)
def test_an_unmapped_hosts_posts_are_faked_even_when_they_are_reads(run_workflow, mode):
    shadow = run_workflow(W, "unmapped_hosts", mode)
    assert shadow.by_label() == {
        # A GraphQL query is a read, but irimi cannot tell it from a mutation: both are faked.
        "graphql_query": (200, "fake-L0"),
        "graphql_mutation": (200, "fake-L0"),
        "unmapped_get": (200, None),
        "slack_unrouted": (200, "fake-L0"),
    }
    assert (
        "faked         unknown   POST slack.com/api/chat.delete -> 200  [unclassified]"
        in shadow.exchange_lines()
    )
    assert [r.path for r in shadow.internet.requests()] == ["/health"]


@pytest.mark.parametrize("mode", MODES)
def test_a_get_that_writes_escapes_and_the_scenario_says_so(run_workflow, mode):
    shadow = run_workflow(W, "get_that_writes", mode)
    assert [(r.method, r.host, r.path) for r in shadow.internet.writes()] == [
        ("GET", "legacy.internal", "/api/delete_user")
    ]
    assert (
        "real          read      GET legacy.internal/api/delete_user -> 200"
        in shadow.exchange_lines()
    )


def test_a_replay_answers_the_get_that_writes_from_its_recording_so_it_writes_nothing(replayed):
    """A replay answers a GET that deleted from its recording, so the delete does not happen again.

    It escaped when recorded, as every GET does: a read is live."""
    recorded, replay = replayed(W, "get_that_writes")
    assert [(r.method, r.path) for r in recorded.internet.writes()] == [("GET", "/api/delete_user")]
    assert replay.internet.writes() == []
    assert (
        "recorded      read      GET legacy.internal/api/delete_user -> 200"
        "  [from-recording]" in replay.exchange_lines()
    )


@pytest.mark.parametrize("scenario", ["cut_refund", "cut_update"])
def test_a_replay_refuses_a_cut_write_again_and_answers_the_rest_from_its_recording(
    replayed, scenario
):
    """A replay refuses a cut write again, before any policy, and answers the rest from its record.

    The refusal carries no replay flag and never enters the write log, so the checked $9 refund
    after it replays from the recording. `irimi compare` calls the cut pair `?` and exits 1."""
    recorded, replay = replayed(W, scenario)
    cut, refund = replay.reported
    assert (cut.kind, cut.answered_by, cut.response.status, cut.flags) == (
        "write",
        "irimi",
        502,
        ("decision-failed", "body-truncated"),
    )
    assert replay.internet.requests() == []
    assert (refund.precondition.outcome, refund.flags) == ("passed", ("from-recording",))
    code, out, err = replay.compare_runs(recorded.process_run_id(), replay.process_run_id())
    assert (code, err) == (1, [])
    assert out[-1] == (
        "  1 same · 0 changed · 0 only in A · 0 only in B · 1 not compared (a body was cut when "
        "stored)"
    )


def _files(root) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()
    }


def test_a_replay_never_writes_into_the_run_it_replays(replayed):
    """A replay never writes into the run it answers from, even when the agent posts to it again.

    Replayed by its command, `control_runs` posts `ctl-direct` again. That run's files stay byte
    for byte; what the replay said of it is filed in `unattributed`, flagged `bad-run-id`, and
    still answered from the recording."""
    recorded, replay = replayed(W, "control_runs", run_id="ctl-direct", command=True)
    stored = _files(recorded.store_root() / RUNS_DIR / "ctl-direct")
    assert stored and _files(replay.store_root() / RUNS_DIR / "ctl-direct") == stored
    assert recorded.stored().load_run(trace.UNATTRIBUTED).events == []
    filed = [
        e for e in replay.stored().load_run(trace.UNATTRIBUTED).events if e.run_id == "ctl-direct"
    ]
    assert [type(e).__name__ for e in filed] == ["ToolCall", "Exchange", "ToolCall"]
    [read] = [e for e in filed if isinstance(e, Exchange)]
    assert (read.answered_by, read.request.path) == ("recording", f"/v1/charges/{CHARGE}")
    assert read.flags == ("from-recording", "bad-run-id")
    assert replay.by_label()["direct_read"] == (200, "recording")


def test_a_replay_writes_no_other_recorded_run_its_agent_names_again(replayed):
    """A replay writes only the runs it makes, though its agent names other recorded runs.

    What it said of each is filed in `unattributed`, its exchanges flagged `bad-run-id`."""
    recorded, replay = replayed(W, "control_runs", run_id="ctl-direct", command=True)
    runs = recorded.store_root() / RUNS_DIR
    for directory in runs.iterdir():
        assert _files(replay.store_root() / RUNS_DIR / directory.name) == _files(directory)
    assert [r.run_id for r in replay.stored().list_runs() if r.replay_of is not None] == [
        replay.process_run_id()
    ]
    filed = replay.stored().load_run(trace.UNATTRIBUTED).events
    assert sorted({e.run_id for e in filed}) == [
        "ctl-direct",
        "ctl-error",
        "ctl-proxied",
        "ctl-thread-1",
        "ctl-thread-2",
    ]
    assert all("bad-run-id" in e.flags for e in filed if isinstance(e, Exchange))


# -- bodies that decode past irimi's cap ----------------------------------------------------------
#
# `bombs` sends and reads gzip bodies that decode to 128 MiB, past irimi's cap. Each is recorded
# bodiless and `body-truncated`: a write is refused, a live call forwarded as sent, a read never
# overlaid, and a control post refused 413.

BOMB_LINES = [
    "irimi         write     POST api.stripe.com/v1/refunds -> 502  "
    "[decision-failed, body-truncated]",
    "real          llm       POST api.anthropic.com/v1/messages -> 413  [body-truncated]",
    f"irimi-check   read      GET api.stripe.com/v1/charges/{CHARGE} -> 200",
    "faked         write     POST api.stripe.com/v1/refunds -> 200",
    "real          read      GET api.stripe.com/v1/refunds -> 200  [body-truncated]",
]
CONTROL_BOMBS = ("control_bomb_lines", "control_bomb_after_a_line", "control_bomb_one_line")


def control_refusal(message: str) -> dict:
    """Return the body of a control request the endpoint refused."""
    return {"error": {"type": "irimi_control_refused", "message": message}}


def sent(result, label: str) -> dict:
    """What the agent logged of the body it sent under `label`: its length and SHA-256."""
    [event] = [e for e in result.events("sent", with_time=False) if e["label"] == label]
    return event


def digest(body: bytes) -> tuple[int, str]:
    return len(body), hashlib.sha256(body).hexdigest()


def stored_bomb(result, method: str, path: str) -> Exchange:
    """The one exchange stored for `method path` that is flagged as not all there."""
    [ex] = [
        e
        for e in result.stored_events()
        if isinstance(e, Exchange) and (e.request.method, e.request.path) == (method, path)
        if Flag.BODY_TRUNCATED in e.flags
    ]
    return ex


@pytest.mark.parametrize("mode", MODES)
def test_bodies_that_decode_past_the_cap_are_answered_by_irimi_or_the_service(run_workflow, mode):
    """Under shadow a bombed refund gets irimi's 502, a model call the service's 413.

    Each control post gets irimi's 413. Bare, the services refuse the two bodies 413, and the
    control posts reach the fake internet, 502."""
    bare = run_workflow(W, "bombs", "bare")
    shadow = run_workflow(W, "bombs", mode)
    assert bare.by_label() == {
        "refund_gzip_bomb": (413, None),
        "llm_gzip_bomb": (413, None),
        "refund_plain": (200, None),
        "refunds_padded": (200, None),
        **dict.fromkeys(CONTROL_BOMBS, (502, None)),
        "health_after": (502, None),
    }
    assert shadow.by_label() == {
        "refund_gzip_bomb": (502, "irimi"),
        "llm_gzip_bomb": (413, None),
        "refund_plain": (200, "fake-L1"),
        "refunds_padded": (200, None),
        **dict.fromkeys(CONTROL_BOMBS, (413, "control")),
        "health_after": (200, "control"),
    }
    assert shadow.exchange_lines() == BOMB_LINES
    assert bare.exit_code == shadow.exit_code == 0


@pytest.mark.parametrize("mode", MODES)
def test_a_write_whose_body_decodes_past_the_cap_is_refused_and_never_leaves(run_workflow, mode):
    """A refund whose form decodes past the cap fails closed: irimi's 502, and nothing leaves.

    A form is one line, so irimi keeps none of it. It is recorded under its route and flagged,
    claims no webhooks, and the summary prints it as refused, with no `human:` sentence."""
    shadow = run_workflow(W, "bombs", mode)
    posted = [(r.method, r.path) for r in shadow.internet.requests() if r.method == "POST"]
    assert posted == [("POST", "/v1/messages")]
    ex = stored_bomb(shadow, "POST", "/v1/refunds")
    assert (ex.answered_by, ex.service, ex.operation) == ("irimi", "stripe", "refunds.create")
    assert ex.flags == ("decision-failed", Flag.BODY_TRUNCATED)
    assert ex.request.body == b""
    assert ex.response is not None and ex.response.status == 502
    assert json.loads(ex.response.body)["error"]["type"] == "irimi_decision_failed"
    summary = summary_of(shadow)
    assert [line for line in summary if line.startswith("  api.stripe.com ")] == [
        "  api.stripe.com     1 read  1 irimi check read  1 faked write  1 write refused by irimi"
    ]
    assert [line for line in summary if line.startswith(("  ○", "  ✗"))] == [
        f"  {CUT_REFUND}",
        f"  ○ stripe  refund $9.00 on {CHARGE}  faked; checked against live state: would succeed",
    ]
    assert "  would have fired: refund.created, charge.refunded" in summary


@pytest.mark.parametrize("mode", MODES)
def test_a_live_call_whose_body_decodes_past_the_cap_reaches_the_service_byte_for_byte(
    run_workflow,
    mode,
):
    """A model call past the cap reaches the service as the agent's own gzip bytes.

    The service answers 413 as it does bare; irimi records the request bodiless, flagged once."""
    bare = run_workflow(W, "bombs", "bare")
    shadow = run_workflow(W, "bombs", mode)
    posted = sent(shadow, "llm_gzip_bomb")
    assert posted == sent(bare, "llm_gzip_bomb")
    for result in (bare, shadow):
        [req] = result.internet.requests("api.anthropic.com")
        assert req.headers["content-encoding"] == "gzip"
        assert digest(req.body) == (posted["bytes"], posted["sha256"])
    ex = stored_bomb(shadow, "POST", "/v1/messages")
    assert (ex.answered_by, ex.flags, ex.request.body) == ("live", (Flag.BODY_TRUNCATED,), b"")
    assert ex.request.header("content-encoding") == "gzip"
    assert ex.response is not None and ex.response.status == 413


@pytest.mark.parametrize("mode", MODES)
def test_a_read_back_that_decodes_past_the_cap_is_never_overlaid(run_workflow, mode):
    """A read-back past the cap reaches the agent as the service's bytes, never overlaid.

    It is recorded bodiless, flagged and `partial`, so the summary says it did not show the faked
    refund, as for a read too big to parse."""
    bare = run_workflow(W, "bombs", "bare")
    shadow = run_workflow(W, "bombs", mode)
    for result in (bare, shadow):
        [padded] = result.internet.padded
        [received] = result.events("received", with_time=False)
        assert (received["bytes"], received["sha256"]) == digest(padded)
        assert received["encoding"] == "gzip"
    assert [c["answered_by"] for c in shadow.calls("refunds_padded")] == [None]
    ex = stored_bomb(shadow, "GET", "/v1/refunds")
    assert (ex.answered_by, ex.flags, ex.overlay) == ("live", (Flag.BODY_TRUNCATED,), "partial")
    assert ex.response is not None and ex.response.body == b""
    summary = summary_of(shadow)
    refund = summary.index(
        f"  ○ stripe  refund $9.00 on {CHARGE}  faked; checked against live state: would succeed"
    )
    assert summary[refund + 1] == "    ↳ GET /v1/refunds did not show it  real (partial)"


@pytest.mark.parametrize("mode", MODES)
def test_a_run_start_that_decodes_past_the_cap_is_refused_413_unread(run_workflow, mode):
    """A run start that decodes past the control endpoint's limit is refused 413, whatever it holds.

    Each of three starts is valid JSON followed by 128 MiB of blanks, laid out three ways. No start
    makes a run, and the store writes only the exchanges."""
    shadow = run_workflow(W, "bombs", mode)
    allowed = MAX_CONTROL_BODY
    assert {e["label"]: e["body"] for e in shadow.events("refusal")} == dict.fromkeys(
        CONTROL_BOMBS, control_refusal(f"the body decodes past the {allowed} bytes allowed")
    )
    root = shadow.home / paths.STORE_DIR_NAME
    # No run but the process run, which serve mode does not have.
    process = [shadow.process_run_id()] if mode == "shadow" else []
    assert sorted(p.name for p in (root / "runs").glob("*")) == process
    [health] = shadow.events("health")
    assert health["body"]["store"] == {"queued": 0, "written": len(BOMB_LINES), "dropped": 0}


# ---------------------------------------------------------------------- irimi compare
#
# W9 is a plain script, so its writes are the process run's, or under serve `unattributed`'s. Every
# serve run would share that pool, so each pin records into a store of its own, at most one serve.

PLAIN_REFUND = (
    "○ stripe  refund $9.00 on ch_GAUNTLET  faked; checked against live state: would succeed"
)
# A write cut at the cap prints as refused, with no sentence filled from a body irimi did not read.
CUT_REFUND = (
    "✗ stripe  POST api.stripe.com/v1/refunds  refused by irimi: "
    "its body is too large to read whole"
)


def recorded_pair(shared_store_run, a: str, b: str, mode: str, store: str):
    """Run `a` under shadow and `b` under `mode` into one store; return each with its run id."""
    one = shared_store_run(W, a, "shadow", store)
    other = shared_store_run(W, b, mode, store)
    assert check_invariants(other) == []
    return one, one.process_run_id(), other, other.engine_run_id()


def side(result, which: str) -> str:
    """Return compare's header line for side `which`: a process run's command, or nothing.

    The unattributed pool records nothing of what started it."""
    return f"  {which}" if result.mode == "serve" else f"  {which}  {sys.executable}"


@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_keeps_two_cut_writes_of_different_routes_apart(shared_store_run, mode):
    """`irimi compare` never pairs two cut writes of different routes.

    A refused cut write keeps its route, so `cut_refund`'s and `cut_update`'s are one only in A and
    one only in B."""
    one, a, other, b = recorded_pair(shared_store_run, "cut_refund", "cut_update", mode, "cut")
    code, out, err = other.compare_runs(a, b)
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
        side(one, "A"),
        side(other, "B"),
        "  (the triggers' args differ)",
        "",
        f"  - {CUT_REFUND}",
        "  + ✗ stripe  POST api.stripe.com/v1/customers/cus_GAUNTLET  refused by irimi: its body "
        "is too large to read whole",
        f"  = {PLAIN_REFUND}",
        "",
        "  1 same · 0 changed · 1 only in A · 1 only in B",
    ]
    assert rows_not_shown(other, a, b, out) == []


@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_will_not_call_two_cut_refunds_of_different_amounts_the_same(
    shared_store_run, mode
):
    """`irimi compare` calls a pair with a cut side `?`, and exits 1, though both print alike.

    `bombs` refunds 800 and `cut_refund` 700, but neither stored body says how much."""
    one, a, other, b = recorded_pair(shared_store_run, "bombs", "cut_refund", mode, "cut-equal")
    code, out, err = other.compare_runs(a, b)
    assert (code, err) == (1, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
        side(one, "A"),
        side(other, "B"),
        "  (the triggers' args differ)",
        "",
        f"  ? A {CUT_REFUND}",
        f"    B {CUT_REFUND}",
        f"  = {PLAIN_REFUND}",
        "",
        "  1 same · 0 changed · 0 only in A · 0 only in B · 1 not compared (a body was cut when "
        "stored)",
    ]
    assert rows_not_shown(other, a, b, out) == []
    # The two refunds the agents sent were not the same: their bodies' hashes differ.
    sent = [
        e["sha256"]
        for result, label in ((one, "refund_gzip_bomb"), (other, "cut_write"))
        for e in result.events("sent")
        if e["label"] == label
    ]
    assert len(sent) == len(set(sent)) == 2


def test_irimi_compare_of_one_key_used_three_times_under_shadow_against_serve(shared_store_run):
    """`idempotency` compares the same under shadow and serve, and a same-key retry is no write.

    The key reused for another amount would fail; the two keyless refunds are two writes."""
    one, a, other, b = recorded_pair(
        shared_store_run, "idempotency", "idempotency", "serve", "idem"
    )
    assert b == trace.UNATTRIBUTED
    code, out, err = other.compare_runs(a, b)
    rows = [
        "○ stripe  refund $1.00 on ch_GAUNTLET  faked; checked against live state: would succeed",
        REUSED_KEY,
        "○ stripe  refund $0.50 on ch_GAUNTLET  faked; checked against live state: would succeed",
        "○ stripe  refund $0.50 on ch_GAUNTLET  faked; checked against live state: would succeed",
    ]
    assert (code, err) == (0, [])
    assert out == [
        f"irimi compare · {a} vs {b}",
        side(one, "A"),
        side(other, "B"),
        "  (the triggers' args differ)",
        "",
        *[f"  = {row}" for row in rows],
        "",
        "  4 same · 0 changed · 0 only in A · 0 only in B",
    ]
    assert rows_not_shown(other, a, b, out) == []


def test_a_process_run_with_no_trigger_replays_by_running_its_argv_again(run_workflow, replayed):
    """A process run with no SDK trigger replays by running its recorded argv again.

    Every call is answered from the recording, and the replay is one run with the same trigger."""
    recorded, replay = replayed(W, "verbs")
    [source] = recorded.stored().list_runs()
    assert source.attribution == "process" and source.trigger is not None
    [mine] = [r for r in replay.stored().list_runs() if r.replay_of == source.run_id]
    assert (mine.run_id, mine.trigger) == (replay.process_run_id(), source.trigger)
    assert replay.agent_exit_code() == recorded.exit_code == 0
    assert [start["replay"] for start in replay.events("start")] == [replay.process_run_id()]
    assert replay.answered() == [
        (label, status, "recording" if by is None else by)
        for label, status, by in recorded.answered()
    ]
    assert replay.internet.requests() == []
    assert replay.exit_code == 0
    # The header says only the argv ran again: nothing else of the process is recorded.
    argv = shlex.join(source.trigger.argv)
    assert replay.replay_result() == [
        f"irimi replay · {source.run_id} -> {replay.process_run_id()}",
        f"  re-ran {argv} (argv only: environment, cwd and stdin are not recorded)",
        "  agent exited 0",
        "  no divergence: every call was answered from the recording",
        "  9 from the recording · 0 missed",
        "",
        *(
            f"  = ○ stripe  {request}  faked; not checked (unclassified)"
            for request in (
                "DELETE api.stripe.com/v1/customers/cus_GAUNTLET",
                "PATCH api.stripe.com/v1/customers/cus_GAUNTLET",
                "PUT api.stripe.com/v1/charges/ch_GAUNTLET",
                "POST api.stripe.com/v1/subscriptions",
            )
        ),
        "  = ○ stripe  cancel pi_GAUNTLET  faked; not checked",
        "  = ○ slack  add :eyes: to a message in C0GAUNT  faked; not checked",
        "",
        "  6 same · 0 changed · 0 only in A · 0 only in B",
    ]
