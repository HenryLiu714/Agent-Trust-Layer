"""`irimi compare`: two stored runs' writes side by side, and no verdict (#79).

Each test stores two runs with `DirectoryStore`, built from hand-made exchanges and tool calls,
and reads them back through the real `irimi compare`, which loads the shipped maps as `irimi runs
show` does. `tests/workflows` compares runs the real `irimi shadow` recorded.
"""

import dataclasses
import json

import pytest

from irimi import compare, redact, report, trace
from irimi.cli import main
from irimi.exchange import (
    BODY_TRUNCATED_FLAG,
    IDEMPOTENT_REPLAY_FLAG,
    Exchange,
    Request,
    Response,
)
from irimi.servicemap import MapError
from irimi.store import DirectoryStore, StoredRun
from irimi.trace import Event, RunRecord, ToolCall, Trigger
from tests import test_phase_exit

home = test_phase_exit.home  # bound here so pytest finds it

AT = 1_700_000_000.0
FORM = (("content-type", "application/x-www-form-urlencoded"),)
TICKET = {"ticket": "T-1"}
HEADER = "irimi compare · a vs b · a comparison, not a verdict: nothing was replayed"
LABELS = [
    "  A  refund_ticket  agent:refund_ticket  agent 1.0",
    "  B  refund_ticket  agent:refund_ticket  agent 1.0",
]
REFUND = "○ refund $49.00 on ch_1  unvalidated (L3 preconditions passed)"
TOOL = "○ tool db.mark_refunded  shadow stand-in ran"


def _refund(amount: int = 4900, **changes) -> Exchange:
    """A refund irimi faked after its L3 read passed, as `report` prints `REFUND` for 4900."""
    body = f"charge=ch_1&amount={amount}&currency=usd".encode()
    refund = Exchange(
        request=Request("POST", "https", "api.stripe.com", 443, "/v1/refunds", "", FORM, body),
        response=Response(200, (), b"{}"),
        service="stripe",
        operation="refunds.create",
        kind="write",
        answered_by="fake-L1",
        validation="unvalidated",
        run_id="",
        precondition="passed",
    )
    return dataclasses.replace(refund, **changes)


def _post(text: str) -> Exchange:
    body = json.dumps({"channel": "C1", "text": text}).encode()
    headers = (("content-type", "application/json"),)
    return Exchange(
        request=Request(
            "POST", "https", "slack.com", 443, "/api/chat.postMessage", "", headers, body
        ),
        response=Response(200, (), b'{"ok":true}'),
        service="slack",
        operation="chat.postMessage",
        kind="write",
        answered_by="fake-L1",
        validation="unvalidated",
        run_id="",
    )


def _read(path: str, answered: bytes = b"{}") -> Exchange:
    return Exchange(
        request=Request("GET", "https", "api.stripe.com", 443, path, "", (), b""),
        response=Response(200, (), answered),
        service="stripe",
        operation="charges.retrieve",
        kind="read",
        answered_by="live",
        validation="unvalidated",
        run_id="",
    )


def _llm(prompt: str) -> Exchange:
    body = json.dumps({"messages": [{"role": "user", "content": prompt}]}).encode()
    headers = (("content-type", "application/json"),)
    return Exchange(
        request=Request(
            "POST", "https", "api.anthropic.com", 443, "/v1/messages", "", headers, body
        ),
        response=Response(200, (), b"{}"),
        service="anthropic",
        operation="messages.create",
        kind="llm",
        answered_by="live",
        validation="unvalidated",
        run_id="",
    )


def _tool(name: str = "db.mark_refunded", kind: str = "write", ran: str = "shadow") -> ToolCall:
    return ToolCall("tc", "", name, kind, ran, {}, None, None, AT, AT + 1)  # type: ignore[arg-type]


def _record(run_id: str, args: trace.JSONValue = TICKET, **changes) -> RunRecord:
    record = RunRecord(
        schema_version=trace.SCHEMA_VERSION,
        run_id=run_id,
        mode="shadow",
        attribution="sdk",
        trigger=Trigger("refund_ticket", "agent:refund_ticket", args, True),
        agent_version="1.0",
        engine_version="0.0.0",
        sdk_version="0.0.0",
        started_at=AT,
        ended_at=AT + 1,
        outcome="ok",
        error=None,
        exit_code=None,
    )
    return dataclasses.replace(record, **changes)


def _store(home, runs: dict[str, tuple[RunRecord, list[Event]]]) -> None:
    """Each run's record and its events, in `seq` order, in `home`'s default store."""
    s = DirectoryStore(home / "store", redact.load_key(home))
    for run_id, (record, events) in runs.items():
        s.start_run(record)
        for event in events:
            if isinstance(event, ToolCall):
                s.record_tool_call(dataclasses.replace(event, run_id=run_id))
            else:
                s.record(dataclasses.replace(event, run_id=run_id))
    s.close()


def _run(events: list[Event]) -> StoredRun:
    """A run as `StoreReader.load_run` returns one, never stored."""
    return StoredRun(_record("r"), events)


def _compare(home, capfd, a: list[Event], b: list[Event], **b_record) -> tuple[int, list[str]]:
    """`irimi compare a b` over two runs holding `a`'s and `b`'s events; its exit code and the
    lines it printed after the header. Nothing may go to stderr."""
    _store(home, {"a": (_record("a"), a), "b": (_record("b", **b_record), b)})
    code = main(["compare", "a", "b"])
    out, err = capfd.readouterr()
    assert err == ""
    lines = out.splitlines()
    assert lines[0] == HEADER
    return code, lines[1:]


def test_two_runs_that_wrote_the_same_print_every_write_as_same_and_exit_0(home, capfd):
    events = [_read("/v1/charges/ch_1"), _refund(), _tool()]
    assert _compare(home, capfd, events, events) == (
        0,
        [
            *LABELS,
            "",
            f"  = {REFUND}",
            f"  = {TOOL}",
            "",
            "  2 same · 0 changed · 0 only in A · 0 only in B",
        ],
    )


def test_a_refund_of_another_amount_is_one_write_changed(home, capfd):
    code, lines = _compare(home, capfd, [_refund(), _tool()], [_refund(2450), _tool()])
    assert (code, lines[3:]) == (
        1,
        [
            f"  ~ A {REFUND}",
            "    B ○ refund $24.50 on ch_1  unvalidated (L3 preconditions passed)",
            f"  = {TOOL}",
            "",
            "  1 same · 1 changed · 0 only in A · 0 only in B",
        ],
    )


def test_a_slack_post_only_b_made_is_one_write_added(home, capfd):
    code, lines = _compare(home, capfd, [_refund()], [_refund(), _post("refunded T-1")])
    assert (code, lines[3:]) == (
        1,
        [
            f"  = {REFUND}",
            '  + ○ post to #C1: "refunded T-1"  unvalidated (L1)',
            "",
            "  1 same · 0 changed · 0 only in A · 1 only in B",
        ],
    )


def test_a_write_tool_b_dropped_is_one_write_only_in_a(home, capfd):
    code, lines = _compare(home, capfd, [_refund(), _tool()], [_refund()])
    assert (code, lines[3:]) == (
        1,
        [f"  = {REFUND}", f"  - {TOOL}", "", "  1 same · 0 changed · 1 only in A · 0 only in B"],
    )


def test_a_refund_l3_rejected_in_b_is_the_same_write_changed_and_shows_its_mark(home, capfd):
    rejected = _refund(9800, precondition="rejected", rejection_code="amount_too_large")
    code, lines = _compare(home, capfd, [_refund()], [rejected])
    assert (code, lines[3:]) == (
        1,
        [
            f"  ~ A {REFUND}",
            "    B ✗ refund $98.00 on ch_1  would fail: amount_too_large",
            "",
            "  0 same · 1 changed · 0 only in A · 0 only in B",
        ],
    )


def test_a_second_refused_refund_is_the_write_added_not_the_first(home, capfd):
    """Lined up by key alone, B's first refund pairs with nothing and its refused second one with
    A's refund: `+` on the refund both runs made, and `~` against the refusal. Lining up the same
    writes first leaves the refusal as the one B added (#79)."""
    refused = _refund(precondition="rejected", rejection_code="charge_already_refunded")
    code, lines = _compare(home, capfd, [_refund(), _tool()], [_refund(), refused, _tool()])
    assert (code, lines[3:]) == (
        1,
        [
            f"  = {REFUND}",
            "  + ✗ refund $49.00 on ch_1  would fail: charge_already_refunded",
            f"  = {TOOL}",
            "",
            "  2 same · 0 changed · 0 only in A · 1 only in B",
        ],
    )


def test_reads_llm_calls_read_tools_real_tools_and_a_replayed_retry_are_not_writes(home, capfd):
    a = [_read("/v1/charges/ch_1"), _llm("refund it"), _refund(), _tool()]
    replayed = _refund(flags=(IDEMPOTENT_REPLAY_FLAG,))
    b = [
        _llm("refund it, politely"),
        _read("/v1/charges/ch_1", b'{"amount": 1}'),
        _read("/v1/customers/cus_1"),
        _tool("db.lookup", kind="read", ran="real"),
        # A write tool the SDK ran for real is in no summary: it posts none, and none is a write
        # that did not happen.
        _tool("db.archive", ran="real"),
        _refund(),
        replayed,
        _tool(),
    ]
    code, lines = _compare(home, capfd, a, b)
    assert (code, lines[3:]) == (
        0,
        [f"  = {REFUND}", f"  = {TOOL}", "", "  2 same · 0 changed · 0 only in A · 0 only in B"],
    )


def test_a_delegated_write_is_a_write_and_prints_where_it_went(home, capfd):
    """The summary prints a write an answer target answered, so it is compared too."""
    delegated = _refund(answered_by="delegated", target="http://127.0.0.1:9999", precondition=None)
    code, lines = _compare(home, capfd, [_refund()], [delegated])
    assert (code, lines[3:5]) == (
        1,
        [
            f"  ~ A {REFUND}",
            "    B ○ refund $49.00 on ch_1 → http://127.0.0.1:9999  unvalidated (delegated)",
        ],
    )


def test_two_writes_whose_bodies_were_cut_are_not_compared_and_not_called_the_same(home, capfd):
    cut = _refund(2450, flags=(BODY_TRUNCATED_FLAG,))
    code, lines = _compare(home, capfd, [_refund(), _tool()], [cut, _tool()])
    assert (code, lines[3:]) == (
        1,
        [
            f"  ? A {REFUND}",
            "    B ○ refund $24.50 on ch_1  unvalidated (L3 preconditions passed)",
            f"  = {TOOL}",
            "",
            "  1 same · 0 changed · 0 only in A · 0 only in B · 1 not compared "
            "(a body was cut when stored)",
        ],
    )
    # A cut body whose write still reads the same is the same.
    same = _refund(flags=(BODY_TRUNCATED_FLAG,))
    assert compare.align(*(compare.actions(_run(e), None) for e in ([_refund()], [same]))) == (
        compare.Row(
            "same",
            compare.Action(
                (compare.EXCHANGE_KEY, "stripe", "refunds.create"),
                "○ POST api.stripe.com/v1/refunds  unvalidated (L3 preconditions passed)",
            ),
            compare.Action(
                (compare.EXCHANGE_KEY, "stripe", "refunds.create"),
                "○ POST api.stripe.com/v1/refunds  unvalidated (L3 preconditions passed)",
                cut=True,
            ),
        ),
    )


def test_runs_that_wrote_nothing_print_only_the_totals_and_exit_0(home, capfd):
    code, lines = _compare(home, capfd, [_read("/v1/charges/ch_1")], [])
    assert (code, lines) == (0, [*LABELS, "", "  0 same · 0 changed · 0 only in A · 0 only in B"])


@pytest.mark.parametrize(
    "args", [{"ticket": "T-2"}, {"ticket": "T-1", "n": 1}, {"ticket": True}, {"ticket": ["T-1"]}]
)
def test_triggers_given_other_args_say_so_under_the_labels(home, capfd, args):
    _, lines = _compare(home, capfd, [_refund()], [_refund()], args=args)
    assert lines[:4] == [*LABELS, "  (the triggers' args differ)", ""]


def test_args_are_compared_as_json_so_true_is_not_1_and_key_order_is_nothing():
    one, true = _record("a", args={"n": 1, "m": 2}), _record("b", args={"n": True, "m": 2})
    assert compare.header_lines(one, true)[3:] == ["  (the triggers' args differ)"]
    assert compare.header_lines(one, _record("b", args={"m": 2, "n": 1}))[3:] == []


def test_args_nested_deeper_than_json_writes_are_called_different_and_never_raise():
    """A million deep, past what either Python's `json.dumps` writes, as `test_runs` builds it."""
    deep: trace.JSONValue = []
    for _ in range(1_000_000):
        deep = [deep]
    a, b = _record("a", args=deep), _record("b", args=deep)
    assert compare.header_lines(a, b)[3:] == ["  (the triggers' args differ)"]


def test_a_write_s_text_is_escaped_so_one_write_is_one_line_and_printing_never_raises(home, capfd):
    """A Slack post's text is the agent's own: a newline split the row, an escape sequence was
    obeyed by the terminal, and a lone surrogate raised out of `print` (#79)."""
    code, lines = _compare(home, capfd, [_post("hi")], [_post("hi\nthere\x1b[31m\ud800")])
    assert (code, lines[3:5]) == (
        1,
        [
            '  ~ A ○ post to #C1: "hi"  unvalidated (L1)',
            '    B ○ post to #C1: "hi\\nthere\\x1b[31m\\ud800"  unvalidated (L1)',
        ],
    )


def test_a_tool_never_lines_up_with_an_exchange_of_a_service_named_tool(home, capfd):
    """An exchange's key is longer than a tool call's, so a service a map calls `tool` cannot pair
    one of its writes with a tool call of its operation's name (#79)."""
    write = dataclasses.replace(_refund(), service="tool", operation="db.mark_refunded")
    code, lines = _compare(home, capfd, [write], [_tool()])
    assert (code, [line[:3] for line in lines[3:5]]) == (1, ["  -", "  +"])


def test_a_run_with_no_trigger_or_version_is_labelled_with_dashes(home, capfd):
    _store(
        home,
        {
            "a": (_record("a", trigger=None, agent_version=None, attribution="process"), []),
            "b": (_record("b", trigger=Trigger("t\nx", None, {"n": 1}, False)), []),
        },
    )
    assert main(["compare", "a", "b"]) == 0
    assert capfd.readouterr().out.splitlines()[1:4] == [
        "  A  -  -  agent -",
        "  B  t\\nx  -  agent 1.0",
        "  (the triggers' args differ)",
    ]


@pytest.mark.parametrize("missing", ["a", "b"])
def test_an_unknown_run_exits_2_with_the_documented_message(home, capfd, missing):
    present = "b" if missing == "a" else "a"
    _store(home, {present: (_record(present), [])})
    assert main(["compare", "a", "b"]) == compare.TROUBLE
    out, err = capfd.readouterr()
    assert (out, err) == ("", f"error: no run {missing} in {home / 'store'}\n")


def test_a_damaged_run_exits_2_and_names_it(home, capfd):
    _store(home, {"a": (_record("a"), [_refund()]), "b": (_record("b"), [])})
    for blob in (home / "store" / "blobs").iterdir():
        blob.unlink()
    assert main(["compare", "a", "b"]) == compare.TROUBLE
    out, err = capfd.readouterr()
    assert out == ""
    assert err.startswith("error: run a could not be read: blob ")


def test_a_store_named_with_store_is_the_one_read(home, capfd, tmp_path):
    elsewhere = tmp_path / "elsewhere"
    s = DirectoryStore(elsewhere, redact.load_key(home))
    s.start_run(_record("a"))
    s.start_run(_record("b"))
    s.close()
    assert main(["compare", "a", "b", "--store", str(elsewhere)]) == 0
    assert main(["compare", "a", "b"]) == compare.TROUBLE
    assert capfd.readouterr().err == f"error: no run a in {home / 'store'}\n"


def test_writes_print_as_requests_with_a_warning_when_the_maps_do_not_load(
    home, capfd, monkeypatch
):
    _store(home, {"a": (_record("a"), [_refund()]), "b": (_record("b"), [_refund()])})

    def refuse(**_kwargs):
        raise MapError("broken")

    monkeypatch.setattr("irimi.servicemap.load", refuse)
    assert main(["compare", "a", "b"]) == 0
    out, err = capfd.readouterr()
    assert err == "warning: maps did not load; writes are shown as requests\n"
    assert out.splitlines()[4] == (
        "  = ○ POST api.stripe.com/v1/refunds  unvalidated (L3 preconditions passed)"
    )


def test_an_action_s_text_is_its_summary_line_without_the_indent():
    """The comparison and the summary print one string for one write (#79)."""
    run = _run([_refund(), _tool()])
    summary = report.stored_summary_lines(run, None)
    assert [f"  {a.text}" for a in compare.actions(run, None)] == [
        line for line in summary if line.startswith("  ○ ")
    ]
