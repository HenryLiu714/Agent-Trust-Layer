"""`irimi compare`: two stored runs' writes side by side.

Runs are stored from hand-made events and read back through the real command and shipped maps."""

import dataclasses
import json
import random
import unicodedata

import pytest

from irimi import servicemaps
from irimi.cli import main
from irimi.cli.compose import maps_for_stored_runs
from irimi.core import exitcodes
from irimi.core.exchange import Exchange, Flag, Precondition, Request, Response
from irimi.protocol import trace
from irimi.protocol.trace import ErrorInfo, Event, RunRecord, StoredRun, ToolCall, Trigger
from irimi.record import redact
from irimi.record.store.writer import DirectoryStore
from irimi.replay import compare
from irimi.replay.index import compared_body
from irimi.report.model import RunView
from irimi.report.summary import summary_lines
from irimi.servicemaps import MapError
from tests.support import builders
from tests.support.builders import FORM_HEADER

AT = 1_700_000_000.0
FORM = (FORM_HEADER,)
TICKET: dict[str, trace.JSONValue] = {"ticket": "T-1"}
HEADER = "irimi compare · a vs b"
LABELS = [
    "  A  refund_ticket  agent:refund_ticket  agent 1.0",
    "  B  refund_ticket  agent:refund_ticket  agent 1.0",
]
REFUND = "○ stripe  refund $49.00 on ch_1  faked; checked against live state: would succeed"
TOOL = "○ tool  db.mark_refunded  fake ran instead"


def _refund(amount: int = 4900, **changes) -> Exchange:
    """A refund irimi faked after its precondition check passed; `REFUND` prints it at 4900."""
    return builders.refund_exchange(amount, **changes)


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
        run_id="",
    )


def _tool(name: str = "db.mark_refunded", kind: str = "write", ran: str = "shadow") -> ToolCall:
    return builders.tool_call(
        tool_call_id="tc", run_id="", name=name, kind=kind, ran=ran, started_at=AT, ended_at=AT + 1
    )


def _record(run_id: str, args: trace.JSONValue = TICKET, **changes) -> RunRecord:
    """An SDK run of `refund_ticket` that started at `AT` and ended a second later."""
    end = trace.RunEnd(changes.pop("ended_at", AT + 1), "ok", None, None)
    record = builders.run_record(
        run_id,
        AT,
        attribution="sdk",
        trigger=Trigger("refund_ticket", "agent:refund_ticket", args, True),
        agent_version="1.0",
        sdk_version="0.0.0",
        end=end,
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
                assert isinstance(event, Exchange)
                s.record(dataclasses.replace(event, run_id=run_id))
    s.close()


def _run(events: list[Event]) -> StoredRun:
    """A run as `StoreReader.load_run` returns one, never stored."""
    return StoredRun(_record("r"), events)


def _compare(home, capfd, a: list[Event], b: list[Event], **b_record) -> tuple[int, list[str]]:
    """Return `irimi compare a b`'s exit code and the lines after its header.

    The two runs hold `a`'s and `b`'s events. Nothing may go to stderr."""
    _store(home, {"a": (_record("a"), a), "b": (_record("b", **b_record), b)})
    code = main(["compare", "a", "b"])
    out, err = capfd.readouterr()
    assert err == ""
    lines = out.splitlines()
    assert lines[0] == HEADER
    return code, lines[1:]


def test_two_runs_that_wrote_the_same_print_every_write_as_same_and_exit_0(home, capfd):
    events: list[Event] = [_read("/v1/charges/ch_1"), _refund(), _tool()]
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
            "    B ○ stripe  refund $24.50 on ch_1  faked; checked against live state: "
            "would succeed",
            f"  = {TOOL}",
            "",
            "  1 same · 1 changed · 0 only in A · 0 only in B",
        ],
    )


def _update(body: bytes) -> Exchange:
    """A customer update irimi faked: `update customer {customer}` names no body field."""
    path = "/v1/customers/cus_1"
    return Exchange(
        request=Request("POST", "https", "api.stripe.com", 443, path, "", FORM, body),
        response=Response(200, (), b"{}"),
        service="stripe",
        operation="customers.update",
        kind="write",
        answered_by="fake-L1",
        run_id="",
    )


UPDATE = "○ stripe  update customer cus_1  faked; not checked"


def test_two_updates_that_print_alike_but_send_other_fields_are_changed_and_say_which(home, capfd):
    """Two updates whose map sentence names only the customer are compared by their fields."""
    a: list[Event] = [_update(b"email=alice%40x.com")]
    b: list[Event] = [_update(b"metadata[blocked]=true&email=")]
    code, lines = _compare(home, capfd, a, b)
    assert (code, lines[3:]) == (
        1,
        [
            f"  ~ A {UPDATE}",
            f"    B {UPDATE}",
            '      body.email: "alice@x.com" -> ""',
            '      body.metadata: (absent) -> {"blocked":"true"}',
            "",
            "  0 same · 1 changed · 0 only in A · 0 only in B",
        ],
    )


def test_a_refund_whose_reason_alone_differs_is_changed(home, capfd):
    a = _refund()
    b = dataclasses.replace(
        a, request=dataclasses.replace(a.request, body=a.request.body + b"&reason=fraudulent")
    )
    code, lines = _compare(home, capfd, [a], [b])
    assert (code, lines[3:6]) == (
        1,
        [f"  ~ A {REFUND}", f"    B {REFUND}", '      body.reason: (absent) -> "fraudulent"'],
    )


def test_an_ignored_field_is_not_compared_and_any_other_is():
    """A field in the route's `compare_ignores:` is left out, as a replay's match leaves it out.

    The test map is the shipped one with `nonce` ignored on the refund route."""
    index = servicemaps.MapIndex(
        tuple(
            dataclasses.replace(
                sm,
                routes=tuple(
                    dataclasses.replace(route, compare_ignores=("nonce",))
                    if route.operation == "refunds.create"
                    else route
                    for route in sm.routes
                ),
            )
            for sm in servicemaps.loader.load_shipped()
        )
    )

    def refund(body: bytes) -> Exchange:
        request = dataclasses.replace(_refund().request, body=body)
        return dataclasses.replace(_refund(), request=request)

    one = refund(b"charge=ch_1&amount=4900&currency=usd&nonce=n-1")
    other = refund(b"currency=usd&nonce=n-2&amount=4900&charge=ch_1")
    assert compare.compare_runs(_run([one]), _run([other]), index).exit_code == exitcodes.OK
    reason = refund(b"charge=ch_1&amount=4900&currency=usd&nonce=n-1&reason=duplicate")
    assert compare.compare_runs(_run([one]), _run([reason]), index).exit_code == exitcodes.DIFFERENT


def test_a_write_answered_otherwise_is_changed_though_its_line_reads_the_same():
    """What irimi did with a write is compared too: here, the currency its check read found.

    The target and a refusal's reasons are compared the same way."""
    index = servicemaps.load()
    a = dataclasses.replace(_update(b"email=a%40x.com"), precondition=Precondition(currency="usd"))
    b = dataclasses.replace(a, precondition=Precondition(currency="eur"))
    assert compare.compare_runs(_run([a]), _run([b]), index).lines() == [
        f"  ~ A {UPDATE}",
        f"    B {UPDATE}",
        '      currency: "usd" -> "eur"',
        "",
        "  0 same · 1 changed · 0 only in A · 0 only in B",
    ]


def test_a_field_diff_shows_five_fields_each_cut_to_a_readable_length():
    many = "&".join(f"metadata[k{n}]={n}" for n in range(3))
    a = _update(f"name={'x' * 100}&email=a&phone=p1&{many}".encode())
    added = "description=d&invoice_prefix=P&balance=5"
    b = _update(f"name={'y' * 100}&email=b&phone=p2&{added}&{many}".encode())
    lines = compare.compare_runs(_run([a]), _run([b]), servicemaps.load()).lines()
    # In name order, as a body's fields are compared; a form integer is a number.
    assert lines[2:8] == [
        "      body.balance: (absent) -> 5",
        '      body.description: (absent) -> "d"',
        '      body.email: "a" -> "b"',
        '      body.invoice_prefix: (absent) -> "P"',
        f'      body.name: "{"x" * 59}… -> "{"y" * 59}…',
        "      and 1 more",
    ]


def test_two_stand_ins_given_other_args_are_changed_and_print_the_args():
    a = dataclasses.replace(_tool(), args={"ticket": "T-1"})
    b = dataclasses.replace(_tool(), args={"ticket": "T-2"})
    assert compare.compare_runs(_run([a]), _run([b]), None).lines() == [
        f"  ~ A {TOOL}",
        f"    B {TOOL}",
        '      args.ticket: "T-1" -> "T-2"',
        "",
        "  0 same · 1 changed · 0 only in A · 0 only in B",
    ]
    again = dataclasses.replace(_tool(), args={"ticket": "T-1"})
    assert compare.compare_runs(_run([a]), _run([again]), None).exit_code == exitcodes.OK


def test_an_update_to_another_customer_is_changed_by_the_object_its_path_names():
    other = dataclasses.replace(
        _update(b"email=a"),
        request=dataclasses.replace(
            _update(b"").request, path="/v1/customers/cus_2", body=b"email=a"
        ),
    )
    lines = compare.compare_runs(_run([_update(b"email=a")]), _run([other]), servicemaps.load())
    assert lines.lines()[:2] == [
        f"  ~ A {UPDATE}",
        "    B ○ stripe  update customer cus_2  faked; not checked",
    ]


def test_a_webhook_s_redacted_path_is_not_compared_and_its_text_is():
    """A webhook's redacted path is left out of the comparison, and its text is compared.

    The path is redacted with each install's own key, so two installs spell one webhook apart."""

    def hook(path: str, text: str) -> Exchange:
        body = json.dumps({"text": text}).encode()
        headers = (("content-type", "application/json"),)
        return Exchange(
            request=Request("POST", "https", "hooks.slack.com", 443, path, "", headers, body),
            response=Response(200, (), b"ok"),
            service="slack",
            operation="incoming_webhook",
            kind="write",
            answered_by="fake-L0",
            run_id="",
        )

    index = servicemaps.load()
    a, b = hook("/<redacted:aaaa>", "hi"), hook("/<redacted:bbbb>", "hi")
    assert compare.compare_runs(_run([a]), _run([b]), index).exit_code == exitcodes.OK
    c = hook("/<redacted:aaaa>", "bye")
    assert compare.compare_runs(_run([a]), _run([c]), index).exit_code == exitcodes.DIFFERENT


def test_an_llm_body_is_compared_less_who_asked_and_how_as_a_replay_matches_it():
    """An llm body is compared less its route's `compare_ignores:` and every `cache_control`."""
    asked = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    noisy = {
        **asked,
        "metadata": {"user_id": "u-1"},
        "stream": True,
        "user": "u-1",
        "messages": [{"role": "user", "content": "hi", "cache_control": {"type": "ephemeral"}}],
    }
    request = _llm("hi").request
    body = dataclasses.replace(request, body=json.dumps(noisy).encode())
    found = servicemaps.load().route_for(request.host, request.method, request.path)
    assert found is not None
    assert compared_body(body, "llm", found[1]) == asked


def test_a_slack_post_only_b_made_is_one_write_added(home, capfd):
    code, lines = _compare(home, capfd, [_refund()], [_refund(), _post("refunded T-1")])
    assert (code, lines[3:]) == (
        1,
        [
            f"  = {REFUND}",
            '  + ○ slack  post to #C1: "refunded T-1"  faked; not checked',
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
    rejected = _refund(9800, precondition=Precondition("rejected", "amount_too_large"))
    code, lines = _compare(home, capfd, [_refund()], [rejected])
    assert (code, lines[3:]) == (
        1,
        [
            f"  ~ A {REFUND}",
            "    B ✗ stripe  refund $98.00 on ch_1  faked; would fail: amount_too_large",
            "",
            "  0 same · 1 changed · 0 only in A · 0 only in B",
        ],
    )


def test_a_second_refused_refund_is_the_write_added_not_the_first(home, capfd):
    """Equal writes pair first, so B's refused second refund is the one marked `+`."""
    refused = _refund(precondition=Precondition("rejected", "charge_already_refunded"))
    code, lines = _compare(home, capfd, [_refund(), _tool()], [_refund(), refused, _tool()])
    assert (code, lines[3:]) == (
        1,
        [
            f"  = {REFUND}",
            "  + ✗ stripe  refund $49.00 on ch_1  faked; would fail: charge_already_refunded",
            f"  = {TOOL}",
            "",
            "  2 same · 0 changed · 0 only in A · 1 only in B",
        ],
    )


def test_reads_llm_calls_read_tools_real_tools_and_a_replayed_retry_are_not_writes(home, capfd):
    a: list[Event] = [_read("/v1/charges/ch_1"), _llm("refund it"), _refund(), _tool()]
    replayed = _refund(flags=(Flag.IDEMPOTENT_REPLAY,))
    b: list[Event] = [
        _llm("refund it, politely"),
        _read("/v1/charges/ch_1", b'{"amount": 1}'),
        _read("/v1/customers/cus_1"),
        _tool("db.lookup", kind="read", ran="real"),
        # A write tool the SDK ran for real is in no summary: it is not a faked write.
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
    """The summary prints a write a target answered, so it is compared too."""
    delegated = _refund(
        answered_by="delegated", target="http://127.0.0.1:9999", precondition=Precondition()
    )
    code, lines = _compare(home, capfd, [_refund()], [delegated])
    assert (code, lines[3:5]) == (
        1,
        [
            f"  ~ A {REFUND}",
            "    B ○ stripe  refund $49.00 on ch_1  sent to target 127.0.0.1:9999",
        ],
    )


def test_two_writes_whose_bodies_were_cut_are_not_compared_and_not_called_the_same(home, capfd):
    cut = _refund(2450, flags=(Flag.BODY_TRUNCATED,))
    code, lines = _compare(home, capfd, [_refund(), _tool()], [cut, _tool()])
    assert (code, lines[3:]) == (
        1,
        [
            f"  ? A {REFUND}",
            "    B ○ stripe  refund $24.50 on ch_1  faked; checked against live state: "
            "would succeed",
            f"  = {TOOL}",
            "",
            "  1 same · 0 changed · 0 only in A · 0 only in B · 1 not compared "
            "(a body was cut when stored)",
        ],
    )


def test_a_cut_pair_whose_lines_read_the_same_is_still_not_compared_and_exits_1(home, capfd):
    """A pair with either side cut is `?` whatever its lines say.

    A cut body prints one line for different requests, so two cut refunds can print alike."""
    cut = _refund(flags=(Flag.BODY_TRUNCATED,))
    code, lines = _compare(home, capfd, [_refund(), _tool()], [cut, _tool()])
    assert (code, lines[3:]) == (
        1,
        [
            f"  ? A {REFUND}",
            f"    B {REFUND}",
            f"  = {TOOL}",
            "",
            "  1 same · 0 changed · 0 only in A · 0 only in B · 1 not compared "
            "(a body was cut when stored)",
        ],
    )
    # Nor is a cut write the same as itself: a run compared with itself says so.
    _store(home, {"c": (_record("c"), [cut, _tool()])})
    assert main(["compare", "c", "c"]) == 1
    assert capfd.readouterr().out.splitlines()[4:] == [
        f"  ? A {REFUND}",
        f"    B {REFUND}",
        f"  = {TOOL}",
        "",
        "  1 same · 0 changed · 0 only in A · 0 only in B · 1 not compared "
        "(a body was cut when stored)",
    ]


def test_a_cut_write_no_write_of_its_key_pairs_with_is_only_in_its_run(home, capfd):
    cut = _post("hi")
    cut = dataclasses.replace(cut, flags=(Flag.BODY_TRUNCATED,))
    code, lines = _compare(home, capfd, [_refund(), cut], [_refund()])
    assert (code, lines[3:]) == (
        1,
        [
            f"  = {REFUND}",
            '  - ○ slack  post to #C1: "hi"  faked; not checked',
            "",
            "  1 same · 0 changed · 1 only in A · 0 only in B",
        ],
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
    """Args a million deep, past what `json.dumps` writes, compare as different."""
    deep: trace.JSONValue = []
    for _ in range(1_000_000):
        deep = [deep]
    a, b = _record("a", args=deep), _record("b", args=deep)
    assert compare.header_lines(a, b)[3:] == ["  (the triggers' args differ)"]


def test_a_write_s_text_is_escaped_so_one_write_is_one_line_and_printing_never_raises(home, capfd):
    """A post's text is escaped: newlines, escape sequences and lone surrogates.

    Unescaped, a newline would split the row and a lone surrogate would raise out of `print`."""
    code, lines = _compare(home, capfd, [_post("hi")], [_post("hi\nthere\x1b[31m\ud800")])
    assert (code, lines[3:5]) == (
        1,
        [
            '  ~ A ○ slack  post to #C1: "hi"  faked; not checked',
            '    B ○ slack  post to #C1: "hi\\nthere\\x1b[31m\\ud800"  faked; not checked',
        ],
    )


def test_a_tool_never_lines_up_with_an_exchange_of_a_service_named_tool(home, capfd):
    """A write of a service named `tool` never pairs with a tool call of its operation's name."""
    write = dataclasses.replace(_refund(), service="tool", operation="db.mark_refunded")
    code, lines = _compare(home, capfd, [write], [_tool()])
    assert (code, [line[:3] for line in lines[3:5]]) == (1, ["  -", "  +"])


def test_a_label_leaves_out_what_the_run_did_not_record(home, capfd):
    _store(
        home,
        {
            "a": (_record("a", trigger=None, agent_version=None, attribution="process"), []),
            "b": (_record("b", trigger=Trigger("t\nx", None, {"n": 1}, False)), []),
        },
    )
    assert main(["compare", "a", "b"]) == 0
    assert capfd.readouterr().out.splitlines()[1:4] == [
        "  A",
        "  B  t\\nx  agent 1.0",
        "  (the triggers' args differ)",
    ]


def test_a_label_escapes_the_entrypoint_and_the_agent_version_too(home, capfd):
    """The entrypoint and agent version are escaped as `runs show` escapes them."""
    trigger = Trigger("t", "mod:f\x1b[2J", {"n": 1}, False)
    _store(
        home,
        {
            "a": (_record("a", trigger=trigger, agent_version="1.0\nB  forged"), []),
            "b": (_record("b", trigger=trigger, agent_version="\ud800"), []),
        },
    )
    assert main(["compare", "a", "b"]) == 0
    assert capfd.readouterr().out.splitlines()[1:4] == [
        "  A  t  mod:f\\x1b[2J  agent 1.0\\nB  forged",
        "  B  t  mod:f\\x1b[2J  agent \\ud800",
        "",
    ]


@pytest.mark.parametrize("missing", ["a", "b"])
def test_an_unknown_run_exits_2_with_the_documented_message(home, capfd, missing):
    present = "b" if missing == "a" else "a"
    _store(home, {present: (_record(present), [])})
    assert main(["compare", "a", "b"]) == exitcodes.ERROR
    out, err = capfd.readouterr()
    assert (out, err) == ("", f"error: no run {missing} in {home / 'store'}\n")


def test_a_damaged_run_exits_2_and_names_it(home, capfd):
    _store(home, {"a": (_record("a"), [_refund()]), "b": (_record("b"), [])})
    for blob in (home / "store" / "blobs").iterdir():
        blob.unlink()
    assert main(["compare", "a", "b"]) == exitcodes.ERROR
    out, err = capfd.readouterr()
    events = home / "store" / "runs" / "a" / "events.jsonl"
    assert out == ""
    assert err.startswith(f"error: run a could not be read: {events}, line 1: blob ")


def test_a_store_named_with_store_is_the_one_read(home, capfd, tmp_path):
    elsewhere = tmp_path / "elsewhere"
    s = DirectoryStore(elsewhere, redact.load_key(home))
    s.start_run(_record("a"))
    s.start_run(_record("b"))
    s.close()
    assert main(["compare", "a", "b", "--store", str(elsewhere)]) == 0
    assert main(["compare", "a", "b"]) == exitcodes.ERROR
    assert capfd.readouterr().err == f"error: no run a in {home / 'store'}\n"


def test_writes_print_as_requests_with_a_warning_when_the_maps_do_not_load(
    home, capfd, monkeypatch
):
    _store(home, {"a": (_record("a"), [_refund()]), "b": (_record("b"), [_refund()])})

    def refuse(**_kwargs):
        raise MapError("broken")

    monkeypatch.setattr("irimi.servicemaps.load", refuse)
    assert main(["compare", "a", "b"]) == 0
    out, err = capfd.readouterr()
    assert err == "warning: maps did not load; writes are shown as requests\n"
    assert out.splitlines()[4] == (
        "  = ○ stripe  POST api.stripe.com/v1/refunds  faked; checked against live state: "
        "would succeed"
    )


def test_an_action_s_text_is_its_summary_line_without_the_indent():
    """The comparison and the summary print one string for one write."""
    run = _run([_refund(), _tool()])
    summary = summary_lines(RunView(run.record, run.events), None)
    assert [f"  {a.text}" for a in compare.actions(run, None)] == [
        line for line in summary if line.startswith("  ○ ")
    ]


def test_a_newline_and_a_typed_backslash_n_print_alike_and_are_still_not_the_same(home, capfd):
    """A newline and a typed `\\n` are changed, and the pair escapes its backslashes to differ.

    Escaping leaves a backslash as it is, so the summary prints both posts as one line."""
    code, lines = _compare(home, capfd, [_post("line1\nline2")], [_post("line1\\nline2")])
    assert (code, lines[3:]) == (
        1,
        [
            '  ~ A ○ slack  post to #C1: "line1\\nline2"  faked; not checked',
            '    B ○ slack  post to #C1: "line1\\\\nline2"  faked; not checked',
            "",
            "  0 same · 1 changed · 0 only in A · 0 only in B",
        ],
    )
    # A tool's name is its key, but the error type its fake raised is only in its line.
    raised: list[Event] = [
        dataclasses.replace(_tool(), error=ErrorInfo(kind, "")) for kind in ("a\nb", "a\\nb")
    ]
    comparison = compare.compare_runs(_run(raised[:1]), _run(raised[1:]), None)
    assert comparison.lines() == [
        "  ~ A ○ tool  db.mark_refunded  fake raised a\\nb",
        "    B ○ tool  db.mark_refunded  fake raised a\\\\nb",
        "",
        "  0 same · 1 changed · 0 only in A · 0 only in B",
    ]


def test_a_pair_printed_unlike_keeps_the_summary_s_lines_and_a_cut_pair_alike_is_not_escaped():
    """Only a pair that prints alike though it differs escapes its backslashes.

    A pair that prints unlike, or a cut write compared with itself, keeps the summary's lines."""
    index = servicemaps.load()
    pair = compare.compare_runs(_run([_post("a\\b")]), _run([_post("a\\c")]), index)
    assert pair.lines()[:2] == [
        '  ~ A ○ slack  post to #C1: "a\\b"  faked; not checked',
        '    B ○ slack  post to #C1: "a\\c"  faked; not checked',
    ]
    cut = _run([dataclasses.replace(_post("a\\b\n"), flags=(Flag.BODY_TRUNCATED,))])
    assert compare.compare_runs(cut, cut, index).lines()[:2] == [
        '  ? A ○ slack  post to #C1: "a\\b\\n"  faked; not checked',
        '    B ○ slack  post to #C1: "a\\b\\n"  faked; not checked',
    ]


def test_every_write_shape_s_action_is_the_line_the_stored_summary_prints_for_it(home):
    """For every recorded write shape, the actions are the summary's write lines, byte for byte.

    Exchanges come in `seq` order, then tool calls, as the summary prints them."""
    target = "http://127.0.0.1:9"
    # An unclassified write keeps its host as its service and its request as its operation.
    unclassified = _refund(kind="unknown", answered_by="fake-L0", precondition=Precondition())
    unclassified = dataclasses.replace(unclassified, flags=(Flag.UNCLASSIFIED,))
    events: list[Event] = [
        _read("/v1/charges/ch_1"),
        _llm("refund it"),
        _refund(),
        dataclasses.replace(unclassified, service="api.stripe.com", operation="POST /v1/refunds"),
        _refund(answered_by="delegated", target=target, precondition=Precondition()),
        _refund(
            answered_by="delegated",
            target=target,
            precondition=Precondition(),
            response=None,
            flags=(Flag.TARGET_FAILED,),
        ),
        # A body that decoded past the cap: the decision failed and its 502 is recorded.
        dataclasses.replace(
            unclassified,
            service="api.stripe.com",
            operation="",
            flags=(Flag.UNCLASSIFIED, Flag.DECISION_FAILED, Flag.BODY_TRUNCATED),
        ),
        _refund(flags=(Flag.IDEMPOTENT_REPLAY,)),
        _refund(
            flags=(Flag.IDEMPOTENCY_CONFLICT,),
            precondition=Precondition("passed", "idempotency_key_in_use"),
        ),
        _refund(9800, precondition=Precondition("rejected", "amount_too_large")),
        _refund(flags=(Flag.BODY_TRUNCATED, Flag.STREAM_TRUNCATED)),
        _post("hi\nthere\x1b[31m\ud800"),
        _tool(),
        _tool("db.lookup", kind="read", ran="real"),
        _tool("db.archive", ran="real"),
        dataclasses.replace(_tool("db.x\n"), error=ErrorInfo("Boom\x1b", "no")),
    ]
    run, index = _run(events), servicemaps.load()
    summary = summary_lines(RunView(run.record, run.events), index)
    acts = compare.actions(run, index)
    exchanges = [a for a in acts if a.key[0] == compare.EXCHANGE_KEY]
    tools = [a for a in acts if a.key[0] == compare.TOOL_KEY]
    assert [f"  {a.text}" for a in [*exchanges, *tools]] == [
        line for line in summary if line.startswith(("  ○ ", "  ✗ "))
    ]
    assert (len(exchanges), len(tools)) == (9, 2)
    # The decision-failed write's body and the plain refund's were cut.
    assert [i for i, action in enumerate(exchanges) if action.cut] == [4, 7]


def test_align_keeps_every_action_once_in_its_order_and_a_run_is_the_same_as_itself():
    """Property test over seeded random runs drawn from few keys and fields, so writes recur.

    The text is drawn apart from the fields: two actions are the same by their data alone."""
    rng = random.Random(79)

    def action() -> compare.Action:
        key: tuple[str, ...] = (
            compare.EXCHANGE_KEY,
            "stripe",
            rng.choice(["refunds.create", "customers.update"]),
        )
        key = rng.choice([key, (compare.TOOL_KEY, "db.x")])
        fields = (("body.amount", rng.choice(["1", "2"])),)
        return compare.Action(key, rng.choice("ab"), cut=rng.random() < 0.2, fields=fields)

    def side(rows: tuple[compare.Row, ...], which: str) -> list[compare.Action]:
        return [action for row in rows if (action := getattr(row, which)) is not None]

    for _ in range(3000):
        a = [action() for _ in range(rng.randrange(8))]
        b = [action() for _ in range(rng.randrange(8))]
        rows = compare.align(a, b)
        assert [id(x) for x in side(rows, "a")] == [id(x) for x in a]
        assert [id(y) for y in side(rows, "b")] == [id(y) for y in b]
        for row in rows:
            if row.a is None or row.b is None:
                assert row.mark == ("only_b" if row.a is None else "only_a"), row
                continue
            assert row.a.key == row.b.key, row
            if row.a.cut or row.b.cut:
                assert row.mark == "cut", row  # whatever the two texts
            else:
                assert row.mark == ("same" if row.a.fields == row.b.fields else "changed"), row
        # A run against itself pairs each action with itself: `?` when it was cut, else `=`.
        assert [(row.mark, row.a, row.b) for row in compare.align(a, a)] == [
            ("cut" if x.cut else "same", x, x) for x in a
        ]


def test_a_cut_pair_whose_lines_print_alike_though_they_differ_is_told_apart_too():
    """A `?` pair that prints alike though it differs escapes its backslashes, as `~` does."""
    cut = dataclasses.replace(_post("line1\nline2"), flags=(Flag.BODY_TRUNCATED,))
    pair = compare.compare_runs(_run([cut]), _run([_post("line1\\nline2")]), servicemaps.load())
    assert pair.lines()[:2] == [
        '  ? A ○ slack  post to #C1: "line1\\nline2"  faked; not checked',
        '    B ○ slack  post to #C1: "line1\\\\nline2"  faked; not checked',
    ]


def test_a_run_of_hundreds_of_one_write_is_lined_up_write_by_write():
    """250 identical fakes after differing first writes each line up as `=`.

    `difflib`'s junk heuristic, which would drop an item over 1% of a long sequence, is off."""
    many = [_tool() for _ in range(250)]
    comparison = compare.compare_runs(_run([_post("hi"), *many]), _run([_refund(), *many]), None)
    assert comparison.lines() == [
        "  - ○ slack  POST slack.com/api/chat.postMessage  faked; not checked",
        "  + ○ stripe  POST api.stripe.com/v1/refunds  faked; checked against live state: "
        "would succeed",
        *[f"  = {TOOL}"] * 250,
        "",
        "  250 same · 0 changed · 1 only in A · 1 only in B",
    ]


@pytest.mark.parametrize(
    ("a", "b", "spelled_a", "spelled_b"),
    [
        ("pay​out", "payout", "pay\\u200bout", "payout"),
        ("on schedule", "on schedule", "on\\u00a0schedule", "on schedule"),
        ("‮abc", "abc", "\\u202eabc", "abc"),
        ("a\U000e0001b", "ab", "a\\U000e0001b", "ab"),
    ],
)
def test_a_pair_that_differs_by_a_character_no_one_can_see_spells_it(a, b, spelled_a, spelled_b):
    """A pair that prints alike spells invisible characters, such as a zero-width space, as escapes.

    `report.terminal.printable` escapes control characters only."""
    pair = compare.compare_runs(_run([_post(a)]), _run([_post(b)]), servicemaps.load())
    assert pair.lines()[:2] == [
        f'  ~ A ○ slack  post to #C1: "{spelled_a}"  faked; not checked',
        f'    B ○ slack  post to #C1: "{spelled_b}"  faked; not checked',
    ]


def test_compare_takes_a_prefix_only_one_run_starts_with_and_last(home, capfd):
    """Each side may be a unique run-id prefix or `last`; the header names the whole ids."""
    _store(
        home,
        {
            "alpha1": (_record("alpha1"), [_refund()]),
            "beta22": (_record("beta22", started_at=AT + 5, ended_at=AT + 6), [_refund()]),
        },
    )
    assert main(["compare", "alp", "last"]) == 0
    out, err = capfd.readouterr()
    assert err == ""
    assert out.splitlines()[0] == "irimi compare · alpha1 vs beta22"


def test_compare_refuses_a_prefix_more_than_one_run_starts_with(home, capfd):
    _store(home, {"ab1": (_record("ab1"), []), "ab2": (_record("ab2"), [])})
    assert main(["compare", "ab", "ab1"]) == exitcodes.ERROR
    assert capfd.readouterr() == ("", "error: run id ab is ambiguous; it starts ab1, ab2\n")


# ------------------------------------------------------ members and posts that print alike


def _posted(run_id: str, body: dict[str, object]) -> StoredRun:
    request = Request(
        "POST",
        "https",
        "slack.com",
        443,
        "/api/chat.postMessage",
        "",
        (("content-type", "application/json"),),
        json.dumps(body).encode(),
    )
    post = Exchange(
        request,
        Response(200, (), b'{"ok":true}'),
        "slack",
        "chat.postMessage",
        "write",
        "fake-L1",
        run_id,
        started_at=1.0,
        ended_at=2.0,
    )
    return StoredRun(RunRecord.header(run_id), [post])


@pytest.mark.parametrize(
    ("one", "other"),
    [
        ("a\nb", "a\\nb"),  # a newline and a typed one
        ("note", "note\u200b"),  # a zero-width space prints as nothing
        ("a b", "a\u00a0b"),  # a space and a no-break one
    ],
)
def test_members_that_print_alike_are_named_apart_in_a_field_diff(one, other):
    a = _posted("a", {"channel": "C1", "text": "hi", "metadata": {one: 1}})
    b = _posted("b", {"channel": "C1", "text": "hi", "metadata": {other: 1}})
    comparison = compare.compare_runs(a, b, maps_for_stored_runs())
    assert comparison.exit_code == 1
    rows = comparison.lines()
    assert rows[0].startswith('  ~ A ○ slack  post to #C1: "hi"')
    diff = [line.strip() for line in rows[2:] if line.startswith("      ")]
    # As a reader sees each name: what prints as nothing dropped, what prints as a space a space.
    names = {_seen(line.split(": ", 1)[0]) for line in diff}
    assert len(diff) == 2 and len(names) == 2, diff


def _seen(text: str) -> str:
    kept = (char for char in text if unicodedata.category(char) != "Cf")
    return "".join(" " if unicodedata.category(char) == "Zs" else char for char in kept)


def test_a_plain_member_is_named_as_it_is():
    a = _posted("a", {"channel": "C1", "text": "hi", "metadata": {"source": "web"}})
    b = _posted("b", {"channel": "C1", "text": "hi", "metadata": {"source": "cli"}})
    rows = compare.compare_runs(a, b, maps_for_stored_runs()).lines()
    assert rows[2] == '      body.metadata.source: "web" -> "cli"'


def test_a_quoted_member_names_its_escapes():
    a = _posted("a", {"channel": "C1", "text": "hi", "metadata": {"a​": 1}})
    b = _posted("b", {"channel": "C1", "text": "hi", "metadata": {"a​": 2}})
    rows = compare.compare_runs(a, b, maps_for_stored_runs()).lines()
    assert rows[2] == '      body.metadata."a\\u200b": 1 -> 2'


def _posts(run_id: str, *texts: str) -> StoredRun:
    events = [
        event
        for i, text in enumerate(texts)
        for event in _posted(f"{run_id}{i}", {"channel": "C1", "text": text}).events
    ]
    events = [dataclasses.replace(event, run_id=run_id) for event in events]
    return StoredRun(RunRecord.header(run_id), events)


def test_a_post_only_one_run_made_never_prints_like_one_both_made():
    """A post only B made is escaped when it prints like one both made, so it never reads twice.

    B posts a typed `\\n` and then A's newline: the newline lines up `=`, the typed one `+`."""
    a = _posts("a", "a\nb")
    b = _posts("b", "a\\nb", "a\nb")
    rows = compare.compare_runs(a, b, maps_for_stored_runs()).lines()
    assert rows[:2] == [
        '  + ○ slack  post to #C1: "a\\\\nb"  faked; not checked',
        '  = ○ slack  post to #C1: "a\\nb"  faked; not checked',
    ]
