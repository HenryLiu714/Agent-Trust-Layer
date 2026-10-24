"""`report`: the banner, the line printed per exchange, and the summary a run ends with."""

import dataclasses
import itertools
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from irimi.cli.compose import maps_for_stored_runs
from irimi.core.exchange import Exchange, Flag, Precondition, Request, Response
from irimi.core.netaddr import Target
from irimi.protocol.trace import ErrorInfo, RunRecord
from irimi.report import model, summary
from irimi.report.model import RunView, for_summary
from irimi.report.summary import Live
from irimi.report.terminal import banner_lines, exchange_line, target_lines, tool_call_line
from tests.support import builders
from tests.support.builders import FORM_CONTENT_TYPE


def summary_lines(
    run_id,
    exchanges,
    elapsed_s=0.0,
    index=None,
    tool_calls=(),
    sdk_runs=None,
    tls_failures=None,
    ca_cert=None,
):
    """The summary `irimi shadow` prints for a run of these `exchanges` and `tool_calls`."""
    view = RunView(RunRecord.header(run_id), [*map(for_summary, exchanges), *tool_calls])
    live = Live(sdk_runs or {}, tls_failures or {}, ca_cert)
    return summary.summary_lines(view, index, elapsed_s, live)


def write_sentence(exchange, index):
    """The summary's sentence for one write, as `irimi compare` prints it."""
    (entry,) = model.build(RunView(RunRecord.header("7f3a"), [exchange]), index).writes
    return summary.write_sentence(entry)


def _exchange(
    method="GET",
    kind="read",
    answered_by="live",
    status=200,
    flags=(),
    host="api.stripe.com",
    path="/v1/charges",
    body=b"",
    content_type="",
    service="stripe",
    target="",
):
    """An exchange in run `7f3a` whose operation is its request line; `status=None` is no answer."""
    headers = (("content-type", content_type),) if content_type else ()
    return builders.exchange(
        request=builders.request(method=method, host=host, path=path, headers=headers, body=body),
        response=builders.response(status) if status is not None else None,
        service=service,
        operation=f"{method} {path}",
        kind=kind,
        answered_by=answered_by,
        flags=flags,
        target=target,
    )


def _refund(
    answered_by="fake-L0",
    body=b"charge=ch_3QabcXYZ&amount=4900&currency=usd",
    target="",
    flags=(),
    status=200,
):
    return _exchange(
        method="POST",
        kind="write",
        answered_by=answered_by,
        path="/v1/refunds",
        body=body,
        content_type=FORM_CONTENT_TYPE,
        target=target,
        flags=flags,
        status=status,
    )


def _unreachable(target="http://127.0.0.1:3999/refund"):
    """A write sent to a target that could not be dialled, as the engine records it.

    No response and the `target-failed` flag, as a stub that is not running produces."""
    return _refund(
        answered_by="delegated",
        target=target,
        flags=("target-failed",),
        status=None,
    )


def test_the_banner_names_the_command_run_listener_and_ca():
    lines = banner_lines("shadow", "7f3a", "127.0.0.1", 4000, Path("/ca.pem"))
    assert len(lines) == 2
    assert "shadow" in lines[0]
    assert "7f3a" in lines[0]
    assert "127.0.0.1:4000" in lines[0]
    assert "/ca.pem" in lines[0]
    assert lines[1] == (
        "only traffic sent through this proxy is shadowed; a client that ignores HTTPS_PROXY "
        "talks to the real service"
    )


def test_an_exchange_line_for_a_live_read():
    line = exchange_line(_exchange())
    assert line.startswith("real ")
    assert "read" in line
    assert "GET" in line
    assert "api.stripe.com/v1/charges" in line
    assert "200" in line


def test_exchange_line_shows_flags_and_missing_response():
    line = exchange_line(_exchange(status=None, flags=("upstream-error",)))
    assert "-" in line
    assert "[upstream-error]" in line


def test_summary_header_names_the_run_the_count_and_the_time():
    lines = summary_lines("7f3a", [_exchange(), _exchange()], 2.34)
    assert lines[0] == "irimi shadow · run 7f3a · 2 exchanges · 2.3s"


def test_summary_header_says_one_exchange_not_one_exchanges():
    assert "1 exchange ·" in summary_lines("7f3a", [_exchange()], 0.0)[0]


def test_a_summary_with_no_exchanges_says_irimi_saw_no_traffic():
    """A run with no exchange says irimi saw no traffic, and why an agent's calls may not show."""
    lines = summary_lines("7f3a", [], 0.0)
    assert "0 exchanges" in lines[0]
    assert lines[-2:] == [
        "  0 exchanges · 0 real · 0 answered by irimi (0 faked writes) · 0 sent to targets",
        "  irimi saw no HTTP(S) traffic; if the agent made calls, they bypassed the proxy or "
        "failed TLS",
    ]
    assert not any("None of these writes" in line for line in lines)


def test_a_run_with_traffic_does_not_say_it_saw_none():
    assert not any(
        "saw no HTTP(S) traffic" in line for line in summary_lines("7f3a", [_exchange()])
    )


def test_refused_tls_handshakes_are_counted_per_host_with_the_ca_to_point_at():
    lines = summary_lines(
        "7f3a",
        [],
        0.0,
        tls_failures={"api.stripe.com": 2, "hooks.slack.com": 1},
        ca_cert=Path("/home/me/.irimi/ca/ca.pem"),
    )
    assert lines[-3:] == [
        "  irimi saw no HTTP(S) traffic; if the agent made calls, they bypassed the proxy or "
        "failed TLS",
        "  2 connections to api.stripe.com failed TLS: the client did not trust irimi's CA; point "
        "it at /home/me/.irimi/ca/ca.pem (some clients ignore SSL_CERT_FILE)",
        "  1 connection to hooks.slack.com failed TLS: the client did not trust irimi's CA; point "
        "it at /home/me/.irimi/ca/ca.pem (some clients ignore SSL_CERT_FILE)",
    ]


def test_exchange_line_columns_line_up_for_the_nine_character_values():
    """The kind column fits `telemetry` and the first column its longest word, so rows line up."""
    lines = [
        exchange_line(_exchange(answered_by=answered_by, kind=kind))
        for answered_by, kind in [
            ("live", "read"),
            ("live", "telemetry"),
            ("fake-L0", "unknown"),
            ("delegated", "write"),
        ]
    ]
    starts = {line.index("GET") if "GET" in line else line.index("POST") for line in lines}
    assert len(starts) == 1, lines


def test_exchange_line_names_a_delegated_answer():
    assert exchange_line(_exchange(answered_by="delegated")).startswith("target ")


def _service(**kwargs):
    from irimi.servicemaps import ServiceMap

    base: dict[str, Any] = {
        "service": "stripe",
        "hosts": frozenset({"api.stripe.com"}),
        "routes": (),
    }
    base.update(kwargs)
    if isinstance(base.get("target"), str):
        base["target"] = Target.parse(base["target"])
    return ServiceMap(**base)


def _route(**kwargs):
    from irimi.servicemaps import Route

    base: dict[str, Any] = {
        "method": "POST",
        "path": "/v1/refunds",
        "operation": "refunds.create",
        "kind": "write",
    }
    base.update(kwargs)
    if isinstance(base.get("target"), str):
        base["target"] = Target.parse(base["target"])
    return Route(**base)


def _index(*services):
    from irimi.servicemaps import MapIndex

    return MapIndex(services=tuple(services))


def test_a_service_with_no_target_adds_no_banner_line():
    """No targeted service, no new lines: the banner a reader already knows stays as it was."""
    assert target_lines(_index(_service(routes=(_route(),)))) == []


def test_a_service_target_names_the_service_the_target_and_what_it_answers():
    lines = target_lines(_index(_service(target="http://127.0.0.1:3000")))
    assert lines == ["target: stripe -> http://127.0.0.1:3000 (writes)"]


def test_target_reads_says_the_reads_are_delegated_too():
    """The banner's "reads are real" claim is exactly what `target_reads` makes false."""
    lines = target_lines(_index(_service(target="http://127.0.0.1:3000", target_reads=True)))
    assert lines == ["target: stripe -> http://127.0.0.1:3000 (reads + writes)"]


def test_a_route_target_is_named_even_when_the_service_itself_has_none():
    """`irimi maps list` printed `target: self` for exactly this shape; the banner must not."""
    sm = _service(routes=(_route(target="http://127.0.0.1:3111"), _route(path="/v1/charges")))
    assert target_lines(_index(sm)) == [
        "target: stripe POST /v1/refunds -> http://127.0.0.1:3111 (writes)"
    ]


def test_a_delegated_read_route_says_reads():
    sm = _service(routes=(_route(kind="read", target="http://127.0.0.1:3111"),))
    assert target_lines(_index(sm))[0].endswith("(reads)")


def test_a_target_that_is_not_loopback_says_so_and_is_red_only_when_asked():
    """--allow-remote-target is how a target stops being loopback, and the line has to say it."""
    index = _index(_service(target="http://stub.example:3000"))
    plain, notice = target_lines(index)
    assert "not loopback" in notice
    assert all("\033[" not in line for line in (plain, notice))
    assert all(len(line) <= 100 for line in (plain, notice))
    coloured = target_lines(index, color=True)
    assert len(coloured) == 2
    assert all(c.startswith("\033[31m") and c.endswith("\033[0m") for c in coloured)


def test_a_loopback_target_is_never_painted_red():
    (line,) = target_lines(_index(_service(target="http://127.0.0.1:3000")), color=True)
    assert "\033[" not in line
    assert "not loopback" not in line


def test_delegated_lines_are_sorted_by_service():
    index = _index(
        _service(service="stripe", target="http://127.0.0.1:3000"),
        _service(service="acme", hosts=frozenset({"acme.test"}), target="http://127.0.0.1:3001"),
    )
    assert [line.split()[1] for line in target_lines(index)] == ["acme", "stripe"]


# ------------------------------------------------------------------ the exit summary


def _maps():
    from irimi import servicemaps

    return servicemaps.load()


def _block(lines, needle):
    return next(line for line in lines if needle in line)


def _host(lines, host="api.stripe.com"):
    """`host`'s line in the per-host block, found by the host column it starts with."""
    return next(line for line in lines if line.startswith(f"  {host} "))


def _totals(lines):
    return _block(lines, " answered by irimi (")


def _fired(lines):
    """The `would have fired:` line, or None when the summary promises no webhooks."""
    return next((line for line in lines if line.startswith("  would have fired:")), None)


NOTHING_HAPPENED = "  None of these writes reached the real service:"


def test_a_host_line_separates_reads_from_intercepted_writes():
    lines = summary_lines("7f3a", [_exchange(), _exchange(), _refund()], 0.0, _maps())
    assert _host(lines) == "  api.stripe.com  2 reads  1 faked write"


def test_a_delegated_read_is_never_counted_as_a_live_read():
    """A read answered by a target is not counted among the real reads."""
    rows = [_exchange(), _exchange(answered_by="delegated", target="http://127.0.0.1:3000")]
    line = _host(summary_lines("7f3a", rows, 0.0, _maps()))
    assert line == "  api.stripe.com  1 read  1 read from the target"


def test_an_overlaid_read_is_a_real_read_and_says_it_shows_the_run_s_writes():
    """An overlaid read reached the real service, so it counts as a real read.

    Its body is not what the service sent, so the line says that too."""
    rows = [_exchange(), _exchange(answered_by="overlay")]
    line = _host(summary_lines("7f3a", rows, 0.0, _maps()))
    assert line == "  api.stripe.com  2 reads (1 showing this run's writes)"


def test_a_read_answered_from_the_recording_is_a_read_and_says_so():
    """A read answered from the recording counts as a read, flagged `from-recording`.

    The recorded run made it, so it is counted as what it was; the totals count it answered by
    irimi."""
    rows = [
        _exchange(),
        _exchange(answered_by="overlay", flags=(Flag.FROM_RECORDING,)),
        _exchange(answered_by="recording", flags=(Flag.FROM_RECORDING,)),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _host(lines) == (
        "  api.stripe.com  3 reads (1 showing this run's writes, 2 from the recording)"
    )
    assert _totals(lines) == (
        "  3 exchanges · 1 real · 2 answered by irimi (0 faked writes) · 0 sent to targets"
    )


def test_llm_calls_answered_from_the_recording_say_so():
    llm = _exchange(method="POST", kind="llm", host="api.anthropic.com", service="anthropic")
    rows = [llm, dataclasses.replace(llm, answered_by="recording", flags=(Flag.FROM_RECORDING,))]
    line = _block(summary_lines("7f3a", rows, 0.0, _maps()), "api.anthropic.com")
    assert line == "  api.anthropic.com  2 llm (1 from the recording)"


def test_a_replayed_read_the_overlay_could_not_complete_names_who_answered_it():
    """A `partial` read the overlay left alone names who answered it: service or recording."""
    refund = _refund()
    read = dataclasses.replace(
        _exchange(), answered_by="recording", overlay="partial", flags=(Flag.FROM_RECORDING,)
    )
    lines = summary_lines("7f3a", [refund, read], 0.0, _maps())
    assert "    ↳ GET /v1/charges did not show it  recorded (partial)" in lines


def test_an_overlaid_read_is_counted_real_not_answered_by_irimi():
    """An overlaid read counts as real, not answered by irimi: the service answered it too."""
    rows = [_exchange(answered_by="overlay"), _refund()]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _totals(lines) == (
        "  2 exchanges · 1 real · 1 answered by irimi (1 faked write) · 0 sent to targets"
    )


def test_an_intercepted_write_that_was_delegated_says_so_in_its_host_line():
    rows = [_refund(), _refund(answered_by="delegated", target="http://127.0.0.1:3000/refund")]
    line = _host(summary_lines("7f3a", rows, 0.0, _maps()))
    assert line == "  api.stripe.com  1 faked write  1 write sent to the target"


def test_an_unclassified_write_is_counted_and_named():
    rows = [_refund(), _exchange(method="DELETE", kind="unknown", answered_by="fake-L0")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _host(lines) == "  api.stripe.com  2 faked writes (1 unclassified)"
    assert "  ○ stripe  DELETE api.stripe.com/v1/charges  faked; not checked (unclassified)" in (
        lines
    )


def test_llm_gets_its_own_word():
    rows = [_exchange(kind="llm", host="api.openai.com", service="openai", method="POST")]
    assert _block(summary_lines("7f3a", rows, 0.0, _maps()), "api.openai.com").endswith("1 llm")


def test_every_telemetry_host_shares_one_line():
    """Telemetry hosts share one line, saying they went live, so the agent's hosts stay visible."""
    rows = [
        _exchange(kind="telemetry", host="api.datadoghq.com", service="datadog", method="POST"),
        _exchange(kind="telemetry", host="api.datadoghq.com", service="datadog", method="POST"),
        _exchange(kind="telemetry", host="o0.ingest.sentry.io", service="sentry", method="POST"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _block(lines, "telemetry") == "  telemetry  3 exchanges to 2 hosts, forwarded live"
    assert not any("api.datadoghq.com" in line for line in lines)


def test_a_telemetry_host_still_gets_a_host_line_for_its_control_plane_writes():
    """A write on a telemetry host gets its own host line; `kind` is per route."""
    rows = [
        _exchange(kind="telemetry", host="api.datadoghq.com", service="datadog", method="POST"),
        _exchange(
            method="DELETE",
            kind="unknown",
            answered_by="fake-L0",
            host="api.datadoghq.com",
            service="datadog",
            path="/api/v1/dashboard/x",
        ),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  api.datadoghq.com  1 faked write (1 unclassified)" in lines
    assert "  telemetry          1 exchange to 1 host, forwarded live" in lines


def test_a_write_is_rendered_from_the_maps_human_template():
    lines = summary_lines("7f3a", [_refund()], 0.0, _maps())
    assert "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; not checked" in lines


def test_an_amount_with_no_currency_in_the_request_is_left_as_it_was_sent():
    """An amount with no known currency prints as minor units, never divided by 100.

    Dividing would print ¥49.00 for a 4900-yen refund. No check read gave this write a currency."""
    lines = summary_lines("7f3a", [_refund(body=b"charge=ch_9&amount=4900")], 0.0, _maps())
    assert (
        "  ○ stripe  refund 4900 (minor units, currency unknown) on ch_9  faked; not checked"
        in lines
    )


def test_an_amount_is_denominated_by_the_currency_the_l3_read_found():
    """A write naming no currency takes the one irimi's check read found on the charge."""
    write = replace(
        _refund(body=b"charge=ch_9&amount=4900"), precondition=Precondition(currency="usd")
    )
    lines = summary_lines("7f3a", [write], 0.0, _maps())
    assert "  ○ stripe  refund $49.00 on ch_9  faked; not checked" in lines


def test_the_requests_own_currency_still_wins_over_the_one_the_read_found():
    """The request's own currency wins over the check read's: irimi does not correct the caller."""
    write = replace(
        _refund(body=b"charge=ch_9&amount=4900&currency=jpy"),
        precondition=Precondition(currency="usd"),
    )
    lines = summary_lines("7f3a", [write], 0.0, _maps())
    assert "  ○ stripe  refund ¥4900 on ch_9  faked; not checked" in lines


def test_a_rejected_write_is_denominated_the_same_way():
    """A `✗` line formats its amount as the `○` line does."""
    write = replace(
        _refund(body=b"charge=ch_9&amount=4900"),
        precondition=Precondition("rejected", "charge_already_refunded", "usd"),
    )
    lines = summary_lines("7f3a", [write], 0.0, _maps())
    assert "  ✗ stripe  refund $49.00 on ch_9  faked; would fail: charge_already_refunded" in lines


def test_a_zero_decimal_currency_is_not_divided():
    lines = summary_lines(
        "7f3a", [_refund(body=b"charge=ch_9&amount=4900&currency=jpy")], 0.0, _maps()
    )
    assert "  ○ stripe  refund ¥4900 on ch_9  faked; not checked" in lines


def test_a_currency_with_no_symbol_is_named():
    lines = summary_lines(
        "7f3a", [_refund(body=b"charge=ch_9&amount=4900&currency=sek")], 0.0, _maps()
    )
    assert "  ○ stripe  refund 49.00 SEK on ch_9  faked; not checked" in lines


def test_an_amount_the_template_does_not_mark_as_money_is_printed_as_sent():
    """Only a `{name:money}` hole is money; an unmarked `amount` prints as sent, unsummed."""
    from irimi.servicemaps import MapIndex

    stripe = _maps().service_for("api.stripe.com")
    assert stripe is not None
    routes = tuple(
        replace(r, human="refund {amount} on {charge}") if r.operation == "refunds.create" else r
        for r in stripe.routes
    )
    lines = summary_lines("7f3a", [_refund()], 0.0, MapIndex((replace(stripe, routes=routes),)))
    assert "  ○ stripe  refund 4900 on ch_3QabcXYZ  faked; not checked" in lines
    assert not any(line.startswith("  money:") for line in lines)


def test_a_template_hole_the_request_does_not_fill_is_marked_not_dropped():
    lines = summary_lines("7f3a", [_refund(body=b"amount=4900&currency=usd")], 0.0, _maps())
    assert "  ○ stripe  refund $49.00 on ?  faked; not checked" in lines


def test_a_path_parameter_fills_a_hole_the_body_does_not():
    rows = [
        _exchange(
            method="POST",
            kind="write",
            answered_by="fake-L0",
            path="/v1/payment_intents/pi_REAL999/cancel",
        )
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  ○ stripe  cancel pi_REAL999  faked; not checked" in lines


def test_an_unmapped_write_is_spelled_as_the_request_it_was():
    rows = [
        _exchange(
            method="POST",
            kind="unknown",
            answered_by="fake-L0",
            host="api.unmapped.test",
            service="api.unmapped.test",
            path="/do/a/thing",
        )
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert (
        "  ○ api.unmapped.test  POST api.unmapped.test/do/a/thing  faked; not checked "
        "(unclassified)" in lines
    )


def test_a_delegated_write_names_the_address_that_answered_it():
    rows = [_refund(answered_by="delegated", target="http://127.0.0.1:3000/refund")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  ○ stripe  refund $49.00 on ch_3QabcXYZ  sent to target 127.0.0.1:3000" in lines


def test_the_three_buckets_are_counted_on_one_line():
    rows = [
        _exchange(),
        _exchange(kind="telemetry", host="api.datadoghq.com", service="datadog", method="POST"),
        _refund(),
        _refund(answered_by="delegated", target="http://127.0.0.1:3000/refund"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert lines[-1] == (
        "  4 exchanges · 2 real · 1 answered by irimi (1 faked write) · 1 sent to targets"
    )


def test_the_closing_line_is_the_plain_claim_when_nothing_was_delegated():
    """The writes block opens with the claim that none of its writes reached the real service."""
    lines = summary_lines("7f3a", [_refund()], 0.0, _maps())
    assert lines[1:4] == [
        "",
        NOTHING_HAPPENED,
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; not checked",
    ]


def test_the_closing_lines_say_where_a_delegated_write_went():
    rows = [_refund(), _refund(answered_by="delegated", target="http://127.0.0.1:3000/refund")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; not checked",
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  sent to target 127.0.0.1:3000",
    ]
    assert NOTHING_HAPPENED in lines


def test_a_write_whose_target_was_never_reached_is_not_counted_as_delegated():
    """A write whose target was never reached does not read as sent to the target."""
    lines = summary_lines("7f3a", [_unreachable()], 0.0, _maps())
    assert _host(lines) == "  api.stripe.com  1 write that could not reach the target"
    assert not any("sent to target 127" in line for line in lines)
    assert _totals(lines) == (
        "  1 exchange · 0 real · 1 answered by irimi (0 faked writes) · 0 sent to targets"
    )


def test_a_delegated_write_and_an_unreachable_one_are_counted_apart():
    rows = [_refund(answered_by="delegated", target="http://127.0.0.1:3000/refund"), _unreachable()]
    assert _host(summary_lines("7f3a", rows, 0.0, _maps())) == (
        "  api.stripe.com  1 write sent to the target  1 write that could not reach the target"
    )


def test_an_unreachable_target_says_the_write_was_not_answered_at_all():
    lines = summary_lines("7f3a", [_unreachable()], 0.0, _maps())
    assert "  ✗ stripe  refund $49.00 on ch_3QabcXYZ  could not reach target 127.0.0.1:3999" in (
        lines
    )


def test_the_closing_lines_never_claim_an_unreachable_target_answered():
    lines = summary_lines("7f3a", [_unreachable()], 0.0, _maps())
    assert NOTHING_HAPPENED in lines
    assert not any("sent to target 127" in line for line in lines)
    assert _fired(lines) is None


def test_a_delegated_write_and_an_unreachable_one_each_get_their_own_sentence():
    rows = [_refund(answered_by="delegated", target="http://127.0.0.1:3000/refund"), _unreachable()]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  sent to target 127.0.0.1:3000",
        "  ✗ stripe  refund $49.00 on ch_3QabcXYZ  could not reach target 127.0.0.1:3999",
    ]


def test_two_unreachable_writes_read_as_were():
    line = _host(summary_lines("7f3a", [_unreachable()] * 2, 0.0, _maps()))
    assert line == "  api.stripe.com  2 writes that could not reach the target"


def test_two_delegated_writes_read_as_were():
    rows = [_refund(answered_by="delegated", target="http://127.0.0.1:3000/refund")] * 2
    assert _host(summary_lines("7f3a", rows, 0.0, _maps())) == (
        "  api.stripe.com  2 writes sent to the target"
    )


def _tool_call(
    kind="write",
    ran="shadow",
    name="db.mark_refunded",
    tool_call_id="t",
    started_at=1.0,
    ended_at=2.0,
):
    return builders.tool_call(
        tool_call_id=tool_call_id,
        name=name,
        kind=kind,
        ran=ran,
        started_at=started_at,
        ended_at=ended_at,
    )


def test_a_delegated_write_s_closing_lines_say_the_tool_writes_did_not_happen_either():
    """A write tool whose fake ran gets a line in the writes block, after the HTTP writes.

    A read tool ran for real and gets none."""
    tools = [_tool_call(), _tool_call(kind="read", ran="real", name="db.lookup")]
    delegated = _refund(answered_by="delegated", target="http://127.0.0.1:3000/refund")
    faked = replace(_refund(answered_by="fake-L1"), would_fire=("refund.created",))
    lines = summary_lines("7f3a", [faked, delegated], 0.0, _maps(), tools)
    assert _write_block(lines) == [
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; not checked",
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  sent to target 127.0.0.1:3000",
        "  ○ tool  db.mark_refunded  fake ran instead",
    ]
    assert _fired(lines) == "  would have fired: refund.created"
    lines = summary_lines("7f3a", [], 0.0, _maps(), tools)
    assert lines[1:4] == ["", NOTHING_HAPPENED, "  ○ tool  db.mark_refunded  fake ran instead"]
    lines = summary_lines("7f3a", [delegated], 0.0, _maps(), tools[1:])
    assert not any("○ tool" in line for line in lines)


def test_stood_in_tool_writes_print_in_the_order_they_ended_whatever_order_they_came_in():
    """Tool writes print in the order they ended, so the live and stored summaries agree.

    A tie is broken by the start, then the id, then the line: a hand-posted call can repeat both."""
    ended = [
        _tool_call(name="db.first", ended_at=2.0),
        _tool_call(name="db.second", ended_at=3.0),
        _tool_call(name="db.tie_started_first", started_at=0.5, ended_at=4.0, tool_call_id="z"),
        _tool_call(name="db.tie_lower_id", ended_at=4.0, tool_call_id="a"),
        _tool_call(name="db.tie_higher_id_a", ended_at=4.0, tool_call_id="b"),
        _tool_call(name="db.tie_higher_id_b", ended_at=4.0, tool_call_id="b"),
    ]
    printed = [f"  ○ tool  {call.name}  fake ran instead" for call in ended]
    for calls in itertools.permutations(ended):
        lines = summary_lines("7f3a", [], 0.0, None, calls)
        assert [line for line in lines if "○ tool" in line] == printed


def test_a_tool_fake_that_raised_says_so():
    call = replace(_tool_call(), error=ErrorInfo("builtins.KeyError", "k"))
    lines = summary_lines("7f3a", [], 0.0, None, [call])
    assert "  ○ tool  db.mark_refunded  fake raised builtins.KeyError" in lines


def test_a_run_that_only_read_claims_nothing_about_writes():
    lines = summary_lines("7f3a", [_exchange()], 0.0, _maps())
    assert NOTHING_HAPPENED not in lines
    assert _fired(lines) is None
    assert not any(line.startswith("  money:") for line in lines)


def test_the_write_line_names_the_fidelity_the_answer_actually_had():
    """A faked write's line reads alike for an echo and a fixture, with no `L` code."""
    for answered_by in ("fake-L0", "fake-L1"):
        lines = summary_lines("7f3a", [_refund(answered_by=answered_by)], 0.0, _maps())
        assert "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; not checked" in lines
        assert not any("(L" in line for line in lines)


# ------------------------------------------------------ what the precondition check said


def test_a_rejected_write_is_crossed_out_and_names_the_code_it_would_fail_with():
    """A write the precondition check refused gets `✗` and its code, never the `○` of a success."""
    rows = [
        replace(
            _refund(answered_by="fake-L1", status=400),
            precondition=Precondition("rejected", "charge_already_refunded"),
        )
    ]
    line = _block(summary_lines("7f3a", rows, 0.0, _maps()), "would fail")
    assert line.startswith("  ✗ ")
    assert line == (
        "  ✗ stripe  refund $49.00 on ch_3QabcXYZ  faked; would fail: charge_already_refunded"
    )


def test_a_passed_write_reads_l3_preconditions_passed():
    rows = [replace(_refund(answered_by="fake-L1"), precondition=Precondition("passed"))]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert (
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; checked against live state: would succeed"
        in lines
    )


def test_a_write_l3_could_not_evaluate_reads_l2():
    """A write irimi could not check, with no check read in the run, says it could not check."""
    rows = [replace(_refund(answered_by="fake-L1"), precondition=Precondition("not_evaluable"))]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert (
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; could not check: irimi made no check read"
        in lines
    )


def test_a_write_answered_from_the_recording_says_its_recorded_run_could_not_check_it():
    """A replayed write never borrows a check read from the replay's own run, which made none."""
    recorded = replace(
        _refund(answered_by="fake-L1", flags=(Flag.FROM_RECORDING,)),
        precondition=Precondition("not_evaluable"),
    )
    lines = summary_lines("7f3a", [_check_read(401), recorded], 0.0, _maps())
    assert _write_block(lines) == [
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; could not check: the recorded run "
        "could not check it"
    ]


def _check_read(status=200, path="/v1/charges/ch_3QabcXYZ"):
    """irimi's own precondition read, which the engine records before the write it decides."""
    return replace(_exchange(path=path, status=status), issued_by="engine")


def _checked_refund(**kwargs):
    fired = ("refund.created", "charge.refunded")
    return replace(
        _refund(answered_by="fake-L1"),
        precondition=Precondition("not_evaluable"),
        would_fire=fired,
        **kwargs,
    )


def test_a_check_read_refused_for_its_credentials_says_so_and_promises_no_webhooks():
    """A check read answered 401 or 403 says why it could not check, and promises no webhooks.

    The real write would most likely have been refused for the same credentials."""
    for status in (401, 403):
        lines = summary_lines("7f3a", [_check_read(status), _checked_refund()], 0.0, _maps())
        assert _write_block(lines) == [
            "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; could not check: stripe answered "
            f"irimi's check read with {status}"
        ]
        assert _fired(lines) is None


def test_a_check_read_that_failed_otherwise_still_promises_its_webhooks():
    """A 429 or a 500 says nothing about the write, which was faked, so its events are owed."""
    for status in (429, 500):
        lines = summary_lines("7f3a", [_check_read(status), _checked_refund()], 0.0, _maps())
        assert _write_block(lines) == [
            "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; could not check: stripe answered "
            f"irimi's check read with {status}"
        ]
        assert _fired(lines) == "  would have fired: refund.created, charge.refunded"


def test_a_check_read_with_no_answer_or_an_unusable_one_says_which():
    lines = summary_lines("7f3a", [_check_read(None), _checked_refund()], 0.0, _maps())
    assert (
        "could not check: irimi's check read of stripe got no usable answer"
        in (_write_block(lines)[0])
    )
    lines = summary_lines("7f3a", [_check_read(200), _checked_refund()], 0.0, _maps())
    assert "could not check: irimi could not tell from its check read" in _write_block(lines)[0]


def test_each_write_takes_its_own_check_read():
    """Two refunds, each checked by a read of its own: the first's 401 is not the second's."""
    rows = [_check_read(401), _checked_refund(), _check_read(429), _checked_refund()]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert [line.rsplit(" ", 1)[1] for line in _write_block(lines)] == ["401", "429"]
    assert _fired(lines) == "  would have fired: refund.created, charge.refunded"


def test_a_write_on_a_route_with_no_precondition_keeps_its_own_level():
    rows = [_refund(answered_by="fake-L1")]
    assert rows[0].precondition.outcome is None
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; not checked" in lines


def test_a_delegated_write_l3_could_not_evaluate_still_reads_delegated():
    """A write sent to a target reads so even when unchecked: the target answered, not irimi."""
    rows = [
        replace(
            _refund(answered_by="delegated", target="http://127.0.0.1:3000/refund"),
            precondition=Precondition("not_evaluable"),
        )
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  ○ stripe  refund $49.00 on ch_3QabcXYZ  sent to target 127.0.0.1:3000" in lines
    assert not any("could not check" in line for line in lines)


def test_an_engine_read_is_counted_apart_from_the_agents_reads_but_still_went_live():
    """irimi's check read is real but counted apart: `N reads` counts the agent's reads only."""
    rows = [
        _exchange(),
        replace(_exchange(path="/v1/charges/ch_3QabcXYZ"), issued_by="engine"),
        replace(_refund(answered_by="fake-L1"), precondition=Precondition("passed")),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _host(lines) == "  api.stripe.com  1 read  1 irimi check read  1 faked write"
    assert lines[0].startswith("irimi shadow · run 7f3a · 3 exchanges · ")
    assert _totals(lines) == (
        "  3 exchanges · 2 real · 1 answered by irimi (1 faked write) · 0 sent to targets"
    )


def test_the_summary_still_names_a_write_without_the_maps():
    """With no maps (`index=None`) a write still gets a line, naming the request it saw."""
    lines = summary_lines("7f3a", [_refund()], 0.0, None)
    assert "  ○ stripe  POST api.stripe.com/v1/refunds  faked; not checked" in lines


# ------------------------------------------------------------------- failed reads


def test_a_host_line_counts_the_agent_s_failed_reads():
    """A run whose reads came back 401 worked blind, and `1 read` alone hid it."""
    rows = [_exchange(status=401), _exchange(), _exchange(status=404), _exchange(status=401)]
    line = _host(summary_lines("7f3a", rows, 0.0, _maps()))
    assert line == "  api.stripe.com  4 reads (3 failed: 401, 404)"
    line = _host(summary_lines("7f3a", [_exchange(status=None)], 0.0, _maps()))
    assert line == "  api.stripe.com  1 read (1 failed: no answer)"


def test_a_read_irimi_refused_gets_a_phrase_of_its_own():
    rows = [_exchange(answered_by="irimi", status=403, flags=("refused",))]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _host(lines) == "  api.stripe.com  1 read refused by irimi"
    assert _totals(lines) == (
        "  1 exchange · 0 real · 1 answered by irimi (0 faked writes) · 0 sent to targets"
    )


# ------------------------------------------------------------------- money


def test_the_money_line_sums_the_faked_writes_and_what_would_fail():
    rejected = replace(
        _refund(answered_by="fake-L1", status=400),
        precondition=Precondition("rejected", "charge_already_refunded"),
    )
    lines = summary_lines("7f3a", [_refund(answered_by="fake-L1"), rejected], 0.0, _maps())
    assert "  money: $98.00 in faked writes; $49.00 of it would fail" in lines


def test_the_money_line_keeps_each_currency_apart():
    rows = [_refund(), _refund(body=b"charge=ch_9&amount=4900&currency=jpy")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  money: $49.00 + ¥4900 in faked writes" in lines


def test_an_amount_in_an_unknown_currency_is_counted_apart():
    rows = [_refund(), _refund(body=b"charge=ch_9&amount=4900")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  money: $49.00 in faked writes; 1 amount in an unknown currency not counted" in lines


def test_no_money_line_when_no_amount_has_a_known_currency():
    rows = [_refund(body=b"charge=ch_9&amount=4900")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert not any(line.startswith("  money:") for line in lines)


def test_writes_irimi_did_not_fake_are_not_in_the_money_line():
    rows = [_refund(answered_by="delegated", target="http://127.0.0.1:3000/refund")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert not any(line.startswith("  money:") for line in lines)


# ------------------------------------------------------------------- the stream


def test_exchange_line_marks_irimi_s_own_check_read():
    line = exchange_line(_check_read())
    assert line.startswith("irimi-check   read      GET api.stripe.com/v1/charges/ch_3QabcXYZ")


def test_exchange_line_uses_one_word_per_answer_and_drops_the_fidelity_flags():
    cases = {
        "live": "real",
        "overlay": "real",
        "fake-L0": "faked",
        "fake-L1": "faked",
        "delegated": "target",
        "recording": "recorded",
        "irimi": "irimi",
    }
    for answered_by, word in cases.items():
        flags = ("upstream-error",)
        line = exchange_line(_exchange(answered_by=answered_by, flags=flags))
        assert line.split()[0] == word, line
        assert "upstream-error" in line
    assert exchange_line(_exchange(answered_by="overlay")).endswith("  [overlay]")
    recorded = _exchange(answered_by="overlay", flags=(Flag.FROM_RECORDING,))
    assert exchange_line(recorded).startswith("recorded ")


def test_exchange_line_escapes_control_characters_in_the_agent_s_text():
    line = exchange_line(_exchange(path="/v1/charges/\x1b[31mred\n"))
    assert "\x1b" not in line and "\n" not in line
    assert "/v1/charges/\\x1b[31mred\\n" in line


def test_tool_call_line_is_in_the_exchange_line_s_columns():
    real = tool_call_line(_tool_call(kind="read", ran="real", name="db.lookup"))
    fake = tool_call_line(_tool_call())
    recorded = tool_call_line(_tool_call(kind="read", ran="recording", name="db.lookup"))
    assert real == "tool-real     read      db.lookup -> ok"
    assert fake == "tool-fake     write     db.mark_refunded -> ok"
    assert recorded == "tool-recorded read      db.lookup -> ok"
    assert real.index("read") == exchange_line(_exchange()).index("read")


# ------------------------------------------------------------------- SDK runs in the process


def _sdk_record(run_id, name="handle_ticket", args=None):
    from irimi import __version__
    from irimi.protocol.trace import SCHEMA_VERSION, RunEnd, RunRecord, Trigger

    trigger = Trigger(name=name, entrypoint="agent:handle_ticket", args=args or {}, replayable=True)
    return RunRecord(
        schema_version=SCHEMA_VERSION,
        run_id=run_id,
        mode="shadow",
        attribution="sdk",
        trigger=trigger,
        agent_version=None,
        engine_version=__version__,
        sdk_version=__version__,
        started_at=1.0,
        end=RunEnd(2.0, "ok", None, None),
    )


def test_a_summary_over_sdk_runs_says_it_covers_the_process_and_lists_each_run():
    sdk_run = "b7aa0abc12345678"
    rows = [
        replace(_exchange(), run_id=sdk_run),
        replace(_refund(), run_id=sdk_run),
        replace(_exchange(), run_id="cafe0000cafe0000"),
    ]
    runs = {
        sdk_run: _sdk_record(sdk_run, args={"ticket": "T1"}),
        "cafe0000cafe0000": None,
    }
    lines = summary_lines("7f3a", rows, 0.0, _maps(), sdk_runs=runs)
    assert lines[0] == "irimi shadow · run 7f3a (whole process) · 3 exchanges · 0.0s"
    assert lines[-4:] == [
        "",
        "  run b7aa0abc  handle_ticket(...)  2 exchanges · 1 write",
        "  run cafe0000  ?  1 exchange · 0 writes",
        "  replay with: irimi replay b7aa0abc",
    ]


def test_an_sdk_run_with_no_args_prints_empty_brackets():
    run = "b7aa0abc12345678"
    lines = summary_lines(
        "7f3a", [replace(_exchange(), run_id=run)], 0.0, _maps(), sdk_runs={run: _sdk_record(run)}
    )
    assert "  run b7aa0abc  handle_ticket()  1 exchange · 0 writes" in lines


def test_a_summary_with_no_sdk_runs_keeps_its_plain_header():
    lines = summary_lines("7f3a", [_exchange()], 0.0, _maps(), sdk_runs={})
    assert lines[0] == "irimi shadow · run 7f3a · 1 exchange · 0.0s"
    assert not any("replay with" in line for line in lines)


# ------------------------------------------------------------------- idempotency keys


def test_a_replayed_write_is_counted_once():
    """A retry is the same write: one line and one faked write, its exchange answered by irimi."""
    rows = [
        _refund(answered_by="fake-L1", flags=()),
        _refund(answered_by="fake-L1", flags=(Flag.IDEMPOTENT_REPLAY,)),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert [line for line in lines if line.startswith("  ○ ")] == [
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; not checked"
    ]
    assert _host(lines) == "  api.stripe.com  1 faked write"
    assert _totals(lines) == (
        "  2 exchanges · 0 real · 2 answered by irimi (1 faked write) · 0 sent to targets"
    )


def test_an_idempotency_conflict_prints_as_a_write_that_would_fail():
    """A key reused for a different write gets the `✗` line, though no precondition check ran."""
    rows = [
        _refund(answered_by="fake-L1"),
        replace(
            _refund(
                answered_by="fake-L1",
                status=400,
                flags=(Flag.IDEMPOTENCY_CONFLICT,),
            ),
            precondition=Precondition(rejection_code="idempotency_error"),
        ),
    ]
    assert rows[1].precondition.outcome is None
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    line = _block(lines, "would fail")
    assert line == "  ✗ stripe  refund $49.00 on ch_3QabcXYZ  faked; would fail: idempotency_error"
    assert _host(lines) == "  api.stripe.com  2 faked writes"


# ------------------------------------------------------------ a write irimi refused


def test_a_write_irimi_could_not_decide_prints_as_refused_and_names_why():
    """A write refused for its body size prints `✗` as its request line, saying why.

    Its `human:` sentence would describe a body irimi never read. Its host line counts it as
    `refused by irimi`, as its line names it."""
    cut = _exchange(
        method="POST",
        kind="unknown",
        answered_by="fake-L0",
        status=502,
        path="/v1/refunds",
        content_type=FORM_CONTENT_TYPE,
        flags=(Flag.UNCLASSIFIED, Flag.DECISION_FAILED, Flag.BODY_TRUNCATED, Flag.REDACTION_FAILED),
    )
    lines = summary_lines("7f3a", [cut], 0.0, _maps())
    assert _write_block(lines) == [
        "  ✗ stripe  POST api.stripe.com/v1/refunds  refused by irimi: its body is too large to "
        "read whole"
    ]
    assert _host(lines) == "  api.stripe.com  1 write refused by irimi"
    assert NOTHING_HAPPENED in lines
    presumed = _exchange(method="DELETE", kind="unknown", answered_by="fake-L0")
    lines = summary_lines("7f3a", [cut, presumed], 0.0, _maps())
    assert _host(lines) == (
        "  api.stripe.com  1 faked write (1 unclassified)  1 write refused by irimi"
    )


def test_a_write_the_request_hook_refused_says_refused_by_irimi():
    refused = replace(_refund(answered_by="irimi", status=400, flags=("refused",)), kind="unknown")
    assert write_sentence(refused, _maps()) == (
        "✗ stripe  refund $49.00 on ch_3QabcXYZ  refused by irimi"
    )


def test_a_write_whose_decision_raised_prints_its_sentence_as_refused():
    """A write whose decision raised keeps the map's sentence, which read the whole body.

    So two refunds of different amounts print, and compare, as different writes."""
    raised = _exchange(
        method="POST",
        kind="unknown",
        answered_by="fake-L0",
        status=502,
        path="/v1/refunds",
        body=b"charge=ch_3QabcXYZ&amount=4900&currency=usd",
        content_type=FORM_CONTENT_TYPE,
        flags=(Flag.UNCLASSIFIED, Flag.DECISION_FAILED),
    )
    assert write_sentence(raised, _maps()) == (
        "✗ stripe  refund $49.00 on ch_3QabcXYZ  refused by irimi: irimi could not decide how to "
        "answer it"
    )
    body = b"charge=ch_3QabcXYZ&amount=2450&currency=usd"
    half = replace(raised, request=replace(raised.request, body=body))
    assert write_sentence(half, _maps()) == (
        "✗ stripe  refund $24.50 on ch_3QabcXYZ  refused by irimi: irimi could not decide how to "
        "answer it"
    )


# ------------------------------ the reads that saw a write, and what it would have fired


def _write_block(lines):
    """The write block as printed: each `○`/`✗` line, with the `↳` lines hanging under it.

    Any `↳` line is kept whatever its indent, so one at the wrong depth is still seen."""
    return [line for line in lines if line.startswith(("  ○ ", "  ✗ ")) or "↳" in line]


def _slack_post(answered_by="fake-L1", target=""):
    return _exchange(
        method="POST",
        kind="write",
        answered_by=answered_by,
        host="slack.com",
        service="slack",
        path="/api/chat.postMessage",
        body=b"channel=C0123&text=refunded",
        content_type=FORM_CONTENT_TYPE,
        target=target,
    )


def _slack_history(answered_by="live"):
    return _exchange(
        method="POST",
        host="slack.com",
        service="slack",
        path="/api/conversations.history",
        answered_by=answered_by,
    )


PASSED_REFUND = (
    "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; checked against live state: would succeed"
)

UNCHECKED_REFUND = "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; not checked"


def test_an_overlaid_read_is_listed_under_the_write_it_saw():
    """An overlaid read is listed under the write it saw."""
    rows = [
        replace(_refund(answered_by="fake-L1"), precondition=Precondition("passed")),
        replace(_exchange(path="/v1/refunds", answered_by="overlay"), overlay="full"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [PASSED_REFUND, "    ↳ GET /v1/refunds saw it  overlay"]


def test_a_partial_overlay_hit_says_it_saw_only_part():
    """A `partial` overlay says the read saw only part of the run's writes, not a plain `saw it`."""
    rows = [
        _refund(answered_by="fake-L1"),
        replace(_exchange(path="/v1/refunds", answered_by="overlay"), overlay="partial"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        UNCHECKED_REFUND,
        "    ↳ GET /v1/refunds saw it in part  overlay (partial)",
    ]


def test_a_read_irimi_knew_was_incomplete_says_so_even_though_it_answered_none_of_it():
    """A read irimi left untouched but knew was incomplete still says it missed the write.

    A Slack read whose channel the two sides spell differently does not show the post."""
    rows = [_slack_post(), replace(_slack_history(), overlay="partial")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        '  ○ slack  post to #C0123: "refunded"  faked; not checked',
        "    ↳ POST /api/conversations.history did not show it  real (partial)",
    ]


def test_a_translated_read_that_needed_no_edit_gets_no_overlay_line():
    """A translated read whose body needed no edit (`overlay: full`, `live`) gets no `↳` line."""
    rows = [
        _refund(answered_by="fake-L1"),
        replace(_exchange(path="/v1/refunds"), overlay="full"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [UNCHECKED_REFUND]


def test_an_engine_issued_read_never_gets_an_overlay_line():
    """irimi's check read never gets a `↳` line, which lists the agent's reads of the write."""
    rows = [
        replace(_refund(answered_by="fake-L1"), precondition=Precondition("passed")),
        replace(_exchange(path="/v1/charges/ch_3QabcXYZ"), issued_by="engine", overlay="partial"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [PASSED_REFUND]


def test_an_overlaid_read_is_filed_under_its_own_services_write():
    """An overlaid read is filed under its own service's latest write, not the latest write.

    The overlay applies only a service's own writes to that service's reads."""
    rows = [
        _refund(answered_by="fake-L1"),
        _slack_post(),
        replace(_slack_history(answered_by="overlay"), overlay="full"),
        replace(_exchange(path="/v1/charges/ch_3QabcXYZ", answered_by="overlay"), overlay="full"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        UNCHECKED_REFUND,
        "    ↳ GET /v1/charges/ch_3QabcXYZ saw it  overlay",
        '  ○ slack  post to #C0123: "refunded"  faked; not checked',
        "    ↳ POST /api/conversations.history saw it  overlay",
    ]


def test_an_overlaid_read_is_never_filed_under_a_write_the_overlay_could_not_have_shown():
    """An overlaid read is never filed under a refused write, which never enters the write log."""
    rows = [
        replace(_refund(answered_by="fake-L1"), precondition=Precondition("passed")),
        replace(
            _refund(answered_by="fake-L1", status=400),
            precondition=Precondition("rejected", "charge_already_refunded"),
        ),
        replace(
            _refund(answered_by="fake-L1", status=400, flags=(Flag.IDEMPOTENCY_CONFLICT,)),
            precondition=Precondition(rejection_code="idempotency_error"),
        ),
        replace(_exchange(path="/v1/refunds", answered_by="overlay"), overlay="full"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        PASSED_REFUND,
        "    ↳ GET /v1/refunds saw it  overlay",
        "  ✗ stripe  refund $49.00 on ch_3QabcXYZ  faked; would fail: charge_already_refunded",
        "  ✗ stripe  refund $49.00 on ch_3QabcXYZ  faked; would fail: idempotency_error",
    ]


def test_an_engine_read_the_overlay_edited_is_never_counted_as_one_of_the_agents_reads():
    """An overlaid check read is counted once, as irimi's, never in the agent's `N reads`."""
    rows = [
        replace(_refund(answered_by="fake-L1"), precondition=Precondition("passed")),
        replace(
            _exchange(path="/v1/charges/ch_3QabcXYZ", answered_by="overlay"),
            issued_by="engine",
            overlay="full",
        ),
    ]
    line = _host(summary_lines("7f3a", rows, 0.0, _maps()))
    assert line == "  api.stripe.com  1 irimi check read  1 faked write"


def test_the_closing_line_names_each_event_once_in_first_seen_order():
    """`would have fired` names each event once, in the order first seen, not sorted."""
    fired = ("refund.created", "charge.refunded")
    rows = [
        replace(_refund(answered_by="fake-L1"), would_fire=fired),
        replace(_refund(answered_by="fake-L1"), would_fire=fired),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _fired(lines) == "  would have fired: refund.created, charge.refunded"
    assert sum("would have fired" in line for line in lines) == 1


def test_a_run_whose_every_write_was_refused_promises_no_webhooks():
    """A run whose every write was refused prints no `would have fired` line at all."""
    rows = [
        replace(
            _refund(answered_by="fake-L1", status=400),
            precondition=Precondition("rejected", "charge_already_refunded"),
        )
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert NOTHING_HAPPENED in lines
    assert _fired(lines) is None


def test_a_delegated_write_lists_none_of_its_own_but_does_not_hide_the_faked_ones():
    """A write sent to a target promises no events; a faked write beside it still lists its own."""
    rows = [
        replace(_refund(answered_by="fake-L1"), would_fire=("refund.created",)),
        _slack_post(answered_by="delegated", target="http://127.0.0.1:3000/slack"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _fired(lines) == "  would have fired: refund.created"
    assert '  ○ slack  post to #C0123: "refunded"  sent to target 127.0.0.1:3000' in lines


def test_a_replayed_write_neither_prints_a_line_nor_promises_its_events_twice():
    """A retry, answered with the first write's bytes, adds no line and names its events once."""
    fired = ("refund.created", "charge.refunded")
    rows = [
        replace(_refund(answered_by="fake-L1", flags=()), would_fire=fired),
        _refund(answered_by="fake-L1", flags=(Flag.IDEMPOTENT_REPLAY,)),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [UNCHECKED_REFUND]
    assert _fired(lines) == "  would have fired: refund.created, charge.refunded"


def test_a_write_l3_could_not_check_still_promises_its_events():
    """A write irimi could not check still lists its events: `not_evaluable` is no refusal."""
    rows = [
        replace(
            _refund(answered_by="fake-L1"),
            precondition=Precondition("not_evaluable"),
            would_fire=("refund.created", "charge.refunded"),
        )
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; could not check: irimi made no check read"
    ]
    assert _fired(lines) == "  would have fired: refund.created, charge.refunded"


def test_the_summary_s_blocks_come_in_order():
    """The summary prints the writes, money and webhooks, then the hosts, then the totals."""
    write = replace(_refund(answered_by="fake-L1"), would_fire=("refund.created",))
    lines = summary_lines("7f3a", [_exchange(), write], 0.0, _maps())
    assert lines[1:] == [
        "",
        NOTHING_HAPPENED,
        UNCHECKED_REFUND,
        "  money: $49.00 in faked writes",
        "  would have fired: refund.created",
        "",
        "  api.stripe.com  1 read  1 faked write",
        "",
        "  2 exchanges · 1 real · 1 answered by irimi (1 faked write) · 0 sent to targets",
    ]


REFUND_FORM = (("content-type", "application/x-www-form-urlencoded"),)


def _refund_checked_at_once(charge: str, started_at: float, ended_at: float) -> Exchange:
    body = f"charge={charge}&amount=500&currency=usd".encode()
    return Exchange(
        Request("POST", "https", "api.stripe.com", 443, "/v1/refunds", "", REFUND_FORM, body),
        Response(200, (), b"{}"),
        "stripe",
        "refunds.create",
        "write",
        "fake-L1",
        "run1",
        precondition=Precondition("not_evaluable"),
        would_fire=(f"refund.created.{charge}",),
        started_at=started_at,
        ended_at=ended_at,
    )


def _check_read_at(charge: str, status: int, started_at: float) -> Exchange:
    path = f"/v1/charges/{charge}"
    return Exchange(
        Request("GET", "https", "api.stripe.com", 443, path, "", (), b""),
        Response(status, (), b"{}"),
        "stripe",
        "charges.retrieve",
        "read",
        "live",
        "run1",
        issued_by="engine",
        started_at=started_at,
        ended_at=started_at + 0.01,
    )


@pytest.mark.parametrize("a_checked_first", [True, False])
def test_two_writes_checked_at_once_each_take_their_own_check_read(a_checked_first):
    """Writes decided at once each take their own check read, in either order (429, then 401)."""
    a_at, b_at = (1.1, 1.2) if a_checked_first else (1.2, 1.1)
    events = sorted(
        [_check_read_at("ch_A1", 429, a_at), _check_read_at("ch_B2", 401, b_at)],
        key=lambda e: e.started_at,
    )
    events += [
        _refund_checked_at_once("ch_A1", 1.0, 2.0),
        _refund_checked_at_once("ch_B2", 1.05, 2.1),
    ]
    lines = summary.summary_lines(RunView(RunRecord.header("run1"), events), maps_for_stored_runs())
    writes = [line.strip() for line in lines if "refund $5.00" in line]
    assert writes == [
        "○ stripe  refund $5.00 on ch_A1  faked; could not check: "
        "stripe answered irimi's check read with 429",
        "○ stripe  refund $5.00 on ch_B2  faked; could not check: "
        "stripe answered irimi's check read with 401",
    ]
    assert "  would have fired: refund.created.ch_A1" in lines
