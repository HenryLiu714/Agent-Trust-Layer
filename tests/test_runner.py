import asyncio
import threading
from dataclasses import replace
from pathlib import Path

from irimi.exchange import (
    IDEMPOTENCY_CONFLICT_FLAG,
    IDEMPOTENT_REPLAY_FLAG,
    Exchange,
    Request,
    Response,
)
from irimi.report import banner_lines, delegated_lines, exchange_line, summary_lines
from irimi.runner import EngineThread, child_env, exit_code_for


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
    req = Request(
        method=method,
        scheme="https",
        host=host,
        port=443,
        path=path,
        query="",
        headers=((("content-type", content_type),) if content_type else ()),
        body=body,
    )
    resp = Response(status=status, headers=(), body=b"") if status is not None else None
    return Exchange(
        request=req,
        response=resp,
        service=service,
        operation=f"{method} {path}",
        kind=kind,
        answered_by=answered_by,
        validation="unvalidated",
        run_id="7f3a",
        flags=flags,
        target=target,
    )


FORM = "application/x-www-form-urlencoded"


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
        content_type=FORM,
        target=target,
        flags=flags,
        status=status,
    )


def _unreachable(target="http://127.0.0.1:3999/refund"):
    """A delegated write whose target could not be dialled: `IrimiAddon.error` records it with no
    response and the `target-failed` flag, exactly as a stub that is not running produces."""
    return _refund(
        answered_by="delegated",
        target=target,
        flags=("fidelity:delegated", "target-failed"),
        status=None,
    )


def test_child_env_sets_proxy_and_ca():
    env = child_env({}, "127.0.0.1", 4000, Path("/ca.pem"), "7f3a")
    assert env["HTTP_PROXY"] == "http://127.0.0.1:4000"
    assert env["HTTPS_PROXY"] == "http://127.0.0.1:4000"
    assert env["http_proxy"] == "http://127.0.0.1:4000"
    assert env["https_proxy"] == "http://127.0.0.1:4000"
    assert env["NO_PROXY"] == "localhost,127.0.0.1"
    assert env["no_proxy"] == "localhost,127.0.0.1"
    for var in ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "NODE_EXTRA_CA_CERTS"):
        assert env[var] == "/ca.pem"
    assert env["NODE_USE_ENV_PROXY"] == "1"
    assert env["IRIMI_ENGINE_ACTIVE"] == "1"
    assert env["IRIMI_RUN"] == "7f3a"


def test_child_env_preserves_base_and_does_not_mutate():
    base = {"PATH": "/bin", "HTTP_PROXY": "old"}
    env = child_env(base, "127.0.0.1", 4000, Path("/ca.pem"), "7f3a")
    assert env["PATH"] == "/bin"
    assert env["HTTP_PROXY"] == "http://127.0.0.1:4000"
    assert base["HTTP_PROXY"] == "old"


def test_banner_lines():
    lines = banner_lines("shadow", "7f3a", "127.0.0.1", 4000, Path("/ca.pem"))
    assert len(lines) == 3
    assert "shadow" in lines[0]
    assert "7f3a" in lines[0]
    assert "127.0.0.1:4000" in lines[0]
    assert "/ca.pem" in lines[0]
    assert lines[1] == "hosts not routed through the proxy are NOT virtualized."
    assert lines[2] == "backstop: none (Phase 4)"


def test_exchange_line_live_read():
    line = exchange_line(_exchange())
    assert "live" in line
    assert "read" in line
    assert "GET" in line
    assert "api.stripe.com/v1/charges" in line
    assert "200" in line


def test_exchange_line_shows_flags_and_missing_response():
    line = exchange_line(_exchange(status=None, flags=("upstream-error",)))
    assert "-" in line
    assert "[upstream-error]" in line


def test_summary_header_names_the_run_the_count_the_time_and_the_backstop():
    lines = summary_lines("7f3a", [_exchange(), _exchange()], 2.34)
    assert lines[0] == "irimi shadow · run 7f3a · 2 exchanges · 2.3s · backstop: none (Phase 4)"


def test_summary_header_says_one_exchange_not_one_exchanges():
    assert "1 exchange ·" in summary_lines("7f3a", [_exchange()], 0.0)[0]


def test_summary_empty():
    lines = summary_lines("7f3a", [], 0.0)
    assert "0 exchanges" in lines[0]
    assert any("0 live · 0 delegated · 0 virtualized" in line for line in lines)
    assert not any("did not happen" in line for line in lines)


def test_exit_code_for():
    assert exit_code_for(0) == 0
    assert exit_code_for(7) == 7
    assert exit_code_for(-15) == 143
    assert exit_code_for(-2) == 130
    assert exit_code_for(-9) == 137


def test_engine_thread_stop_is_idempotent():
    loop = asyncio.new_event_loop()

    class _StubEngine:
        def shutdown(self) -> None:
            # Like MitmEngine.shutdown(), this reaches into the serving loop; a closed one raises.
            loop.call_soon_threadsafe(loop.stop)

    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    handle = EngineThread(engine=_StubEngine(), thread=thread, loop=loop)
    handle.stop()
    assert not thread.is_alive()
    assert loop.is_closed()
    handle.stop()  # a second stop must not reach the closed loop


def test_exchange_line_columns_line_up_for_the_nine_character_values():
    """`telemetry` and `delegated` are nine characters. At eight, every row carrying one was
    pushed a column right in the most-read output the tool produces (#29)."""
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
    assert exchange_line(_exchange(answered_by="delegated")).startswith("delegated")


def _service(**kwargs):
    from irimi.servicemap import ServiceMap

    base = {
        "service": "stripe",
        "hosts": frozenset({"api.stripe.com"}),
        "routes": (),
    }
    base.update(kwargs)
    return ServiceMap(**base)


def _route(**kwargs):
    from irimi.servicemap import Route

    base = {"method": "POST", "path": "/v1/refunds", "operation": "refunds.create", "kind": "write"}
    base.update(kwargs)
    return Route(**base)


def _index(*services):
    from irimi.servicemap import MapIndex

    return MapIndex(services=tuple(services))


def test_a_service_with_no_target_adds_no_banner_line():
    """No delegated service, no new lines: the banner a reader already knows stays as it was."""
    assert delegated_lines(_index(_service(routes=(_route(),)))) == []


def test_a_service_target_names_the_service_the_target_and_what_it_answers():
    lines = delegated_lines(_index(_service(target="http://127.0.0.1:3000")))
    assert lines == ["delegated: stripe → http://127.0.0.1:3000 (writes)"]


def test_target_reads_says_the_reads_are_delegated_too():
    """The banner's "reads are real" claim is exactly what `target_reads` makes false (#20)."""
    lines = delegated_lines(_index(_service(target="http://127.0.0.1:3000", target_reads=True)))
    assert lines == ["delegated: stripe → http://127.0.0.1:3000 (reads + writes)"]


def test_a_route_target_is_named_even_when_the_service_itself_has_none():
    """`irimi maps list` printed `target: self` for exactly this shape; the banner must not."""
    sm = _service(routes=(_route(target="http://127.0.0.1:3111"), _route(path="/v1/charges")))
    assert delegated_lines(_index(sm)) == [
        "delegated: stripe POST /v1/refunds → http://127.0.0.1:3111 (writes)"
    ]


def test_a_delegated_read_route_says_reads():
    sm = _service(routes=(_route(kind="read", target="http://127.0.0.1:3111"),))
    assert delegated_lines(_index(sm))[0].endswith("(reads)")


def test_a_target_that_is_not_loopback_says_so_and_is_red_only_when_asked():
    """--allow-target-host is how a target stops being loopback, and the line has to say it."""
    index = _index(_service(target="http://stub.example:3000"))
    plain, notice = delegated_lines(index)
    assert "NOT loopback" in notice
    assert all("\033[" not in line for line in (plain, notice))
    assert all(len(line) <= 100 for line in (plain, notice))
    coloured = delegated_lines(index, color=True)
    assert len(coloured) == 2
    assert all(c.startswith("\033[31m") and c.endswith("\033[0m") for c in coloured)


def test_a_loopback_target_is_never_painted_red():
    (line,) = delegated_lines(_index(_service(target="http://127.0.0.1:3000")), color=True)
    assert "\033[" not in line
    assert "NOT loopback" not in line


def test_delegated_lines_are_sorted_by_service():
    index = _index(
        _service(service="stripe", target="http://127.0.0.1:3000"),
        _service(service="acme", hosts=frozenset({"acme.test"}), target="http://127.0.0.1:3001"),
    )
    assert [line.split()[1] for line in delegated_lines(index)] == ["acme", "stripe"]


# ------------------------------------------------------------------ the exit summary (#13, #20)


def _maps():
    from irimi import servicemap

    return servicemap.load()


def _block(lines, needle):
    return next(line for line in lines if needle in line)


def test_a_host_line_separates_reads_from_intercepted_writes():
    lines = summary_lines("7f3a", [_exchange(), _exchange(), _refund()], 0.0, _maps())
    assert _block(lines, "api.stripe.com") == "  api.stripe.com  2 reads  1 write intercepted"


def test_a_delegated_read_is_never_counted_as_a_live_read():
    """The claim behind `reads are real` is what that count means to whoever reads it, and a read
    answered by a target is not a real read (#20)."""
    rows = [_exchange(), _exchange(answered_by="delegated", target="http://127.0.0.1:3000")]
    line = _block(summary_lines("7f3a", rows, 0.0, _maps()), "api.stripe.com")
    assert line == "  api.stripe.com  1 read  1 read from the target"


def test_an_overlaid_read_is_a_real_read_and_says_it_shows_the_run_s_writes():
    """The opposite of #20's bug: an overlaid read DID reach the real service, so leaving it out of
    the read count understated the reads the agent really made. Its body is not what the service
    sent, though, so the line says that too (#43)."""
    rows = [_exchange(), _exchange(answered_by="overlay")]
    line = _block(summary_lines("7f3a", rows, 0.0, _maps()), "api.stripe.com")
    assert line == "  api.stripe.com  2 reads (1 showing this run's writes)"


def test_an_overlaid_read_is_counted_live_and_never_virtualized():
    """`virtualized` means irimi answered instead of the service. It answered this one WITH the
    service, so counting it there claimed a read the agent really made was fabricated (#43)."""
    rows = [_exchange(answered_by="overlay"), _refund()]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  2 exchanges · 1 live · 0 delegated · 1 virtualized" in lines


def test_an_intercepted_write_that_was_delegated_says_so_in_its_host_line():
    rows = [_refund(), _refund(answered_by="delegated", target="http://127.0.0.1:3000/refund")]
    line = _block(summary_lines("7f3a", rows, 0.0, _maps()), "api.stripe.com")
    assert line == "  api.stripe.com  2 writes intercepted (1 delegated)"


def test_an_unclassified_write_is_counted_and_named():
    rows = [_refund(), _exchange(method="DELETE", kind="unknown", answered_by="fake-L0")]
    line = _block(summary_lines("7f3a", rows, 0.0, _maps()), "api.stripe.com")
    assert line == "  api.stripe.com  2 writes intercepted (1 unclassified)"


def test_llm_gets_its_own_word():
    rows = [_exchange(kind="llm", host="api.openai.com", service="openai", method="POST")]
    assert _block(summary_lines("7f3a", rows, 0.0, _maps()), "api.openai.com").endswith("1 llm")


def test_every_telemetry_host_shares_one_line():
    """A run posts to three vendors in the same breath; a line each would bury the two hosts the
    agent's actual work is on. It says they were forwarded live, because they really were (#20)."""
    rows = [
        _exchange(kind="telemetry", host="api.datadoghq.com", service="datadog", method="POST"),
        _exchange(kind="telemetry", host="api.datadoghq.com", service="datadog", method="POST"),
        _exchange(kind="telemetry", host="o0.ingest.sentry.io", service="sentry", method="POST"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _block(lines, "telemetry") == "  telemetry  3 exchanges to 2 hosts, forwarded live"
    assert not any("api.datadoghq.com" in line for line in lines)


def test_a_telemetry_host_still_gets_a_host_line_for_its_control_plane_writes():
    """`kind` is per route: a DELETE on a dashboard is a write on the same host the logs go to,
    and lumping it into the telemetry line is exactly the scope mistake #30 was filed for."""
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
    assert "  api.datadoghq.com  1 write intercepted (1 unclassified)" in lines
    assert "  telemetry          1 exchange to 1 host, forwarded live" in lines


def test_a_write_is_rendered_from_the_maps_human_template():
    lines = summary_lines("7f3a", [_refund()], 0.0, _maps())
    assert "  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L0)" in lines


def test_an_amount_with_no_currency_in_the_request_is_left_as_it_was_sent():
    """Dividing by 100 without knowing the currency prints ¥49.00 for a 4900-yen refund, which is
    a wrong number rather than an unformatted one. Nothing else in a shadow run knows the
    currency: the charge it refers to was read live and its fields are Phase 2's to carry."""
    lines = summary_lines("7f3a", [_refund(body=b"charge=ch_9&amount=4900")], 0.0, _maps())
    assert "  ○ refund 4900 on ch_9  unvalidated (L0)" in lines


def test_a_zero_decimal_currency_is_not_divided():
    lines = summary_lines(
        "7f3a", [_refund(body=b"charge=ch_9&amount=4900&currency=jpy")], 0.0, _maps()
    )
    assert "  ○ refund ¥4900 on ch_9  unvalidated (L0)" in lines


def test_a_currency_with_no_symbol_is_named():
    lines = summary_lines(
        "7f3a", [_refund(body=b"charge=ch_9&amount=4900&currency=sek")], 0.0, _maps()
    )
    assert "  ○ refund 49.00 SEK on ch_9  unvalidated (L0)" in lines


def test_a_template_hole_the_request_does_not_fill_is_marked_not_dropped():
    lines = summary_lines("7f3a", [_refund(body=b"amount=4900&currency=usd")], 0.0, _maps())
    assert "  ○ refund $49.00 on ?  unvalidated (L0)" in lines


def test_a_path_parameter_fills_a_hole_the_body_does_not():
    rows = [
        _exchange(
            method="POST",
            kind="write",
            answered_by="fake-L0",
            path="/v1/payment_intents/pi_REAL999/cancel",
        )
    ]
    assert "  ○ cancel pi_REAL999  unvalidated (L0)" in summary_lines("7f3a", rows, 0.0, _maps())


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
    assert "  ○ POST api.unmapped.test/do/a/thing  unclassified (L0)" in lines


def test_a_delegated_write_names_the_address_that_answered_it():
    rows = [_refund(answered_by="delegated", target="http://127.0.0.1:3000/refund")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert (
        "  ○ refund $49.00 on ch_3QabcXYZ → http://127.0.0.1:3000/refund  unvalidated (delegated)"
        in lines
    )


def test_the_three_buckets_are_counted_on_one_line():
    rows = [
        _exchange(),
        _exchange(kind="telemetry", host="api.datadoghq.com", service="datadog", method="POST"),
        _refund(),
        _refund(answered_by="delegated", target="http://127.0.0.1:3000/refund"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  4 exchanges · 2 live · 1 delegated · 1 virtualized" in lines


def test_the_closing_line_is_the_plain_claim_when_nothing_was_delegated():
    lines = summary_lines("7f3a", [_refund()], 0.0, _maps())
    assert lines[-1] == "  These writes did not happen."


def test_the_closing_lines_say_where_a_delegated_write_went():
    rows = [_refund(), _refund(answered_by="delegated", target="http://127.0.0.1:3000/refund")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert lines[-2] == "  These writes did not reach stripe."
    assert lines[-1] == "  1 was delegated to http://127.0.0.1:3000/refund."


def test_a_write_whose_target_was_never_reached_is_not_counted_as_delegated():
    """The host line said `1 delegated` for a write no stub ever saw, which reads as a working
    delegation when the developer's stub was simply not running."""
    line = _block(summary_lines("7f3a", [_unreachable()], 0.0, _maps()), "api.stripe.com")
    assert line == "  api.stripe.com  1 write intercepted (1 target unreachable)"


def test_a_delegated_write_and_an_unreachable_one_are_counted_apart():
    rows = [_refund(answered_by="delegated", target="http://127.0.0.1:3000/refund"), _unreachable()]
    assert _block(summary_lines("7f3a", rows, 0.0, _maps()), "api.stripe.com") == (
        "  api.stripe.com  2 writes intercepted (1 delegated, 1 target unreachable)"
    )


def test_an_unreachable_target_says_the_write_was_not_answered_at_all():
    lines = summary_lines("7f3a", [_unreachable()], 0.0, _maps())
    assert (
        "  ○ refund $49.00 on ch_3QabcXYZ → http://127.0.0.1:3999/refund  "
        "unanswered (target unreachable)" in lines
    )


def test_the_closing_lines_never_claim_an_unreachable_target_answered():
    lines = summary_lines("7f3a", [_unreachable()], 0.0, _maps())
    assert lines[-2] == "  These writes did not reach stripe."
    assert lines[-1] == (
        "  1 was not answered at all: http://127.0.0.1:3999/refund could not be reached, "
        "and the agent got a 502."
    )
    assert not any("delegated to" in line for line in lines)


def test_a_delegated_write_and_an_unreachable_one_each_get_their_own_sentence():
    rows = [_refund(answered_by="delegated", target="http://127.0.0.1:3000/refund"), _unreachable()]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert lines[-3] == "  These writes did not reach stripe."
    assert lines[-2] == "  1 was delegated to http://127.0.0.1:3000/refund."
    assert lines[-1].startswith("  1 was not answered at all:")


def test_two_unreachable_writes_read_as_were():
    assert summary_lines("7f3a", [_unreachable()] * 2, 0.0, _maps())[-1].startswith(
        "  2 were not answered at all:"
    )


def test_two_delegated_writes_read_as_were():
    rows = [_refund(answered_by="delegated", target="http://127.0.0.1:3000/refund")] * 2
    assert summary_lines("7f3a", rows, 0.0, _maps())[-1].startswith("  2 were delegated to")


def test_a_run_that_only_read_claims_nothing_about_writes():
    lines = summary_lines("7f3a", [_exchange()], 0.0, _maps())
    assert not any("did not happen" in line or "did not reach" in line for line in lines)


def test_the_write_line_names_the_fidelity_the_answer_actually_had():
    """`L0`, `L1` or `delegated`, read off `answered_by` so there is no second mapping to keep in
    step. A summary claiming `L0` for an L1 answer is the same class of untruth as counting a
    delegated read as live (#20, #41)."""
    lines = summary_lines("7f3a", [_refund(answered_by="fake-L1")], 0.0, _maps())
    assert "  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L1)" in lines
    lines = summary_lines("7f3a", [_refund()], 0.0, _maps())
    assert "  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L0)" in lines


# ------------------------------------------------------ what L3 said, in Notion's own words (#45)


def test_a_rejected_write_is_crossed_out_and_names_the_code_it_would_fail_with():
    """A write L3 rejected is the line the baseline report exists to print: the real service would
    have refused it. Printed as `unvalidated (L1)` it read as a write that was faked (#45)."""
    rows = [
        replace(
            _refund(answered_by="fake-L1", status=400),
            precondition="rejected",
            rejection_code="charge_already_refunded",
        )
    ]
    line = _block(summary_lines("7f3a", rows, 0.0, _maps()), "would fail")
    assert line.startswith("  ✗ ")
    assert line == "  ✗ refund $49.00 on ch_3QabcXYZ  would fail: charge_already_refunded"


def test_a_passed_write_reads_l3_preconditions_passed():
    rows = [replace(_refund(answered_by="fake-L1"), precondition="passed")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L3 preconditions passed)" in lines


def test_a_write_l3_could_not_evaluate_reads_l2():
    """Faked at L1, but irimi could not find out whether the service would have taken it, so what
    it knew was the overlay's worth of truth and no more."""
    rows = [replace(_refund(answered_by="fake-L1"), precondition="not_evaluable")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L2)" in lines


def test_a_write_on_a_route_with_no_precondition_keeps_its_own_level():
    rows = [_refund(answered_by="fake-L1")]
    assert rows[0].precondition is None
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert "  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L1)" in lines


def test_a_delegated_write_l3_could_not_evaluate_still_reads_delegated():
    """`L2` is a claim about a fake irimi built, and irimi built nothing here - the target did."""
    rows = [
        replace(
            _refund(answered_by="delegated", target="http://127.0.0.1:3000/refund"),
            precondition="not_evaluable",
        )
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert (
        "  ○ refund $49.00 on ch_3QabcXYZ → http://127.0.0.1:3000/refund  unvalidated (delegated)"
        in lines
    )
    assert not any("(L2)" in line for line in lines)


def test_an_engine_read_is_counted_apart_from_the_agents_reads_but_still_went_live():
    """irimi's own precondition read reached the real service, so it is an exchange and it is
    `live`; but the agent did not make it, and `N reads` is the agent's number (#45)."""
    rows = [
        _exchange(),
        replace(_exchange(path="/v1/charges/ch_3QabcXYZ"), issued_by="engine"),
        replace(_refund(answered_by="fake-L1"), precondition="passed"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _block(lines, "api.stripe.com  ") == (
        "  api.stripe.com  1 read  1 engine read  1 write intercepted"
    )
    assert lines[0].startswith("irimi shadow · run 7f3a · 3 exchanges · ")
    assert "  3 exchanges · 2 live · 0 delegated · 1 virtualized" in lines


def test_the_summary_still_names_a_write_without_the_maps():
    """`index=None` is the empty-map case, and the write still gets a line: the summary degrades
    to the request it saw rather than dropping a write it could not name."""
    lines = summary_lines("7f3a", [_refund()], 0.0, None)
    assert "  ○ POST api.stripe.com/v1/refunds  unvalidated (L0)" in lines


# ------------------------------------------------------------------- idempotency keys (#46)


def test_a_replayed_write_is_counted_once():
    """The agent retried with the key it already sent and got the first answer back: the same
    write, so one line and one intercepted write, or "nine would have been rejected" counts
    retries instead of intentions (#46). The exchange itself is still counted and virtualized."""
    rows = [
        _refund(answered_by="fake-L1", flags=("fidelity:L1",)),
        _refund(answered_by="fake-L1", flags=("fidelity:L1", IDEMPOTENT_REPLAY_FLAG)),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert [line for line in lines if line.startswith("  ○ ")] == [
        "  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L1)"
    ]
    assert _block(lines, "api.stripe.com") == "  api.stripe.com  1 write intercepted"
    assert "  2 exchanges · 0 live · 0 delegated · 2 virtualized" in lines


def test_an_idempotency_conflict_prints_as_a_write_that_would_fail():
    """A key reused for a different write is a write the real service would have refused. L3 was
    never asked, so it is not `precondition: rejected`, and it still gets the `✗` line (#46)."""
    rows = [
        _refund(answered_by="fake-L1"),
        replace(
            _refund(
                answered_by="fake-L1",
                status=400,
                flags=("fidelity:L1", IDEMPOTENCY_CONFLICT_FLAG),
            ),
            rejection_code="idempotency_error",
        ),
    ]
    assert rows[1].precondition is None
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    line = _block(lines, "would fail")
    assert line == "  ✗ refund $49.00 on ch_3QabcXYZ  would fail: idempotency_error"
    assert _block(lines, "api.stripe.com") == "  api.stripe.com  2 writes intercepted"


# ------------------------------ the reads that saw a write, and what it would have fired (#48)


def _write_block(lines):
    """The write block as printed: each `○`/`✗` line, with the `↳` lines hanging under it.

    Any line carrying `↳` is kept whatever its indent, so a test that expects none still sees one
    printed at the wrong depth rather than filtering it out."""
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
        content_type=FORM,
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


def test_an_overlaid_read_is_listed_under_the_write_it_saw():
    """The per-host count said a read showed this run's writes and never said which read, or which
    write. The reader could not tell whether the agent re-read its own refund (#48)."""
    rows = [
        replace(_refund(answered_by="fake-L1"), precondition="passed"),
        replace(_exchange(path="/v1/refunds", answered_by="overlay"), overlay="full"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        "  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L3 preconditions passed)",
        "    ↳ GET /v1/refunds saw it  overlay",
    ]


def test_a_partial_overlay_hit_says_it_saw_only_part():
    """`partial` means irimi knowingly showed the agent an incomplete world. Printing it as a plain
    `saw it` would claim a fidelity the trace itself denies (#43)."""
    rows = [
        _refund(answered_by="fake-L1"),
        replace(_exchange(path="/v1/refunds", answered_by="overlay"), overlay="partial"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        "  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L1)",
        "    ↳ GET /v1/refunds saw it in part  overlay (partial)",
    ]


def test_a_read_irimi_knew_was_incomplete_says_so_even_though_it_answered_none_of_it():
    """A Slack read whose channel the two sides spell differently comes back untouched and `live`,
    yet irimi knows it did not show the post (#44). Staying silent because the body was not edited
    would hide the one thing irimi knows about that read (#43)."""
    rows = [_slack_post(), replace(_slack_history(), overlay="partial")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        '  ○ post to #C0123: "refunded"  unvalidated (L1)',
        "    ↳ POST /api/conversations.history did not show it  live (partial)",
    ]


def test_a_translated_read_that_needed_no_edit_gets_no_overlay_line():
    """`overlay: full` on a `live` read means irimi translated the request and the body needed no
    edit. It is not an overlay hit, and a `↳` line would say the agent saw a write it did not
    (#43)."""
    rows = [
        _refund(answered_by="fake-L1"),
        replace(_exchange(path="/v1/refunds"), overlay="full"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == ["  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L1)"]


def test_an_engine_issued_read_never_gets_an_overlay_line():
    """The `↳` line says which of the AGENT's reads saw the write. irimi's own precondition read is
    one the agent never made, so hanging it there would credit the agent with a look it did not
    take (#45)."""
    rows = [
        replace(_refund(answered_by="fake-L1"), precondition="passed"),
        replace(_exchange(path="/v1/charges/ch_3QabcXYZ"), issued_by="engine", overlay="partial"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        "  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L3 preconditions passed)"
    ]


def test_an_overlaid_read_is_filed_under_its_own_services_write():
    """The overlay applies only a service's own writes to that service's reads, so a read belongs
    under the latest write of ITS service, not simply the latest write. Filed by position alone, the
    Stripe re-read below would sit under the Slack post it never saw (#48)."""
    rows = [
        _refund(answered_by="fake-L1"),
        _slack_post(),
        replace(_slack_history(answered_by="overlay"), overlay="full"),
        replace(_exchange(path="/v1/charges/ch_3QabcXYZ", answered_by="overlay"), overlay="full"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        "  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L1)",
        "    ↳ GET /v1/charges/ch_3QabcXYZ saw it  overlay",
        '  ○ post to #C0123: "refunded"  unvalidated (L1)',
        "    ↳ POST /api/conversations.history saw it  overlay",
    ]


def test_an_engine_read_the_overlay_edited_is_never_counted_as_one_of_the_agents_reads():
    """The bracket qualifies the agent's `N reads`, so it counts the agent's reads only. An engine
    read is its own phrase; counted in both, one read would show up twice (#45, #48)."""
    rows = [
        replace(_refund(answered_by="fake-L1"), precondition="passed"),
        replace(
            _exchange(path="/v1/charges/ch_3QabcXYZ", answered_by="overlay"),
            issued_by="engine",
            overlay="full",
        ),
    ]
    line = _block(summary_lines("7f3a", rows, 0.0, _maps()), "api.stripe.com  ")
    assert line == "  api.stripe.com  1 engine read  1 write intercepted"


def test_the_closing_line_names_each_event_once_in_first_seen_order():
    """Two refunds carry two identical tuples; naming `refund.created` twice would promise a webhook
    per line rather than per event, and a sorted list would lose the order the map gives (#47)."""
    fired = ("refund.created", "charge.refunded")
    rows = [
        replace(_refund(answered_by="fake-L1"), would_fire=fired),
        replace(_refund(answered_by="fake-L1"), would_fire=fired),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert lines[-1] == (
        "  These writes did not happen. Would have fired: refund.created, charge.refunded."
    )
    assert sum("Would have fired" in line for line in lines) == 1


def test_a_run_whose_every_write_was_refused_promises_no_webhooks():
    """A write the real service would have refused fires nothing. `Would have fired: nothing.` would
    be a claim; the bare sentence is the truth (#47)."""
    rows = [
        replace(
            _refund(answered_by="fake-L1", status=400),
            precondition="rejected",
            rejection_code="charge_already_refunded",
        )
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert lines[-1] == "  These writes did not happen."
    assert not any("Would have fired" in line for line in lines)


def test_a_delegated_write_lists_none_of_its_own_but_does_not_hide_the_faked_ones():
    """A delegated write's `would_fire` is empty by construction, because what the target did is not
    irimi's to promise. The faked write beside it still owes the reader its events (#47)."""
    rows = [
        replace(_refund(answered_by="fake-L1"), would_fire=("refund.created",)),
        _slack_post(answered_by="delegated", target="http://127.0.0.1:3000/slack"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert lines[-2] == (
        "  These writes did not reach slack, stripe. Would have fired: refund.created."
    )
    assert lines[-1] == "  1 was delegated to http://127.0.0.1:3000/slack."


def test_a_replayed_write_neither_prints_a_line_nor_promises_its_events_twice():
    """A replay is the same write answered with the first one's bytes: one line, and its events
    named once, or the summary counts retries instead of intentions (#46)."""
    fired = ("refund.created", "charge.refunded")
    rows = [
        replace(_refund(answered_by="fake-L1", flags=("fidelity:L1",)), would_fire=fired),
        _refund(answered_by="fake-L1", flags=("fidelity:L1", IDEMPOTENT_REPLAY_FLAG)),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == ["  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L1)"]
    assert lines[-1] == (
        "  These writes did not happen. Would have fired: refund.created, charge.refunded."
    )


def test_a_write_l3_could_not_check_still_promises_its_events():
    """`not_evaluable` means irimi could not find out whether the service would have taken the
    write, not that it would have refused it. The write was faked, so its events are still owed
    (#47)."""
    rows = [
        replace(
            _refund(answered_by="fake-L1"),
            precondition="not_evaluable",
            would_fire=("refund.created", "charge.refunded"),
        )
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == ["  ○ refund $49.00 on ch_3QabcXYZ  unvalidated (L2)"]
    assert lines[-1] == (
        "  These writes did not happen. Would have fired: refund.created, charge.refunded."
    )
