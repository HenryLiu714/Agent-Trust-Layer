import asyncio
import dataclasses
import itertools
import threading
from dataclasses import replace
from pathlib import Path

import pytest

from irimi.exchange import Exchange, Flag, Precondition, Request, Response
from irimi.netaddr import Target
from irimi.report import model, summary
from irimi.report.model import RunView, for_summary
from irimi.report.summary import Live
from irimi.report.terminal import banner_lines, delegated_lines, exchange_line, tool_call_line
from irimi.runner import EngineThread, child_env, exit_code_for
from irimi.trace import ErrorInfo, RunRecord, ToolCall


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
    """The summary `irimi shadow` prints for a run whose engine reported `exchanges` and
    `tool_calls`."""
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
        flags=("target-failed",),
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
    # The control endpoint on the same listener, with no trailing slash.
    assert env["IRIMI_CONTROL"] == "http://127.0.0.1:4000/_irimi"


def test_child_env_preserves_base_and_does_not_mutate():
    base = {"PATH": "/bin", "HTTP_PROXY": "old"}
    env = child_env(base, "127.0.0.1", 4000, Path("/ca.pem"), "7f3a")
    assert env["PATH"] == "/bin"
    assert env["HTTP_PROXY"] == "http://127.0.0.1:4000"
    assert base["HTTP_PROXY"] == "old"


def test_banner_lines():
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


def test_exchange_line_live_read():
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


def test_summary_empty():
    """A run with no exchange says irimi saw no traffic, and why an agent's calls may not show."""
    lines = summary_lines("7f3a", [], 0.0)
    assert "0 exchanges" in lines[0]
    assert lines[-2:] == [
        "  0 exchanges · 0 real · 0 answered by irimi (0 faked writes) · 0 sent to targets",
        "  irimi saw no HTTP(S) traffic; if the agent made calls, they bypassed the proxy or "
        "failed TLS",
    ]
    assert not any("None of these writes" in line for line in lines)


def test_an_engine_that_never_binds_is_an_engine_start_error_with_a_message():
    """A ready timeout carries no message of its own; `start_engine` gives it one, so every
    command prints the same line for it."""
    from irimi.engine import EngineStartError
    from irimi.runner import start_engine

    stopped = threading.Event()

    class _NeverReady:
        async def run(self) -> None:
            while not stopped.is_set():
                await asyncio.sleep(0.01)

        async def wait_ready(self) -> None:
            await asyncio.Event().wait()

        def shutdown(self) -> None:
            stopped.set()

    with pytest.raises(EngineStartError) as refused:
        start_engine(_NeverReady(), timeout=1.0)
    assert str(refused.value) == "proxy did not start within 1s"
    assert stopped.is_set()


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
    """The kind column is nine wide for `telemetry`, and the first column as wide as its longest
    word, so no row is pushed a column right in the most-read output the tool produces."""
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
    from irimi.servicemap import ServiceMap

    base = {
        "service": "stripe",
        "hosts": frozenset({"api.stripe.com"}),
        "routes": (),
    }
    base.update(kwargs)
    if isinstance(base.get("target"), str):
        base["target"] = Target.parse(base["target"])
    return ServiceMap(**base)


def _route(**kwargs):
    from irimi.servicemap import Route

    base = {"method": "POST", "path": "/v1/refunds", "operation": "refunds.create", "kind": "write"}
    base.update(kwargs)
    if isinstance(base.get("target"), str):
        base["target"] = Target.parse(base["target"])
    return Route(**base)


def _index(*services):
    from irimi.servicemap import MapIndex

    return MapIndex(services=tuple(services))


def test_a_service_with_no_target_adds_no_banner_line():
    """No delegated service, no new lines: the banner a reader already knows stays as it was."""
    assert delegated_lines(_index(_service(routes=(_route(),)))) == []


def test_a_service_target_names_the_service_the_target_and_what_it_answers():
    lines = delegated_lines(_index(_service(target="http://127.0.0.1:3000")))
    assert lines == ["delegated: stripe -> http://127.0.0.1:3000 (writes)"]


def test_target_reads_says_the_reads_are_delegated_too():
    """The banner's "reads are real" claim is exactly what `target_reads` makes false."""
    lines = delegated_lines(_index(_service(target="http://127.0.0.1:3000", target_reads=True)))
    assert lines == ["delegated: stripe -> http://127.0.0.1:3000 (reads + writes)"]


def test_a_route_target_is_named_even_when_the_service_itself_has_none():
    """`irimi maps list` printed `target: self` for exactly this shape; the banner must not."""
    sm = _service(routes=(_route(target="http://127.0.0.1:3111"), _route(path="/v1/charges")))
    assert delegated_lines(_index(sm)) == [
        "delegated: stripe POST /v1/refunds -> http://127.0.0.1:3111 (writes)"
    ]


def test_a_delegated_read_route_says_reads():
    sm = _service(routes=(_route(kind="read", target="http://127.0.0.1:3111"),))
    assert delegated_lines(_index(sm))[0].endswith("(reads)")


def test_a_target_that_is_not_loopback_says_so_and_is_red_only_when_asked():
    """--allow-remote-target is how a target stops being loopback, and the line has to say it."""
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


# ------------------------------------------------------------------ the exit summary


def _maps():
    from irimi import servicemap

    return servicemap.load()


def _block(lines, needle):
    return next(line for line in lines if needle in line)


def _host(lines, host="api.stripe.com"):
    """`host`'s line in the per-host block. A write line can name a host too, in its request, so
    the line is found by the host column it starts with."""
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
    """The claim behind `reads are real` is what that count means to whoever reads it, and a read
    answered by a target is not a real read."""
    rows = [_exchange(), _exchange(answered_by="delegated", target="http://127.0.0.1:3000")]
    line = _host(summary_lines("7f3a", rows, 0.0, _maps()))
    assert line == "  api.stripe.com  1 read  1 read from the target"


def test_an_overlaid_read_is_a_real_read_and_says_it_shows_the_run_s_writes():
    """An overlaid read DID reach the real service, so leaving it out of the read count would
    understate the reads the agent really made. Its body is not what the service
    sent, though, so the line says that too."""
    rows = [_exchange(), _exchange(answered_by="overlay")]
    line = _host(summary_lines("7f3a", rows, 0.0, _maps()))
    assert line == "  api.stripe.com  2 reads (1 showing this run's writes)"


def test_a_read_answered_from_the_recording_is_a_read_and_says_so():
    """A replay answers a read with what the recorded run was sent: a real read, made by the
    recorded run, so it is counted with the reads, as what it was - one the overlay edited is
    still overlaid - and qualified by its `from-recording` flag. irimi answered it, not the
    service, so the totals count it answered by irimi."""
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
    """A read flagged `partial` the overlay left alone is whoever answered it: the service, or a
    replay's recording, which is not `real`."""
    refund = _refund()
    read = dataclasses.replace(
        _exchange(), answered_by="recording", overlay="partial", flags=(Flag.FROM_RECORDING,)
    )
    lines = summary_lines("7f3a", [refund, read], 0.0, _maps())
    assert "    ↳ GET /v1/charges did not show it  recorded (partial)" in lines


def test_an_overlaid_read_is_counted_live_and_never_virtualized():
    """`answered by irimi` means irimi answered instead of the service. It answered this one WITH
    the service, so counting it there would claim a read the agent really made was fabricated."""
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
    """A run posts to three vendors in the same breath; a line each would bury the two hosts the
    agent's actual work is on. It says they were forwarded live, because they really were."""
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
    and lumping it into the telemetry line would hide a write behind a host's other traffic."""
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
    """Dividing by 100 without knowing the currency prints ¥49.00 for a 4900-yen refund, which is
    a wrong number rather than an unformatted one, so the amount says it is in minor units.

    A write MAY be denominated by the L3 precondition read that checked it - the charge's own
    `currency`, carried across on the exchange. This write had no such read, so nothing knows
    what it is in."""
    lines = summary_lines("7f3a", [_refund(body=b"charge=ch_9&amount=4900")], 0.0, _maps())
    assert (
        "  ○ stripe  refund 4900 (minor units, currency unknown) on ch_9  faked; not checked"
        in lines
    )


def test_an_amount_is_denominated_by_the_currency_the_l3_read_found():
    """The write names no currency; the charge irimi read before faking it does,
    and the exchange carries it across."""
    write = replace(
        _refund(body=b"charge=ch_9&amount=4900"), precondition=Precondition(currency="usd")
    )
    lines = summary_lines("7f3a", [write], 0.0, _maps())
    assert "  ○ stripe  refund $49.00 on ch_9  faked; not checked" in lines


def test_the_requests_own_currency_still_wins_over_the_one_the_read_found():
    """The request is what the caller actually said. A charge in usd and a refund posted in jpy is
    the caller's business to be wrong about, not irimi's to correct."""
    write = replace(
        _refund(body=b"charge=ch_9&amount=4900&currency=jpy"),
        precondition=Precondition(currency="usd"),
    )
    lines = summary_lines("7f3a", [write], 0.0, _maps())
    assert "  ○ stripe  refund ¥4900 on ch_9  faked; not checked" in lines


def test_a_rejected_write_is_denominated_the_same_way():
    """`report.summary.write_sentence` renders the sentence before it branches on the rejection, so
    the `✗` line carries an amount too and must read the same as the `○` one."""
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
    """Only a `{name:money}` hole is minor units: a map whose `amount` counts something else prints
    it as the number it is, and adds nothing to the money line."""
    from irimi.servicemap import MapIndex

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
    """A write no stub ever saw must not read as a working delegation when the developer's stub
    was simply not running."""
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
    return ToolCall(tool_call_id, "7f3a", name, kind, ran, {}, None, None, started_at, ended_at)


def test_a_delegated_write_s_closing_lines_say_the_tool_writes_did_not_happen_either():
    """A write tool whose fake ran did not happen either, so it gets a line in the writes block
    under the same opening claim, after the HTTP writes. A read tool, which ran for real, is no
    write and changes nothing."""
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
    """Live, the summary's tool calls come in the order their posts reached the control endpoint,
    which two tools ending at once on two threads can reverse; a stored run's come in `seq` order,
    run by run. Each summary printed them as they came, so the two disagreed. They print in
    the order they ended, a tie broken by the start, then by the id, then by the line itself: a
    call posted by hand can repeat another's id and times."""
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
    """A faked write's line says it was faked and whether irimi checked it, whichever body it was
    answered with: the echo and the fixture read alike, and neither prints an `L` code."""
    for answered_by in ("fake-L0", "fake-L1"):
        lines = summary_lines("7f3a", [_refund(answered_by=answered_by)], 0.0, _maps())
        assert "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; not checked" in lines
        assert not any("(L" in line for line in lines)


# ------------------------------------------------------ what the precondition check said


def test_a_rejected_write_is_crossed_out_and_names_the_code_it_would_fail_with():
    """A write L3 rejected is the line the baseline report exists to print: the real service would
    have refused it. Printed with the `○` marker it read as a write that would have gone through."""
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
    """Faked, but irimi could not find out whether the service would have taken it, and the run
    holds no check read to say why."""
    rows = [replace(_refund(answered_by="fake-L1"), precondition=Precondition("not_evaluable"))]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert (
        "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; could not check: irimi made no check read"
        in lines
    )


def test_a_write_answered_from_the_recording_says_its_recorded_run_could_not_check_it():
    """A replay answers a recorded write without making a check read: the recorded run made it,
    so a check read in the replay's own run belongs to another write and is never borrowed."""
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
    """The service answered irimi's check read 401 or 403 for the agent's own credentials, so
    the real write would most likely have been refused too: the line says why it was not checked,
    and the webhooks it would have fired are not promised."""
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
    """A precondition outcome is a claim about a fake irimi built, and irimi built nothing here -
    the target did."""
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
    """irimi's own precondition read reached the real service, so it is an exchange and it is
    real; but the agent did not make it, and `N reads` is the agent's number."""
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
    """`index=None` is the empty-map case, and the write still gets a line: the summary degrades
    to the request it saw rather than dropping a write it could not name."""
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
    from irimi.trace import SCHEMA_VERSION, RunEnd, RunRecord, Trigger

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
    """The agent retried with the key it already sent and got the first answer back: the same
    write, so one line and one faked write, or "nine would have been rejected" counts retries
    instead of intentions. The exchange itself is still counted, as answered by irimi."""
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
    """A key reused for a different write is a write the real service would have refused. L3 was
    never asked, so it is not `precondition: rejected`, and it still gets the `✗` line."""
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
    """irimi answered this refund with its own 502: its body decodes past what irimi reads,
    so it was recorded with no body and flagged. Rendered through its route's `human:` template it
    would be described from a body irimi never read. It prints as refused, `✗`, as the request it
    was, saying why in words. It is still a write that did not happen.

    Its host line counts it as its line names it, `refused by irimi`: counted `unclassified`, the
    word a route no map names prints, the host line would name a write no line showed."""
    cut = _exchange(
        method="POST",
        kind="unknown",
        answered_by="fake-L0",
        status=502,
        path="/v1/refunds",
        content_type=FORM,
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
    """A decision that raised read the whole body, so the refusal keeps the map's sentence, and two
    refunds of different amounts that both raised print, and compare, as different writes:
    the request line alone would read the same for both."""
    raised = _exchange(
        method="POST",
        kind="unknown",
        answered_by="fake-L0",
        status=502,
        path="/v1/refunds",
        body=b"charge=ch_3QabcXYZ&amount=4900&currency=usd",
        content_type=FORM,
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


PASSED_REFUND = (
    "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; checked against live state: would succeed"
)
UNCHECKED_REFUND = "  ○ stripe  refund $49.00 on ch_3QabcXYZ  faked; not checked"


def test_an_overlaid_read_is_listed_under_the_write_it_saw():
    """The per-host count said a read showed this run's writes and never said which read, or which
    write. The reader could not tell whether the agent re-read its own refund."""
    rows = [
        replace(_refund(answered_by="fake-L1"), precondition=Precondition("passed")),
        replace(_exchange(path="/v1/refunds", answered_by="overlay"), overlay="full"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [PASSED_REFUND, "    ↳ GET /v1/refunds saw it  overlay"]


def test_a_partial_overlay_hit_says_it_saw_only_part():
    """`partial` means irimi knowingly showed the agent an incomplete world. Printing it as a plain
    `saw it` would claim a fidelity the trace itself denies."""
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
    """A Slack read whose channel the two sides spell differently comes back untouched and real,
    yet irimi knows it did not show the post. Staying silent because the body was not edited
    would hide the one thing irimi knows about that read."""
    rows = [_slack_post(), replace(_slack_history(), overlay="partial")]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [
        '  ○ slack  post to #C0123: "refunded"  faked; not checked',
        "    ↳ POST /api/conversations.history did not show it  real (partial)",
    ]


def test_a_translated_read_that_needed_no_edit_gets_no_overlay_line():
    """`overlay: full` on a `live` read means irimi translated the request and the body needed no
    edit. It is not an overlay hit, and a `↳` line would say the agent saw a write it did not.
    """
    rows = [
        _refund(answered_by="fake-L1"),
        replace(_exchange(path="/v1/refunds"), overlay="full"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [UNCHECKED_REFUND]


def test_an_engine_issued_read_never_gets_an_overlay_line():
    """The `↳` line says which of the AGENT's reads saw the write. irimi's own precondition read is
    one the agent never made, so hanging it there would credit the agent with a look it did not
    take."""
    rows = [
        replace(_refund(answered_by="fake-L1"), precondition=Precondition("passed")),
        replace(_exchange(path="/v1/charges/ch_3QabcXYZ"), issued_by="engine", overlay="partial"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [PASSED_REFUND]


def test_an_overlaid_read_is_filed_under_its_own_services_write():
    """The overlay applies only a service's own writes to that service's reads, so a read belongs
    under the latest write of ITS service, not simply the latest write. Filed by position alone, the
    Stripe re-read below would sit under the Slack post it never saw."""
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
    """A refused write is printed, but it never enters the write log, so no read ever saw it. A
    read after a refund and its refused retry saw the REFUND; filed by position it would hang
    under the `✗` line and tell the reader the agent was shown a refund that was refused."""
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
    """The bracket qualifies the agent's `N reads`, so it counts the agent's reads only. An engine
    read is its own phrase; counted in both, one read would show up twice."""
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
    """Two refunds carry two identical tuples; naming `refund.created` twice would promise a webhook
    per line rather than per event, and a sorted list would lose the order the map gives."""
    fired = ("refund.created", "charge.refunded")
    rows = [
        replace(_refund(answered_by="fake-L1"), would_fire=fired),
        replace(_refund(answered_by="fake-L1"), would_fire=fired),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _fired(lines) == "  would have fired: refund.created, charge.refunded"
    assert sum("would have fired" in line for line in lines) == 1


def test_a_run_whose_every_write_was_refused_promises_no_webhooks():
    """A write the real service would have refused fires nothing. `would have fired: nothing` would
    be a claim; no line at all is the truth."""
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
    """A delegated write's `would_fire` is empty by construction, because what the target did is not
    irimi's to promise. The faked write beside it still owes the reader its events."""
    rows = [
        replace(_refund(answered_by="fake-L1"), would_fire=("refund.created",)),
        _slack_post(answered_by="delegated", target="http://127.0.0.1:3000/slack"),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _fired(lines) == "  would have fired: refund.created"
    assert '  ○ slack  post to #C0123: "refunded"  sent to target 127.0.0.1:3000' in lines


def test_a_replayed_write_neither_prints_a_line_nor_promises_its_events_twice():
    """A replay is the same write answered with the first one's bytes: one line, and its events
    named once, or the summary counts retries instead of intentions."""
    fired = ("refund.created", "charge.refunded")
    rows = [
        replace(_refund(answered_by="fake-L1", flags=()), would_fire=fired),
        _refund(answered_by="fake-L1", flags=(Flag.IDEMPOTENT_REPLAY,)),
    ]
    lines = summary_lines("7f3a", rows, 0.0, _maps())
    assert _write_block(lines) == [UNCHECKED_REFUND]
    assert _fired(lines) == "  would have fired: refund.created, charge.refunded"


def test_a_write_l3_could_not_check_still_promises_its_events():
    """`not_evaluable` means irimi could not find out whether the service would have taken the
    write, not that it would have refused it. The write was faked, so its events are still owed.
    """
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
    """The writes first, under the claim that none happened, with their money and webhooks; then
    the hosts; then the totals."""
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
