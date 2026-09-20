import asyncio
import threading
from pathlib import Path

from irimi.exchange import Exchange, Request, Response
from irimi.runner import (
    EngineThread,
    banner_lines,
    child_env,
    delegated_lines,
    exchange_line,
    exit_code_for,
    summary_lines,
)


def _exchange(method="GET", kind="read", answered_by="live", status=200, flags=()):
    req = Request(
        method=method,
        scheme="https",
        host="api.stripe.com",
        port=443,
        path="/v1/charges",
        query="",
        headers=(),
        body=b"",
    )
    resp = Response(status=status, headers=(), body=b"") if status is not None else None
    return Exchange(
        request=req,
        response=resp,
        service="api.stripe.com",
        operation=f"{method} /v1/charges",
        kind=kind,
        answered_by=answered_by,
        validation="unvalidated",
        run_id="7f3a",
        flags=flags,
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


def test_summary_counts():
    exchanges = [
        _exchange(),
        _exchange(),
        _exchange(method="POST", kind="unknown", answered_by="fake-L0"),
    ]
    lines = summary_lines("7f3a", exchanges)
    assert "7f3a" in lines[0]
    assert "3 exchange(s)" in lines[0]
    assert "live:" in lines[1]
    assert "2" in lines[1]
    assert "read=2" in lines[1]
    assert "virtualized:" in lines[2]
    assert "1" in lines[2]
    assert "unknown=1" in lines[2]


def test_summary_empty():
    lines = summary_lines("7f3a", [])
    assert "0 exchange(s)" in lines[0]
    assert lines[1].endswith("0")
    assert lines[2].endswith("0")


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
    (plain,) = delegated_lines(index)
    assert "NOT loopback" in plain
    assert "\033[" not in plain
    (coloured,) = delegated_lines(index, color=True)
    assert coloured.startswith("\033[31m") and coloured.endswith("\033[0m")


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
