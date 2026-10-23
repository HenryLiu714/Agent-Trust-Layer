"""`Irimi-Run` attributes an exchange to a run and never travels past irimi."""

from irimi.core.exchange import Flag
from irimi.decide import targets
from tests.support.engine import (
    RUN_HEADERS,
    carries_run_header,
    engine_config,
    leaf_cert_for_loopback,
    read_via_proxy,
    start_engine,
    via_base_url,
    via_proxy,
    writes_in,
)
from tests.support.maps import targeted_index
from tests.support.upstreams import Upstream, serve, tls_context


def test_the_run_header_attributes_the_exchange_to_its_run(engine, upstream):
    eng, seen = engine
    via_proxy(
        eng.listen_port(),
        "GET",
        f"http://127.0.0.1:{upstream}/hello",
        extra_headers={"irimi-run": "abcd"},
    )
    assert seen[0].run_id == "abcd"


def test_a_live_read_is_attributed_and_its_run_header_is_not_forwarded(engine, upstream):
    eng, seen = engine
    status, _ = via_proxy(
        eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello", extra_headers=RUN_HEADERS
    )
    assert status == 200
    ((path, received),) = upstream.seen
    assert path == "/hello"
    assert not carries_run_header(received), "the run id reached the upstream"
    (ex,) = seen
    assert ex.run_id == "abc"
    assert not carries_run_header(ex.request.headers), "Exchange.run_id already holds it"


def test_a_base_url_read_is_attributed_and_its_run_header_is_not_forwarded(tmp_path, monkeypatch):
    cfg = engine_config(tmp_path, monkeypatch, reverse_hosts=frozenset({"127.0.0.1"}))
    up = serve(
        Upstream, ssl_context=tls_context(leaf_cert_for_loopback(cfg.ca, tmp_path / "leaf.pem"))
    )
    eng, seen, stop = start_engine(cfg, trust_upstream_ca=cfg.ca.cert)
    try:
        status, _ = via_base_url(
            eng.listen_port(), "GET", f"/127.0.0.1:{up}/hello", extra_headers=RUN_HEADERS
        )
    finally:
        stop()
        up.stop()
    assert status == 200
    ((path, received),) = up.seen
    assert path == "/hello"
    assert not carries_run_header(received), "the run id reached the upstream"
    (ex,) = seen
    assert (ex.door, ex.run_id) == ("reverse", "abc")
    assert not carries_run_header(ex.request.headers)


def test_a_delegated_write_is_attributed_and_its_target_never_sees_the_run_header(
    tmp_path, monkeypatch, target
):
    maps = targeted_index(
        tmp_path, monkeypatch, targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{target}/w")]
    )
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, _ = via_proxy(
            eng.listen_port(),
            "POST",
            "http://127.0.0.1/things",
            body=b"{}",
            extra_headers=RUN_HEADERS,
        )
    finally:
        stop()
    assert status == 200
    ((method, path, _, received),) = target.seen
    assert (method, path) == ("POST", "/w")
    assert not carries_run_header(received.items()), "the run id reached the answer target"
    (ex,) = seen
    assert (ex.answered_by, ex.run_id) == ("delegated", "abc")
    assert not carries_run_header(ex.request.headers)


def test_the_precondition_read_never_carries_the_run_header_and_both_exchanges_are_attributed(
    precondition_stub,
):
    proxy, stub, seen = precondition_stub
    status, _, _ = read_via_proxy(
        proxy,
        f"http://127.0.0.1:{stub}/v1/refunds",
        extra_headers={"content-type": "application/x-www-form-urlencoded", **RUN_HEADERS},
        method="POST",
        body=b"charge=ch_REAL1&amount=100",
    )
    assert status == 200
    assert stub.seen == [("GET", "/v1/charges/ch_REAL1")]
    (received,) = stub.heard
    assert not carries_run_header(received), "the run id reached the service on irimi's own read"
    (issued,) = [ex for ex in seen if ex.issued_by == "engine"]
    (write,) = writes_in(seen)
    assert (write.precondition.outcome, write.run_id, issued.run_id) == ("passed", "abc", "abc")
    assert not carries_run_header(write.request.headers)


def test_a_failed_decision_is_attributed_from_the_run_header_it_stripped(
    tmp_path, monkeypatch, upstream, refused_port
):
    """The 502 path attributes the run itself, from the value `request()` read before it stripped
    the header - the request it is handed no longer carries one."""
    maps = targeted_index(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "/things", f"http://127.0.0.1:{refused_port}/w")],
    )

    def boom(*args, **kwargs):
        raise RuntimeError("the decision exploded")

    monkeypatch.setattr(targets, "target_url", boom)
    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, _ = via_proxy(
            eng.listen_port(),
            "POST",
            f"http://127.0.0.1:{upstream}/things",
            body=b"{}",
            extra_headers=RUN_HEADERS,
        )
    finally:
        stop()
    assert status == 502
    (ex,) = seen
    assert Flag.DECISION_FAILED in ex.flags
    assert ex.run_id == "abc"
    assert not carries_run_header(ex.request.headers)


def test_every_other_header_is_forwarded_untouched(engine, upstream):
    """Only irimi's own header is stripped: a W3C trace context the agent propagates reaches the
    service exactly as sent."""
    eng, seen = engine
    traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    via_proxy(
        eng.listen_port(),
        "GET",
        f"http://127.0.0.1:{upstream}/hello",
        extra_headers={"traceparent": traceparent, **RUN_HEADERS},
    )
    ((_, received),) = upstream.seen
    assert [v for k, v in received if k.lower() == "traceparent"] == [traceparent]
    assert ("traceparent", traceparent) in seen[0].request.headers


# ------------------------------------ a run id names a directory; every exchange is timed


def test_an_invalid_run_header_is_stripped_and_the_exchange_keeps_the_engines_run(engine, upstream):
    """`../x` would name a directory outside the trace store, so it is no run id: the
    exchange keeps the engine's own. It is still stripped, like any other value."""
    eng, seen = engine
    status, _ = via_proxy(
        eng.listen_port(),
        "GET",
        f"http://127.0.0.1:{upstream}/hello",
        extra_headers={"Irimi-Run": "../x"},
    )
    assert status == 200
    ((_, received),) = upstream.seen
    assert not carries_run_header(received), "an invalid run id reached the upstream"
    (ex,) = seen
    assert ex.run_id == "t3st"
    assert not carries_run_header(ex.request.headers)
