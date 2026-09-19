from dataclasses import replace

import pytest

from irimi.exchange import Request, Response
from irimi.pipeline import (
    Classification,
    ReverseDoorRefused,
    annotate,
    attribute_run,
    classify,
    detect_door,
    is_loopback,
    is_self_host,
    parse,
    respond,
    rewrite_reverse,
)


def _parse(**overrides) -> Request:
    args = {
        "method": "GET",
        "scheme": "https",
        "host": "api.stripe.com",
        "port": 443,
        "path_and_query": "/v1/charges",
        "headers": (),
        "body": b"",
    }
    args.update(overrides)
    return parse(**args)


def _req(
    method: str = "GET",
    headers: tuple[tuple[str, str], ...] = (),
    path: str = "/v1/charges",
) -> Request:
    return Request(
        method=method,
        scheme="https",
        host="api.stripe.com",
        port=443,
        path=path,
        query="",
        headers=headers,
        body=b"",
    )


ALLOWED = frozenset({"api.stripe.com", "127.0.0.1"})


def _door_req(
    path: str, host: str = "localhost", port: int = 4000, query: str = "", method: str = "GET"
) -> Request:
    """What the reverse door sees: plain http, addressed to the listener, host header set."""
    return Request(
        method=method,
        scheme="http",
        host=host,
        port=port,
        path=path,
        query=query,
        headers=(("host", f"{host}:{port}"), ("accept", "*/*")),
        body=b"",
    )


def test_parse_upper_cases_method():
    assert _parse(method="get").method == "GET"


def test_parse_lower_cases_host():
    assert _parse(host="API.Stripe.COM").host == "api.stripe.com"


def test_parse_splits_query():
    req = _parse(path_and_query="/v1/charges?limit=1")
    assert req.path == "/v1/charges"
    assert req.query == "limit=1"


def test_parse_without_query():
    req = _parse(path_and_query="/v1/charges")
    assert req.path == "/v1/charges"
    assert req.query == ""


def test_parse_empty_path_becomes_root():
    assert _parse(path_and_query="").path == "/"


def test_parse_lower_cases_header_names_only():
    req = _parse(headers=[("Content-Type", "Application/JSON"), ("Irimi-Run", "AbC")])
    assert req.headers == (("content-type", "Application/JSON"), ("irimi-run", "AbC"))


def test_parse_none_body_becomes_empty_bytes():
    assert _parse(body=None).body == b""


def test_parse_keeps_body_and_port():
    req = _parse(body=b'{"a":1}', port=8443)
    assert req.body == b'{"a":1}'
    assert req.port == 8443


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS"])
def test_classify_safe_methods_are_reads(method):
    cls = classify(_req(method))
    assert cls.kind == "read"
    assert cls.flags == ()
    assert cls.operation == f"{method} /v1/charges"
    assert cls.service == "api.stripe.com"


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_classify_other_methods_are_unknown(method):
    cls = classify(_req(method))
    assert cls.kind == "unknown"
    assert cls.flags == ("unclassified",)
    assert cls.operation == f"{method} /v1/charges"
    assert cls.service == "api.stripe.com"


# ------------------------------------------------------------ classification against the maps

DEMO_MAP = """
version: 1
service: demo
hosts:
  - demo.example
routes:
  - match:
      method: POST
      path: /v1/things
    operation: things.create
    kind: write
  - match:
      method: POST
      path: /v1/guess
    operation: things.guess
    kind: unknown
"""


def _index(tmp_path, monkeypatch, *docs: str):
    """A MapIndex from `docs`, or from the shipped maps when none are given.

    Both override locations are pointed at empty places: the loader reads `./irimi.maps.yaml` and
    then `$IRIMI_HOME/maps.yaml`, so a developer with a real overrides file would otherwise see a
    different index, or a MapError.
    """
    from irimi import paths, servicemap

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "ambient-home"))
    if not docs:
        return servicemap.load(cwd=tmp_path, maps_dir=servicemap.shipped_dir())
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    for i, doc in enumerate(docs):
        (maps_dir / f"{i}.yaml").write_text(doc)
    return servicemap.load(cwd=tmp_path, maps_dir=maps_dir)


def test_classify_uses_the_route_rule(tmp_path, monkeypatch):
    index = _index(tmp_path, monkeypatch)
    cls = classify(_req("POST", path="/v1/refunds"), index)
    assert cls.service == "stripe"  # the map's service name, not the host
    assert cls.operation == "refunds.create"
    assert cls.kind == "write"
    assert cls.flags == ()


def test_classify_matched_carries_the_service_map_and_route(tmp_path, monkeypatch):
    index = _index(tmp_path, monkeypatch)
    cls = classify(_req("POST", path="/v1/refunds"), index)
    assert cls.matched == index.route_for("api.stripe.com", "POST", "/v1/refunds")
    service_map, route = cls.matched
    assert service_map.service == "stripe"
    assert route.human == "refund {amount} on {charge}"


def test_classify_matches_a_pattern_segment(tmp_path, monkeypatch):
    cls = classify(_req(path="/v1/charges/ch_1"), _index(tmp_path, monkeypatch))
    assert (cls.operation, cls.kind) == ("charges.retrieve", "read")


def test_classify_reads_a_post_only_services_reads_as_reads(tmp_path, monkeypatch):
    """The whole point of the maps: Slack sends every call as POST, so the verb rule alone would
    call this a write and fake it."""
    req = replace(_req("POST"), host="slack.com", path="/api/conversations.history")
    cls = classify(req, _index(tmp_path, monkeypatch))
    assert cls.service == "slack"
    assert cls.operation == "conversations.history"
    assert cls.kind == "read"
    assert cls.flags == ()


def test_classify_falls_back_on_a_mapped_host_with_no_route(tmp_path, monkeypatch):
    index = _index(tmp_path, monkeypatch)
    read = classify(_req(path="/v1/nope"), index)
    assert (read.service, read.operation, read.kind) == ("stripe", "GET /v1/nope", "read")
    assert read.matched is None
    write = classify(_req("POST", path="/v1/nope"), index)
    assert (write.service, write.operation, write.kind) == ("stripe", "POST /v1/nope", "unknown")
    assert write.flags == ("unclassified",)


def test_classify_unmapped_host_keeps_the_host_as_the_service(tmp_path, monkeypatch):
    index = _index(tmp_path, monkeypatch)
    req = replace(_req("POST"), host="unmapped.example")
    cls = classify(req, index)
    assert (cls.service, cls.operation, cls.kind) == (
        "unmapped.example",
        "POST /v1/charges",
        "unknown",
    )
    assert cls.flags == ("unclassified",)
    assert classify(replace(req, method="GET"), index).kind == "read"


def test_classify_flags_a_declared_unknown_too(tmp_path, monkeypatch):
    """A map that says `kind: unknown` has looked and does not know, which the agent's operator
    needs to see for the same reason an unmapped route does."""
    req = replace(_req("POST"), host="demo.example", path="/v1/guess")
    cls = classify(req, _index(tmp_path, monkeypatch, DEMO_MAP))
    assert (cls.operation, cls.kind, cls.flags) == ("things.guess", "unknown", ("unclassified",))


def test_classify_default_kind_beats_the_verb_rule(tmp_path, monkeypatch):
    """`default_kind` is per-service, so it also catches the GETs the verb rule would forward."""
    index = _index(
        tmp_path,
        monkeypatch,
        DEMO_MAP.replace("service: demo", "service: demo\ndefault_kind: write"),
    )
    for method in ("GET", "POST"):
        cls = classify(replace(_req(method), host="demo.example", path="/v1/whatever"), index)
        assert (cls.kind, cls.flags) == ("write", ()), method
    # An explicit route rule still wins over the service default.
    mapped = classify(replace(_req("POST"), host="demo.example", path="/v1/things"), index)
    assert (mapped.operation, mapped.kind) == ("things.create", "write")


def test_classify_with_an_empty_index_is_the_verb_rule():
    from irimi.servicemap import MapIndex

    assert classify(_req("POST"), MapIndex()).kind == "unknown"
    assert classify(_req(), MapIndex()).kind == "read"


def test_attribute_run_without_header_uses_default():
    assert attribute_run(_req(), "dflt") == "dflt"


def test_attribute_run_uses_header():
    assert attribute_run(_req(headers=(("irimi-run", "7f3a"),)), "dflt") == "7f3a"


def test_attribute_run_blank_header_uses_default():
    assert attribute_run(_req(headers=(("irimi-run", "   "),)), "dflt") == "dflt"


@pytest.mark.parametrize("answered_by", ["live", "fake-L0"])
def test_annotate_copies_fields(answered_by):
    req = _req()
    resp = Response(status=200, headers=(), body=b"{}")
    cls = Classification("api.stripe.com", "GET /v1/charges", "read", ())
    ex = annotate(req, resp, cls, answered_by, "7f3a")
    assert ex.request is req
    assert ex.response is resp
    assert ex.service == "api.stripe.com"
    assert ex.operation == "GET /v1/charges"
    assert ex.kind == "read"
    assert ex.answered_by == answered_by
    assert ex.validation == "unvalidated"
    assert ex.run_id == "7f3a"
    assert ex.flags == ()


def test_annotate_appends_extra_flags():
    cls = Classification("api.stripe.com", "POST /v1/charges", "unknown", ("unclassified",))
    ex = annotate(_req("POST"), None, cls, "live", "7f3a", extra_flags=("upstream-error",))
    assert ex.flags == ("unclassified", "upstream-error")
    assert ex.response is None


def test_respond_returns_exchange_response():
    resp = Response(status=200, headers=(), body=b"ok")
    cls = classify(_req())
    ex = annotate(_req(), resp, cls, "live", "7f3a")
    assert respond(ex) is resp


def test_respond_returns_none_without_response():
    ex = annotate(_req(), None, classify(_req()), "live", "7f3a")
    assert respond(ex) is None


@pytest.mark.parametrize(
    "host",
    [
        "localhost",
        "localhost.",
        "127.0.0.1",
        "127.0.0.2",
        "127.1",
        "::1",
        "::ffff:127.0.0.1",
        "0.0.0.0",
        "::",
    ],
)
def test_detect_door_reverse_for_loopback_hosts(host):
    assert detect_door(_door_req("/api.stripe.com/v1", host=host), 4000) == "reverse"


def test_detect_door_resolves_unknown_name_on_own_port(monkeypatch):
    import socket

    from irimi import pipeline

    def fake_getaddrinfo(host, port, **kwargs):
        if host == "self.test":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 0))]
        raise socket.gaierror("no such host")

    monkeypatch.setattr(pipeline.socket, "getaddrinfo", fake_getaddrinfo)
    assert detect_door(_door_req("/api.stripe.com/v1", host="self.test"), 4000) == "reverse"
    assert detect_door(_door_req("/api.stripe.com/v1", host="nowhere.test"), 4000) == "forward"
    # Other ports never resolve anything.
    assert detect_door(_door_req("/x", host="self.test", port=4001), 4000) == "forward"


def test_is_self_host():
    for host in ["localhost", "127.0.0.1", "127.255.0.1", "127.1", "::1", "::ffff:127.0.0.1"]:
        assert is_self_host(host), host
    assert is_self_host("0.0.0.0")
    assert is_self_host("::")
    for host in ["10.0.0.1", "api.stripe.com", "", "nonsense", "::ffff:10.0.0.1"]:
        assert not is_self_host(host), host


def test_detect_door_forward_for_other_port():
    assert detect_door(_door_req("/api.stripe.com/v1", port=4001), 4000) == "forward"


def test_detect_door_forward_for_real_host():
    assert detect_door(_req(), 4000) == "forward"


def test_is_loopback():
    assert is_loopback("127.0.0.1")
    assert is_loopback("127.0.0.2")
    assert is_loopback("::1")
    assert not is_loopback("10.0.0.1")
    assert not is_loopback("")
    assert not is_loopback("nonsense")


def test_rewrite_reverse_basic():
    req = rewrite_reverse(_door_req("/api.stripe.com/v1/charges", query="limit=1"), ALLOWED)
    assert req.scheme == "https"
    assert req.host == "api.stripe.com"
    assert req.port == 443
    assert req.path == "/v1/charges"
    assert req.query == "limit=1"
    assert req.method == "GET"
    assert req.body == b""
    assert req.url == "https://api.stripe.com/v1/charges?limit=1"


def test_rewrite_reverse_rewrites_host_header():
    req = rewrite_reverse(_door_req("/api.stripe.com/v1/charges", query="limit=1"), ALLOWED)
    assert ("host", "api.stripe.com") in req.headers
    assert ("accept", "*/*") in req.headers
    assert not any("localhost" in value for _, value in req.headers)


def test_rewrite_reverse_adds_missing_host_header():
    bare = replace(_door_req("/api.stripe.com/v1"), headers=(("accept", "*/*"),))
    assert ("host", "api.stripe.com") in rewrite_reverse(bare, ALLOWED).headers
    bare = replace(_door_req("/127.0.0.1:8443/v1"), headers=())
    assert rewrite_reverse(bare, ALLOWED).headers == (("host", "127.0.0.1:8443"),)


def test_rewrite_reverse_refuses_ipv6_literal():
    with pytest.raises(ReverseDoorRefused, match="IPv6"):
        rewrite_reverse(_door_req("/[::1]:8443/x"), ALLOWED | {"::1"})


def test_rewrite_reverse_lower_cases_host():
    assert rewrite_reverse(_door_req("/API.Stripe.COM/v1"), ALLOWED).host == "api.stripe.com"


def test_rewrite_reverse_explicit_port():
    req = rewrite_reverse(_door_req("/127.0.0.1:8443/hello"), ALLOWED)
    assert req.host == "127.0.0.1"
    assert req.port == 8443
    assert req.path == "/hello"
    assert ("host", "127.0.0.1:8443") in req.headers


@pytest.mark.parametrize("path", ["/api.stripe.com", "/api.stripe.com/"])
def test_rewrite_reverse_bare_host_becomes_root(path):
    assert rewrite_reverse(_door_req(path), ALLOWED).path == "/"


def test_rewrite_reverse_keeps_deeper_path():
    req = rewrite_reverse(_door_req("/api.stripe.com/v1/charges/ch_1/refunds"), ALLOWED)
    assert req.path == "/v1/charges/ch_1/refunds"


def test_rewrite_reverse_keeps_method_and_body():
    door = _door_req("/api.stripe.com/v1/refunds", method="POST")
    req = rewrite_reverse(replace(door, body=b"charge=ch_1"), ALLOWED)
    assert req.method == "POST"
    assert req.body == b"charge=ch_1"


def test_rewrite_reverse_refuses_unlisted_host():
    with pytest.raises(ReverseDoorRefused) as exc:
        rewrite_reverse(_door_req("/evil.example/x"), ALLOWED)
    assert "evil.example" in str(exc.value)
    assert "--allow-host" in str(exc.value)


def test_rewrite_reverse_refuses_missing_host():
    with pytest.raises(ReverseDoorRefused) as exc:
        rewrite_reverse(_door_req("/"), ALLOWED)
    assert "/<upstream-host>/<path>" in str(exc.value)


@pytest.mark.parametrize(
    "path", ["/api.stripe.com:abc/v1", "/api.stripe.com:0/v1", "/api.stripe.com:70000/v1"]
)
def test_rewrite_reverse_refuses_bad_port(path):
    with pytest.raises(ReverseDoorRefused) as exc:
        rewrite_reverse(_door_req(path), ALLOWED)
    assert "bad port" in str(exc.value)


def test_rewrite_reverse_empty_allowlist_refuses_everything():
    with pytest.raises(ReverseDoorRefused):
        rewrite_reverse(_door_req("/api.stripe.com/v1"), frozenset())


def test_annotate_door_defaults_to_forward():
    assert annotate(_req(), None, classify(_req()), "live", "7f3a").door == "forward"


def test_annotate_records_reverse_door():
    ex = annotate(_req(), None, classify(_req()), "live", "7f3a", door="reverse")
    assert ex.door == "reverse"


# ------------------------------------------------- the llm, telemetry and webhook maps (#8/#9/#10)


def _shipped(tmp_path, monkeypatch, method: str, host: str, path: str) -> Classification:
    """Classify one request against the real shipped maps."""
    return classify(replace(_req(method), host=host, path=path), _index(tmp_path, monkeypatch))


def test_openai_inference_routes_are_llm(tmp_path, monkeypatch):
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.openai.com", "/v1/chat/completions")
    assert (cls.service, cls.operation, cls.kind) == ("openai", "chat.completions.create", "llm")
    assert cls.flags == ()


def test_openai_models_is_llm_on_a_safe_method(tmp_path, monkeypatch):
    """`kind: llm` on a GET is legal: `_check_route_rules` only constrains `kind: read`."""
    cls = _shipped(tmp_path, monkeypatch, "GET", "api.openai.com", "/v1/models")
    assert (cls.operation, cls.kind) == ("models.list", "llm")


def test_an_unlisted_openai_write_is_unknown_and_flagged(tmp_path, monkeypatch):
    """/v1/files is deliberately unmapped: the RFC fallback makes it unknown, so it is faked."""
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.openai.com", "/v1/files")
    assert (cls.service, cls.operation, cls.kind) == ("openai", "POST /v1/files", "unknown")
    assert cls.flags == ("unclassified",)


def test_an_unlisted_openai_get_falls_back_to_read(tmp_path, monkeypatch):
    """Intended: with no `default_kind`, the RFC fallback forwards an unlisted GET live. The maps
    set no default here because `default_kind: write` would turn harmless GETs into fake writes."""
    cls = _shipped(tmp_path, monkeypatch, "GET", "api.openai.com", "/v1/batches")
    assert (cls.service, cls.kind) == ("openai", "read")
    assert cls.flags == ()


def test_anthropic_messages_is_llm(tmp_path, monkeypatch):
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.anthropic.com", "/v1/messages")
    assert (cls.service, cls.operation, cls.kind) == ("anthropic", "messages.create", "llm")


def test_anthropic_count_tokens_is_its_own_route(tmp_path, monkeypatch):
    """Different segment counts, so `/v1/messages` cannot swallow it. The operation proves which
    route won."""
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.anthropic.com", "/v1/messages/count_tokens")
    assert (cls.operation, cls.kind) == ("messages.count_tokens", "llm")


def test_an_unlisted_anthropic_write_is_unknown_and_flagged(tmp_path, monkeypatch):
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.anthropic.com", "/v1/messages/batches")
    assert cls.kind == "unknown"
    assert cls.flags == ("unclassified",)


def test_a_sentry_project_subdomain_classifies_through_the_wildcard(tmp_path, monkeypatch):
    """The test issue #9 asks for: a wildcard in a shipped map really resolves a subdomain."""
    cls = _shipped(tmp_path, monkeypatch, "POST", "o0.ingest.sentry.io", "/api/7/envelope/")
    assert (cls.service, cls.operation, cls.kind) == ("sentry", "envelope.send", "telemetry")
    assert cls.flags == ()


def test_a_posthog_regional_host_classifies(tmp_path, monkeypatch):
    cls = _shipped(tmp_path, monkeypatch, "POST", "eu.i.posthog.com", "/batch/")
    assert (cls.service, cls.operation, cls.kind) == ("posthog", "batch.capture", "telemetry")


def test_an_unlisted_telemetry_path_is_telemetry_by_default_kind(tmp_path, monkeypatch):
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.datadoghq.com", "/some/unlisted/path")
    assert (cls.service, cls.kind) == ("datadog", "telemetry")
    assert cls.flags == ()


def test_default_kind_telemetry_beats_the_safe_method_fallback(tmp_path, monkeypatch):
    """Deliberate: `default_kind` is checked before the RFC fallback, so an unlisted GET on a
    telemetry host is telemetry, not a read. Both forward live; the difference is the count and
    that telemetry is never recorded."""
    cls = _shipped(tmp_path, monkeypatch, "GET", "api.honeycomb.io", "/1/auth")
    assert (cls.service, cls.kind) == ("honeycomb", "telemetry")


def test_a_telemetry_wildcard_is_not_a_reverse_door_host(tmp_path, monkeypatch):
    """Decision: a wildcard classifies through the forward door only. `--allow-host` is the way in
    through the reverse door, and `rewrite_reverse` refuses the host until someone passes it."""
    index = _index(tmp_path, monkeypatch)
    assert "*.ingest.sentry.io" not in index.hosts
    assert "o0.ingest.sentry.io" not in index.hosts
    with pytest.raises(ReverseDoorRefused):
        rewrite_reverse(_door_req("/o0.ingest.sentry.io/api/7/envelope/"), index.hosts)


def test_the_slack_webhook_is_a_named_write(tmp_path, monkeypatch):
    cls = _shipped(tmp_path, monkeypatch, "POST", "hooks.slack.com", "/services/T000/B000/abc123")
    assert (cls.service, cls.operation, cls.kind) == ("slack", "incoming_webhook", "write")
    assert cls.flags == ()


def test_a_webhook_url_of_another_shape_still_falls_back_to_unknown(tmp_path, monkeypatch):
    """No `*` path wildcard, so a two-segment webhook path misses the route and the fallback
    catches it: still answered locally, and flagged so the operator sees the guess."""
    cls = _shipped(tmp_path, monkeypatch, "POST", "hooks.slack.com", "/services/T000/B000")
    assert (cls.service, cls.kind) == ("slack", "unknown")
    assert cls.flags == ("unclassified",)
