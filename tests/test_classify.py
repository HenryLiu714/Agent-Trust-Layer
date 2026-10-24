"""`servicemaps.classify`: a request's service, operation and kind, from the maps or the verb."""

from dataclasses import replace

import pytest

from irimi.servicemaps.classify import Classification, classify, unclassified
from tests.support import builders
from tests.support.maps import index_from


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS"])
def test_a_safe_method_is_a_read(method):
    cls = classify(builders.request(method=method))
    assert cls.kind == "read"
    assert cls.flags == ()
    assert cls.operation == f"{method} /v1/charges"
    assert cls.service == "api.stripe.com"


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_an_unsafe_method_is_unknown(method):
    cls = classify(builders.request(method=method))
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


def test_a_matched_route_decides_the_kind(tmp_path, monkeypatch):
    index = index_from(tmp_path, monkeypatch)
    cls = classify(builders.request(method="POST", path="/v1/refunds"), index)
    assert cls.service == "stripe"  # the map's service name, not the host
    assert cls.operation == "refunds.create"
    assert cls.kind == "write"
    assert cls.flags == ()


def test_classify_matched_carries_the_service_map_and_route(tmp_path, monkeypatch):
    index = index_from(tmp_path, monkeypatch)
    cls = classify(builders.request(method="POST", path="/v1/refunds"), index)
    assert cls.matched == index.route_for("api.stripe.com", "POST", "/v1/refunds")
    assert cls.matched is not None
    service_map, route = cls.matched
    assert service_map.service == "stripe"
    assert route.human == "refund {amount:money} on {charge}"


def test_classification_route_is_the_matched_route_or_none(tmp_path, monkeypatch):
    unmatched = Classification(service="x", operation="", kind="unknown", flags=(), matched=None)
    assert unmatched.route is None
    cls = classify(
        builders.request(method="POST", path="/v1/refunds"), index_from(tmp_path, monkeypatch)
    )
    assert cls.matched is not None
    assert cls.route is cls.matched[1]


def test_unclassified_is_unknown_under_its_own_host():
    req = builders.request(method="POST")
    cls = unclassified(req)
    assert cls.service == req.host
    assert cls.operation == ""
    assert cls.kind == "unknown"
    assert cls.flags == ("unclassified",)


def test_a_route_pattern_matches_any_one_segment(tmp_path, monkeypatch):
    cls = classify(builders.request(path="/v1/charges/ch_1"), index_from(tmp_path, monkeypatch))
    assert (cls.operation, cls.kind) == ("charges.retrieve", "read")


def test_classify_reads_a_post_only_services_reads_as_reads(tmp_path, monkeypatch):
    """A Slack read sent as POST classifies as a read; the verb rule alone would fake it."""
    req = replace(
        builders.request(method="POST"), host="slack.com", path="/api/conversations.history"
    )
    cls = classify(req, index_from(tmp_path, monkeypatch))
    assert cls.service == "slack"
    assert cls.operation == "conversations.history"
    assert cls.kind == "read"
    assert cls.flags == ()


def test_classify_falls_back_on_a_mapped_host_with_no_route(tmp_path, monkeypatch):
    index = index_from(tmp_path, monkeypatch)
    read = classify(builders.request(path="/v1/nope"), index)
    assert (read.service, read.operation, read.kind) == ("stripe", "GET /v1/nope", "read")
    assert read.matched is None
    write = classify(builders.request(method="POST", path="/v1/nope"), index)
    assert (write.service, write.operation, write.kind) == ("stripe", "POST /v1/nope", "unknown")
    assert write.flags == ("unclassified",)


def test_classify_unmapped_host_keeps_the_host_as_the_service(tmp_path, monkeypatch):
    index = index_from(tmp_path, monkeypatch)
    req = replace(builders.request(method="POST"), host="unmapped.example")
    cls = classify(req, index)
    assert (cls.service, cls.operation, cls.kind) == (
        "unmapped.example",
        "POST /v1/charges",
        "unknown",
    )
    assert cls.flags == ("unclassified",)
    assert classify(replace(req, method="GET"), index).kind == "read"


def test_classify_flags_a_declared_unknown_too(tmp_path, monkeypatch):
    """A route declared `kind: unknown` is flagged `unclassified`, as an unmapped route is."""
    req = replace(builders.request(method="POST"), host="demo.example", path="/v1/guess")
    cls = classify(req, index_from(tmp_path, monkeypatch, DEMO_MAP))
    assert (cls.operation, cls.kind, cls.flags) == ("things.guess", "unknown", ("unclassified",))


def test_classify_default_kind_beats_the_verb_rule(tmp_path, monkeypatch):
    """`default_kind` is per-service, so it also catches the GETs the verb rule would forward."""
    index = index_from(
        tmp_path,
        monkeypatch,
        DEMO_MAP.replace("service: demo", "service: demo\ndefault_kind: write"),
    )
    for method in ("GET", "POST"):
        cls = classify(
            replace(builders.request(method=method), host="demo.example", path="/v1/whatever"),
            index,
        )
        assert (cls.kind, cls.flags) == ("write", ()), method
    # An explicit route rule still wins over the service default.
    mapped = classify(
        replace(builders.request(method="POST"), host="demo.example", path="/v1/things"), index
    )
    assert (mapped.operation, mapped.kind) == ("things.create", "write")


def test_classify_with_an_empty_index_is_the_verb_rule():
    from irimi.servicemaps import MapIndex

    assert classify(builders.request(method="POST"), MapIndex()).kind == "unknown"
    assert classify(builders.request(), MapIndex()).kind == "read"


# ------------------------------------------------- the llm, telemetry and webhook maps


def _shipped(tmp_path, monkeypatch, method: str, host: str, path: str) -> Classification:
    """Classify one request against the real shipped maps."""
    return classify(
        replace(builders.request(method=method), host=host, path=path),
        index_from(tmp_path, monkeypatch),
    )


def test_openai_inference_routes_are_llm(tmp_path, monkeypatch):
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.openai.com", "/v1/chat/completions")
    assert (cls.service, cls.operation, cls.kind) == ("openai", "chat.completions.create", "llm")
    assert cls.flags == ()


def test_openai_models_listing_is_a_read_not_llm(tmp_path, monkeypatch):
    """A models listing is a read, not inference, so the read overlay applies to it."""
    cls = _shipped(tmp_path, monkeypatch, "GET", "api.openai.com", "/v1/models")
    assert (cls.operation, cls.kind) == ("models.list", "read")


def test_an_unlisted_openai_write_is_unknown_and_flagged(tmp_path, monkeypatch):
    """/v1/files is deliberately unmapped: the RFC fallback makes it unknown, so it is faked."""
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.openai.com", "/v1/files")
    assert (cls.service, cls.operation, cls.kind) == ("openai", "POST /v1/files", "unknown")
    assert cls.flags == ("unclassified",)


def test_an_unlisted_openai_get_falls_back_to_read(tmp_path, monkeypatch):
    """With no `default_kind`, an unlisted GET falls back to a live read.

    `default_kind: write` would turn harmless GETs into faked writes."""
    cls = _shipped(tmp_path, monkeypatch, "GET", "api.openai.com", "/v1/batches")
    assert (cls.service, cls.kind) == ("openai", "read")
    assert cls.flags == ()


def test_anthropic_messages_is_llm(tmp_path, monkeypatch):
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.anthropic.com", "/v1/messages")
    assert (cls.service, cls.operation, cls.kind) == ("anthropic", "messages.create", "llm")


def test_anthropic_count_tokens_is_its_own_route(tmp_path, monkeypatch):
    """`/v1/messages/count_tokens` matches its own route, not `/v1/messages`."""
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.anthropic.com", "/v1/messages/count_tokens")
    assert (cls.operation, cls.kind) == ("messages.count_tokens", "llm")


def test_an_unlisted_anthropic_write_is_unknown_and_flagged(tmp_path, monkeypatch):
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.anthropic.com", "/v1/messages/batches")
    assert cls.kind == "unknown"
    assert cls.flags == ("unclassified",)


def test_a_sentry_project_subdomain_classifies_through_the_wildcard(tmp_path, monkeypatch):
    """A wildcard in a shipped map really resolves a subdomain."""
    cls = _shipped(tmp_path, monkeypatch, "POST", "o0.ingest.sentry.io", "/api/7/envelope/")
    assert (cls.service, cls.operation, cls.kind) == ("sentry", "envelope.send", "telemetry")
    assert cls.flags == ()


def test_a_posthog_regional_host_classifies(tmp_path, monkeypatch):
    cls = _shipped(tmp_path, monkeypatch, "POST", "eu.i.posthog.com", "/batch/")
    assert (cls.service, cls.operation, cls.kind) == ("posthog", "batch.capture", "telemetry")


@pytest.mark.parametrize(
    "host",
    ["o4507.ingest.us.sentry.io", "o4507.ingest.de.sentry.io", "o0.ingest.sentry.io"],
)
def test_a_region_qualified_sentry_ingest_host_classifies(tmp_path, monkeypatch, host):
    """A region-qualified Sentry ingest host (`…ingest.us.sentry.io`) classifies as telemetry.

    `*.ingest.sentry.io` alone does not match it."""
    cls = _shipped(tmp_path, monkeypatch, "POST", host, "/api/4507/envelope/")
    assert (cls.service, cls.operation, cls.kind) == ("sentry", "envelope.send", "telemetry")
    assert cls.flags == ()


def test_the_sentry_web_app_is_not_claimed_by_the_ingest_map(tmp_path, monkeypatch):
    """The Sentry web app's hosts are not telemetry, so its control plane is never forwarded live.

    Subdomains are probed, not the bare host: a `*.x` pattern misses the bare `x` too, so probing
    `sentry.io` alone would pass under an over-broad pattern."""
    for host in ("sentry.io", "us.sentry.io", "acme.sentry.io"):
        cls = _shipped(tmp_path, monkeypatch, "DELETE", host, "/api/0/projects/acme/web/")
        assert (cls.service, cls.kind) == (host, "unknown"), host
    index = index_from(tmp_path, monkeypatch)
    assert index.service_for("us.sentry.io") is None
    # The regional ingest hosts the three patterns exist for are still claimed.
    for host in ("o1.ingest.sentry.io", "o1.ingest.us.sentry.io", "o1.ingest.de.sentry.io"):
        assert index.service_for(host) is not None, host


def test_one_posthog_pattern_covers_the_regional_ingest_hosts(tmp_path, monkeypatch):
    """`*.posthog.com` matches one or more leading labels, so it covers `*.i.posthog.com` too."""
    index = index_from(tmp_path, monkeypatch)
    assert index.by_suffix.get(".i.posthog.com") is None
    for host in ("eu.i.posthog.com", "us.i.posthog.com", "eu.posthog.com"):
        assert index.service_for(host) is not None, host
        assert index.service_for(host).service == "posthog"


@pytest.mark.parametrize(
    ("host", "path", "operation"),
    [
        # Datadog's sites are separate domains: EU1 is `datadoghq.eu`, US1-FED `ddog-gov.com`.
        ("http-intake.logs.datadoghq.eu", "/api/v2/logs", "logs.submit"),
        ("api.datadoghq.eu", "/api/v2/series", "series.submit.v2"),
        ("api.ddog-gov.com", "/api/v2/logs", "logs.submit"),
        # A regional subdomain of a listed host, matched by the existing pattern.
        ("api.us3.datadoghq.com", "/api/v2/logs", "logs.submit"),
        # Honeycomb's EU instance and LangSmith's EU tenant are each their own exact host.
        ("api.eu1.honeycomb.io", "/1/batch/ds", "batch.send"),
        ("eu.api.smith.langchain.com", "/runs/batch", "runs.batch"),
    ],
)
def test_a_regional_telemetry_host_classifies(tmp_path, monkeypatch, host, path, operation):
    """A regional telemetry host classifies as telemetry, so its intake is forwarded, not faked."""
    cls = _shipped(tmp_path, monkeypatch, "POST", host, path)
    assert (cls.operation, cls.kind, cls.flags) == (operation, "telemetry", ())


def test_the_regional_telemetry_patterns_do_not_claim_their_vendors_web_apps(tmp_path, monkeypatch):
    """Honeycomb's and LangSmith's web apps are not claimed by their telemetry maps.

    Their domains are shared with a web app and a control plane, so the maps list exact hosts.
    Subdomains are probed because a `*.x` pattern misses the bare `x` too."""
    index = index_from(tmp_path, monkeypatch)
    for host in ("ui.honeycomb.io", "www.honeycomb.io", "smith.langchain.com", "x.langchain.com"):
        assert index.service_for(host) is None, host
    # Datadog's control plane is inside its map, unlisted, so it is `unknown` and faked.
    cls = _shipped(tmp_path, monkeypatch, "DELETE", "api.datadoghq.eu", "/api/v1/dashboard/abc")
    assert (cls.service, cls.kind, cls.flags) == ("datadog", "unknown", ("unclassified",))


def test_an_unlisted_telemetry_write_is_faked_not_forwarded(tmp_path, monkeypatch):
    """An unlisted write to a telemetry host is faked and flagged, not forwarded live.

    These vendors serve their control plane from the intake host, so the telemetry maps set no
    `default_kind`."""
    cls = _shipped(tmp_path, monkeypatch, "DELETE", "api.datadoghq.com", "/api/v1/dashboard/abc")
    assert (cls.service, cls.kind) == ("datadog", "unknown")
    assert cls.flags == ("unclassified",)


@pytest.mark.parametrize(
    ("host", "path"),
    [
        ("api.datadoghq.com", "/api/v1/monitor"),
        ("api.honeycomb.io", "/1/triggers/ds/tid"),
        ("cloud.langfuse.com", "/api/public/datasets"),
        ("api.smith.langchain.com", "/runs/abc"),
    ],
)
def test_no_telemetry_control_plane_write_is_forwarded(tmp_path, monkeypatch, host, path):
    """Every vendor whose control plane shares a host with its intake, pinned at once."""
    cls = _shipped(tmp_path, monkeypatch, "POST", host, path)
    assert cls.kind == "unknown"


def test_a_listed_intake_path_is_still_telemetry(tmp_path, monkeypatch):
    """A listed intake route is telemetry, so real intake is forwarded live."""
    cls = _shipped(tmp_path, monkeypatch, "POST", "api.datadoghq.com", "/api/v2/logs")
    assert (cls.service, cls.kind, cls.flags) == ("datadog", "telemetry", ())


def test_the_slack_webhook_is_a_named_write(tmp_path, monkeypatch):
    cls = _shipped(tmp_path, monkeypatch, "POST", "hooks.slack.com", "/services/T000/B000/abc123")
    assert (cls.service, cls.operation, cls.kind) == ("slack", "incoming_webhook", "write")
    assert cls.flags == ()


def test_a_webhook_url_of_another_shape_still_falls_back_to_unknown(tmp_path, monkeypatch):
    """A webhook path of another shape is `unknown`: answered by irimi and flagged."""
    cls = _shipped(tmp_path, monkeypatch, "POST", "hooks.slack.com", "/services/T000/B000")
    assert (cls.service, cls.kind) == ("slack", "unknown")
    assert cls.flags == ("unclassified",)


def test_classify_reports_the_service_map_even_when_no_route_matched(tmp_path, monkeypatch):
    """`service_map` is set for an unlisted route on a mapped host, where `matched` is None.

    It tells an unmapped host from an unlisted route, so a service-level target reaches both."""
    index = index_from(tmp_path, monkeypatch)
    listed = classify(replace(builders.request(method="POST"), path="/v1/refunds"), index)
    assert listed.matched is not None
    assert listed.service_map is listed.matched[0]

    unlisted = classify(
        replace(builders.request(method="POST"), path="/v1/tax/calculations"), index
    )
    assert unlisted.matched is None
    assert unlisted.service_map is not None and unlisted.service_map.service == "stripe"

    unmapped = classify(
        replace(builders.request(method="POST"), host="nope.example", path="/x"), index
    )
    assert unmapped.matched is None and unmapped.service_map is None
