"""The scope rule: no classification that forwards live applies to an unsafe method it did not
name, held when a map loads and again at the decision."""

from dataclasses import replace

import pytest

from irimi import servicemaps
from irimi.core.exchange import KINDS, LIVE_KINDS, Flag
from irimi.servicemaps import MapError
from irimi.servicemaps.classify import classify
from tests.support import builders
from tests.support.maps import GOOD_MAP, assert_refused, load_docs

# ------------------------------------------------------------ no write downgraded to a read


def test_unsafe_method_read_needs_live_because(tmp_path):
    """On a `style: rest` service, `kind: read` on any unsafe method is a downgraded write."""
    base = GOOD_MAP.replace(
        "    live_because: search persists nothing; Stripe's own spec calls it a POST read\n", ""
    )
    assert_refused(tmp_path, base, "downgrades a write")
    as_delete = base.replace(
        "      method: POST\n      path: /v1/search", "      method: DELETE\n      path: /v1/search"
    )
    # DELETE is caught one rule earlier now, by the live-kind rule that covers every live kind
    # rather than `read` alone - a stricter refusal of the same configuration.
    assert_refused(tmp_path, as_delete, "would be performed for real")


def test_an_empty_live_because_is_no_justification(tmp_path):
    doc = GOOD_MAP.replace(
        "    live_because: search persists nothing; Stripe's own spec calls it a POST read\n",
        "    live_because: '  '\n",
    )
    assert_refused(tmp_path, doc, "Add `live_because:")


def test_post_only_services_are_exempt(tmp_path):
    doc = """
version: 1
service: postonly
style: rpc
hosts: [postonly.example]
routes:
  - match:
      method: POST
      path: /api/things.list
    operation: things.list
    kind: read
    human: list things
"""
    sm = load_docs(tmp_path, doc).services[0]
    assert sm.routes[0].kind == "read"


@pytest.mark.parametrize("kind", ["write", "unknown"])
def test_default_kind_is_accepted_for_the_kinds_that_are_answered_locally(tmp_path, kind):
    sm = load_docs(tmp_path, GOOD_MAP.replace("style: rest", f"default_kind: {kind}")).services[0]
    assert sm.default_kind == kind


def test_default_kinds_is_derived_from_the_live_set(tmp_path):
    """The rule is `not forwarded live`, not a hand-kept list: a live kind added to LIVE_KINDS
    later must be refused as a default the day it is added, with no second edit here."""
    assert set(servicemaps.model.DEFAULT_KINDS) == set(KINDS) - set(LIVE_KINDS)
    assert set(servicemaps.model.DEFAULT_KINDS).isdisjoint(LIVE_KINDS)


def test_default_kind_defaults_to_none(tmp_path):
    assert load_docs(tmp_path).services[0].default_kind is None


def test_default_kind_may_not_be_read(tmp_path):
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("style: rest", "default_kind: read"),
        "may not be `read`",
    )


def test_default_kind_may_not_be_llm(tmp_path):
    assert_refused(
        tmp_path, GOOD_MAP.replace("style: rest", "default_kind: llm"), "may not be `llm`"
    )


def test_default_kind_may_not_be_telemetry(tmp_path):
    """`telemetry` is forwarded live, and most of these vendors serve their REST control plane
    from the same host as their intake, so a telemetry default would perform
    `DELETE /api/v1/dashboard/{id}` for real. The loader refuses it."""
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("style: rest", "default_kind: telemetry"),
        "may not be `telemetry`",
    )


@pytest.mark.parametrize("kind", LIVE_KINDS)
def test_every_live_kind_is_refused_as_a_default_with_its_own_reason(tmp_path, kind):
    """Each refusal explains itself: a generic message would not tell a map author what to write
    instead. The loop is over LIVE_KINDS so a new live kind fails here until it has a reason."""
    reason = servicemaps.loader.DEFAULT_KIND_REASONS[kind]
    assert_refused(tmp_path, GOOD_MAP.replace("style: rest", f"default_kind: {kind}"), reason)


def test_default_kind_must_be_a_kind(tmp_path):
    assert_refused(
        tmp_path, GOOD_MAP.replace("style: rest", "default_kind: maybe"), "must be one of"
    )


def test_an_override_cannot_set_default_kind(tmp_path):
    """`default_kind` decides what happens to every route the map does not list, so letting a
    user's overrides file set it would be a way to reclassify writes."""
    assert_refused(
        tmp_path,
        GOOD_MAP,
        "unknown override key(s) default_kind",
        override="service: demo\ndefault_kind: write\n",
    )


def test_a_loopback_webhook_route_does_not_license_an_off_machine_service_target():
    """THE SCOPE RULE, host scope. The rule is keyed on the host, not on (service, operation):
    keyed on the route, a loopback target on the *listed* webhook route would satisfy it while a
    service target sent every *unlisted* path off the machine. `/workflows/...` and
    `/triggers/...` are real Slack webhook forms whose path is the credential just as
    `/services/...` is."""
    with pytest.raises(MapError) as exc:
        servicemaps.load(
            cwd=None,
            maps_dir=servicemaps.loader.shipped_dir(),
            allow_target_hosts=frozenset({"stub.internal"}),
            targets=[
                # The route-level rule is satisfied: this one really is loopback.
                ("hooks.slack.com", "/services/{team}/{bot}/{token}", "http://127.0.0.1:3000"),
                # ... and this would delegate every path the map does not list.
                ("slack.com", "", "http://stub.internal:9000"),
            ],
        )
    assert "a target on this host may only be loopback" in str(exc.value)


# --------------------------------------------------- THE SCOPE RULE: live kinds and methods

_ANY_METHOD_TELEMETRY = """
version: 1
service: probe
hosts:
  - probe.example
routes:
  - match:
      path: /api/{v}/{thing}
    operation: probe.anything
    kind: telemetry
    human: send a probe event
"""


def test_a_live_kind_may_not_match_every_method(tmp_path):
    """THE SCOPE RULE, method scope. `kind` is written per route, but "forward this to the real
    service" is a decision about a verb: a `match:` with no `method:` means `*`, and `*` includes
    DELETE. This is the live `default_kind` hazard one scope down, and reachable by omission
    rather than by writing `"*"` on purpose - every shipped route happens to carry a `method:`,
    which is the only reason nothing shipped broken."""
    assert_refused(tmp_path, _ANY_METHOD_TELEMETRY, "may not match every method")
    # Naming the method it actually serves is all it takes.
    named = _ANY_METHOD_TELEMETRY.replace("      path:", "      method: POST\n      path:")
    assert load_docs(tmp_path, named).service_for("probe.example") is not None


@pytest.mark.parametrize("method", sorted(servicemaps.model.DESTRUCTIVE_METHODS))
@pytest.mark.parametrize("kind", ["read", "llm", "telemetry"])
def test_a_live_kind_on_a_destructive_method_must_justify_itself(tmp_path, method, kind):
    """Naming the verb is necessary but not sufficient: `DELETE api.datadoghq.com/api/v1/
    dashboard/{id}` classified `telemetry` is performed for real. The author has to say why it
    persists nothing, which is the same bar `kind: read` already had to clear."""
    doc = _ANY_METHOD_TELEMETRY.replace(
        "      path:", f"      method: {method}\n      path:"
    ).replace("kind: telemetry", f"kind: {kind}")
    assert_refused(tmp_path, doc, "would be performed for real")
    justified = doc.replace(
        "    human: send a probe event",
        "    human: send a probe event\n    live_because: why it is safe",
    )
    assert load_docs(tmp_path, justified).service_for("probe.example") is not None


def test_post_is_still_the_honest_verb_for_inference_and_intake(tmp_path):
    """The rule names the destructive verbs, not every RFC-unsafe one. POST is what an LLM
    completion and a telemetry batch are, so requiring a justification for it would mean every
    shipped `llm` and `telemetry` route carrying one."""
    doc = _ANY_METHOD_TELEMETRY.replace("      path:", "      method: POST\n      path:")
    assert load_docs(tmp_path, doc).service_for("probe.example") is not None


def test_a_live_kind_is_downgraded_when_the_route_did_not_name_the_method():
    """THE SCOPE RULE, decision half. The loader refuses this configuration, so nothing can reach
    the classifier this way today - which is the point. The rule is asked of the request in front
    of us, so a live kind arriving by a route the loader does not police is answered locally
    instead of performed on the real service.

    The map is built here rather than loaded, because the loader refuses to produce one.
    """
    from irimi.servicemaps import MapIndex, Route, ServiceMap

    route = Route(
        method="*",
        path="/api/{v}/{thing}",
        operation="probe.anything",
        kind="telemetry",
        human="send a probe event",
    )
    sm = ServiceMap(
        service="probe",
        hosts=frozenset({"probe.example"}),
        routes=(route,),
        source="built-in-code.yaml",
    )
    index = MapIndex((sm,))
    for method in ("DELETE", "PUT", "PATCH"):
        cls = classify(
            replace(
                builders.request(method=method), host="probe.example", path="/api/v1/dashboard"
            ),
            index,
        )
        assert cls.kind == "unknown", f"{method} was classified live"
        assert Flag.DOWNGRADED in cls.flags
    # POST is left alone: it is what a telemetry batch is, and the route still does not name it,
    # so it is downgraded too. Naming the verb is what makes it explicit.
    named = replace(route, method="DELETE")
    index = MapIndex((replace(sm, routes=(named,)),))
    cls = classify(
        replace(builders.request(method="DELETE"), host="probe.example", path="/api/v1/dashboard"),
        index,
    )
    assert cls.kind == "telemetry" and Flag.DOWNGRADED not in cls.flags
