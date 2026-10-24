"""The scope rule: no live-forwarding classification applies to an unsafe method it did not name.

It is held when a map loads and again at the decision.
"""

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
    # DELETE is caught one rule earlier, by the rule that covers every live kind, not `read` alone.
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
    """The allowed default kinds are the kinds not forwarded live, so a new live kind is refused."""
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
    """`telemetry` is refused as a default, since it forwards live.

    A vendor's intake host often serves its REST control plane too, so a dashboard DELETE would
    run for real."""
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("style: rest", "default_kind: telemetry"),
        "may not be `telemetry`",
    )


@pytest.mark.parametrize("kind", LIVE_KINDS)
def test_every_live_kind_is_refused_as_a_default_with_its_own_reason(tmp_path, kind):
    """Each live kind refused as a default gives its own reason, so the author knows what to do."""
    reason = servicemaps.loader.DEFAULT_KIND_REASONS[kind]
    assert_refused(tmp_path, GOOD_MAP.replace("style: rest", f"default_kind: {kind}"), reason)


def test_default_kind_must_be_a_kind(tmp_path):
    assert_refused(
        tmp_path, GOOD_MAP.replace("style: rest", "default_kind: maybe"), "must be one of"
    )


def test_an_override_cannot_set_default_kind(tmp_path):
    """An overrides file may not set `default_kind`, which would reclassify every unlisted write."""
    assert_refused(
        tmp_path,
        GOOD_MAP,
        "unknown override key(s) default_kind",
        override="service: demo\ndefault_kind: write\n",
    )


def test_a_loopback_webhook_route_does_not_license_an_off_machine_service_target():
    """The scope rule's host scope: a credential-path host's targets must all be loopback.

    Keyed on the route, a loopback target on the listed webhook route would let a service target
    send every unlisted path (`/workflows/...`, `/triggers/...`) off the machine."""
    with pytest.raises(MapError) as exc:
        servicemaps.load(
            cwd=None,
            maps_dir=servicemaps.loader.shipped_dir(),
            allow_target_hosts=frozenset({"stub.internal"}),
            targets=[
                # The route-level rule is satisfied: this one really is loopback.
                ("hooks.slack.com", "/services/{team}/{bot}/{token}", "http://127.0.0.1:3000"),
                # ... and this would send every path the map does not list to a target.
                ("slack.com", "", "http://stub.internal:9000"),
            ],
        )
    assert "a target on this host may only be loopback" in str(exc.value)


# --------------------------------------------------- the scope rule: live kinds and methods

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
    """The scope rule's method scope: a live route must name its method.

    A `match:` with no `method:` means `*`, which includes DELETE."""
    assert_refused(tmp_path, _ANY_METHOD_TELEMETRY, "may not match every method")
    # Naming the method it actually serves is all it takes.
    named = _ANY_METHOD_TELEMETRY.replace("      path:", "      method: POST\n      path:")
    assert load_docs(tmp_path, named).service_for("probe.example") is not None


@pytest.mark.parametrize("method", sorted(servicemaps.model.DESTRUCTIVE_METHODS))
@pytest.mark.parametrize("kind", ["read", "llm", "telemetry"])
def test_a_live_kind_on_a_destructive_method_must_justify_itself(tmp_path, method, kind):
    """A live kind on a destructive method needs a `live_because:`, as `kind: read` on one does.

    A `DELETE` of a dashboard classified `telemetry` is performed for real."""
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
    """A live kind on POST needs no justification: the rule names the destructive verbs only.

    POST is what an LLM completion and a telemetry batch are."""
    doc = _ANY_METHOD_TELEMETRY.replace("      path:", "      method: POST\n      path:")
    assert load_docs(tmp_path, doc).service_for("probe.example") is not None


def test_a_live_kind_is_downgraded_when_the_route_did_not_name_the_method():
    """The scope rule's decision half: an unnamed method on a live route is answered by irimi.

    The loader refuses this map, so it is built here; the classifier still holds the rule itself.
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
    # Naming the verb is what makes it explicit.
    named = replace(route, method="DELETE")
    index = MapIndex((replace(sm, routes=(named,)),))
    cls = classify(
        replace(builders.request(method="DELETE"), host="probe.example", path="/api/v1/dashboard"),
        index,
    )
    assert cls.kind == "telemetry" and Flag.DOWNGRADED not in cls.flags
