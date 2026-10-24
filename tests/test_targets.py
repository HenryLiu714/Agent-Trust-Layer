"""`decide.targets`: which requests a target answers, the URL it gets, and what it never sees."""

from dataclasses import replace
from urllib.parse import urlsplit

import pytest

from irimi import servicemaps
from irimi.core.netaddr import Target
from irimi.decide import targets
from irimi.decide.targets import TargetRefused, refuse_self_target, target_url
from irimi.record.redact import is_credential_header
from irimi.servicemaps.classify import Classification, classify
from tests.support import builders
from tests.support.decisions import answer_shipped, answer_with, make_request, shipped_with_targets


def test_a_route_target_makes_the_answer_delegated_not_faked():
    index = shipped_with_targets(
        [("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000/refund")]
    )
    ans = answer_with(index, make_request("POST", path="/v1/refunds"))
    assert ans.answered_by == "delegated"
    assert ans.response is None  # the engine forwards; the policy does no I/O
    assert ans.forward_to == targets.ForwardTo(
        Target.parse("http://127.0.0.1:3000/refund"), forward_auth=False
    )


def test_self_is_the_default_target_and_is_exactly_the_old_behaviour():
    """With no target set, irimi fakes the write itself."""
    ans = answer_shipped(make_request("POST", path="/v1/refunds"))
    assert ans.answered_by == "fake-L1"
    assert ans.forward_to is None


def test_a_service_target_covers_writes_but_not_reads_without_target_reads():
    index = shipped_with_targets([("api.stripe.com", "", "http://127.0.0.1:3000")])
    write = answer_with(index, make_request("POST", path="/v1/refunds"))
    assert write.answered_by == "delegated"
    assert write.forward_to.url == "http://127.0.0.1:3000/v1/refunds"
    read = answer_with(index, make_request("GET", path="/v1/charges"))
    assert read.answered_by == "live" and read.forward_to is None


def test_target_reads_delegates_the_services_reads_too():
    index = shipped_with_targets(
        [("api.stripe.com", "", "http://127.0.0.1:3000")], target_reads=["api.stripe.com"]
    )
    read = answer_with(index, make_request("GET", path="/v1/charges"))
    assert read.answered_by == "delegated"
    assert read.forward_to.url == "http://127.0.0.1:3000/v1/charges"


def test_an_unlisted_route_on_a_targeted_service_is_delegated_too():
    """A service target answers its unlisted routes too, so one service is never split."""
    index = shipped_with_targets([("api.stripe.com", "", "http://127.0.0.1:3000")])
    ans = answer_with(index, make_request("POST", path="/v1/tax/calculations"))
    assert ans.answered_by == "delegated"
    assert ans.forward_to.url == "http://127.0.0.1:3000/v1/tax/calculations"


def test_llm_and_telemetry_are_never_delegated():
    """A service target never answers an llm request, so inference stays live."""
    index = shipped_with_targets(
        [("api.openai.com", "", "http://127.0.0.1:3000")], target_reads=["api.openai.com"]
    )
    llm = answer_with(
        index, make_request("POST", host="api.openai.com", path="/v1/chat/completions")
    )
    assert llm.answered_by == "live" and llm.forward_to is None


def test_resolve_returns_none_for_a_host_no_map_claims():
    ans = answer_shipped(make_request("POST", host="nowhere.example", path="/x"))
    assert ans.answered_by == "fake-L0" and ans.forward_to is None


def test_forward_auth_reaches_the_engine_through_the_answer():
    from dataclasses import replace as _replace

    index = shipped_with_targets([("api.stripe.com", "/v1/refunds", "http://127.0.0.1:3000/r")])
    stripe = index.service_for("api.stripe.com")
    routes = tuple(
        _replace(r, forward_auth=True) if r.target != servicemaps.SELF_TARGET else r
        for r in stripe.routes
    )
    index = servicemaps.MapIndex(
        tuple(_replace(sm, routes=routes) if sm is stripe else sm for sm in index.services)
    )
    ans = answer_with(index, make_request("POST", path="/v1/refunds"))
    assert ans.forward_to.forward_auth is True


def test_an_unlisted_read_on_a_targeted_service_is_not_delegated_without_target_reads():
    """An unlisted read on a targeted service stays live without `target_reads`.

    This is `resolve`'s unmatched branch, which the listed-route tests never reach."""
    index = shipped_with_targets([("api.stripe.com", "", "http://127.0.0.1:3000")])
    ans = answer_with(index, make_request("GET", path="/v1/tax/calculations"))  # GET, and unlisted
    assert ans.answered_by == "live"
    assert ans.forward_to is None


def test_an_unmatched_llm_or_telemetry_request_is_never_delegated():
    """An unlisted llm or telemetry request never goes to a service target.

    The classification is built by hand: the loader refuses the live `default_kind` that could give
    an unlisted route a live kind."""
    sm = servicemaps.loader.load_shipped()[0]
    sm = replace(sm, target=Target.parse("http://127.0.0.1:3000"), target_reads=True)
    for kind in ("llm", "telemetry", "read"):
        cls = Classification(
            service=sm.service,
            operation="",
            kind=kind,
            flags=(),
            matched=None,  # unlisted: this is the `elif` branch, not `target_for`
            service_map=sm,
        )
        forward = targets.resolve(make_request("POST", path="/anything"), cls)
        if kind == "read":
            assert forward is not None, "target_reads is what delegates a read"
        else:
            assert forward is None, f"an unmatched {kind} request was delegated"


def test_a_credential_path_host_is_never_delegated_off_the_machine():
    """A credential-path host is never sent to an off-machine target (the scope rule, at decision).

    The loader refuses this map, so it is built by hand to test `resolve` itself."""
    slack = next(sm for sm in servicemaps.loader.load_shipped() if sm.service == "slack")
    assert "hooks.slack.com" in slack.hosts
    off_machine = replace(slack, target=Target.parse("http://stub.internal:9000"))
    index = servicemaps.MapIndex((off_machine,))

    for path in ("/services/T0/B0/SECRET", "/workflows/T0/A0/SECRET/xyz", "/triggers/T0/1/abc"):
        request = make_request("POST", host="hooks.slack.com", path=path)
        forward = targets.resolve(request, classify(request, index))
        assert forward is None, f"{path} was delegated to {forward and forward.url}"

    # The same service's other hosts are not credential-path hosts, so they still delegate.
    request = make_request("POST", host="slack.com", path="/api/chat.postMessage")
    forward = targets.resolve(request, classify(request, index))
    assert forward is not None and forward.url.startswith("http://stub.internal:9000")

    # A loopback target on the credential host is the supported way to stub it.
    local = servicemaps.MapIndex((replace(slack, target=Target.parse("http://127.0.0.1:3000")),))
    request = make_request("POST", host="hooks.slack.com", path="/workflows/T0/A0/SECRET/xyz")
    forward = targets.resolve(request, classify(request, local))
    assert forward is not None and forward.url.startswith("http://127.0.0.1:3000")


# ------------------------------------------------------------------ target URLs


@pytest.mark.parametrize(
    ("target", "path", "query", "matched", "expected"),
    [
        # A bare origin keeps the request's own path.
        ("http://127.0.0.1:3000", "/v1/refunds", "", True, "http://127.0.0.1:3000/v1/refunds"),
        # A target carrying a path replaces the matched part, which is the whole path.
        ("http://127.0.0.1:3000/refund", "/v1/refunds", "", True, "http://127.0.0.1:3000/refund"),
        # Nothing matched, so the path is appended.
        (
            "http://127.0.0.1:3000/stub",
            "/v1/tax/calculations",
            "",
            False,
            "http://127.0.0.1:3000/stub/v1/tax/calculations",
        ),
        # The query is always kept.
        (
            "http://127.0.0.1:3000/refund",
            "/v1/refunds",
            "expand=charge",
            True,
            "http://127.0.0.1:3000/refund?expand=charge",
        ),
        ("http://localhost:3000", "/a", "b=1", True, "http://localhost:3000/a?b=1"),
    ],
)
def test_target_url_follows_proxy_pass_path_semantics(target, path, query, matched, expected):
    request = replace(builders.request(method="POST"), path=path, query=query)
    assert target_url(Target.parse(target), request, matched=matched).url == expected


def test_a_target_on_our_own_listener_is_refused():
    """A target on irimi's own port is refused, in every loopback spelling.

    Targets are loopback-only, so only the port tells a stub from irimi's own listener."""
    with pytest.raises(TargetRefused) as exc:
        refuse_self_target(Target.parse("http://127.0.0.1:4000"), 4000)
    assert "own listener" in str(exc.value)
    for spelling in ["http://localhost:4000", "http://127.1:4000", "http://0.0.0.0:4000"]:
        with pytest.raises(TargetRefused):
            refuse_self_target(Target.parse(spelling), 4000)


def test_a_target_on_another_port_is_allowed():
    assert refuse_self_target(Target.parse("http://127.0.0.1:3000"), 4000) is None  # type: ignore[func-returns-value]
    # Port 80, not ours.
    assert refuse_self_target(Target.parse("http://127.0.0.1"), 4000) is None  # type: ignore[func-returns-value]


def test_a_target_naming_the_address_irimi_was_reached_at_is_refused(monkeypatch):
    """A target at the address irimi was reached at, or a name for it, is irimi's own listener.

    Bound wider than loopback, that address is irimi too; only `--allow-remote-target` allows it."""
    import socket

    real = socket.getaddrinfo

    def resolving(host, *args, **kwargs):
        return real("10.0.0.5" if host == "irimi.test" else host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", resolving)
    for target in ["http://10.0.0.5:4000", "http://irimi.test:4000/w"]:
        for local in ["10.0.0.5", "::ffff:10.0.0.5"]:
            with pytest.raises(TargetRefused, match="own listener"):
                refuse_self_target(Target.parse(target), 4000, local)
        # Reached on loopback, it is another machine's.
        assert refuse_self_target(Target.parse(target), 4000) is None  # type: ignore[func-returns-value]
        assert refuse_self_target(Target.parse(target), 4000, "10.0.0.6") is None  # type: ignore[func-returns-value]
    assert refuse_self_target(Target.parse("http://10.0.0.5:3000"), 4000, "10.0.0.5") is None  # type: ignore[func-returns-value]


def test_a_hash_in_the_request_path_survives_the_target_url():
    """A `#` in the request path or query is escaped in the target URL, not read as a fragment.

    The engine splits the URL again; unescaped, `/unlisted#x?a=1` would lose its query."""
    req = replace(builders.request(method="POST"), path="/unlisted#x", query="a=1")
    url = target_url(Target.parse("http://127.0.0.1:3000"), req, matched=False).url
    parts = urlsplit(url)
    assert parts.path == "/unlisted%23x"
    assert parts.query == "a=1"
    assert parts.fragment == ""
    # A `#` in the query is the same hazard one character later.
    req = replace(builders.request(method="POST"), path="/p", query="a=1#b")
    sent_to = target_url(Target.parse("http://127.0.0.1:3000"), req, matched=False)
    assert urlsplit(sent_to.url).query == "a=1%23b"


# ------------------------------------------- credential headers a target must not see


@pytest.mark.parametrize(
    "name",
    [
        "Authorization",
        "proxy-authorization",
        "Cookie",
        "x-api-key",
        "DD-API-KEY",
        "dd-application-key",
        "X-Honeycomb-Team",
        "x-goog-api-key",
        "x-amz-security-token",
        "X-Slack-Signature",
        "x-acme-apikey",  # a vendor spelling nobody has written down yet
    ],
)
def test_a_credential_header_is_recognised_whoever_spells_it(name):
    assert is_credential_header(name)


@pytest.mark.parametrize(
    "name", ["content-type", "content-length", "host", "user-agent", "accept", "idempotency-key"]
)
def test_an_ordinary_header_is_not_a_credential(name):
    """An ordinary header an SDK needs survives the wide credential rule."""
    assert not is_credential_header(name)
