"""Service maps: the shipped ones, the loader's schema, the overrides file and `--target`."""

import os
from pathlib import Path

import pytest

from irimi import servicemaps, services
from irimi.config import paths
from irimi.core.exchange import LIVE_KINDS, SAFE_METHODS
from irimi.servicemaps import IdSpec, MapError, MapIndex, Route, ServiceMap
from tests.support.maps import GOOD_MAP, assert_refused, load_docs, write_maps

# ------------------------------------------------------------------- the shipped maps round-trip


def test_the_shipped_maps_load():
    index = servicemaps.load(cwd=None, maps_dir=servicemaps.loader.shipped_dir())
    assert {sm.service for sm in index.services} == {
        "anthropic",
        "datadog",
        "honeycomb",
        "langfuse",
        "langsmith",
        "openai",
        "posthog",
        "sentry",
        "slack",
        "stripe",
    }


def test_shipped_stripe_map_is_complete():
    index = servicemaps.load(maps_dir=servicemaps.loader.shipped_dir())
    stripe = index.service_for("api.stripe.com")
    assert stripe is not None
    assert stripe.style == "rest"
    assert stripe.target is None
    assert stripe.target_reads is False
    assert stripe.hosts == frozenset(
        {"api.stripe.com", "connect.stripe.com", "files.stripe.com", "meter-events.stripe.com"}
    )
    assert len(stripe.routes) == 10
    refund = servicemaps.model.match_route(stripe, "POST", "/v1/refunds")
    assert refund is not None
    assert (refund.operation, refund.kind) == ("refunds.create", "write")
    assert refund.human == "refund {amount:money} on {charge}"
    assert refund.ids == {"id": IdSpec(prefix="re_"), "balance_transaction": IdSpec(prefix="txn_")}
    assert refund.compare_ignores == ()
    assert refund.fixture == "refund"
    assert refund.fires == ("refund.created", "charge.refunded")


def test_shipped_slack_map_is_rpc_style_and_owns_the_webhook_host():
    index = servicemaps.load(maps_dir=servicemaps.loader.shipped_dir())
    slack = index.service_for("slack.com")
    assert slack is not None
    assert slack.style == "rpc"
    assert slack.credential_path_hosts == frozenset({"hooks.slack.com"})
    # One service, not two: `_check_unique` forbids the host appearing in both, and a second
    # service would split the Slack summary in two.
    assert index.service_for("hooks.slack.com") is slack
    post = servicemaps.model.match_route(slack, "POST", "/api/chat.postMessage")
    history = servicemaps.model.match_route(slack, "POST", "/api/conversations.history")
    assert post is not None and post.kind == "write"
    assert post.human == 'post to #{channel}: "{text}"'
    assert history is not None and history.kind == "read"


def test_no_shipped_route_downgrades_a_write_to_a_read():
    """The classification invariant: a declared write is never a read without `live_because:`.

    The loader refuses one for a `style: rest` service; this holds the property itself. A
    `style: rpc` service is exempt: its SDKs POST every call.
    """
    index = servicemaps.load(maps_dir=servicemaps.loader.shipped_dir())
    rest = [sm for sm in index.services if sm.style == "rest"]
    assert rest, "the shipped maps should still contain a `style: rest` service"
    for sm in rest:
        for route in sm.routes:
            if route.kind == "read" and route.method not in SAFE_METHODS:
                assert route.live_because, f"{sm.service} {route.operation}"


def test_slack_read_templates_still_name_the_channel():
    """The Slack reads that name a channel keep `#{channel}` in their template.

    An unquoted `#` starts a YAML comment, so `look up #{channel}` would load as `look up`."""
    index = servicemaps.load(maps_dir=servicemaps.loader.shipped_dir())
    slack = index.service_for("slack.com")
    assert slack is not None
    humans = {route.operation: route.human for route in slack.routes}
    assert humans["conversations.history"] == "read the #{channel} history"
    assert humans["conversations.replies"] == "read a thread in #{channel}"
    assert humans["conversations.info"] == "look up #{channel}"


def test_no_shipped_human_template_is_eaten_by_a_yaml_comment():
    """The class of bug the test above catches one instance of, checked against the raw files."""
    offenders = []
    for path in sorted(
        p for p in servicemaps.loader.shipped_dir().iterdir() if p.suffix == ".yaml"
    ):
        for number, line in enumerate(path.read_text().splitlines(), 1):
            stripped = line.strip()
            if not stripped.startswith("human:"):
                continue
            value = stripped[len("human:") :].strip()
            if "#" in value and value[:1] not in ("'", '"'):
                offenders.append(f"{path.name}:{number}: {stripped}")
    assert offenders == [], (
        "a human template containing '#' must be quoted, or YAML eats the rest of the line:\n"
        + "\n".join(offenders)
    )


def test_shipped_maps_have_no_target_and_a_human_on_every_write():
    for sm in servicemaps.load(maps_dir=servicemaps.loader.shipped_dir()).services:
        assert not servicemaps.is_delegated(sm), f"{sm.service} ships with a target"
        for route in sm.routes:
            if route.kind == "write":
                assert route.human, f"{sm.service} {route.operation} has no human template"


MAP_FILE_NAMES = [
    "anthropic.yaml",
    "openai.yaml",
    "slack.yaml",
    "stripe.yaml",
    "telemetry.yaml",
]

# The response fixtures, one JSON file per service that has any.
FIXTURE_FILE_NAMES = ["slack.json", "stripe.json"]


def test_the_shipped_maps_directory_holds_every_map():
    names = sorted(
        p.name for p in servicemaps.loader.shipped_dir().iterdir() if p.suffix == ".yaml"
    )
    assert names == MAP_FILE_NAMES


def test_the_shipped_maps_and_fixtures_are_inside_a_built_wheel(tmp_path):
    """A built wheel holds the shipped maps, the fixtures and `py.typed`.

    It builds one rather than reading `shipped_dir()`, the source tree: a `.gitignore` line or a
    pyproject `exclude` can empty the wheel's data while the working copy looks right. A skip means
    the environment cannot build a wheel (`-rs` shows it).
    """
    import shutil
    import subprocess
    import zipfile

    root = Path(__file__).resolve().parents[1]
    if not (root / "pyproject.toml").is_file():
        pytest.skip("not running from a source checkout, so there is nothing to build")
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is not on PATH; this repo builds with `uv build`")
    built = subprocess.run(
        [uv, "build", "--wheel", "--out-dir", str(tmp_path)],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if built.returncode != 0:
        pytest.skip(f"`uv build --wheel` failed in this environment:\n{built.stderr}")
    wheels = sorted(tmp_path.glob("*.whl"))
    assert len(wheels) == 1, f"expected one wheel, got {[w.name for w in wheels]}"
    with zipfile.ZipFile(wheels[0]) as wheel:
        packaged = sorted(
            name.removeprefix("irimi/maps/")
            for name in wheel.namelist()
            if name.startswith("irimi/maps/") and name.endswith(".yaml")
        )
        # The entry point the maps are loaded through has to be in there too, or the refusal the
        # missing maps would raise never gets the chance to run.
        assert "irimi/servicemaps/loader.py" in wheel.namelist()
        # Without the PEP 561 marker a user's type checker treats every `irimi` import as untyped.
        assert "irimi/py.typed" in wheel.namelist()
        fixtures = sorted(
            name.removeprefix("irimi/fixtures/")
            for name in wheel.namelist()
            if name.startswith("irimi/fixtures/") and name.endswith(".json")
        )
    assert packaged == MAP_FILE_NAMES
    assert fixtures == FIXTURE_FILE_NAMES


# ------------------------------------------------------------------------------------ every field


def test_every_field_round_trips(tmp_path):
    index = load_docs(tmp_path)
    sm = index.services[0]
    assert sm.service == "demo"
    assert sm.style == "rest"
    assert sm.hosts == frozenset({"demo.example", "files.demo.example"})
    assert sm.source.endswith("0.yaml")
    listing, retrieve, create, search = sm.routes
    assert (listing.method, listing.path, listing.operation) == ("GET", "/v1/things", "things.list")
    assert listing.human == "list things"
    assert retrieve.human == ""  # optional
    assert create.kind == "write"
    assert create.ids == {"id": IdSpec(prefix="th_")}
    assert create.fixture == "thing"
    assert listing.fixture == ""  # optional, and meaningless on a live route
    assert create.compare_ignores == ("nonce",)
    assert create.live_because == ""
    assert create.target is None
    assert create.forward_auth is False
    assert search.live_because.startswith("search persists nothing")


def test_an_index_looks_up_a_service_and_a_route_by_host(tmp_path):
    index = load_docs(tmp_path)
    assert index.hosts == frozenset({"demo.example", "files.demo.example"})
    assert index.service_for("DEMO.EXAMPLE") is index.services[0]
    assert index.service_for("nope.example") is None
    assert index.route_for("nope.example", "GET", "/v1/things") is None
    assert index.route_for("demo.example", "GET", "/nope") is None
    found = index.route_for("demo.example", "GET", "/v1/things")
    assert found is not None and found[1].operation == "things.list"


def test_a_route_is_found_by_its_operation_when_its_path_cannot_be_matched(tmp_path):
    """A redacted path finds its route by the recorded operation, when exactly one route answers.

    Two routes naming the operation for this method are no answer, nor is a method it does not
    take."""
    index = load_docs(tmp_path)
    found = index.route_named("DEMO.EXAMPLE", "post", "things.create")
    assert found is not None and found.path == "/v1/things"
    assert index.route_named("demo.example", "GET", "things.create") is None
    assert index.route_named("demo.example", "POST", "things.nope") is None
    assert index.route_named("nope.example", "POST", "things.create") is None
    twice = GOOD_MAP.replace("operation: things.retrieve", "operation: things.list")
    (tmp_path / "twice").mkdir()
    assert (
        load_docs(tmp_path / "twice", twice).route_named("demo.example", "GET", "things.list")
        is None
    )


def test_an_empty_index_is_usable():
    assert MapIndex().hosts == frozenset()
    assert MapIndex().service_for("anything") is None


# --------------------------------------------------------------------------------- route matching


@pytest.mark.parametrize(
    ("method", "path", "operation"),
    [
        ("GET", "/v1/things", "things.list"),
        ("get", "/v1/things", "things.list"),  # method is compared upper-case
        ("GET", "/v1/things/", "things.list"),  # a trailing slash is not a segment
        ("GET", "/v1/things/th_1", "things.retrieve"),
        ("POST", "/v1/things", "things.create"),
        ("POST", "/v1/search", "things.search"),
    ],
)
def test_a_route_matches_its_method_and_path(tmp_path, method, path, operation):
    sm = load_docs(tmp_path).services[0]
    route = servicemaps.model.match_route(sm, method, path)
    assert route is not None and route.operation == operation


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("DELETE", "/v1/things"),  # no route for the method
        ("GET", "/v1/things/th_1/extra"),  # too many segments
        ("GET", "/v2/things"),  # literal segment differs
        ("GET", "/"),
    ],
)
def test_a_route_does_not_match_another_path(tmp_path, method, path):
    assert servicemaps.model.match_route(load_docs(tmp_path).services[0], method, path) is None


def test_literal_segment_beats_a_pattern(tmp_path):
    doc = (
        GOOD_MAP
        + """
  - match:
      method: GET
      path: /v1/things/latest
    operation: things.latest
    kind: read
"""
    )
    sm = load_docs(tmp_path, doc).services[0]
    latest = servicemaps.model.match_route(sm, "GET", "/v1/things/latest")
    other = servicemaps.model.match_route(sm, "GET", "/v1/things/th_1")
    assert latest is not None and latest.operation == "things.latest"
    assert other is not None and other.operation == "things.retrieve"


def test_star_method_matches_anything(tmp_path):
    doc = """
version: 1
service: demo
hosts: [demo.example]
routes:
  - match:
      method: "*"
      path: /anything
    operation: demo.anything
    kind: write
    human: do anything
"""
    sm = load_docs(tmp_path, doc).services[0]
    for method in ("GET", "POST", "PATCH"):
        route = servicemaps.model.match_route(sm, method, "/anything")
        assert route is not None and route.operation == "demo.anything"


# ------------------------------------------------------------------------------ schema validation


@pytest.mark.parametrize(
    ("bad", "good", "message"),
    [
        ("version: 1", "version: 2", "`version` must be 1"),
        ("service: demo", "service: ''", "`service` is required"),
        ("style: rest", "style: sometimes", "`style` must be one of"),
        ("kind: read\n    human: list things", "kind: reed\n    human: list things", "`kind` must"),
        ("operation: things.list", "operation: things.list\n    nope: 1", "unknown route key(s)"),
        ("hosts:", "nope:\nhosts:", "unknown map key(s)"),
        ("      id: th_", "      id: ''", "must be a non-empty id prefix"),
        ("      path: /v1/things\n", "\n", "must start with"),
        ("      method: GET\n      path: /v1/things\n", "      verb: GET\n", "unknown match"),
        ("compare_ignores:\n      - nonce", "compare_ignores: 3", "must be a list of strings"),
        ("    fixture: thing", "    fixture: 3", "`fixture` must be a string"),
        ("    fixture: thing", "    fires: 3", "`fires` must be a list of strings"),
        ("    fixture: thing", "    fires:\n      - 3", "`fires` entry must be a string"),
    ],
)
def test_schema_errors_name_the_rule(tmp_path, bad, good, message):
    assert bad in GOOD_MAP
    assert_refused(tmp_path, GOOD_MAP.replace(bad, good, 1), message)


def test_hosts_must_be_bare_names(tmp_path):
    assert_refused(
        tmp_path, GOOD_MAP.replace("- demo.example", "- https://demo.example"), "bare name"
    )
    assert_refused(tmp_path, GOOD_MAP.replace("- demo.example", "- demo.example:443"), "bare name")


def test_hosts_must_be_a_non_empty_list(tmp_path):
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("hosts:\n  - demo.example\n  - files.demo.example", "hosts: []"),
        "non-empty list",
    )


def test_routes_must_be_a_non_empty_list(tmp_path):
    doc = "version: 1\nservice: demo\nhosts: [demo.example]\nroutes: []\n"
    assert_refused(tmp_path, doc, "`routes` must be a non-empty list")


def test_a_route_cannot_appear_twice(tmp_path):
    doc = (
        GOOD_MAP
        + """
  - match:
      method: GET
      path: /v1/things
    operation: things.list.again
    kind: read
"""
    )
    assert_refused(tmp_path, doc, "appears twice")


def test_duplicate_service_and_host_are_refused(tmp_path):
    with pytest.raises(MapError, match="already defined"):
        load_docs(tmp_path, GOOD_MAP, GOOD_MAP)
    other = GOOD_MAP.replace("service: demo", "service: other")
    with pytest.raises(MapError, match="already mapped"):
        load_docs(tmp_path, GOOD_MAP, other)


def test_empty_and_invalid_files_are_refused(tmp_path):
    assert_refused(tmp_path, "", "file is empty")
    assert_refused(tmp_path, "version: 1\nservice: [", "not valid YAML")
    assert_refused(tmp_path, "- just a list\n", "must be a mapping")


def test_a_fixture_on_a_live_route_is_refused(tmp_path):
    """The loader refuses a `fixture:` on a live route, which the real service answers."""
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("    human: list things", "    fixture: thing"),
        "`fixture:` names the object a locally answered write starts from",
    )


def test_a_fixture_on_an_unknown_route_is_allowed(tmp_path):
    """`unknown` is answered locally exactly like `write`, so it may start from a fixture too."""
    doc = GOOD_MAP.replace(
        "    operation: things.create\n    kind: write",
        "    operation: things.create\n    kind: unknown",
    )
    index = load_docs(tmp_path, doc)
    create = servicemaps.model.match_route(index.services[0], "POST", "/v1/things")
    assert create is not None and create.fixture == "thing"


def test_every_shipped_precondition_names_a_check_that_exists():
    """Every shipped `precondition:` names an entry in its `Service.preconditions`.

    One with no entry would make every such write `not_evaluable`, silently."""
    index = servicemaps.load(cwd=None, maps_dir=servicemaps.loader.shipped_dir())
    named = [
        (sm.service, route.precondition)
        for sm in index.services
        for route in sm.routes
        if route.precondition
    ]
    assert named, "no shipped route declares a precondition"
    for service, name in named:
        assert name in services.of(service).preconditions, (service, name)


def test_a_precondition_on_a_live_route_is_refused(tmp_path):
    """The loader refuses a `precondition:` on a live route, which would never run."""
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("    human: list things", "    precondition: thing_exists"),
        "`precondition:` names the check run before a write is faked",
    )


def test_every_shipped_fires_list_is_pinned_to_the_writes_the_effects_model():
    """The shipped `fires:` lists are pinned to the three writes Stripe's effects model.

    `fires:` is free text, and a route firing an event the overlay does not model is a write whose
    visible effect and webhooks disagree."""
    index = servicemaps.load(maps_dir=servicemaps.loader.shipped_dir())
    fired = {
        (sm.service, route.operation): route.fires
        for sm in index.services
        for route in sm.routes
        if route.fires
    }
    assert fired == {
        ("stripe", "refunds.create"): ("refund.created", "charge.refunded"),
        ("stripe", "customers.update"): ("customer.updated",),
        ("stripe", "payment_intents.cancel"): ("payment_intent.canceled",),
    }


def test_every_shipped_fires_route_is_a_write_irimi_answers_itself():
    """No shipped route with `fires:` is live: the real service sends its own webhooks."""
    index = servicemaps.load(maps_dir=servicemaps.loader.shipped_dir())
    for sm in index.services:
        for route in sm.routes:
            if route.fires:
                assert route.kind not in LIVE_KINDS, (sm.service, route.operation)


def test_fires_on_a_live_route_is_refused(tmp_path):
    """The loader refuses `fires:` on a live route, whose webhooks the real service sends."""
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("    human: list things", "    fires:\n      - thing.listed"),
        "`fires:` names the webhooks a FAKED write would have sent",
    )


def test_fires_on_an_unknown_route_is_allowed(tmp_path):
    """`unknown` is answered locally exactly like `write`, so it may name its webhooks too."""
    doc = GOOD_MAP.replace(
        "    operation: things.create\n    kind: write",
        "    operation: things.create\n    kind: unknown",
    ).replace("    fixture: thing", "    fixture: thing\n    fires:\n      - thing.created")
    index = load_docs(tmp_path, doc)
    create = servicemaps.model.match_route(index.services[0], "POST", "/v1/things")
    assert create is not None and create.fires == ("thing.created",)


def test_a_repeated_event_name_in_fires_is_refused(tmp_path):
    """A repeat in `fires:` is refused: the summary would list one write's event twice."""
    assert_refused(
        tmp_path,
        GOOD_MAP.replace(
            "    fixture: thing",
            "    fixture: thing\n    fires:\n      - thing.created\n      - thing.created",
        ),
        "names 'thing.created' more than once",
    )


def test_a_misspelled_fires_key_is_refused(tmp_path):
    """`ROUTE_KEYS` is a closed set, so `fired:` is refused by name, not ignored."""
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("    fixture: thing", "    fired:\n      - thing.created"),
        "unknown route key(s) fired",
    )


def test_live_because_and_compare_ignores_are_restricted_to_their_kinds(tmp_path):
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("    ids:\n      id: th_\n", "    live_because: it is safe\n"),
        "`live_because` belongs on a route that is forwarded live",
    )
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("    human: list things", "    compare_ignores: [x]"),
        "`compare_ignores` belongs on a `write`, `unknown` or `llm` route",
    )


# ----------------------------------------------------------------------------------------- targets


def test_default_target_is_self(tmp_path):
    sm = load_docs(tmp_path).services[0]
    assert sm.target is None
    for route in sm.routes:
        assert servicemaps.target_for(sm, route) is None
    assert servicemaps.is_delegated(sm) is False


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3000/refund",
        "http://localhost:3000",
        "https://127.0.0.1:3000",
        "http://[::1]:3000",
    ],
)
def test_loopback_targets_are_accepted(tmp_path, url):
    doc = GOOD_MAP.replace("style: rest", f"style: rest\ntarget: {url}")
    sm = load_docs(tmp_path, doc).services[0]
    assert sm.target.url == url.rstrip("/")
    assert servicemaps.is_delegated(sm) is True


def test_a_service_target_answers_writes_and_unknowns_but_not_reads(tmp_path):
    doc = GOOD_MAP.replace("style: rest", "style: rest\ntarget: http://127.0.0.1:3000")
    sm = load_docs(tmp_path, doc).services[0]
    listing, _retrieve, create, search = sm.routes
    target = servicemaps.target_for(sm, create)
    assert target is not None
    assert target.url == "http://127.0.0.1:3000"
    assert servicemaps.target_for(sm, listing) is None
    assert servicemaps.target_for(sm, search) is None


def test_target_reads_sends_reads_to_the_target_too(tmp_path):
    doc = GOOD_MAP.replace(
        "style: rest", "style: rest\ntarget: http://127.0.0.1:3000\ntarget_reads: true"
    )
    sm = load_docs(tmp_path, doc).services[0]
    for route in sm.routes:
        target = servicemaps.target_for(sm, route)
        assert target is not None
        assert target.url == "http://127.0.0.1:3000"


def test_target_reads_without_a_target_is_refused(tmp_path):
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("style: rest", "style: rest\ntarget_reads: true"),
        "needs a `target:` URL to send the reads to",
    )


def test_llm_and_telemetry_routes_are_never_targeted(tmp_path):
    doc = """
version: 1
service: demo
hosts: [demo.example]
target: http://127.0.0.1:3000
target_reads: true
routes:
  - match:
      method: POST
      path: /v1/messages
    operation: messages.create
    kind: llm
  - match:
      method: POST
      path: /v1/traces
    operation: traces.ingest
    kind: telemetry
"""
    sm = load_docs(tmp_path, doc).services[0]
    for route in sm.routes:
        assert servicemaps.target_for(sm, route) is None


def test_route_level_target_wins_over_the_service(tmp_path):
    doc = GOOD_MAP.replace("style: rest", "style: rest\ntarget: http://127.0.0.1:3000").replace(
        "    ids:\n      id: th_\n",
        "    target: http://127.0.0.1:4111/create\n    ids:\n      id: th_\n",
    )
    sm = load_docs(tmp_path, doc).services[0]
    target = servicemaps.target_for(sm, sm.routes[2])
    assert target is not None
    assert target.url == "http://127.0.0.1:4111/create"


def test_route_level_target_on_a_read_is_refused(tmp_path):
    doc = GOOD_MAP.replace("    human: list things", "    target: http://127.0.0.1:3000")
    assert_refused(tmp_path, doc, "valid on `write` and `unknown` routes only")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("ftp://127.0.0.1", "an http(s) URL"),
        ("127.0.0.1:3000", "an http(s) URL"),
        ("self-ish", "an http(s) URL"),
        ("http://127.0.0.1:3000?a=b", "no query"),
        ("http://user:pw@127.0.0.1:3000", "no query"),
        ("http://", "has no host"),
    ],
)
def test_bad_target_values_are_refused(tmp_path, value, message):
    assert_refused(
        tmp_path, GOOD_MAP.replace("style: rest", f"style: rest\ntarget: {value!r}"), message
    )


def test_non_loopback_targets_need_allow_target_host(tmp_path):
    doc = GOOD_MAP.replace("style: rest", "style: rest\ntarget: http://stub.example:3000")
    assert_refused(tmp_path, doc, "is not loopback")
    sm = load_docs(tmp_path, doc, allow=frozenset({"stub.example"})).services[0]
    assert sm.target.url == "http://stub.example:3000"


def test_forward_auth_needs_a_target(tmp_path):
    doc = GOOD_MAP.replace("    ids:\n      id: th_\n", "    forward_auth: true\n")
    assert_refused(tmp_path, doc, "needs a target to forward to")
    ok = GOOD_MAP.replace(
        "    ids:\n      id: th_\n", "    forward_auth: true\n    target: http://127.0.0.1:3000\n"
    )
    assert load_docs(tmp_path, ok).services[0].routes[2].forward_auth is True


def test_target_must_be_a_string_and_booleans_must_be_booleans(tmp_path):
    assert_refused(
        tmp_path, GOOD_MAP.replace("style: rest", "style: rest\ntarget: 3000"), "must be a str"
    )
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("style: rest", "style: rest\ntarget_reads: yes please"),
        "must be true or false",
    )


# ---------------------------------------------------------------------------- the overrides file


OVERRIDE = """
service: demo
target: http://127.0.0.1:3000
"""


def test_override_sets_a_service_target(tmp_path):
    sm = load_docs(tmp_path, GOOD_MAP, override=OVERRIDE).services[0]
    assert sm.target.url == "http://127.0.0.1:3000"
    target = servicemaps.target_for(sm, sm.routes[2])
    assert target is not None
    assert target.url == "http://127.0.0.1:3000"
    assert sm.source.endswith(servicemaps.loader.CWD_OVERRIDE_NAME)


def test_override_sets_a_route_target_and_forward_auth(tmp_path):
    override = """
service: demo
routes:
  - match:
      method: POST
      path: /v1/things
    target: http://127.0.0.1:3000/create
    forward_auth: true
"""
    sm = load_docs(tmp_path, GOOD_MAP, override=override).services[0]
    create = sm.routes[2]
    assert create.target.url == "http://127.0.0.1:3000/create"
    assert create.forward_auth is True
    assert sm.routes[0].target is None  # the other routes are untouched


def test_override_can_delegate_reads(tmp_path):
    override = OVERRIDE + "target_reads: true\n"
    sm = load_docs(tmp_path, GOOD_MAP, override=override).services[0]
    assert sm.target_reads is True
    target = servicemaps.target_for(sm, sm.routes[0])
    assert target is not None
    assert target.url == "http://127.0.0.1:3000"


def test_override_may_hold_several_documents(tmp_path):
    other = GOOD_MAP.replace("service: demo", "service: other").replace(
        "demo.example", "other.example"
    )
    override = OVERRIDE + "---\nservice: other\ntarget: http://127.0.0.1:4000\n"
    index = load_docs(tmp_path, GOOD_MAP, other, override=override)
    assert [sm.target.url for sm in index.services] == [
        "http://127.0.0.1:3000",
        "http://127.0.0.1:4000",
    ]


def test_override_naming_an_unknown_service_is_an_error(tmp_path):
    with pytest.raises(MapError, match="no shipped map for service 'nope'"):
        load_docs(tmp_path, GOOD_MAP, override="service: nope\ntarget: http://127.0.0.1:3000\n")


def test_override_naming_an_unknown_route_is_an_error(tmp_path):
    override = """
service: demo
routes:
  - match:
      method: POST
      path: /v1/nope
    target: http://127.0.0.1:3000
"""
    with pytest.raises(MapError, match="names no route"):
        load_docs(tmp_path, GOOD_MAP, override=override)


def test_override_cannot_change_a_routes_kind(tmp_path):
    override = """
service: demo
routes:
  - match:
      method: POST
      path: /v1/things
    kind: read
"""
    with pytest.raises(MapError, match="unknown override route key"):
        load_docs(tmp_path, GOOD_MAP, override=override)


def test_override_cannot_add_hosts_or_routes(tmp_path):
    with pytest.raises(MapError, match="unknown override key"):
        load_docs(tmp_path, GOOD_MAP, override="service: demo\nhosts: [evil.example]\n")


def test_override_target_is_validated_like_any_other(tmp_path):
    with pytest.raises(MapError, match="is not loopback"):
        load_docs(tmp_path, GOOD_MAP, override="service: demo\ntarget: http://stub.example\n")


# --------------------------------------------------------------------- where the override lives


def test_cwd_override_wins_over_home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    cwd = tmp_path / "cwd"
    home.mkdir()
    cwd.mkdir(exist_ok=True)  # tests/conftest.py already made this one and chdir'd into it
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(home))
    (home / servicemaps.loader.HOME_OVERRIDE_NAME).write_text(
        "service: demo\ntarget: http://127.0.0.1:1\n"
    )
    (cwd / servicemaps.loader.CWD_OVERRIDE_NAME).write_text(
        "service: demo\ntarget: http://127.0.0.1:2\n"
    )
    assert servicemaps.override_path(cwd) == cwd / servicemaps.loader.CWD_OVERRIDE_NAME
    index = servicemaps.load(cwd=cwd, maps_dir=write_maps(tmp_path, GOOD_MAP))
    target = index.services[0].target
    assert target is not None
    assert target.url == "http://127.0.0.1:2"


def test_home_override_is_the_fallback(tmp_path, monkeypatch):
    home = tmp_path / "home"
    cwd = tmp_path / "cwd"
    home.mkdir()
    cwd.mkdir(exist_ok=True)  # tests/conftest.py already made this one and chdir'd into it
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(home))
    (home / servicemaps.loader.HOME_OVERRIDE_NAME).write_text(
        "service: demo\ntarget: http://127.0.0.1:1\n"
    )
    assert servicemaps.override_path(cwd) == home / servicemaps.loader.HOME_OVERRIDE_NAME
    index = servicemaps.load(cwd=cwd, maps_dir=write_maps(tmp_path, GOOD_MAP))
    target = index.services[0].target
    assert target is not None
    assert target.url == "http://127.0.0.1:1"


def test_no_override_file_is_fine(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "empty-home"))
    assert servicemaps.override_path(tmp_path) is None


# ------------------------------------------------------------------------- the dataclasses alone


def test_route_and_servicemap_defaults():
    route = Route(method="POST", path="/x", operation="x.create", kind="write")
    assert (route.human, route.ids, route.compare_ignores, route.fixture) == ("", {}, (), "")
    assert (route.fires, route.live_because, route.forward_auth) == ((), "", False)
    sm = ServiceMap(service="x", hosts=frozenset({"x.example"}), routes=(route,))
    assert (sm.style, sm.target, sm.target_reads) == ("rest", None, False)
    assert sm.credential_path_hosts == frozenset()
    assert sm.default_kind is None
    assert servicemaps.target_for(sm, route) is None


# ------------------------------------------------- a maps directory that is missing or holds none


def test_missing_maps_directory_is_a_named_refusal(tmp_path):
    missing = tmp_path / "nope"
    with pytest.raises(MapError) as exc:
        servicemaps.loader.load_shipped(maps_dir=missing)
    assert str(missing) in str(exc.value)
    assert "cannot read the service maps directory" in str(exc.value)


def test_empty_maps_directory_is_a_named_refusal(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(MapError) as exc:
        servicemaps.loader.load_shipped(maps_dir=empty)
    assert str(empty) in str(exc.value)
    assert "no service maps found" in str(exc.value)


def test_a_directory_with_no_yaml_is_the_same_refusal(tmp_path):
    """The rule is 'no maps parsed', not 'no files present'."""
    directory = tmp_path / "not-maps"
    directory.mkdir()
    (directory / "notes.txt").write_text("not a map\n")
    with pytest.raises(MapError) as exc:
        servicemaps.loader.load_shipped(maps_dir=directory)
    assert "no service maps found" in str(exc.value)


def test_a_maps_directory_that_cannot_be_scanned_is_a_named_refusal(tmp_path, monkeypatch):
    """An OSError from `iterdir` is a named refusal, without a directory mode, so it runs as root.

    The mode-based test below skips as root.
    """
    directory = tmp_path / "locked"
    directory.mkdir()
    (directory / "demo.yaml").write_text(GOOD_MAP)
    real_iterdir = Path.iterdir

    def refuse(self):
        if os.path.samefile(self, directory):
            raise PermissionError(13, "Permission denied")
        return real_iterdir(self)

    monkeypatch.setattr(Path, "iterdir", refuse)
    with pytest.raises(MapError) as exc:
        servicemaps.loader.load_shipped(maps_dir=directory)
    assert str(directory) in str(exc.value)
    assert "cannot read the service maps directory" in str(exc.value)
    assert "Permission denied" in str(exc.value)


def test_an_unreadable_maps_directory_is_a_named_refusal(tmp_path):
    """A maps directory with mode 000 is a named refusal; root, which ignores modes, skips it."""
    import os

    if os.geteuid() == 0:
        pytest.skip("root ignores the directory mode, so the scan would succeed")
    directory = tmp_path / "locked"
    directory.mkdir()
    (directory / "demo.yaml").write_text(GOOD_MAP)
    os.chmod(directory, 0o000)
    try:
        try:
            list(directory.iterdir())
        except PermissionError:
            pass
        else:
            pytest.skip("this filesystem does not honour mode 0o000 on a directory")
        with pytest.raises(MapError) as exc:
            servicemaps.loader.load_shipped(maps_dir=directory)
    finally:
        os.chmod(directory, 0o700)
    assert str(directory) in str(exc.value)
    assert "cannot read the service maps directory" in str(exc.value)


@pytest.mark.parametrize("argv", [["maps", "list"], ["serve"], ["shadow", "--", "true"]])
def test_a_missing_maps_directory_is_one_line_on_the_cli(tmp_path, monkeypatch, capsys, argv):
    """A stripped install prints a named refusal, not a traceback."""
    from irimi.cli import main

    missing = tmp_path / "gone"
    monkeypatch.setattr(servicemaps.loader, "shipped_dir", lambda: missing)
    assert main(argv) == 2
    err = capsys.readouterr().err
    assert err.startswith("error: ")
    assert str(missing) in err
    assert "Traceback" not in err
    assert err.count("\n") == 1


# ------------------------------------------------------------------------- wildcard hosts

# `*` opens a YAML alias, so every wildcard host has to be quoted or the document does not parse.
WILDCARD = """
version: 1
service: wild
hosts:
  - "*.demo.example"
routes:
  - match:
      method: POST
      path: /ingest
    operation: wild.ingest
    kind: write
    human: ingest one event
"""

EXACT_UNDER_WILDCARD = """
version: 1
service: exact
hosts:
  - one.demo.example
routes:
  - match:
      method: POST
      path: /ingest
    operation: exact.ingest
    kind: write
    human: ingest one event
"""

DEEPER_WILDCARD = """
version: 1
service: deeper
hosts:
  - "*.eu.demo.example"
routes:
  - match:
      method: POST
      path: /ingest
    operation: deeper.ingest
    kind: write
    human: ingest one event
"""


def test_a_wildcard_host_matches_one_or_more_leading_labels(tmp_path):
    index = load_docs(tmp_path, WILDCARD)
    assert index.service_for("a.demo.example") is index.services[0]
    assert index.service_for("a.b.demo.example") is index.services[0]
    assert index.service_for("A.DEMO.EXAMPLE") is index.services[0]


def test_a_wildcard_host_does_not_match_the_bare_domain(tmp_path):
    """`*.demo.example` misses `demo.example` and a name merely ending in it."""
    index = load_docs(tmp_path, WILDCARD)
    assert index.service_for("demo.example") is None
    assert index.service_for("notdemo.example") is None
    assert index.service_for("demo.example.evil.test") is None


def test_an_exact_host_beats_a_wildcard(tmp_path):
    index = load_docs(tmp_path, WILDCARD, EXACT_UNDER_WILDCARD)
    one = index.service_for("one.demo.example")
    two = index.service_for("two.demo.example")
    assert one is not None and one.service == "exact"
    assert two is not None and two.service == "wild"


def test_the_longest_wildcard_wins(tmp_path):
    index = load_docs(tmp_path, WILDCARD, DEEPER_WILDCARD)
    eu = index.service_for("a.eu.demo.example")
    us = index.service_for("a.us.demo.example")
    assert eu is not None and eu.service == "deeper"
    assert us is not None and us.service == "wild"


def test_a_wildcard_host_routes_like_any_other(tmp_path):
    index = load_docs(tmp_path, WILDCARD)
    found = index.route_for("a.demo.example", "POST", "/ingest")
    assert found is not None and found[1].operation == "wild.ingest"


@pytest.mark.parametrize(
    "host", ["*", "*.", "*foo.demo.example", "foo.*.demo.example", "**.demo.example", "*.example"]
)
def test_a_malformed_wildcard_host_is_refused_by_name(tmp_path, host):
    assert_refused(
        tmp_path,
        WILDCARD.replace('"*.demo.example"', f'"{host}"'),
        "may use a wildcard only as a leading '*.' label",
    )


def test_the_same_wildcard_in_two_maps_is_a_duplicate_host(tmp_path):
    with pytest.raises(MapError) as exc:
        load_docs(tmp_path, WILDCARD, WILDCARD.replace("service: wild", "service: wild2"))
    assert "host '*.demo.example' is already mapped by" in str(exc.value)


def test_a_wildcard_and_an_exact_host_under_it_are_not_a_duplicate(tmp_path):
    index = load_docs(tmp_path, WILDCARD, EXACT_UNDER_WILDCARD)
    assert {sm.service for sm in index.services} == {"wild", "exact"}


def test_patterns_are_not_in_the_reverse_doors_allow_list(tmp_path):
    """A wildcard host classifies through the proxy but is not a base-URL host.

    Base-URL mode compares one literal host against its allowed set, so a pattern there would match
    nothing; `--base-url-host` names one.
    """
    index = load_docs(tmp_path, WILDCARD, EXACT_UNDER_WILDCARD)
    assert index.hosts == frozenset({"one.demo.example"})
    assert index.patterns == ("*.demo.example",)
    assert MapIndex().patterns == ()


# ------------------------------------------------------ the conftest isolation guards itself


def test_the_conftest_redirects_irimi_home_away_from_the_developers_own(tmp_path):
    """`tests/conftest.py` points `$IRIMI_HOME` away from the developer's own.

    Without it the suite fails only for a developer with a `~/.irimi/maps.yaml`; this fails on
    every machine.
    """
    home = Path(os.environ[paths.IRIMI_HOME_ENV])
    assert home != Path.home() / ".irimi"
    assert paths.irimi_home() == home

    home.mkdir(parents=True, exist_ok=True)
    (home / servicemaps.loader.HOME_OVERRIDE_NAME).write_text(
        "service: demo\ntarget: http://127.0.0.1:3000\n"
    )
    assert (
        servicemaps.override_path(cwd=tmp_path / "empty")
        == home / servicemaps.loader.HOME_OVERRIDE_NAME
    )
    assert load_docs(tmp_path).services[0].target.url == "http://127.0.0.1:3000"


# ------------------------------------------------------------------------------- path parameters


def test_path_params_binds_nothing_when_the_literal_segments_differ():
    """`path_params` binds nothing to a path that shares the segment count but not the literals."""
    assert servicemaps.path_params("/v1/customers/{customer}", "/v9/charges/cus_X") == {}
    assert servicemaps.path_params("/v1/customers/{customer}", "/v1/customers/cus_X") == {
        "customer": "cus_X"
    }
    assert servicemaps.path_params("/v1/customers/{customer}", "/v1/customers") == {}


def test_path_params_percent_decodes_a_captured_segment():
    """A captured segment is percent-decoded, as the service decodes it; literals compare raw.

    `cus%5FREAL123` is `cus_REAL123`, so the echo reuses the id the request named."""
    assert servicemaps.path_params("/v1/customers/{customer}", "/v1/customers/cus%5FX") == {
        "customer": "cus_X"
    }
    assert servicemaps.path_params("/v1/customers/{c}", "/v1/customers/a%20b") == {"c": "a b"}
    # A literal segment spelled with an escape does not match; decoding only reads the holes.
    assert servicemaps.path_params("/v1/customers/{c}", "/v1/custom%65rs/x") == {}


def test_a_route_pattern_may_not_repeat_a_parameter_name(tmp_path):
    """`/a/{x}/b/{x}` is refused: it would drop one capture and echo the wrong id."""
    doc = """
version: 1
service: dup
hosts:
  - dup.example
routes:
  - match:
      method: POST
      path: /a/{x}/b/{x}
    operation: dup.thing
    kind: write
    human: do a thing
"""
    assert_refused(tmp_path, doc, "appears more than once in the path")


# ------------------------------------------------ the --target and --target-reads flags


def _flagged(tmp_path, targets=(), target_reads=(), allow=frozenset()):
    return servicemaps.load(
        allow_target_hosts=allow,
        cwd=tmp_path / "cwd",
        maps_dir=write_maps(tmp_path, GOOD_MAP),
        targets=targets,
        target_reads=target_reads,
    )


WITH_PATTERN_WRITE = (
    GOOD_MAP
    + """
  - match:
      method: POST
      path: /v1/things/{thing}/archive
    operation: things.archive
    kind: write
"""
)


def test_a_bare_host_target_sets_the_service_target(tmp_path):
    index = _flagged(tmp_path, targets=[("demo.example", "", "http://127.0.0.1:3000")])
    assert index.services[0].target.url == "http://127.0.0.1:3000"
    assert all(r.target is None for r in index.services[0].routes)


def test_a_host_and_path_target_sets_only_the_matching_targetable_routes(tmp_path):
    index = _flagged(tmp_path, targets=[("demo.example", "/v1/things", "http://127.0.0.1:3000")])
    targeted = [r for r in index.services[0].routes if r.target is not None]
    assert [r.kind for r in targeted] == ["write"]  # the GET of the same path is left alone
    assert index.services[0].target is None


@pytest.mark.parametrize("spec", ["/v1/things/{thing}/archive", "/v1/things/th_REAL1/archive"])
def test_a_target_path_may_be_the_pattern_or_a_real_path(tmp_path, spec):
    """A target names its route by the map's pattern or by a concrete request path."""
    index = servicemaps.load(
        cwd=tmp_path / "cwd",
        maps_dir=write_maps(tmp_path, WITH_PATTERN_WRITE),
        targets=[("demo.example", spec, "http://127.0.0.1:3000")],
    )
    targeted = [r for r in index.services[0].routes if r.target is not None]
    assert [r.operation for r in targeted] == ["things.archive"]


def test_target_reads_marks_the_service_delegated(tmp_path):
    index = _flagged(
        tmp_path,
        targets=[("demo.example", "", "http://127.0.0.1:3000")],
        target_reads=["demo.example"],
    )
    assert index.services[0].target_reads is True
    assert servicemaps.is_delegated(index.services[0])


def test_target_reads_without_a_target_is_still_refused_when_it_comes_from_a_flag(tmp_path):
    """`--target-reads` with no target is refused, as `target_reads: true` in a map is."""
    with pytest.raises(MapError) as exc:
        _flagged(tmp_path, target_reads=["demo.example"])
    assert str(exc.value).endswith(
        "`target_reads: true` needs a `target:` URL to send the reads to"
    )


def test_a_flag_target_is_validated_like_every_other_one(tmp_path):
    with pytest.raises(MapError) as exc:
        _flagged(tmp_path, targets=[("demo.example", "", "http://example.com")])
    assert str(exc.value).endswith(
        "target host 'example.com' is not loopback; targets must be 127.0.0.1, ::1 or localhost "
        "unless you pass --allow-remote-target example.com"
    )
    ok = _flagged(
        tmp_path,
        targets=[("demo.example", "", "http://example.com")],
        allow=frozenset({"example.com"}),
    )
    assert ok.services[0].target.url == "http://example.com"


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("http://127.0.0.1:99999", "is not a URL irimi can parse"),
        ("http://[::1/x", "is not a URL irimi can parse"),
        ("http://127.0.0.1:0", "has a bad port"),
    ],
)
def test_a_target_url_python_cannot_parse_is_a_named_refusal(tmp_path, url, message):
    """A target `urlsplit` or its `.port` raises on is a one-line MapError, not a traceback."""
    with pytest.raises(MapError) as exc:
        _flagged(tmp_path, targets=[("demo.example", "", url)])
    assert message in str(exc.value)


def test_a_flag_naming_an_unknown_host_or_route_is_an_error(tmp_path):
    """A typo that quietly changed nothing would look exactly like a working target."""
    with pytest.raises(MapError) as exc:
        _flagged(tmp_path, targets=[("nope.example", "", "http://127.0.0.1:3000")])
    assert "no loaded service map claims host" in str(exc.value)

    with pytest.raises(MapError) as exc:
        _flagged(tmp_path, targets=[("demo.example", "/nope", "http://127.0.0.1:3000")])
    assert "no `write` or `unknown` route" in str(exc.value)

    with pytest.raises(MapError) as exc:
        _flagged(tmp_path, target_reads=["nope.example"])
    assert "no loaded service map claims host" in str(exc.value)


def test_a_flag_target_beats_the_overrides_file(tmp_path):
    cwd = tmp_path / "cwd"
    cwd.mkdir(exist_ok=True)
    (cwd / servicemaps.loader.CWD_OVERRIDE_NAME).write_text(
        "service: demo\ntarget: http://127.0.0.1:1111\n"
    )
    index = _flagged(tmp_path, targets=[("demo.example", "", "http://127.0.0.1:2222")])
    assert index.services[0].target.url == "http://127.0.0.1:2222"


def test_a_credential_path_host_may_only_be_targeted_at_loopback():
    """`--allow-remote-target` does not reach a webhook host, whose URL is the whole credential."""
    index = servicemaps.load(
        cwd=None,
        maps_dir=servicemaps.loader.shipped_dir(),
        targets=[("hooks.slack.com", "", "http://127.0.0.1:3000")],
    )
    hooks = index.service_for("hooks.slack.com")
    assert hooks is not None and hooks.target is not None
    assert hooks.target.url == "http://127.0.0.1:3000"

    with pytest.raises(MapError) as exc:
        servicemaps.load(
            cwd=None,
            maps_dir=servicemaps.loader.shipped_dir(),
            allow_target_hosts=frozenset({"stub.internal"}),
            targets=[("hooks.slack.com", "", "http://stub.internal:3000")],
        )
    assert "a target on this host may only be loopback" in str(exc.value)


def test_the_rule_follows_the_host_through_the_overrides_file_too(tmp_path):
    """The webhook rule holds for a flag target too: it is checked once the map layers merge."""
    with pytest.raises(MapError) as exc:
        servicemaps.load(
            cwd=None,
            maps_dir=servicemaps.loader.shipped_dir(),
            allow_target_hosts=frozenset({"stub.internal"}),
            targets=[("slack.com", "/api/chat.postMessage", "http://stub.internal:9000")],
        )
    assert "a target on this host may only be loopback" in str(exc.value)


def test_the_webhook_rule_covers_the_other_host_of_the_same_service():
    """The webhook rule holds when the service is targeted by its other host, `slack.com`."""
    with pytest.raises(MapError):
        servicemaps.load(
            cwd=None,
            maps_dir=servicemaps.loader.shipped_dir(),
            allow_target_hosts=frozenset({"stub.internal"}),
            targets=[("slack.com", "", "http://stub.internal:3000")],
        )


# ------------------------------------------------------------------- retired and renamed keys


@pytest.mark.parametrize(
    ("good", "retired", "message"),
    [
        ("style: rest", "verbs: honest", "`verbs:` is now `style:`"),
        (
            "    compare_ignores:\n      - nonce",
            "    volatile:\n      - nonce",
            "is now `compare_ignores:`",
        ),
        (
            "    live_because: search persists nothing; Stripe's own spec calls it a POST read",
            "    persists: false",
            "`persists:` is now `live_because:",
        ),
        (
            "    live_because: search persists nothing; Stripe's own spec calls it a POST read",
            "    comment: search persists nothing",
            "`comment:` is gone",
        ),
    ],
)
def test_a_retired_key_is_refused_naming_its_replacement(tmp_path, good, retired, message):
    assert good in GOOD_MAP
    assert_refused(tmp_path, GOOD_MAP.replace(good, retired, 1), message)


def test_a_route_level_error_names_the_route(tmp_path):
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("    fixture: thing", "    fixtrue: thing"),
        "route POST /v1/things: unknown route key(s) fixtrue",
    )
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("      method: GET\n      path: /v1/things\n", "      verb: GET\n", 1),
        "routes[0]: unknown match key(s) verb",
    )


# ------------------------------------------------------------------------------------ id formats


def test_ids_accept_an_integer_or_a_uuid_format_beside_a_prefix(tmp_path):
    doc = GOOD_MAP.replace(
        "    ids:\n      id: th_\n",
        "    ids:\n      id: th_\n"
        "      number:\n        format: int\n"
        "      ref:\n        format: uuid\n",
    )
    create = servicemaps.model.match_route(
        load_docs(tmp_path, doc).services[0], "POST", "/v1/things"
    )
    assert create is not None
    assert create.ids == {
        "id": IdSpec(prefix="th_"),
        "number": IdSpec(format="int"),
        "ref": IdSpec(format="uuid"),
    }


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ("\n        format: hex", "`ids.id.format` must be one of"),
        ("\n        prefix: th_", "unknown id key(s) prefix"),
        (" 3", "`ids.id` must be a string"),
    ],
)
def test_a_bad_id_format_is_refused(tmp_path, spec, message):
    assert_refused(tmp_path, GOOD_MAP.replace("      id: th_\n", f"      id:{spec}\n"), message)


# ------------------------------------------------------------------------------- human templates


def test_a_human_hole_may_name_the_money_format_and_nothing_else(tmp_path):
    marked = GOOD_MAP.replace(
        "human: create thing {name}", "human: charge {amount:money} for {name}"
    )
    create = servicemaps.model.match_route(
        load_docs(tmp_path, marked).services[0], "POST", "/v1/things"
    )
    assert create is not None and create.human == "charge {amount:money} for {name}"
    assert_refused(
        tmp_path,
        GOOD_MAP.replace("human: create thing {name}", "human: create thing {name:upper}"),
        "`:upper` is not a format a hole may name",
    )


# ------------------------------------------------------------------------- credential path hosts


def test_a_map_may_declare_credential_path_hosts_of_its_own(tmp_path):
    doc = GOOD_MAP.replace("routes:", "credential_path_hosts:\n  - files.demo.example\nroutes:", 1)
    sm = load_docs(tmp_path, doc).services[0]
    assert sm.credential_path_hosts == frozenset({"files.demo.example"})
    # The code's own credential-path hosts apply on top of whatever a map declares.
    assert sm.credential_hosts == frozenset({"files.demo.example", "hooks.slack.com"})


def test_a_declared_credential_host_must_be_one_of_the_maps_hosts(tmp_path):
    doc = GOOD_MAP.replace("routes:", "credential_path_hosts:\n  - elsewhere.example\nroutes:", 1)
    assert_refused(tmp_path, doc, "'elsewhere.example' is not one of this map's exact `hosts`")


def test_a_declared_credential_host_keeps_its_targets_on_loopback(tmp_path):
    doc = GOOD_MAP.replace(
        "routes:",
        "target: http://stub.example:3000\ncredential_path_hosts:\n  - demo.example\nroutes:",
        1,
    )
    assert_refused(
        tmp_path,
        doc,
        "a target on this host may only be loopback",
        allow=frozenset({"stub.example"}),
    )


def test_an_override_may_not_touch_credential_path_hosts(tmp_path):
    override = "service: demo\ncredential_path_hosts: []\n"
    assert_refused(
        tmp_path, GOOD_MAP, "`credential_path_hosts` belongs to the shipped map", override=override
    )


def test_the_shipped_credential_hosts_are_the_floor_plus_the_maps():
    assert servicemaps.shipped_credential_hosts() >= servicemaps.model.CREDENTIAL_PATH_HOSTS
    assert "hooks.slack.com" in servicemaps.shipped_credential_hosts()


# ------------------------------------------------------ the Python tables agree with the maps


def test_the_shipped_maps_pass_the_services_check():
    services.check(servicemaps.load(cwd=None, maps_dir=servicemaps.loader.shipped_dir()))


def test_every_map_set_a_user_can_start_with_passes_the_services_check(tmp_path, monkeypatch):
    """Overrides and target flags only set targets, so they pass `services.check`."""
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "home"))
    (tmp_path / servicemaps.loader.CWD_OVERRIDE_NAME).write_text(
        "service: slack\ntarget: http://127.0.0.1:3000\n---\n"
        "service: stripe\nroutes:\n"
        "  - match: {method: POST, path: /v1/refunds}\n    target: http://127.0.0.1:3001\n"
    )
    index = servicemaps.load(
        cwd=tmp_path,
        targets=(("api.stripe.com", "", "http://127.0.0.1:3002"),),
        target_reads=("api.stripe.com",),
    )
    slack = index.service_for("slack.com")
    assert slack is not None and slack.target is not None
    assert slack.target.url == "http://127.0.0.1:3000"
    services.check(index)
    assert {sm.service for sm in index.services} >= set(services.REGISTRY)


def _shipped_text(name: str) -> str:
    return (servicemaps.loader.shipped_dir() / name).read_text()


def _check_maps(tmp_path, **replaced: tuple[str, str]):
    """Load the shipped maps with one file's text edited, and run `services.check` over them."""
    directory = tmp_path / "maps"
    directory.mkdir()
    for path in servicemaps.loader.shipped_dir().iterdir():
        if path.suffix == ".yaml":
            text = path.read_text()
            if path.name in replaced:
                old, new = replaced[path.name]
                assert old in text
                text = text.replace(old, new, 1)
            (directory / path.name).write_text(text)
    services.check(servicemaps.load(cwd=tmp_path, maps_dir=directory))


@pytest.mark.parametrize(
    ("file", "old", "new", "message"),
    [
        (
            "slack.yaml",
            "operation: chat.postMessage",
            "operation: chat.post",
            "no route names operation 'chat.postMessage', which irimi's slack tables key on",
        ),
        (
            "stripe.yaml",
            "service: stripe",
            "service: stripe2",
            "define no service 'stripe', which irimi's built-in tables key on",
        ),
        (
            "stripe.yaml",
            "precondition: charge_refundable",
            "precondition: charge_refundible",
            "`precondition: charge_refundible` names no check irimi has for stripe",
        ),
        (
            "stripe.yaml",
            "fixture: refund",
            "fixture: refnud",
            "`fixture: refnud` names no object in fixtures/stripe.json",
        ),
    ],
)
def test_the_services_check_refuses_a_name_the_tables_do_not_know(
    tmp_path, monkeypatch, file, old, new, message
):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "home"))
    with pytest.raises(MapError) as exc:
        _check_maps(tmp_path, **{file: (old, new)})
    assert message in str(exc.value)
    if "define no service" not in message:
        assert file in str(exc.value)


def test_every_operation_the_answers_key_on_is_declared_by_its_service():
    """Each service's answer tables key only on operations the service declares."""
    from irimi.services.slack import answers as slack_answers

    assert set(slack_answers.SLACK_ENVELOPES) <= services.REGISTRY["slack"].operations
    for spec in services.REGISTRY.values():
        for operation in spec.answer.literal if spec.answer is not None else ():
            assert operation in spec.operations
