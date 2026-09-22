import os
from pathlib import Path

import pytest

from irimi import paths, servicemap
from irimi.exchange import KINDS, LIVE_KINDS, SAFE_METHODS
from irimi.servicemap import MapError, MapIndex, Route, ServiceMap

# A complete, valid one-service document. Tests mutate a copy of this to make one thing wrong.
GOOD = """
version: 1
service: demo
verbs: honest
hosts:
  - demo.example
  - files.demo.example
routes:
  - match:
      method: GET
      path: /v1/things
    operation: things.list
    kind: read
    human: list things
  - match:
      method: GET
      path: /v1/things/{thing}
    operation: things.retrieve
    kind: read
  - match:
      method: POST
      path: /v1/things
    operation: things.create
    kind: write
    human: create thing {name}
    ids:
      id: th_
    volatile:
      - idempotency_key
  - match:
      method: POST
      path: /v1/search
    operation: things.search
    kind: read
    persists: false
    comment: search persists nothing; Stripe's own spec calls it a POST read
"""


def write_maps(tmp_path, *docs: str):
    """Write each document as its own <n>.yaml in a fresh maps directory and return it."""
    directory = tmp_path / "maps"
    directory.mkdir(exist_ok=True)
    for index, doc in enumerate(docs):
        (directory / f"{index}.yaml").write_text(doc)
    return directory


def load(tmp_path, *docs: str, allow=frozenset(), override: str | None = None):
    """Load `docs` as the shipped maps, with an optional ./irimi.maps.yaml overrides file."""
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    cwd = tmp_path / "cwd"
    cwd.mkdir(exist_ok=True)
    if override is not None:
        (cwd / servicemap.CWD_OVERRIDE_NAME).write_text(override)
    return servicemap.load(
        allow_target_hosts=allow, cwd=cwd, maps_dir=write_maps(tmp_path, *(docs or (GOOD,)))
    )


def refuses(tmp_path, doc: str, message: str, **kwargs):
    """Assert loading `doc` raises MapError whose text contains `message`."""
    with pytest.raises(MapError) as exc:
        load(tmp_path, doc, **kwargs)
    assert message in str(exc.value)


# ------------------------------------------------------------------- the shipped maps round-trip


def test_shipped_maps_load():
    index = servicemap.load(cwd=None, maps_dir=servicemap.shipped_dir())
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
    index = servicemap.load(maps_dir=servicemap.shipped_dir())
    stripe = index.service_for("api.stripe.com")
    assert stripe is not None
    assert stripe.verbs == "honest"
    assert stripe.target == servicemap.SELF_TARGET
    assert stripe.target_reads is False
    assert stripe.hosts == frozenset(
        {"api.stripe.com", "connect.stripe.com", "files.stripe.com", "meter-events.stripe.com"}
    )
    assert len(stripe.routes) == 10
    refund = servicemap.match_route(stripe, "POST", "/v1/refunds")
    assert refund is not None
    assert (refund.operation, refund.kind) == ("refunds.create", "write")
    assert refund.human == "refund {amount} on {charge}"
    assert refund.ids == {"id": "re_", "balance_transaction": "txn_"}
    assert refund.volatile == ("idempotency_key",)


def test_shipped_slack_map_is_post_only_and_owns_the_webhook_host():
    index = servicemap.load(maps_dir=servicemap.shipped_dir())
    slack = index.service_for("slack.com")
    assert slack is not None
    assert slack.verbs == "post-only"
    # One service, not two: `_check_unique` forbids the host appearing in both, and a second
    # service would split the Slack summary in two.
    assert index.service_for("hooks.slack.com") is slack
    post = servicemap.match_route(slack, "POST", "/api/chat.postMessage")
    history = servicemap.match_route(slack, "POST", "/api/conversations.history")
    assert post is not None and post.kind == "write"
    assert post.human == 'post to #{channel}: "{text}"'
    assert history is not None and history.kind == "read"


def test_no_shipped_route_downgrades_a_write_to_a_read():
    """The classification invariant: a declared write is never a read without `persists: false`.

    `_check_route_rules` refuses it at load time for a service whose verbs are honest, so a
    violation is a MapError rather than a silent downgrade; this asserts the property itself, so it
    keeps holding if the rule ever moves. A `post-only` service is exempt by design: its SDKs POST
    every call, so there is no honest verb to downgrade from.
    """
    index = servicemap.load(maps_dir=servicemap.shipped_dir())
    honest = [sm for sm in index.services if sm.verbs == "honest"]
    assert honest, "the shipped maps should still contain a service with honest verbs"
    for sm in honest:
        for route in sm.routes:
            if route.kind == "read" and route.method not in SAFE_METHODS:
                where = f"{sm.service} {route.operation}"
                assert route.persists is False, where
                assert route.comment.strip(), where


def test_slack_read_templates_still_name_the_channel():
    """An unquoted `#` starts a YAML comment, so `human: look up #{channel}` would load as
    `look up`. The three Slack reads that name a channel have to stay quoted in the file."""
    index = servicemap.load(maps_dir=servicemap.shipped_dir())
    slack = index.service_for("slack.com")
    assert slack is not None
    humans = {route.operation: route.human for route in slack.routes}
    assert humans["conversations.history"] == "read the #{channel} history"
    assert humans["conversations.replies"] == "read a thread in #{channel}"
    assert humans["conversations.info"] == "look up #{channel}"


def test_no_shipped_human_template_is_eaten_by_a_yaml_comment():
    """The class of bug the test above catches one instance of, checked against the raw files."""
    offenders = []
    for path in sorted(p for p in servicemap.shipped_dir().iterdir() if p.suffix == ".yaml"):
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
    for sm in servicemap.load(maps_dir=servicemap.shipped_dir()).services:
        assert not servicemap.is_delegated(sm), f"{sm.service} ships with a target"
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


def test_the_shipped_maps_directory_holds_every_map():
    names = sorted(p.name for p in servicemap.shipped_dir().iterdir() if p.suffix == ".yaml")
    assert names == MAP_FILE_NAMES


def test_the_shipped_maps_are_inside_a_built_wheel(tmp_path):
    """The maps are data files, and a wheel that drops them is an install that refuses to start.

    This builds one and looks inside it. Reading `shipped_dir()` instead - which is what this test
    did until #29 - reads the *source tree*, so it passes for a packaging change that ships no
    YAML at all: hatchling's default file selection honours `.gitignore`, so one `*.yaml` line
    there, or an `exclude` in pyproject, empties `irimi/maps/` in the wheel while the working
    copy still looks right. That install then fails on `irimi maps list` with the named refusal
    #21 added, which is the failure this test exists to get ahead of.

    A skip here is an environment that cannot build (no `uv`, no network for the build backend),
    not a packaging verdict: the assertion is about what is inside a wheel, so with no wheel there
    is nothing to assert. Run with `-rs` to see it.
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
        assert "irimi/servicemap/loader.py" in wheel.namelist()
    assert packaged == MAP_FILE_NAMES


# ------------------------------------------------------------------------------------ every field


def test_every_field_round_trips(tmp_path):
    index = load(tmp_path)
    sm = index.services[0]
    assert sm.service == "demo"
    assert sm.verbs == "honest"
    assert sm.hosts == frozenset({"demo.example", "files.demo.example"})
    assert sm.source.endswith("0.yaml")
    listing, retrieve, create, search = sm.routes
    assert (listing.method, listing.path, listing.operation) == ("GET", "/v1/things", "things.list")
    assert listing.human == "list things"
    assert retrieve.human == ""  # optional
    assert create.kind == "write"
    assert create.ids == {"id": "th_"}
    assert create.volatile == ("idempotency_key",)
    assert create.persists is None
    assert create.target == servicemap.SELF_TARGET
    assert create.forward_auth is False
    assert search.persists is False
    assert search.comment.startswith("search persists nothing")


def test_index_lookups(tmp_path):
    index = load(tmp_path)
    assert index.hosts == frozenset({"demo.example", "files.demo.example"})
    assert index.service_for("DEMO.EXAMPLE") is index.services[0]
    assert index.service_for("nope.example") is None
    assert index.route_for("nope.example", "GET", "/v1/things") is None
    assert index.route_for("demo.example", "GET", "/nope") is None
    found = index.route_for("demo.example", "GET", "/v1/things")
    assert found is not None and found[1].operation == "things.list"


def test_empty_index_is_usable():
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
def test_match_route_finds(tmp_path, method, path, operation):
    sm = load(tmp_path).services[0]
    route = servicemap.match_route(sm, method, path)
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
def test_match_route_misses(tmp_path, method, path):
    assert servicemap.match_route(load(tmp_path).services[0], method, path) is None


def test_literal_segment_beats_a_pattern(tmp_path):
    doc = (
        GOOD
        + """
  - match:
      method: GET
      path: /v1/things/latest
    operation: things.latest
    kind: read
"""
    )
    sm = load(tmp_path, doc).services[0]
    latest = servicemap.match_route(sm, "GET", "/v1/things/latest")
    other = servicemap.match_route(sm, "GET", "/v1/things/th_1")
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
    sm = load(tmp_path, doc).services[0]
    for method in ("GET", "POST", "PATCH"):
        route = servicemap.match_route(sm, method, "/anything")
        assert route is not None and route.operation == "demo.anything"


# ------------------------------------------------------------------------------ schema validation


@pytest.mark.parametrize(
    ("bad", "good", "message"),
    [
        ("version: 1", "version: 2", "`version` must be 1"),
        ("service: demo", "service: ''", "`service` is required"),
        ("verbs: honest", "verbs: sometimes", "`verbs` must be one of"),
        ("kind: read\n    human: list things", "kind: reed\n    human: list things", "`kind` must"),
        ("operation: things.list", "operation: things.list\n    nope: 1", "unknown route key(s)"),
        ("hosts:", "nope:\nhosts:", "unknown map key(s)"),
        ("      id: th_", "      id: ''", "must be a non-empty id prefix"),
        ("      path: /v1/things\n", "\n", "must start with"),
        ("      method: GET\n      path: /v1/things\n", "      verb: GET\n", "unknown match"),
        ("volatile:\n      - idempotency_key", "volatile: 3", "must be a list of strings"),
    ],
)
def test_schema_errors_name_the_rule(tmp_path, bad, good, message):
    assert bad in GOOD
    refuses(tmp_path, GOOD.replace(bad, good, 1), message)


def test_hosts_must_be_bare_names(tmp_path):
    refuses(tmp_path, GOOD.replace("- demo.example", "- https://demo.example"), "bare name")
    refuses(tmp_path, GOOD.replace("- demo.example", "- demo.example:443"), "bare name")


def test_hosts_must_be_a_non_empty_list(tmp_path):
    refuses(
        tmp_path,
        GOOD.replace("hosts:\n  - demo.example\n  - files.demo.example", "hosts: []"),
        "non-empty list",
    )


def test_routes_must_be_a_non_empty_list(tmp_path):
    doc = "version: 1\nservice: demo\nhosts: [demo.example]\nroutes: []\n"
    refuses(tmp_path, doc, "`routes` must be a non-empty list")


def test_a_route_cannot_appear_twice(tmp_path):
    doc = (
        GOOD
        + """
  - match:
      method: GET
      path: /v1/things
    operation: things.list.again
    kind: read
"""
    )
    refuses(tmp_path, doc, "appears twice")


def test_duplicate_service_and_host_are_refused(tmp_path):
    with pytest.raises(MapError, match="already defined"):
        load(tmp_path, GOOD, GOOD)
    other = GOOD.replace("service: demo", "service: other")
    with pytest.raises(MapError, match="already mapped"):
        load(tmp_path, GOOD, other)


def test_empty_and_invalid_files_are_refused(tmp_path):
    refuses(tmp_path, "", "file is empty")
    refuses(tmp_path, "version: 1\nservice: [", "not valid YAML")
    refuses(tmp_path, "- just a list\n", "must be a mapping")


# ------------------------------------------------------------ no write downgraded to a read (#7)


def test_unsafe_method_read_needs_persists_false_and_a_comment(tmp_path):
    """On a `verbs: honest` service, `kind: read` on any unsafe method is a downgraded write."""
    base = GOOD.replace("    persists: false\n", "").replace(
        "    comment: search persists nothing; Stripe's own spec calls it a POST read\n", ""
    )
    refuses(tmp_path, base, "downgrades a write")
    as_delete = base.replace(
        "      method: POST\n      path: /v1/search", "      method: DELETE\n      path: /v1/search"
    )
    # DELETE is caught one rule earlier now, by the live-kind rule that covers every live kind
    # rather than `read` alone - a stricter refusal of the same configuration.
    refuses(tmp_path, as_delete, "would be performed for real")


def test_persists_false_without_a_comment_is_refused(tmp_path):
    doc = GOOD.replace(
        "    comment: search persists nothing; Stripe's own spec calls it a POST read\n", ""
    )
    refuses(tmp_path, doc, "add a `comment:`")


def test_post_only_services_are_exempt(tmp_path):
    doc = """
version: 1
service: postonly
verbs: post-only
hosts: [postonly.example]
routes:
  - match:
      method: POST
      path: /api/things.list
    operation: things.list
    kind: read
    human: list things
"""
    sm = load(tmp_path, doc).services[0]
    assert sm.routes[0].kind == "read"


@pytest.mark.parametrize("kind", ["write", "unknown"])
def test_default_kind_is_accepted_for_the_kinds_that_are_answered_locally(tmp_path, kind):
    sm = load(tmp_path, GOOD.replace("verbs: honest", f"default_kind: {kind}")).services[0]
    assert sm.default_kind == kind


def test_default_kinds_is_derived_from_the_live_set(tmp_path):
    """The rule is `not forwarded live`, not a hand-kept list: a live kind added to LIVE_KINDS
    later must be refused as a default the day it is added, with no second edit here (#30)."""
    assert set(servicemap.DEFAULT_KINDS) == set(KINDS) - set(LIVE_KINDS)
    assert set(servicemap.DEFAULT_KINDS).isdisjoint(LIVE_KINDS)


def test_default_kind_defaults_to_none(tmp_path):
    assert load(tmp_path).services[0].default_kind is None


def test_default_kind_may_not_be_read(tmp_path):
    refuses(
        tmp_path,
        GOOD.replace("verbs: honest", "default_kind: read"),
        "may not be `read`",
    )


def test_default_kind_may_not_be_llm(tmp_path):
    refuses(tmp_path, GOOD.replace("verbs: honest", "default_kind: llm"), "may not be `llm`")


def test_default_kind_may_not_be_telemetry(tmp_path):
    """The bug PR #24 shipped: `telemetry` is forwarded live, and most of these vendors serve
    their REST control plane from the same host as their intake, so a telemetry default performed
    `DELETE /api/v1/dashboard/{id}` for real. Fixed in the map data then; refused here now."""
    refuses(
        tmp_path,
        GOOD.replace("verbs: honest", "default_kind: telemetry"),
        "may not be `telemetry`",
    )


@pytest.mark.parametrize("kind", LIVE_KINDS)
def test_every_live_kind_is_refused_as_a_default_with_its_own_reason(tmp_path, kind):
    """Each refusal explains itself: a generic message would not tell a map author what to write
    instead. The loop is over LIVE_KINDS so a new live kind fails here until it has a reason."""
    reason = servicemap.DEFAULT_KIND_REASONS[kind]
    refuses(tmp_path, GOOD.replace("verbs: honest", f"default_kind: {kind}"), reason)


def test_default_kind_must_be_a_kind(tmp_path):
    refuses(tmp_path, GOOD.replace("verbs: honest", "default_kind: maybe"), "must be one of")


def test_an_override_cannot_set_default_kind(tmp_path):
    """`default_kind` decides what happens to every route the map does not list, so letting a
    user's overrides file set it would be a way to reclassify writes."""
    refuses(
        tmp_path,
        GOOD,
        "unknown override key(s) default_kind",
        override="service: demo\ndefault_kind: write\n",
    )


def test_persists_and_volatile_are_restricted_to_their_kinds(tmp_path):
    refuses(
        tmp_path,
        GOOD.replace("    ids:\n      id: th_\n", "    persists: true\n"),
        "`persists` belongs on a route that is forwarded live",
    )
    refuses(
        tmp_path,
        GOOD.replace("    human: list things", "    volatile: [x]"),
        "`volatile` belongs on a write",
    )


# -------------------------------------------------------------------------- targets (design D20)


def test_default_target_is_self(tmp_path):
    sm = load(tmp_path).services[0]
    assert sm.target == servicemap.SELF_TARGET
    for route in sm.routes:
        assert servicemap.target_for(sm, route) == servicemap.SELF_TARGET
    assert servicemap.is_delegated(sm) is False


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
    doc = GOOD.replace("verbs: honest", f"verbs: honest\ntarget: {url}")
    sm = load(tmp_path, doc).services[0]
    assert sm.target == url.rstrip("/")
    assert servicemap.is_delegated(sm) is True


def test_a_service_target_answers_writes_and_unknowns_but_not_reads(tmp_path):
    doc = GOOD.replace("verbs: honest", "verbs: honest\ntarget: http://127.0.0.1:3000")
    sm = load(tmp_path, doc).services[0]
    listing, _retrieve, create, search = sm.routes
    assert servicemap.target_for(sm, create) == "http://127.0.0.1:3000"
    assert servicemap.target_for(sm, listing) == servicemap.SELF_TARGET
    assert servicemap.target_for(sm, search) == servicemap.SELF_TARGET


def test_target_reads_sends_reads_to_the_target_too(tmp_path):
    doc = GOOD.replace(
        "verbs: honest", "verbs: honest\ntarget: http://127.0.0.1:3000\ntarget_reads: true"
    )
    sm = load(tmp_path, doc).services[0]
    for route in sm.routes:
        assert servicemap.target_for(sm, route) == "http://127.0.0.1:3000"


def test_target_reads_without_a_target_is_refused(tmp_path):
    refuses(
        tmp_path,
        GOOD.replace("verbs: honest", "verbs: honest\ntarget_reads: true"),
        "would be a twin",
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
    sm = load(tmp_path, doc).services[0]
    for route in sm.routes:
        assert servicemap.target_for(sm, route) == servicemap.SELF_TARGET


def test_route_level_target_wins_over_the_service(tmp_path):
    doc = GOOD.replace("verbs: honest", "verbs: honest\ntarget: http://127.0.0.1:3000").replace(
        "    ids:\n      id: th_\n",
        "    target: http://127.0.0.1:4111/create\n    ids:\n      id: th_\n",
    )
    sm = load(tmp_path, doc).services[0]
    assert servicemap.target_for(sm, sm.routes[2]) == "http://127.0.0.1:4111/create"


def test_route_level_target_on_a_read_is_refused(tmp_path):
    doc = GOOD.replace("    human: list things", "    target: http://127.0.0.1:3000")
    refuses(tmp_path, doc, "valid on `write` and `unknown` routes only")


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
    refuses(tmp_path, GOOD.replace("verbs: honest", f"verbs: honest\ntarget: {value!r}"), message)


def test_non_loopback_targets_need_allow_target_host(tmp_path):
    doc = GOOD.replace("verbs: honest", "verbs: honest\ntarget: http://stub.example:3000")
    refuses(tmp_path, doc, "is not loopback")
    sm = load(tmp_path, doc, allow=frozenset({"stub.example"})).services[0]
    assert sm.target == "http://stub.example:3000"


def test_forward_auth_needs_a_target(tmp_path):
    doc = GOOD.replace("    ids:\n      id: th_\n", "    forward_auth: true\n")
    refuses(tmp_path, doc, "needs a target to forward to")
    ok = GOOD.replace(
        "    ids:\n      id: th_\n", "    forward_auth: true\n    target: http://127.0.0.1:3000\n"
    )
    assert load(tmp_path, ok).services[0].routes[2].forward_auth is True


def test_target_must_be_a_string_and_booleans_must_be_booleans(tmp_path):
    refuses(tmp_path, GOOD.replace("verbs: honest", "verbs: honest\ntarget: 3000"), "must be a str")
    refuses(
        tmp_path,
        GOOD.replace("verbs: honest", "verbs: honest\ntarget_reads: yes please"),
        "must be true or false",
    )


# ---------------------------------------------------------------------------- the overrides file


OVERRIDE = """
service: demo
target: http://127.0.0.1:3000
"""


def test_override_sets_a_service_target(tmp_path):
    sm = load(tmp_path, GOOD, override=OVERRIDE).services[0]
    assert sm.target == "http://127.0.0.1:3000"
    assert servicemap.target_for(sm, sm.routes[2]) == "http://127.0.0.1:3000"
    assert sm.source.endswith(servicemap.CWD_OVERRIDE_NAME)


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
    sm = load(tmp_path, GOOD, override=override).services[0]
    create = sm.routes[2]
    assert create.target == "http://127.0.0.1:3000/create"
    assert create.forward_auth is True
    assert sm.routes[0].target == servicemap.SELF_TARGET  # the other routes are untouched


def test_override_can_delegate_reads(tmp_path):
    override = OVERRIDE + "target_reads: true\n"
    sm = load(tmp_path, GOOD, override=override).services[0]
    assert sm.target_reads is True
    assert servicemap.target_for(sm, sm.routes[0]) == "http://127.0.0.1:3000"


def test_override_may_hold_several_documents(tmp_path):
    other = GOOD.replace("service: demo", "service: other").replace("demo.example", "other.example")
    override = OVERRIDE + "---\nservice: other\ntarget: http://127.0.0.1:4000\n"
    index = load(tmp_path, GOOD, other, override=override)
    assert [sm.target for sm in index.services] == [
        "http://127.0.0.1:3000",
        "http://127.0.0.1:4000",
    ]


def test_override_naming_an_unknown_service_is_an_error(tmp_path):
    with pytest.raises(MapError, match="no shipped map for service 'nope'"):
        load(tmp_path, GOOD, override="service: nope\ntarget: http://127.0.0.1:3000\n")


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
        load(tmp_path, GOOD, override=override)


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
        load(tmp_path, GOOD, override=override)


def test_override_cannot_add_hosts_or_routes(tmp_path):
    with pytest.raises(MapError, match="unknown override key"):
        load(tmp_path, GOOD, override="service: demo\nhosts: [evil.example]\n")


def test_override_target_is_validated_like_any_other(tmp_path):
    with pytest.raises(MapError, match="is not loopback"):
        load(tmp_path, GOOD, override="service: demo\ntarget: http://stub.example\n")


# --------------------------------------------------------------------- where the override lives


def test_cwd_override_wins_over_home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    cwd = tmp_path / "cwd"
    home.mkdir()
    cwd.mkdir(exist_ok=True)  # tests/conftest.py already made this one and chdir'd into it
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(home))
    (home / servicemap.HOME_OVERRIDE_NAME).write_text("service: demo\ntarget: http://127.0.0.1:1\n")
    (cwd / servicemap.CWD_OVERRIDE_NAME).write_text("service: demo\ntarget: http://127.0.0.1:2\n")
    assert servicemap.override_path(cwd) == cwd / servicemap.CWD_OVERRIDE_NAME
    index = servicemap.load(cwd=cwd, maps_dir=write_maps(tmp_path, GOOD))
    assert index.services[0].target == "http://127.0.0.1:2"


def test_home_override_is_the_fallback(tmp_path, monkeypatch):
    home = tmp_path / "home"
    cwd = tmp_path / "cwd"
    home.mkdir()
    cwd.mkdir(exist_ok=True)  # tests/conftest.py already made this one and chdir'd into it
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(home))
    (home / servicemap.HOME_OVERRIDE_NAME).write_text("service: demo\ntarget: http://127.0.0.1:1\n")
    assert servicemap.override_path(cwd) == home / servicemap.HOME_OVERRIDE_NAME
    index = servicemap.load(cwd=cwd, maps_dir=write_maps(tmp_path, GOOD))
    assert index.services[0].target == "http://127.0.0.1:1"


def test_no_override_file_is_fine(tmp_path, monkeypatch):
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "empty-home"))
    assert servicemap.override_path(tmp_path) is None


# ------------------------------------------------------------------------- the dataclasses alone


def test_route_and_servicemap_defaults():
    route = Route(method="POST", path="/x", operation="x.create", kind="write")
    assert (route.human, route.ids, route.volatile) == ("", {}, ())
    assert (route.persists, route.comment, route.forward_auth) == (None, "", False)
    sm = ServiceMap(service="x", hosts=frozenset({"x.example"}), routes=(route,))
    assert (sm.verbs, sm.target, sm.target_reads) == ("honest", servicemap.SELF_TARGET, False)
    assert sm.default_kind is None
    assert servicemap.target_for(sm, route) == servicemap.SELF_TARGET


# ------------------------------------------------- a maps directory that is missing or holds none


def test_missing_maps_directory_is_a_named_refusal(tmp_path):
    missing = tmp_path / "nope"
    with pytest.raises(MapError) as exc:
        servicemap.load_shipped(maps_dir=missing)
    assert str(missing) in str(exc.value)
    assert "cannot read the service maps directory" in str(exc.value)


def test_empty_maps_directory_is_a_named_refusal(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(MapError) as exc:
        servicemap.load_shipped(maps_dir=empty)
    assert str(empty) in str(exc.value)
    assert "no service maps found" in str(exc.value)


def test_a_directory_with_no_yaml_is_the_same_refusal(tmp_path):
    """The rule is 'no maps parsed', not 'no files present'."""
    directory = tmp_path / "not-maps"
    directory.mkdir()
    (directory / "notes.txt").write_text("not a map\n")
    with pytest.raises(MapError) as exc:
        servicemap.load_shipped(maps_dir=directory)
    assert "no service maps found" in str(exc.value)


def test_a_maps_directory_that_cannot_be_scanned_is_a_named_refusal(tmp_path, monkeypatch):
    """The same refusal as the test below, reached without a directory mode, so it also runs as
    root - in a root CI container the mode-based one skips and this path was then uncovered (#29).

    The OSError is raised from `iterdir` itself, which is where a real unreadable directory raises
    it: everything between there and the MapError is the code under test.
    """
    directory = tmp_path / "locked"
    directory.mkdir()
    (directory / "demo.yaml").write_text(GOOD)
    real_iterdir = Path.iterdir

    def refuse(self):
        if os.path.samefile(self, directory):
            raise PermissionError(13, "Permission denied")
        return real_iterdir(self)

    monkeypatch.setattr(Path, "iterdir", refuse)
    with pytest.raises(MapError) as exc:
        servicemap.load_shipped(maps_dir=directory)
    assert str(directory) in str(exc.value)
    assert "cannot read the service maps directory" in str(exc.value)
    assert "Permission denied" in str(exc.value)


def test_an_unreadable_maps_directory_is_a_named_refusal(tmp_path):
    """The same refusal against a real directory mode, which is the thing that actually happens
    on a stripped install. It cannot run as root, which is why the test above exists."""
    import os

    if os.geteuid() == 0:
        pytest.skip("root ignores the directory mode, so the scan would succeed")
    directory = tmp_path / "locked"
    directory.mkdir()
    (directory / "demo.yaml").write_text(GOOD)
    os.chmod(directory, 0o000)
    try:
        try:
            list(directory.iterdir())
        except PermissionError:
            pass
        else:
            pytest.skip("this filesystem does not honour mode 0o000 on a directory")
        with pytest.raises(MapError) as exc:
            servicemap.load_shipped(maps_dir=directory)
    finally:
        os.chmod(directory, 0o700)
    assert str(directory) in str(exc.value)
    assert "cannot read the service maps directory" in str(exc.value)


@pytest.mark.parametrize("argv", [["maps", "list"], ["serve"], ["shadow", "--", "true"]])
def test_a_missing_maps_directory_is_one_line_on_the_cli(tmp_path, monkeypatch, capsys, argv):
    """The bug this issue exists for: a stripped install printed a traceback, not a refusal."""
    from irimi.cli import main

    missing = tmp_path / "gone"
    monkeypatch.setattr(servicemap.loader, "shipped_dir", lambda: missing)
    assert main(argv) == 1
    err = capsys.readouterr().err
    assert err.startswith("error: ")
    assert str(missing) in err
    assert "Traceback" not in err
    assert err.count("\n") == 1


# ------------------------------------------------------------------------- wildcard hosts (#9)

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
    index = load(tmp_path, WILDCARD)
    assert index.service_for("a.demo.example") is index.services[0]
    assert index.service_for("a.b.demo.example") is index.services[0]
    assert index.service_for("A.DEMO.EXAMPLE") is index.services[0]


def test_a_wildcard_host_does_not_match_the_bare_domain(tmp_path):
    """`*.demo.example` claims subdomains only; the stored suffix keeps its leading dot so that
    `demo.example` itself, and a name merely ending in it, both miss."""
    index = load(tmp_path, WILDCARD)
    assert index.service_for("demo.example") is None
    assert index.service_for("notdemo.example") is None
    assert index.service_for("demo.example.evil.test") is None


def test_an_exact_host_beats_a_wildcard(tmp_path):
    index = load(tmp_path, WILDCARD, EXACT_UNDER_WILDCARD)
    one = index.service_for("one.demo.example")
    two = index.service_for("two.demo.example")
    assert one is not None and one.service == "exact"
    assert two is not None and two.service == "wild"


def test_the_longest_wildcard_wins(tmp_path):
    index = load(tmp_path, WILDCARD, DEEPER_WILDCARD)
    eu = index.service_for("a.eu.demo.example")
    us = index.service_for("a.us.demo.example")
    assert eu is not None and eu.service == "deeper"
    assert us is not None and us.service == "wild"


def test_a_wildcard_host_routes_like_any_other(tmp_path):
    index = load(tmp_path, WILDCARD)
    found = index.route_for("a.demo.example", "POST", "/ingest")
    assert found is not None and found[1].operation == "wild.ingest"


@pytest.mark.parametrize(
    "host", ["*", "*.", "*foo.demo.example", "foo.*.demo.example", "**.demo.example", "*.example"]
)
def test_a_malformed_wildcard_host_is_refused_by_name(tmp_path, host):
    refuses(
        tmp_path,
        WILDCARD.replace('"*.demo.example"', f'"{host}"'),
        "may use a wildcard only as a leading '*.' label",
    )


def test_the_same_wildcard_in_two_maps_is_a_duplicate_host(tmp_path):
    with pytest.raises(MapError) as exc:
        load(tmp_path, WILDCARD, WILDCARD.replace("service: wild", "service: wild2"))
    assert "host '*.demo.example' is already mapped by" in str(exc.value)


def test_a_wildcard_and_an_exact_host_under_it_are_not_a_duplicate(tmp_path):
    index = load(tmp_path, WILDCARD, EXACT_UNDER_WILDCARD)
    assert {sm.service for sm in index.services} == {"wild", "exact"}


def test_patterns_are_not_in_the_reverse_doors_allow_list(tmp_path):
    """A wildcard classifies through the forward door and is deliberately not an allow-list entry.

    `pipeline.rewrite_reverse` compares one literal host with `host not in allowed_hosts`, so a
    pattern in that set would never match anything. `--allow-host` is how you reach one.
    """
    index = load(tmp_path, WILDCARD, EXACT_UNDER_WILDCARD)
    assert index.hosts == frozenset({"one.demo.example"})
    assert index.patterns == ("*.demo.example",)
    assert MapIndex().patterns == ()


# ------------------------------------------------------ the conftest isolation guards itself (#29)


def test_the_conftest_redirects_irimi_home_away_from_the_developers_own(tmp_path):
    """Regression guard for the `$IRIMI_HOME` half of `tests/conftest.py`, which had none.

    Deleting its `monkeypatch.setenv` left the suite green on a clean box and failed only for a
    developer who followed the README and created `~/.irimi/maps.yaml`. This fails on every box:
    the env var is gone, so the lookup below raises, and the overrides file the loader then reads
    is the real one. Renaming the `cwd` half already fails four tests immediately (#29).
    """
    home = Path(os.environ[paths.IRIMI_HOME_ENV])
    assert home != Path.home() / ".irimi"
    assert paths.irimi_home() == home

    home.mkdir(parents=True, exist_ok=True)
    (home / servicemap.HOME_OVERRIDE_NAME).write_text(
        "service: demo\ntarget: http://127.0.0.1:3000\n"
    )
    assert servicemap.override_path(cwd=tmp_path / "empty") == home / servicemap.HOME_OVERRIDE_NAME
    assert load(tmp_path).services[0].target == "http://127.0.0.1:3000"


def test_path_params_binds_nothing_when_the_literal_segments_differ():
    """`path_params` asks `_match_path`, which is the whole reason it lives in this module. A
    segment count alone would bind `customer` to a path that shares no literal with the pattern
    - the second parser its own docstring exists to prevent."""
    assert servicemap.path_params("/v1/customers/{customer}", "/v9/charges/cus_X") == {}
    assert servicemap.path_params("/v1/customers/{customer}", "/v1/customers/cus_X") == {
        "customer": "cus_X"
    }
    assert servicemap.path_params("/v1/customers/{customer}", "/v1/customers") == {}


def test_path_params_percent_decodes_a_captured_segment():
    """The service decodes it, so we do: `cus%5FREAL123` is the same customer as `cus_REAL123`,
    and reading the raw segment made `policy.named_id` mint a fresh id for a resource the request
    already named (#33). Literal segments are still compared raw, so this cannot widen a match."""
    assert servicemap.path_params("/v1/customers/{customer}", "/v1/customers/cus%5FX") == {
        "customer": "cus_X"
    }
    assert servicemap.path_params("/v1/customers/{c}", "/v1/customers/a%20b") == {"c": "a b"}
    # A literal segment spelled with an escape does not match; decoding only reads the holes.
    assert servicemap.path_params("/v1/customers/{c}", "/v1/custom%65rs/x") == {}


def test_a_route_pattern_may_not_repeat_a_parameter_name(tmp_path):
    """`/a/{x}/b/{x}` binds `x` once and drops the first capture silently, which is how
    `named_id` would come to echo the wrong id."""
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
    refuses(tmp_path, doc, "appears more than once in the path")


# ------------------------------------------------ the --target and --target-reads flags (#16)


def _flagged(tmp_path, targets=(), target_reads=(), allow=frozenset()):
    return servicemap.load(
        allow_target_hosts=allow,
        cwd=tmp_path / "cwd",
        maps_dir=write_maps(tmp_path, GOOD),
        targets=targets,
        target_reads=target_reads,
    )


WITH_PATTERN_WRITE = (
    GOOD
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
    assert index.services[0].target == "http://127.0.0.1:3000"
    assert all(r.target == servicemap.SELF_TARGET for r in index.services[0].routes)


def test_a_host_and_path_target_sets_only_the_matching_targetable_routes(tmp_path):
    index = _flagged(tmp_path, targets=[("demo.example", "/v1/things", "http://127.0.0.1:3000")])
    targeted = [r for r in index.services[0].routes if r.target != servicemap.SELF_TARGET]
    assert [r.kind for r in targeted] == ["write"]  # the GET of the same path is left alone
    assert index.services[0].target == servicemap.SELF_TARGET


@pytest.mark.parametrize("spec", ["/v1/things/{thing}/archive", "/v1/things/th_REAL1/archive"])
def test_a_target_path_may_be_the_pattern_or_a_real_path(tmp_path, spec):
    """The caller should not have to know how the map spells its `{…}` segment, so a concrete
    request path names the route as well as the pattern does."""
    index = servicemap.load(
        cwd=tmp_path / "cwd",
        maps_dir=write_maps(tmp_path, WITH_PATTERN_WRITE),
        targets=[("demo.example", spec, "http://127.0.0.1:3000")],
    )
    targeted = [r for r in index.services[0].routes if r.target != servicemap.SELF_TARGET]
    assert [r.operation for r in targeted] == ["things.archive"]


def test_target_reads_marks_the_service_delegated(tmp_path):
    index = _flagged(
        tmp_path,
        targets=[("demo.example", "", "http://127.0.0.1:3000")],
        target_reads=["demo.example"],
    )
    assert index.services[0].target_reads is True
    assert servicemap.is_delegated(index.services[0])


def test_target_reads_without_a_target_is_still_refused_when_it_comes_from_a_flag(tmp_path):
    """The load-time rule holds however the value arrived: a service irimi answers end to end
    would be the twin the design rejects, and `--target-reads` must not be a way around it."""
    with pytest.raises(MapError) as exc:
        _flagged(tmp_path, target_reads=["demo.example"])
    assert "needs a `target:` URL" in str(exc.value)


def test_a_flag_target_is_validated_like_every_other_one(tmp_path):
    with pytest.raises(MapError) as exc:
        _flagged(tmp_path, targets=[("demo.example", "", "http://example.com")])
    assert "is not loopback" in str(exc.value)
    ok = _flagged(
        tmp_path,
        targets=[("demo.example", "", "http://example.com")],
        allow=frozenset({"example.com"}),
    )
    assert ok.services[0].target == "http://example.com"


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("http://127.0.0.1:99999", "is not a URL irimi can parse"),
        ("http://[::1/x", "is not a URL irimi can parse"),
        ("http://127.0.0.1:0", "has a bad port"),
    ],
)
def test_a_target_url_python_cannot_parse_is_a_named_refusal(tmp_path, url, message):
    """`urlsplit` and its `.port` accessor both raise ValueError on input a user can type, and a
    ValueError escaping the loader is an uncaught traceback out of `irimi serve` rather than the
    one-line refusal every other bad target gets."""
    with pytest.raises(MapError) as exc:
        _flagged(tmp_path, targets=[("demo.example", "", url)])
    assert message in str(exc.value)


def test_a_flag_naming_an_unknown_host_or_route_is_an_error(tmp_path):
    """A typo that quietly changed nothing would look exactly like a working delegation."""
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
    (cwd / servicemap.CWD_OVERRIDE_NAME).write_text(
        "service: demo\ntarget: http://127.0.0.1:1111\n"
    )
    index = _flagged(tmp_path, targets=[("demo.example", "", "http://127.0.0.1:2222")])
    assert index.services[0].target == "http://127.0.0.1:2222"


def test_a_credential_path_host_may_only_be_targeted_at_loopback():
    """A Slack incoming webhook URL is the whole credential, so `--allow-target-host` does not
    reach it: sending one off this machine hands the secret to whoever is listening (§7)."""
    index = servicemap.load(
        cwd=None,
        maps_dir=servicemap.shipped_dir(),
        targets=[("hooks.slack.com", "", "http://127.0.0.1:3000")],
    )
    assert index.service_for("hooks.slack.com").target == "http://127.0.0.1:3000"

    with pytest.raises(MapError) as exc:
        servicemap.load(
            cwd=None,
            maps_dir=servicemap.shipped_dir(),
            allow_target_hosts=frozenset({"stub.internal"}),
            targets=[("hooks.slack.com", "", "http://stub.internal:3000")],
        )
    assert "a target on this host may only be loopback" in str(exc.value)


def test_a_loopback_webhook_route_does_not_license_an_off_machine_service_target():
    """THE SCOPE RULE, host scope. The rule used to be keyed on (service, operation), so a
    loopback target on the *listed* webhook route satisfied it while a service target sent every
    *unlisted* path off the machine. `/workflows/...` and `/triggers/...` are real Slack webhook
    forms whose path is the credential just as `/services/...` is (#16 review D-2)."""
    with pytest.raises(MapError) as exc:
        servicemap.load(
            cwd=None,
            maps_dir=servicemap.shipped_dir(),
            allow_target_hosts=frozenset({"stub.internal"}),
            targets=[
                # The route-level rule is satisfied: this one really is loopback.
                ("hooks.slack.com", "/services/{team}/{bot}/{token}", "http://127.0.0.1:3000"),
                # ... and this used to delegate every path the map does not list.
                ("slack.com", "", "http://stub.internal:9000"),
            ],
        )
    assert "a target on this host may only be loopback" in str(exc.value)


def test_the_rule_follows_the_host_through_the_overrides_file_too(tmp_path):
    """Load time is the one place all three layers have already merged, so the shipped map, the
    overrides file and `--target` are all covered by the same check."""
    with pytest.raises(MapError) as exc:
        servicemap.load(
            cwd=None,
            maps_dir=servicemap.shipped_dir(),
            allow_target_hosts=frozenset({"stub.internal"}),
            targets=[("slack.com", "/api/chat.postMessage", "http://stub.internal:9000")],
        )
    assert "a target on this host may only be loopback" in str(exc.value)


def test_the_webhook_rule_covers_the_other_host_of_the_same_service():
    """Routes match per service, not per host: `slack.com/services/...` resolves to the same
    `incoming_webhook` route, so targeting the service by either host hits the rule."""
    with pytest.raises(MapError):
        servicemap.load(
            cwd=None,
            maps_dir=servicemap.shipped_dir(),
            allow_target_hosts=frozenset({"stub.internal"}),
            targets=[("slack.com", "", "http://stub.internal:3000")],
        )


# --------------------------------------------------- THE SCOPE RULE: live kinds and methods (#31)

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
    DELETE. This is #30's bug one scope down, and reachable by omission rather than by writing
    `"*"` on purpose - every shipped route happens to carry a `method:`, which is the only reason
    nothing shipped broken."""
    refuses(tmp_path, _ANY_METHOD_TELEMETRY, "may not match every method")
    # Naming the method it actually serves is all it takes.
    named = _ANY_METHOD_TELEMETRY.replace("      path:", "      method: POST\n      path:")
    assert load(tmp_path, named).service_for("probe.example") is not None


@pytest.mark.parametrize("method", sorted(servicemap.DESTRUCTIVE_METHODS))
@pytest.mark.parametrize("kind", ["read", "llm", "telemetry"])
def test_a_live_kind_on_a_destructive_method_must_justify_itself(tmp_path, method, kind):
    """Naming the verb is necessary but not sufficient: `DELETE api.datadoghq.com/api/v1/
    dashboard/{id}` classified `telemetry` is performed for real. The author has to say why it
    persists nothing, which is the same bar `kind: read` already had to clear."""
    doc = _ANY_METHOD_TELEMETRY.replace(
        "      path:", f"      method: {method}\n      path:"
    ).replace("kind: telemetry", f"kind: {kind}")
    refuses(tmp_path, doc, "would be performed for real")
    justified = doc.replace(
        "    human: send a probe event",
        "    human: send a probe event\n    persists: false\n    comment: why it is safe",
    )
    assert load(tmp_path, justified).service_for("probe.example") is not None


def test_post_is_still_the_honest_verb_for_inference_and_intake(tmp_path):
    """The rule names the destructive verbs, not every RFC-unsafe one. POST is what an LLM
    completion and a telemetry batch are, so requiring a justification for it would mean every
    shipped `llm` and `telemetry` route carrying one."""
    doc = _ANY_METHOD_TELEMETRY.replace("      path:", "      method: POST\n      path:")
    assert load(tmp_path, doc).service_for("probe.example") is not None
