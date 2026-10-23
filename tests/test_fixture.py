"""`services.generic`'s fixtures: the vendored response objects the L1 faker starts from.

Everything here is about the module never raising and never handing out the object it cached.
It is read from inside a mitmproxy hook, where a raise forwards the flow and a forwarded write
escapes shadow mode, so every bad file below has to answer "no such object" instead.
"""

import json
import re

import pytest

from irimi.servicemaps.loader import load_shipped
from irimi.services import generic


def named_by_maps(service: str) -> list[str]:
    """Every object a shipped map's `fixture:` names for `service`."""
    return sorted(
        {
            r.fixture
            for sm in load_shipped()
            if sm.service == service
            for r in sm.routes
            if r.fixture
        }
    )


SHIPPED = tuple(named_by_maps("stripe"))


@pytest.fixture(autouse=True)
def clear_fixture_cache():
    """The parsed files are cached for the life of the process; these tests swap the directory."""
    generic.clear_fixture_cache()
    yield
    generic.clear_fixture_cache()


def write_fixtures(tmp_path, monkeypatch, name: str, document) -> None:
    """Point the fixtures at a directory of our own holding one `<name>.json`."""
    directory = tmp_path / "fixtures"
    directory.mkdir(exist_ok=True)
    text = document if isinstance(document, str) else json.dumps(document)
    (directory / f"{name}.json").write_text(text)
    monkeypatch.setattr(generic, "fixtures_dir", lambda: directory)
    generic.clear_fixture_cache()


# ------------------------------------------------------------------- the file that really ships


@pytest.mark.parametrize("service", ["stripe", "slack"])
def test_the_shipped_fixtures_hold_exactly_the_objects_the_maps_name(service):
    """An object no route names is one nothing answers with, and one a route names but the file
    lacks is a write degraded to L0."""
    assert sorted(generic.fixture_objects(service)) == named_by_maps(service)


def test_the_shipped_fixtures_name_where_they_came_from():
    """Vendored third-party data with no provenance is data nobody can update or re-check."""
    source = generic.fixture_source("stripe")
    assert "stripe-mock" in source and "MIT" in source
    assert "fixtures3.json" in source


# The id prefix Stripe gives each object type the shipped fixture holds, nested ones included.
STRIPE_ID_PREFIXES = {
    "account": "acct_",
    "customer": "cus_",
    "payment_intent": "pi_",
    "refund": "re_",
}


def _stripe_objects(value):
    """Every dict in `value` that names its `object` type, at any depth."""
    if isinstance(value, dict):
        if isinstance(value.get("object"), str):
            yield value
        for item in value.values():
            yield from _stripe_objects(item)
    elif isinstance(value, list):
        for item in value:
            yield from _stripe_objects(item)


def test_every_shipped_stripe_id_starts_with_its_objects_prefix():
    """A placeholder id (`obj_123`) is not the shape an agent reading the answer expects."""
    checked = [
        (obj["object"], obj["id"])
        for name in SHIPPED
        for obj in _stripe_objects(generic.fixture_object("stripe", name))
        if "id" in obj
    ]
    assert {kind for kind, _ in checked} == set(STRIPE_ID_PREFIXES)
    assert [(k, i) for k, i in checked if not i.startswith(STRIPE_ID_PREFIXES[k])] == []


def test_the_shipped_customer_has_no_discount():
    """A customer with no coupon carries `discount: null`; anything else reads as an active one."""
    customer = generic.fixture_object("stripe", "customer")
    assert customer is not None
    assert customer["discount"] is None


def test_a_shipped_object_is_the_real_shape():
    refund = generic.fixture_object("stripe", "refund")
    assert refund is not None
    assert refund["object"] == "refund"
    assert refund["status"] == "succeeded"
    # Stripe's refund object carries no `livemode`, which is why the L1 faker sets that field
    # only where the fixture already has it.
    assert "livemode" not in refund
    customer = generic.fixture_object("stripe", "customer")
    assert customer is not None
    assert "livemode" in customer


def test_the_fixtures_directory_holds_one_json_file_per_service():
    names = sorted(p.name for p in generic.fixtures_dir().iterdir())
    assert names == ["slack.json", "stripe.json"]


def test_the_shipped_slack_fixtures_hold_the_message_object():
    """chat.postMessage is the one Slack write with a payload of its own. `fixture:` on its route
    names this object and the envelope nests it under `message:`."""
    assert sorted(generic.fixture_objects("slack")) == ["message"]
    message = generic.fixture_object("slack", "message")
    assert message is not None
    assert message["type"] == "message"
    assert re.fullmatch(r"\d+\.\d{6}", message["ts"])
    # A null placeholder, not a value: the field is here so a threaded post's own `thread_ts` can
    # be reflected onto it, and `slack.answers.SlackEnvelope.optional` drops it again when none was
    # posted. A non-null value here would ship a thread the caller never named.
    assert message["thread_ts"] is None


def test_the_slack_fixtures_name_where_they_came_from():
    """Hand-written rather than vendored, which is exactly why the file has to say so: nobody can
    re-check data against an upstream it does not name."""
    source = generic.fixture_source("slack")
    assert "Hand-written" in source
    assert "api.slack.com" in source


# ---------------------------------------------------------------------- handing out a copy, not


def test_get_hands_out_a_copy_so_one_answer_cannot_change_the_next():
    """The faker writes the request's fields straight over the object it is given. Handing out
    the cached one would make every later refund carry the first caller's amount and metadata."""
    first = generic.fixture_object("stripe", "refund")
    assert first is not None
    first["amount"] = 999_999
    first["metadata"]["leaked"] = "yes"
    second = generic.fixture_object("stripe", "refund")
    assert second is not None
    assert second["amount"] != 999_999
    assert second["metadata"] == {}


def test_objects_is_keyed_by_name_and_get_returns_none_for_an_unknown_one():
    assert generic.fixture_object("stripe", "no_such_object") is None
    assert generic.fixture_object("no_such_service", "refund") is None


# ------------------------------------------------------------------ every way a file can be bad


def test_a_missing_file_is_no_objects_rather_than_a_raise(tmp_path, monkeypatch):
    monkeypatch.setattr(generic, "fixtures_dir", lambda: tmp_path / "nope")
    generic.clear_fixture_cache()
    assert generic.fixture_objects("stripe") == {}
    assert generic.fixture_object("stripe", "refund") is None
    assert generic.fixture_source("stripe") == ""


@pytest.mark.parametrize(
    "document",
    ["not json at all", "[1, 2, 3]", '"a string"', '{"resources": []}', '{"no_resources": 1}'],
)
def test_a_malformed_file_is_no_objects_rather_than_a_raise(tmp_path, monkeypatch, document):
    write_fixtures(tmp_path, monkeypatch, "demo", document)
    assert generic.fixture_objects("demo") == {}
    assert generic.fixture_object("demo", "thing") is None


def test_an_entry_that_is_not_an_object_is_skipped_and_the_rest_still_load(tmp_path, monkeypatch):
    write_fixtures(
        tmp_path, monkeypatch, "demo", {"resources": {"good": {"object": "good"}, "bad": [1]}}
    )
    assert sorted(generic.fixture_objects("demo")) == ["good"]
    assert generic.fixture_object("demo", "bad") is None


@pytest.mark.parametrize("service", ["../outside", "a/b", "", ".", "..", "sub/../outside"])
def test_a_service_name_that_is_not_a_bare_name_reads_no_file(tmp_path, monkeypatch, service):
    """`<service>.json` is a file name built from a map's `service:`, which is any non-empty
    string the loader accepts. A name spelled `../…` would otherwise choose the file that is
    read; shipped maps are the only ones that can set `fixture:` today, and this keeps that
    true of the ones added later.

    A real file is planted one directory up, so removing the guard makes this test read it
    rather than merely miss a file that was never there.
    """
    write_fixtures(tmp_path, monkeypatch, "demo", {"resources": {"thing": {"object": "thing"}}})
    (tmp_path / "outside.json").write_text(json.dumps({"resources": {"thing": {"id": "leaked"}}}))
    assert generic.fixture_objects(service) == {}
    assert generic.fixture_object(service, "thing") is None


def test_the_document_is_parsed_once_and_cached(tmp_path, monkeypatch):
    write_fixtures(tmp_path, monkeypatch, "demo", {"resources": {"thing": {"object": "thing"}}})
    assert generic.fixture_object("demo", "thing") is not None
    (tmp_path / "fixtures" / "demo.json").unlink()
    assert generic.fixture_object("demo", "thing") is not None, "the parsed file should be cached"
    generic.clear_fixture_cache()
    assert generic.fixture_object("demo", "thing") is None
