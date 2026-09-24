"""`irimi.fixture`: the vendored response objects the L1 faker starts from (#41).

Everything here is about the module never raising and never handing out the object it cached.
It is read from inside a mitmproxy hook, where a raise forwards the flow and a forwarded write
escapes shadow mode, so every bad file below has to answer "no such object" instead.
"""

import json
import re

import pytest

from irimi import fixture

SHIPPED = ("balance_transaction", "charge", "customer", "payment_intent", "refund")


@pytest.fixture(autouse=True)
def clear_fixture_cache():
    """The parsed files are cached for the life of the process; these tests swap the directory."""
    fixture.clear_cache()
    yield
    fixture.clear_cache()


def write_fixtures(tmp_path, monkeypatch, name: str, document) -> None:
    """Point `fixture` at a directory of our own holding one `<name>.json`."""
    directory = tmp_path / "fixtures"
    directory.mkdir(exist_ok=True)
    text = document if isinstance(document, str) else json.dumps(document)
    (directory / f"{name}.json").write_text(text)
    monkeypatch.setattr(fixture, "fixtures_dir", lambda: directory)
    fixture.clear_cache()


# ------------------------------------------------------------------- the file that really ships


def test_the_shipped_stripe_fixtures_hold_every_object_the_maps_name():
    assert sorted(fixture.objects("stripe")) == sorted(SHIPPED)


def test_the_shipped_fixtures_name_where_they_came_from():
    """Vendored third-party data with no provenance is data nobody can update or re-check."""
    source = fixture.source("stripe")
    assert "stripe-mock" in source and "MIT" in source
    assert "fixtures3.json" in source


def test_a_shipped_object_is_the_real_shape():
    refund = fixture.get("stripe", "refund")
    assert refund["object"] == "refund"
    assert refund["status"] == "succeeded"
    # Stripe's refund object carries no `livemode`, which is why the L1 faker sets that field
    # only where the fixture already has it.
    assert "livemode" not in refund
    assert "livemode" in fixture.get("stripe", "customer")


def test_the_fixtures_directory_holds_one_json_file_per_service():
    names = sorted(p.name for p in fixture.fixtures_dir().iterdir())
    assert names == ["slack.json", "stripe.json"]


def test_the_shipped_slack_fixtures_hold_the_message_object():
    """chat.postMessage is the one Slack write with a payload of its own. `fixture:` on its route
    names this object and the envelope nests it under `message:` (#42)."""
    assert sorted(fixture.objects("slack")) == ["message"]
    message = fixture.get("slack", "message")
    assert message["type"] == "message"
    assert re.fullmatch(r"\d+\.\d{6}", message["ts"])


def test_the_slack_fixtures_name_where_they_came_from():
    """Hand-written rather than vendored, which is exactly why the file has to say so: nobody can
    re-check data against an upstream it does not name."""
    source = fixture.source("slack")
    assert "Hand-written" in source
    assert "api.slack.com" in source


# ---------------------------------------------------------------------- handing out a copy, not


def test_get_hands_out_a_copy_so_one_answer_cannot_change_the_next():
    """The faker writes the request's fields straight over the object it is given. Handing out
    the cached one would make every later refund carry the first caller's amount and metadata."""
    first = fixture.get("stripe", "refund")
    first["amount"] = 999_999
    first["metadata"]["leaked"] = "yes"
    second = fixture.get("stripe", "refund")
    assert second["amount"] != 999_999
    assert second["metadata"] == {}


def test_objects_is_keyed_by_name_and_get_returns_none_for_an_unknown_one():
    assert fixture.get("stripe", "no_such_object") is None
    assert fixture.get("no_such_service", "refund") is None


# ------------------------------------------------------------------ every way a file can be bad


def test_a_missing_file_is_no_objects_rather_than_a_raise(tmp_path, monkeypatch):
    monkeypatch.setattr(fixture, "fixtures_dir", lambda: tmp_path / "nope")
    fixture.clear_cache()
    assert fixture.objects("stripe") == {}
    assert fixture.get("stripe", "refund") is None
    assert fixture.source("stripe") == ""


@pytest.mark.parametrize(
    "document",
    ["not json at all", "[1, 2, 3]", '"a string"', '{"resources": []}', '{"no_resources": 1}'],
)
def test_a_malformed_file_is_no_objects_rather_than_a_raise(tmp_path, monkeypatch, document):
    write_fixtures(tmp_path, monkeypatch, "demo", document)
    assert fixture.objects("demo") == {}
    assert fixture.get("demo", "thing") is None


def test_an_entry_that_is_not_an_object_is_skipped_and_the_rest_still_load(tmp_path, monkeypatch):
    write_fixtures(
        tmp_path, monkeypatch, "demo", {"resources": {"good": {"object": "good"}, "bad": [1]}}
    )
    assert sorted(fixture.objects("demo")) == ["good"]
    assert fixture.get("demo", "bad") is None


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
    assert fixture.objects(service) == {}
    assert fixture.get(service, "thing") is None


def test_the_document_is_parsed_once_and_cached(tmp_path, monkeypatch):
    write_fixtures(tmp_path, monkeypatch, "demo", {"resources": {"thing": {"object": "thing"}}})
    assert fixture.get("demo", "thing") is not None
    (tmp_path / "fixtures" / "demo.json").unlink()
    assert fixture.get("demo", "thing") is not None, "the parsed file should be cached"
    fixture.clear_cache()
    assert fixture.get("demo", "thing") is None
