"""`bodies`: the fields a request body holds, read from JSON and from forms."""

import json

import pytest

from irimi.core import bodies
from tests.support.decisions import answer_shipped, make_request


def test_reflect_reads_a_json_object():
    assert bodies.reflect(make_request(body=b'{"a": 1}', content_type="application/json")) == {
        "a": 1
    }


def test_reflect_honours_content_type_parameters():
    req = make_request(body=b'{"a": 1}', content_type="application/json; charset=utf-8")
    assert bodies.reflect(req) == {"a": 1}


@pytest.mark.parametrize(
    "body",
    [b"[1, 2]", b'"a string"', b"null", b"7", b"{not json", b"", b"\xff\xfe\x00bad"],
)
def test_reflect_returns_nothing_for_a_body_that_is_not_a_json_object(body):
    assert bodies.reflect(make_request(body=body, content_type="application/json")) == {}


@pytest.mark.parametrize("body", [b'{"a": NaN}', b'{"a": Infinity}', b'{"a": 1e400}'])
def test_reflect_refuses_a_number_a_strict_json_parser_would_refuse(body):
    """json.dumps writes NaN/Infinity straight back out; the echo has to stay parseable."""
    assert bodies.reflect(make_request(body=body, content_type="application/json")) == {}


def test_reflect_reads_a_form_body():
    req = make_request(body=b"a=1&b=two&c=", content_type="application/x-www-form-urlencoded")
    assert bodies.reflect(req) == {"a": 1, "b": "two", "c": ""}


@pytest.mark.parametrize("value", [b"007", b"0012345", b"000123456789012345678", b"1e3", b"-1"])
def test_reflect_leaves_a_non_canonical_number_alone(value):
    req = make_request(body=b"v=" + value, content_type="application/x-www-form-urlencoded")
    assert bodies.reflect(req)["v"] == value.decode()


def test_reflect_leaves_a_slack_timestamp_alone():
    req = make_request(
        body=b"ts=1700000000.000600", content_type="application/x-www-form-urlencoded"
    )
    assert bodies.reflect(req)["ts"] == "1700000000.000600"


def test_reflect_leaves_a_number_too_wide_for_a_double_alone():
    req = make_request(body=b"n=1234567890123456", content_type="application/x-www-form-urlencoded")
    assert bodies.reflect(req)["n"] == "1234567890123456"


def test_reflect_reads_a_form_body_that_is_not_utf8():
    req = make_request(body=b"a=\xff\xfe", content_type="application/x-www-form-urlencoded")
    assert isinstance(bodies.reflect(req)["a"], str)


@pytest.mark.parametrize("ct", [None, "text/plain", "multipart/form-data; boundary=x", ""])
def test_reflect_ignores_every_other_content_type(ct):
    assert bodies.reflect(make_request(body=b'{"a": 1}', content_type=ct)) == {}


def test_reflect_never_raises_on_a_big_body():
    body = json.dumps({"k": "x" * 2_000_000}).encode()
    assert bodies.reflect(make_request(body=body, content_type="application/json"))["k"].startswith(
        "x"
    )


def test_reflect_never_raises_on_a_deeply_nested_body():
    assert bodies.reflect(make_request(body=b"[" * 5000, content_type="application/json")) == {}


def test_an_answer_for_an_unparseable_body_is_still_json():
    ans = answer_shipped(make_request("POST", host="example.invalid", path="/x", body=b"\xff" * 64))
    assert json.loads(ans.response.body) == {}


# ------------------------------------------------- form bodies with structure in them


def test_a_bracket_nested_form_field_becomes_a_nested_object():
    """stripe-python's `metadata[order_id]=6735` echoes as a nested `metadata` object.

    The live API always answers with `metadata`, so a flat echo makes `Refund.metadata` raise."""
    body = bodies.parse_form("charge=ch_test&amount=4900&metadata[order_id]=6735")
    assert body == {"charge": "ch_test", "amount": 4900, "metadata": {"order_id": "6735"}}


def test_a_metadata_value_stays_a_string_at_any_depth():
    """A metadata value stays a string at any depth: the field name decides, wherever it sits."""
    assert bodies.parse_form("metadata[n]=6735")["metadata"]["n"] == "6735"
    assert bodies.parse_form("a[metadata][n]=6735")["a"]["metadata"]["n"] == "6735"


def test_a_nested_number_is_a_number_like_the_same_number_at_the_top_level():
    """A nested numeric field is coerced like a top-level one; only `metadata` stays a string.

    An uncoerced `quantity` makes `quantity * 2` evaluate to `"22"` without raising."""
    assert bodies.parse_form("n=6735")["n"] == 6735
    body = bodies.parse_form("line_items[0][quantity]=2&line_items[0][price]=price_1")
    assert body == {"line_items": [{"quantity": 2, "price": "price_1"}]}
    assert bodies.parse_form("expand[]=2")["expand"] == [2]
    # The same rules the top level has: not canonical, so not a number.
    assert bodies.parse_form("a[b]=007")["a"]["b"] == "007"


def test_a_repeated_bare_key_collects_into_a_list():
    """`requests.post(data={"tags": ["a", "b"]})` and `urlencode(doseq=True)` both send these."""
    assert bodies.parse_form("tags=a&tags=b&tags=c") == {"tags": ["a", "b", "c"]}
    assert bodies.parse_form("tags=a") == {"tags": "a"}


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("expand[0]=a&expand[1]=b", {"expand": ["a", "b"]}),
        ("i[0][price]=p1&i[1][price]=p2", {"i": [{"price": "p1"}, {"price": "p2"}]}),
        ("a[b][c]=1", {"a": {"b": {"c": 1}}}),
        ("x[1]=only", {"x": {"1": "only"}}),  # a gap stays a dict; nothing is invented
        ("x[0]=a&x[2]=c", {"x": {"0": "a", "2": "c"}}),
        ("x[007]=a", {"x": {"007": "a"}}),  # not a canonical index, so not a list
    ],
)
def test_an_indexed_form_key_becomes_a_list_only_when_the_indices_are_complete(text, expected):
    assert bodies.parse_form(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("a[b", {"a[b": ""}),
        ("a[b]c", {"a[b]c": ""}),
        ("a[[b]]", {"a[[b]]": ""}),
        ("a[][b]=1", {"a[][b]": 1}),  # an append in the middle of a path means nothing
        ("a[b][]=1", {"a[b][]": 1}),
        ("[b]=1", {"[b]": 1}),
    ],
)
def test_a_bracket_shape_we_will_not_guess_at_stays_flat(text, expected):
    """An odd bracket shape echoes unchanged rather than guessed at.

    The whole dict is asserted: a membership check would pass on an empty result too."""
    assert bodies.parse_form(text) == expected


@pytest.mark.parametrize(
    "text", ["a=2&a[0]=1", "a[0]=1&a=2", "a[0]=1&a=2&a[1]=3", "a=2&a[0]=1&a=9"]
)
def test_a_bracketed_key_beats_a_bare_one_in_either_order(text):
    """A bracketed key beats a bare one of the same name, whichever arrives first."""
    result = bodies.parse_form(text)
    assert isinstance(result["a"], list), result
    assert 1 in result["a"]


@pytest.mark.parametrize("text", ["a[0][b]=1&a[0]=2", "a[0]=2&a[0][b]=1"])
def test_a_bracket_path_beats_a_scalar_at_the_same_leaf_in_either_order(text):
    """A deeper bracket path beats a scalar at the same leaf, whichever arrives first."""
    assert bodies.parse_form(text) == {"a": [{"b": 1}]}


@pytest.mark.parametrize("text", ["a[]=1&a[0][b]=2", "a[0][b]=2&a[]=1"])
def test_a_bracket_path_beats_an_append_of_the_same_name_in_either_order(text):
    assert bodies.parse_form(text) == {"a": [{"b": 2}]}


@pytest.mark.parametrize("text", ["a[]=1&a=2", "a=2&a[]=1"])
def test_an_append_beats_a_bare_key_of_the_same_name_in_either_order(text):
    assert bodies.parse_form(text) == {"a": [1]}


def test_an_empty_bracket_pair_is_a_list(tmp_path):
    """An empty bracket pair is a list, with one repeat or none.

    `expand[]=a&expand[]=b` is Stripe's documented curl spelling; a literal `"expand[]"` key is a
    field no SDK looks for."""
    assert bodies.parse_form("expand[]=a&expand[]=b") == {"expand": ["a", "b"]}
    assert bodies.parse_form("expand[]=a") == {"expand": ["a"]}
    assert bodies.parse_form("charge=ch_1&expand[]=a") == {"charge": "ch_1", "expand": ["a"]}
    # `metadata` is still the caller's to key and to spell, at this shape too.
    assert bodies.parse_form("metadata[]=6735") == {"metadata": ["6735"]}


def _nest(depth: int):
    node: object = 1
    for _ in range(depth):
        node = {"b": node}
    return node


def test_a_bracket_path_deeper_than_the_cap_stays_flat():
    """A bracket path deeper than the cap stays flat instead of recursing inside a hook.

    A RecursionError in a mitmproxy hook would break the fail-closed hook rule. The cap is twice
    Stripe's deepest real key, `line_items[0][price_data][product_data][name]`."""
    # Pin the value too: a cap of 2 would flatten Stripe's real 5-segment key and still pass.
    assert bodies._MAX_FORM_DEPTH == 8
    assert bodies.parse_form("line_items[0][price_data][product_data][name]=x") == {
        "line_items": [{"price_data": {"product_data": {"name": "x"}}}]
    }
    deep = "a" + "[b]" * bodies._MAX_FORM_DEPTH
    assert bodies.parse_form(deep + "=1") == {"a": _nest(bodies._MAX_FORM_DEPTH)}
    too_deep = "a" + "[b]" * (bodies._MAX_FORM_DEPTH + 1)
    assert bodies.parse_form(too_deep + "=1") == {too_deep: 1}  # flat, so int-coerced


def test_parse_form_never_raises_on_hostile_input():
    for text in ["[" * 5000, "a" + "[b]" * 2000 + "=1", "=", "&&&", "a=%%%", "a[0]=1&a=2"]:
        assert isinstance(bodies.parse_form(text), dict)


def test_a_hostile_form_body_still_reflects_and_never_raises():
    """Whatever the form body, irimi answers the write itself."""
    request = make_request(
        "POST",
        path="/v1/refunds",
        body=("a" + "[b]" * 2000 + "=1").encode(),
        content_type="application/x-www-form-urlencoded",
    )
    ans = answer_shipped(request)
    assert ans.answered_by == "fake-L1"
    assert json.loads(ans.response.body)["id"].startswith("re_")
