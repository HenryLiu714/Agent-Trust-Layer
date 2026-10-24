"""`core.exchange`: requests, responses and exchanges, their defaults and their timing."""

from dataclasses import replace

from irimi.core.exchange import clip_chunks, media_type
from tests.support import builders


def test_a_url_leaves_out_the_https_default_port():
    assert builders.request().url == "https://api.stripe.com/v1/charges"


def test_url_https_non_default_port_included():
    assert builders.request(port=8443).url == "https://api.stripe.com:8443/v1/charges"


def test_url_query_appended_only_when_non_empty():
    assert builders.request(query="limit=1").url == "https://api.stripe.com/v1/charges?limit=1"
    assert "?" not in builders.request(query="").url


def test_a_url_leaves_out_the_http_default_port():
    assert builders.request(scheme="http", port=80).url == "http://api.stripe.com/v1/charges"


def test_header_is_found_case_insensitively_and_first_wins():
    r = replace(builders.request(), headers=(("Content-Type", "a"), ("content-type", "b")))
    assert r.header("content-type") == "a"
    assert r.header("x-absent") is None


def test_without_header_strips_every_copy_and_is_the_same_object_when_absent():
    r = replace(
        builders.request(), headers=(("irimi-rewrote", "x"), ("a", "1"), ("Irimi-Rewrote", "y"))
    )
    assert r.without_header("irimi-rewrote").headers == (("a", "1"),)
    plain = builders.request()
    assert plain.without_header("irimi-rewrote") is plain


def test_path_and_query_is_the_path_with_its_query():
    assert builders.request(query="limit=1").path_and_query == "/v1/charges?limit=1"
    assert builders.request().path_and_query == "/v1/charges"


def test_media_type_drops_parameters_and_case():
    assert media_type("Application/JSON; charset=utf-8") == "application/json"
    assert media_type("") == ""


def test_an_exchange_has_no_flags_by_default():
    assert builders.exchange().flags == ()


def test_exchanges_from_same_args_are_equal():
    assert builders.exchange() == builders.exchange()


def test_an_exchange_came_through_the_proxy_by_default():
    assert builders.exchange().door == "forward"


def test_answered_by_names_every_way_an_exchange_is_answered():
    """`AnsweredBy` names live, echo, fixture, target, overlay, recording and irimi's own errors.

    `delegated` is the on-disk value for a target's answer; `fake-L0` is the echo and `fake-L1`
    the fixture."""
    from typing import get_args

    from irimi.core.exchange import AnsweredBy

    assert set(get_args(AnsweredBy)) == {
        "live",
        "fake-L0",
        "fake-L1",
        "delegated",
        "overlay",
        "recording",
        "irimi",
    }


def test_fake_level_is_the_locally_answered_subset_of_answered_by():
    """`FakeLevel` is a subset of `AnsweredBy`, so the faker can name its level but never `live`."""
    from typing import get_args

    from irimi.core.exchange import AnsweredBy, FakeLevel

    assert set(get_args(FakeLevel)) == {"fake-L0", "fake-L1"}
    assert set(get_args(FakeLevel)) <= set(get_args(AnsweredBy))


def test_target_defaults_to_empty_and_records_a_delegated_address():
    ex = builders.exchange()
    assert ex.target == ""
    ex = replace(ex, target="http://127.0.0.1:3000/refund")
    assert ex.target == "http://127.0.0.1:3000/refund"


def test_overlay_is_not_a_fake_level():
    """`overlay` is not a `FakeLevel`: the overlay edits a real response and fakes no write."""
    from typing import get_args

    from irimi.core.exchange import FakeLevel

    assert "overlay" not in set(get_args(FakeLevel))


def test_overlay_fidelity_is_unset_until_the_overlay_considers_an_exchange():
    ex = builders.exchange()
    assert ex.overlay is None
    ex = replace(ex, overlay="partial")
    assert ex.overlay == "partial"


def test_stream_chunks_are_empty_unless_a_response_streamed():
    assert builders.exchange().stream_chunks == ()


def test_clip_chunks_keeps_the_chunks_of_a_body_s_first_bytes():
    """The store cuts a body at MAX_STORED_BODY; its chunks are cut at the same byte."""
    assert clip_chunks((3, 4, 5), 12) == (3, 4, 5)
    assert clip_chunks((3, 4, 5), 20) == (3, 4, 5)
    assert clip_chunks((3, 4, 5), 7) == (3, 4)
    assert clip_chunks((3, 4, 5), 9) == (3, 4, 2)
    assert clip_chunks((3, 4, 5), 1) == (1,)
    assert clip_chunks((3, 4, 5), 0) == ()
    assert clip_chunks((), 0) == ()


def test_clip_chunks_is_the_same_tuple_when_nothing_is_cut():
    chunks = (3, 4, 5)
    assert clip_chunks(chunks, 12) is chunks


def test_an_exchange_carries_the_callers_timestamps():
    ex = builders.exchange(response=None, started_at=1.5, ended_at=2.25)
    assert (ex.started_at, ex.ended_at) == (1.5, 2.25)
    assert builders.exchange(response=None).started_at == 0.0


def test_an_exchange_never_ends_before_it_started():
    """The wall clock can step back mid-exchange; a stored run's reader subtracts the two."""
    ex = builders.exchange(response=None, started_at=2.25, ended_at=1.5)
    assert (ex.started_at, ex.ended_at) == (2.25, 2.25)
