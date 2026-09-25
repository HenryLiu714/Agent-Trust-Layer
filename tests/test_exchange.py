from irimi.exchange import Exchange, Request, Response


def _req(scheme: str = "https", port: int = 443, query: str = "") -> Request:
    return Request(
        method="GET",
        scheme=scheme,
        host="api.stripe.com",
        port=port,
        path="/v1/charges",
        query=query,
        headers=(),
        body=b"",
    )


def test_url_https_default_port_omitted():
    assert _req().url == "https://api.stripe.com/v1/charges"


def test_url_https_non_default_port_included():
    assert _req(port=8443).url == "https://api.stripe.com:8443/v1/charges"


def test_url_query_appended_only_when_non_empty():
    assert _req(query="limit=1").url == "https://api.stripe.com/v1/charges?limit=1"
    assert "?" not in _req(query="").url


def test_url_http_default_port_omitted():
    assert _req(scheme="http", port=80).url == "http://api.stripe.com/v1/charges"


def _exchange() -> Exchange:
    return Exchange(
        request=_req(),
        response=Response(status=200, headers=(), body=b""),
        service="api.stripe.com",
        operation="GET /v1/charges",
        kind="read",
        answered_by="live",
        validation="unvalidated",
        run_id="7f3a",
    )


def test_exchange_default_flags_empty():
    assert _exchange().flags == ()


def test_exchanges_from_same_args_are_equal():
    assert _exchange() == _exchange()


def test_exchange_default_door_is_forward():
    assert _exchange().door == "forward"


def test_answered_by_names_every_way_an_exchange_is_answered():
    """`delegated` is a frozen seam value, so it lands in Phase 1 even though the summary that
    counts it separately is #20 (design D20, issue #16). `fake-L1` is #41's fixture answer, and
    `fake-L0` stays beside it: L0 is the floor every route without a fixture is still answered at.
    """
    from typing import get_args

    from irimi.exchange import AnsweredBy

    assert set(get_args(AnsweredBy)) == {
        "live",
        "fake-L0",
        "fake-L1",
        "delegated",
        "overlay",
    }


def test_fake_level_is_the_locally_answered_subset_of_answered_by():
    """`echo.fake_response` returns a `FakeLevel`, which the Exchange carries as an `AnsweredBy`.
    Keeping it a subset is what lets the faker name the level without being able to say `live`."""
    from typing import get_args

    from irimi.exchange import AnsweredBy, FakeLevel

    assert set(get_args(FakeLevel)) == {"fake-L0", "fake-L1"}
    assert set(get_args(FakeLevel)) <= set(get_args(AnsweredBy))


def test_every_locally_decided_answer_has_a_fidelity_flag():
    """One mapping rather than a branch per caller. A new `answered_by` value that forgets its
    fidelity flag would print as `L0` in the summary and read as the floor it is not (#41)."""
    from typing import get_args

    from irimi.exchange import (
        FIDELITY_DELEGATED_FLAG,
        FIDELITY_FLAGS,
        FIDELITY_L0_FLAG,
        FIDELITY_L1_FLAG,
        FIDELITY_OVERLAY_FLAG,
        AnsweredBy,
    )

    assert set(FIDELITY_FLAGS) == set(get_args(AnsweredBy)) - {"live"}
    assert FIDELITY_FLAGS == {
        "fake-L0": FIDELITY_L0_FLAG,
        "fake-L1": FIDELITY_L1_FLAG,
        "delegated": FIDELITY_DELEGATED_FLAG,
        "overlay": FIDELITY_OVERLAY_FLAG,
    }


def test_target_defaults_to_empty_and_records_a_delegated_address():
    ex = _exchange()
    assert ex.target == ""
    ex.target = "http://127.0.0.1:3000/refund"
    assert ex.target == "http://127.0.0.1:3000/refund"


def test_overlay_is_not_a_fake_level():
    """`FakeLevel` is what `echo.fake_response` may return, and the overlay fakes nothing: it
    edits a real response. Keeping it out is what stops a read from being counted as a faked
    write by anything that reads the level (#43)."""
    from typing import get_args

    from irimi.exchange import FakeLevel

    assert "overlay" not in set(get_args(FakeLevel))


def test_overlay_fidelity_is_unset_until_the_overlay_considers_an_exchange():
    ex = _exchange()
    assert ex.overlay is None
    ex.overlay = "partial"
    assert ex.overlay == "partial"
