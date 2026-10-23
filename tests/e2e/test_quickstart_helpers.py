"""The quickstart agent's helpers: which charge it picks, how it prints money, and how it sets
stripe-python up from the environment."""

from types import SimpleNamespace

import pytest
import stripe

from examples.quickstart import agent
from tests.support.credentials import HERMETIC_STRIPE_KEY, REFUSED_LIVE_KEY


@pytest.fixture
def stripe_settings(monkeypatch):
    """stripe-python's module-wide settings, put back after the test: `connect` sets them."""
    for name in ("api_key", "api_base", "proxy", "ca_bundle_path"):
        monkeypatch.setattr(stripe, name, getattr(stripe, name))


def _charge(**kwargs):
    base = {
        "id": "ch_1",
        "status": "succeeded",
        "paid": True,
        "amount": 4900,
        "amount_refunded": 0,
        "currency": "usd",
    }
    base.update(kwargs)
    return SimpleNamespace(**base)


def test_pick_charge_skips_unrefundable_charges():
    charges = [
        _charge(id="ch_failed", status="failed"),
        _charge(id="ch_unpaid", paid=False),
        _charge(id="ch_exhausted", amount_refunded=4900),
        _charge(id="ch_good"),
    ]
    assert agent.pick_charge(charges).id == "ch_good"


def test_pick_charge_takes_a_charge_with_any_amount_left():
    # The agent refunds whatever remains, so one minor unit left is still a charge to refund.
    charges = [
        _charge(id="ch_exhausted", amount_refunded=4900),
        _charge(id="ch_one_left", amount_refunded=4899),
    ]
    assert agent.pick_charge(charges).id == "ch_one_left"


def test_pick_charge_returns_none_when_nothing_is_refundable():
    assert agent.pick_charge([_charge(amount_refunded=4900)]) is None
    assert agent.pick_charge([]) is None


def test_money_formats_minor_units_in_their_currency():
    assert agent.money(4900, "usd") == "49.00 USD"
    assert agent.money(100, "eur") == "1.00 EUR"


def test_connect_refuses_a_key_that_is_not_test_mode(stripe_settings):
    for env in ({}, {"STRIPE_API_KEY": REFUSED_LIVE_KEY}):
        with pytest.raises(ValueError, match=r"STRIPE_API_KEY must be a test-mode key"):
            agent.connect(env)
    assert stripe.api_key != REFUSED_LIVE_KEY


def test_connect_points_stripe_python_at_irimi_only_under_irimi(stripe_settings):
    proxy = stripe.proxy
    agent.connect({"STRIPE_API_KEY": HERMETIC_STRIPE_KEY})
    assert (stripe.api_key, stripe.api_base) == (HERMETIC_STRIPE_KEY, "https://api.stripe.com")
    assert stripe.proxy == proxy

    under_irimi = {
        "STRIPE_API_KEY": HERMETIC_STRIPE_KEY,
        "STRIPE_API_BASE": "http://127.0.0.1:4100",
        "IRIMI_ENGINE_ACTIVE": "1",
        "HTTPS_PROXY": "http://127.0.0.1:4000",
        "SSL_CERT_FILE": "/home/irimi/ca/ca.pem",
    }
    agent.connect(under_irimi)
    assert stripe.api_base == "http://127.0.0.1:4100"
    assert (stripe.proxy, stripe.ca_bundle_path) == (
        "http://127.0.0.1:4000",
        "/home/irimi/ca/ca.pem",
    )
