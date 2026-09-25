"""Unit tests for the example refund agent's pure helpers.

The agent is loaded by path, not imported as a package: `examples/` is not on sys.path and is not
part of the wheel. It must import cleanly in the dev venv, where `stripe` is not installed - which
is why `agent.py` imports stripe inside its functions.
"""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

AGENT_PATH = Path(__file__).resolve().parent.parent / "examples" / "refund_agent" / "agent.py"


def _load_agent():
    spec = importlib.util.spec_from_file_location("refund_agent_fixture", AGENT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


agent = _load_agent()


def test_door_base_none_without_engine():
    assert agent.door_base({"HTTPS_PROXY": "http://127.0.0.1:4000"}, "api.stripe.com") is None


def test_door_base_none_without_proxy():
    assert agent.door_base({"IRIMI_ENGINE_ACTIVE": "1"}, "api.stripe.com") is None


def test_door_base_builds_the_door_url():
    env = {"IRIMI_ENGINE_ACTIVE": "1", "HTTPS_PROXY": "http://127.0.0.1:4717"}
    assert agent.door_base(env, "api.stripe.com") == "http://127.0.0.1:4717/api.stripe.com"


def test_door_base_honours_lowercase_proxy_and_trailing_slash():
    env = {"IRIMI_ENGINE_ACTIVE": "1", "https_proxy": "http://127.0.0.1:4717/"}
    assert agent.door_base(env, "api.stripe.com") == "http://127.0.0.1:4717/api.stripe.com"


def test_field_returns_none_for_a_missing_attribute():
    class Empty:
        def __getattr__(self, name):
            raise AttributeError(name)

    assert agent.field(Empty(), "id") is None
    assert agent.field(SimpleNamespace(id="re_123"), "id") == "re_123"


def _charge(**kwargs):
    base = {
        "id": "ch_1",
        "status": "succeeded",
        "paid": True,
        "refunded": False,
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
        _charge(id="ch_refunded", refunded=True),
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
    assert agent.pick_charge([_charge(refunded=True)]) is None
    assert agent.pick_charge([_charge(amount_refunded=4900)]) is None


def test_money_formats_minor_units():
    assert agent.money(4900, "usd") == "49.00 USD"
    assert agent.money(100, "eur") == "1.00 EUR"
