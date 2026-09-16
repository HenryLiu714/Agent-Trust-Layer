import pytest

from irimi.exchange import Request, Response
from irimi.policy import ShadowPolicy


def _req(method: str = "GET") -> Request:
    return Request(
        method=method,
        scheme="https",
        host="api.stripe.com",
        port=443,
        path="/v1/charges",
        query="",
        headers=(),
        body=b"",
    )


@pytest.mark.parametrize("kind", ["read", "llm", "telemetry"])
def test_shadow_forwards_live(kind):
    ans = ShadowPolicy().answer(_req(), kind)
    assert ans.answered_by == "live"
    assert ans.response is None


@pytest.mark.parametrize("kind", ["write", "unknown"])
def test_shadow_fakes_l0(kind):
    ans = ShadowPolicy().answer(_req("POST"), kind)
    assert ans.answered_by == "fake-L0"
    assert isinstance(ans.response, Response)
    assert ans.response.status == 200
    assert ("content-type", "application/json") in ans.response.headers
    assert ans.response.body == b"{}"


def test_shadow_policy_shape():
    policy = ShadowPolicy()
    assert policy.name == "shadow"
    assert callable(policy.answer)
