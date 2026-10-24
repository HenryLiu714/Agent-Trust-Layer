"""`engine.ingress`: telling a base-URL request from a proxied one, and rewriting it."""

from dataclasses import replace

import pytest

from irimi.core.exchange import Request
from irimi.engine.ingress import BaseUrlRefused, detect_door, rewrite_base_url
from tests.support import builders
from tests.support.maps import index_from

ALLOWED = frozenset({"api.stripe.com", "127.0.0.1"})


def _door_req(
    path: str, host: str = "localhost", port: int = 4000, query: str = "", method: str = "GET"
) -> Request:
    """A base-URL request: plain http, addressed to the listener, host header set."""
    return Request(
        method=method,
        scheme="http",
        host=host,
        port=port,
        path=path,
        query=query,
        headers=(("host", f"{host}:{port}"), ("accept", "*/*")),
        body=b"",
    )


@pytest.mark.parametrize(
    "host",
    [
        "localhost",
        "localhost.",
        "127.0.0.1",
        "127.0.0.2",
        "127.1",
        "::1",
        "::ffff:127.0.0.1",
        "0.0.0.0",
        "::",
    ],
)
def test_a_request_to_a_loopback_listener_is_base_url_mode(host):
    assert detect_door(_door_req("/api.stripe.com/v1", host=host), 4000) == "reverse"


def test_an_unknown_name_on_our_own_port_resolving_here_is_base_url_mode(monkeypatch):
    import socket

    from irimi.core import netaddr

    def fake_getaddrinfo(host, port, **kwargs):
        if host == "self.test":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 0))]
        raise socket.gaierror("no such host")

    monkeypatch.setattr(netaddr.socket, "getaddrinfo", fake_getaddrinfo)  # type: ignore[attr-defined]
    assert detect_door(_door_req("/api.stripe.com/v1", host="self.test"), 4000) == "reverse"
    assert detect_door(_door_req("/api.stripe.com/v1", host="nowhere.test"), 4000) == "forward"
    # Other ports never resolve anything.
    assert detect_door(_door_req("/x", host="self.test", port=4001), 4000) == "forward"


def test_the_address_a_connection_arrived_on_is_this_machine(monkeypatch):
    """The address a connection arrived on, or a name resolving to it, is base-URL mode.

    Either spelling a dual-stack socket reports counts. Another address, another port, or no local
    address means the request came through the proxy."""
    import socket

    from irimi.core import netaddr

    def fake_getaddrinfo(host, port, **kwargs):
        if host == "irimi":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("172.18.0.3", 0))]
        raise socket.gaierror("no such host")

    monkeypatch.setattr(netaddr.socket, "getaddrinfo", fake_getaddrinfo)  # type: ignore[attr-defined]
    for host in ("172.18.0.3", "irimi"):
        req = _door_req("/api.stripe.com/v1", host=host)
        assert detect_door(req, 4000, "172.18.0.3") == "reverse", host
        assert detect_door(req, 4000, "::ffff:172.18.0.3") == "reverse", host
        assert detect_door(req, 4000) == "forward", host
        assert detect_door(req, 4000, "172.18.0.4") == "forward", host
        assert detect_door(_door_req("/x", host=host, port=4001), 4000, "172.18.0.3") == "forward"


def test_a_request_for_another_port_came_through_the_proxy():
    assert detect_door(_door_req("/api.stripe.com/v1", port=4001), 4000) == "forward"


def test_a_request_for_a_real_host_came_through_the_proxy():
    assert detect_door(builders.request(), 4000) == "forward"


def test_a_base_url_path_is_rewritten_to_the_service_it_names():
    req = rewrite_base_url(_door_req("/api.stripe.com/v1/charges", query="limit=1"), ALLOWED)
    assert req.scheme == "https"
    assert req.host == "api.stripe.com"
    assert req.port == 443
    assert req.path == "/v1/charges"
    assert req.query == "limit=1"
    assert req.method == "GET"
    assert req.body == b""
    assert req.url == "https://api.stripe.com/v1/charges?limit=1"


def test_a_base_url_rewrite_sets_the_host_header():
    req = rewrite_base_url(_door_req("/api.stripe.com/v1/charges", query="limit=1"), ALLOWED)
    assert ("host", "api.stripe.com") in req.headers
    assert ("accept", "*/*") in req.headers
    assert not any("localhost" in value for _, value in req.headers)


def test_a_base_url_rewrite_adds_a_missing_host_header():
    bare = replace(_door_req("/api.stripe.com/v1"), headers=(("accept", "*/*"),))
    assert ("host", "api.stripe.com") in rewrite_base_url(bare, ALLOWED).headers
    bare = replace(_door_req("/127.0.0.1:8443/v1"), headers=())
    assert rewrite_base_url(bare, ALLOWED).headers == (("host", "127.0.0.1:8443"),)


def test_a_base_url_rewrite_refuses_an_ipv6_literal():
    with pytest.raises(BaseUrlRefused, match="IPv6"):
        rewrite_base_url(_door_req("/[::1]:8443/x"), ALLOWED | {"::1"})


def test_a_base_url_rewrite_lower_cases_the_host():
    assert rewrite_base_url(_door_req("/API.Stripe.COM/v1"), ALLOWED).host == "api.stripe.com"


def test_a_base_url_rewrite_keeps_an_explicit_port():
    req = rewrite_base_url(_door_req("/127.0.0.1:8443/hello"), ALLOWED)
    assert req.host == "127.0.0.1"
    assert req.port == 8443
    assert req.path == "/hello"
    assert ("host", "127.0.0.1:8443") in req.headers


@pytest.mark.parametrize("path", ["/api.stripe.com", "/api.stripe.com/"])
def test_a_base_url_rewrite_of_a_bare_host_is_the_root(path):
    assert rewrite_base_url(_door_req(path), ALLOWED).path == "/"


def test_a_base_url_rewrite_keeps_a_deeper_path():
    req = rewrite_base_url(_door_req("/api.stripe.com/v1/charges/ch_1/refunds"), ALLOWED)
    assert req.path == "/v1/charges/ch_1/refunds"


def test_a_base_url_rewrite_keeps_the_method_and_body():
    door = _door_req("/api.stripe.com/v1/refunds", method="POST")
    req = rewrite_base_url(replace(door, body=b"charge=ch_1"), ALLOWED)
    assert req.method == "POST"
    assert req.body == b"charge=ch_1"


def test_a_base_url_rewrite_refuses_an_unlisted_host():
    with pytest.raises(BaseUrlRefused) as exc:
        rewrite_base_url(_door_req("/evil.example/x"), ALLOWED)
    assert "evil.example" in str(exc.value)
    assert "--base-url-host" in str(exc.value)


def test_a_base_url_rewrite_refuses_a_missing_host():
    with pytest.raises(BaseUrlRefused) as exc:
        rewrite_base_url(_door_req("/"), ALLOWED)
    assert "/<upstream-host>/<path>" in str(exc.value)


@pytest.mark.parametrize(
    "path", ["/api.stripe.com:abc/v1", "/api.stripe.com:0/v1", "/api.stripe.com:70000/v1"]
)
def test_a_base_url_rewrite_refuses_a_bad_port(path):
    with pytest.raises(BaseUrlRefused) as exc:
        rewrite_base_url(_door_req(path), ALLOWED)
    assert "bad port" in str(exc.value)


def test_a_base_url_rewrite_with_no_hosts_refuses_everything():
    with pytest.raises(BaseUrlRefused):
        rewrite_base_url(_door_req("/api.stripe.com/v1"), frozenset())


def test_a_telemetry_wildcard_is_not_a_base_url_host(tmp_path, monkeypatch):
    """A wildcard host classifies only through the proxy; base-URL mode needs `--base-url-host`."""
    index = index_from(tmp_path, monkeypatch)
    assert "*.ingest.sentry.io" not in index.hosts
    assert "o0.ingest.sentry.io" not in index.hosts
    with pytest.raises(BaseUrlRefused):
        rewrite_base_url(_door_req("/o0.ingest.sentry.io/api/7/envelope/"), index.hosts)
