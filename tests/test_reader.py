"""`UpstreamReader`: irimi's own read of the real service, which never follows a redirect."""

import pytest

from irimi.core.exchange import Request
from irimi.decide import reader
from irimi.decide.reader import UpstreamReader
from tests.support.upstreams import Logged, refusing_port, serve

# ------------------------------------------------------------------ the upstream reader


class _Upstream(Logged):
    """Answers every GET with `status`, `answer` and an optional `location`, and logs each path.

    A test sets these on its own server, as `upstream.handler.status`."""

    status = 200
    answer = b'{"object": "charge"}'
    location: str | None = None

    def do_GET(self):
        self.seen.append(self.path)
        self.send_response(self.status)
        if self.location is not None:
            self.send_header("location", self.location)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(self.answer)))
        self.end_headers()
        self.wfile.write(self.answer)


@pytest.fixture
def upstream():
    port = serve(_Upstream)
    yield port
    port.stop()


def _local(port: int) -> Request:
    return Request("GET", "http", "127.0.0.1", port, "/v1/charges/ch_1", "", (), b"")


@pytest.mark.parametrize("status", [200, 429])
def test_the_upstream_reader_returns_what_the_service_answered(upstream, status, monkeypatch):
    """A 429 is information the exchange records, so it comes back as a response, not as None."""
    upstream.handler.status = status
    response = UpstreamReader()(_local(upstream))
    assert response is not None
    assert response.status == status
    assert response.body == _Upstream.answer


def test_the_upstream_reader_refuses_a_body_past_the_cap(upstream, monkeypatch):
    monkeypatch.setattr(reader, "MAX_BODY_BYTES", len(_Upstream.answer) - 1)
    assert UpstreamReader()(_local(upstream)) is None


def test_an_unreachable_upstream_is_none_and_not_a_raise():
    assert UpstreamReader()(_local(refusing_port())) is None


def test_the_upstream_reader_dials_the_upstream_whatever_proxy_the_shell_exported(
    upstream, monkeypatch
):
    """irimi's check read ignores an exported `HTTP_PROXY`/`HTTPS_PROXY` and dials the upstream.

    mitmproxy's own forwarding ignores them too, so a dead shell proxy must not break the check."""
    dead = f"http://127.0.0.1:{refusing_port()}"
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY"):
        monkeypatch.setenv(name, dead)
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
    response = UpstreamReader()(_local(upstream))
    assert response is not None
    assert (response.status, response.body) == (200, _Upstream.answer)
    assert upstream.seen == ["/v1/charges/ch_1"]


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_the_upstream_reader_returns_a_redirect_and_never_follows_it(upstream, status, monkeypatch):
    """A 3xx comes back as the answer, body and all; the host it points at is never asked."""
    upstream.handler.status = status
    upstream.handler.location = f"http://127.0.0.1:{upstream}/elsewhere"
    response = UpstreamReader()(_local(upstream))
    assert response is not None
    assert (response.status, response.body) == (status, _Upstream.answer)
    assert upstream.seen == ["/v1/charges/ch_1"]
