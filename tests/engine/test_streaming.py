"""Server-sent events and other streamed answers reach the client as they arrive."""

import http.client
import time

import pytest

from irimi.core.exchange import Response
from irimi.decide.overlay import Overlaid
from tests.support.engine import engine_config, start_engine, via_proxy
from tests.support.maps import READ_STREAM_MAP, STREAM_MAP, map_index
from tests.support.upstreams import SSE_FIRST, SSE_SECOND, StreamUpstream, serve


@pytest.mark.parametrize(
    ("content_type", "expected"),
    [
        ("text/event-stream", True),
        ("text/event-stream; charset=utf-8", True),
        ("Text/Event-Stream", True),
        ("application/json", False),
        ("", False),
    ],
)
def test_only_an_event_stream_content_type_is_streamed(content_type, expected):
    from irimi.engine.mitm import _is_event_stream

    assert _is_event_stream(content_type) is expected


def test_a_server_sent_event_response_reaches_the_client_in_chunks(tmp_path, monkeypatch):
    """The whole point of the `responseheaders` hook: without `flow.response.stream = True`
    mitmproxy buffers the body, so the first chunk would not arrive until the upstream finished.

    The upstream holds the second chunk until this test has read the first, and the client's
    socket timeout is far shorter than the upstream's wait, so buffering fails the test rather
    than slowing it down.
    """
    srv = serve(StreamUpstream)
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch, STREAM_MAP))
    eng, seen, stop = start_engine(cfg)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=5)
        url = f"http://127.0.0.1:{srv}/v1/chat/completions"
        conn.request(
            "POST",
            url,
            body=b"{}",
            headers={
                "host": f"127.0.0.1:{srv}",
                "content-type": "application/json",
            },
        )
        resp = conn.getresponse()
        assert resp.status == 200
        assert resp.getheader("content-type") == "text/event-stream"
        first = resp.read(len(SSE_FIRST))  # times out at 5 s if the proxy buffered the body
        assert first == SSE_FIRST
        srv.gate.set()
        assert resp.read() == SSE_SECOND
        conn.close()
    finally:
        srv.gate.set()
        stop()
        srv.stop()
    assert len(seen) == 1
    ex = seen[0]
    assert (ex.service, ex.operation, ex.kind) == ("llmhost", "chat.completions.create", "llm")
    assert ex.answered_by == "live"
    assert ex.flags == ()
    # mitmproxy never assembles a streamed body; the exchange records the chunks the agent was
    # sent, joined, and how long each was.
    assert ex.response.status == 200
    assert ex.response.body == SSE_FIRST + SSE_SECOND
    assert ex.stream_chunks == (len(SSE_FIRST), len(SSE_SECOND))


def test_a_streamed_response_ends_when_its_stream_does_not_when_its_headers_leave(
    tmp_path, monkeypatch
):
    """A streamed exchange is finished by the `response` hook, which mitmproxy runs once the
    last chunk has gone - so its `ended_at` covers the whole stream the agent waited on, and not
    only the headers `responseheaders` let through early."""
    srv = serve(StreamUpstream)
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch, STREAM_MAP))
    eng, seen, stop = start_engine(cfg)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=5)
        authority = f"127.0.0.1:{srv}"
        conn.request(
            "POST",
            f"http://{authority}/v1/chat/completions",
            body=b"{}",
            headers={"host": authority, "content-type": "application/json"},
        )
        resp = conn.getresponse()
        assert resp.read(len(SSE_FIRST)) == SSE_FIRST
        first_chunk_at = time.time()
        time.sleep(0.05)  # a gap the exchange's span has to cover if it covers the stream
        released_at = time.time()
        srv.gate.set()
        assert resp.read() == SSE_SECOND
        conn.close()
    finally:
        srv.gate.set()
        stop()
        srv.stop()
    (ex,) = seen
    assert ex.response is not None and ex.response.body == SSE_FIRST + SSE_SECOND
    assert 0 < ex.started_at <= first_chunk_at < released_at <= ex.ended_at


class _RewritingOverlay:
    """A real overlay's shape: it returns a NEW Response rather than the one it was handed.
    `NoOverlay` returns the same object, so it cannot show the addon assigning a new one."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(
        self, write_log, classification, read_request, upstream_response, translated_from=None
    ):
        self.calls.append(read_request.path)
        return Overlaid(
            Response(
                status=200,
                headers=(("content-type", "application/json"),),
                body=b'{"overlaid": true}',
            )
        )

    def rewrite(self, write_log, classification, read_request):
        return None


def test_an_overlay_is_not_handed_a_streamed_read_and_cannot_rewrite_one(tmp_path, monkeypatch):
    """`responseheaders` streams any live `text/event-stream` response, `kind: read`
    included, and mitmproxy never assembles a streamed body. An overlay called there would see
    `body == b""` and return a new Response, which the addon would then assign to the flow -
    turning a streamed read into a buffered, empty-bodied one. It presents as "reads through the
    proxy mysteriously return nothing", and only for streaming endpoints.

    Both halves are asserted, because either one alone would let the bug back in: the overlay is
    never called, and the chunks reach the client unchanged.
    """
    srv = serve(StreamUpstream)
    cfg = engine_config(
        tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch, READ_STREAM_MAP)
    )
    overlay = _RewritingOverlay()
    eng, seen, stop = start_engine(cfg, overlay=overlay)
    try:
        # A faked write first, so the overlay WOULD be asked about this read if it were not
        # streamed: with an empty write log it is never asked at all, and this test would pass
        # without the streaming guard.
        via_proxy(eng.listen_port(), "POST", f"http://127.0.0.1:{srv}/w", body=b"{}")
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=5)
        url = f"http://127.0.0.1:{srv}/v1/chat/completions"
        conn.request("GET", url, headers={"host": f"127.0.0.1:{srv}"})
        resp = conn.getresponse()
        assert resp.status == 200
        assert resp.getheader("content-type") == "text/event-stream"
        assert resp.read(len(SSE_FIRST)) == SSE_FIRST
        srv.gate.set()
        assert resp.read() == SSE_SECOND
        conn.close()
    finally:
        srv.gate.set()
        stop()
        srv.stop()
    assert overlay.calls == [], "the overlay was handed a body mitmproxy never assembled"
    _, ex = seen
    assert (ex.kind, ex.answered_by) == ("read", "live")


def test_a_json_response_is_still_buffered_and_recorded(tmp_path, monkeypatch, upstream):
    """The hook keys on the content type, so an ordinary live read is unaffected."""
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch))
    eng, seen, stop = start_engine(cfg)
    try:
        status, data = via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    finally:
        stop()
    assert (status, data) == (200, b"hello from upstream")
    assert seen[0].response.body == b"hello from upstream"
