"""A streamed answer is recorded as its chunks, copied off the stream without slowing it."""

import gzip
import http.client
import itertools
import threading

import pytest
from mitmproxy.net import encoding

from irimi import services
from irimi.core.exchange import Exchange, Flag
from irimi.core.limits import MAX_STORED_BODY
from irimi.engine import capture
from irimi.engine.capture import StreamCapture
from irimi.record import redact
from irimi.record.store.reader import StoreReader
from irimi.record.store.writer import DirectoryStore
from tests.support.credentials import TORN_STREAM_SECRET
from tests.support.engine import (
    THREE_CHUNKS,
    engine_config,
    leaf_cert_for_loopback,
    open_stream,
    recorded,
    start_engine,
    stream_server,
    streamed_flow,
)
from tests.support.maps import READ_STREAM_MAP, STREAM_MAP, map_index
from tests.support.servers import stop_server
from tests.support.upstreams import SSE_FIRST, SSE_SECOND, StreamUpstream, serve


def _stream_three_chunks(tmp_path, monkeypatch, store=None):
    """THREE_CHUNKS through the proxy, each held by the upstream until the client has read the
    one before, so the client provably gets each chunk before the next is sent. The exchanges."""
    gates = [threading.Event(), threading.Event()]
    srv = stream_server(THREE_CHUNKS, gates)
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch, STREAM_MAP))
    eng, seen, stop = start_engine(cfg, store=store)
    try:
        conn, resp = open_stream(eng, srv)
        assert resp.status == 200
        for chunk, gate in zip(THREE_CHUNKS, [*gates, None], strict=True):
            assert resp.read(len(chunk)) == chunk  # times out at 5 s if the proxy buffered it
            if gate is not None:
                gate.set()
        assert resp.read() == b""
        conn.close()
    finally:
        for gate in gates:
            gate.set()
        stop()
        stop_server(srv)
    return seen


def test_a_streamed_response_is_recorded_as_its_chunks_joined_with_each_chunk_s_length(
    tmp_path, monkeypatch
):
    """The agent gets each chunk as it arrives, and the exchange holds the whole stream - its
    body the chunks joined, `stream_chunks` their lengths in arrival order."""
    (ex,) = _stream_three_chunks(tmp_path, monkeypatch)
    assert (ex.kind, ex.answered_by, ex.flags) == ("llm", "live", ())
    assert ex.response is not None and ex.response.body == b"".join(THREE_CHUNKS)
    assert ex.stream_chunks == tuple(len(chunk) for chunk in THREE_CHUNKS)


def test_a_streamed_response_is_stored_and_read_back_with_its_body_and_chunks(
    tmp_path, monkeypatch
):
    store = DirectoryStore(tmp_path / "store", bytes(32))
    (ex,) = _stream_three_chunks(tmp_path, monkeypatch, store=store)
    assert store.stats().dropped == 0
    (stored,) = StoreReader(tmp_path / "store").load_run("t3st").events
    assert isinstance(stored, Exchange) and stored.response is not None
    assert stored.response.body == b"".join(THREE_CHUNKS)
    assert stored.stream_chunks == ex.stream_chunks == tuple(len(c) for c in THREE_CHUNKS)


def test_a_stream_longer_than_the_stored_maximum_reaches_the_client_whole(tmp_path, monkeypatch):
    """The copy stops at MAX_STORED_BODY and the stream does not: the client gets all 9 MiB, and
    the exchange keeps the first 8 MiB and says it is not the whole stream. Each MiB is 1024
    lines, so the cap falls at the end of a line and the whole 8 MiB is kept."""
    mib = 1024 * 1024
    chunks = [(bytes([ord("a") + i]) * 1023 + b"\n") * 1024 for i in range(9)]
    srv = stream_server(chunks)
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch, STREAM_MAP))
    eng, seen, stop = start_engine(cfg)
    try:
        conn, resp = open_stream(eng, srv)
        received = resp.read()
        conn.close()
    finally:
        stop()
        stop_server(srv)
    assert received == b"".join(chunks)
    (ex,) = seen
    assert ex.flags == (Flag.STREAM_TRUNCATED,)
    assert ex.response is not None and ex.response.body == received[:MAX_STORED_BODY]
    assert sum(ex.stream_chunks) == MAX_STORED_BODY == 8 * mib


def test_a_copy_that_raises_stops_the_recording_and_never_the_stream(tmp_path, monkeypatch):
    """The tee's own copy raises on the second chunk. The client still gets every byte, the
    recording keeps what it copied before, and says it is not the whole stream."""
    calls = itertools.count(1)
    capture = StreamCapture._capture

    def raise_on_the_second(self, chunk):
        if next(calls) == 2:
            raise RuntimeError("the copy broke")
        capture(self, chunk)

    monkeypatch.setattr(StreamCapture, "_capture", raise_on_the_second)
    (ex,) = _stream_three_chunks(tmp_path, monkeypatch)
    assert ex.flags == (Flag.STREAM_TRUNCATED,)
    assert ex.response is not None and ex.response.body == THREE_CHUNKS[0]
    assert ex.stream_chunks == (len(THREE_CHUNKS[0]),)


def test_an_upstream_reset_mid_stream_is_recorded_with_what_arrived_and_flagged(
    tmp_path, monkeypatch
):
    """A chunked stream that loses its upstream after the first chunk ends in the `error` hook.
    The agent already has the headers and that chunk, so the exchange keeps them, and says both
    that the upstream failed and that the recorded body is not the whole stream."""
    # Reset only once the client has the first chunk: a reset that overtakes data the proxy has
    # not read yet takes that data with it.
    gate = threading.Event()
    srv = stream_server(THREE_CHUNKS[:1], [gate], reset=True)
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch, STREAM_MAP))
    eng, seen, stop = start_engine(cfg)
    try:
        conn, resp = open_stream(eng, srv)
        assert resp.status == 200
        assert resp.read(len(THREE_CHUNKS[0])) == THREE_CHUNKS[0]
        gate.set()
        with pytest.raises(http.client.IncompleteRead):
            resp.read()
        conn.close()
    finally:
        gate.set()
        stop()
        stop_server(srv)
    (ex,) = seen
    assert ex.flags == (Flag.UPSTREAM_ERROR, Flag.STREAM_TRUNCATED)
    assert ex.response is not None
    assert (ex.response.status, ex.response.body) == (200, THREE_CHUNKS[0])
    assert ex.stream_chunks == (len(THREE_CHUNKS[0]),)


def test_a_stream_over_tls_is_recorded_as_its_chunks(tmp_path, monkeypatch):
    """The tee sits on mitmproxy's HTTP layer, above TLS, so a stream from an HTTPS upstream is
    recorded as a plain one is. Through the reverse door, which always dials HTTPS."""
    cfg = engine_config(
        tmp_path,
        monkeypatch,
        reverse_hosts=frozenset({"127.0.0.1"}),
        maps=map_index(tmp_path, monkeypatch, STREAM_MAP),
    )
    gates = [threading.Event(), threading.Event()]
    srv = stream_server(
        THREE_CHUNKS, gates, tls_leaf=leaf_cert_for_loopback(cfg.ca, tmp_path / "leaf.pem")
    )
    eng, seen, stop = start_engine(cfg, trust_upstream_ca=cfg.ca.cert)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=5)
        up = srv.server_address[1]
        conn.request(
            "POST",
            f"/127.0.0.1:{up}/v1/chat/completions",
            body=b"{}",
            headers={"host": f"127.0.0.1:{eng.listen_port()}", "content-type": "application/json"},
        )
        resp = conn.getresponse()
        assert resp.status == 200
        for chunk, gate in zip(THREE_CHUNKS, [*gates, None], strict=True):
            assert resp.read(len(chunk)) == chunk
            if gate is not None:
                gate.set()
        assert resp.read() == b""
        conn.close()
    finally:
        for gate in gates:
            gate.set()
        stop()
        stop_server(srv)
    (ex,) = seen
    assert (ex.request.scheme, ex.door, ex.kind, ex.flags) == ("https", "reverse", "llm", ())
    assert ex.response is not None and ex.response.body == b"".join(THREE_CHUNKS)
    assert ex.stream_chunks == tuple(len(chunk) for chunk in THREE_CHUNKS)


def test_a_reset_inside_a_character_records_only_the_complete_lines_before_it(
    tmp_path, monkeypatch
):
    """The last read before a reset can end anywhere, inside a UTF-8 character included. A body
    that does not decode is stored unscanned, so the recording keeps only its complete
    lines: the key on the first line is then redacted like any other."""
    secret = TORN_STREAM_SECRET
    first = b'data: {"api_key": "' + secret.encode() + b'"}\n\n'
    torn = b'data: {"text": "caf\xc3'  # the first byte of a two-byte character
    gates = [threading.Event(), threading.Event()]
    srv = stream_server([first, torn], gates, reset=True)
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch, STREAM_MAP))
    eng, seen, stop = start_engine(cfg)
    try:
        conn, resp = open_stream(eng, srv)
        assert resp.read(len(first)) == first
        gates[0].set()
        assert resp.read(len(torn)) == torn
        gates[1].set()
        with pytest.raises(http.client.IncompleteRead):
            resp.read()
        conn.close()
    finally:
        for gate in gates:
            gate.set()
        stop()
        stop_server(srv)
    (ex,) = seen
    assert ex.flags == (Flag.UPSTREAM_ERROR, Flag.STREAM_TRUNCATED)
    assert ex.response is not None and ex.response.body == first
    assert ex.stream_chunks == (len(first),)
    stored = redact.redact_exchange(ex, bytes(32))
    assert stored.response is not None
    assert secret.encode() not in stored.response.body
    assert redact.placeholder(bytes(32), secret).encode() in stored.response.body


def test_a_compressed_stream_is_recorded_decoded_as_one_chunk(tmp_path, monkeypatch):
    """A body is recorded as `_body` reads a buffered one, `Content-Encoding` decoded, so
    redaction reads text and not gzip. Decoded bytes do not split where the wire's did, so the
    body is one chunk. The client gets the compressed bytes it was sent."""
    plain = b"".join(THREE_CHUNKS)
    packed = gzip.compress(plain)
    half = len(packed) // 2
    gate = threading.Event()
    srv = stream_server(
        [packed[:half], packed[half:]], [gate], extra_headers=(("content-encoding", "gzip"),)
    )
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch, STREAM_MAP))
    eng, seen, stop = start_engine(cfg)
    try:
        conn, resp = open_stream(eng, srv)
        assert resp.read(half) == packed[:half]
        gate.set()
        assert resp.read() == packed[half:]
        conn.close()
    finally:
        gate.set()
        stop()
        stop_server(srv)
    (ex,) = seen
    assert ex.response is not None and ex.response.body == plain
    assert (ex.stream_chunks, ex.flags) == ((len(plain),), ())


def test_a_streamed_read_is_not_observed_now_that_its_body_is_recorded(tmp_path, monkeypatch):
    """A streamed body is recorded, and `services.observe_read` never sees one: a stream is not
    a document an observer can read, and its copy may be cut short."""
    observed = []
    monkeypatch.setattr(
        services, "observe_read", lambda service, body, clock: observed.append(body)
    )
    srv = serve(StreamUpstream)
    cfg = engine_config(
        tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch, READ_STREAM_MAP)
    )
    eng, seen, stop = start_engine(cfg)
    try:
        authority = f"127.0.0.1:{srv}"
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=5)
        conn.request("GET", f"http://{authority}/v1/chat/completions", headers={"host": authority})
        resp = conn.getresponse()
        assert resp.read(len(SSE_FIRST)) == SSE_FIRST
        srv.gate.set()
        assert resp.read() == SSE_SECOND
        conn.close()
    finally:
        srv.gate.set()
        stop()
        srv.stop()
    (ex,) = seen
    assert (ex.kind, ex.answered_by) == ("read", "live")
    assert ex.response is not None and ex.response.body == SSE_FIRST + SSE_SECOND
    assert observed == []


@pytest.fixture
def small_cap(monkeypatch):
    """A tee that keeps at most 10 bytes, so its cap is testable without megabytes."""
    monkeypatch.setattr(capture, "MAX_STORED_BODY", 10)


def test_a_capped_stream_is_recorded_up_to_its_last_complete_line(small_cap):
    """The cap cuts the copy inside a character here. The recorded body ends at the last line
    end before it, so it is text redaction can read, with no half of anything on it."""
    tee = StreamCapture()
    tee(b"ok\n")
    tee(b"123456\xc3\xa9\n")  # the cap at 10 keeps b"123456\xc3"
    assert tee.truncated
    response, chunks, cut = recorded(streamed_flow(), tee, whole=True)
    assert (response.body, chunks, cut) == (b"ok\n", (3,), True)


def test_a_whole_stream_is_recorded_as_it_came_even_without_a_final_line_end():
    tee = StreamCapture()
    tee(b"data: one\n\n")
    tee(b"data: two")
    response, chunks, cut = recorded(streamed_flow(), tee, whole=True)
    assert (response.body, chunks, cut) == (b"data: one\n\ndata: two", (11, 9), False)


def test_a_stream_that_ended_in_an_error_keeps_lines_a_lone_carriage_return_ends():
    """SSE ends a line at CRLF, LF or a lone CR, and redaction reads it the same way."""
    tee = StreamCapture()
    tee(b"data: one\r\rdata: tw")
    response, chunks, cut = recorded(streamed_flow(), tee, whole=False)
    assert (response.body, chunks, cut) == (b"data: one\r\r", (11,), True)


@pytest.mark.parametrize("content_encoding", ["rot13", "br", "gzip", "utf-8", "gzip, br"])
def test_a_stream_that_cannot_be_decoded_is_recorded_empty_and_flagged(content_encoding):
    """Never as its compressed bytes, which redaction cannot read. mitmproxy raises TypeError for
    an encoding it does not know, which `get_content(strict=False)` does not catch, decodes
    `utf-8` to a str, and has no decoder for two encodings stacked."""
    tee = StreamCapture()
    tee(b"not compressed at all\n")
    flow = streamed_flow((b"content-encoding", content_encoding.encode()))
    response, chunks, cut = recorded(flow, tee, whole=True)
    assert (response.body, chunks, cut) == (b"", (), True)


@pytest.mark.parametrize("content_encoding", ["gzip", "GZIP", "deflate", "br", "zstd"])
def test_a_whole_stream_in_every_encoding_mitmproxy_decodes_is_recorded_decoded(content_encoding):
    """Split anywhere on the wire, it is one chunk once decoded."""
    plain = b"".join(THREE_CHUNKS)
    packed = encoding.encode(plain, content_encoding.lower())
    tee = StreamCapture()
    tee(packed[:5])
    tee(packed[5:])
    flow = streamed_flow((b"content-encoding", content_encoding.encode()))
    response, chunks, cut = recorded(flow, tee, whole=True)
    assert (response.body, chunks, cut) == (plain, (len(plain),), False)


def test_an_identity_encoded_stream_keeps_its_chunks():
    tee = StreamCapture()
    tee(THREE_CHUNKS[0])
    tee(THREE_CHUNKS[1])
    flow = streamed_flow((b"content-encoding", b"identity"))
    response, chunks, cut = recorded(flow, tee, whole=True)
    assert response.body == THREE_CHUNKS[0] + THREE_CHUNKS[1]
    assert (chunks, cut) == ((len(THREE_CHUNKS[0]), len(THREE_CHUNKS[1])), False)


@pytest.mark.parametrize("content_encoding", ["gzip", "deflate", "br", "zstd"])
def test_a_compressed_stream_cut_short_is_recorded_as_whole_decoded_lines_or_empty(
    content_encoding,
):
    """Decision 6's cut stream. `gzip` decodes what arrived, which is kept up to its last complete
    line; `br` and `deflate` refuse a cut body and `zstd` decodes it to nothing, so it is recorded
    empty. Never the compressed bytes, and never half a line."""
    plain = b"".join(THREE_CHUNKS) * 200
    packed = encoding.encode(plain, content_encoding)
    tee = StreamCapture()
    tee(packed[: len(packed) // 2])
    flow = streamed_flow((b"content-encoding", content_encoding.encode()))
    response, chunks, cut = recorded(flow, tee, whole=False)
    assert cut
    assert plain.startswith(response.body)
    assert bool(response.body) is (content_encoding == "gzip")
    assert response.body[-1:] in (b"", b"\n")
    assert sum(chunks) == len(response.body) and 0 not in chunks


def test_the_tee_hands_every_chunk_back_unchanged_and_keeps_a_copy():
    tee = StreamCapture()
    assert tee(b"one") == b"one"
    assert tee(b"two") == b"two"
    assert tee(b"") == b""  # mitmproxy's end-of-message call, which is not a chunk
    assert (tee.chunks, tee.truncated) == ([b"one", b"two"], False)


def test_the_tee_keeps_a_stream_exactly_as_long_as_its_cap_whole(small_cap):
    tee = StreamCapture()
    tee(b"12345")
    tee(b"67890")
    assert (tee.chunks, tee.truncated) == ([b"12345", b"67890"], False)


def test_the_tee_cuts_the_chunk_that_crosses_its_cap_and_keeps_nothing_after(small_cap):
    tee = StreamCapture()
    assert tee(b"1234567") == b"1234567"
    assert tee(b"89abc") == b"89abc"
    assert tee(b"def") == b"def"
    assert (tee.chunks, tee.truncated) == ([b"1234567", b"89a"], True)


def test_the_tee_stops_copying_for_good_once_its_copy_raises(monkeypatch):
    tee = StreamCapture()
    tee(b"one")
    monkeypatch.setattr(tee, "chunks", None)  # `.append` now raises AttributeError
    assert tee(b"two") == b"two"
    monkeypatch.setattr(tee, "chunks", [b"one"])
    assert tee(b"three") == b"three"
    assert (tee.chunks, tee.truncated) == ([b"one"], True)
