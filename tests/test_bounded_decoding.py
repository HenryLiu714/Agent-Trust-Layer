"""A compressed body is decoded only up to a cap, inside every hook that decodes one (#94).

irimi records bodies decoded, so that redaction reads text and not gzip. A few KB of gzip, br or
zstd can decode to gigabytes, and a hook that decoded it whole stalled every flow behind it and
could exhaust memory. Each codec now runs incrementally and stops at `mitm.MAX_DECODED_BODY`: what
was decoded is kept to its last complete line and the exchange is flagged, while the agent and the
service still get every byte they were sent.

The first part holds each codec to the cap with a small one. The second decodes real bombs at the
real cap, measuring what the hook holds. The last drives one through the engine on every path that
decodes: a buffered read, a stream, a faked write's request, and the control endpoint.
"""

import bz2
import gzip
import threading
import tracemalloc
import zlib
from http.server import BaseHTTPRequestHandler, HTTPServer

import brotli
import pytest
import zstandard
from mitmproxy.net import encoding
from mitmproxy.test import tutils

from examples.workflows.harness.internet import BOMB_LINE, gzip_bomb
from irimi.engine import mitm
from irimi.exchange import BODY_TRUNCATED_FLAG, STREAM_TRUNCATED_FLAG, Response
from irimi.overlay import Overlaid
from irimi.store import MAX_STORED_BODY
from tests.test_engine_mitm import (
    STREAM_MAP,
    _config,
    _maps,
    _open_stream,
    _reverse,
    _start,
    _stream_server,
    _via_proxy,
)

CAP = mitm.MAX_DECODED_BODY
LINE = b"x" * 1023 + b"\n"  # a body of whole 1 KiB lines, so a cut one keeps whole lines
MIB = 1024 * 1024


def _lines(n_bytes: int) -> bytes:
    return LINE * (n_bytes // len(LINE))


# ------------------------------------------------------------------------- each codec, small cap


@pytest.fixture
def cap_of_10_lines(monkeypatch):
    monkeypatch.setattr(mitm, "MAX_DECODED_BODY", 10 * len(LINE))
    return 10 * len(LINE)


ENCODINGS = ["gzip", "deflate", "br", "zstd"]


@pytest.mark.parametrize("content_encoding", ENCODINGS)
def test_a_body_past_the_cap_decodes_to_the_cap_and_says_it_was_cut(
    cap_of_10_lines, content_encoding
):
    plain = _lines(100 * len(LINE))
    decoded = mitm._decoded(encoding.encode(plain, content_encoding), content_encoding)
    assert decoded is not None
    assert (decoded.body, decoded.cut) == (plain[:cap_of_10_lines], True)


@pytest.mark.parametrize("content_encoding", ENCODINGS)
@pytest.mark.parametrize("lines", [9, 10])
def test_a_body_at_or_under_the_cap_decodes_whole(cap_of_10_lines, content_encoding, lines):
    """Exactly the cap is not past it: the one byte more each codec is asked for is not there."""
    plain = LINE * lines
    decoded = mitm._decoded(encoding.encode(plain, content_encoding), content_encoding)
    assert decoded is not None
    assert (decoded.body, decoded.cut) == (plain, False)


def test_a_cut_buffered_body_is_kept_to_its_last_complete_line(cap_of_10_lines):
    """The cap lands inside a line here, and inside a UTF-8 character: a body cut there is one
    redaction cannot read, so it is stored unscanned (#69). What is kept ends at a line end."""
    plain = b"ok\n" + "é".encode() * (20 * len(LINE))
    response = tutils.tresp(content=None, headers=((b"content-encoding", b"gzip"),))
    response.raw_content = gzip.compress(plain)
    assert mitm._body(response) == mitm._Decoded(b"ok\n", True)


@pytest.mark.parametrize(
    ("content_encoding", "packed"),
    [("bz2", bz2.compress(b"x" * 10_000)), ("zlib", zlib.compress(b"x" * 10_000))],
)
def test_an_encoding_irimi_does_not_decode_is_not_run_through_python_s_codecs(
    content_encoding, packed
):
    """mitmproxy's decoder fell back to `codecs.decode` for a name it did not know, where `bz2`
    and `zlib` are decompressions with no bound of their own. Neither is an HTTP content coding;
    such a body is undecodable, and recorded as it came (#94)."""
    assert mitm._decoded(packed, content_encoding) is None


# --------------------------------------------------------------------- real bombs at the real cap


def _zlib_bomb(n_bytes: int, wbits: int) -> bytes:
    deflater = zlib.compressobj(1, zlib.DEFLATED, wbits)  # fast; small enough at level 1
    block = _lines(MIB)
    return b"".join([deflater.compress(block) for _ in range(n_bytes // MIB)] + [deflater.flush()])


def _brotli_bomb(n_bytes: int) -> bytes:
    compressor = brotli.Compressor(quality=5)
    block = _lines(MIB)
    return b"".join(
        [compressor.process(block) for _ in range(n_bytes // MIB)] + [compressor.finish()]
    )


def _zstd_bomb(n_bytes: int) -> bytes:
    compressor = zstandard.ZstdCompressor().compressobj()
    block = _lines(MIB)
    return b"".join(
        [compressor.compress(block) for _ in range(n_bytes // MIB)] + [compressor.flush()]
    )


@pytest.fixture(scope="module")
def gigabyte_gzip() -> bytes:
    """About 2 MiB of gzip that decodes to 1 GiB: the W10 `gzip_bomb_read` fault's, built once
    per session for both."""
    return gzip_bomb()


@pytest.fixture(scope="module")
def bombs(gigabyte_gzip) -> dict[str, bytes]:
    """One bomb per codec: gzip's decodes to 1 GiB, as #94 asks, and the others to four times the
    cap, which is as far past it as they need to be."""
    return {
        "gzip": gigabyte_gzip,
        "deflate": _zlib_bomb(4 * CAP, zlib.MAX_WBITS),
        "br": _brotli_bomb(4 * CAP),
        "zstd": _zstd_bomb(4 * CAP),
    }


@pytest.mark.parametrize("content_encoding", ENCODINGS)
def test_a_bomb_never_holds_more_than_twice_the_cap(bombs, content_encoding):
    """What the hook holds decoding a bomb: never more than twice the cap, and not the gigabyte.
    A codec's output is assembled once - zlib's own output buffer, brotli's steps - and zstd
    reads into one buffer of the cap's size. What is recorded is the store's share of it."""
    message = tutils.tresp(
        content=None, headers=((b"content-encoding", content_encoding.encode()),)
    )
    message.raw_content = bombs[content_encoding]
    tracemalloc.start()
    try:
        body = mitm._body(message)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert body.cut
    # Recorded as the store would keep it: its first MAX_STORED_BODY bytes, all whole lines.
    assert (len(body.body), body.body[-1:]) == (MAX_STORED_BODY, b"\n")
    assert peak < 2 * CAP + 4 * MIB, peak


# ------------------------------------------------------------------- every path through the engine


class _BombUpstream(BaseHTTPRequestHandler):
    """Answers every GET with `bomb`, gzip-encoded. Never answers a POST: a faked write that
    escaped is the failure it would show."""

    bomb = b""

    def do_GET(self):
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-encoding", "gzip")
        self.send_header("content-length", str(len(self.bomb)))
        self.end_headers()
        self.wfile.write(self.bomb)

    def do_POST(self):
        self.send_response(500)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def bomb_upstream(gigabyte_gzip):
    _BombUpstream.bomb = gigabyte_gzip
    srv = HTTPServer(("127.0.0.1", 0), _BombUpstream)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    yield srv.server_address[1]
    srv.shutdown()


def _assert_recorded_cut(body: bytes) -> None:
    """A cut body is recorded as its first MAX_STORED_BODY bytes, which are whole lines here."""
    assert body == BOMB_LINE * (MAX_STORED_BODY // len(BOMB_LINE))


def test_a_buffered_read_bomb_reaches_the_agent_whole_and_is_recorded_cut(
    tmp_path, monkeypatch, bomb_upstream, gigabyte_gzip
):
    """The agent gets the service's bytes, every one, and the overlay is never asked: a body cut
    at the cap is the start of a document, and an overlay that edited it would send the agent half
    a document as the whole (#94). The recording is the cap's worth of whole lines, flagged."""
    asked = []

    class _Overlay:
        def __call__(self, write_log, read_request, upstream_response):
            asked.append(read_request.path)
            return Overlaid(Response(200, (), b"OVERLAID"))

        def rewrite(self, write_log, read_request):
            return read_request

    eng, seen, stop = _start(_config(tmp_path, monkeypatch), overlay=_Overlay())
    base = f"http://127.0.0.1:{bomb_upstream}"
    try:
        # A write first, or the overlay is never asked at all.
        _via_proxy(eng.listen_port(), "POST", f"{base}/things", body=b"{}")
        status, data = _via_proxy(eng.listen_port(), "GET", f"{base}/bomb")
    finally:
        stop()
    assert (status, data == gigabyte_gzip) == (200, True)
    assert asked == []
    read = seen[-1]
    assert (read.answered_by, read.flags) == ("live", (BODY_TRUNCATED_FLAG,))
    assert read.response is not None
    _assert_recorded_cut(read.response.body)


def test_a_streamed_bomb_reaches_the_agent_whole_and_is_recorded_cut(
    tmp_path, monkeypatch, gigabyte_gzip
):
    srv = _stream_server([gigabyte_gzip], extra_headers=(("content-encoding", "gzip"),))
    eng, seen, stop = _start(
        _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch, STREAM_MAP))
    )
    try:
        conn, resp = _open_stream(eng, srv)
        assert resp.read() == gigabyte_gzip
        conn.close()
    finally:
        stop()
        srv.shutdown()
    (ex,) = seen
    assert (ex.kind, ex.answered_by, ex.flags) == ("llm", "live", (STREAM_TRUNCATED_FLAG,))
    assert ex.response is not None
    _assert_recorded_cut(ex.response.body)
    assert ex.stream_chunks == (MAX_STORED_BODY,)


def test_a_faked_write_whose_request_is_a_bomb_is_still_faked_and_flagged(
    tmp_path, monkeypatch, bomb_upstream, gigabyte_gzip
):
    """The request hook decodes the body it decides on. Capped, the write is still answered
    locally - the upstream answers every POST with 500, so it was never reached - and its exchange
    says the request it records is not all of it."""
    eng, seen, stop = _start(_config(tmp_path, monkeypatch))
    try:
        status, _ = _via_proxy(
            eng.listen_port(),
            "POST",
            f"http://127.0.0.1:{bomb_upstream}/things",
            body=gigabyte_gzip,
            extra_headers={"content-encoding": "gzip", "content-type": "text/plain"},
        )
    finally:
        stop()
    assert status == 200
    (ex,) = seen
    assert ex.answered_by == "fake-L0"
    assert BODY_TRUNCATED_FLAG in ex.flags
    _assert_recorded_cut(ex.request.body)


def test_a_control_request_bomb_is_refused_without_decoding_it_whole(
    tmp_path, monkeypatch, gigabyte_gzip
):
    """The control endpoint refuses a body past 2 MiB, and checks it decoded (#73). Decoded at
    most to the cap, a 1 GiB bomb is refused as over 2 MiB, and never decoded to its gigabyte."""
    eng, seen, stop = _start(_config(tmp_path, monkeypatch))
    try:
        status, data = _reverse(
            eng.listen_port(),
            "POST",
            "/_irimi/runs/r1/start",
            body=gigabyte_gzip,
            # Closed by the answer, so the engine is not stopped under a connection mitmproxy
            # is still holding open for the next request.
            extra_headers={"content-encoding": "gzip", "connection": "close"},
        )
    finally:
        stop()
    assert status == 413, data
    assert f"the body is {MAX_STORED_BODY} bytes".encode() in data
    assert seen == []


def test_the_bomb_fixtures_decode_past_the_cap(bombs):
    """The bombs are what the tests above say they are: small, and far past the cap decoded."""
    assert len(bombs["gzip"]) < 2.1 * MIB  # #94's bomb: about 1 MiB of gzip to 1 GiB
    assert all(len(bomb) < 4 * MIB for bomb in bombs.values())
    for content_encoding, bomb in bombs.items():
        decoded = mitm._decoded(bomb, content_encoding)
        assert decoded is not None and decoded.cut, content_encoding
