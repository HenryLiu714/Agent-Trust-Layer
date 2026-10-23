"""A compressed body is decoded only up to a cap, inside every hook that decodes one.

irimi records bodies decoded, so that redaction reads text and not gzip. A few KB of gzip, br or
zstd can decode to gigabytes, and a hook that decoded it whole stalled every flow behind it and
could exhaust memory. Each codec now runs incrementally and stops at `decoding.MAX_DECODED_BODY`. A
body cut there is recorded empty, a stream as its start in whole lines, and the exchange is
flagged, while whatever irimi forwards still carries every byte it was sent.

The first part holds each codec to the cap with a small one. The second decodes real bombs at the
real cap, measuring what the hook holds. The last drives one through the engine on every path that
decodes: a buffered read, a stream, a write's request, a live request, and the control endpoint.
"""

import bz2
import gzip
import http.client
import json
import os
import socket
import threading
import tracemalloc
import zlib
from http.server import BaseHTTPRequestHandler, HTTPServer

import brotli
import pytest
import zstandard
from mitmproxy.net import encoding
from mitmproxy.test import tutils

from irimi import control
from irimi.control import MAX_CONTROL_BODY, ControlEndpoint
from irimi.engine import capture, decoding, mitm
from irimi.engine.capture import StreamCapture
from irimi.engine.decoding import Decoded, decode_body
from irimi.exchange import Exchange, Flag, Request, Response
from irimi.overlay import Overlaid
from irimi.store import MAX_STORED_BODY, DirectoryStore, NullStore, StoreReader
from tests.test_engine_mitm import (
    STREAM_MAP,
    _config,
    _maps,
    _open_stream,
    _recorded,
    _reverse,
    _start,
    _stream_server,
    _streamed_flow,
    _targeted,
    _via_proxy,
)

CAP = decoding.MAX_DECODED_BODY
LINE = b"x" * 1023 + b"\n"  # a body of whole 1 KiB lines, so a cut one keeps whole lines
MIB = 1024 * 1024


def _lines(n_bytes: int) -> bytes:
    return LINE * (n_bytes // len(LINE))


# ------------------------------------------------------------------------- each codec, small cap


@pytest.fixture
def cap_of_10_lines(monkeypatch):
    monkeypatch.setattr(decoding, "MAX_DECODED_BODY", 10 * len(LINE))
    return 10 * len(LINE)


ENCODINGS = ["gzip", "deflate", "br", "zstd"]


@pytest.mark.parametrize("content_encoding", ENCODINGS)
def test_a_body_past_the_cap_decodes_to_the_cap_and_says_it_was_cut(
    cap_of_10_lines, content_encoding
):
    plain = _lines(100 * len(LINE))
    decoded = decode_body(encoding.encode(plain, content_encoding), content_encoding)
    assert decoded is not None
    assert (decoded.body, decoded.cut) == (plain[:cap_of_10_lines], True)


@pytest.mark.parametrize("content_encoding", ENCODINGS)
@pytest.mark.parametrize("lines", [9, 10])
def test_a_body_at_or_under_the_cap_decodes_whole(cap_of_10_lines, content_encoding, lines):
    """Exactly the cap is not past it: the one byte more each codec is asked for is not there."""
    plain = LINE * lines
    decoded = decode_body(encoding.encode(plain, content_encoding), content_encoding)
    assert decoded is not None
    assert (decoded.body, decoded.cut) == (plain, False)


def test_a_cut_buffered_body_is_recorded_empty(cap_of_10_lines):
    """Never redacted whole, so never stored in part: the start of a document is not one."""
    response = tutils.tresp(content=None, headers=((b"content-encoding", b"gzip"),))
    response.raw_content = gzip.compress(LINE * 11)
    assert mitm._body(response) == Decoded(b"", True)


def test_a_cut_stream_is_kept_to_its_last_complete_line(cap_of_10_lines):
    """The cap lands inside a line here, and inside a UTF-8 character: a body cut there is one
    redaction cannot read, so it is stored unscanned. What is kept ends at a line end, as
    one chunk, flagged."""
    tee = StreamCapture()
    tee(gzip.compress(b"ok\n" + "é".encode() * (20 * len(LINE))))
    flow = _streamed_flow((b"content-encoding", b"gzip"))
    response, chunks, cut = _recorded(flow, tee, whole=True)
    assert (response.body, chunks, cut) == (b"ok\n", (3,), True)


@pytest.mark.parametrize(
    ("content_encoding", "packed"),
    [("bz2", bz2.compress(b"x" * 10_000)), ("zlib", zlib.compress(b"x" * 10_000))],
)
def test_an_encoding_irimi_does_not_decode_is_not_run_through_python_s_codecs(
    content_encoding, packed
):
    """mitmproxy's decoder fell back to `codecs.decode` for a name it did not know, where `bz2`
    and `zlib` are decompressions with no bound of their own. Neither is an HTTP content coding;
    such a body is undecodable, and recorded as it came."""
    assert decode_body(packed, content_encoding) is None


@pytest.mark.parametrize("content_encoding", ENCODINGS)
def test_an_empty_body_decodes_to_nothing_in_every_encoding(content_encoding):
    """As mitmproxy's decoders say. brotli's own refuses an empty stream as one that ends early."""
    assert decode_body(b"", content_encoding) == Decoded(b"", False)


@pytest.mark.parametrize("cut_by", [1, 3, 600])
def test_a_zstd_stream_that_ends_early_is_still_cut_at_the_cap(cap_of_10_lines, cut_by):
    """Incompressible bytes are a raw block, which zstd hands out as it arrives even when the
    stream ends early, as mitmproxy's decoder keeps them. Read to the cap and then once more, the
    second read comes back empty, and the body cut at the cap must not be called whole."""
    plain = os.urandom(20 * len(LINE))
    decoded = decode_body(encoding.encode(plain, "zstd")[:-cut_by], "zstd")
    assert decoded is not None
    assert (decoded.body, decoded.cut) == (plain[:cap_of_10_lines], True)


def test_a_deflate_stream_that_ends_early_is_refused_even_right_at_the_cap(cap_of_10_lines):
    """`deflate` refuses a stream that ends early, as `zlib.decompress` does: at the cap, as
    anywhere else. Its last four bytes are the checksum, so all of the body is there."""
    plain = LINE * 10
    assert len(plain) == cap_of_10_lines
    assert decode_body(zlib.compress(plain)[:-2], "deflate") is None
    assert decode_body(zlib.compress(plain), "deflate") == Decoded(plain, False)


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


# The line the gzip bomb decodes to, a GiB of it. Short lines compress about 500 to 1.
BOMB_LINE = b" " * 15 + b"\n"


@pytest.fixture(scope="module")
def gigabyte_gzip() -> bytes:
    """About 2 MiB of gzip that decodes to 1 GiB of BOMB_LINE: built here rather than
    taken from the sample workflows' harness, whose bombs are theirs to change."""
    deflater = zlib.compressobj(9, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
    block = BOMB_LINE * (MIB // len(BOMB_LINE))
    return b"".join([deflater.compress(block) for _ in range(1024)] + [deflater.flush()])


@pytest.fixture(scope="module")
def bombs(gigabyte_gzip) -> dict[str, bytes]:
    """One bomb per codec: gzip's decodes to 1 GiB, and the others to four times the
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
    reads one byte past the cap. What is recorded of it is nothing."""
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
    assert body == Decoded(b"", True)
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


@pytest.mark.parametrize(
    ("said", "recorded"), [(None, None), ("full", "partial"), ("partial", "partial")]
)
def test_a_buffered_read_bomb_reaches_the_agent_whole_and_is_recorded_empty(
    tmp_path, monkeypatch, bomb_upstream, gigabyte_gzip, said, recorded
):
    """The agent gets the service's bytes, every one, whatever the overlay returns: irimi has only
    the start of the document, and an overlay that edited it would send the agent half a document
    as the whole. The overlay is shown the empty body irimi records, so a read the run has a
    write for is `partial`, as a body it cannot parse is, and the summary says the read did not
    show the write. The recording is empty, flagged."""
    shown = []

    class _Overlay:
        def __call__(
            self, write_log, classification, read_request, upstream_response, translated_from=None
        ):
            shown.append(upstream_response.body)
            return Overlaid(Response(200, (), b"OVERLAID"), said)

        def rewrite(self, write_log, classification, read_request):
            return None

    eng, seen, stop = _start(_config(tmp_path, monkeypatch), overlay=_Overlay())
    base = f"http://127.0.0.1:{bomb_upstream}"
    try:
        # A write first, or the overlay is never asked at all.
        _via_proxy(eng.listen_port(), "POST", f"{base}/things", body=b"{}")
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=10)
        conn.request("GET", f"{base}/bomb", headers={"host": base.split("/")[2]})
        resp = conn.getresponse()
        status, answered_by, data = resp.status, resp.getheader("irimi-answered-by"), resp.read()
        conn.close()
    finally:
        stop()
    assert (status, answered_by, data == gigabyte_gzip) == (200, None, True)
    assert shown == [b""]
    read = seen[-1]
    assert (read.answered_by, read.flags, read.overlay) == (
        "live",
        (Flag.BODY_TRUNCATED,),
        recorded,
    )
    assert read.response is not None and read.response.body == b""


def test_a_delegated_read_bomb_reaches_the_agent_whole_stamped_and_recorded_empty(
    tmp_path, monkeypatch, bomb_upstream, gigabyte_gzip
):
    """A delegated answer is rebuilt to stamp `Irimi-Answered-By`. Only its headers are written
    back, so the agent gets every byte the target sent, never the empty body irimi records."""
    maps = _targeted(
        tmp_path,
        monkeypatch,
        targets=[("127.0.0.1", "", f"http://127.0.0.1:{bomb_upstream}")],
        target_reads=["127.0.0.1"],
    )
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=10)
        conn.request("GET", "http://127.0.0.1:9/hello", headers={"host": "127.0.0.1:9"})
        resp = conn.getresponse()
        status, answered_by, data = resp.status, resp.getheader("irimi-answered-by"), resp.read()
        conn.close()
    finally:
        stop()
    assert (status, answered_by, data == gigabyte_gzip) == (200, "delegated", True)
    (ex,) = seen
    assert (ex.answered_by, ex.flags) == (
        "delegated",
        (Flag.BODY_TRUNCATED,),
    )
    assert ex.response is not None and ex.response.body == b""


# A secret no shape gives away, under a key that names it: only a walk of the JSON document finds
# it, and the start of a document is not one.
SECRET = b"hunter2-but-longer"


def _pretty_document_bomb() -> bytes:
    """gzip of a pretty-printed JSON document, its secret on its second line, that decodes past
    the cap: the shape of an export a real API answers, not of an attack."""
    deflater = zlib.compressobj(6, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
    head = b'{\n  "password": "' + SECRET + b'",\n  "rows": [\n'
    rows = b'    "row",\n' * (MIB // 12)
    return b"".join(
        [deflater.compress(head)]
        + [deflater.compress(rows) for _ in range(CAP // len(rows) + 2)]
        + [deflater.flush()]
    )


def test_a_document_that_decodes_past_the_cap_is_stored_empty_never_in_part(
    tmp_path, monkeypatch, bomb_upstream
):
    """A STORED BODY IS ALWAYS A PREFIX OF THE REDACTED WHOLE. A body past the cap is never
    decoded whole, so never redacted whole, and the start of a JSON document is not JSON:
    redaction reads it line by line, where `"password": "..."` is a line no shape matches. So it
    is recorded empty and flagged, as the store records any body past MAX_REDACTED_BODY."""
    _BombUpstream.bomb = _pretty_document_bomb()
    store = DirectoryStore(tmp_path / "store", bytes(32))
    eng, seen, stop = _start(_config(tmp_path, monkeypatch), store=store)
    try:
        status, data = _via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{bomb_upstream}/x")
    finally:
        stop()
    assert (status, data == _BombUpstream.bomb) == (200, True)
    (ex,) = seen
    assert ex.response is not None
    assert (ex.response.body, ex.flags) == (b"", (Flag.BODY_TRUNCATED,))
    (stored,) = StoreReader(tmp_path / "store").load_run("t3st").events
    assert isinstance(stored, Exchange) and stored.response is not None
    assert (stored.response.body, stored.flags) == (b"", (Flag.BODY_TRUNCATED,))
    on_disk = [f.read_bytes() for f in (tmp_path / "store").rglob("*") if f.is_file()]
    assert not any(SECRET in data for data in on_disk)


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
    assert (ex.kind, ex.answered_by, ex.flags) == ("llm", "live", (Flag.STREAM_TRUNCATED,))
    assert ex.response is not None
    # A stream keeps its start, in whole lines, as much of it as the store keeps of any body.
    assert ex.response.body == BOMB_LINE * (MAX_STORED_BODY // len(BOMB_LINE))
    assert ex.stream_chunks == (MAX_STORED_BODY,)


@pytest.mark.parametrize("content_encoding", ["gzip", "zstd"])
def test_a_compressed_stream_the_tee_cut_is_recorded_as_the_whole_lines_it_decodes_to(
    monkeypatch, content_encoding
):
    """The tee caps its copy of the ENCODED stream, so what it kept is a compressed stream that
    ends early. It decodes as far as it goes, under the cap here, and is kept to its last complete
    line as one chunk, flagged once as not the whole stream."""
    plain = b"".join(b"data: %d\n" % i for i in range(200_000))
    packed = encoding.encode(plain, content_encoding)
    monkeypatch.setattr(capture, "MAX_STORED_BODY", len(packed) // 2)
    tee = StreamCapture()
    tee(packed)
    assert tee.truncated
    flow = _streamed_flow((b"content-encoding", content_encoding.encode()))
    response, chunks, cut = _recorded(flow, tee, whole=True)
    assert cut
    assert len(response.body) > len(plain) // 4
    assert plain.startswith(response.body) and response.body.endswith(b"\n")
    assert chunks == (len(response.body),)


def test_a_write_whose_request_is_a_bomb_is_refused_never_faked_from_part_of_it(
    tmp_path, monkeypatch, bomb_upstream, gigabyte_gzip
):
    """IRIMI NEVER ANSWERS A WRITE FROM PART OF ITS BODY. The request hook decodes the body
    it decides on only up to the cap, and the policy would fake the write from that start - its
    echo, its idempotency key's body hash, its precondition - and tell the agent a write it never
    sent succeeded. It is refused as an undecidable request is: locally, with irimi's own 502,
    flagged, and never forwarded - the upstream answers every POST with 500."""
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch)))
    try:
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=10)
        url = f"http://127.0.0.1:{bomb_upstream}/things"
        headers = {"host": url.split("/")[2], "content-encoding": "gzip"}
        conn.request("POST", url, body=gigabyte_gzip, headers=headers)
        resp = conn.getresponse()
        status, answered_by, data = resp.status, resp.getheader("irimi-answered-by"), resp.read()
        conn.close()
    finally:
        stop()
    assert (status, answered_by) == (502, "irimi")
    error = json.loads(data)["error"]
    assert error["type"] == "irimi_decision_failed"
    assert error["message"] == (
        "irimi could not decide how to answer this request: request body decompresses to more "
        f"than {CAP // (1024 * 1024)} MiB; irimi does not fake a write it cannot read whole"
    )
    # The refusal keeps the write's classification, so the summary can name the operation.
    (ex,) = seen
    assert (ex.answered_by, ex.flags) == ("irimi", (Flag.DECISION_FAILED, Flag.BODY_TRUNCATED))
    assert (ex.service, ex.operation, ex.kind) == ("demo", "things.create", "write")
    assert ex.request.body == b""


class _EchoBombUpstream(BaseHTTPRequestHandler):
    """Keeps every POST body it is sent, and answers it with `bomb`, gzip-encoded."""

    bomb = b""
    bodies: list[bytes] = []

    def do_POST(self):
        _EchoBombUpstream.bodies.append(self.rfile.read(int(self.headers["content-length"])))
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-encoding", "gzip")
        self.send_header("content-length", str(len(self.bomb)))
        self.end_headers()
        self.wfile.write(self.bomb)

    def log_message(self, *args):
        pass


@pytest.mark.parametrize(
    ("path", "delegated"), [("/things", True), ("/unrouted", False)], ids=["delegated", "unknown"]
)
def test_a_bomb_is_refused_on_every_path_a_write_takes(
    tmp_path, monkeypatch, gigabyte_gzip, path, delegated
):
    """Not only a faked write: a write its map hands to an answer target, whose target would get
    every byte but whose exchange irimi could not record, and a request no route names, which is
    answered as a presumed write. Each is refused with irimi's own 502, never forwarded, and a
    retry with the same body is refused again rather than replayed from any slot."""
    _EchoBombUpstream.bomb, _EchoBombUpstream.bodies = b"{}", []
    srv = HTTPServer(("127.0.0.1", 0), _EchoBombUpstream)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    port = srv.server_address[1]
    targets = [("127.0.0.1", path, f"http://127.0.0.1:{port}/w")] if delegated else []
    maps = _targeted(tmp_path, monkeypatch, targets=targets)
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    answers = []
    try:
        for _ in range(2):
            conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=10)
            headers = {
                "host": f"127.0.0.1:{port}",
                "content-encoding": "gzip",
                "idempotency-key": "same-key",
            }
            conn.request("POST", f"http://127.0.0.1:{port}{path}", gigabyte_gzip, headers)
            resp = conn.getresponse()
            answers.append((resp.status, resp.getheader("irimi-answered-by")))
            resp.read()
            conn.close()
    finally:
        stop()
        srv.shutdown()
    assert answers == [(502, "irimi")] * 2
    assert _EchoBombUpstream.bodies == []
    kind, unclassified = ("write", ()) if delegated else ("unknown", (Flag.UNCLASSIFIED,))
    assert [(ex.answered_by, ex.kind, ex.target, ex.flags) for ex in seen] == [
        ("irimi", kind, "", (*unclassified, Flag.DECISION_FAILED, Flag.BODY_TRUNCATED))
    ] * 2


def test_a_live_request_that_is_a_bomb_is_forwarded_as_the_agent_sent_it(
    tmp_path, monkeypatch, gigabyte_gzip
):
    """A live kind is classified without its body, and forwarded with the agent's own bytes, not
    what irimi decoded of them. Its answer is a bomb too: the exchange is cut on both sides, and
    says so once."""
    _EchoBombUpstream.bomb, _EchoBombUpstream.bodies = gigabyte_gzip, []
    srv = HTTPServer(("127.0.0.1", 0), _EchoBombUpstream)
    threading.Thread(target=lambda: srv.serve_forever(poll_interval=0.01), daemon=True).start()
    maps = _maps(tmp_path, monkeypatch, STREAM_MAP)
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, data = _via_proxy(
            eng.listen_port(),
            "POST",
            f"http://127.0.0.1:{srv.server_address[1]}/v1/chat/completions",
            body=gigabyte_gzip,
            extra_headers={"content-encoding": "gzip"},
        )
    finally:
        stop()
        srv.shutdown()
    assert (status, data == gigabyte_gzip) == (200, True)
    assert _EchoBombUpstream.bodies == [gigabyte_gzip]
    (ex,) = seen
    assert (ex.kind, ex.answered_by, ex.flags) == ("llm", "live", (Flag.BODY_TRUNCATED,))
    assert ex.response is not None
    assert (ex.request.body, ex.response.body) == (b"", b"")


def test_a_live_request_that_is_a_bomb_says_so_when_its_upstream_fails_too(
    tmp_path, monkeypatch, gigabyte_gzip
):
    """The `error` hook builds a live forward's flags afresh, and kept only `upstream-error`: the
    request it records is still cut."""
    with socket.socket() as sock:  # a port nothing listens on
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    maps = _maps(tmp_path, monkeypatch, STREAM_MAP)
    eng, seen, stop = _start(_config(tmp_path, monkeypatch, maps=maps))
    try:
        status, _ = _via_proxy(
            eng.listen_port(),
            "POST",
            f"http://127.0.0.1:{port}/v1/chat/completions",
            body=gigabyte_gzip,
            extra_headers={"content-encoding": "gzip"},
        )
    finally:
        stop()
    assert status == 502
    (ex,) = seen
    assert (ex.answered_by, ex.flags) == ("live", (Flag.BODY_TRUNCATED, Flag.UPSTREAM_ERROR))
    assert ex.request.body == b""


def test_a_control_request_bomb_is_refused_without_decoding_it_whole(
    tmp_path, monkeypatch, gigabyte_gzip
):
    """The control endpoint refuses a body past 2 MiB, and checks it decoded. Decoded at
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
    assert json.loads(data) == {
        "error": {
            "type": "irimi_control_refused",
            "message": f"the body decodes past the {MAX_CONTROL_BODY} bytes allowed",
        }
    }
    assert seen == []


def _one_line_bomb() -> bytes:
    """About 100 KiB of gzip that decodes to an open brace and 100 MiB of spaces: no line end."""
    deflater = zlib.compressobj(9, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
    spaces = b" " * MIB
    return b"".join(
        [deflater.compress(b"{")]
        + [deflater.compress(spaces) for _ in range(100)]
        + [deflater.flush()]
    )


def test_a_control_request_bomb_of_one_line_is_refused_as_too_large_not_as_bad_json(
    tmp_path, monkeypatch
):
    """A cut body reaches the endpoint empty, and an empty body is not JSON: it answered 400 for
    JSON it was never sent. A cut body is past 2 MiB whatever its start holds, so 413."""
    eng, seen, stop = _start(_config(tmp_path, monkeypatch))
    try:
        status, data = _reverse(
            eng.listen_port(),
            "POST",
            "/_irimi/runs/r1/start",
            body=_one_line_bomb(),
            extra_headers={"content-encoding": "gzip", "connection": "close"},
        )
    finally:
        stop()
    assert status == 413, data
    assert seen == []


def test_the_control_endpoint_refuses_a_body_that_is_not_whole_whatever_it_holds():
    """Whatever `request.body` holds, even valid JSON under 2 MiB: the engine says it is not the
    body, which decoded past the cap."""
    body = json.dumps({"ended_at": 1.0, "outcome": "ok", "error": None}).encode()
    request = Request("POST", "http", "127.0.0.1", 1, "/_irimi/runs/r1/end", "", (), body)
    endpoint = ControlEndpoint(NullStore(), serve=False)
    assert endpoint.answer(request).status == 204
    refused = endpoint.answer(request, whole_body=False)
    assert refused.status == 413
    assert refused.header("irimi-answered-by") == "control"


def test_a_cut_body_is_always_past_the_control_endpoint_s_limit():
    """Why a cut control body is refused as too large without reading it."""
    assert decoding.MAX_DECODED_BODY > control.MAX_CONTROL_BODY


def test_the_bomb_fixtures_decode_past_the_cap(bombs):
    """The bombs are what the tests above say they are: small, and far past the cap decoded."""
    assert len(bombs["gzip"]) < 2.1 * MIB  # the gzip bomb: about 2 MiB of gzip to 1 GiB
    assert all(len(bomb) < 4 * MIB for bomb in bombs.values())
    for content_encoding, bomb in bombs.items():
        decoded = decode_body(bomb, content_encoding)
        assert decoded is not None and decoded.cut, content_encoding
