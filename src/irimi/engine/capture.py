"""Capturing a streamed response for the trace while every chunk goes on to the agent unchanged."""

from irimi.core.exchange import Headers, Response, clip_chunks
from irimi.core.limits import MAX_STORED_BODY
from irimi.engine.decoding import decode_body
from irimi.record import redact


class StreamCapture:
    """`flow.response.stream` for a streamed response: hands each chunk on unchanged, and keeps a
    copy of it for the trace.

    mitmproxy calls it with each chunk as it arrives and once more with `b""` at the end of the
    message, and sends the client whatever it returns. So THE CHUNK IS RETURNED WHATEVER HAPPENS
    HERE: the copy is best-effort, and the agent's stream is not. mitmproxy does not guard the
    call, so a raise would break the agent's stream, not just the recording.

    The copy stops at MAX_STORED_BODY, the most the store keeps of any body: later chunks still
    reach the client and are not kept. It also stops for good the first time copying raises.
    Either way `truncated` is set, and the exchange is flagged `stream-truncated`.
    """

    def __init__(self) -> None:
        self.chunks: list[bytes] = []
        self._size = 0
        self.truncated = False

    def __call__(self, chunk: bytes) -> bytes:
        try:
            self._capture(chunk)
        except Exception:
            self.truncated = True
        return chunk

    def _capture(self, chunk: bytes) -> None:
        if self.truncated or not chunk:
            return
        room = MAX_STORED_BODY - self._size
        if len(chunk) > room:
            self.truncated = True
            chunk = chunk[:room]
        if chunk:
            self.chunks.append(chunk)
            self._size += len(chunk)


def recorded_stream(
    status: int, headers: Headers, capture: StreamCapture, *, whole: bool
) -> tuple[Response, tuple[int, ...], bool]:
    """A streamed response as the trace records it: its status and headers, the body `capture`
    kept, the length of each chunk, and whether that body is short of the whole stream.
    `whole` is False when the stream ended in the engine's `error` hook. Nothing here raises.

    A BODY THAT IS NOT THE WHOLE STREAM IS KEPT UP TO ITS LAST COMPLETE LINE. The copy stops at an
    arbitrary byte - the cap, the last read before a reset - and a cut inside a UTF-8 character
    makes the whole body undecodable, which redaction stores unscanned: every key in it
    would reach disk. A cut inside a line would also store half of a secret on it. Redaction reads
    a stream line by line, so a body of whole lines is one it can read.

    A `Content-Encoding` is decoded, as the engine decodes a buffered body, so that redaction reads
    text rather than gzip; decoded bytes do not split where the wire's chunks did, so such a body
    is one chunk. One that cannot be decoded - a cut `br` stream, an encoding irimi does not
    decode - is recorded empty and flagged, never as the compressed bytes redaction cannot read.
    One that decodes past MAX_DECODED_BODY keeps the start of what was decoded, and is flagged
    too."""
    body = b"".join(capture.chunks)
    chunks = tuple(len(chunk) for chunk in capture.chunks)
    cut = capture.truncated or not whole
    # Every value of the header, joined as mitmproxy reads it, so two encodings stacked over two
    # headers are not decoded as the first alone.
    content_encoding = ", ".join(v for k, v in headers if k.lower() == "content-encoding")
    if content_encoding and body:
        decoded = decode_body(body, content_encoding)
        if decoded is None:
            body, chunks, cut = b"", (), True
        elif decoded.cut:
            # Kept, below, to its last complete line, as any stream that is not whole is: a stream
            # is redacted line by line, so the start of one is redacted as the whole would be. No
            # more of it than the store keeps: past that, the writer only redacts what it throws
            # away - 64 MiB of short lines is half a minute, longer than the store waits to close.
            body, cut = decoded.body[:MAX_STORED_BODY], True
            chunks = (len(body),)
        elif decoded.body != body:
            body, chunks = decoded.body, ((len(decoded.body),) if decoded.body else ())
    if cut:
        body = redact.complete_lines(body)
        chunks = clip_chunks(chunks, len(body))
    return Response(status=status, headers=headers, body=body), chunks, cut
