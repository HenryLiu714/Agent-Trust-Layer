"""Capturing a streamed response for the trace while every chunk goes on to the agent unchanged."""

from irimi.core.exchange import Headers, Response, clip_chunks
from irimi.core.limits import MAX_STORED_BODY
from irimi.engine.decoding import decode_body
from irimi.record import redact


class StreamCapture:
    """`flow.response.stream` callable: return each chunk unchanged and keep a copy for the trace.

    Never raises (fail-closed hook rule). Copying stops at MAX_STORED_BODY or on error.
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
    """Return the recorded response, its chunk lengths, and whether the body is short of the stream.

    Never raises. A short body is cut to its last whole line, since redaction reads by line.
    """
    body = b"".join(capture.chunks)
    chunks = tuple(len(chunk) for chunk in capture.chunks)
    cut = capture.truncated or not whole
    # Joined as mitmproxy reads it, so encodings stacked over two headers decode in full.
    content_encoding = ", ".join(v for k, v in headers if k.lower() == "content-encoding")
    if content_encoding and body:
        decoded = decode_body(body, content_encoding)
        if decoded is None:
            body, chunks, cut = b"", (), True
        elif decoded.cut:
            # Capped as stored, since redacting 64 MiB of lines outlasts the store's close timeout.
            body, cut = decoded.body[:MAX_STORED_BODY], True
            chunks = (len(body),)
        elif decoded.body != body:
            body, chunks = decoded.body, ((len(decoded.body),) if decoded.body else ())
    if cut:
        body = redact.complete_lines(body)
        chunks = clip_chunks(chunks, len(body))
    return Response(status=status, headers=headers, body=body), chunks, cut
