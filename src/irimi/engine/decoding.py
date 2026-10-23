"""Bounded decoding of a body's `Content-Encoding`.

A few KB of gzip, br or zstd can decode to gigabytes, and a hook that decoded it whole would stall
every flow behind it and could exhaust memory. Each codec here runs incrementally and stops at
MAX_DECODED_BODY. Nothing here raises, because every body the hooks record is decoded here.
"""

import io
import zlib
from collections.abc import Callable
from dataclasses import dataclass

import brotli
import zstandard

from irimi.core.limits import MAX_DECODED_BODY

# What a body's identity encodings decode to: the bytes as they came, which are already in memory
# and are no bigger for being "decoded".
_IDENTITY_ENCODINGS = frozenset({"identity", "none"})


@dataclass(frozen=True)
class Decoded:
    body: bytes
    cut: bool  # decoding stopped at MAX_DECODED_BODY: `body` is the start of it, not all of it


def decode_body(body: bytes, content_encoding: str) -> Decoded | None:
    """`body` decoded per `content_encoding` up to MAX_DECODED_BODY, or None when it cannot be.

    DECODING NEVER RAISES: every body the hooks record is decoded here, because a raise out of a
    hook drops the exchange or forwards the flow. Nor is it ever unbounded: each codec runs
    incrementally, stops at the cap and asks for one byte more to learn whether it was cut. So the
    hook holds at most twice the cap, while a codec assembles its output into one bytes object,
    and never what a few KB of a bomb would have decoded to. The engine's `_body` and
    `capture.recorded_stream` say what of a cut body is recorded.
    `tests/engine/test_bounded_decoding.py` measures the bound.

    The encodings are mitmproxy's own: gzip, deflate (and its raw form), br, zstd and the
    identities, each as lenient as mitmproxy's decoder of it. Any other name is not decoded.
    mitmproxy's fallback runs it through Python's codecs, where `bz2` and `zlib` are unbounded
    decompressions of their own and `utf-8` is not a decoding at all."""
    name = content_encoding.lower()
    if name in _IDENTITY_ENCODINGS:
        return Decoded(body, False)
    decode = _DECODERS.get(name)
    if decode is None:
        return None
    if not body:  # nothing, as mitmproxy's decoders say, though brotli's would refuse it
        return Decoded(body, False)
    try:
        decoded, cut = decode(body, MAX_DECODED_BODY)
    except Exception:
        return None
    return Decoded(decoded, cut)


def _inflate(body: bytes, limit: int, wbits: int, *, whole: bool) -> tuple[bytes, bool]:
    """At most `limit` bytes of a zlib-family stream, and whether there was more. `whole` refuses
    a stream that ends early, as `zlib.decompress` does; without it the bytes that did arrive are
    kept, as mitmproxy's gzip decoder keeps them."""
    inflater = zlib.decompressobj(wbits)
    out = inflater.decompress(body, limit)
    if len(out) == limit and not inflater.eof and inflater.decompress(inflater.unconsumed_tail, 1):
        return out, True
    # Every input byte was consumed below the limit, or at it with nothing more to decode, so what
    # is left inside is a short tail. A stream that ends right at the limit is still refused here
    # when it is not whole, as one that ends anywhere else is.
    out += inflater.flush()
    if whole and not inflater.eof:
        raise zlib.error("incomplete or truncated stream")
    return out[:limit], len(out) > limit


def _gzip(body: bytes, limit: int) -> tuple[bytes, bool]:
    return _inflate(body, limit, 32 + zlib.MAX_WBITS, whole=False)  # gzip or zlib, detected


def _deflate(body: bytes, limit: int) -> tuple[bytes, bool]:
    """With a zlib header, as RFC 9110 says, or without one, as some servers send it."""
    try:
        return _inflate(body, limit, zlib.MAX_WBITS, whole=True)
    except zlib.error:
        return _inflate(body, limit, -zlib.MAX_WBITS, whole=True)


# brotli's `output_buffer_limit` stops its buffer growing once it reaches the limit, by however
# much the last block took it past: a quarter again at 64 MiB. Asked for this much at a time, it
# is never more than this past.
_BROTLI_STEP = 1024 * 1024


def _brotli(body: bytes, limit: int) -> tuple[bytes, bool]:
    """Refuses a stream that ends early, as `brotli.decompress` does. The input goes in on the
    first call, and every later one passes nothing and takes more out (brotli 1.2's contract)."""
    decompressor = brotli.Decompressor()
    parts: list[bytes] = []
    size = 0
    data = body
    while size <= limit and not decompressor.is_finished():
        part: bytes = decompressor.process(data, output_buffer_limit=_BROTLI_STEP)
        data = b""
        if not part:
            break
        parts.append(part)
        size += len(part)
    if not decompressor.is_finished() and size <= limit:
        raise brotli.error("incomplete or truncated stream")
    if size > limit:  # cut the last part, not the joined whole: a third copy of the cap
        parts[-1] = parts[-1][: len(parts[-1]) - (size - limit)]
    return b"".join(parts), size > limit


def _zstd(body: bytes, limit: int) -> tuple[bytes, bool]:
    """One read of a byte past `limit`, which fills unless the frames end first. Not `limit` and
    then one more: a second read comes back empty when the first stopped inside a block of a
    stream that is cut short, and a body cut at the cap would be called whole."""
    reader = zstandard.ZstdDecompressor().stream_reader(io.BytesIO(body), read_across_frames=True)
    out: bytes = reader.read(limit + 1)
    if len(out) > limit:
        return out[:limit], True
    return out, False


_DECODERS: dict[str, Callable[[bytes, int], tuple[bytes, bool]]] = {
    "gzip": _gzip,
    "deflate": _deflate,
    "deflateraw": _deflate,
    "br": _brotli,
    "zstd": _zstd,
}
