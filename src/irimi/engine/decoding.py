"""Bounded decoding of a body's `Content-Encoding`.

A few KB of gzip, br or zstd can decode to gigabytes. Each codec here runs incrementally and stops
at MAX_DECODED_BODY. Never raises (fail-closed hook rule): hooks decode every recorded body here.
"""

import io
import zlib
from collections.abc import Callable
from dataclasses import dataclass

import brotli
import zstandard

from irimi.core.limits import MAX_DECODED_BODY

_IDENTITY_ENCODINGS = frozenset({"identity", "none"})


@dataclass(frozen=True)
class Decoded:
    body: bytes
    cut: bool  # decoding stopped at MAX_DECODED_BODY: `body` is the start of it, not all of it


def decode_body(body: bytes, content_encoding: str) -> Decoded | None:
    """Return `body` decoded up to MAX_DECODED_BODY, or None when it cannot be decoded.

    Never raises; holds at most twice the cap. Other encodings than `_DECODERS` and the identities
    return None, since mitmproxy's codec fallback is unbounded.
    """
    name = content_encoding.lower()
    if name in _IDENTITY_ENCODINGS:
        return Decoded(body, False)
    decode = _DECODERS.get(name)
    if decode is None:
        return None
    if not body:  # brotli would refuse an empty body; mitmproxy's decoders accept it
        return Decoded(body, False)
    try:
        decoded, cut = decode(body, MAX_DECODED_BODY)
    except Exception:
        return None
    return Decoded(decoded, cut)


def _inflate(body: bytes, limit: int, wbits: int, *, whole: bool) -> tuple[bytes, bool]:
    """Return up to `limit` inflated bytes and whether more remain; `whole` refuses a cut stream."""
    inflater = zlib.decompressobj(wbits)
    out = inflater.decompress(body, limit)
    if len(out) == limit and not inflater.eof and inflater.decompress(inflater.unconsumed_tail, 1):
        return out, True
    # Only a short tail is left inside, so flushing it stays bounded.
    out += inflater.flush()
    if whole and not inflater.eof:
        raise zlib.error("incomplete or truncated stream")
    return out[:limit], len(out) > limit


def _gzip(body: bytes, limit: int) -> tuple[bytes, bool]:
    return _inflate(body, limit, 32 + zlib.MAX_WBITS, whole=False)  # gzip or zlib, detected


def _deflate(body: bytes, limit: int) -> tuple[bytes, bool]:
    """Decode deflate with a zlib header (RFC 9110) or without one, as some servers send it."""
    try:
        return _inflate(body, limit, zlib.MAX_WBITS, whole=True)
    except zlib.error:
        return _inflate(body, limit, -zlib.MAX_WBITS, whole=True)


# brotli's `output_buffer_limit` can overshoot by one block; small steps bound the overshoot.
_BROTLI_STEP = 1024 * 1024


def _brotli(body: bytes, limit: int) -> tuple[bytes, bool]:
    """Decode br, refusing a stream that ends early."""
    decompressor = brotli.Decompressor()
    parts: list[bytes] = []
    size = 0
    data = body
    while size <= limit and not decompressor.is_finished():
        # Input goes in on the first call only; later calls drain output (brotli 1.2's contract).
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
    """Decode zstd in one read of `limit + 1`; a second read misses a stream cut inside a block."""
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
