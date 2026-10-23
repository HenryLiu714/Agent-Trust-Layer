"""mitmproxy-backed Engine. The only module allowed to import mitmproxy."""

import asyncio
import errno
import io
import logging
import socket
import time
import zlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from urllib.parse import urlsplit

import brotli  # type: ignore[import-untyped]  # ships no stubs
import zstandard
from mitmproxy import ctx, http, tls
from mitmproxy.addons import default_addons
from mitmproxy.master import Master
from mitmproxy.options import Options

from irimi import ca, control, delegation, netaddr, pipeline, redact, reverse_door
from irimi.control import ControlEndpoint, OnToolCall
from irimi.engine import EngineConfig, EngineStartError, OnExchange, OnTlsFailure
from irimi.exchange import (
    BODY_TRUNCATED_FLAG,
    DECISION_FAILED_FLAG,
    FIDELITY_FLAGS,
    LIVE_KINDS,
    REFUSED_FLAG,
    STREAM_TRUNCATED_FLAG,
    TARGET_FAILED_FLAG,
    UPSTREAM_ERROR_FLAG,
    AnsweredBy,
    Door,
    Exchange,
    Headers,
    OverlayFidelity,
    Request,
    Response,
    clip_chunks,
    is_authored_write,
    media_type,
)
from irimi.overlay import Overlaid, Overlay, Translation
from irimi.policy import Answer, AnswerPolicy
from irimi.store import MAX_REDACTED_BODY, MAX_STORED_BODY, TraceStore
from irimi.writelog import WriteLog

logger = logging.getLogger(__name__)

META_KEY = "irimi"  # flow.metadata slot holding the per-flow state below
STREAM_KEY = "irimi-stream"  # flow.metadata slot holding a streamed response's `_StreamTee`
# flow.metadata slot set when the request's body was decoded only up to MAX_DECODED_BODY.
REQUEST_CUT_KEY = "irimi-request-cut"

# Called once from the running hook with (bound port, None) or (None, why it failed).
OnRunning = Callable[[int | None, EngineStartError | None], None]


@dataclass(frozen=True)
class _Pending:
    """What `request()` decided about a flow, for the hooks that finish it. `flags` is the
    answer's own plus the cut request's; everything else L3, the webhooks and the currency said
    is read off `answer`. `started_at` is when `request()` first parsed the flow."""

    request: Request
    classification: pipeline.Classification
    run_id: str
    door: Door
    answer: Answer
    flags: tuple[str, ...]
    started_at: float
    target: str = ""  # the answer target this flow was pointed at; "" when irimi answered it
    translated_from: str | None = None  # the query pair the overlay dropped; see `_Decision`

    @property
    def answered_by(self) -> AnsweredBy:
        return self.answer.answered_by


def _exchange(
    pending: _Pending,
    response: Response | None,
    answered_by: AnsweredBy,
    flags: tuple[str, ...],
    overlay: OverlayFidelity | None = None,
    stream_chunks: tuple[int, ...] = (),
) -> Exchange:
    """The Exchange for a flow `request()` decided about, carrying its answer's L3 outcome,
    rejection code, webhooks and currency, and a streamed body's chunks.

    It ends now: every caller is the hook that finishes the flow, and builds it just before
    `_finish`. Stamped here and not in `_finish`, which also records the engine's own reads,
    whose `ended_at` is their `Reader` call's, and whose write-log copy must be the same object."""
    answer = pending.answer
    return pipeline.annotate(
        pending.request,
        response,
        pending.classification,
        answered_by,
        pending.run_id,
        extra_flags=flags,
        door=pending.door,
        target=pending.target,
        overlay=overlay,
        precondition=answer.precondition,
        rejection_code=answer.rejection_code,
        would_fire=answer.would_fire,
        currency=answer.currency,
        started_at=pending.started_at,
        ended_at=time.time(),
        stream_chunks=stream_chunks,
    )


class _StreamTee:
    """`flow.response.stream` for a streamed response: hands each chunk on unchanged, and keeps a
    copy of it for the trace.

    mitmproxy calls it with each chunk as it arrives and once more with `b""` at the end of the
    message, and sends the client whatever it returns. So THE CHUNK IS RETURNED WHATEVER HAPPENS
    HERE: the copy is best-effort, and the agent's stream is not. mitmproxy does not guard the
    call, so a raise would break the agent's stream, not just the recording.

    The copy stops at MAX_STORED_BODY, the most the store keeps of any body: later chunks still
    reach the client and are not kept. It also stops for good the first time copying raises.
    Either way `truncated` is set, and the exchange is flagged STREAM_TRUNCATED_FLAG.
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


def _recorded_stream(
    flow: http.HTTPFlow, tee: _StreamTee, *, whole: bool
) -> tuple[Response, tuple[int, ...], bool]:
    """A streamed response as the trace records it: the flow's status and headers, the body `tee`
    kept, the length of each chunk, and whether that body is short of the whole stream.
    `whole` is False when the stream ended in the `error` hook. Nothing is written to the flow,
    and nothing here raises.

    A BODY THAT IS NOT THE WHOLE STREAM IS KEPT UP TO ITS LAST COMPLETE LINE. The copy stops at an
    arbitrary byte - the cap, the last read before a reset - and a cut inside a UTF-8 character
    makes the whole body undecodable, which redaction stores unscanned: every key in it
    would reach disk. A cut inside a line would also store half of a secret on it. Redaction reads
    a stream line by line, so a body of whole lines is one it can read.

    A `Content-Encoding` is decoded, as `_body` decodes a buffered body, so that redaction reads
    text rather than gzip; decoded bytes do not split where the wire's chunks did, so such a body
    is one chunk. One that cannot be decoded - a cut `br` stream, an encoding irimi does not
    decode - is recorded empty and flagged, never as the compressed bytes redaction cannot read.
    One that decodes past MAX_DECODED_BODY keeps the start of what was decoded, and is flagged
    too."""
    assert flow.response is not None
    body = b"".join(tee.chunks)
    chunks = tuple(len(chunk) for chunk in tee.chunks)
    cut = tee.truncated or not whole
    content_encoding = flow.response.headers.get("content-encoding", "")
    if content_encoding and body:
        decoded = _decoded(body, content_encoding)
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
    headers = _headers_from_fields(flow.response.headers.fields)
    return Response(status=flow.response.status_code, headers=headers, body=body), chunks, cut


# THE MOST A BODY IS DECODED TO. A few KB of gzip, br or zstd can decode to gigabytes, and a
# hook that decoded it whole would stall every flow behind it and could exhaust memory. A body
# decodes up to this many bytes and no further, and the exchange is flagged: `body-truncated`, or
# `stream-truncated` for a stream. It is the store's own bound, because past it the store keeps no
# body at all (`store._as_queued`).
MAX_DECODED_BODY = MAX_REDACTED_BODY

# What a body's identity encodings decode to: the bytes as they came, which are already in memory
# and are no bigger for being "decoded".
_IDENTITY_ENCODINGS = frozenset({"identity", "none"})


@dataclass(frozen=True)
class _Decoded:
    body: bytes
    cut: bool  # decoding stopped at MAX_DECODED_BODY: `body` is the start of it, not all of it


def _decoded(body: bytes, content_encoding: str) -> _Decoded | None:
    """`body` decoded per `content_encoding` up to MAX_DECODED_BODY, or None when it cannot be.

    DECODING NEVER RAISES: every body the hooks record is decoded here, because a raise out of a
    hook drops the exchange or forwards the flow. Nor is it ever unbounded: each codec runs
    incrementally, stops at the cap and asks for one byte more to learn whether it was cut. So the
    hook holds at most twice the cap, while a codec assembles its output into one bytes object,
    and never what a few KB of a bomb would have decoded to. `_body` and `_recorded_stream`
    say what of a cut body is recorded. `tests/test_bounded_decoding.py` measures the bound.

    The encodings are mitmproxy's own: gzip, deflate (and its raw form), br, zstd and the
    identities, each as lenient as mitmproxy's decoder of it. Any other name is not decoded.
    mitmproxy's fallback runs it through Python's codecs, where `bz2` and `zlib` are unbounded
    decompressions of their own and `utf-8` is not a decoding at all."""
    name = content_encoding.lower()
    if name in _IDENTITY_ENCODINGS:
        return _Decoded(body, False)
    decode = _DECODERS.get(name)
    if decode is None:
        return None
    if not body:  # nothing, as mitmproxy's decoders say, though brotli's would refuse it
        return _Decoded(body, False)
    try:
        decoded, cut = decode(body, MAX_DECODED_BODY)
    except Exception:
        return None
    return _Decoded(decoded, cut)


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


@dataclass(frozen=True)
class _Decision:
    """What the worker thread decided, with nothing of mitmproxy's in it.

    `request` is the request as the trace should show it - rewritten, if the overlay translated a
    cursor, whoever answered it. `rewritten` is that same object when the read is forwarded
    live with a translation, which is what tells the loop side there is a flow to edit, and None
    otherwise. `translated_from` is the query pair the translation dropped, which the overlay
    is handed back with the response; it stays in irimi and never reaches the service.

    `failure` is set when irimi will not answer the request as decided: the decision raised, or the
    body was cut. `target_refused` is set when the answer names a target irimi refused or could
    not reach. Either way the request is answered with irimi's own 502.
    """

    request: Request
    classification: pipeline.Classification
    run_id: str
    answer: Answer
    rewritten: Request | None
    translated_from: str | None = None
    failure: Exception | None = None
    target_refused: delegation.TargetRefused | None = None


def _failed_decision(
    req: Request, exc: Exception, classification: pipeline.Classification | None = None
) -> _Decision:
    """A decision irimi answers with its own 502, recorded under `classification`, or as
    unclassified when it never got that far. Only `request`, `classification` and `failure` are
    read; the rest exist because the dataclass is frozen and total."""
    return _Decision(
        req,
        classification or pipeline.unclassified(req),
        "",
        Answer(answered_by="irimi", response=None),
        None,
        failure=exc,
    )


class RequestBodyCut(Exception):
    """A write whose body decoded past MAX_DECODED_BODY, which `_decide` refuses to answer."""


def _request_line(flow: http.HTTPFlow) -> Request:
    """The flow's request line alone, for recording a request whose full parse failed, or an
    empty request when even that fails."""
    try:
        r = flow.request
        return pipeline.parse(r.method, r.scheme, r.host, r.port, r.path, (), b"")
    except Exception:
        return pipeline.parse("", "", "", 0, "/", (), b"")


def _request_cut_flags(flow: http.HTTPFlow) -> tuple[str, ...]:
    """`body-truncated` when the request hook decoded this flow's request body only up to the cap,
    so the exchange it becomes says its recorded request is not all of it."""
    return (BODY_TRUNCATED_FLAG,) if flow.metadata.get(REQUEST_CUT_KEY) else ()


def _point_flow(
    flow: http.HTTPFlow, scheme: str, host: str, port: int, path: str, host_header: str
) -> None:
    """Send this flow to `scheme://host:port/path` instead. mitmproxy opens the server connection
    after the request hook, so rewriting the flow there IS the forward. The Host header is set
    explicitly: the host/port setters only rewrite one that already exists."""
    flow.request.scheme = scheme
    flow.request.host = host
    flow.request.port = port
    flow.request.path = path
    flow.request.host_header = host_header


def _strip_header(flow: http.HTTPFlow, req: Request, name: str) -> Request:
    """Take every `name` header off the flow and off the recorded request: irimi's own wire
    vocabulary reaches neither the service nor the trace. `del` on mitmproxy's
    `Headers` removes every instance, in any spelling. The same `req` back when there was none,
    so the common path allocates nothing."""
    if name not in flow.request.headers:
        return req
    del flow.request.headers[name]
    return req.without_header(name)


def _headers_from_fields(fields: Sequence[tuple[bytes, bytes]]) -> Headers:
    return tuple((k.decode("latin-1"), v.decode("latin-1")) for k, v in fields)


def _body(message: http.Message) -> _Decoded:
    """A buffered body as the trace records it, and whether its decoding stopped at the cap.

    A body is recorded as it came when its Content-Encoding cannot be decoded: an exception in a
    hook makes mitmproxy forward the flow untouched, which for a write means it escapes shadow
    mode, and one in `response` drops the exchange from the trace.

    A BODY THAT DECODES PAST MAX_DECODED_BODY IS RECORDED EMPTY, as the store records any
    body past `store.MAX_REDACTED_BODY`, the same bound. A stored body is a prefix of the whole
    body redacted, never the start of one redacted on its own: the start of a JSON document is
    not JSON, redaction reads it line by line, and `"password": "..."` is a line no shape gives
    away. A stream is kept to its lines instead (`_recorded_stream`), because it is redacted by
    line whole or not."""
    raw = message.raw_content or b""
    content_encoding = message.headers.get("content-encoding", "")
    if not raw or not content_encoding:
        return _Decoded(raw, False)
    decoded = _decoded(raw, content_encoding)
    if decoded is None:
        return _Decoded(raw, False)
    if decoded.cut:
        return _Decoded(b"", True)
    return decoded


def _request_from_flow(flow: http.HTTPFlow) -> tuple[Request, bool]:
    """The flow's request, and whether its body's decoding stopped at the cap."""
    body = _body(flow.request)
    request = pipeline.parse(
        flow.request.method,
        flow.request.scheme,
        flow.request.host,
        flow.request.port,
        flow.request.path,
        _headers_from_fields(flow.request.headers.fields),
        body.body,
    )
    return request, body.cut


def _response_from_flow(flow: http.HTTPFlow) -> tuple[Response, bool]:
    """The flow's response, and whether its body's decoding stopped at the cap."""
    assert flow.response is not None
    body = _body(flow.response)
    response = Response(
        status=flow.response.status_code,
        headers=_headers_from_fields(flow.response.headers.fields),
        body=body.body,
    )
    return response, body.cut


def _is_event_stream(content_type: str) -> bool:
    return media_type(content_type) == "text/event-stream"


def _fields(headers: Headers) -> list[tuple[bytes, bytes]]:
    # A list of pairs keeps repeated headers (Set-Cookie); a dict would collapse them.
    return [(k.encode("latin-1"), v.encode("latin-1")) for k, v in headers]


def _to_mitm_response(r: Response) -> http.Response:
    # ENCODING NEVER RAISES. `make` re-encodes the body per its `content-encoding`, and mitmproxy
    # drops an encoding it cannot apply when that is a ValueError but lets the TypeError of a text
    # codec through (`rot13`, `utf-8`). An overlay keeps the upstream's headers, and `_body`
    # records such a body raw rather than raising, so a rebuilt read can reach here with one and
    # would raise out of the `response` hook. The body goes out as it is, without the encoding, as
    # mitmproxy's own fallback sends it.
    try:
        return http.Response.make(r.status, r.body, _fields(r.headers))
    except TypeError:
        fields = [(k, v) for k, v in _fields(r.headers) if k.lower() != b"content-encoding"]
        return http.Response.make(r.status, r.body, fields)


def _send(flow: http.HTTPFlow, out: Response, upstream: Response) -> None:
    """Put `out` on the flow, without re-encoding a body nothing changed.

    `http.Response.make` assigns `.content`, and mitmproxy's `set_content` re-encodes it per the
    surviving `content-encoding` header. That is right for a body we built and wrong for one we
    are handing back: `_body` returns the RAW bytes when `Content-Encoding` cannot be decoded, so
    rebuilding would take bytes that are already compressed - or were never valid gzip - and
    compress them again, and the agent would receive different bytes than the target sent.

    `respond` returns a new Response for every non-live answer, a delegated one included. When
    the body is the very object we read off the flow, only the headers changed - that is all
    `respond` does - so only the headers are written back and the wire bytes are left alone.
    """
    assert flow.response is not None
    if out.body is upstream.body:
        flow.response.status_code = out.status
        flow.response.headers = http.Headers(fields=_fields(out.headers))
        return
    flow.response = _to_mitm_response(out)


# How long the probe below waits for a target to accept a connection. It runs on the decision's
# worker thread, so a slow target stalls only the request that names it.
TARGET_PROBE_TIMEOUT_S = 2.0


def _probe_target(url: str) -> None:
    """Raise TargetUnreachable when nothing accepts a connection at an answer target.

    `_to_target` rewrites the flow and lets mitmproxy dial the target, which keeps a streamed
    target response streaming. When that dial fails, mitmproxy answers with its own HTML page,
    which names neither irimi nor the target, and no hook can replace it. So the dial is tried
    here first, and a target that cannot be reached gets irimi's JSON 502 instead. Best-effort: a
    target that dies between the probe and the dial still gets mitmproxy's page.
    """
    parts = urlsplit(url)
    port = parts.port or (443 if parts.scheme == "https" else 80)
    try:
        socket.create_connection((parts.hostname or "", port), TARGET_PROBE_TIMEOUT_S).close()
    except OSError as exc:
        raise delegation.TargetUnreachable(
            f"answer target {url!r} could not be reached: {exc}"
        ) from None


def _target_refusal(
    forward: delegation.ForwardTo, listener: tuple[str, int]
) -> delegation.TargetRefused | None:
    """Why irimi will not send this request to its answer target, or None when it may: the target
    is irimi's own listener, which would make the engine dial itself in a loop, or it cannot be
    reached. On the decision's worker thread, so the probe never blocks the event loop."""
    try:
        delegation.refuse_self_target(forward.url, listener[1], listener[0])
        _probe_target(forward.url)
    except delegation.TargetRefused as exc:
        return exc
    except Exception as exc:
        return delegation.TargetRefused(
            f"answer target {forward.url!r} could not be applied: {exc}"
        )
    return None


def _start_failure(config: EngineConfig, cause: BaseException | None) -> str:
    """Why the listener did not bind, in irimi's words: mitmproxy's own message names its own
    `--mode` flag, which irimi does not have."""
    where = netaddr.authority(config.listen_host, config.listen_port)
    if isinstance(cause, OSError) and cause.errno == errno.EADDRINUSE:
        return (
            f"port {config.listen_port} on {config.listen_host} is in use; pick another with "
            "--port, or --port 0 for any free port"
        )
    if isinstance(cause, OSError) and cause.strerror:
        return f"could not listen on {where}: {cause.strerror.lower()}"
    return f"could not listen on {where}" + ("" if cause is None else f": {cause}")


class IrimiAddon:
    """The mitmproxy addon: runs the pipeline on every flow. One instance per engine."""

    def __init__(
        self,
        config: EngineConfig,
        policy: AnswerPolicy,
        store: TraceStore,
        overlay: Overlay,
        on_exchange: OnExchange | None,
        on_running: OnRunning,
        on_tool_call: OnToolCall | None = None,
        write_log: WriteLog | None = None,
        on_tls_failure: OnTlsFailure | None = None,
    ) -> None:
        self.config = config
        self.policy = policy
        self.store = store
        self.overlay = overlay
        self.on_exchange = on_exchange
        self.on_running = on_running
        self.on_tls_failure = on_tls_failure
        # The policy that decides the wire answers the SDK's read tools there too: from a
        # replay's recording, or from nothing.
        self.control = ControlEndpoint(
            store,
            serve=config.serve,
            process_run=config.run_id,
            on_tool_call=on_tool_call,
            policy=policy,
        )
        self.write_log = write_log if write_log is not None else WriteLog()

    def running(self) -> None:
        # Without mitmproxy's ErrorCheck addon a failed bind does not sys.exit(); Master.run()
        # still reaches this hook with no listener, so report the outcome ourselves.
        proxyserver = ctx.master.addons.get("proxyserver")
        addrs = proxyserver.listen_addrs()
        if addrs:
            self.on_running(addrs[0][1], None)
            return
        cause = next((s.last_exception for s in proxyserver.servers if s.last_exception), None)
        self.on_running(None, EngineStartError(_start_failure(self.config, cause)))
        ctx.master.shutdown()

    def tls_failed_client(self, data: tls.TlsData) -> None:
        """Report a client that refused irimi's certificate, by the host it asked for. No flow
        exists for it, so nothing else would say why the agent's call never arrived."""
        if self.on_tls_failure is None:
            return
        try:
            address = data.context.server.address
            host = data.conn.sni or (address[0] if address is not None else "an unnamed host")
            self.on_tls_failure(str(host))
        except Exception:
            logger.debug("irimi: could not report a client TLS failure", exc_info=True)

    async def request(self, flow: http.HTTPFlow) -> None:
        # When this exchange began, for the trace. Read before anything else the hook does,
        # the reverse door and the decision included, so the span is the whole of irimi's part.
        started_at = time.time()
        try:
            req, cut = _request_from_flow(flow)
        except Exception as exc:  # never fail open: an unparseable request is answered locally
            logger.warning("irimi: refusing request that could not be parsed: %s", exc)
            self._refuse(
                flow,
                _request_line(flow),
                "forward",
                started_at,
                400,
                "could not parse this request",
                parsed=False,
            )
            return
        if cut:  # the exchange this flow becomes says its request body is not all there
            flow.metadata[REQUEST_CUT_KEY] = True
        # sockname is the socket this request arrived on, i.e. our own listener: its port, and the
        # address it was reached at, which is this machine too when the bind is wider than
        # loopback. Guarded like the rewrite below: a raise here would forward the flow, and
        # a request this check exists for is one addressed to irimi itself.
        try:
            local_address, local_port = flow.client_conn.sockname[:2]
            listener = (str(local_address), int(local_port))
            door = reverse_door.detect_door(req, listener[1], listener[0])
        except Exception as exc:  # never fail open: a request whose door is unknown may be ours
            logger.warning(
                "irimi: could not tell whether this is a proxy or base-URL request: %s", exc
            )
            self._refuse(flow, req, "forward", started_at, 400, "could not route this request")
            return
        # THE CONTROL ENDPOINT is answered here and returns, before the reverse door, which
        # would read `_irimi` as an upstream host and refuse it, and before `Irimi-Run` is read: a
        # control request names its run in its path. It is never forwarded, never decided and
        # never recorded as an exchange.
        if control.is_control_request(req, door):
            self._answer_control(flow, req, whole_body=not cut)
            return
        if door == "reverse":
            try:
                req = self._through_reverse_door(flow, req)
            except reverse_door.ReverseDoorRefused as exc:
                logger.warning("irimi: %s", exc)
                self._refuse(flow, req, door, started_at, 403, str(exc))
                return
            except Exception as exc:  # never fail open: an unrewritten flow would go to ourselves
                logger.warning(
                    "irimi: refusing a base-URL request that could not be rewritten: %s", exc
                )
                self._refuse(flow, req, door, started_at, 400, "could not rewrite this request")
                return
        # IRIMI'S OWN WIRE VOCABULARY IS NEVER THE AGENT'S. irimi sends no `Irimi-Rewrote`, so
        # one the agent sent is taken off the flow and the recorded request, as `Irimi-Run` is
        # below. Guarded, like everything else in this hook: a raise here would forward the flow
        # untouched - a write included - while a header left on is a lesser harm.
        try:
            req = _strip_header(flow, req, pipeline.REWROTE_HEADER)
        except Exception as exc:
            logger.warning(
                "irimi: could not strip an agent-sent %s: %s", pipeline.REWROTE_HEADER, exc
            )
        # IRIMI-RUN STOPS HERE. It names the run this request belongs to, which only irimi
        # asks, so it is read first and then taken off the flow and the recorded request -
        # `Exchange.run_id` holds the value. Left on, every live forward, answer target and L3
        # read would carry the agent's run id to the service, and the SDK sets it on every request
        # a run makes. Guarded like the strip above: if it raises, the run is still attributed and
        # the header is merely left on.
        run_id = self.config.run_id
        try:
            run_id = pipeline.attribute_run(req, run_id)
            req = _strip_header(flow, req, pipeline.RUN_HEADER)
        except Exception as exc:
            logger.warning("irimi: could not strip %s: %s", pipeline.RUN_HEADER, exc)
        # THE NEVER-RAISE RULE, stated once for the whole decision rather than per call site.
        #
        # mitmproxy forwards a flow untouched when a hook raises. Every statement between here
        # and `flow.metadata[META_KEY] = ...` decides whether this request reaches the real
        # service, so a raise anywhere in it sends the agent's request - a write included - to
        # production, and `_Pending` is never set, so `response()` returns early and no Exchange
        # is recorded either. The write is performed for real AND is invisible in the trace.
        #
        # So the decision fails closed as a whole: any exception answers locally with a 502 and
        # the `decision-failed` flag, and nothing is forwarded. Individual pieces still guard
        # themselves where they can say something more useful (`_act_on` names the target it
        # could not apply); this is the backstop that makes the rule hold for pieces that do not,
        # and for every piece added to this path.
        #
        # The decision runs on a worker thread. The hook is `async def`, which mitmproxy 12
        # awaits, so the proxy serves every other flow while one write's L3 precondition read or
        # a target's probe is in the air. Nothing inside `_decide` may touch `flow`: mitmproxy's
        # flow objects belong to the event loop. The run's writes are read here, before the
        # hand-off; the tuple is the log's own and never changes.
        writes = self.write_log.for_run(run_id)
        try:
            decision = await asyncio.to_thread(self._decide, req, writes, run_id, cut, listener)
        except Exception as handoff:  # the hand-off itself, e.g. an executor shut down under us
            decision = _failed_decision(req, handoff)
        if decision.failure is None and decision.rewritten is not None:
            # Part of the decision, so under its backstop. A raise here would escape the hook and
            # forward a half-edited read with no Exchange recorded; like any other part of the
            # decision, it is answered locally.
            try:
                self._apply_rewrite(flow, decision.rewritten)
            except Exception as exc:
                decision = _failed_decision(req, exc)
        if decision.failure is not None:
            self._answer_failed_decision(
                flow, req, decision.classification, door, run_id, started_at, decision.failure
            )
            return
        self._act_on(flow, decision, door, started_at)

    def _answer_control(self, flow: http.HTTPFlow, req: Request, *, whole_body: bool) -> None:
        """Answer a control request. `ControlEndpoint.answer` never raises; this guards turning
        its answer into mitmproxy's anyway, because a control request that escaped this hook would
        be forwarded to the listener it was addressed to - irimi itself. `whole_body` is
        False when the request's body decoded past the cap, so `req.body` is not it."""
        # A RESPONSE TO HEAD CARRIES NO CONTENT (RFC 9110 9.3.2), and mitmproxy sends whatever the
        # response holds: `HEAD /_irimi/health`'s 405 JSON would be read as the start of the next
        # response on the connection. Nor a Content-Length, which a HEAD answer may only send when
        # it is what a GET would get (8.6), and a HEAD's 405 is not that.
        head = req.method == "HEAD"
        try:
            response = _to_mitm_response(self.control.answer(req, whole_body=whole_body))
            if head:
                response.raw_content = b""
                response.headers.pop("content-length", None)
        except Exception:
            logger.exception("irimi: could not send the control endpoint's answer")
            error = control.INTERNAL_ERROR
            response = http.Response.make(
                error.status, b"" if head else error.body, _fields(error.headers)
            )
            if head:
                response.headers.pop("content-length", None)
        flow.response = response

    def _answer_with_error(
        self,
        flow: http.HTTPFlow,
        req: Request,
        classification: pipeline.Classification,
        door: Door,
        run_id: str,
        started_at: float,
        *,
        status: int,
        error_type: str,
        message: str,
        flags: tuple[str, ...],
        target: str = "",
    ) -> None:
        """Answer `flow` with irimi's own JSON error, stamped `irimi`, and record it flagged with
        why. The one path for every error irimi answers with itself.

        The response is on the flow before `_finish` runs, so an `on_exchange` that raises there
        escapes the hook with the error already set, never with the flow bare."""
        response = pipeline.irimi_error(status, error_type, message)
        flow.response = _to_mitm_response(pipeline.stamped(response, pipeline.IRIMI_ANSWER))
        ex = pipeline.annotate(
            req,
            response,
            classification,
            pipeline.IRIMI_ANSWER,
            run_id,
            extra_flags=flags,
            door=door,
            target=target,
            started_at=started_at,
            ended_at=time.time(),
        )
        self._finish(ex)

    def _refuse(
        self,
        flow: http.HTTPFlow,
        req: Request,
        door: Door,
        started_at: float,
        status: int,
        reason: str,
        *,
        parsed: bool = True,
    ) -> None:
        """The request hook's answer to a request it will not decide about, flagged `refused` and
        recorded under the run it names. A parsed request keeps its usual classification, so a
        refused GET is not counted as a write; one irimi could not parse is recorded unclassified.
        Nothing of it is forwarded, so only the recorded request loses irimi's own headers."""
        run_id = self.config.run_id
        try:
            run_id = pipeline.attribute_run(req, run_id)
            req = req.without_header(pipeline.RUN_HEADER).without_header(pipeline.REWROTE_HEADER)
        except Exception:
            logger.debug("irimi: could not attribute a refused request", exc_info=True)
        classification = pipeline.unclassified(req)
        if parsed:
            try:
                classification = pipeline.classify(req, self.config.maps)
            except Exception:
                logger.debug("irimi: could not classify a refused request", exc_info=True)
        self._answer_with_error(
            flow,
            req,
            classification,
            door,
            run_id,
            started_at,
            status=status,
            error_type="irimi_refused",
            message=f"irimi refused this request: {reason}",
            flags=(REFUSED_FLAG, *_request_cut_flags(flow)),
        )

    def _answer_failed_decision(
        self,
        flow: http.HTTPFlow,
        req: Request,
        classification: pipeline.Classification,
        door: Door,
        run_id: str,
        started_at: float,
        failure: Exception,
    ) -> None:
        """The request hook's answer when irimi will not answer as decided: a 502 flagged
        `decision-failed`, stamped and recorded. A body cut at the cap keeps the classification
        it was refused under; a decision that raised is recorded unclassified."""
        if isinstance(failure, RequestBodyCut):  # the agent's body, not irimi's failure
            logger.warning("irimi: refused write: %s", failure)
        else:
            logger.exception("irimi: answering locally; the decision raised", exc_info=failure)
        self._answer_with_error(
            flow,
            req,
            classification,
            door,
            run_id,
            started_at,
            status=502,
            error_type="irimi_decision_failed",
            message=f"irimi could not decide how to answer this request: {failure}",
            flags=(DECISION_FAILED_FLAG, *_request_cut_flags(flow)),
        )

    def _act_on(
        self, flow: http.HTTPFlow, decision: _Decision, door: Door, started_at: float
    ) -> None:
        """The request hook's last part: carry out a decision that did not raise.

        Every step that could leave the flow bare is guarded. The one thing that may still raise is
        reporting the engine's own reads (`_finish` for `ans.issued`, whose `on_exchange` is not
        guarded), which is why it runs last, once the write's own answer is on the flow."""
        req = decision.request
        cls, run_id, ans = decision.classification, decision.run_id, decision.answer
        flags: tuple[str, ...] = ans.flags + _request_cut_flags(flow)
        target = ans.forward_to.url if ans.forward_to is not None else ""
        target_error = None if decision.target_refused is None else str(decision.target_refused)
        if ans.forward_to is not None and target_error is None:
            try:
                self._to_target(flow, ans.forward_to)
            except Exception as exc:  # never fail open: an unrewritten flow goes to the real API
                target_error = f"answer target {target!r} could not be applied: {exc}"
        if target_error is not None:
            # Never a silent fall back to the local fake, which would hide a broken setup and
            # look like a working shadow run. Nothing answered faithfully, so the answer's
            # fidelity flag goes and `target-failed` says why.
            logger.warning("irimi: %s", target_error)
            self._answer_with_error(
                flow,
                req,
                cls,
                door,
                run_id,
                started_at,
                status=502,
                error_type="irimi_target_failed",
                message=target_error,
                flags=(
                    *(f for f in flags if f not in FIDELITY_FLAGS.values()),
                    TARGET_FAILED_FLAG,
                ),
                target=target,
            )
        else:
            built: http.Response | None = None
            refused: Exception | None = None
            if ans.response is not None:
                # Guarded, because the answer may not be irimi's own: a replay answers with a
                # response read off disk, whose headers mitmproxy may refuse to encode. A raise
                # here would forward the flow - the write included - so it fails closed like a
                # decision that did.
                try:
                    built = _to_mitm_response(ans.response)
                except Exception as exc:
                    refused = exc
            if refused is not None:
                self._answer_failed_decision(
                    flow, req, pipeline.unclassified(req), door, run_id, started_at, refused
                )
            else:
                flow.metadata[META_KEY] = _Pending(
                    req,
                    cls,
                    run_id,
                    door,
                    ans,
                    flags,
                    started_at,
                    target,
                    decision.translated_from,
                )
                if built is not None:
                    flow.response = built
        for ex in ans.issued:
            # Recorded on the loop, like every other exchange, so the store and the per-exchange
            # line stay single-threaded. They are reads irimi made on its own account; they never
            # reach the agent and they are never writes. Last, once the write's own answer is on
            # the flow: an `on_exchange` that raised any earlier would escape this hook with no
            # response set, and mitmproxy would forward the write. Recorded too when that
            # answer could not be sent: the read was made, and a trace without it hides a request
            # irimi sent on the agent's behalf.
            self._finish(ex)

    def _decide(
        self,
        req: Request,
        writes: tuple[Exchange, ...],
        run_id: str,
        cut: bool,
        listener: tuple[str, int],
    ) -> _Decision:
        """The whole decision, on a worker thread, with no flow in sight. `run_id` is read by
        `request()` before it strips `Irimi-Run`, since `req` no longer carries it. `cut` is
        whether the request's body decoded past the cap, so `req.body` is not it. `listener` is
        the address and port the request reached irimi at, which no target may be."""
        try:
            cls = pipeline.classify(req, self.config.maps)
            # IRIMI NEVER ANSWERS A WRITE FROM PART OF ITS BODY. A body that decodes past
            # MAX_DECODED_BODY reaches the decision empty, as it is recorded. The policy would fake
            # the write from that - its echo, its idempotency key's body hash, its L3
            # precondition, and the effect the overlay later shows - and tell the agent a write it
            # never sent succeeded. So the write fails closed, as a decision that raised does:
            # answered locally, flagged, never forwarded. A delegated write is refused too, though
            # its target would get every byte, because irimi cannot record what it was. Classifying
            # reads no body, so the kind is still the agent's; a live kind is forwarded with the
            # agent's own bytes, which no hook edits.
            if cut and cls.kind not in LIVE_KINDS:
                refusal = RequestBodyCut(
                    "request body decompresses to more than "
                    f"{MAX_DECODED_BODY // (1024 * 1024)} MiB; irimi does not fake a write it "
                    "cannot read whole"
                )
                return _failed_decision(req, refusal, cls)
            # A READ IS RECORDED AS IRIMI WOULD ASK IT, WHOEVER ANSWERS IT. It is translated
            # before the policy decides, so the policy, the trace and a later replay of that trace
            # all see the one request; only a live forward carries the translation onto the flow.
            # A delegated read is never translated: its target owns that service's state.
            translation = None
            if cls.kind == "read" and writes and delegation.delegate(req, cls) is None:
                translation = self._rewrite_read(req, writes)
            asked = req if translation is None else translation.request
            ans = self.policy.answer(asked, cls, writes, run_id)
            live = ans.answered_by == "live"
            rewritten = translation.request if translation is not None and live else None
            removed = translation.removed if translation is not None else None
            refused = None if ans.forward_to is None else _target_refusal(ans.forward_to, listener)
            return _Decision(asked, cls, run_id, ans, rewritten, removed, target_refused=refused)
        except Exception as exc:
            return _failed_decision(req, exc)

    def _rewrite_read(self, req: Request, writes: tuple[Exchange, ...]) -> Translation | None:
        """Let the overlay translate a read before the policy decides about it. None
        when nothing changed; a live forward's flow is edited by `_apply_rewrite`, back on the loop.

        Guarded on its own inside the decision's never-raise guard, because the two failures want
        opposite answers: a decision that raises must be answered locally with a 502, since the
        thing it could not decide about may be a write, while a rewrite that fails must forward
        the read unchanged - nothing is performed by letting a read through, and 502-ing it would
        break a run over a cosmetic translation.

        Only the query is carried onto the flow, because that is the only thing a rewrite may
        change. The Exchange records the rewritten request: what irimi asked the service is what
        the trace has to show.
        """
        try:
            return self.overlay.rewrite(writes, req)
        except Exception:
            logger.exception("irimi: the overlay's rewrite raised; forwarding the read unchanged")
            return None

    def _apply_rewrite(self, flow: http.HTTPFlow, rewritten: Request) -> None:
        """Carry what `_rewrite_read` translated onto the flow, on the event loop: its query."""
        flow.request.path = rewritten.path_and_query

    def _to_target(self, flow: http.HTTPFlow, forward: delegation.ForwardTo) -> None:
        """Point the flow at its answer target, the way `_through_reverse_door` points it upstream.

        mitmproxy opens the server connection after this hook, so rewriting the flow here *is* the
        forward: no second HTTP client, nothing blocking the proxy's event loop while a stub
        thinks, and a streamed target response still streams. The decision has already refused
        a target that is our own listener or cannot be reached (`_target_refusal`).

        Only the flow is rewritten. The Exchange keeps the request the **agent** made, because
        `POST api.stripe.com/v1/refunds` is what the agent did and what the summary has to name;
        where the answer came from is `Exchange.target`. This is the opposite of the reverse door,
        which rewrites the recorded request too - there the rewritten host *is* what the caller
        asked for, spelled as a path.
        """
        parts = urlsplit(forward.url)
        host = (parts.hostname or "").lower()
        default_port = 443 if parts.scheme == "https" else 80
        port = parts.port or default_port
        if not forward.forward_auth:
            # Every credential-bearing header, not just Authorization: the rule is "a local stub
            # does not need your real key", and `Cookie`, `x-api-key` and `DD-API-KEY` are keys
            # too. `forward_auth` keeps all of them, which is what a sandbox tenant or an
            # internal simulator opting in actually wants.
            for name in list(flow.request.headers.keys()):
                if delegation.is_credential_header(name):
                    del flow.request.headers[name]
        path = parts.path or "/"
        _point_flow(
            flow,
            parts.scheme,
            host,
            port,
            f"{path}?{parts.query}" if parts.query else path,
            netaddr.authority(host, port, default_port),
        )

    def _through_reverse_door(self, flow: http.HTTPFlow, req: Request) -> Request:
        """Rewrite a reverse-door request to its upstream, on our Request and on the flow.

        mitmproxy opens the server connection after this hook, so changing the flow's target here
        is enough to forward there. Raises ReverseDoorRefused for a non-loopback client or a host
        that is not allowed.
        """
        peer = flow.client_conn.peername[0] if flow.client_conn.peername else ""
        if not netaddr.is_loopback(peer):
            raise reverse_door.ReverseDoorRefused(
                f"base-URL requests come from loopback only; refusing {peer!r}"
            )
        req = reverse_door.rewrite_reverse(req, self.config.reverse_hosts)
        host_header = req.header("host")
        if host_header is None:  # `rewrite_reverse` always sets one; never forward without it
            raise ValueError("the rewritten base-URL request carries no Host header")
        _point_flow(flow, req.scheme, req.host, req.port, req.path_and_query, host_header)
        return req

    def responseheaders(self, flow: http.HTTPFlow) -> None:
        """Stream a server-sent-event response instead of buffering it.

        mitmproxy buffers a whole response body before the `response` hook by default, which turns
        a streamed completion into one late blob. Only a live-forwarded response can stream: one we
        synthesized has no upstream to stream from. mitmproxy never assembles a streamed body, so
        `flow.response.stream` is a `_StreamTee`: each chunk goes to the agent as it arrives, and a
        copy of it goes to the trace.
        """
        pending: _Pending | None = flow.metadata.get(META_KEY)
        if pending is None or flow.response is None:
            return
        if pending.answered_by not in ("live", "delegated"):  # a synthesized answer has no upstream
            return
        stamp = pipeline.answered_by_header(pending.answered_by)
        if stamp is not None:
            # `respond` stamps every other answer, but the `response` hook is too late for a
            # streamed one: mitmproxy has already sent these headers by the time it runs, so the
            # rebuilt response never reaches the client. Here they have not gone out yet, so a
            # streamed delegated answer says who answered it like every other one.
            flow.response.headers[pipeline.ANSWERED_BY_HEADER] = stamp
        if _is_event_stream(flow.response.headers.get("content-type", "")):
            tee = _StreamTee()
            flow.metadata[STREAM_KEY] = tee
            flow.response.stream = tee

    def response(self, flow: http.HTTPFlow) -> None:
        pending: _Pending | None = flow.metadata.pop(META_KEY, None)
        if pending is None:  # not ours (refused in request()), or already finished
            return
        # A STREAMED RESPONSE IS ALREADY ON ITS WAY OUT.
        #
        # `responseheaders` streams any live or delegated `text/event-stream` body, `kind: read`
        # included, and mitmproxy neither assembles such a body nor lets these headers be changed
        # afterwards - they were sent before this hook ran. Two things follow, and both are the
        # engine's to enforce rather than the overlay's to remember:
        #
        #   * the overlay must not be called. Whatever it returned could not reach the agent,
        #     whose chunks have already gone, so the trace would record an answer nobody got.
        #   * nothing may be written back to the flow. `NoOverlay` returns the object it was given
        #     so the rewrite below never fired; the first overlay that returns a NEW Response
        #     would have turned every streamed read into a buffered one, presenting as "reads
        #     through the proxy mysteriously return nothing" for streaming endpoints only. The
        #     same trap is one `respond` change away on the delegated path.
        #
        # Both hold on the response having streamed, never on its body being empty: a streamed
        # body is recorded from the chunks `_StreamTee` kept.
        streamed = flow.response is not None and bool(flow.response.stream)
        stream_chunks: tuple[int, ...] = ()
        flags = pending.flags
        tee = flow.metadata.pop(STREAM_KEY, None)
        # A body that decodes past the cap is recorded empty, never as the document, so
        # nothing reads it as one. The read observers are skipped below and nothing the overlay
        # returns is sent, so the agent gets the service's bytes untouched, and only the recording
        # is short.
        cut = False
        if streamed and isinstance(tee, _StreamTee):
            upstream, stream_chunks, cut = _recorded_stream(flow, tee, whole=True)
            if cut:
                flags += (STREAM_TRUNCATED_FLAG,)
        else:
            upstream, cut = _response_from_flow(flow)
            # Once, though the request may have been cut too: a flag is a fact, not a count.
            if cut and BODY_TRUNCATED_FLAG not in flags:
                flags += (BODY_TRUNCATED_FLAG,)
        resp = upstream
        # A read irimi forwarded is the only place the real values a later fake has to sort
        # against appear - the write log holds writes, and the trace store is write-only - so
        # every read's body goes past the policy's `observe_read` on its way out. Which services
        # learn anything from one is `echo`'s to know and not the engine's: a service with no
        # observer is a no-op, and so is a body that carries nothing. A streamed body is not a
        # document an observer can read, and may be cut short, so the guard names it.
        # A delegated read is observed too: a per-route `target:` can pair delegated reads with
        # locally faked writes, and the target's values are then the ones the agent sees.
        if not streamed and not cut and pending.classification.kind == "read":
            self._observe_read(pending.classification.service, upstream.body)
        # The overlay stays off for a delegated read: the target owns that service's state, and
        # layering our own minted objects over it would corrupt read-after-write there.
        answered_by = pending.answered_by
        # What the answer already says of the overlay: a read a replay answered from the recording
        # carries the recorded read's own fidelity, the overlay's work in the run it replays.
        overlay_fidelity: OverlayFidelity | None = pending.answer.overlay
        # Only once there is a write to show: with an empty log there is nothing to apply, and the
        # request side asks the overlay under the same condition.
        if (
            not streamed
            and pending.answered_by == "live"
            and pending.classification.kind == "read"
            and self.write_log.for_run(pending.run_id)
        ):
            overlaid = self._overlaid(pending, upstream)
            if cut:
                # A READ CUT AT THE CAP IS NEVER OVERLAID, AND SAYS WHAT IT COULD NOT SHOW.
                # The overlay is shown what irimi recorded, an empty body, and says `partial` when
                # the run has a write it would have shown in this read, as it says of any body it
                # cannot parse (`ServiceOverlay._apply`). Whatever response it returns is not sent:
                # the agent gets the service's bytes, and the summary says the read did not show
                # the write, as it says of a read too big to parse, rather than nothing at all.
                overlay_fidelity = None if overlaid.fidelity is None else "partial"
            else:
                resp, overlay_fidelity = overlaid.response, overlaid.fidelity
            if resp is not upstream:
                # The bytes are no longer the service's, so the answer has to say whose they are:
                # the header tells an answer of ours from a live forward, and a changed body with
                # no header says "the real service sent this".
                answered_by = "overlay"
                flags += (FIDELITY_FLAGS["overlay"],)
        ex = _exchange(pending, resp, answered_by, flags, overlay_fidelity, stream_chunks)
        # The write log is what the overlay replays onto live reads. `is_authored_write` is the
        # one statement of what may enter it, because the summary files a read under a write by
        # the same rule. A target that could not be dialled never reaches here: `_act_on` or
        # `error()` handles that one and does not touch the log.
        if is_authored_write(ex):
            self.write_log.add(ex)
        out = pipeline.respond(ex)
        if out is not None and out is not upstream and not streamed:
            _send(flow, out, upstream)
        self._finish(ex)

    def _observe_read(self, service: str, body: bytes) -> None:
        """Hand a forwarded read to the policy, whose fakes sort after what it saw. Guarded like
        `_overlaid`: a raise here would escape the `response` hook."""
        try:
            self.policy.observe_read(service, body)
        except Exception:
            logger.exception("irimi: the policy could not observe a read")

    def _overlaid(self, pending: _Pending, upstream: Response) -> Overlaid:
        """The overlay's answer for one live read, over its own run's writes, or the upstream one
        if it failed.

        `ServiceOverlay` guards itself, and this guards the call: THE NEVER-RAISE RULE is the
        addon's, and an overlay swapped in later - a test double, a replay overlay - does not
        inherit the other one's care. A raise here would escape the `response` hook, where
        mitmproxy has already committed the flow.
        """
        try:
            writes = self.write_log.for_run(pending.run_id)
            return self.overlay(writes, pending.request, upstream, pending.translated_from)
        except Exception:
            logger.exception("irimi: the overlay raised; the agent gets the upstream read")
            return Overlaid(upstream, "partial")

    def error(self, flow: http.HTTPFlow) -> None:
        pending: _Pending | None = flow.metadata.pop(META_KEY, None)
        if pending is None:
            return
        # What failed depends on who was going to answer. A live forward lost the real service;
        # a delegated one could not reach its target, which must be said out loud rather than
        # fall back to the local fake. A flow irimi answered itself never opened a connection at
        # all, so the failure is the client going away - calling that an upstream error asserts
        # something untrue about a write that never left the machine, and drops the fidelity flag
        # the answer actually carried.
        extra_flags: tuple[str, ...]
        if pending.answered_by == "live":
            # THE ANSWER'S OWN FLAGS ARE KEPT ON EVERY PATH, A LIVE ONE'S TOO. They are the
            # policy's and the cut request's (`_act_on`): a replay's miss forwarded live says
            # `replay-miss` whether its upstream then answered or failed, as `response` keeps it.
            extra_flags = (*pending.flags, UPSTREAM_ERROR_FLAG)
        elif pending.answered_by == "delegated":
            # A target irimi refused never gets this far: `_act_on` answered it with irimi's own
            # 502. This is one irimi pointed the flow at and mitmproxy could not finish.
            extra_flags = (*pending.flags, TARGET_FAILED_FLAG)
        else:
            extra_flags = pending.flags
        # A stream that ended in an error - an upstream reset, an agent that hung up - had its
        # headers and some chunks reach the agent already. The recording keeps what `_StreamTee`
        # copied and says it is not the whole stream, rather than dropping the response.
        response: Response | None = None
        stream_chunks: tuple[int, ...] = ()
        tee = flow.metadata.pop(STREAM_KEY, None)
        if flow.response is not None and isinstance(tee, _StreamTee):
            response, stream_chunks, _ = _recorded_stream(flow, tee, whole=False)
            extra_flags += (STREAM_TRUNCATED_FLAG,)
        overlay = pending.answer.overlay  # a replayed read's recorded fidelity, as `response` takes
        ex = _exchange(pending, response, pending.answered_by, extra_flags, overlay, stream_chunks)
        self._finish(ex)

    def _finish(self, ex: Exchange) -> None:
        # Every exchange goes to the store, telemetry included: the store keeps only that a
        # telemetry exchange happened and to which host, never its request or response, so a
        # stored run's summary can count it.
        #
        # A RECORDING FAILURE NEVER AFFECTS TRAFFIC. `TraceStore.record` may
        # not raise, and this guards the call anyway, as `_overlaid` guards the overlay: a store
        # swapped in later does not inherit `DirectoryStore`'s care, and a raise here would escape
        # the hook and take the terminal line and the summary's count of this exchange with it.
        # It logs the exception's type only, as the store logs its own failures: a broken store
        # fails on every exchange, and a message may quote the value it choked on.
        try:
            self.store.record(ex)
        except Exception as exc:
            logger.warning(
                "irimi: the trace store raised %s; the exchange is not stored", type(exc).__name__
            )
            logger.debug("irimi: the trace store's traceback", exc_info=True)
        if self.on_exchange:
            self.on_exchange(ex)


class MitmEngine:
    def __init__(
        self,
        config: EngineConfig,
        policy: AnswerPolicy,
        store: TraceStore,
        overlay: Overlay,
        on_exchange: OnExchange | None = None,
        on_tool_call: OnToolCall | None = None,
        write_log: WriteLog | None = None,
        on_tls_failure: OnTlsFailure | None = None,
    ) -> None:
        self.config = config
        self.policy = policy
        self.store = store
        self.overlay = overlay
        self.on_exchange = on_exchange
        self.on_tool_call = on_tool_call
        self.on_tls_failure = on_tls_failure
        # Given by the composition root, which ends its runs as the store reports them ended.
        self.write_log = write_log if write_log is not None else WriteLog()
        self._ready = asyncio.Event()  # binds to the serving loop on first use (Python >= 3.10)
        self._start_error: EngineStartError | None = None
        self._stopped = False
        self._master: Master | None = None
        self._port: int | None = None

    def _on_running(self, port: int | None, error: EngineStartError | None) -> None:
        self._port = port
        self._start_error = error
        self._ready.set()

    async def run(self) -> None:
        try:
            # The bundle must exist before the master, or mitmproxy silently mints its own CA.
            ca.write_mitm_bundle(self.config.ca, self.config.confdir)
            opts = Options(
                listen_host=self.config.listen_host,
                listen_port=self.config.listen_port,
                confdir=str(self.config.confdir),
                mode=["regular"],
            )
            # Addon-registered options are only known once the addons load.
            opts.update_defer(
                onboarding=False,  # mitm.it must not be answered locally
                # `eager`, mitmproxy's default, dials the real host - sending a ClientHello
                # carrying real SNI - before it has seen the request it would have answered or
                # redirected. A targeted HTTPS route could then not be answered at all when the
                # real service was unreachable, which is precisely the delegation use case: an
                # offline, decommissioned or not-yet-built API. `lazy` connects when there
                # is something to send, so a faked or delegated request never touches the real
                # host. The cost is mitmproxy's eager-only conveniences - upstream-cert details
                # for the generated leaf, and ALPN mirroring - neither of which shadow mode uses.
                connection_strategy="lazy",
                # Raw TCP relays bytes past every hook. mitmproxy picks it for a plain CONNECT
                # tunnel whose first bytes do not look like HTTP to it: a method that does not
                # start with three letters (`M-SEARCH`), or any request whose first segment holds
                # fewer than three bytes. And it picks it after a live `101` to anything but a
                # WebSocket. Each is a write irimi never sees, reaching the real service with the
                # `Irimi-Run` the SDK put on it for irimi to strip. Off, the tunnel is parsed
                # as HTTP, bytes that are not HTTP fail there, and the upgraded connection is
                # closed: never forwarded.
                rawtcp=False,
            )
            # A plain Master, not DumpMaster: DumpMaster adds ErrorCheck, which sys.exit()s on
            # a bind failure before the running hook, and dump-CLI conveniences we do not use.
            self._master = Master(opts)
            self._master.addons.add(*default_addons())
            self._master.addons.add(
                IrimiAddon(
                    self.config,
                    self.policy,
                    self.store,
                    self.overlay,
                    self.on_exchange,
                    self._on_running,
                    self.on_tool_call,
                    self.write_log,
                    self.on_tls_failure,
                )
            )
            if self._stopped:  # shutdown() was called before we got here
                return
            await self._master.run()
            if self._start_error is not None:
                raise self._start_error
        finally:
            self.store.close()
            if not self._ready.is_set():
                self._start_error = EngineStartError("engine stopped before it was ready")
                self._ready.set()

    async def wait_ready(self) -> None:
        """Returns once the listener is bound; raises EngineStartError if it never will be."""
        await self._ready.wait()
        if self._start_error is not None:
            raise self._start_error

    def listen_port(self) -> int | None:
        return self._port

    def shutdown(self) -> None:
        self._stopped = True
        if self._master is not None:
            self._master.shutdown()
