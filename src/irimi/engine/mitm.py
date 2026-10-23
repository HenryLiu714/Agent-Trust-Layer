"""mitmproxy-backed Engine. The only module allowed to import mitmproxy.

It turns flows into `Request`s and `Response`s, carries the session's plans onto them, and
calls the session back when an answer arrives or the connection fails. What irimi decides and
records is `irimi.engine.session`'s; nothing here decides anything.
"""

import asyncio
import errno
import logging
import time
from collections.abc import Callable, Sequence

from mitmproxy import ctx, http, tls
from mitmproxy.addons import default_addons
from mitmproxy.master import Master
from mitmproxy.options import Options

from irimi.config import ca
from irimi.core import netaddr
from irimi.core.exchange import Flag, Headers, Request, Response, media_type
from irimi.decide import targets
from irimi.decide.overlay import Overlay
from irimi.decide.policy import AnswerPolicy, Delegate, Forward, Respond
from irimi.decide.writelog import WriteLog
from irimi.engine import (
    EngineConfig,
    EngineStartError,
    OnExchange,
    OnTlsFailure,
    Replay,
    Serve,
    control,
    ingress,
    wire,
)
from irimi.engine.capture import StreamCapture, recorded_stream
from irimi.engine.control import ControlEndpoint
from irimi.engine.decoding import Decoded, decode_body
from irimi.engine.ingress import Admitted, ControlCall, Refusal
from irimi.engine.session import Plan, Received, ShadowSession
from irimi.protocol.headers import ANSWERED_BY_HEADER, REWROTE_HEADER, RUN_HEADER
from irimi.record import redact
from irimi.record.store import RunStore

logger = logging.getLogger(__name__)

META_KEY = "irimi"  # flow.metadata slot holding the flow's `Plan`
STREAM_KEY = "irimi-stream"  # flow.metadata slot holding a streamed response's capture
# flow.metadata slot set when the request's body was decoded only up to MAX_DECODED_BODY.
REQUEST_CUT_KEY = "irimi-request-cut"

# Called once from the running hook with (bound port, None) or (None, why it failed).
OnRunning = Callable[[int | None, EngineStartError | None], None]
# Why a request irimi could not parse is refused.
PARSE_FAILED = "could not parse this request"


def _request_line(flow: http.HTTPFlow) -> Request:
    """The flow's request line alone, for recording a request whose full parse failed, or an
    empty request when even that fails."""
    try:
        r = flow.request
        return wire.parse(r.method, r.scheme, r.host, r.port, r.path, (), b"")
    except Exception:
        return wire.parse("", "", "", 0, "/", (), b"")


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


def _headers_from_fields(fields: Sequence[tuple[bytes, bytes]]) -> Headers:
    return tuple((k.decode("latin-1"), v.decode("latin-1")) for k, v in fields)


def _recorded_stream(
    flow: http.HTTPFlow, capture: StreamCapture, *, whole: bool
) -> tuple[Response, tuple[int, ...], bool]:
    """The flow's streamed response as the trace records it (`capture.recorded_stream`)."""
    assert flow.response is not None
    headers = _headers_from_fields(flow.response.headers.fields)
    return recorded_stream(flow.response.status_code, headers, capture, whole=whole)


def _body(message: http.Message) -> Decoded:
    """A buffered body as the trace records it, and whether its decoding stopped at the cap.

    A body is recorded as it came when its Content-Encoding cannot be decoded, rather than
    raising out of a hook. A body that decodes past MAX_DECODED_BODY is recorded empty, as the
    store records any body past `limits.MAX_REDACTED_BODY`, the same bound: a stored body is a
    prefix of the whole body redacted, never the start of one redacted on its own, and the start
    of a JSON document is not JSON. A stream is kept to its lines instead
    (`capture.recorded_stream`), because it is redacted by line."""
    raw = message.raw_content or b""
    content_encoding = message.headers.get("content-encoding", "")
    if not raw or not content_encoding:
        return Decoded(raw, False)
    decoded = decode_body(raw, content_encoding)
    if decoded is None:
        return Decoded(raw, False)
    if decoded.cut:
        return Decoded(b"", True)
    return decoded


def _request_from_flow(flow: http.HTTPFlow) -> tuple[Request, bool]:
    """The flow's request, and whether its body's decoding stopped at the cap."""
    body = _body(flow.request)
    request = wire.parse(
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
    # `make` re-encodes the body per its `content-encoding`, and mitmproxy drops an encoding it
    # cannot apply when that is a ValueError but lets the TypeError of a text codec through
    # (`rot13`, `utf-8`). So the body goes out as it is, without the encoding, as mitmproxy's own
    # fallback sends it.
    try:
        return http.Response.make(r.status, r.body, _fields(r.headers))
    except TypeError:
        fields = [(k, v) for k, v in _fields(r.headers) if k.lower() != b"content-encoding"]
        return http.Response.make(r.status, r.body, fields)


def _send(flow: http.HTTPFlow, out: Response, upstream: Response) -> None:
    """Put `out` on the flow, without re-encoding a body nothing changed.

    `http.Response.make` re-encodes a body per its surviving `content-encoding` header. That is
    right for a body irimi built and wrong for a live or delegated one it is handing back:
    `_body` keeps the raw bytes when their encoding cannot be decoded, and re-encoding them would
    send the agent different bytes than the service sent. So when `out` carries the very body
    read off the flow, only its status and headers are written back.
    """
    assert flow.response is not None
    if out.body is upstream.body:
        flow.response.status_code = out.status
        flow.response.headers = http.Headers(fields=_fields(out.headers))
        return
    flow.response = _to_mitm_response(out)


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


def default_control(config: EngineConfig, policy: AnswerPolicy, store: RunStore) -> ControlEndpoint:
    """The control endpoint an engine answers with when its composition root gives it none: over
    its own store, with its policy answering a replay's read tools, and no tool-call callback."""
    return ControlEndpoint(
        store,
        serve=isinstance(config.mode, Serve),
        process_run=config.run_id,
        policy=policy,
        replay_of=config.mode.source_run_id if isinstance(config.mode, Replay) else None,
    )


class IrimiAddon:
    """The mitmproxy addon: carries the session's plans onto flows. One instance per engine.

    Nothing in a hook may raise: a raised request hook forwards the flow, and a forwarded write
    escapes shadow mode. The request hook runs under one guard that answers with irimi's own 502
    instead; the steps inside it handle a failure themselves only where it means something else.
    """

    def __init__(
        self,
        config: EngineConfig,
        policy: AnswerPolicy,
        store: RunStore,
        overlay: Overlay,
        on_exchange: OnExchange | None,
        on_running: OnRunning,
        control: ControlEndpoint | None = None,
        write_log: WriteLog | None = None,
        on_tls_failure: OnTlsFailure | None = None,
    ) -> None:
        self.config = config
        self.on_running = on_running
        self.on_tls_failure = on_tls_failure
        self.control = control if control is not None else default_control(config, policy, store)
        self.session = ShadowSession(
            policy, store, overlay, config.maps, config.run_id, on_exchange, write_log
        )

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

    # ------------------------------------------------------------------------------ request

    async def request(self, flow: http.HTTPFlow) -> None:
        # When this exchange began: before anything else the hook does, so the span is the
        # whole of irimi's part.
        started_at = time.time()
        try:
            await self._request(flow, started_at)
        except Exception as exc:
            if flow.response is not None:
                raise  # already answered: mitmproxy sends that answer and forwards nothing
            flow.metadata.pop(META_KEY, None)
            cut = bool(flow.metadata.get(REQUEST_CUT_KEY))
            admitted = Admitted(
                _request_line(flow), self.config.run_id, "forward", ("", 0), started_at, cut
            )
            self._answer_now(flow, self.session.decision_failed(admitted, exc))

    async def _request(self, flow: http.HTTPFlow, started_at: float) -> None:
        try:
            request, cut = _request_from_flow(flow)
        except Exception as exc:  # never fail open: an unparseable request is answered by irimi
            logger.warning("irimi: refusing request that could not be parsed: %s", exc)
            line = _request_line(flow)
            refusal = Refusal(line, "forward", 400, PARSE_FAILED, started_at, parsed=False)
            self._answer_now(flow, self.session.refuse(refusal))
            return
        if cut:  # the exchange this flow becomes says its request body is not all there
            flow.metadata[REQUEST_CUT_KEY] = True
        try:
            # The socket this request arrived on is our own listener: its port, and the address
            # it was reached at, which is this machine too when the bind is wider than loopback.
            local_address, local_port = flow.client_conn.sockname[:2]
            listener = (str(local_address), int(local_port))
        except Exception as exc:  # a request whose door is unknown may be addressed to irimi
            logger.warning(
                "irimi: could not tell whether this is a proxy or base-URL request: %s", exc
            )
            refusal = Refusal(request, "forward", 400, ingress.ROUTE_FAILED, started_at, cut)
            self._answer_now(flow, self.session.refuse(refusal))
            return
        peer = flow.client_conn.peername[0] if flow.client_conn.peername else ""
        admitted = ingress.admit(
            request,
            listener=listener,
            peer=str(peer),
            reverse_hosts=self.config.reverse_hosts,
            default_run_id=self.config.run_id,
            started_at=started_at,
            cut=cut,
        )
        if isinstance(admitted, ControlCall):
            self._answer_control(flow, admitted)
            return
        if isinstance(admitted, Refusal):
            self._answer_now(flow, self.session.refuse(admitted))
            return
        if admitted.door == "reverse":
            try:
                self._through_reverse_door(flow, admitted.request)
            except Exception as exc:  # never fail open: an unpointed flow would go to ourselves
                logger.warning(
                    "irimi: refusing a base-URL request that could not be rewritten: %s", exc
                )
                refusal = Refusal(request, "reverse", 400, ingress.REWRITE_FAILED, started_at, cut)
                self._answer_now(flow, self.session.refuse(refusal))
                return
        self._strip_irimi_headers(flow)
        # The decision runs on a worker thread: mitmproxy 12 awaits an `async def` hook, so the
        # proxy serves every other flow while one write's precondition read or a target's probe
        # is in the air. Nothing the session does touches the flow.
        try:
            plan = await asyncio.to_thread(self.session.plan, admitted)
        except Exception as handoff:  # the hand-off itself, e.g. an executor shut down under us
            plan = self.session.decision_failed(admitted, handoff)
        self._carry_out(flow, admitted, plan)

    def _carry_out(self, flow: http.HTTPFlow, admitted: Admitted, plan: Plan) -> None:
        """Put a plan on its flow. irimi's own reads are recorded last, once the request's own
        answer is on the flow: a callback that raised any earlier would escape the hook with no
        response set, and mitmproxy would forward a write."""
        decision = plan.decision
        if plan.settled:
            self._answer_now(flow, plan)
            return
        if isinstance(decision, Forward) and decision.translation is not None:
            try:
                self._carry_translation(flow, plan.request)
            except Exception as exc:
                self._answer_now(flow, self.session.decision_failed(admitted, exc))
                return
        elif isinstance(decision, Delegate):
            try:
                self._to_target(flow, decision.target)
            except Exception as exc:  # never fail open: an unpointed flow goes to the real API
                message = f"answer target {plan.target!r} could not be applied: {exc}"
                self._answer_now(flow, self.session.target_failed(plan, message))
                return
        elif isinstance(decision, Respond):
            # Guarded, because the answer may not be irimi's own: a replay answers with a
            # response read off disk, whose headers mitmproxy may refuse to encode.
            try:
                built = _to_mitm_response(decision.response)
            except Exception as exc:
                refused = self.session.decision_failed(
                    admitted, exc, request=plan.request, issued=plan.issued
                )
                self._answer_now(flow, refused)
                return
            flow.metadata[META_KEY] = plan
            flow.response = built
            self._record_issued(plan)
            return
        flow.metadata[META_KEY] = plan
        self._record_issued(plan)

    def _carry_translation(self, flow: http.HTTPFlow, translated: Request) -> None:
        """Carry a translated read onto the flow: its query, all a translation may change."""
        flow.request.path = translated.path_and_query

    def _answer_now(self, flow: http.HTTPFlow, plan: Plan) -> None:
        """Answer a settled plan: its response on the flow first, so a callback that raises while
        it is recorded escapes the hook with the answer already set, never with the flow bare."""
        assert isinstance(plan.decision, Respond)
        flow.response = _to_mitm_response(plan.decision.response)
        self.session.record(self.session.settle(plan))
        self._record_issued(plan)

    def _record_issued(self, plan: Plan) -> None:
        # Reads irimi made on its own account: they never reach the agent and are never writes.
        # Recorded on the loop, like every other exchange, so the store stays single-threaded.
        for exchange in plan.issued:
            self.session.record(exchange)

    def _answer_control(self, flow: http.HTTPFlow, call: ControlCall) -> None:
        """Answer a control request. `ControlEndpoint.answer` never raises; this guards turning
        its answer into mitmproxy's anyway, because a control request that escaped this hook would
        be forwarded to the listener it was addressed to - irimi itself."""
        # A response to HEAD carries no content (RFC 9110 9.3.2), and mitmproxy sends whatever the
        # response holds. Nor a Content-Length, which a HEAD answer may only send when it is what
        # a GET would get (8.6), and a HEAD's 405 is not that.
        head = call.request.method == "HEAD"
        try:
            answer = self.control.answer(call.request, whole_body=call.whole_body)
            response = _to_mitm_response(answer)
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

    def _strip_irimi_headers(self, flow: http.HTTPFlow) -> None:
        """Take irimi's own headers off the flow, as `ingress.admit` took them off the request:
        `Irimi-Run` and an agent-sent `Irimi-Rewrote` never reach the service. A header left on
        is a lesser harm than a raise, which would forward the flow untouched."""
        for name in (REWROTE_HEADER, RUN_HEADER):
            try:
                if name in flow.request.headers:
                    del flow.request.headers[name]  # every instance, in any spelling
            except Exception as exc:
                logger.warning("irimi: could not strip %s: %s", name, exc)

    def _to_target(self, flow: http.HTTPFlow, forward: targets.ForwardTo) -> None:
        """Point the flow at its answer target. mitmproxy opens the server connection after this
        hook, so rewriting the flow here is the forward, and a streamed target response still
        streams. The session has already refused a target that is our own listener or cannot be
        reached. The exchange keeps the request the agent made; where the answer came from is
        `Exchange.target`."""
        target = forward.target
        if not forward.forward_auth:
            # Every credential-bearing header, not just Authorization: a local stub does not need
            # your real key, and `Cookie`, `x-api-key` and `DD-API-KEY` are keys too.
            for name in list(flow.request.headers.keys()):
                if redact.is_credential_header(name):
                    del flow.request.headers[name]
        _point_flow(
            flow, target.scheme, target.host, target.port, target.path or "/", target.authority
        )

    def _through_reverse_door(self, flow: http.HTTPFlow, request: Request) -> None:
        """Point a base-URL request's flow at the upstream `ingress.admit` rewrote it to."""
        host_header = request.header("host")
        if host_header is None:  # `admit` always sets one; never forward without it
            raise ValueError("the rewritten base-URL request carries no Host header")
        _point_flow(
            flow, request.scheme, request.host, request.port, request.path_and_query, host_header
        )

    # ------------------------------------------------------------------- response and error

    def responseheaders(self, flow: http.HTTPFlow) -> None:
        """Stamp and stream a live or delegated response before its headers go out.

        mitmproxy buffers a whole response body before the `response` hook by default, which turns
        a streamed completion into one late blob, and by then the headers have gone: a streamed
        answer is stamped here or never. Only a forwarded response can stream; one irimi built has
        no upstream. A `StreamCapture` sends each chunk to the agent as it arrives and keeps a copy
        for the trace.
        """
        plan: Plan | None = flow.metadata.get(META_KEY)
        if plan is None or flow.response is None:
            return
        if plan.answered_by not in ("live", "delegated"):
            return
        stamp = wire.answered_by_header(plan.answered_by)
        if stamp is not None:
            flow.response.headers[ANSWERED_BY_HEADER] = stamp
        if _is_event_stream(flow.response.headers.get("content-type", "")):
            tee = StreamCapture()
            flow.metadata[STREAM_KEY] = tee
            flow.response.stream = tee

    def response(self, flow: http.HTTPFlow) -> None:
        plan: Plan | None = flow.metadata.pop(META_KEY, None)
        if plan is None:  # not ours (answered in the request hook), or already finished
            return
        try:
            streamed = flow.response is not None and bool(flow.response.stream)
            tee = flow.metadata.pop(STREAM_KEY, None)
            if streamed and isinstance(tee, StreamCapture):
                upstream, chunks, cut = _recorded_stream(flow, tee, whole=True)
                received = Received(upstream, True, chunks, Flag.STREAM_TRUNCATED if cut else None)
            else:
                upstream, cut = _response_from_flow(flow)
                received = Received(upstream, streamed, (), Flag.BODY_TRUNCATED if cut else None)
            completed = self.session.complete(plan, received)
            if completed.send is not None:
                _send(flow, completed.send, upstream)
        except Exception:
            logger.exception("irimi: could not record a response; the agent gets it as it came")
            return
        self.session.record(completed.exchange)

    def error(self, flow: http.HTTPFlow) -> None:
        plan: Plan | None = flow.metadata.pop(META_KEY, None)
        if plan is None:
            return
        try:
            received = None
            tee = flow.metadata.pop(STREAM_KEY, None)
            if flow.response is not None and isinstance(tee, StreamCapture):
                response, chunks, _ = _recorded_stream(flow, tee, whole=False)
                received = Received(response, True, chunks)
            exchange = self.session.fail(plan, received)
        except Exception:
            logger.exception("irimi: could not record a failed connection")
            return
        self.session.record(exchange)


def _mitm_options(config: EngineConfig) -> Options:
    opts = Options(
        listen_host=config.listen_host,
        listen_port=config.listen_port,
        confdir=str(config.confdir),
        mode=["regular"],
    )
    # Addon-registered options are only known once the addons load.
    opts.update_defer(
        onboarding=False,  # mitm.it must not be answered by irimi
        # `lazy` dials the real host only when there is something to send, so a faked or
        # delegated request never sends the real service a ClientHello carrying its SNI.
        connection_strategy="lazy",
        # Raw TCP would relay bytes past every hook; off, they fail as HTTP and are never forwarded.
        rawtcp=False,
    )
    return opts


async def _close_listeners(master: Master) -> None:
    """Close the proxy's listening sockets, which `Master.run` returns with still open. Only a
    server that is listening is stopped: one whose bind failed has nothing to close."""
    try:
        proxyserver = master.addons.get("proxyserver")
        for server in list(proxyserver.servers) if proxyserver is not None else ():
            if server.listen_addrs:
                await server.stop()
    except Exception:
        logger.debug("irimi: could not close the proxy's listener", exc_info=True)


class MitmEngine:
    def __init__(
        self,
        config: EngineConfig,
        policy: AnswerPolicy,
        store: RunStore,
        overlay: Overlay,
        on_exchange: OnExchange | None = None,
        control: ControlEndpoint | None = None,
        write_log: WriteLog | None = None,
        on_tls_failure: OnTlsFailure | None = None,
    ) -> None:
        self.config = config
        self.policy = policy
        # The store is the composition root's to open and to close.
        self.store = store
        self.overlay = overlay
        self.on_exchange = on_exchange
        self.control = control if control is not None else default_control(config, policy, store)
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
            # A plain Master, not DumpMaster: DumpMaster adds ErrorCheck, which sys.exit()s on
            # a bind failure before the running hook, and dump-CLI conveniences we do not use.
            self._master = Master(_mitm_options(self.config))
            self._master.addons.add(*default_addons())
            self._master.addons.add(
                IrimiAddon(
                    self.config,
                    self.policy,
                    self.store,
                    self.overlay,
                    self.on_exchange,
                    self._on_running,
                    self.control,
                    self.write_log,
                    self.on_tls_failure,
                )
            )
            if self._stopped:  # shutdown() was called before we got here
                return
            try:
                await self._master.run()
            finally:
                await _close_listeners(self._master)
            if self._start_error is not None:
                raise self._start_error
        finally:
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
