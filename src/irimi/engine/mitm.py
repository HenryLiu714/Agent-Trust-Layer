"""mitmproxy-backed Engine. The only module allowed to import mitmproxy."""

import asyncio
import json
import logging
import socket
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from urllib.parse import urlsplit

from mitmproxy import ctx, http
from mitmproxy.addons import default_addons
from mitmproxy.master import Master
from mitmproxy.options import Options

from irimi import ca, delegation, echo, netaddr, pipeline, reverse_door
from irimi.engine import EngineConfig, EngineStartError, OnExchange
from irimi.exchange import (
    DECISION_FAILED_FLAG,
    FIDELITY_FLAGS,
    TARGET_FAILED_FLAG,
    UNCLASSIFIED_FLAG,
    UPSTREAM_ERROR_FLAG,
    AnsweredBy,
    Door,
    Exchange,
    Headers,
    OverlayFidelity,
    PreconditionOutcome,
    Request,
    Response,
    is_authored_write,
)
from irimi.overlay import Overlaid, Overlay
from irimi.policy import Answer, AnswerPolicy
from irimi.store import TraceStore

logger = logging.getLogger(__name__)

META_KEY = "irimi"  # flow.metadata slot holding the per-flow state below

# Called once from the running hook with (bound port, None) or (None, why it failed).
OnRunning = Callable[[int | None, EngineStartError | None], None]


@dataclass(frozen=True)
class _Pending:
    request: Request
    classification: pipeline.Classification
    run_id: str
    answered_by: AnsweredBy
    door: Door
    flags: tuple[str, ...]  # what the policy attached to its answer, e.g. fidelity:L0
    target: str = ""  # the answer target this flow was pointed at; "" when irimi answered it
    precondition: PreconditionOutcome | None = None  # what L3 decided before the fake (#45)
    rejection_code: str = ""  # the machine code of an L3 rejection; "" for everything else
    would_fire: tuple[str, ...] = ()  # the webhooks an accepted write would have sent (#47)


@dataclass(frozen=True)
class _Decision:
    """What the worker thread decided, with nothing of mitmproxy's in it (#45).

    `request` is the request as the trace should show it - rewritten, if the overlay translated a
    cursor. `rewritten` is None when nothing was translated, and is otherwise the same object,
    which is what tells the loop side there is a flow to edit.
    """

    request: Request
    classification: pipeline.Classification
    run_id: str
    answer: Answer
    rewritten: Request | None
    failure: Exception | None = None


def _failed_decision(req: Request, exc: Exception) -> _Decision:
    """A decision that raised. Every field but `failure` is never read: the caller checks
    `failure` first and returns, and the 502 path builds its own classification on the loop. They
    exist because the dataclass is frozen and total."""
    return _Decision(
        req,
        pipeline.Classification(service=req.host, operation="", kind="unknown", flags=()),
        "",
        Answer(answered_by="fake-L0", response=None),
        None,
        failure=exc,
    )


def _target_failed(reason: str) -> Response:
    """The answer when a target cannot be dialled. Never a silent fall back to the local fake,
    which would hide a broken setup and look like a working shadow run (design D20)."""
    body = json.dumps(
        {"error": {"type": "irimi_target_failed", "message": reason}}, allow_nan=False
    ).encode()
    return Response(status=502, headers=(("content-type", "application/json"),), body=body)


def _decision_failed(reason: str) -> Response:
    """The answer when the classify/answer decision itself raised. See `IrimiAddon.request`."""
    body = json.dumps(
        {"error": {"type": "irimi_decision_failed", "message": reason}}, allow_nan=False
    ).encode()
    return Response(status=502, headers=(("content-type", "application/json"),), body=body)


def _headers_from_fields(fields: Sequence[tuple[bytes, bytes]]) -> Headers:
    return tuple((k.decode("latin-1"), v.decode("latin-1")) for k, v in fields)


def _body(message: http.Message) -> bytes:
    # strict=False hands back the raw bytes when Content-Encoding cannot be decoded. The
    # strict accessor raises, and an exception in a hook makes mitmproxy forward the flow
    # untouched, which for a write means it escapes shadow mode.
    return message.get_content(strict=False) or b""


def _request_from_flow(flow: http.HTTPFlow) -> Request:
    return pipeline.parse(
        flow.request.method,
        flow.request.scheme,
        flow.request.host,
        flow.request.port,
        flow.request.path,
        _headers_from_fields(flow.request.headers.fields),
        _body(flow.request),
    )


def _response_from_flow(flow: http.HTTPFlow) -> Response:
    assert flow.response is not None
    return Response(
        status=flow.response.status_code,
        headers=_headers_from_fields(flow.response.headers.fields),
        body=_body(flow.response),
    )


def _is_event_stream(content_type: str) -> bool:
    return content_type.split(";")[0].strip().lower() == "text/event-stream"


def _fields(headers: Headers) -> list[tuple[bytes, bytes]]:
    # A list of pairs keeps repeated headers (Set-Cookie); a dict would collapse them.
    return [(k.encode("latin-1"), v.encode("latin-1")) for k, v in headers]


def _to_mitm_response(r: Response) -> http.Response:
    return http.Response.make(r.status, r.body, _fields(r.headers))


def _send(flow: http.HTTPFlow, out: Response, upstream: Response) -> None:
    """Put `out` on the flow, without re-encoding a body nothing changed.

    `http.Response.make` assigns `.content`, and mitmproxy's `set_content` re-encodes it per the
    surviving `content-encoding` header. That is right for a body we built and wrong for one we
    are handing back: `_body` reads `get_content(strict=False)`, which returns the RAW bytes when
    `Content-Encoding` cannot be decoded, so rebuilding took bytes that were already compressed -
    or were never valid gzip - and compressed them again. The agent then received different bytes
    than the target sent, in a tool whose thesis is that it sees what the service would have sent
    (#39).

    Since #12 `respond` returns a new Response for every non-live answer, so the rebuild ran for
    every delegated exchange; before that it never ran at all and the bug could not show. When
    the body is the very object we read off the flow, only the headers changed - that is all
    `respond` does - so only the headers are written back and the wire bytes are left alone.
    """
    assert flow.response is not None
    if out.body is upstream.body:
        flow.response.status_code = out.status
        flow.response.headers = http.Headers(fields=_fields(out.headers))
        return
    flow.response = _to_mitm_response(out)


# How long the probe below waits for a loopback target to accept a connection. Loopback either
# accepts or refuses in microseconds; the timeout is only there so a stub wedged mid-accept cannot
# stall the proxy, and it is short because this runs on the event loop.
TARGET_PROBE_TIMEOUT_S = 0.5


def _probe_local_target(url: str, host: str, port: int) -> None:
    """Raise TargetUnreachable when nothing is listening on a **loopback** answer target.

    #16 says an unreachable target answers `502` with a JSON body naming irimi and the target. The
    flag, the absence of a fallback to the fake and the summary line were all right, but the body
    was mitmproxy's own HTML error page: `_to_target` rewrites the flow and lets mitmproxy open the
    connection - which is what keeps delegation free of a second HTTP client and keeps a streamed
    target response streaming - and when that dial fails, mitmproxy sends its own error page from
    inside the proxy layer. The `error` hook runs first but cannot set a response; by then it is
    committed. There is no hook between the failed dial and the page (#38).

    So the failure that actually happens - the developer's own stub is not running - is caught
    before the flow is rewritten, and takes the existing `_target_failed` path. An SDK parses the
    body, and stripe-python, openai and slack_sdk all raise on an HTML blob naming neither irimi
    nor the target, which is how "my shadow run started failing" gave no hint that the stub was
    down.

    **Loopback only.** A connect to loopback costs microseconds; a connect to a host named by
    `--allow-target-host` could block the proxy's event loop for a full timeout on every delegated
    request, which is what `_to_target` avoids doing in the first place. A remote target that
    cannot be dialled still gets mitmproxy's page, and the README says so. The probe is also
    inherently best-effort: a stub that dies between the probe and the dial gets the old page too,
    and the flag is right either way.
    """
    if not netaddr.is_local_target(url):
        return
    try:
        socket.create_connection((host, port), timeout=TARGET_PROBE_TIMEOUT_S).close()
    except OSError as exc:
        raise delegation.TargetUnreachable(
            f"answer target {url!r} could not be reached: {exc}"
        ) from None


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
    ) -> None:
        self.config = config
        self.policy = policy
        self.store = store
        self.overlay = overlay
        self.on_exchange = on_exchange
        self.on_running = on_running
        self.write_log: list[Exchange] = []

    def running(self) -> None:
        # Without mitmproxy's ErrorCheck addon a failed bind does not sys.exit(); Master.run()
        # still reaches this hook with no listener, so report the outcome ourselves.
        proxyserver = ctx.master.addons.get("proxyserver")
        addrs = proxyserver.listen_addrs()
        if addrs:
            self.on_running(addrs[0][1], None)
            return
        cause = next((s.last_exception for s in proxyserver.servers if s.last_exception), None)
        where = f"{self.config.listen_host}:{self.config.listen_port}"
        self.on_running(None, EngineStartError(f"proxy did not start on {where}: {cause}"))
        ctx.master.shutdown()

    async def request(self, flow: http.HTTPFlow) -> None:
        try:
            req = _request_from_flow(flow)
        except Exception as exc:  # never fail open: an unparseable request is answered locally
            logger.warning("irimi: refusing request that could not be parsed: %s", exc)
            flow.response = http.Response.make(400, b"irimi: could not parse request\n")
            return
        # sockname is the socket this request arrived on, i.e. our own listener.
        door = reverse_door.detect_door(req, flow.client_conn.sockname[1])
        if door == "reverse":
            try:
                req = self._through_reverse_door(flow, req)
            except reverse_door.ReverseDoorRefused as exc:
                logger.warning("irimi: %s", exc)
                flow.response = http.Response.make(403, f"irimi: {exc}\n".encode())
                return
            except Exception as exc:  # never fail open: an unrewritten flow would go to ourselves
                logger.warning(
                    "irimi: refusing reverse-door request that could not be rewritten: %s", exc
                )
                flow.response = http.Response.make(400, b"irimi: could not rewrite request\n")
                return
        # IRIMI'S OWN WIRE VOCABULARY IS NEVER THE AGENT'S (#53).
        #
        # Before the decision, and whatever the write log holds: the rewrite path is where the
        # overlay strips a forged `Irimi-Rewrote`, and the engine only enters it for a live read
        # once the log holds a write, so until then the agent's own reached the real service.
        # After the reverse door, so `req` is the request as it will be forwarded and recorded.
        #
        # Guarded, like everything else in this hook: a raise here would forward the flow
        # untouched - a write included - and a header that could not be stripped is only the
        # behaviour this branch replaced.
        try:
            req = self._strip_agent_rewrote(flow, req)
        except Exception as exc:
            logger.warning(
                "irimi: could not strip an agent-sent %s: %s", pipeline.REWROTE_HEADER, exc
            )
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
        # themselves where they can say something more useful (`_to_target` below names the
        # target it could not apply); this is the backstop that makes the rule hold for pieces
        # that do not, and for every piece added later - #12, #13 and #20 all extend this path.
        #
        # The decision now runs on a worker thread. The hook is `async def`, which mitmproxy 12
        # awaits, so the proxy serves every other flow while one write's L3 precondition read is
        # in the air (#45) - the reason #38 refused to probe remote targets was that this hook
        # was synchronous. Nothing inside `_decide` may touch `flow`: mitmproxy's flow objects
        # belong to the event loop. The write log is snapshotted here, before the hand-off.
        writes = tuple(self.write_log)
        try:
            decision = await asyncio.to_thread(self._decide, req, writes)
        except Exception as handoff:  # the hand-off itself, e.g. an executor shut down under us
            decision = _failed_decision(req, handoff)
        if decision.failure is None and decision.rewritten is not None:
            # Under the decision's backstop, where these lines sat before the hand-off split them
            # out (#45). A raise here would escape the hook and forward a half-edited read with no
            # Exchange recorded; like any other part of the decision, it is answered locally.
            try:
                self._apply_rewrite(flow, decision.rewritten)
            except Exception as exc:
                decision = _failed_decision(req, exc)
        if decision.failure is not None:
            failure = decision.failure
            # Recorded, not just refused. A write that vanishes from the trace is the other half
            # of this bug: #13's "log of every write" has to show the one irimi could not decide
            # about, and `unknown` + `unclassified` is the honest classification for it.
            logger.exception("irimi: answering locally; the decision raised", exc_info=failure)
            refusal = _decision_failed(
                f"irimi could not decide how to answer this request: {failure}"
            )
            ex = pipeline.annotate(
                req,
                refusal,
                pipeline.Classification(
                    service=req.host,
                    operation="",
                    kind="unknown",
                    flags=(UNCLASSIFIED_FLAG,),
                ),
                "fake-L0",
                pipeline.attribute_run(req, self.config.run_id),
                extra_flags=(DECISION_FAILED_FLAG,),
                door=door,
            )
            # Through `respond`, like every other answer of ours. This is the one path that never
            # sets `_Pending`, so `response()` returns early and never runs - and it was therefore
            # the one engine-answered response reaching the client with no `Irimi-Answered-By`,
            # while being recorded as `fake-L0`. Invariant (a) of #12 reads the header to tell an
            # answer of ours from the real service's, and this one said "real service".
            flow.response = _to_mitm_response(pipeline.respond(ex) or refusal)
            self._finish(ex)
            return
        req = decision.request
        cls, run_id, ans = decision.classification, decision.run_id, decision.answer
        response: Response | None = ans.response
        flags: tuple[str, ...] = ans.flags
        target = ""
        if ans.forward_to is not None:
            target = ans.forward_to.url
            try:
                self._to_target(flow, req, ans.forward_to)
            except delegation.TargetRefused as exc:
                logger.warning("irimi: %s", exc)
                response, flags = _target_failed(str(exc)), flags + (TARGET_FAILED_FLAG,)
            except Exception as exc:  # never fail open: an unrewritten flow goes to the real API
                logger.warning("irimi: refusing a target that could not be applied: %s", exc)
                response, flags = (
                    _target_failed(f"answer target {target!r} could not be applied: {exc}"),
                    flags + (TARGET_FAILED_FLAG,),
                )
        flow.metadata[META_KEY] = _Pending(
            req,
            cls,
            run_id,
            ans.answered_by,
            door,
            flags,
            target,
            precondition=ans.precondition,
            rejection_code=ans.rejection_code,
            would_fire=ans.would_fire,
        )
        if response is not None:
            flow.response = _to_mitm_response(response)
        for ex in ans.issued:
            # Recorded on the loop, like every other exchange, so the store and the per-exchange
            # line stay single-threaded. They are reads irimi made on its own account; they never
            # reach the agent and they are never writes. Last, once the write's own answer is on
            # the flow: a store or `on_exchange` that raised any earlier would escape this hook
            # with no response set, and mitmproxy would forward the write (#45).
            self._finish(ex)

    def _decide(self, req: Request, writes: tuple[Exchange, ...]) -> _Decision:
        """The whole decision, on a worker thread, with no flow in sight."""
        try:
            cls = pipeline.classify(req, self.config.maps)
            run_id = pipeline.attribute_run(req, self.config.run_id)
            ans = self.policy.answer(req, cls, writes, run_id)
            rewritten = None
            if ans.answered_by == "live" and cls.kind == "read" and writes:
                rewritten = self._rewrite_read(req, writes)
            return _Decision(rewritten or req, cls, run_id, ans, rewritten)
        except Exception as exc:
            return _failed_decision(req, exc)

    def _rewrite_read(self, req: Request, writes: tuple[Exchange, ...]) -> Request | None:
        """Let the overlay translate a live read before mitmproxy forwards it (#43). None when
        nothing changed; the flow is edited by `_apply_rewrite`, back on the loop (#45).

        Guarded on its own inside the decision's never-raise guard, because the two failures want
        opposite answers: a decision that raises must be answered locally with a 502, since the
        thing it could not decide about may be a write, while a rewrite that fails must forward
        the read unchanged - nothing is performed by letting a read through, and 502-ing it would
        break a run over a cosmetic translation.

        Only the query and the `Irimi-Rewrote` header are carried onto the flow, because those
        are the only things a rewrite may change. The Exchange records the rewritten request:
        what irimi asked the service is what the trace has to show.
        """
        try:
            rewritten = self.overlay.rewrite(writes, req)
        except Exception:
            logger.exception("irimi: the overlay's rewrite raised; forwarding the read unchanged")
            return None
        if rewritten is req:
            return None
        return rewritten

    def _apply_rewrite(self, flow: http.HTTPFlow, rewritten: Request) -> None:
        """Carry what `_rewrite_read` translated onto the flow, on the event loop (#45)."""
        flow.request.path = (
            f"{rewritten.path}?{rewritten.query}" if rewritten.query else rewritten.path
        )
        stamp = next((v for k, v in rewritten.headers if k == pipeline.REWROTE_HEADER), None)
        if stamp is not None:
            flow.request.headers[pipeline.REWROTE_HEADER] = stamp
        elif pipeline.REWROTE_HEADER in flow.request.headers:
            # The overlay stripped one the agent sent: only irimi may tell the service side that a
            # page follows a minted refund, so the agent's own never reaches the real service.
            del flow.request.headers[pipeline.REWROTE_HEADER]

    def _strip_agent_rewrote(self, flow: http.HTTPFlow, req: Request) -> Request:
        """Take an agent-sent `Irimi-Rewrote` off the flow and off the recorded request (#53).

        `Irimi-Rewrote` is irimi's own vocabulary: irimi puts it on a read it translated, naming
        the query pair it dropped, and `stripe._refunds_list` reads it on the way back to know
        this page already follows a minted refund (#43). One the AGENT sent is a forgery that
        would suppress the agent's own minted refund from a list page.

        `ServiceOverlay.rewrite` strips one too, and that guard stays - the overlay is called
        directly by `tests/test_overlay.py` and will be called by Phase 5's replay, neither of
        which comes through here. But the engine only enters the rewrite path once the write log
        holds a write, so that strip could not run before the run's first faked write, and the
        forged header was forwarded to the real service intact. Two more holes closed by moving
        it here: an `llm` or `telemetry` kind is forwarded live and never went near the rewrite
        path at all, and the snapshot `request()` takes can be empty while a concurrent write
        lands before this read's `response` hook - `response()` tests `self.write_log`, the live
        list, so `_overlaid` does run and `ServiceOverlay._apply` reads the forged header off the
        recorded request.

        Both sides, because they answer different questions: `flow.request.headers` is what
        mitmproxy forwards to the service, and the returned `Request` is what the trace records -
        a trace showing a header irimi did not set is its own small untruth. `del` on a
        mitmproxy `Headers` removes every instance of the name, which is what a forged repeat
        would need. The same object back when there was nothing to strip, so the common path
        allocates nothing and `_rewrite_read`'s identity check keeps meaning "nothing to do".
        """
        if pipeline.REWROTE_HEADER not in flow.request.headers:
            return req
        del flow.request.headers[pipeline.REWROTE_HEADER]
        kept = tuple((k, v) for k, v in req.headers if k != pipeline.REWROTE_HEADER)
        return replace(req, headers=kept)

    def _to_target(self, flow: http.HTTPFlow, req: Request, forward: delegation.ForwardTo) -> None:
        """Point the flow at its answer target, the way `_through_reverse_door` points it upstream.

        mitmproxy opens the server connection after this hook, so rewriting the flow here *is* the
        forward: no second HTTP client, nothing blocking the proxy's event loop while a stub
        thinks, and a streamed target response still streams. Raises TargetRefused for our own
        listener, which would otherwise make the engine dial itself in a loop (#4's bug, #16's
        rule).

        Only the flow is rewritten. The Exchange keeps the request the **agent** made, because
        `POST api.stripe.com/v1/refunds` is what the agent did and what the summary has to name;
        where the answer came from is `Exchange.target`. This is the opposite of the reverse door,
        which rewrites the recorded request too - there the rewritten host *is* what the caller
        asked for, spelled as a path.
        """
        delegation.refuse_self_target(forward.url, flow.client_conn.sockname[1])
        parts = urlsplit(forward.url)
        host = (parts.hostname or "").lower()
        default_port = 443 if parts.scheme == "https" else 80
        port = parts.port or default_port
        _probe_local_target(forward.url, host, port)
        # `parts.hostname` has already had an IPv6 literal's brackets stripped, so `::1` has to be
        # put back in them: RFC 3986 spells the authority `[::1]:3000`, and `::1:3000` is a
        # different (and unparseable) thing. Python's own handler is lenient about it; nginx and
        # Go's net/http are not, and the README lists `::1` as a supported target. `rewrite_reverse`
        # refuses IPv6 literals outright on the reverse door; here they are supported, so they have
        # to be spelled correctly.
        literal = f"[{host}]" if ":" in host else host
        authority = literal if port == default_port else f"{literal}:{port}"
        if not forward.forward_auth:
            # Every credential-bearing header, not just Authorization: the rule is "a local stub
            # does not need your real key", and `Cookie`, `x-api-key` and `DD-API-KEY` are keys
            # too (#32). `forward_auth` keeps all of them, which is what a sandbox tenant or an
            # internal simulator opting in actually wants.
            for name in list(flow.request.headers.keys()):
                if delegation.is_credential_header(name):
                    del flow.request.headers[name]
        flow.request.scheme = parts.scheme
        flow.request.host = host
        flow.request.port = port
        path = parts.path or "/"
        flow.request.path = f"{path}?{parts.query}" if parts.query else path
        flow.request.host_header = authority

    def _through_reverse_door(self, flow: http.HTTPFlow, req: Request) -> Request:
        """Rewrite a reverse-door request to its upstream, on our Request and on the flow.

        mitmproxy opens the server connection after this hook, so changing the flow's target here
        is enough to forward there. The Host header is set explicitly: the host/port setters only
        rewrite one that already exists. Raises ReverseDoorRefused for a non-loopback client or a
        host that is not allowed.
        """
        peer = flow.client_conn.peername[0] if flow.client_conn.peername else ""
        if not netaddr.is_loopback(peer):
            raise reverse_door.ReverseDoorRefused(f"reverse door: loopback only, refusing {peer!r}")
        req = reverse_door.rewrite_reverse(req, self.config.reverse_hosts)
        flow.request.scheme = req.scheme
        flow.request.host = req.host
        flow.request.port = req.port
        flow.request.path = f"{req.path}?{req.query}" if req.query else req.path
        flow.request.host_header = next(v for k, v in req.headers if k == "host")
        return req

    def responseheaders(self, flow: http.HTTPFlow) -> None:
        """Stream a server-sent-event response instead of buffering it.

        mitmproxy buffers a whole response body before the `response` hook by default, which turns
        a streamed completion into one late blob. Only a live-forwarded response can stream: one we
        synthesized has no upstream to stream from. The `response` hook still runs for a streamed
        flow, but `flow.response.content` is None there, so the recorded exchange carries an empty
        body — that is the trade for the agent seeing tokens as they arrive (#8).
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
            # streamed delegated answer says who answered it like every other one (#12, #28).
            flow.response.headers[pipeline.ANSWERED_BY_HEADER] = stamp
        if _is_event_stream(flow.response.headers.get("content-type", "")):
            flow.response.stream = True

    def response(self, flow: http.HTTPFlow) -> None:
        pending: _Pending | None = flow.metadata.pop(META_KEY, None)
        if pending is None:  # not ours (refused in request()), or already finished
            return
        # A STREAMED RESPONSE IS ALREADY ON ITS WAY OUT (#28).
        #
        # `responseheaders` streams any live or delegated `text/event-stream` body, `kind: read`
        # included, and mitmproxy neither assembles such a body nor lets these headers be changed
        # afterwards - they were sent before this hook ran. Two things follow, and both are the
        # engine's to enforce rather than the overlay's to remember:
        #
        #   * the overlay must not be called. It would be handed `body == b""`, which is not what
        #     the service sent, and whatever it returned could only be wrong.
        #   * nothing may be written back to the flow. `NoOverlay` returns the object it was given
        #     so the rewrite below never fired; the first overlay that returns a NEW Response
        #     would have turned every streamed read into a buffered, empty-bodied one, presenting
        #     as "reads through the proxy mysteriously return nothing" for streaming endpoints
        #     only. The same trap is one `respond` change away on the delegated path (#12).
        #
        # The exchange is still recorded, with the empty body streaming costs it - that trade is
        # `responseheaders`' own, and Phase 3 replay inherits it.
        streamed = flow.response is not None and bool(flow.response.stream)
        upstream = _response_from_flow(flow)
        resp = upstream
        # A read irimi forwarded is the only place the real values a later fake has to sort
        # against appear - the write log holds writes, and the trace store is write-only - so
        # every read's body goes past `echo.observe_read` on its way out. Which services learn
        # anything from one is `echo`'s to know and not the engine's: a service with no observer
        # is a no-op, and so is a body that carries nothing (#42). A streamed body is empty here,
        # which is why the guard names it (#28). A delegated read is observed too: a per-route
        # `target:` can pair delegated reads with locally faked writes, and the target's values
        # are then the ones the agent sees.
        if not streamed and pending.classification.kind == "read":
            echo.observe_read(pending.classification.service, upstream.body)
        # The overlay stays off for a delegated read: the target owns that service's state, and
        # layering our own minted objects over it would corrupt read-after-write there (D20).
        answered_by = pending.answered_by
        flags = pending.flags
        overlay_fidelity: OverlayFidelity | None = None
        # Only once there is a write to show: with an empty log there is nothing to apply, and the
        # request side asks the overlay under the same condition.
        if (
            not streamed
            and pending.answered_by == "live"
            and pending.classification.kind == "read"
            and self.write_log
        ):
            overlaid = self._overlaid(pending.request, upstream)
            resp, overlay_fidelity = overlaid.response, overlaid.fidelity
            if resp is not upstream:
                # The bytes are no longer the service's, so the answer has to say whose they are:
                # invariant (a) of #12 reads the header to tell an answer of ours from a live
                # forward, and a changed body with no header says "the real service sent this".
                answered_by = "overlay"
                flags += (FIDELITY_FLAGS["overlay"],)
        ex = pipeline.annotate(
            pending.request,
            resp,
            pending.classification,
            answered_by,
            pending.run_id,
            extra_flags=flags,
            door=pending.door,
            target=pending.target,
            overlay=overlay_fidelity,
            precondition=pending.precondition,
            rejection_code=pending.rejection_code,
            would_fire=pending.would_fire,
        )
        # The write log is what the overlay replays onto live reads. `is_authored_write` is the
        # one statement of what may enter it, because the summary files a read under a write by
        # the same rule (#48). A target that could not be *dialled* never reaches here: `error()`
        # handles that one and does not touch the log.
        if is_authored_write(ex):
            self.write_log.append(ex)
        out = pipeline.respond(ex)
        if out is not None and out is not upstream and not streamed:
            _send(flow, out, upstream)
        self._finish(ex)

    def _overlaid(self, request: Request, upstream: Response) -> Overlaid:
        """The overlay's answer for one live read, or the upstream one if it failed.

        `ServiceOverlay` guards itself, and this guards the call: THE NEVER-RAISE RULE is the
        addon's, and an overlay swapped in later - a test double, a replay overlay - does not
        inherit the other one's care. A raise here would escape the `response` hook, where
        mitmproxy has already committed the flow.
        """
        try:
            return self.overlay(tuple(self.write_log), request, upstream)
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
        # the answer actually carried (#29).
        extra_flags: tuple[str, ...]
        if pending.answered_by == "live":
            extra_flags = (UPSTREAM_ERROR_FLAG,)
        elif pending.answered_by == "delegated":
            # A target irimi refused was already flagged in `request()`, and the client going away
            # afterwards does not make it fail twice. The flag is a fact about the exchange, not a
            # counter (#32).
            extra_flags = pending.flags
            if TARGET_FAILED_FLAG not in extra_flags:
                extra_flags += (TARGET_FAILED_FLAG,)
        else:
            extra_flags = pending.flags
        ex = pipeline.annotate(
            pending.request,
            None,
            pending.classification,
            pending.answered_by,
            pending.run_id,
            extra_flags=extra_flags,
            door=pending.door,
            target=pending.target,
            precondition=pending.precondition,
            rejection_code=pending.rejection_code,
            would_fire=pending.would_fire,
        )
        self._finish(ex)

    def _finish(self, ex: Exchange) -> None:
        # Telemetry is forwarded but never stored: a trace of the agent's own observability
        # traffic is noise, and replaying it would re-emit someone else's events (#9). It is still
        # reported, so the per-exchange line and the run summary both count it.
        if ex.kind != "telemetry":
            self.store.record(ex)
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
    ) -> None:
        self.config = config
        self.policy = policy
        self.store = store
        self.overlay = overlay
        self.on_exchange = on_exchange
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
                # offline, decommissioned or not-yet-built API (#32). `lazy` connects when there
                # is something to send, so a faked or delegated request never touches the real
                # host. The cost is mitmproxy's eager-only conveniences - upstream-cert details
                # for the generated leaf, and ALPN mirroring - neither of which shadow mode uses.
                connection_strategy="lazy",
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
