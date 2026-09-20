"""mitmproxy-backed Engine. The only module allowed to import mitmproxy."""

import asyncio
import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from urllib.parse import urlsplit

from mitmproxy import ctx, http
from mitmproxy.addons import default_addons
from mitmproxy.master import Master
from mitmproxy.options import Options

from irimi import ca, pipeline
from irimi.engine import EngineConfig, EngineStartError, OnExchange
from irimi.exchange import LIVE_KINDS, AnsweredBy, Door, Exchange, Headers, Request, Response
from irimi.overlay import Overlay
from irimi.policy import AnswerPolicy, ForwardTo
from irimi.store import TraceStore

logger = logging.getLogger(__name__)

META_KEY = "irimi"  # flow.metadata slot holding the per-flow state below
UPSTREAM_ERROR_FLAG = "upstream-error"

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


def _to_mitm_response(r: Response) -> http.Response:
    # A list of pairs keeps repeated headers (Set-Cookie); a dict would collapse them.
    fields = [(k.encode("latin-1"), v.encode("latin-1")) for k, v in r.headers]
    return http.Response.make(r.status, r.body, fields)


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

    def request(self, flow: http.HTTPFlow) -> None:
        try:
            req = _request_from_flow(flow)
        except Exception as exc:  # never fail open: an unparseable request is answered locally
            logger.warning("irimi: refusing request that could not be parsed: %s", exc)
            flow.response = http.Response.make(400, b"irimi: could not parse request\n")
            return
        # sockname is the socket this request arrived on, i.e. our own listener.
        door = pipeline.detect_door(req, flow.client_conn.sockname[1])
        if door == "reverse":
            try:
                req = self._through_reverse_door(flow, req)
            except pipeline.ReverseDoorRefused as exc:
                logger.warning("irimi: %s", exc)
                flow.response = http.Response.make(403, f"irimi: {exc}\n".encode())
                return
            except Exception as exc:  # never fail open: an unrewritten flow would go to ourselves
                logger.warning(
                    "irimi: refusing reverse-door request that could not be rewritten: %s", exc
                )
                flow.response = http.Response.make(400, b"irimi: could not rewrite request\n")
                return
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
        try:
            cls = pipeline.classify(req, self.config.maps)
            run_id = pipeline.attribute_run(req, self.config.run_id)
            ans = self.policy.answer(req, cls)
        except Exception as exc:
            # Recorded, not just refused. A write that vanishes from the trace is the other half
            # of this bug: #13's "log of every write" has to show the one irimi could not decide
            # about, and `unknown` + `unclassified` is the honest classification for it.
            logger.exception("irimi: answering locally; the decision raised")
            response = _decision_failed(f"irimi could not decide how to answer this request: {exc}")
            ex = pipeline.annotate(
                req,
                response,
                pipeline.Classification(
                    service=req.host,
                    operation="",
                    kind="unknown",
                    flags=(pipeline.UNCLASSIFIED_FLAG,),
                ),
                "fake-L0",
                pipeline.attribute_run(req, self.config.run_id),
                extra_flags=(pipeline.DECISION_FAILED_FLAG,),
                door=door,
            )
            # Through `respond`, like every other answer of ours. This is the one path that never
            # sets `_Pending`, so `response()` returns early and never runs - and it was therefore
            # the one engine-answered response reaching the client with no `Irimi-Answered-By`,
            # while being recorded as `fake-L0`. Invariant (a) of #12 reads the header to tell an
            # answer of ours from the real service's, and this one said "real service".
            flow.response = _to_mitm_response(pipeline.respond(ex) or response)
            self._finish(ex)
            return
        response, flags, target = ans.response, ans.flags, ""
        if ans.forward_to is not None:
            target = ans.forward_to.url
            try:
                self._to_target(flow, req, ans.forward_to)
            except pipeline.TargetRefused as exc:
                logger.warning("irimi: %s", exc)
                response, flags = _target_failed(str(exc)), flags + (pipeline.TARGET_FAILED_FLAG,)
            except Exception as exc:  # never fail open: an unrewritten flow goes to the real API
                logger.warning("irimi: refusing a target that could not be applied: %s", exc)
                response, flags = (
                    _target_failed(f"answer target {target!r} could not be applied: {exc}"),
                    flags + (pipeline.TARGET_FAILED_FLAG,),
                )
        flow.metadata[META_KEY] = _Pending(req, cls, run_id, ans.answered_by, door, flags, target)
        if response is not None:
            flow.response = _to_mitm_response(response)

    def _to_target(self, flow: http.HTTPFlow, req: Request, forward: ForwardTo) -> None:
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
        pipeline.refuse_self_target(forward.url, flow.client_conn.sockname[1])
        parts = urlsplit(forward.url)
        host = (parts.hostname or "").lower()
        default_port = 443 if parts.scheme == "https" else 80
        port = parts.port or default_port
        # `parts.hostname` has already had an IPv6 literal's brackets stripped, so `::1` has to be
        # put back in them: RFC 3986 spells the authority `[::1]:3000`, and `::1:3000` is a
        # different (and unparseable) thing. Python's own handler is lenient about it; nginx and
        # Go's net/http are not, and the README lists `::1` as a supported target. `rewrite_reverse`
        # refuses IPv6 literals outright on the reverse door; here they are supported, so they have
        # to be spelled correctly.
        literal = f"[{host}]" if ":" in host else host
        authority = literal if port == default_port else f"{literal}:{port}"
        if not forward.forward_auth:
            flow.request.headers.pop(pipeline.AUTH_HEADER, None)
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
        if not pipeline.is_loopback(peer):
            raise pipeline.ReverseDoorRefused(f"reverse door: loopback only, refusing {peer!r}")
        req = pipeline.rewrite_reverse(req, self.config.reverse_hosts)
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
        upstream = _response_from_flow(flow)
        resp = upstream
        # The overlay stays off for a delegated read: the target owns that service's state, and
        # layering our own minted objects over it would corrupt read-after-write there (D20).
        if pending.answered_by == "live" and pending.classification.kind == "read":
            resp = self.overlay(self.write_log, pending.request, upstream)
        ex = pipeline.annotate(
            pending.request,
            resp,
            pending.classification,
            pending.answered_by,
            pending.run_id,
            extra_flags=pending.flags,
            door=pending.door,
            target=pending.target,
        )
        # The write log is what the overlay replays onto live reads, so it holds *writes*:
        # exchanges that changed state somewhere the real service does not know about.
        # `answered_by != "live"` was an exhaustive spelling of that only while every read was
        # live. `target_reads` makes a delegated read the first non-live read, and a GET replayed
        # onto later reads is not a write by any reading - #20 owns "no overlay for a delegated
        # service" and #28 is the streaming twin of the same hazard. A failed target (502) is not
        # a write either: nothing was performed, so it is excluded by its flag. A target that
        # could not be *dialled* never reaches here at all - `error()` handles that one and does
        # not touch the log - but a target irimi *refused* is answered in `request()`, which does
        # set `_Pending`, so without the flag check it landed here and the overlay would replay a
        # write that was performed nowhere.
        if (
            ex.kind not in LIVE_KINDS
            and ex.answered_by != "live"
            and pipeline.TARGET_FAILED_FLAG not in ex.flags
        ):
            self.write_log.append(ex)
        out = pipeline.respond(ex)
        if out is not None and out is not upstream:
            flow.response = _to_mitm_response(out)
        self._finish(ex)

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
        if pending.answered_by == "live":
            extra_flags = (UPSTREAM_ERROR_FLAG,)
        elif pending.answered_by == "delegated":
            extra_flags = pending.flags + (pipeline.TARGET_FAILED_FLAG,)
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
            opts.update_defer(onboarding=False)  # mitm.it must not be answered locally
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
