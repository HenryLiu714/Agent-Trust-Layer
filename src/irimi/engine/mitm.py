"""mitmproxy-backed Engine. The only module allowed to import mitmproxy."""

import asyncio
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from mitmproxy import ctx, http
from mitmproxy.addons import default_addons
from mitmproxy.master import Master
from mitmproxy.options import Options

from irimi import ca, pipeline
from irimi.engine import EngineConfig, EngineStartError, OnExchange
from irimi.exchange import AnsweredBy, Door, Exchange, Headers, Request, Response
from irimi.overlay import Overlay
from irimi.policy import AnswerPolicy
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
        self.listen_port: int = config.listen_port  # replaced by the bound port in running()

    def running(self) -> None:
        # Without mitmproxy's ErrorCheck addon a failed bind does not sys.exit(); Master.run()
        # still reaches this hook with no listener, so report the outcome ourselves.
        proxyserver = ctx.master.addons.get("proxyserver")
        addrs = proxyserver.listen_addrs()
        if addrs:
            self.listen_port = addrs[0][1]
            self.on_running(self.listen_port, None)
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
        door = pipeline.detect_door(req, self.listen_port)
        if door == "reverse":
            try:
                req = self._through_reverse_door(flow, req)
            except pipeline.ReverseDoorRefused as exc:
                logger.warning("irimi: %s", exc)
                flow.response = http.Response.make(403, f"irimi: {exc}\n".encode())
                return
        cls = pipeline.classify(req)
        run_id = pipeline.attribute_run(req, self.config.run_id)
        ans = self.policy.answer(req, cls.kind)
        flow.metadata[META_KEY] = _Pending(req, cls, run_id, ans.answered_by, door)
        if ans.response is not None:
            flow.response = _to_mitm_response(ans.response)

    def _through_reverse_door(self, flow: http.HTTPFlow, req: Request) -> Request:
        """Rewrite a reverse-door request to its upstream, on our Request and on the flow.

        mitmproxy opens the server connection after this hook, so changing the flow's target here
        is enough to forward there. The host/port setters also rewrite the Host header. Raises
        ReverseDoorRefused for a non-loopback client or a host that is not allowed.
        """
        peer = flow.client_conn.peername[0] if flow.client_conn.peername else ""
        if not pipeline.is_loopback(peer):
            raise pipeline.ReverseDoorRefused(f"reverse door: loopback only, refusing {peer!r}")
        req = pipeline.rewrite_reverse(req, self.config.reverse_hosts)
        flow.request.scheme = req.scheme
        flow.request.host = req.host
        flow.request.port = req.port
        flow.request.path = f"{req.path}?{req.query}" if req.query else req.path
        return req

    def response(self, flow: http.HTTPFlow) -> None:
        pending: _Pending | None = flow.metadata.pop(META_KEY, None)
        if pending is None:  # not ours (refused in request()), or already finished
            return
        upstream = _response_from_flow(flow)
        resp = upstream
        if pending.answered_by == "live" and pending.classification.kind == "read":
            resp = self.overlay(self.write_log, pending.request, upstream)
        ex = pipeline.annotate(
            pending.request,
            resp,
            pending.classification,
            pending.answered_by,
            pending.run_id,
            door=pending.door,
        )
        if ex.answered_by != "live":
            self.write_log.append(ex)
        out = pipeline.respond(ex)
        if out is not None and out is not upstream:
            flow.response = _to_mitm_response(out)
        self._finish(ex)

    def error(self, flow: http.HTTPFlow) -> None:
        pending: _Pending | None = flow.metadata.pop(META_KEY, None)
        if pending is None:
            return
        ex = pipeline.annotate(
            pending.request,
            None,
            pending.classification,
            pending.answered_by,
            pending.run_id,
            extra_flags=(UPSTREAM_ERROR_FLAG,),
            door=pending.door,
        )
        self._finish(ex)

    def _finish(self, ex: Exchange) -> None:
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
