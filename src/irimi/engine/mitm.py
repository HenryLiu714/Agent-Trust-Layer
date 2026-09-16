"""mitmproxy-backed Engine. The only module allowed to import mitmproxy."""

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass

from mitmproxy import http
from mitmproxy.options import Options
from mitmproxy.tools.dump import DumpMaster

from irimi import ca, pipeline
from irimi.engine import EngineConfig, OnExchange
from irimi.exchange import AnsweredBy, Exchange, Headers, Request, Response
from irimi.overlay import Overlay
from irimi.policy import AnswerPolicy
from irimi.store import TraceStore

META_KEY = "irimi"  # flow.metadata slot holding the per-flow state below
UPSTREAM_ERROR_FLAG = "upstream-error"


@dataclass(frozen=True)
class _Pending:
    request: Request
    classification: pipeline.Classification
    run_id: str
    answered_by: AnsweredBy


def _headers_from_fields(fields: Sequence[tuple[bytes, bytes]]) -> Headers:
    return tuple((k.decode("latin-1"), v.decode("latin-1")) for k, v in fields)


def _request_from_flow(flow: http.HTTPFlow) -> Request:
    return pipeline.parse(
        flow.request.method,
        flow.request.scheme,
        flow.request.host,
        flow.request.port,
        flow.request.path,
        _headers_from_fields(flow.request.headers.fields),
        flow.request.content,
    )


def _response_from_flow(flow: http.HTTPFlow) -> Response:
    assert flow.response is not None
    return Response(
        status=flow.response.status_code,
        headers=_headers_from_fields(flow.response.headers.fields),
        body=flow.response.content or b"",
    )


def _to_mitm_response(r: Response) -> http.Response:
    return http.Response.make(r.status, r.body, dict(r.headers))


class IrimiAddon:
    """The mitmproxy addon: runs the pipeline on every flow. One instance per engine."""

    def __init__(
        self,
        config: EngineConfig,
        policy: AnswerPolicy,
        store: TraceStore,
        overlay: Overlay,
        on_exchange: OnExchange | None,
        ready: asyncio.Event,
    ) -> None:
        self.config = config
        self.policy = policy
        self.store = store
        self.overlay = overlay
        self.on_exchange = on_exchange
        self.ready = ready
        self.write_log: list[Exchange] = []

    def running(self) -> None:
        self.ready.set()

    def request(self, flow: http.HTTPFlow) -> None:
        req = _request_from_flow(flow)
        cls = pipeline.classify(req)
        run_id = pipeline.attribute_run(req, self.config.run_id)
        ans = self.policy.answer(req, cls.kind)
        flow.metadata[META_KEY] = _Pending(req, cls, run_id, ans.answered_by)
        if ans.response is not None:
            flow.response = _to_mitm_response(ans.response)

    def response(self, flow: http.HTTPFlow) -> None:
        pending: _Pending = flow.metadata[META_KEY]
        resp = _response_from_flow(flow)
        if pending.answered_by == "live" and pending.classification.kind == "read":
            resp = self.overlay(self.write_log, pending.request, resp)
        ex = pipeline.annotate(
            pending.request, resp, pending.classification, pending.answered_by, pending.run_id
        )
        if ex.answered_by != "live":
            self.write_log.append(ex)
        out = pipeline.respond(ex)
        if out is not None and out is not resp:
            flow.response = _to_mitm_response(out)
        flow.metadata["irimi_done"] = True  # error() must not finish this flow again
        self._finish(ex)

    def error(self, flow: http.HTTPFlow) -> None:
        if META_KEY not in flow.metadata or "irimi_done" in flow.metadata:
            return
        pending: _Pending = flow.metadata[META_KEY]
        ex = pipeline.annotate(
            pending.request,
            None,
            pending.classification,
            pending.answered_by,
            pending.run_id,
            extra_flags=(UPSTREAM_ERROR_FLAG,),
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
        # Created in run() so the Event and DumpMaster bind to the loop that serves them.
        self._ready: asyncio.Event | None = None
        self._master: DumpMaster | None = None
        self._port: int | None = None

    async def run(self) -> None:
        # The bundle must exist before DumpMaster, or mitmproxy silently mints its own CA.
        ca.write_mitm_bundle(self.config.ca, self.config.confdir)
        self._ready = asyncio.Event()
        opts = Options(
            listen_host=self.config.listen_host,
            listen_port=self.config.listen_port,
            confdir=str(self.config.confdir),
            mode=["regular"],
        )
        self._master = DumpMaster(opts, with_termlog=False, with_dumper=False)
        self._master.addons.add(
            IrimiAddon(
                self.config, self.policy, self.store, self.overlay, self.on_exchange, self._ready
            )
        )
        try:
            await self._master.run()
        finally:
            self.store.close()

    async def wait_ready(self) -> None:
        while self._ready is None:
            await asyncio.sleep(0.01)
        await self._ready.wait()
        assert self._master is not None
        self._port = self._master.addons.get("proxyserver").listen_addrs()[0][1]

    def listen_port(self) -> int | None:
        return self._port

    def shutdown(self) -> None:
        if self._master:
            self._master.shutdown()
