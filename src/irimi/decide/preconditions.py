"""The precondition check: would the real service have taken this write?

Before faking a write whose route names a `precondition:`, irimi's check read fetches the object
from the real service, the run's faked writes are applied to it, and the service's check judges.

- `check` never raises: a check irimi could not finish is `not_evaluable` and the write is faked.
- The check read is returned for the caller to record; it never reaches the agent.
"""

import json
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from irimi import services
from irimi.core import bodies
from irimi.core.exchange import ANY_METHOD, Exchange, Precondition, Request
from irimi.decide import writelog
from irimi.decide.reader import NoReader, Reader, answered_as
from irimi.servicemaps import MapIndex, fill_path
from irimi.servicemaps.classify import Classification, classify

logger = logging.getLogger(__name__)

NOT_EVALUABLE = Precondition("not_evaluable")


@dataclass(frozen=True)
class Checked:
    """The check's precondition, the read it issued, and the rejection if the service would refuse.

    The default means nothing was asked: no `precondition:` on the route, or no reader.
    """

    precondition: Precondition = Precondition()
    issued: tuple[Exchange, ...] = ()
    rejection: services.Rejection | None = None


def check(
    request: Request,
    classification: Classification,
    writes: Sequence[Exchange],
    run_id: str,
    reader: Reader,
    maps: MapIndex,
    observe_read: Callable[[str, bytes], None],
) -> Checked:
    """Check the write against real state plus this run's writes, before it is faked.

    `NoReader` asks nothing and returns the default `Checked`. `observe_read` gets the check
    read's body, which never passes the engine's `response` hook (Slack's `ts` watermark needs
    it). A `not_evaluable` verdict keeps the currency read (see `Precondition`).
    """
    route = classification.route
    if route is None or not route.precondition or isinstance(reader, NoReader):
        return Checked()
    issued: tuple[Exchange, ...] = ()
    try:
        service = classification.service
        # A map naming a check nothing implements degrades with a warning, as a broken fixture does.
        found = services.of(service).preconditions.get(route.precondition)
        if found is None:
            logger.warning("irimi: no precondition named %r for %s", route.precondition, service)
            return Checked(NOT_EVALUABLE)
        proposal = services.Proposal(classification.operation, bodies.reflect(request))
        probed = _probe_for(request, service, found, proposal, maps)
        if probed is None:
            return Checked(NOT_EVALUABLE)
        probe, probe_request, probe_cls = probed
        read = _read(probe, probe_request, probe_cls, run_id, reader)
        issued = (read,)
        if read.response is None or read.response.status != 200:
            # Only a 200 is evaluable. A 429, a 404 or a 5xx says nothing about the write.
            return Checked(NOT_EVALUABLE, issued)
        observe_read(service, read.response.body)
        return _judge(found, proposal, probe_request, probe_cls, read.response.body, writes, issued)
    except Exception:
        # irimi asked and could not answer, so this is `not_evaluable`, not "nothing asked".
        logger.exception("irimi: precondition check failed; faking the write unchecked")
        return Checked(NOT_EVALUABLE, issued)


def _probe_for(
    request: Request,
    service: str,
    found: services.Check,
    proposal: services.Proposal,
    maps: MapIndex,
) -> tuple[services.Probe, Request, Classification] | None:
    """Return the check read as a request with its classification, or None if it cannot be made."""
    probe = found.probe(proposal)
    if probe is None:
        return None
    probe_request = probe_request_for(request, service, probe, maps)
    if probe_request is None:
        logger.warning(
            "irimi: no route of %s's map builds the precondition read %s", service, probe.operation
        )
        return None
    # Only a read the maps classify as a read is made (a Slack `conversations.info` POST is one).
    probe_cls = classify(probe_request, maps)
    if probe_cls.kind != "read":
        logger.warning(
            "irimi: refusing the precondition read %s, which the maps call %s",
            probe.operation,
            probe_cls.kind,
        )
        return None
    return probe, probe_request, probe_cls


def _read(
    probe: services.Probe,
    probe_request: Request,
    probe_cls: Classification,
    run_id: str,
    reader: Reader,
) -> Exchange:
    """Make the check read and return it as an engine-issued exchange, timed by the `Reader` call.

    A failed read is recorded with no response, so every attempt leaves a trace.
    """
    started_at = time.time()
    try:
        response = reader(probe_request)
    except Exception as exc:
        # By the `Reader` contract, raising means the same as None.
        logger.warning("irimi: the precondition read %s raised: %s", probe.operation, exc)
        response = None
    ended_at = time.time()
    answered_by, flags = answered_as(reader, response is not None)
    return Exchange(
        request=probe_request,
        response=response,
        service=probe_cls.service,
        operation=probe_cls.operation,
        kind=probe_cls.kind,
        answered_by=answered_by,
        run_id=run_id,
        flags=probe_cls.flags + flags,
        issued_by="engine",
        started_at=started_at,
        ended_at=ended_at,
    )


def _judge(
    found: services.Check,
    proposal: services.Proposal,
    probe_request: Request,
    probe_cls: Classification,
    body: bytes,
    writes: Sequence[Exchange],
    issued: tuple[Exchange, ...],
) -> Checked:
    """Return the verdict on the read's document with the run's writes applied."""
    document: Any = bodies.json_object(body)
    if document is None:
        return Checked(NOT_EVALUABLE, issued)
    applied = writelog.apply(
        probe_cls.service, probe_cls.operation, probe_request, lambda: document, writes
    )
    if applied is not None:
        if applied.partial:
            # Against a document known to be incomplete, neither a pass nor a rejection is honest.
            return Checked(NOT_EVALUABLE, issued)
        document = applied.document
    currency = found.currency(document) if found.currency is not None else ""
    verdict = found.verdict(proposal, document)
    if isinstance(verdict, services.NotEvaluable):
        # The check read the document and could not tell (a Slack `missing_scope`, an unparsable
        # charge); a pass would claim the write was checked.
        return Checked(Precondition("not_evaluable", currency=currency), issued)
    if verdict is not None:
        return Checked(Precondition("rejected", verdict.code, currency), issued, verdict)
    return Checked(Precondition("passed", currency=currency), issued)


def probe_request_for(
    request: Request, service: str, probe: services.Probe, maps: MapIndex
) -> Request | None:
    """Return the check read, sent where the write was with its credentials, or None.

    None when the host has no single route for the probe's operation or the probe does not fill
    its path. Only the credential (`services.probe_headers_of`) and scope headers are copied;
    others would, for example, send an `Idempotency-Key` on a GET.
    """
    route = maps.operation_route(request.host, probe.operation)
    if route is None or route.method == ANY_METHOD:
        return None
    path = fill_path(route.path, probe.params)
    if path is None:
        return None
    wanted = (*services.probe_headers_of(service), *services.of(service).scope_headers)
    headers = [(k, v) for k, v in request.headers if k in wanted]
    headers.append(("accept", "application/json"))
    body = b""
    if probe.posted is not None:
        body = json.dumps(probe.posted).encode()
        headers.append(("content-type", "application/json; charset=utf-8"))
    return Request(
        method=route.method,
        scheme=request.scheme,
        host=request.host,
        port=request.port,
        path=path,
        query=probe.query,
        headers=tuple(headers),
        body=body,
    )
