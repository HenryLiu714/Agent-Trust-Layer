"""The precondition check: before irimi fakes a write whose route names a `precondition:`, it
reads the object the write names from the real service, applies the run's own faked writes to
it, and asks the service's check whether the real service would have taken the write.

`check` is the one entry point. It never raises: a check irimi could not finish is
`not_evaluable`, and the write is still faked. The read it makes is returned for the caller to
record; it never reaches the agent.
"""

import json
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from irimi import bodies, services, writelog
from irimi.classify import Classification, classify
from irimi.exchange import ANY_METHOD, Exchange, Precondition, Request
from irimi.reader import NoReader, Reader, answered_as
from irimi.servicemap import MapIndex, fill_path

logger = logging.getLogger(__name__)

NOT_EVALUABLE = Precondition("not_evaluable")


@dataclass(frozen=True)
class Checked:
    """What the check made of one write: its precondition, the read it issued, and the rejection
    when the service would have refused it. The default is "nothing was asked" - a route with no
    `precondition:`, or no reader to ask with."""

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

    A reader that is `NoReader` asks nothing, so nothing is claimed: the same state as a route
    with no `precondition:`, and not `not_evaluable`, which is for a question irimi put and could
    not get an answer to. `observe_read` is handed the read's body, because it never passes the
    engine's `response` hook and Slack's `ts` watermark has to learn from it.

    `currency` is read off the same document the verdict saw, so a `not_evaluable` verdict keeps
    it: irimi did read the charge, and what a write is denominated in is a different question
    from whether it could be checked.
    """
    route = classification.route
    if route is None or not route.precondition or isinstance(reader, NoReader):
        return Checked()
    issued: tuple[Exchange, ...] = ()
    try:
        service = classification.service
        # A map naming a check nothing implements is the same class of thing as a fixture that
        # will not load: honest degradation, said out loud.
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
        # An exception means irimi did put the question and could not answer it, so this is
        # `not_evaluable` and never "nothing asked".
        logger.exception("irimi: precondition check failed; faking the write unchecked")
        return Checked(NOT_EVALUABLE, issued)


def _probe_for(
    request: Request,
    service: str,
    found: services.Check,
    proposal: services.Proposal,
    maps: MapIndex,
) -> tuple[services.Probe, Request, Classification] | None:
    """The read the check needs, as a request, and how the maps classify it, or None when it
    cannot be made."""
    probe = found.probe(proposal)
    if probe is None:
        return None
    probe_request = probe_request_for(request, service, probe, maps)
    if probe_request is None:
        logger.warning(
            "irimi: no route of %s's map builds the precondition read %s", service, probe.operation
        )
        return None
    # "Read operations only, never a read-like POST", in the one form that can be enforced: ask
    # the maps, which are what says a Slack `conversations.info` POST is a read, and refuse
    # whatever they do not call one.
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
    """Make the check read, recorded as irimi's own: with no response when the reader got none,
    so a network failure, a 429 and a body irimi cannot use all leave a trace of the read irimi
    attempted on the agent's behalf. Its span is the `Reader` call's, not the write's."""
    started_at = time.time()
    try:
        response = reader(probe_request)
    except Exception as exc:
        # The `Reader` contract: raising is the same answer as None.
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
    """The verdict on the read's document with the run's writes applied to it: the same effects
    the overlay applies to a live read, so the check sees what the agent would see."""
    document: Any = bodies.json_object(body)
    if document is None:
        return Checked(NOT_EVALUABLE, issued)
    applied = writelog.apply(
        probe_cls.service, probe_cls.operation, probe_request, lambda: document, writes
    )
    if applied is not None:
        if applied.partial:
            # A write checked against a world known to be incomplete was not checked. Passing
            # it could bless a write production would refuse; rejecting it could invent a
            # refusal that never would have happened.
            return Checked(NOT_EVALUABLE, issued)
        document = applied.document
    currency = found.currency(document) if found.currency is not None else ""
    verdict = found.verdict(proposal, document)
    if isinstance(verdict, services.NotEvaluable):
        # The check read the document and could not tell - a Slack `missing_scope`, a charge
        # whose own numbers will not parse. Distinct from a pass, which claims the write was
        # checked and would have been taken.
        return Checked(Precondition("not_evaluable", currency=currency), issued)
    if verdict is not None:
        return Checked(Precondition("rejected", verdict.code, currency), issued, verdict)
    return Checked(Precondition("passed", currency=currency), issued)


def probe_request_for(
    request: Request, service: str, probe: services.Probe, maps: MapIndex
) -> Request | None:
    """The precondition read, addressed where the write was and carrying its credentials, or None
    when the write's host has no single route naming the probe's operation (or the probe does not
    fill its path).

    The method and path are the map's route for the operation, so a probe repeats neither. The
    agent's credentials (`services.probe_headers_of`) and its scope headers are exactly what make
    this read see the world the write would have been made in. Nothing else of the agent's is
    copied: its other headers would send an `Idempotency-Key` on a GET.
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
