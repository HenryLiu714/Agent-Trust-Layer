"""The request pipeline as plain functions: parse, classify, attribute, annotate, respond.

No mitmproxy here; the engine calls these in order. The two neighbours that are *not* here, each
because it is a decision of its own: the reverse door (`irimi.reverse_door`) and answer targets
(`irimi.delegation`).
"""

from collections.abc import Iterable
from dataclasses import dataclass, replace

from irimi.exchange import (
    DOWNGRADED_FLAG,
    LIVE_KINDS,
    SAFE_METHODS,
    UNCLASSIFIED_FLAG,
    AnsweredBy,
    Door,
    Exchange,
    IssuedBy,
    Kind,
    OverlayFidelity,
    PreconditionOutcome,
    Request,
    Response,
    Validation,
)
from irimi.servicemap import MapIndex, Route, ServiceMap

RUN_HEADER = "irimi-run"  # header names are compared case-insensitively; stored lower-case
# Stamped on every response irimi decided rather than forwarded, carrying the `answered_by` value
# itself (`fake-L0`, `fake-L1`, `delegated`). See `answered_by_header` for why a live forward gets
# none.
ANSWERED_BY_HEADER = "irimi-answered-by"
# Put on a REQUEST irimi rewrote before forwarding it, naming the `key=value` query pair it
# removed (#43). It lives beside the response stamp because both are irimi's own wire vocabulary
# and a second spelling in `services` or `overlay` is the drift one home prevents. The only
# rewrite today is a cursor naming a refund irimi minted, which Stripe has never heard of; the
# header is what tells the response side that this page follows the minted refund, so the effects
# do not prepend it a second time. Unknown *query* parameters were not an option - Stripe rejects
# them with a 400 - and a header costs the agent nothing and shows up in any capture.
REWROTE_HEADER = "irimi-rewrote"
LIVE_ANSWER: AnsweredBy = "live"


@dataclass(frozen=True)
class Classification:
    service: str
    operation: str
    kind: Kind
    flags: tuple[str, ...]
    # What MapIndex.route_for returned, so the faker (#11), the summary (#13) and the answer
    # target (#16) do not have to look the route up a second time. None when nothing matched.
    matched: tuple[ServiceMap, Route] | None = None
    # The service claiming this host, which is not the same question as `matched`: a service-level
    # `target:` delegates the routes its own map does not list too, and for those `matched` is
    # None while this is set. None when no map claims the host at all (#16).
    service_map: ServiceMap | None = None


def parse(
    method: str,
    scheme: str,
    host: str,
    port: int,
    path_and_query: str,
    headers: Iterable[tuple[str, str]],
    body: bytes | None,
) -> Request:
    """Normalize wire values into a Request: upper-case method, lower-case host and header names,
    path split from query, empty path becomes "/", None body becomes b""."""
    path, _, query = path_and_query.partition("?")
    return Request(
        method=method.upper(),
        scheme=scheme.lower(),
        host=host.lower(),
        port=port,
        path=path or "/",
        query=query,
        headers=tuple((k.lower(), v) for k, v in headers),
        body=body or b"",
    )


def classify(request: Request, maps: MapIndex | None = None) -> Classification:
    """What this request is, by the precedence the design fixes (§4.3, D4).

    A route rule in a service map wins; then that service's `default_kind`; then the RFC 9110
    fallback, where `GET`/`HEAD`/`OPTIONS` are reads; then `unknown`. A host no map claims keeps
    its host name as the service and goes straight to the fallback, so it is still MITM'd: its
    reads forward live and everything else is answered locally. `unknown` always carries the
    `unclassified` flag, whether a map declared it or the fallback reached it.

    `maps=None` means the empty index: the fallback alone. Pure function, no I/O.
    """
    matched = (
        maps.route_for(request.host, request.method, request.path) if maps is not None else None
    )
    if matched is not None:
        # route_for found it through service_for, so this costs no second lookup.
        service_map: ServiceMap | None
        service_map, route = matched
        service, operation, kind = service_map.service, route.operation, route.kind
    else:
        service_map = maps.service_for(request.host) if maps is not None else None
        service = service_map.service if service_map is not None else request.host
        operation = f"{request.method} {request.path}"
        if service_map is not None and service_map.default_kind is not None:
            kind = service_map.default_kind
        elif request.method in SAFE_METHODS:
            kind = "read"
        else:
            kind = "unknown"
    kind, downgraded = _refuse_live_on_an_unnamed_method(kind, request, matched)
    flags: tuple[str, ...] = (UNCLASSIFIED_FLAG,) if kind == "unknown" else ()
    if downgraded:
        flags += (DOWNGRADED_FLAG,)
    return Classification(service, operation, kind, flags, matched, service_map)


def _refuse_live_on_an_unnamed_method(
    kind: Kind, request: Request, matched: tuple[ServiceMap, Route] | None
) -> tuple[Kind, bool]:
    """THE SCOPE RULE, decision half (see `servicemap.rules`): no classification that forwards
    live may apply to an unsafe method it did not name explicitly.

    The loader refuses a live kind on `*` and makes a destructive one justify itself, so no map
    can reach this today. This asks the question of the request in front of us instead of of the
    configuration, so a live kind that arrives by some route the loader does not police - a
    `default_kind` rule that drifts again, a wildcard host, a map built in code, a layer added
    later - is answered locally rather than performed on the real service. Downgrading to
    `unknown` is the fail-safe direction: a write answered locally costs the agent an echo, and
    a DELETE forwarded live cannot be undone.
    """
    if kind not in LIVE_KINDS or request.method in SAFE_METHODS:
        return kind, False
    route = matched[1] if matched is not None else None
    if route is not None and route.method == request.method:
        return kind, False  # the route named this verb; that is the explicit part
    return "unknown", True


def attribute_run(request: Request, default_run_id: str) -> str:
    """The run this exchange belongs to: the Irimi-Run header if the client sent one, else the
    engine's own run id."""
    for name, value in request.headers:
        if name == RUN_HEADER and value.strip():
            return value.strip()
    return default_run_id


def annotate(
    request: Request,
    response: Response | None,
    classification: Classification,
    answered_by: AnsweredBy,
    run_id: str,
    extra_flags: tuple[str, ...] = (),
    door: Door = "forward",
    target: str = "",
    overlay: OverlayFidelity | None = None,
    precondition: PreconditionOutcome | None = None,
    issued_by: IssuedBy = "agent",
) -> Exchange:
    """Build the Exchange. Anything the engine answered is unvalidated; live forwards are also
    unvalidated for now (validated is reserved for record mode, later phases). `overlay` is how
    much of the write log the overlay expressed in this read, and is None for everything it did
    not consider. `precondition` is what L3 decided about a write before it was faked, and is None
    when nothing was asked (#45). `issued_by` is `engine` for a read irimi made on its own account
    to decide about a write, and `agent` for everything the agent sent (#45)."""
    validation: Validation = "unvalidated"
    return Exchange(
        request=request,
        response=response,
        service=classification.service,
        operation=classification.operation,
        kind=classification.kind,
        answered_by=answered_by,
        validation=validation,
        run_id=run_id,
        door=door,
        flags=classification.flags + extra_flags,
        target=target,
        overlay=overlay,
        precondition=precondition,
        issued_by=issued_by,
    )


def answered_by_header(answered_by: AnsweredBy) -> str | None:
    """The `Irimi-Answered-By` value an answer must carry, or None when it must carry none.

    The value IS `answered_by`, not a second vocabulary beside it: `fake-L0`, `fake-L1` since #41,
    `delegated` since #16, `overlay` and `recorded` in later phases. One rule, so a new way of
    answering cannot ship a response that does not say who answered it - the header is how a
    client, a test, or a developer reading a capture tells an answer of ours from the real
    service's. It takes the value rather than the Exchange because the engine asks before there is
    one: a streamed answer is stamped in `responseheaders`, where only `_Pending` exists yet.

    A **live forward gets no header**, deliberately. Everything on that path is the real
    service's: adding a header of ours to it would make the response the agent sees differ from
    the one the service sent, which is the one thing a live read must not do. Absence is the
    signal, and it is the signal the invariant tests read.
    """
    return None if answered_by == LIVE_ANSWER else answered_by


def respond(exchange: Exchange) -> Response | None:
    """What goes back to the client: the answer, stamped with who produced it (#12).

    Pure: it returns the response to send and never touches the flow. The engine forwards
    `exchange.response` unchanged when this hands back the very object it was given, which is how
    a live forward stays byte-identical to what the service sent.
    """
    if exchange.response is None:
        return None
    stamp = answered_by_header(exchange.answered_by)
    if stamp is None:
        return exchange.response
    # Any header of this name already on the response came from somewhere else - a target that
    # echoes headers, or a service of the same name - and ours is the one that is true here.
    kept = tuple((k, v) for k, v in exchange.response.headers if k.lower() != ANSWERED_BY_HEADER)
    return replace(exchange.response, headers=kept + ((ANSWERED_BY_HEADER, stamp),))
