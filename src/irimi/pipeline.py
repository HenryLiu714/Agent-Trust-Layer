"""The request pipeline as plain functions. No mitmproxy here; the engine calls these in order."""

from collections.abc import Iterable
from dataclasses import dataclass

from irimi.exchange import (
    SAFE_METHODS,
    AnsweredBy,
    Exchange,
    Kind,
    Request,
    Response,
    Validation,
)

RUN_HEADER = "irimi-run"  # header names are compared case-insensitively; stored lower-case
UNCLASSIFIED_FLAG = "unclassified"


@dataclass(frozen=True)
class Classification:
    service: str
    operation: str
    kind: Kind
    flags: tuple[str, ...]


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


def classify(request: Request) -> Classification:
    """RFC 9110 fallback only (issue #7 adds map lookup in front of this):
    safe methods are reads; everything else is unknown and flagged unclassified."""
    operation = f"{request.method} {request.path}"
    if request.method in SAFE_METHODS:
        return Classification(request.host, operation, "read", ())
    return Classification(request.host, operation, "unknown", (UNCLASSIFIED_FLAG,))


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
) -> Exchange:
    """Build the Exchange. Anything the engine answered is unvalidated; live forwards are also
    unvalidated for now (validated is reserved for record mode, later phases)."""
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
        flags=classification.flags + extra_flags,
    )


def respond(exchange: Exchange) -> Response | None:
    """What goes back to the client. Identity for now; issue #12 adds the Irimi-Answered-By
    header."""
    return exchange.response
