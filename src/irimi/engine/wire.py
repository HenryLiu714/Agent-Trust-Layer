"""irimi's side of the wire: normalise a request, name its run, and stamp an answer."""

from collections.abc import Iterable
from dataclasses import replace

from irimi.core.exchange import AnsweredBy, Headers, Request, Response
from irimi.protocol.headers import ANSWERED_BY_HEADER, RUN_HEADER
from irimi.protocol.trace import is_valid_run_id

IRIMI_ANSWER: AnsweredBy = "irimi"


def parse(
    method: str,
    scheme: str,
    host: str,
    port: int,
    path_and_query: str,
    headers: Iterable[tuple[str, str]],
    body: bytes | None,
) -> Request:
    """Normalise wire values into a Request: case, empty path to "/", None body to b""."""
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


def attribute_run(request: Request, default_run_id: str) -> str:
    """Return the run a valid `Irimi-Run` header names (it becomes a directory), else default."""
    for name, value in request.headers:
        if name.lower() == RUN_HEADER and is_valid_run_id(value.strip()):
            return value.strip()
    return default_run_id


def answered_by_header(answered_by: AnsweredBy) -> str | None:
    """Return the `Irimi-Answered-By` value: its `answered_by`, or None if live.

    Answered-by header: every response irimi decided carries it; a live forward carries none.
    """
    return None if answered_by == "live" else answered_by


def stamp(response: Response, answered_by: AnsweredBy) -> Response:
    """Return `response` stamped for `answered_by`; a live forward gets the same object back."""
    value = answered_by_header(answered_by)
    if value is None:
        return response
    # A same-named header already present came from a target or a service, not from irimi.
    kept = response.without_header(ANSWERED_BY_HEADER)
    return replace(kept, headers=(*kept.headers, (ANSWERED_BY_HEADER, value)))


def irimi_error(status: int, error_type: str, message: str, headers: Headers = ()) -> Response:
    """Return irimi's JSON error, `{"error": {"type": ..., "message": ...}}`, unstamped."""
    return Response.json(status, {"error": {"type": error_type, "message": message}}, headers)
