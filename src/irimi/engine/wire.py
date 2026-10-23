"""irimi's side of the wire: normalising a request, naming its run, and stamping an answer.

No mitmproxy here. The engine parses each flow into a `Request` with `parse`, attributes it to a
run with `attribute_run`, and stamps every answer it decided with `stamp`.
"""

from collections.abc import Iterable
from dataclasses import replace

from irimi.core.exchange import AnsweredBy, Headers, Request, Response
from irimi.protocol.headers import ANSWERED_BY_HEADER, RUN_HEADER
from irimi.protocol.trace import is_valid_run_id

# The `answered_by` of every error irimi answers with itself.
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


def attribute_run(request: Request, default_run_id: str) -> str:
    """The run this exchange belongs to: the Irimi-Run header if the client sent a valid one,
    else the engine's own run id. A value `trace.is_valid_run_id` refuses - blank, `../x`, a
    space, 65 characters - is treated as absent, because a run id becomes a directory name in the
    trace store. The header is stripped whatever its value. Its name is compared as
    `header_value` compares, so a `Request` built outside `parse` is attributed the same way."""
    for name, value in request.headers:
        if name.lower() == RUN_HEADER and is_valid_run_id(value.strip()):
            return value.strip()
    return default_run_id


def answered_by_header(answered_by: AnsweredBy) -> str | None:
    """The `Irimi-Answered-By` value an answer must carry, or None when it must carry none.

    The value IS `answered_by`, not a second vocabulary beside it: `fake-L0`, `fake-L1`,
    `delegated`, `overlay`, `recording` and `irimi`. One rule, so a new way of
    answering cannot ship a response that does not say who answered it - the header is how a
    client, a test, or a developer reading a capture tells an answer of ours from the real
    service's. It takes the value rather than the Exchange because the engine asks before there is
    one: a streamed answer is stamped in `responseheaders`, where only the `Plan` exists.

    A **live forward gets no header**, deliberately. Everything on that path is the real
    service's: adding a header of ours to it would make the response the agent sees differ from
    the one the service sent, which is the one thing a live read must not do. Absence is the
    signal, and it is the signal the invariant tests read.
    """
    return None if answered_by == "live" else answered_by


def stamp(response: Response, answered_by: AnsweredBy) -> Response:
    """What goes back to the client: `response` carrying the `Irimi-Answered-By` that
    `answered_by` owes it.

    Pure: it never touches the flow. A live forward gets `response` itself back, and the engine
    forwards a response unchanged when it is the very object it was given, which is how a live
    forward stays byte-identical to what the service sent.
    """
    value = answered_by_header(answered_by)
    if value is None:
        return response
    # Any header of this name already on the response came from somewhere else - a target that
    # echoes headers, or a service of the same name - and ours is the one that is true here.
    kept = response.without_header(ANSWERED_BY_HEADER)
    return replace(kept, headers=kept.headers + ((ANSWERED_BY_HEADER, value),))


def irimi_error(status: int, error_type: str, message: str, headers: Headers = ()) -> Response:
    """An error irimi answers with itself: `{"error": {"type": ..., "message": ...}}` as JSON.

    The one shape for every such answer, the engine's and the control endpoint's, so a client
    parses one format for "irimi refused this". Unstamped: the caller stamps it."""
    return Response.json(status, {"error": {"type": error_type, "message": message}}, headers)
