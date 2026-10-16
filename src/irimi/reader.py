"""The engine's own reads: the `Reader` seam L3 issues its one precondition read through (#45), and
`UpstreamReader`, shadow mode's reader over the real service. Here rather than in `policy` because
a network client is not a decision; `cli._build_engine` is the only place in the product that
builds one."""

import logging
import ssl
import urllib.error
import urllib.request
from typing import Any, Protocol

from irimi import bodies
from irimi.exchange import AnsweredBy, Request, Response

logger = logging.getLogger(__name__)


# How long the engine waits for its own precondition read. The request hook runs the decision on a
# worker thread as of #45, so this delays one write's answer and nothing else - but a write whose
# answer never comes is a hung agent, so it is bounded, once, with no retry: a retried precondition
# read is a second real request made on the agent's behalf that the agent did not make.
PRECONDITION_TIMEOUT_S = 5.0


class Reader(Protocol):
    """One real read, issued by the engine rather than by the agent (#45).

    Returning None, or raising, means `not_evaluable`: irimi could not find out, says so on the
    exchange, and falls back to the L2 answer. Shadow mode's reader dials the real upstream; a
    replay's reads the recording (`recording.RecordingReader`, #82), which is why this is a seam
    and not a call inside the engine.

    A reader may also say what its reads are answered by, and the flags a read it answered and one
    it could not answer carry, so the policy records the read as what it was: `recording` for one
    a replay answered from disk, `replay-miss` for one the recording lacked (#82). `answered_as`
    reads it, and a reader that says nothing is a live one.
    """

    def __call__(self, request: Request) -> Response | None: ...


def answered_as(reader: Reader, answered: bool) -> tuple[AnsweredBy, tuple[str, ...]]:
    """`(answered_by, flags)` for a read `reader` made, and `answered` or not: its own
    `answered_by`, and its `flags` or `miss_flags`, when it names them, as
    `recording.RecordingReader` does, and `("live", ())` when it does not (#82)."""
    answered_by: AnsweredBy = getattr(reader, "answered_by", "live")
    flags: tuple[str, ...] = getattr(reader, "flags" if answered else "miss_flags", ())
    return answered_by, flags


class NoReader:
    """The default: a policy that does not do L3 at all.

    Distinct from a reader that tried and could not tell. A precondition on a policy holding this
    is recorded `precondition: None` - the same state as a route that declares no check - because
    nothing was asked, and `not_evaluable` is reserved for a question irimi put and could not get
    an answer to. Shadow mode never holds one: `cli._build_engine` wires `UpstreamReader`, and
    `tests/test_invariants.py` pins that it does.
    """

    def __call__(self, request: Request) -> Response | None:
        return None


class UpstreamReader:
    """`Reader` over the real service, on stdlib urllib - no new dependency, no proxy.

    It dials `request.host:request.port` exactly as the classifier saw them, so a test map
    claiming a loopback stub is dialled at the stub and the real Stripe is never touched. It does
    not go through irimi's own listener: the agent's request has already been rewritten to its
    upstream by the time the policy sees it.
    """

    def __call__(self, request: Request) -> Response | None:
        req = urllib.request.Request(
            request.url,
            data=request.body or None,
            headers={k: v for k, v in request.headers if k not in ("host", "content-length")},
            method=request.method,
        )
        try:
            # A context on an `http://` URL is accepted and ignored, so there is no scheme branch.
            with urllib.request.urlopen(
                req, timeout=PRECONDITION_TIMEOUT_S, context=ssl.create_default_context()
            ) as resp:
                return _capped(resp.status, resp.headers.items(), resp)
        except urllib.error.HTTPError as err:
            # A 429 or a 404 is information, not a failure to read: the exchange records it before
            # the policy calls it `not_evaluable` (#45).
            with err:
                return _capped(err.code, err.headers.items(), err)
        except Exception as exc:
            # Not `logger.exception`: an unreachable upstream is a normal outcome here, not a bug.
            logger.warning("irimi: the precondition read of %s failed: %s", request.url, exc)
            return None


def _capped(status: int, headers: Any, body: Any) -> Response | None:
    """The response, or None when its body is past `bodies.MAX_BODY_BYTES`.

    Read with a cap rather than whole, so a huge body cannot be pulled into memory on the answer
    path only to be refused by `bodies.json_object` afterwards (#45).
    """
    data = body.read(bodies.MAX_BODY_BYTES + 1)
    if len(data) > bodies.MAX_BODY_BYTES:
        logger.warning(
            "irimi: a precondition read answered more than %d bytes", bodies.MAX_BODY_BYTES
        )
        return None
    return Response(status=status, headers=tuple(headers), body=data)
