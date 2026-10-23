"""The engine's own reads: the `Reader` seam L3 issues its one precondition read through, and
`UpstreamReader`, shadow mode's reader over the real service. Here rather than in `policy` because
a network client is not a decision; `cli._build_engine` is the only place in the product that
builds one."""

import logging
import ssl
import urllib.error
import urllib.request
from typing import Any, Protocol

from irimi.exchange import AnsweredBy, Flag, Request, Response
from irimi.limits import MAX_BODY_BYTES
from irimi.opener import no_redirect_opener

logger = logging.getLogger(__name__)


# How long the engine waits for its own precondition read. The request hook runs the decision on a
# worker thread, so this delays one write's answer and nothing else - but a write whose
# answer never comes is a hung agent, so it is bounded, once, with no retry: a retried precondition
# read is a second real request made on the agent's behalf that the agent did not make.
PRECONDITION_TIMEOUT_S = 5.0


class Reader(Protocol):
    """One real read, issued by the engine rather than by the agent.

    Returning None, or raising, means `not_evaluable`: irimi could not find out, says so on the
    exchange, and falls back to the L2 answer. Shadow mode's reader dials the real upstream; a
    replay's reads the recording (`replay.index.RecordingReader`), which is why this is a seam
    and not a call inside the engine.

    A reader also says what its reads are answered by, and the flags a read it answered and one
    it could not answer carry, so the policy records the read as what it was: `recording` for one
    a replay answered from disk, `replay-miss` for one the recording lacked. A reader that
    subclasses this one is live unless it says otherwise.
    """

    answered_by: AnsweredBy = "live"
    flags: tuple[Flag, ...] = ()
    miss_flags: tuple[Flag, ...] = ()

    def __call__(self, request: Request) -> Response | None: ...


def answered_as(reader: Reader, answered: bool) -> tuple[AnsweredBy, tuple[Flag, ...]]:
    """`(answered_by, flags)` for a read `reader` made, and `answered` or not."""
    return reader.answered_by, reader.flags if answered else reader.miss_flags


class NoReader(Reader):
    """The default: a policy that does not do L3 at all.

    Distinct from a reader that tried and could not tell. A precondition on a policy holding this
    is recorded `precondition: None` - the same state as a route that declares no check - because
    nothing was asked, and `not_evaluable` is reserved for a question irimi put and could not get
    an answer to. Shadow mode never holds one: `cli._build_engine` wires `UpstreamReader`, and
    `tests/test_invariants.py` pins that it does.
    """

    def __call__(self, request: Request) -> Response | None:
        return None


class UpstreamReader(Reader):
    """`Reader` over the real service, on stdlib urllib - no new dependency, no proxy.

    It dials `request.host:request.port` exactly as the classifier saw them, so a test map
    claiming a loopback stub is dialled at the stub and the real Stripe is never touched. It does
    not go through irimi's own listener: the agent's request has already been rewritten to its
    upstream by the time the policy sees it.

    No proxy and no redirect. `urllib.request.urlopen`'s default opener reads
    `HTTP_PROXY`/`HTTPS_PROXY` and, on macOS, the system's proxy settings, which mitmproxy's own
    forwarding never does: in a shell with a proxy exported it would send this read, and only this
    one, to that proxy, or to nothing when it is not running, and every L3 check would come back
    `not_evaluable`. It also follows a 301, 302, 303, 307 or 308 to wherever it points, a host no
    map classified. So the opener names no proxy, and a 3xx comes back as the answer it is, which
    the policy, evaluating only a 200, takes as `not_evaluable`.
    """

    def __init__(self) -> None:
        self._opener = no_redirect_opener(
            urllib.request.HTTPSHandler(context=ssl.create_default_context())
        )

    def __call__(self, request: Request) -> Response | None:
        req = urllib.request.Request(
            request.url,
            data=request.body or None,
            headers={k: v for k, v in request.headers if k not in ("host", "content-length")},
            method=request.method,
        )
        try:
            with self._opener.open(req, timeout=PRECONDITION_TIMEOUT_S) as resp:
                return _capped(resp.status, resp.headers.items(), resp)
        except urllib.error.HTTPError as err:
            # A 429 or a 404 is information, not a failure to read: the exchange records it before
            # the policy calls it `not_evaluable`.
            with err:
                return _capped(err.code, err.headers.items(), err)
        except Exception as exc:
            # Not `logger.exception`: an unreachable upstream is a normal outcome here, not a bug.
            logger.warning("irimi: the precondition read of %s failed: %s", request.url, exc)
            return None


def _capped(status: int, headers: Any, body: Any) -> Response | None:
    """The response, or None when its body is past `MAX_BODY_BYTES`.

    Read with a cap rather than whole, so a huge body cannot be pulled into memory on the answer
    path only to be refused by `bodies.json_object` afterwards.
    """
    data = body.read(MAX_BODY_BYTES + 1)
    if len(data) > MAX_BODY_BYTES:
        logger.warning("irimi: a precondition read answered more than %d bytes", MAX_BODY_BYTES)
        return None
    return Response(status=status, headers=tuple(headers), body=data)
