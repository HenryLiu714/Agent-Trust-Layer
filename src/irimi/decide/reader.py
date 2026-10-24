"""irimi's check read: the `Reader` protocol, and `UpstreamReader` over the real service."""

import logging
import ssl
import urllib.error
import urllib.request
from typing import Any, Protocol

from irimi.core.exchange import AnsweredBy, Flag, Request, Response
from irimi.core.limits import MAX_BODY_BYTES
from irimi.core.opener import no_redirect_opener

logger = logging.getLogger(__name__)


# A write's answer waits on this read. No retry: a second read is a request the agent never made.
PRECONDITION_TIMEOUT_S = 5.0


class Reader(Protocol):
    """One real read made by irimi; None or a raise means `not_evaluable`, and no check is made."""

    answered_by: AnsweredBy = "live"
    flags: tuple[Flag, ...] = ()
    miss_flags: tuple[Flag, ...] = ()

    def __call__(self, request: Request) -> Response | None: ...


def answered_as(reader: Reader, answered: bool) -> tuple[AnsweredBy, tuple[Flag, ...]]:
    """Return `(answered_by, flags)` for a read `reader` made, answered or not."""
    return reader.answered_by, reader.flags if answered else reader.miss_flags


class NoReader(Reader):
    """A reader that asks nothing, so the precondition is recorded `None`, not `not_evaluable`."""

    def __call__(self, request: Request) -> Response | None:
        return None


class UpstreamReader(Reader):
    """`Reader` that dials the classified `host:port`, with no proxy; a 3xx is not followed."""

    def __init__(self) -> None:
        self._opener = no_redirect_opener(
            urllib.request.HTTPSHandler(context=ssl.create_default_context())
        )

    def __call__(self, request: Request) -> Response | None:
        req = urllib.request.Request(  # noqa: S310  # the service's own http(s) URL
            request.url,
            data=request.body or None,
            headers={k: v for k, v in request.headers if k not in ("host", "content-length")},
            method=request.method,
        )
        try:
            with self._opener.open(req, timeout=PRECONDITION_TIMEOUT_S) as resp:
                return _capped(resp.status, resp.headers.items(), resp)
        except urllib.error.HTTPError as err:
            # A 429 or 404 is still an answer, recorded before the policy calls it `not_evaluable`.
            with err:
                return _capped(err.code, err.headers.items(), err)
        except Exception as exc:
            # An unreachable upstream is a normal outcome, so no traceback.
            logger.warning("irimi: the precondition read of %s failed: %s", request.url, exc)
            return None


def _capped(status: int, headers: Any, body: Any) -> Response | None:
    """Return the response, or None when its body is past `MAX_BODY_BYTES` (read with a cap)."""
    data = body.read(MAX_BODY_BYTES + 1)
    if len(data) > MAX_BODY_BYTES:
        logger.warning("irimi: a precondition read answered more than %d bytes", MAX_BODY_BYTES)
        return None
    return Response(status=status, headers=tuple(headers), body=data)
