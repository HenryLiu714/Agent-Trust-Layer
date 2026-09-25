"""The run's faked writes, decoded out of the trace for whatever wants to apply them (#45).

`overlay` had this to itself until L3: a precondition checks a write against real state *plus the
run's overlay*, so `policy` needs the same decoding, and the two live in the same layer and may
not import each other. It is its own job in any case - turning `Exchange`es into
`services.Write`s is neither deciding nor overlaying - and it is pure, so Phase 5 replay decodes a
recording with the same function.
"""

import json
from collections.abc import Sequence
from typing import Any

from irimi import echo, services
from irimi.exchange import Exchange, Request

# A live body this size is not an object any service's effects model, and parsing it on the answer
# path would cost more than the read or the check it is trying to improve.
MAX_BODY_BYTES = 2_000_000


def decode(service: str, request: Request, write_log: Sequence[Exchange]) -> list[services.Write]:
    """This service's faked writes, in the scope the read is asking about.

    A write made against another connected account or another API version says nothing about
    this read, so it is left out. The scope is taken from each write's OWN request headers,
    because that is the world it was made in.
    """
    wanted = scope(service, request)
    out: list[services.Write] = []
    for exchange in write_log:
        if exchange.service != service or exchange.response is None:
            continue
        if scope(service, exchange.request) != wanted:
            continue
        answer = json_object(exchange.response.body)
        if answer is None:
            continue
        out.append(
            services.Write(
                operation=exchange.operation,
                posted=echo.reflect(exchange.request),
                answer=answer,
            )
        )
    return out


def scope(service: str, request: Request) -> tuple[str, ...]:
    names = services.SCOPE_HEADERS.get(service, ())
    return tuple(next((v for k, v in request.headers if k == name), "") for name in names)


def json_object(body: bytes) -> dict[str, Any] | None:
    """`body` as a JSON object, or None when it is not one irimi should decode."""
    if not body or len(body) > MAX_BODY_BYTES:
        return None
    try:
        parsed = json.loads(body)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None
