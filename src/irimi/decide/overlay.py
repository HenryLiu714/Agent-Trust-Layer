"""The overlay: applies the run's faked writes to a live read, purely, so replay reuses it.

The engine never calls it for reads sent to a target (`target_reads: true`; the target owns that
state) or for streamed responses (headers already sent).
"""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Protocol

from irimi import services
from irimi.core import bodies
from irimi.core.exchange import Exchange, OverlayCoverage, Request, Response
from irimi.decide import writelog
from irimi.decide.writelog import Translation
from irimi.servicemaps.classify import Classification

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Overlaid:
    """What the overlay did to one live read.

    `response` is the same object it was handed when nothing changed; the engine stamps
    `overlay` on any other. `fidelity` is separate: a page the effects cannot model is `partial`
    with the body untouched. An effect replaces the status only for a read of an object this run
    minted, which the real service answers with an error (`services.Applied.status`).
    """

    response: Response
    fidelity: OverlayCoverage | None = None


class Overlay(Protocol):
    def __call__(
        self,
        write_log: Sequence[Exchange],
        classification: Classification,
        read_request: Request,
        upstream_response: Response,
        translated_from: str | None = None,
    ) -> Overlaid: ...

    def rewrite(
        self,
        write_log: Sequence[Exchange],
        classification: Classification,
        read_request: Request,
    ) -> Translation | None:
        """Return the read to forward instead of `read_request`, or None to forward it as is."""
        ...


class NoOverlay:
    def __call__(
        self,
        write_log: Sequence[Exchange],
        classification: Classification,
        read_request: Request,
        upstream_response: Response,
        translated_from: str | None = None,
    ) -> Overlaid:
        return Overlaid(upstream_response)

    def rewrite(
        self,
        write_log: Sequence[Exchange],
        classification: Classification,
        read_request: Request,
    ) -> Translation | None:
        return None


class ServiceOverlay:
    """The real overlay: `irimi.services`' effect tables, keyed by the read's classification.

    Neither entry point raises (fail-closed hook rule): a failure leaves the read as the service
    sent it.
    """

    def __call__(
        self,
        write_log: Sequence[Exchange],
        classification: Classification,
        read_request: Request,
        upstream_response: Response,
        translated_from: str | None = None,
    ) -> Overlaid:
        try:
            return self._apply(
                write_log, classification, read_request, upstream_response, translated_from
            )
        except Exception:
            logger.exception("irimi: the overlay failed; the agent gets the upstream read")
            return Overlaid(upstream_response, "partial")

    def rewrite(
        self,
        write_log: Sequence[Exchange],
        classification: Classification,
        read_request: Request,
    ) -> Translation | None:
        try:
            return self._rewrite(write_log, classification, read_request)
        except Exception:
            logger.exception("irimi: the overlay's rewrite failed; forwarding the read unchanged")
            return None

    def _apply(
        self,
        write_log: Sequence[Exchange],
        classification: Classification,
        read_request: Request,
        upstream_response: Response,
        translated_from: str | None,
    ) -> Overlaid:
        # An unreadable body comes back `partial`, and the agent gets what the service sent.
        applied = writelog.apply(
            classification.service,
            classification.operation,
            read_request,
            lambda: bodies.json_object(upstream_response.body),
            write_log,
            translated_from,
        )
        if applied is None:
            return Overlaid(upstream_response)
        if (applied.changed or applied.status is not None) and not _may_answer_over(
            upstream_response.status, applied.status
        ):
            # A 401, a 429 or a 5xx is the service's answer about the caller or itself, which
            # production would have sent too: never edited into a success, and `partial`.
            return Overlaid(upstream_response, "partial")
        fidelity: OverlayCoverage | None = None
        if applied.partial:
            fidelity = "partial"
        elif applied.changed or translated_from is not None or applied.status is not None:
            # A translated cursor or a replaced status is an effect even when the page needed no
            # edit, so an exchange stamped `overlay` never records `overlay: None`.
            fidelity = "full"
        # A status-only change is rebuilt too: identity is how the engine tells an untouched read.
        status = upstream_response.status if applied.status is None else applied.status
        if not applied.changed and status == upstream_response.status:
            return Overlaid(upstream_response, fidelity)
        return Overlaid(
            replace(
                upstream_response,
                status=status,
                body=json.dumps(applied.document, allow_nan=False).encode(),
            ),
            fidelity,
        )

    def _rewrite(
        self,
        write_log: Sequence[Exchange],
        classification: Classification,
        read_request: Request,
    ) -> Translation | None:
        service = classification.service
        found = writelog.scoped_writes(
            services.of(service).rewrite, service, read_request, write_log
        )
        if found is None:
            return None
        rewrite, writes = found
        rewritten = rewrite(writelog.read_of(classification.operation, read_request), writes)
        if rewritten is None:
            return None
        return Translation(replace(read_request, query=rewritten.query), rewritten.removed)


def _may_answer_over(upstream: int, replaced: int | None) -> bool:
    """Return whether effects may change a read answered `upstream`: a 2xx, or a minted 404.

    A 404 qualifies only when an effect answers it in full (`Applied.status`).
    """
    return 200 <= upstream < 300 or (upstream == 404 and replaced is not None)
