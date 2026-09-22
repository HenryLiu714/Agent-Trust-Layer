"""AnswerPolicy: decides whether an exchange is forwarded live, delegated, or answered locally.

The decision is all that lives here. What a local answer *contains* is `irimi.echo`; where a
delegated one goes is `irimi.delegation`.
"""

import logging
from dataclasses import dataclass
from typing import Protocol

from irimi import echo
from irimi.delegation import ForwardTo, delegate
from irimi.exchange import (
    FIDELITY_DELEGATED_FLAG,
    FIDELITY_L0_FLAG,
    LIVE_KINDS,
    AnsweredBy,
    Request,
    Response,
)
from irimi.pipeline import Classification

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Answer:
    answered_by: AnsweredBy
    response: Response | None  # None means "forward live"; set means "send this, do not forward"
    flags: tuple[str, ...] = ()  # merged into the Exchange by the engine
    # Set only on a `delegated` answer, and then `response` is None: the engine forwards there
    # instead of to the real service, and what comes back is what the agent receives.
    forward_to: ForwardTo | None = None


class AnswerPolicy(Protocol):
    name: str

    def answer(self, request: Request, classification: Classification) -> Answer: ...


class ShadowPolicy:
    """Reads, llm and telemetry go live. write and unknown are answered with the L0 echo."""

    name: str = "shadow"

    def answer(self, request: Request, classification: Classification) -> Answer:
        forward = delegate(request, classification)
        if forward is not None:
            return Answer(
                answered_by="delegated",
                response=None,
                flags=(FIDELITY_DELEGATED_FLAG,),
                forward_to=forward,
            )
        if classification.kind in LIVE_KINDS:
            return Answer(answered_by="live", response=None)
        try:
            body, ct = echo.fake_response(request, classification)
        except Exception:
            # The belt to reflect()'s braces. Raising here would make mitmproxy forward the flow,
            # and a forwarded write escapes shadow mode - an empty object is far better.
            logger.exception("irimi: the L0 echo failed; answering with an empty object")
            body, ct = b"{}", echo.JSON_CT
        return Answer(
            answered_by="fake-L0",
            response=Response(status=200, headers=(("content-type", ct),), body=body),
            flags=(FIDELITY_L0_FLAG,),
        )
