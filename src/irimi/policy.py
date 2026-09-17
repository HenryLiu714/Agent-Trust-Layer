"""AnswerPolicy: decides whether an exchange is forwarded live or answered locally."""

import json
from dataclasses import dataclass
from typing import Literal, Protocol

from irimi.exchange import AnsweredBy, Kind, Request, Response

FAKE_L0_BODY: bytes = json.dumps({}).encode()


@dataclass(frozen=True)
class Answer:
    answered_by: AnsweredBy
    response: Response | None  # None means "forward live"; set means "send this, do not forward"


class AnswerPolicy(Protocol):
    name: str

    def answer(self, request: Request, kind: Kind) -> Answer: ...


class ShadowPolicy:
    """Reads, llm and telemetry go live. write and unknown are answered with a placeholder L0."""

    name: Literal["shadow"] = "shadow"

    def answer(self, request: Request, kind: Kind) -> Answer:
        if kind in ("read", "llm", "telemetry"):
            return Answer(answered_by="live", response=None)
        return Answer(
            answered_by="fake-L0",
            response=Response(
                status=200,
                headers=(("content-type", "application/json"),),
                body=FAKE_L0_BODY,
            ),
        )
