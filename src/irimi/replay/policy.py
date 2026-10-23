"""The `policy.AnswerPolicy` a replay runs under: answer from a stored run, else as shadow mode."""

import logging
from collections.abc import Sequence
from dataclasses import replace

from irimi.core.exchange import (
    LIVE_KINDS,
    AnsweredBy,
    Exchange,
    Flag,
    Request,
    Response,
    WriteOutcome,
)
from irimi.decide import idempotency, policy
from irimi.decide.policy import Decided, Respond, ShadowPolicy
from irimi.decide.targets import ForwardTo
from irimi.protocol.headers import ANSWERED_BY_HEADER
from irimi.protocol.trace import JSONValue, ToolCall
from irimi.replay.index import RecordingReader, ReplayIndex
from irimi.servicemaps import MapIndex
from irimi.servicemaps.classify import Classification

logger = logging.getLogger(__name__)

# The recorded answer's flags a replayed answer keeps. Classifier flags come from this request's
# classification; engine flags (truncation, upstream errors) describe only the recorded exchange.
_ANSWER_FLAGS = frozenset({Flag.FIXTURE_FAILED, Flag.IDEMPOTENT_REPLAY, Flag.IDEMPOTENCY_CONFLICT})
# Recorded headers that no longer describe the served body, which was stored decoded and whole
# (mitmproxy sets the length), plus the recorded stamp, which `wire.stamp` replaces.
_RECORDED_ONLY_HEADERS = frozenset(
    {"content-encoding", "content-length", "transfer-encoding", ANSWERED_BY_HEADER}
)


class ReplayPolicy:
    """Answer a run from a stored one where possible, else as shadow mode; never forward a write.

    | request | in the recording (`from-recording`) | not in it (`replay-miss`) |
    | --- | --- | --- |
    | llm; read before the replay diverged | the recorded response | live |
    | read, after the replay diverged | never taken | live, with the overlay |
    | write, unknown | the recorded answer, at its recorded level | faked; the replay diverges |
    | telemetry, sent to a target | not looked up, no flag | as in shadow mode |

    A write is looked up with its idempotency standing; a checked write is served only while the
    replay has not diverged. A write miss or a stale recorded answer diverges it; a read or llm
    miss does not. A recorded read is not overlaid again: its bytes already show the matched
    writes. A recorded answer keeps its `answered_by`, overlay and minted ids; only `live`
    becomes `recording` (answered-by header). Misses go to `self.shadow`, whose check
    reads come from the recording; a lookup that raises is a miss (fail-closed hook rule).
    """

    name: str = "replay"

    def __init__(
        self,
        recording: ReplayIndex,
        maps: MapIndex | None = None,
        keys: idempotency.Store | None = None,
    ) -> None:
        self.recording = recording
        self.maps = maps if maps is not None else MapIndex()
        self.idempotency = keys if keys is not None else idempotency.Store()
        self.shadow = ShadowPolicy(
            reader=RecordingReader(recording), maps=self.maps, keys=self.idempotency
        )

    def recorded_tool_call(self, name: str, args: JSONValue) -> ToolCall | None:
        """Return the recording's answer to a read tool call, or None to run the tool for real."""
        try:
            return self.recording.tool_answer_for(name, args)
        except Exception:
            logger.exception("irimi: the recording could not be searched; the tool runs for real")
            return None

    def observe_read(self, service: str, body: bytes) -> None:
        self.shadow.observe_read(service, body)

    def decide(
        self,
        request: Request,
        classification: Classification,
        write_log: Sequence[Exchange] = (),
        run_id: str = "",
        target: ForwardTo | None = None,
    ) -> Decided:
        kind = classification.kind
        try:
            if kind == "telemetry" or target is not None:
                return self.shadow.decide(request, classification, write_log, run_id, target)
            found = self._from_recording(request, classification, run_id)
        except Exception:
            logger.exception("irimi: the recording could not be searched; deciding this as a miss")
            found = None
        if found is not None:
            return found
        if kind not in LIVE_KINDS:
            self.recording.diverge()
        decided = self.shadow.decide(request, classification, write_log, run_id, target)
        return replace(decided, flags=(*decided.flags, Flag.REPLAY_MISS))

    def _from_recording(
        self, request: Request, classification: Classification, run_id: str
    ) -> Decided | None:
        """Return the recorded answer to this request, or None when the recording has none."""
        kind = classification.kind
        held = (
            ""
            if kind in LIVE_KINDS
            else policy.idempotency_standing(self.idempotency, request, classification, run_id)
        )
        recorded = self.recording.answer_for(request, kind, held)
        if recorded is None or recorded.response is None:
            return None
        answered_by: AnsweredBy = (
            "recording" if recorded.answered_by == "live" else recorded.answered_by
        )
        answer = Decided(
            Respond(
                _as_served(recorded.response),
                answered_by,
                WriteOutcome(recorded.precondition, recorded.would_fire),
                overlay=recorded.overlay if kind == "read" else None,
            ),
            flags=tuple(flag for flag in recorded.flags if flag in _ANSWER_FLAGS),
        )
        if kind not in LIVE_KINDS:
            # Kept under its idempotency key, without `from-recording`, so a later retry that
            # misses with other parameters is refused by shadow mode as the service would.
            policy.remember_answer(self.idempotency, request, classification, run_id, answer)
        return replace(answer, flags=(*answer.flags, Flag.FROM_RECORDING))


def _as_served(recorded: Response) -> Response:
    """Return the recorded response less `_RECORDED_ONLY_HEADERS`, as a replay sends it."""
    headers = tuple((k, v) for k, v in recorded.headers if k.lower() not in _RECORDED_ONLY_HEADERS)
    return Response(status=recorded.status, headers=headers, body=recorded.body)
