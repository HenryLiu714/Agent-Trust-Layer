"""ReplayPolicy: the `policy.AnswerPolicy` a replay runs under, answering from a stored run where
it can and deciding the rest as shadow mode would. What it answers from is `replay.index`.
"""

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

# The flags a recorded ANSWER carried besides its level: a fixture that did not load, and the two
# halves of idempotency. A replay that answers again carries these and `from-recording`, and no
# other: the classifier's own flags come back from this request's classification, and the
# engine's (a body cut at the cap, a lost upstream) are facts about the recorded exchange, never
# about this one.
_ANSWER_FLAGS = frozenset({Flag.FIXTURE_FAILED, Flag.IDEMPOTENT_REPLAY, Flag.IDEMPOTENCY_CONFLICT})
# Headers a recorded response carries that describe bytes it no longer has. The body was stored
# decoded and whole, so its `Content-Encoding` and framing are not the body served; mitmproxy sets
# the length of what is. And the recorded run's own stamp, which `wire.stamp` replaces.
_RECORDED_ONLY_HEADERS = frozenset(
    {"content-encoding", "content-length", "transfer-encoding", ANSWERED_BY_HEADER}
)


class ReplayPolicy:
    """Answers a run from a stored one where it can, decides the rest as shadow mode would, and
    never forwards a write.

    | request | in the recording (`from-recording`) | not in it (`replay-miss`) |
    | --- | --- | --- |
    | llm; read before the replay diverged | the recorded response | live |
    | read, after the replay diverged | never taken | live, with the overlay |
    | write, unknown | the recorded ANSWER, at its recorded level | faked; the replay diverges |
    | telemetry, delegated | not looked up, no flag | as in shadow mode |

    A write is looked up with its standing in the run's idempotency store, which this policy and
    the `ShadowPolicy` it holds share, and one L3 checked is taken only while the replay has not
    diverged, as a read is (`ReplayIndex.answer_for`).

    AN ANSWER FROM THE RECORDING IS WHAT IT WAS. It keeps the recorded `answered_by` and the
    recorded read's overlay fidelity: a write is the same fake irimi built in the recorded run,
    so `is_authored_write` puts it in the write log, the summary prints it as it did, and its minted
    ids are the recorded ones, which every later read or llm call naming one then matches without
    any mapping of ids; a read the overlay edited is still the overlay's, and its `↳` line comes
    back. One exception: `live` becomes `recording`, because irimi answered it, from disk, and
    `Irimi-Answered-By` must say so. Every such answer carries `from-recording`, which is how a
    reader of the trace tells it from the decision the run would have made itself.

    ONCE A WRITE HAS MISSED, THE REPLAY HAS DIVERGED (`ReplayIndex.diverge`), AND READS GO LIVE. A
    recorded read after that point shows the recorded write and not the new one, so from then on
    every read is answered as shadow mode answers it: live, with the overlay applying every write
    in the write log, recorded and new alike. A write L3 checked is checked again, for the same
    reason. An llm call that misses changes nothing about the world irimi shows, so it does not end
    serving reads. The engine translates a read before it asks, as it translated the one it
    recorded, and a recorded read is not overlaid again: a matched write's effect is already in
    those bytes.

    Everything it cannot answer goes to the `ShadowPolicy` it holds, whose reader is the recording
    (`RecordingReader`): a replay never checks a precondition against live state.

    THE NEVER-RAISE RULE covers matching too. An exception while looking a request up is logged, and
    the request is decided as a miss, so a write that broke the matcher is still faked and never
    forwarded.
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
        """The recording's answer to a read tool call (`ReplayIndex.tool_answer_for`). A lookup
        that raises is logged and is a miss, as a request's is: the tool runs for real."""
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
        """The recorded answer to this request, or None when the recording has none to give."""
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
            # Shadow mode keeps the answer under its idempotency key, so a retry that misses later
            # with the same key and other parameters is refused as the service would refuse it.
            # Kept without `from-recording`: shadow mode answers that retry.
            policy.remember_answer(self.idempotency, request, classification, run_id, answer)
        return replace(answer, flags=(*answer.flags, Flag.FROM_RECORDING))


def _as_served(recorded: Response) -> Response:
    """A recorded response as a replay sends it: the recorded status and body, and the recorded
    headers less the ones that describe how the body travelled (`_RECORDED_ONLY_HEADERS`)."""
    headers = tuple((k, v) for k, v in recorded.headers if k.lower() not in _RECORDED_ONLY_HEADERS)
    return Response(status=recorded.status, headers=headers, body=recorded.body)
