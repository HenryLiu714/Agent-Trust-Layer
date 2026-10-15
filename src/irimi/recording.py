"""A stored run as a replay answers from it (#82): its exchanges, keyed so that a live request can
find the one the recorded run made in its place, and the replay's own state.

A replay re-runs an agent and answers it from a run it made before. `policy.ReplayPolicy` asks
this module one question per request - did the recorded run make this same request, and what
was it answered? - and this module answers it with nothing but the recording: no clock, no I/O,
no minting. It reads `StoredRun.events` (it may not import `store`, which sits above it).

WHAT "THE SAME REQUEST" IS. Two requests match when their keys do, and the key is built to be
equal across two runs of one agent while the agent asks for the same thing:

- no headers, so an `Idempotency-Key`, a `User-Agent`, a credential or a port never decides a match;
- the body as `bodies.reflect` reads it, less the route's `volatile:` fields, as canonical JSON, so
  form encoding, key order and a re-spelled number (`1.10` is `1.1`) do not either;
- an llm call's body less the fields that say who asked and how, not what (`LLM_VOLATILE`);
- and the live request is redacted with the store's key first, because the recording was: a
  secret in a body matches its own placeholder in the recording (#69).

Each recorded exchange answers once, in the order it was recorded, so a third identical read in a
run that recorded two is a miss. The engine's own precondition reads are kept apart: they are never
served to the agent, and the agent's reads are never used for one (#82, "Decided").

THE REPLAY'S STATE IS HERE, AND ONLY HERE: which recorded exchanges have answered, and whether
the replay has diverged - made a write the recording could not answer, or reached a recorded read
that shows a recorded write the replay has not made - after which the recorded reads show a world
that is no longer the replay's. One replay is one run against one engine, whose write log is the
engine's, so both are the engine's too.
"""

import hashlib
import json
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, get_args
from urllib.parse import parse_qsl

from irimi import bodies, pipeline, redact
from irimi.exchange import (
    BODY_TRUNCATED_FLAG,
    DECISION_FAILED_FLAG,
    FIDELITY_RECORDING_FLAG,
    FROM_RECORDING_FLAG,
    LIVE_KINDS,
    REDACTION_FAILED_FLAG,
    REPLAY_MISS_FLAG,
    STREAM_TRUNCATED_FLAG,
    TARGET_FAILED_FLAG,
    UPSTREAM_ERROR_FLAG,
    AnsweredBy,
    Exchange,
    FakeLevel,
    Kind,
    Request,
    Response,
    is_authored_write,
    media_type,
)
from irimi.servicemap import MapIndex
from irimi.trace import Event

# What a match key is: a tuple of strings and tuples of strings, comparable and hashable.
_Key = tuple[Any, ...]

# The top-level fields of an llm call's body that say who asked and how the answer travels, never
# what was asked (#82). `stream` is not in the key, but whether an llm call asks to stream is beside
# it, read off the recorded call's own request: a call that asked to stream is answered from one
# that asked too, whether its answer streamed or was a JSON error, and a streamed answer is never
# served to a call that did not ask for one.
LLM_VOLATILE = frozenset({"metadata", "user", "stream", "stream_options", "prompt_cache_key"})
# A prompt-caching hint, at any depth of an llm call's body: it changes what the call costs, never
# what it asks.
CACHE_CONTROL = "cache_control"

# A recorded exchange carrying any of these holds an answer that is not the whole answer, or not
# the service's: a body cut at the cap, a stream that ended early, irimi's own refusal of a write it
# could not decide about, a target that was never reached, a lost upstream, a part redaction could
# not read. A replay never answers from one as though it were whole (#82, "Since #70/#71/#94").
UNSERVABLE_FLAGS = frozenset(
    {
        BODY_TRUNCATED_FLAG,
        STREAM_TRUNCATED_FLAG,
        DECISION_FAILED_FLAG,
        TARGET_FAILED_FLAG,
        UPSTREAM_ERROR_FLAG,
        REDACTION_FAILED_FLAG,
    }
)

# The levels a recorded write or unknown request may be answered again at: the fakes irimi built.
# A write recorded as anything else was never one irimi answered, and serving it under its recorded
# `answered_by` would send a fake with no stamp, or call it live.
_FAKE_LEVELS: frozenset[str] = frozenset(get_args(FakeLevel))


def _stream_bucket(request: Request, kind: Kind) -> bool:
    """What a request's key carries beside it: whether it is an llm call that asks to stream. Any
    other kind asks for one answer, whatever its body says."""
    return kind == "llm" and _asks_to_stream(request)


def _asks_to_stream(request: Request) -> bool:
    """Whether this request asks for a streamed answer: a JSON body whose `stream` is `true`, as
    every llm API spells it. Anything else asks for one answer."""
    document = _json_body(request)
    return document is not None and document.get("stream") is True


def _json_body(request: Request) -> dict[str, Any] | None:
    """The request's body as a JSON object, whatever its content type says, or None. Not
    `bodies.json_object`, which stops at `bodies.MAX_BODY_BYTES`: a conversation that long is still
    one llm call, and the one way to tell whether it asks to stream. Never raises."""
    try:
        document = json.loads(request.body)
    except Exception:  # not JSON, not UTF-8, or nested past the parser's depth
        return None
    return document if isinstance(document, dict) else None


def _streamed(exchange: Exchange) -> bool:
    """Whether a recorded exchange's answer streamed (#71): it has chunks, or it is an event stream
    that sent no body at all, which records no chunks like a response that did not stream."""
    if exchange.stream_chunks:
        return True
    content_type = "" if exchange.response is None else exchange.response.header("content-type")
    return media_type(content_type or "") == "text/event-stream"


def _servable(exchange: Exchange) -> bool:
    """Whether a replay may answer with this recorded exchange's response: it has one, it is the
    whole of what was answered, and it was not a target's. A delegated answer came from wherever
    the target was, which a replay is not pointed at. A write or an unknown request must have been
    one of irimi's own fakes (`_FAKE_LEVELS`).

    The response must also be one the engine can send. irimi's own writer stores only headers it
    read as latin-1, but a run from elsewhere - edited by hand, pulled from a bucket, written by
    another version - may hold any text, and a header that does not encode raises in the request
    hook, which forwards the flow (#82). And a `text/event-stream` 200 recorded with no body and no
    chunks is a stream recorded before #71 kept streamed bodies, not an empty answer."""
    response = exchange.response
    if response is None or exchange.answered_by == "delegated":
        return False
    if exchange.kind not in LIVE_KINDS and exchange.answered_by not in _FAKE_LEVELS:
        return False
    if any(flag in UNSERVABLE_FLAGS for flag in exchange.flags):
        return False
    if not 100 <= response.status <= 999:
        return False
    try:
        for name, value in response.headers:
            name.encode("latin-1")
            value.encode("latin-1")
    except UnicodeEncodeError:
        return False
    return not (response.status == 200 and not response.body and _streamed(exchange))


def _match_key(request: Request, kind: Kind, maps: MapIndex) -> _Key:
    """The key of a request AS STORED: already redacted. The recorded side calls this on a stored
    request, and `_live_key` on a live one after redacting it, so both sides are keyed alike.

    An llm call whose body is a JSON object (`_json_body`) is keyed by its host, its path and that
    object less `LLM_VOLATILE` and every `cache_control`, as canonical JSON. Everything else is
    keyed by its method, host, path, sorted query pairs and body, the body as `bodies.reflect`
    reads it minus the `volatile:` fields of the route this stored request classifies to; a body
    `reflect` cannot read is keyed by its sha256. The kind leads, so a read never matches a
    write."""
    if kind == "llm":
        document = _json_body(request)
        if document is not None:
            return (kind, request.host, request.path, _canonical(_without_llm_volatile(document)))
    route = pipeline.classify(request, maps).route
    volatile = route.volatile if route is not None else ()
    posted = bodies.reflect(request)
    if posted:
        body = _canonical({k: v for k, v in posted.items() if k not in volatile})
    elif request.body:
        body = "sha256:" + hashlib.sha256(request.body).hexdigest()
    else:
        body = ""
    query = tuple(sorted(parse_qsl(request.query, keep_blank_values=True)))
    return (kind, request.method, request.host, request.path, query, body)


def _live_key(request: Request, kind: Kind, maps: MapIndex, redaction_key: bytes) -> _Key:
    """The key of a live request: `_match_key` over the request as the store would have written
    it, redacted with the store's key. Every redaction rule is deterministic, so a secret in a live
    body becomes the placeholder its recording holds (#69)."""
    return _match_key(_redacted(request, kind, redaction_key), kind, maps)


def _redacted(request: Request, kind: Kind, redaction_key: bytes) -> Request:
    """`request` as `redact.redact_exchange` stores it. Redaction reads only the request's own
    parts for its request, so the rest of the exchange it is handed is a placeholder."""
    exchange = Exchange(
        request=request,
        response=None,
        service="",
        operation="",
        kind=kind,
        answered_by="live",
        validation="unvalidated",
        run_id="",
    )
    return redact.redact_exchange(exchange, redaction_key).request


def _without_llm_volatile(document: dict[str, Any]) -> dict[str, Any]:
    kept = {k: v for k, v in document.items() if k not in LLM_VOLATILE}
    result: dict[str, Any] = _without_cache_control(kept)
    return result


def _without_cache_control(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _without_cache_control(v) for k, v in value.items() if k != CACHE_CONTROL}
    if isinstance(value, list):
        return [_without_cache_control(v) for v in value]
    return value


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass
class _Entry:
    exchange: Exchange
    position: int  # its place among the agent's recorded exchanges
    consumed: bool = False


class Recording:
    """One stored run's exchanges, ready to answer a replay from, and that replay's state (#82).
    Thread-safe: the policy that asks it runs on the engine's worker threads."""

    def __init__(
        self,
        agent: dict[tuple[_Key, bool], list[_Entry]],
        engine: dict[_Key, Exchange],
        maps: MapIndex,
        redaction_key: bytes,
    ) -> None:
        self._agent = agent
        # The recorded writes that entered the write log (`is_authored_write`): the ones every
        # recorded read after them shows.
        self._writes = [
            entry
            for entries in agent.values()
            for entry in entries
            if is_authored_write(entry.exchange)
        ]
        self._engine = engine
        self._maps = maps
        self._redaction_key = redaction_key
        self._diverged = False
        self._lock = threading.Lock()

    @classmethod
    def from_events(
        cls, events: Sequence[Event], redaction_key: bytes, maps: MapIndex
    ) -> "Recording":
        """The recording of a stored run's `events` (`StoredRun.events`, in `seq` order), whose
        bodies the store redacted with `redaction_key`, keyed against `maps`.

        Every exchange the agent made is kept, in order, whether or not it can be served: one that
        cannot is still the agent's Nth such request, so the request in its place is a miss and the
        next one still meets its own. Telemetry is never stored as an exchange (#70), and a tool
        call is #83's. The engine's precondition reads are kept by key, the first that can be served
        of each, or else the first."""
        agent: dict[tuple[_Key, bool], list[_Entry]] = {}
        engine: dict[_Key, Exchange] = {}
        position = 0
        for event in events:
            if not isinstance(event, Exchange) or event.kind == "telemetry":
                continue
            try:
                found = _match_key(event.request, event.kind, maps)
            except Exception:  # an exchange this cannot key is one nothing can match
                continue
            if event.issued_by == "engine":
                held = engine.get(found)
                if held is None or (_servable(event) and not _servable(held)):
                    engine[found] = event
            else:
                bucket = (found, _stream_bucket(event.request, event.kind))
                agent.setdefault(bucket, []).append(_Entry(event, position))
                position += 1
        return cls(agent, engine, maps, redaction_key)

    def take(self, request: Request, kind: Kind) -> Exchange | None:
        """The recorded exchange that answers this live request, or None for a miss.

        It is the first recorded exchange of the same kind, key and stream bucket that has not
        answered yet, and it never answers again. When that one cannot be served (`_servable`), or
        is an llm call's stream and this call does not ask for a stream, the request is a miss all
        the same: the recorded run had no answer this one can be given.

        A READ RECORDED AFTER A WRITE THE REPLAY HAS NOT MADE IS A MISS, AND THE REPLAY DIVERGES. It
        shows that write - a refund in `amount_refunded`, a post in a thread - and the run being
        replayed made no such write, as when a prompt edit stops the agent refunding. Its reads
        from then on go live, as after a write that missed."""
        asks = _stream_bucket(request, kind)
        wanted = (_live_key(request, kind, self._maps, self._redaction_key), asks)
        with self._lock:
            for entry in self._agent.get(wanted, ()):
                if not entry.consumed:
                    if kind == "read" and any(
                        not write.consumed and write.position < entry.position
                        for write in self._writes
                    ):
                        self._diverged = True
                        return None
                    entry.consumed = True
                    recorded = entry.exchange
                    if not _servable(recorded) or (
                        kind == "llm" and _streamed(recorded) and not asks
                    ):
                        return None
                    return recorded
        return None

    def engine_read(self, request: Request) -> Response | None:
        """The response the recorded run's own precondition read of this request got, or None
        when it made none, or got none. Never consumed: two writes may check one charge."""
        found = self._engine.get(_live_key(request, "read", self._maps, self._redaction_key))
        if found is None or not _servable(found):
            return None
        return found.response

    def diverge(self) -> None:
        """Say the replay has made a write the recording could not answer. From then on the
        recorded reads show a world that is not the replay's, and none is served again."""
        with self._lock:
            self._diverged = True

    @property
    def diverged(self) -> bool:
        with self._lock:
            return self._diverged


class RecordingReader:
    """A `reader.Reader` over a recording: the precondition read of a write a replay could not
    answer from the recording is answered with the recorded run's own read of the same thing (#82).

    A REPLAY NEVER CHECKS A PRECONDITION AGAINST LIVE STATE. A charge the recorded run saw
    unrefunded may since have been refunded for real, and a live check would reject the write the
    recording accepted (design §5.5, "Writes"). A read the recording lacks is None, which the policy
    records as `not_evaluable`, as it does any read it could not make. Every read it makes is
    recorded `recording`, and flagged `from-recording` or `replay-miss` (`reader.answered_as`)."""

    answered_by: AnsweredBy = "recording"
    flags: tuple[str, ...] = (FIDELITY_RECORDING_FLAG, FROM_RECORDING_FLAG)
    miss_flags: tuple[str, ...] = (FIDELITY_RECORDING_FLAG, REPLAY_MISS_FLAG)

    def __init__(self, recording: Recording) -> None:
        self.recording = recording

    def __call__(self, request: Request) -> Response | None:
        return self.recording.engine_read(request)
