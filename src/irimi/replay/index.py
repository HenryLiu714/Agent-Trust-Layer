"""Match a replay's live requests to a stored run's exchanges, and hold the replay's state.

- Requests match on kind, method, host, path, sorted query, scope headers, compared body and
  idempotency standing; the live request is redacted with the store's key first.
- Each recorded exchange answers once, in recorded order; read tool calls match by name and args.
- A stale answer is a recorded answer that depended on writes the replay has not made. It is
  never served, and reaching one in `answer_for` diverges the replay.
"""

import hashlib
import threading
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, get_args
from urllib.parse import parse_qsl

from irimi import services
from irimi.core import bodies
from irimi.core.exchange import (
    IDEMPOTENCY_FLAGS,
    LIVE_KINDS,
    AnsweredBy,
    Exchange,
    FakeLevel,
    Flag,
    Kind,
    Request,
    Response,
    is_authored_write,
    media_type,
)
from irimi.decide.reader import Reader
from irimi.protocol.trace import (
    STOPPED_ERROR_TYPES,
    Event,
    JSONValue,
    ToolCall,
    canonical_json,
    holds_description,
)
from irimi.record import redact
from irimi.servicemaps import MapIndex, Route
from irimi.servicemaps.classify import classify

_Key = tuple[Any, ...]
_ToolKey = tuple[str, str]
# A recorded exchange's match key, stream bucket and idempotency flag.
_Bucket = tuple[_Key, bool, str]

# A prompt-caching hint: it changes what an llm call costs, never what it asks.
CACHE_CONTROL = "cache_control"

# A recorded answer with any of these flags is partial or not the service's own.
UNSERVABLE_FLAGS = frozenset(
    {
        Flag.BODY_TRUNCATED,
        Flag.STREAM_TRUNCATED,
        Flag.DECISION_FAILED,
        Flag.TARGET_FAILED,
        Flag.UPSTREAM_ERROR,
        Flag.REDACTION_FAILED,
    }
)

# A recorded write or unknown request is served again only if irimi faked it.
_FAKE_LEVELS: frozenset[str] = frozenset(get_args(FakeLevel))


def _stream_bucket(request: Request, kind: Kind) -> bool:
    """Return True for an llm call asking to stream (the maps leave `stream` out of the key)."""
    return kind == "llm" and _asks_to_stream(request)


def _asks_to_stream(request: Request) -> bool:
    """Return True if the request's JSON body has `stream: true`."""
    document = _json_body(request)
    return document is not None and document.get("stream") is True


def _json_body(request: Request) -> dict[str, Any] | None:
    """Return the body as a JSON object of any size, whatever its content type, or None."""
    return bodies.json_object(request.body, max_bytes=None)


def _idempotency_standing(exchange: Exchange) -> str:
    """Return the idempotency flag a recorded answer carried, or "" for none."""
    return next((flag for flag in IDEMPOTENCY_FLAGS if flag in exchange.flags), "")


def _depends_on_prior_writes(exchange: Exchange) -> bool:
    """Return True if a recorded answer depended on earlier writes: a read, or a checked write."""
    return exchange.kind == "read" or (
        exchange.kind not in LIVE_KINDS and exchange.precondition.outcome is not None
    )


def _streamed(exchange: Exchange) -> bool:
    """Return True if a recorded answer streamed: it has chunks, or is an empty event stream."""
    if exchange.stream_chunks:
        return True
    content_type = "" if exchange.response is None else exchange.response.header("content-type")
    return media_type(content_type or "") == "text/event-stream"


def _servable(exchange: Exchange) -> bool:
    """Return True if a replay may answer with this recorded exchange's response.

    It must be whole, not a target's or irimi's own, a fake if a write, status 200-599, and with
    latin-1 headers: a header that does not encode raises in the request hook."""
    response = exchange.response
    if response is None or exchange.answered_by in ("delegated", "irimi"):
        return False
    if exchange.kind not in LIVE_KINDS and exchange.answered_by not in _FAKE_LEVELS:
        return False
    if any(flag in UNSERVABLE_FLAGS for flag in exchange.flags):
        return False
    if not 200 <= response.status <= 599:
        return False
    try:
        for name, value in response.headers:
            name.encode("latin-1")
            value.encode("latin-1")
    except UnicodeEncodeError:
        return False
    return True


def _match_key(request: Request, kind: Kind, maps: MapIndex) -> _Key:
    """Return the match key of a stored, already redacted request; a read never matches a write."""
    classification = classify(request, maps)
    # Sort stably by name only: `sort=created&sort=amount` asks for another order than the reverse.
    pairs = parse_qsl(request.query, keep_blank_values=True)
    query = tuple(sorted(pairs, key=lambda pair: pair[0]))
    scope = services.scope_of(classification.service, request)
    head = (kind, request.method, request.host, request.path, query, scope)
    body = compared_body(request, kind, classification.route)
    return (*head, body if isinstance(body, str) else canonical_json(body))


def compared_body(request: Request, kind: Kind, route: Route | None) -> JSONValue:
    """Return the part of a request's body that replay matching and `irimi compare` compare.

    An llm call's JSON, or `bodies.reflect`'s reading, less `compare_ignores:` (and, for llm,
    `cache_control`); `sha256:<hex>` for a body neither reads; "" for no body."""
    ignored = route.compare_ignores if route is not None else ()
    if kind == "llm":
        document = _json_body(request)
        if document is not None:
            kept = {k: v for k, v in document.items() if k not in ignored}
            result: dict[str, Any] = _without_cache_control(kept)
            return result
    posted = bodies.reflect(request)
    if posted:
        return {k: v for k, v in posted.items() if k not in ignored}
    if request.body:
        return "sha256:" + hashlib.sha256(request.body).hexdigest()
    return ""


def _live_key(request: Request, kind: Kind, maps: MapIndex, redaction_key: bytes) -> _Key:
    """Return a live request's match key, redacted with the store's key as its recording was."""
    return _match_key(redact.redact_request(request, redaction_key), kind, maps)


def _without_cache_control(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _without_cache_control(v) for k, v in value.items() if k != CACHE_CONTROL}
    if isinstance(value, list):
        return [_without_cache_control(v) for v in value]
    return value


def _tool_key(name: str, args: JSONValue) -> _ToolKey:
    """Return a read tool call's key: its name and its redacted args as canonical JSON."""
    return (name, canonical_json(args))


def _tool_servable(call: ToolCall) -> bool:
    """Return True if a recorded read tool call has an outcome the SDK can hand back."""
    if call.error is not None:
        return (
            call.error.message != redact.REDACTION_FAILED
            and call.error.type not in STOPPED_ERROR_TYPES
        )
    return call.result != redact.REDACTION_FAILED and not holds_description(call.result)


@dataclass
class _Entry:
    exchange: Exchange
    position: int  # its place among the agent's recorded exchanges and read tool calls
    consumed: bool = False


@dataclass
class _ToolEntry:
    call: ToolCall
    position: int  # where its body began: its first inner exchange's position, or its own
    inner: list[_Entry]  # the agent's exchanges recorded within its span
    nested: list["_ToolEntry"] = field(default_factory=list)  # see `_displacing`
    consumed: bool = False


def made_within(started_at: float, ended_at: float, call: ToolCall) -> bool:
    """Return True if something made over `started_at`..`ended_at` falls within `call`'s span.

    Runs record no link from a tool call to its requests, so the span decides, catching other
    tasks' requests too. A `started_at` of 0.0 (no clock) is within no call."""
    return started_at > 0.0 and call.started_at <= started_at and ended_at <= call.ended_at


def nested_in(other: ToolCall, call: ToolCall) -> bool:
    """Return True if `other` is `made_within` `call`'s span; calls clocked alike never nest."""
    span = (other.started_at, other.ended_at)
    return span != (call.started_at, call.ended_at) and made_within(*span, call)


def _displacing(
    within: list[tuple[_ToolKey, _ToolEntry]], tools: dict[_ToolKey, list[_ToolEntry]]
) -> list[_ToolEntry]:
    """Return the read tool calls `within` a call's span that answering the call uses up.

    Only those a later same-key call outside the span follows; the rest may be another task's."""
    inside = {id(entry) for _, entry in within}
    found = []
    for tool_key, entry in within:
        same = tools[tool_key]
        after = next(i for i, other in enumerate(same) if other is entry) + 1
        if any(id(other) not in inside for other in same[after:]):
            found.append(entry)
    return found


class ReplayIndex:
    """One stored run's exchanges and the state of the replay answering from them. Thread-safe."""

    def __init__(
        self,
        agent: dict[_Bucket, list[_Entry]],
        tools: dict[_ToolKey, list[_ToolEntry]],
        engine: dict[_Key, Exchange],
        maps: MapIndex,
        redaction_key: bytes,
    ) -> None:
        self._agent = agent
        self._tools = tools
        # The recorded faked writes (`is_authored_write`): every recorded read after one shows it.
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
    ) -> "ReplayIndex":
        """Build the index from a stored run's `events`, in `seq` order and redacted with the key.

        Agent exchanges are kept servable or not, so an unservable one's place is a miss.
        Telemetry and write tool calls are skipped; per key, the first servable check read wins."""
        agent: dict[_Bucket, list[_Entry]] = {}
        engine: dict[_Key, Exchange] = {}
        calls: list[tuple[_ToolKey, ToolCall, int]] = []
        position = 0
        for event in events:
            if isinstance(event, ToolCall) and event.kind == "read":
                try:
                    calls.append((_tool_key(event.name, event.args), event, position))
                except Exception:  # args this cannot key match nothing
                    continue
                position += 1
                continue
            if not isinstance(event, Exchange) or event.kind == "telemetry":
                continue
            try:
                found = _match_key(event.request, event.kind, maps)
            except Exception:  # an exchange this cannot key matches nothing
                continue
            if event.issued_by == "engine":
                held = engine.get(found)
                if held is None or (_servable(event) and not _servable(held)):
                    engine[found] = event
            else:
                bucket = (
                    found,
                    _stream_bucket(event.request, event.kind),
                    _idempotency_standing(event),
                )
                agent.setdefault(bucket, []).append(_Entry(event, position))
                position += 1
        made = [entry for entries in agent.values() for entry in entries]
        kept: list[tuple[_ToolKey, _ToolEntry]] = []
        for tool_key, call, at in calls:
            inner = [
                e for e in made if made_within(e.exchange.started_at, e.exchange.ended_at, call)
            ]
            kept.append(
                (tool_key, _ToolEntry(call, min([at, *(e.position for e in inner)]), inner))
            )
        tools: dict[_ToolKey, list[_ToolEntry]] = {}
        for tool_key, entry in kept:
            tools.setdefault(tool_key, []).append(entry)
        for _, entry in kept:
            within = [(key, other) for key, other in kept if nested_in(other.call, entry.call)]
            entry.nested = _displacing(within, tools)
        return cls(agent, tools, engine, maps, redaction_key)

    def answer_for(self, request: Request, kind: Kind, held: str = "") -> Exchange | None:
        """Return the recorded exchange that answers this live request, or None for a miss.

        `held` is a write's idempotency flag (`_idempotency_standing`), else "". The first unused
        match answers, once; one unservable, or streamed when not asked, is still a miss. A stale
        match is a miss, is not used up, and diverges the replay."""
        asks = _stream_bucket(request, kind)
        wanted = (_live_key(request, kind, self._maps, self._redaction_key), asks, held)
        with self._lock:
            for entry in self._agent.get(wanted, ()):
                if not entry.consumed:
                    if _depends_on_prior_writes(entry.exchange) and self._stale(entry.position):
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

    def tool_answer_for(self, name: str, args: JSONValue) -> ToolCall | None:
        """Return the recorded read tool call that answers this live one, or None for a miss.

        `args` are unredacted. An unservable match is a miss using up only itself; an answered
        one also uses up what was recorded in its span. A stale match is a miss, is not used up,
        and does not diverge the replay: the tool's body runs instead."""
        redacted = redact.redact_json(args, self._redaction_key)
        if redacted == redact.REDACTION_FAILED:
            return None
        try:
            wanted = _tool_key(name, redacted)
        except Exception:  # `from_events` skips args it cannot key, so no recording holds these
            return None
        with self._lock:
            for entry in self._tools.get(wanted, ()):
                if not entry.consumed:
                    if self._stale(entry.position):
                        return None
                    entry.consumed = True
                    if not _tool_servable(entry.call):
                        # Its body runs and makes its recorded requests, left unused to answer them.
                        return None
                    for made in entry.inner:
                        made.consumed = True
                    for nested in entry.nested:
                        nested.consumed = True
                    return entry.call
        return None

    def _stale(self, position: int) -> bool:
        """Return True if diverged or a write before `position` is unused; call under the lock."""
        return self._diverged or any(
            not write.consumed and write.position < position for write in self._writes
        )

    def engine_read(self, request: Request) -> Response | None:
        """Return the recorded check read's response to this request, or None. Never consumed."""
        found = self._engine.get(_live_key(request, "read", self._maps, self._redaction_key))
        if found is None or not _servable(found):
            return None
        return found.response

    def diverge(self) -> None:
        """Mark the replay diverged: no answer depending on prior writes is served after this."""
        with self._lock:
            self._diverged = True

    @property
    def diverged(self) -> bool:
        with self._lock:
            return self._diverged


class RecordingReader(Reader):
    """A `reader.Reader` that answers irimi's check reads from the recorded run's own reads.

    Never live: a charge refunded since the recording would reject a write it accepted. A read
    the recording lacks is None (`not_evaluable`)."""

    answered_by: AnsweredBy = "recording"
    flags: tuple[Flag, ...] = (Flag.FROM_RECORDING,)
    miss_flags: tuple[Flag, ...] = (Flag.REPLAY_MISS,)

    def __init__(self, recording: ReplayIndex) -> None:
        self.recording = recording

    def __call__(self, request: Request) -> Response | None:
        return self.recording.engine_read(request)
