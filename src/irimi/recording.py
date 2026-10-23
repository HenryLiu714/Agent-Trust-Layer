"""A stored run as a replay answers from it: its exchanges, keyed so that a live request can
find the one the recorded run made in its place, and the replay's own state.

A replay re-runs an agent and answers it from a run it made before. `policy.ReplayPolicy` asks
this module one question per request - did the recorded run make this same request, and what
was it answered? - and this module answers it with nothing but the recording: no clock, no I/O,
no minting. It reads a `trace.StoredRun`'s events.

WHAT "THE SAME REQUEST" IS. Two requests match when their keys do, and the key is built to be
equal across two runs of one agent while the agent asks for the same thing:

- no headers, so an `Idempotency-Key`, a `User-Agent`, a credential or a port never decides a match,
  but the ones that say whose state a request is about (`services.scope_of`): a Connect read of
  `ch_1` in one account is not a read of `ch_1` in another;
- the body as `bodies.reflect` reads it, less the route's `compare_ignores:` fields, as canonical
  JSON, so form encoding, key order and a re-spelled number (`1.10` is `1.1`) do not either;
- an llm call's JSON body less its route's `compare_ignores:` (who asked and how, not what);
- a write's standing with the run's idempotency store: a retry of a key the run has answered, a
  key reused for another write, or neither, which the service answers three different ways
  whatever the bodies say;
- and the live request is redacted with the store's key first, because the recording was: a
  secret in a body matches its own placeholder in the recording.

Each recorded exchange answers once, in the order it was recorded, so a third identical read in a
run that recorded two is a miss. The engine's own precondition reads are kept apart: they are never
served to the agent, and the agent's reads are never used for one.

A READ TOOL THE PROXY CANNOT SEE IS A READ TOO. The SDK asks for its recorded result by the
tool's name and its captured args, redacted with the store's key as the recording's were, compared
as canonical JSON, in order. A recorded tool call answered holds the HTTP its body made inside it,
and the read tools it called, which a replay that answers the call never makes: the exchanges
recorded within the call's span are used up with it, and so are the read tool calls within it that
a later call of the same name and args comes after (`_displacing`), so none of them answers a later
request or tool call of the agent's with what the tool saw.
Under the world rule a tool is placed where its body began, its first such exchange, so a write
its own body made is not one it waits on; and a tool recorded after a write the replay has not
made is a miss that does not diverge the replay, since its body then runs, and its own HTTP is
decided as any request is. A recorded call with no outcome to give (`_tool_servable`: a result
that is a description, as a `Decimal` is captured, or a call stopped from outside) is a miss that
uses up only itself: the SDK could not use it, and the body that runs in its place is answered
what it was answered when recorded. A write tool is not kept: its stand-in runs in a replay as it
did when recorded, and with no tool overlay no recorded read shows what one did, so it
neither diverges a replay nor holds a read back.

THE REPLAY'S STATE IS HERE, AND ONLY HERE: which recorded exchanges have answered, and whether
the replay has diverged - made a write the recording could not answer, or reached a recorded answer
made from a world holding a recorded write the replay has not made - after which a recorded answer
made from the world (`_shows_the_world`) is never served again. One replay is one run against one
engine, whose write log is the engine's, so both are the engine's too.
"""

import hashlib
import threading
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, get_args
from urllib.parse import parse_qsl

from irimi import bodies, redact, services
from irimi.classify import classify
from irimi.exchange import (
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
from irimi.reader import Reader
from irimi.servicemap import MapIndex, Route
from irimi.trace import (
    STOPPED_ERROR_TYPES,
    Event,
    JSONValue,
    ToolCall,
    canonical_json,
    holds_description,
)

# What a match key is: a tuple of strings and tuples of strings, comparable and hashable.
_Key = tuple[Any, ...]
# What a read tool call is matched by: its name, and its args as canonical JSON.
_ToolKey = tuple[str, str]
# Where a recorded exchange waits to be taken: its key, whether it is an llm call that asked to
# stream (`_stream_bucket`), and the idempotency flag its answer carried (`_held`).
_Bucket = tuple[_Key, bool, str]

# A prompt-caching hint, at any depth of an llm call's body: it changes what the call costs, never
# what it asks.
CACHE_CONTROL = "cache_control"

# A recorded exchange carrying any of these holds an answer that is not the whole answer, or not
# the service's: a body cut at the cap, a stream that ended early, irimi's own refusal of a write it
# could not decide about, a target that was never reached, a lost upstream, a part redaction could
# not read. A replay never answers from one as though it were whole.
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

# The levels a recorded write or unknown request may be answered again at: the fakes irimi built.
# A write recorded as anything else was never one irimi answered, and serving it under its recorded
# `answered_by` would send a fake with no stamp, or call it live.
_FAKE_LEVELS: frozenset[str] = frozenset(get_args(FakeLevel))


def _stream_bucket(request: Request, kind: Kind) -> bool:
    """What a request's key carries beside it: whether it is an llm call that asks to stream. Any
    other kind asks for one answer, whatever its body says.

    The LLM maps leave `stream` out of the compared body, so this is what keeps a call that asked
    to stream answered from one that asked too, and a streamed answer away from one that did not.
    """
    return kind == "llm" and _asks_to_stream(request)


def _asks_to_stream(request: Request) -> bool:
    """Whether this request asks for a streamed answer: a JSON body whose `stream` is `true`, as
    every llm API spells it. Anything else asks for one answer."""
    document = _json_body(request)
    return document is not None and document.get("stream") is True


def _json_body(request: Request) -> dict[str, Any] | None:
    """The request's body as a JSON object, whatever its content type says, or None. Of any
    size: a conversation past `limits.MAX_BODY_BYTES` is still one llm call, and the one way to
    tell whether it asks to stream. Never raises."""
    return bodies.json_object(request.body, max_bytes=None)


def _held(exchange: Exchange) -> str:
    """The idempotency flag a recorded answer carried: `idempotent-replay` when it was the run's
    first answer to its key again, `idempotency-conflict` when the key had come back on another
    write, and "" for an answer to a key the run had not answered, or no key.

    It is beside the key because the service answers those three differently whatever the body
    says, and the key leaves the idempotency key out: a retry must not take the recorded answer to
    a second, distinct write (and enter the write log as one), a fresh write must not take a
    recorded retry's (and stay out of it), and a key reused for another write is refused."""
    return next((flag for flag in IDEMPOTENCY_FLAGS if flag in exchange.flags), "")


def _shows_the_world(exchange: Exchange) -> bool:
    """Whether a recorded answer was made from the world the recorded run's writes had made: a
    read, which shows them, or a write L3 checked against them, whose verdict a refund the
    replay has not made, or one the recording never held, would change. A replay serves one only
    while that world is the replay's too (`Recording.take`)."""
    return exchange.kind == "read" or (
        exchange.kind not in LIVE_KINDS and exchange.precondition.outcome is not None
    )


def _streamed(exchange: Exchange) -> bool:
    """Whether a recorded exchange's answer streamed: it has chunks, or it is an event stream
    that sent no body at all, which records no chunks like a response that did not stream."""
    if exchange.stream_chunks:
        return True
    content_type = "" if exchange.response is None else exchange.response.header("content-type")
    return media_type(content_type or "") == "text/event-stream"


def _servable(exchange: Exchange) -> bool:
    """Whether a replay may answer with this recorded exchange's response: it has one, it is the
    whole of what was answered, and it was not a target's or irimi's own error. A delegated answer
    came from wherever the target was, which a replay is not pointed at. A write or an unknown
    request must have been one of irimi's own fakes (`_FAKE_LEVELS`).

    The response must also be one the engine can send. irimi's own writer stores only headers it
    read as latin-1, but a run from elsewhere - edited by hand, pulled from a bucket, written by
    another version - may hold any text, and a header that does not encode raises in the request
    hook, which forwards the flow. Its status must be a final one HTTP defines, 200 to 599:
    a 1xx is never a whole answer, and a 101 switched the recorded connection to a protocol a
    replay cannot carry on."""
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
    """The key of a request AS STORED: already redacted. The recorded side calls this on a stored
    request, and `_live_key` on a live one after redacting it, so both sides are keyed alike.

    Every request is keyed by its kind, method, host, path, query pairs sorted by name and the
    headers that scope its service's state (`services.scope_of`, which the write log, the
    idempotency store and L3 compare too): a query can say what an llm call asks as much as a read's
    (`?alt=sse`, `?api-version=`), and a `Stripe-Account` says whose objects a request names.
    An llm call whose body is a JSON object (`_json_body`) adds that object less its route's
    `compare_ignores:` and every `cache_control`, as canonical JSON. Everything else adds its body
    as `bodies.reflect` reads it minus the `compare_ignores:` fields of the route this stored
    request classifies to; a body
    `reflect` cannot read is keyed by its sha256. The kind leads, so a read never matches a
    write."""
    classification = classify(request, maps)
    # Sorted by name only, and stably: names in any order, but a repeated name's values in theirs,
    # since `sort=created&sort=amount` asks for another order than the reverse.
    pairs = parse_qsl(request.query, keep_blank_values=True)
    query = tuple(sorted(pairs, key=lambda pair: pair[0]))
    scope = services.scope_of(classification.service, request)
    head = (kind, request.method, request.host, request.path, query, scope)
    body = compared_body(request, kind, classification.route)
    return (*head, body if isinstance(body, str) else canonical_json(body))


def compared_body(request: Request, kind: Kind, route: Route | None) -> JSONValue:
    """What of a request's body says what it asks, as a replay matches it and `irimi compare`
    compares it: an llm call's JSON object less `route`'s `compare_ignores:` and every
    `cache_control`; any other body as `bodies.reflect` reads it, less `route`'s
    `compare_ignores:`; the sha256 of a body neither reads, as `sha256:<hex>`; and "" for no
    body."""
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
    """The key of a live request: `_match_key` over the request as the store would have written
    it, redacted with the store's key (`redact.redact_request`). Every redaction rule is
    deterministic, so a secret in a live body becomes the placeholder its recording holds."""
    return _match_key(redact.redact_request(request, redaction_key), kind, maps)


def _without_cache_control(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _without_cache_control(v) for k, v in value.items() if k != CACHE_CONTROL}
    if isinstance(value, list):
        return [_without_cache_control(v) for v in value]
    return value


def _tool_key(name: str, args: JSONValue) -> _ToolKey:
    """A read tool call's key: its name and its args, already redacted, as canonical JSON, so key
    order never decides a match and `true` is never `1`."""
    return (name, canonical_json(args))


def _tool_servable(call: ToolCall) -> bool:
    """Whether a recorded read tool call's outcome may be handed back. Not when the store could
    not redact it: a result stored as `redact.REDACTION_FAILED` would revive as the tool's answer,
    and an error message as that string would be raised as the recorded error's. Not when it
    is no outcome to give: a result holding a description of a value (`trace.holds_description`)
    is not the value, and a call something outside it stopped (`trace.STOPPED_ERROR_TYPES`)
    recorded none of its own. Each of those the SDK would decline, after the call had used up
    what its body made, which its body then runs again and needs. Whether any other result can be
    revived is the SDK's to say."""
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
    # Where its body began, in the same count as `_Entry.position`: its first inner exchange's,
    # or its own when it made none.
    position: int
    # The agent's exchanges recorded within the call's span: its body's HTTP.
    inner: list[_Entry]
    # The read tool calls recorded within the call's span that a later call repeats: the tools its
    # body called, which would answer that later call in its place (`_displacing`).
    nested: list["_ToolEntry"] = field(default_factory=list)
    consumed: bool = False


def made_within(started_at: float, ended_at: float, call: ToolCall) -> bool:
    """Whether an exchange or a tool call made over `started_at`..`ended_at` was made within
    `call`'s span, which is how a stored run says it was the call's body's: a tool call records no
    link to its HTTP or to the tools it called. One another task made in the span is taken
    for the call's too, and so used up with it: in the replay it is then a miss, decided afresh,
    never an answer from the wrong world. One with no clock (`started_at` 0.0, built outside the
    engine and the SDK) is within no call. `irimi replay`'s count of the calls a replay never made
    reads the same rule."""
    return 0.0 < started_at and call.started_at <= started_at and ended_at <= call.ended_at


def _nested_in(other: ToolCall, call: ToolCall) -> bool:
    """Whether the read tool call `other` was made within `call`'s span (`made_within`). Never one
    with the same span, so two calls clocked alike never use each other up."""
    span = (other.started_at, other.ended_at)
    return span != (call.started_at, call.ended_at) and made_within(*span, call)


def _displacing(
    within: list[tuple[_ToolKey, _ToolEntry]], tools: dict[_ToolKey, list[_ToolEntry]]
) -> list[_ToolEntry]:
    """The read tool calls `within` a call's span that answering the call uses up: each one that
    a later recorded call of its name and args, outside that span, comes after, so that it would
    answer that later call in its place.

    A span does not say whose a call was. One within it may be the body's, never made once the
    call is answered, or another task's, made at the same time: two tools gathered, or a sync
    tool's coroutine left pending while the agent awaits another. Used up, the other task's would
    be a miss whenever its lookup came second, so a race would decide it. So only one that would
    answer a later call wrongly is used up, and any other is left to answer its own call. One of
    another task's that a later call repeats is still used up, and that task's call then takes the
    later answer: no stored run says whose it was (the follow-up that records the link)."""
    inside = {id(entry) for _, entry in within}
    found = []
    for tool_key, entry in within:
        same = tools[tool_key]
        after = next(i for i, other in enumerate(same) if other is entry) + 1
        if any(id(other) not in inside for other in same[after:]):
            found.append(entry)
    return found


class Recording:
    """One stored run's exchanges, ready to answer a replay from, and that replay's state.
    Thread-safe: the policy that asks it runs on the engine's worker threads."""

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
        next one still meets its own. Telemetry is never stored as an exchange. Every read
        tool call is kept the same way, by name and args, counted in the same order as the
        exchanges so the world rule can compare them; a write tool call is not (module doc).
        The engine's precondition reads are kept by key, the first that can be served of each, or
        else the first."""
        agent: dict[_Bucket, list[_Entry]] = {}
        engine: dict[_Key, Exchange] = {}
        calls: list[tuple[_ToolKey, ToolCall, int]] = []
        position = 0
        for event in events:
            if isinstance(event, ToolCall) and event.kind == "read":
                try:
                    calls.append((_tool_key(event.name, event.args), event, position))
                except Exception:  # args this cannot key are args nothing can match
                    continue
                position += 1
                continue
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
                bucket = (found, _stream_bucket(event.request, event.kind), _held(event))
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
            within = [(key, other) for key, other in kept if _nested_in(other.call, entry.call)]
            entry.nested = _displacing(within, tools)
        return cls(agent, tools, engine, maps, redaction_key)

    def take(self, request: Request, kind: Kind, held: str = "") -> Exchange | None:
        """The recorded exchange that answers this live request, or None for a miss. `held` is the
        idempotency flag the run's own answer to this request would carry (`_held`), which the
        caller reads off the run's idempotency store; "" for anything that is not a write.

        It is the first recorded exchange of the same kind, key, stream bucket and `held` that has
        not answered yet, and it never answers again. When that one cannot be served
        (`_servable`), or is an llm call's stream and this call does not ask for a stream, the
        request is a miss all the same: the recorded run had no answer this one can be given.

        AN ANSWER MADE FROM A WORLD THE REPLAY IS NOT IN IS A MISS, AND THE REPLAY DIVERGES
        (`_shows_the_world`): a read, or a write L3 checked, recorded after a recorded write the
        replay has not made, or reached once the replay has diverged. It shows that write - a
        refund in `amount_refunded`, a post in a thread - or was refused because of it, and the run
        being replayed made no such write, as when a prompt edit stops the agent refunding; or the
        replay made a write the recorded run did not. It is not consumed, and from then on reads go
        live and checked writes are checked again, against the recording's own reads and the
        replay's write log. The check and the take are one step under the lock, so a write that
        diverges on another thread cannot slip between them."""
        asks = _stream_bucket(request, kind)
        wanted = (_live_key(request, kind, self._maps, self._redaction_key), asks, held)
        with self._lock:
            for entry in self._agent.get(wanted, ()):
                if not entry.consumed:
                    if _shows_the_world(entry.exchange) and self._stale(entry.position):
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

    def take_tool_call(self, name: str, args: JSONValue) -> ToolCall | None:
        """The recorded read tool call that answers this live one, or None for a miss. `args`
        are the call's as the SDK captured them, not yet redacted.

        It is the first recorded read tool call of the same name whose args equal `args` redacted
        with the store's key, as the recording's were, that has not answered yet, and it never
        answers again. Args redaction could not read (`redact.REDACTION_FAILED`) match nothing,
        and one with no outcome to give - one redaction could not read, a result that is a
        description, a call stopped from outside - is a miss in its place (`_tool_servable`),
        which uses up nothing else. The exchanges recorded within an answered call, and the read
        tool calls within it that a later call repeats, are used up with it (module doc).

        A READ TOOL SHOWS THE WORLD, as an HTTP read does (`take`): its result may have been made
        from what it read over HTTP, which showed every recorded write before it. One recorded
        after a write the replay has not made, or reached once the replay has diverged, is a miss,
        and is not used up. Unlike a read's, it does not diverge the replay: its body runs, and
        what the body reads over HTTP is decided as every request is, by the policy."""
        redacted = redact.redact_json(args, self._redaction_key)
        if redacted == redact.REDACTION_FAILED:
            return None
        try:
            wanted = _tool_key(name, redacted)
        except Exception:  # args no recording holds: `from_events` skips what it cannot key
            return None
        with self._lock:
            for entry in self._tools.get(wanted, ()):
                if not entry.consumed:
                    if self._stale(entry.position):
                        return None
                    entry.consumed = True
                    if not _tool_servable(entry.call):
                        # A miss in its place: its body runs, and makes the requests and tool
                        # calls it was recorded making, which are still there to answer them.
                        return None
                    for made in entry.inner:
                        made.consumed = True
                    for nested in entry.nested:
                        nested.consumed = True
                    return entry.call
        return None

    def _stale(self, position: int) -> bool:
        """Whether a recorded answer at `position` that shows the world shows one the replay is not
        in: the replay has diverged, or a recorded write before it has not been made. Called under
        the lock, by the step that would consume that answer."""
        return self._diverged or any(
            not write.consumed and write.position < position for write in self._writes
        )

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


class RecordingReader(Reader):
    """A `reader.Reader` over a recording: the precondition read of a write a replay could not
    answer from the recording is answered with the recorded run's own read of the same thing.

    A REPLAY NEVER CHECKS A PRECONDITION AGAINST LIVE STATE. A charge the recorded run saw
    unrefunded may since have been refunded for real, and a live check would reject the write the
    recording accepted. A read the recording lacks is None, which the policy
    records as `not_evaluable`, as it does any read it could not make. Every read it makes is
    recorded `recording`, and flagged `from-recording` or `replay-miss` (`reader.answered_as`)."""

    answered_by: AnsweredBy = "recording"
    flags: tuple[Flag, ...] = (Flag.FROM_RECORDING,)
    miss_flags: tuple[Flag, ...] = (Flag.REPLAY_MISS,)

    def __init__(self, recording: Recording) -> None:
        self.recording = recording

    def __call__(self, request: Request) -> Response | None:
        return self.recording.engine_read(request)
