"""The control endpoint, `/_irimi/` on irimi's own listener, against a real engine and a real
`DirectoryStore`: every request goes over a socket to the listener, and every claim about the run
is read back off disk with `StoreReader`.
"""

import dataclasses
import http.client
import json
import logging
import socket
import time
from collections.abc import Callable

import pytest

from irimi import __version__
from irimi.core.exchange import Exchange, Request
from irimi.core.limits import MAX_CONTROL_BODY, MAX_ERROR_MESSAGE, MAX_ERROR_TYPE
from irimi.decide.policy import ShadowPolicy
from irimi.engine import Serve
from irimi.engine.control import MAX_REFUSAL, TOO_DEEP, ControlEndpoint
from irimi.protocol import control as protocol_control
from irimi.protocol.trace import MAX_QUOTED, ErrorInfo, JSONValue, ToolCall, Trigger
from irimi.record import redact
from irimi.record.store.reader import StoreReader
from irimi.record.store.writer import DirectoryStore
from irimi.replay.index import ReplayIndex
from irimi.replay.policy import ReplayPolicy
from tests.support.doubles import NullStore
from tests.support.engine import engine_config, start_engine, via_proxy
from tests.support.waiting import eventually

RUN = "sdkrun01"
# The engine's own run, `EngineConfig.run_id` in `engine_config`.
PROCESS_RUN = "t3st"
OWN_RUN = "is irimi's own run, which the SDK may not start or end"
TRIGGER = {
    "name": "handle_ticket",
    "entrypoint": "agent:handle_ticket",
    "args": {"ticket": 7},
    "replayable": True,
}
STAMP = ("irimi-answered-by", "control")
# The body of a control request irimi failed on itself.
INTERNAL = {"error": {"type": "irimi_internal_error", "message": "internal"}}


def _refused(message: str) -> dict:
    """The body of a control request the endpoint refused."""
    return {"error": {"type": "irimi_control_refused", "message": message}}


def _tool_call(started_at: float, name: str = "db.write") -> dict:
    """A `trace.ToolCall` as the SDK posts it: everything but `run_id`, which the path supplies."""
    return {
        "tool_call_id": f"tc-{name}",
        "name": name,
        "kind": "write",
        "ran": "shadow",
        "args": {"row": 1},
        "result": {"ok": True},
        "error": None,
        "started_at": started_at,
        "ended_at": started_at + 0.001,
        "awaited": False,
    }


def _start_body(started_at: float) -> dict:
    return {
        "trigger": TRIGGER,
        "agent_version": "agent-1.2",
        "sdk_version": "0.1.0",
        "started_at": started_at,
    }


def _end_body(ended_at: float) -> dict:
    return {"ended_at": ended_at, "outcome": "ok", "error": None}


@dataclasses.dataclass
class _Engine:
    port: int
    store: DirectoryStore
    seen: list[Exchange]  # what the engine handed `on_exchange`
    tool_calls: list[ToolCall]  # what it handed `on_tool_call`
    stop: Callable[[], object]

    @property
    def root(self):
        return self.store.layout.root

    def call(
        self,
        method: str,
        path: str,
        body: bytes | dict | None = None,
        headers: dict[str, str] | None = None,
    ):
        """One origin-form request to the listener itself, as the SDK makes it when `IRIMI_CONTROL`
        is on NO_PROXY. Returns `(status, headers, body)`, the header names lower-cased."""
        data = json.dumps(body).encode() if isinstance(body, dict) else body
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        sent = {"host": f"127.0.0.1:{self.port}", **(headers or {})}
        conn.request(method, path, body=data, headers=sent)
        resp = conn.getresponse()
        raw = resp.read()
        conn.close()
        return resp.status, [(k.lower(), v) for k, v in resp.getheaders()], raw

    def finish(self):
        """Stop the engine, which closes the store, and read the store back."""
        self.stop()
        return StoreReader(self.root)


@pytest.fixture
def engine(tmp_path, monkeypatch):
    cfg = engine_config(tmp_path, monkeypatch)
    store = DirectoryStore(tmp_path / "store", redact.load_key(tmp_path))
    tool_calls: list[ToolCall] = []
    eng, seen, stop = start_engine(cfg, store=store, on_tool_call=tool_calls.append)
    stopped = False

    def stop_once():
        nonlocal stopped
        if not stopped:
            stopped = True
            stop()

    yield _Engine(eng.listen_port(), store, seen, tool_calls, stop_once)
    stop_once()


def _ok(response):
    """A 204 with the control stamp and nothing else to say."""
    status, headers, body = response
    assert (status, body) == (204, b"")
    assert STAMP in headers


def test_a_start_a_tool_call_and_an_end_are_one_sdk_run_on_disk(engine):
    t0 = time.time()
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/start", _start_body(t0)))
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/tool-calls", _tool_call(t0 + 1)))
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/end", _end_body(t0 + 2)))
    run = engine.finish().load_run(RUN)
    record = run.record
    assert (record.attribution, record.mode, record.end.outcome) == ("sdk", "shadow", "ok")
    assert record.trigger == Trigger("handle_ticket", "agent:handle_ticket", {"ticket": 7}, True)
    assert (record.agent_version, record.sdk_version, record.engine_version) == (
        "agent-1.2",
        "0.1.0",
        __version__,
    )
    assert (record.started_at, record.end.ended_at, record.end.error) == (t0, t0 + 2, None)
    [call] = run.events
    assert call == ToolCall(
        tool_call_id="tc-db.write",
        run_id=RUN,
        name="db.write",
        kind="write",
        ran="shadow",
        args={"row": 1},
        result={"ok": True},
        error=None,
        started_at=t0 + 1,
        ended_at=t0 + 1.001,
    )
    # `on_tool_call` saw the call the store accepted, and no control request was an exchange.
    assert engine.tool_calls == [call]
    assert engine.seen == []


def test_an_exchange_labelled_with_the_run_lands_between_its_tool_calls(engine, upstream):
    """The proxy is the only writer, so it orders the SDK's events and the wire's in one file: an
    exchange sent between two tool calls is stored between them."""
    t0 = time.time()
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/start", _start_body(t0)))
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/tool-calls", _tool_call(time.time(), "before")))
    status, _ = via_proxy(
        engine.port,
        "GET",
        f"http://127.0.0.1:{upstream}/hello",
        extra_headers={"Irimi-Run": RUN},
    )
    assert status == 200
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/tool-calls", _tool_call(time.time(), "after")))
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/end", _end_body(time.time())))
    run = engine.finish().load_run(RUN)
    before, exchange, after = run.events
    assert (before.name, after.name) == ("before", "after")
    assert isinstance(exchange, Exchange) and exchange.request.path == "/hello"
    assert run.record.attribution == "sdk"
    assert run.record.started_at <= exchange.started_at <= exchange.ended_at
    assert exchange.ended_at <= run.record.end.ended_at
    # Only the exchange was reported as one.
    assert [ex.request.path for ex in engine.seen] == ["/hello"]


def test_health_reports_the_engine_serve_mode_and_the_stores_counters(engine):
    """The counters are the store's own `stats()`: once the writer has written the one tool call,
    health says `written: 1`, and says what `stats()` says."""
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/start", _start_body(time.time())))
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/tool-calls", _tool_call(time.time())))
    eventually(lambda: engine.store.stats().written == 1)
    status, headers, body = engine.call("GET", "/_irimi/health")
    doc = json.loads(body)
    assert status == 200
    assert STAMP in headers and ("content-type", "application/json") in headers
    assert doc == {
        "engine_version": __version__,
        "serve": False,
        "store": {"queued": 0, "written": 1, "dropped": 0},
    }
    assert dataclasses.asdict(engine.store.stats()) == doc["store"]


def test_health_says_serve_when_the_engine_serves(tmp_path, monkeypatch):
    cfg = dataclasses.replace(engine_config(tmp_path, monkeypatch), mode=Serve())
    eng, _, stop = start_engine(cfg)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=10)
        conn.request("GET", "/_irimi/health")
        doc = json.loads(conn.getresponse().read())
        conn.close()
    finally:
        stop()
    # NullStore, `start_engine`'s default, counts nothing.
    assert doc == {
        "engine_version": __version__,
        "serve": True,
        "store": {"queued": 0, "written": 0, "dropped": 0},
    }


_START = f"/_irimi/runs/{RUN}/start"
_NOT_JSON = "the body is not JSON: "
# A replay's lookup of a read tool's recorded outcome.
_LOOKUP = f"/_irimi/runs/{RUN}/tool-calls/recorded"
LOOKUP: dict[str, JSONValue] = {"name": "db.balance", "args": {"account": "acc_1"}}

# (method, path, body, status, error). Every refusal names what was wrong in one line.
REFUSALS: list[tuple[str, str, bytes | dict | None, int, str]] = [
    ("GET", "/_irimi/nope", None, 404, "no control route '/_irimi/nope'"),
    (
        "POST",
        f"/_irimi/runs/{RUN}/restart",
        b"{}",
        404,
        f"no control route '/_irimi/runs/{RUN}/restart'",
    ),
    ("GET", "/_irimi/runs", None, 404, "no control route '/_irimi/runs'"),
    ("GET", _START, None, 405, f"{_START} takes POST, not GET"),
    ("POST", "/_irimi/health", b"{}", 405, "/_irimi/health takes GET, not POST"),
    (
        "POST",
        _START,
        b" " * (MAX_CONTROL_BODY + 1),
        413,
        f"the body is {MAX_CONTROL_BODY + 1} bytes, over the {MAX_CONTROL_BODY} allowed",
    ),
    (
        "POST",
        _START,
        b"{",
        400,
        _NOT_JSON + "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
    ),
    ("POST", _START, b'{"started_at": NaN}', 400, _NOT_JSON + "NaN is not a finite number"),
    ("POST", _START, b"[]", 400, "the body is a list, not an object"),
    ("POST", _START, b"null", 400, "the body is null, not an object"),
    (
        "POST",
        _START,
        {k: v for k, v in _start_body(1.0).items() if k != "trigger"},
        400,
        "missing required field 'trigger'",
    ),
    (
        "POST",
        _START,
        {**_start_body(1.0), "sdk_version": None},
        400,
        "'sdk_version' may not be null",
    ),
    (
        "POST",
        _START,
        {**_start_body(1.0), "started_at": "soon"},
        400,
        "'started_at' is a string, not a number",
    ),
    (
        "POST",
        f"/_irimi/runs/{RUN}/tool-calls",
        {**_tool_call(1.0), "run_id": "other"},
        400,
        "'run_id' comes from the path, not the body",
    ),
    (
        "POST",
        f"/_irimi/runs/{RUN}/end",
        {**_end_body(2.0), "outcome": "maybe"},
        400,
        "'outcome' is 'maybe', not one of ('ok', 'error')",
    ),
    (
        "POST",
        "/_irimi/runs/../x/start",
        _start_body(1.0),
        400,
        "'../x' cannot name a run",
    ),
    (
        "POST",
        "/_irimi/runs/unattributed/end",
        _end_body(2.0),
        400,
        "'unattributed' cannot name a run",
    ),
    (
        "POST",
        f"/_irimi/runs/{PROCESS_RUN}/start",
        _start_body(1.0),
        400,
        f"'{PROCESS_RUN}' {OWN_RUN}",
    ),
    (
        "POST",
        f"/_irimi/runs/{PROCESS_RUN}/start",
        b"not json",
        400,
        f"'{PROCESS_RUN}' {OWN_RUN}",
    ),
    (
        "POST",
        f"/_irimi/runs/{PROCESS_RUN}/end",
        _end_body(2.0),
        400,
        f"'{PROCESS_RUN}' {OWN_RUN}",
    ),
    # The lookup is checked in the same order, and refused in the same words.
    ("GET", _LOOKUP, None, 405, f"{_LOOKUP} takes POST, not GET"),
    ("POST", "/_irimi/runs/../x/tool-calls/recorded", LOOKUP, 400, "'../x' cannot name a run"),
    (
        "POST",
        _LOOKUP,
        b" " * (MAX_CONTROL_BODY + 1),
        413,
        f"the body is {MAX_CONTROL_BODY + 1} bytes, over the {MAX_CONTROL_BODY} allowed",
    ),
    (
        "POST",
        _LOOKUP,
        b"{",
        400,
        _NOT_JSON + "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
    ),
    ("POST", _LOOKUP, b'{"name": "x", "args": NaN}', 400, _NOT_JSON + "NaN is not a finite number"),
    ("POST", _LOOKUP, b"[]", 400, "the body is a list, not an object"),
    ("POST", _LOOKUP, {"name": "db.balance"}, 400, "missing required field 'args'"),
    ("POST", _LOOKUP, {"args": {}}, 400, "missing required field 'name'"),
    ("POST", _LOOKUP, {"name": 1, "args": {}}, 400, "'name' is an integer, not a string"),
]


@pytest.mark.parametrize(
    ("method", "path", "body", "status", "error"),
    REFUSALS,
    ids=[f"{s}-{e[:40]}" for _, _, _, s, e in REFUSALS],
)
def test_a_refused_control_request_answers_its_status_and_names_what_was_wrong(
    engine, method, path, body, status, error
):
    got_status, headers, got_body = engine.call(method, path, body)
    assert (got_status, json.loads(got_body)) == (status, _refused(error))
    assert STAMP in headers
    if status == 405:
        assert ("allow", "GET" if path.endswith("/health") else "POST") in headers
    reader = engine.finish()
    # Nothing was stored, so no directory was made: not a run, not `unattributed/`.
    assert reader.list_runs() == []
    assert not (engine.root / "runs").exists() and not (engine.root / "unattributed").exists()
    assert engine.seen == [] and engine.tool_calls == []


def test_a_tool_call_may_name_the_process_run(engine):
    """Only the process run's start and end are irimi's: a tool call made outside any SDK run
    belongs to the process run, as an exchange with no `Irimi-Run` does."""
    _ok(engine.call("POST", f"/_irimi/runs/{PROCESS_RUN}/tool-calls", _tool_call(time.time())))
    [call] = engine.finish().load_run(PROCESS_RUN).events
    assert (call.run_id, call.name) == (PROCESS_RUN, "db.write")
    assert engine.tool_calls == [call]


def test_a_tool_call_made_in_no_run_is_taken_unattributed_but_not_a_start(engine):
    """`irimi serve` gives its agent no process run, so a tool call made outside every SDK
    run names none: it is filed in `unattributed/`, as an exchange with no `Irimi-Run` is there.
    A start or an end for `unattributed` is still refused."""
    _ok(engine.call("POST", "/_irimi/runs/unattributed/tool-calls", _tool_call(time.time())))
    status, _, body = engine.call("POST", "/_irimi/runs/unattributed/start", _start_body(1.0))
    assert (status, json.loads(body)) == (400, _refused("'unattributed' cannot name a run"))
    reader = engine.finish()
    [call] = reader.load_run("unattributed").events
    assert (call.run_id, call.name) == ("unattributed", "db.write")
    assert engine.tool_calls == [call]
    assert reader.list_runs() == []


@pytest.mark.parametrize("run_id", [RUN, PROCESS_RUN, "unattributed"])
def test_a_lookup_under_an_engine_that_is_not_replaying_is_a_404_and_records_nothing(
    engine, run_id
):
    """Shadow mode answers no read tool from anything, so the SDK runs it for real. An SDK
    run, the process run and no run may all ask, and the lookup is no exchange, no tool call and
    no run."""
    route = f"/_irimi/runs/{run_id}/tool-calls/recorded"
    status, headers, body = engine.call("POST", route, LOOKUP)
    assert (status, json.loads(body)) == (404, _refused("no recorded call"))
    assert STAMP in headers
    reader = engine.finish()
    assert reader.list_runs() == []
    assert not (engine.root / "runs").exists() and not (engine.root / "unattributed").exists()
    assert engine.seen == [] and engine.tool_calls == []


@pytest.mark.parametrize("version", ["2", "0", "2.1", "x"])
def test_another_protocol_major_is_answered_426_naming_the_one_irimi_speaks(engine, version):
    status, headers, body = engine.call(
        "POST", _START, _start_body(1.0), {"Irimi-Protocol": version}
    )
    message = f"this irimi speaks control protocol 1, not {version}"
    assert (status, json.loads(body)) == (426, _refused(message))
    assert STAMP in headers and ("irimi-protocol", "1") in headers
    assert engine.finish().list_runs() == []


@pytest.mark.parametrize("headers", [{}, {"Irimi-Protocol": "1"}, {"Irimi-Protocol": "1.4"}])
def test_the_same_protocol_major_or_none_is_served(engine, headers):
    _ok(engine.call("POST", _START, _start_body(1.0), headers))
    assert [r.run_id for r in engine.finish().list_runs()] == [RUN]


def test_every_control_answer_names_the_protocol_irimi_speaks(engine):
    """A client learns the endpoint's version from any answer, not only from a 426."""
    answers = [
        engine.call("GET", "/_irimi/health"),
        engine.call("POST", _START, _start_body(1.0)),
        engine.call("POST", _LOOKUP, LOOKUP),
        engine.call("POST", "/_irimi/nowhere", {}),
    ]
    assert [status for status, _, _ in answers] == [200, 204, 404, 404]
    for _, headers, _ in answers:
        assert ("irimi-protocol", "1") in headers
    engine.finish()


def test_every_route_ignores_a_field_it_does_not_know(engine):
    """A minor addition to a body must not break an older endpoint, on any route."""
    extra = {"added_later": {"x": 1}}
    _ok(engine.call("POST", _START, {**_start_body(1.0), **extra}))
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/tool-calls", {**_tool_call(2.0), **extra}))
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/end", {**_end_body(3.0), **extra}))
    status, _, body = engine.call("POST", _LOOKUP, {**LOOKUP, **extra})
    assert (status, json.loads(body)) == (404, _refused("no recorded call"))
    reader = engine.finish()
    [run] = reader.list_runs()
    assert (run.run_id, run.end.outcome) == (RUN, "ok")
    assert [e.name for e in reader.load_run(RUN).events] == ["db.write"]


def _replaying(tmp_path, monkeypatch, *events, policy=None) -> _Engine:
    """An engine replaying a run whose `events` are given, as `cli.compose.build_engine` builds
    one: its policy a `ReplayPolicy` over their `ReplayIndex`, unless `policy` is given."""
    cfg = engine_config(tmp_path, monkeypatch)
    key = redact.load_key(tmp_path)
    store = DirectoryStore(tmp_path / "store", key, replay_of="recorded01")
    policy = policy or ReplayPolicy(ReplayIndex.from_events(list(events), key, cfg.maps), cfg.maps)
    tool_calls: list[ToolCall] = []
    eng, seen, stop = start_engine(cfg, policy=policy, store=store, on_tool_call=tool_calls.append)
    return _Engine(eng.listen_port(), store, seen, tool_calls, stop)


def test_a_lookup_the_recording_answers_is_a_stamped_200_once_and_records_nothing(
    tmp_path, monkeypatch, caplog
):
    """A hit is not a tool call: the SDK posts the call afterwards, as it posts every call.
    Args nested too deep to redact match nothing, and are a miss, never irimi's 500."""
    recorded = ToolCall(
        "tc-1", "recorded01", "db.balance", "read", "real", LOOKUP["args"], {"n": 1}, None, 1.0, 2.0
    )
    engine = _replaying(tmp_path, monkeypatch, recorded)
    with caplog.at_level(logging.ERROR, logger="irimi.engine.control"):
        try:
            hit = engine.call("POST", _LOOKUP, LOOKUP)
            again = engine.call("POST", _LOOKUP, LOOKUP)
            deep = engine.call("POST", _LOOKUP, _nested(LOOKUP, "args"))
        finally:
            reader = engine.finish()
    answer = {"result": {"n": 1}, "error": None, "awaited": False}
    assert (hit[0], json.loads(hit[2])) == (200, answer)
    for status, _, body in (again, deep):
        assert (status, json.loads(body)) == (404, _refused("no recorded call"))
    assert all(STAMP in headers for _, headers, _ in (hit, again, deep))
    assert [r for r in caplog.records if r.name == "irimi.engine.control"] == []
    assert reader.list_runs() == []
    assert engine.seen == [] and engine.tool_calls == []


def test_a_policy_whose_lookup_raises_is_irimi_s_500_never_a_forward(tmp_path, monkeypatch):
    """`AnswerPolicy.recorded_tool_call` never raises, but one that did would be answered as any
    failure of the endpoint's own is: a stamped 500, which the SDK takes for a miss."""

    class _Broken(ShadowPolicy):
        def recorded_tool_call(self, name, args):
            raise RuntimeError("the recording is on fire")

    engine = _replaying(tmp_path, monkeypatch, policy=_Broken())
    try:
        status, headers, body = engine.call("POST", _LOOKUP, LOOKUP)
    finally:
        engine.finish()
    assert (status, json.loads(body)) == (500, INTERNAL)
    assert STAMP in headers and engine.seen == []


def test_a_control_request_through_the_forward_proxy_is_answered_the_same(engine):
    """An absolute-form URL naming the listener - an SDK whose `IRIMI_CONTROL` is not on NO_PROXY
    - reaches the endpoint, never the reverse door's 403 or the listener itself."""
    base = f"http://127.0.0.1:{engine.port}/_irimi"
    status, body = via_proxy(engine.port, "GET", f"{base}/health")
    assert status == 200 and json.loads(body)["engine_version"] == __version__
    t0 = time.time()
    for path, doc in [("start", _start_body(t0)), ("end", _end_body(t0 + 1))]:
        status, body = via_proxy(
            engine.port, "POST", f"{base}/runs/{RUN}/{path}", body=json.dumps(doc).encode()
        )
        assert (status, body) == (204, b"")
    record = engine.finish().load_run(RUN).record
    assert (record.attribution, record.end.outcome) == ("sdk", "ok")
    assert engine.seen == []


def test_irimis_own_fields_are_never_read_from_a_posted_start(engine):
    """Attribution, mode and the engine's version are irimi's to say: a start that posts its own
    is stored as an SDK run of this engine all the same."""
    body = {**_start_body(1.0), "attribution": "process", "engine_version": "9.9", "mode": "x"}
    _ok(engine.call("POST", _START, body))
    record = engine.finish().load_run(RUN).record
    assert (record.attribution, record.mode, record.engine_version) == (
        "sdk",
        "shadow",
        __version__,
    )


def test_a_path_under_irimi_on_another_host_is_an_ordinary_request(engine, upstream):
    """Only the listener itself serves the control endpoint. The same path on any other host is
    the agent's own request, decided and recorded like every other, and never stamped `control`."""
    status, body = via_proxy(engine.port, "GET", f"http://127.0.0.1:{upstream}/_irimi/health")
    assert (status, body) == (200, b"hello from upstream")
    [exchange] = engine.seen
    assert (exchange.answered_by, exchange.request.path) == ("live", "/_irimi/health")


def test_a_store_that_raises_answers_500_and_the_next_request_is_served(
    engine, monkeypatch, caplog
):
    def broken(record):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(engine.store, "start_run", broken)
    with caplog.at_level(logging.ERROR, logger="irimi.engine.control"):
        status, headers, body = engine.call("POST", _START, _start_body(time.time()))
    assert (status, json.loads(body)) == (500, INTERNAL)
    assert STAMP in headers
    # Logged with its traceback, which the 500's body does not carry.
    [logged] = [r for r in caplog.records if r.name == "irimi.engine.control"]
    assert logged.exc_info is not None and "disk on fire" in str(logged.exc_info[1])
    status, _, _ = engine.call("GET", "/_irimi/health")
    assert status == 200


def test_an_answer_the_engine_cannot_send_is_a_500_never_a_forward(engine, monkeypatch):
    """`ControlEndpoint.answer` never raises, and the engine guards the call anyway: a control
    request that escaped the hook would be forwarded to the listener it names - irimi itself."""

    def broken(self, request):
        raise RuntimeError("no answer")

    monkeypatch.setattr(ControlEndpoint, "answer", broken)
    status, headers, body = engine.call("GET", "/_irimi/health")
    assert (status, json.loads(body)) == (500, INTERNAL)
    assert STAMP in headers
    assert engine.seen == []


def test_the_endpoint_answers_500_itself_when_its_store_raises():
    """Over the seam, with no engine guard behind it."""

    class _Broken(NullStore):
        def start_run(self, record):
            raise RuntimeError("disk on fire")

    body = json.dumps(_start_body(1.0)).encode()
    request = Request("POST", "http", "127.0.0.1", 4000, _START, "", (), body)
    response = ControlEndpoint(_Broken(), serve=False).answer(request)
    assert (response.status, json.loads(response.body)) == (500, INTERNAL)
    assert response.header("irimi-answered-by") == "control"


def test_a_decoder_that_raises_anything_but_its_refusal_is_irimi_s_500_never_too_deep(
    monkeypatch, caplog
):
    """A decoder raises only TraceFormatError, however deep the value it refuses, so one
    that raised RecursionError would be irimi's own bug: a 500 logged with its traceback, never
    TOO_DEEP blaming the agent for a body `json.loads` read."""

    def broken(doc):
        raise RecursionError("maximum recursion depth exceeded")

    monkeypatch.setattr(protocol_control, "run_start_from_json", broken)
    body = json.dumps(_start_body(1.0)).encode()
    request = Request("POST", "http", "127.0.0.1", 4000, _START, "", (), body)
    with caplog.at_level(logging.ERROR, logger="irimi.engine.control"):
        response = ControlEndpoint(NullStore(), serve=False).answer(request)
    assert (response.status, json.loads(response.body)) == (500, INTERNAL)
    [logged] = [r for r in caplog.records if r.name == "irimi.engine.control"]
    assert logged.exc_info is not None and logged.exc_info[0] is RecursionError


def test_an_on_tool_call_that_raises_costs_the_callback_not_the_record(
    tmp_path, monkeypatch, caplog
):
    cfg = engine_config(tmp_path, monkeypatch)
    store = DirectoryStore(tmp_path / "store", redact.load_key(tmp_path))

    def broken(call):
        raise RuntimeError("summary broke")

    eng, _, stop = start_engine(cfg, store=store, on_tool_call=broken)
    try:
        with caplog.at_level(logging.ERROR, logger="irimi.engine.control"):
            conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=10)
            body = json.dumps(_tool_call(1.0))
            conn.request("POST", f"/_irimi/runs/{RUN}/tool-calls", body=body)
            resp = conn.getresponse()
            assert (resp.status, resp.read()) == (204, b"")
            conn.close()
    finally:
        stop()
    [call] = StoreReader(tmp_path / "store").load_run(RUN).events
    assert isinstance(call, ToolCall)
    assert call.name == "db.write"
    # Swallowed, and logged with its traceback.
    [logged] = [r for r in caplog.records if r.name == "irimi.engine.control"]
    assert logged.exc_info is not None and "summary broke" in str(logged.exc_info[1])


def test_an_end_in_error_carries_its_error_to_the_store():
    """The `error` an end posts is read into the `ErrorInfo` the store is handed. Over the seam,
    with a store that remembers its calls: what `DirectoryStore` does with it is tested in
    `tests/test_store.py`."""
    ended = []

    class _Ends(NullStore):
        def end_run(self, run_id, ended_at, outcome, error=None, exit_code=None):
            ended.append((run_id, ended_at, outcome, error, exit_code))

    body = {"ended_at": 2.0, "outcome": "error", "error": {"type": "x.Boom", "message": "no"}}
    path = f"/_irimi/runs/{RUN}/end"
    request = Request("POST", "http", "127.0.0.1", 4000, path, "", (), json.dumps(body).encode())
    assert ControlEndpoint(_Ends(), serve=False).answer(request).status == 204
    assert ended == [(RUN, 2.0, "error", ErrorInfo("x.Boom", "no"), None)]


def test_a_posted_error_message_is_cut_to_the_length_a_trace_keeps(engine):
    """`ErrorInfo.message` is at most MAX_ERROR_MESSAGE characters: a label, not a log. An
    SDK that posts a longer one, on an end or on a tool call, has it cut by the store, once it is
    redacted, never refused, so a long message never costs a run its end."""
    long = {"type": "x.Boom", "message": "m" * (MAX_ERROR_MESSAGE * 3)}
    t0 = time.time()
    _ok(engine.call("POST", _START, _start_body(t0)))
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/tool-calls", {**_tool_call(t0), "error": long}))
    end = {"ended_at": t0 + 1, "outcome": "error", "error": long}
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/end", end))
    run = engine.finish().load_run(RUN)
    [call] = run.events
    cut = ErrorInfo("x.Boom", "m" * MAX_ERROR_MESSAGE)
    assert (run.record.end.outcome, run.record.end.error, call.error) == ("error", cut, cut)


def test_a_posted_error_type_is_cut_to_the_length_a_trace_keeps(engine):
    """A posted error's type is the client's to say, so it may be nearly all of a 2 MiB body. It
    is cut to MAX_ERROR_TYPE characters, on an end and on a tool call, never refused: by the
    store, once it is redacted, and for the live line a tool call prints."""
    long = {"type": "t." * (MAX_CONTROL_BODY // 2 - 500), "message": "no"}
    t0 = time.time()
    _ok(engine.call("POST", _START, _start_body(t0)))
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/tool-calls", {**_tool_call(t0), "error": long}))
    end = {"ended_at": t0 + 1, "outcome": "error", "error": long}
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/end", end))
    run = engine.finish().load_run(RUN)
    [call] = run.events
    cut = ErrorInfo("t." * (MAX_ERROR_TYPE // 2), "no")
    assert (run.record.end.outcome, run.record.end.error, call.error) == ("error", cut, cut)
    assert [c.error for c in engine.tool_calls] == [cut]


def test_a_posted_error_reaches_the_store_whole_so_a_key_straddling_a_cut_is_redacted(
    engine, tmp_path
):
    """The endpoint hands the store a posted error whole, and the store redacts it before it cuts
    it. Cut here first, a key straddling either cut kept a part too short for its shape, which
    redaction no longer found, and which reached disk. The live line's call is cut as
    the stored one is, its type all it prints, and it is never stored."""
    secret = "ghp_" + "C" * 36
    kind = "t" * (MAX_ERROR_TYPE - 30) + "." + secret + ".Boom"
    message = "m" * (MAX_ERROR_MESSAGE - 30) + " " + secret + " and more"
    error = {"type": kind, "message": message}
    t0 = time.time()
    _ok(engine.call("POST", _START, _start_body(t0)))
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/tool-calls", {**_tool_call(t0), "error": error}))
    end = {"ended_at": t0 + 1, "outcome": "error", "error": error}
    _ok(engine.call("POST", f"/_irimi/runs/{RUN}/end", end))
    run = engine.finish().load_run(RUN)
    [call] = run.events
    hidden = redact.placeholder(redact.load_key(tmp_path), secret)
    cut = ErrorInfo(
        kind.replace(secret, hidden)[:MAX_ERROR_TYPE],
        message.replace(secret, hidden)[:MAX_ERROR_MESSAGE],
    )
    assert (run.record.end.error, call.error) == (cut, cut)
    assert [c.error for c in engine.tool_calls] == [
        ErrorInfo(kind[:MAX_ERROR_TYPE], message[:MAX_ERROR_MESSAGE])
    ]
    store_files = (tmp_path / "store").rglob("*")
    assert not any(b"ghp_" in p.read_bytes() for p in store_files if p.is_file())


def test_a_refusal_quotes_at_most_a_line_of_what_it_refused(engine):
    """A word outside a vocabulary is quoted back, a 1 MB one cut by its decoder at MAX_QUOTED
    characters. A refusal that quotes the path is cut at MAX_REFUSAL. A value of the
    wrong type is named by its type, never quoted."""
    body = {**_tool_call(1.0), "kind": "s" * 1_000_000}
    status, _, raw = engine.call("POST", f"/_irimi/runs/{RUN}/tool-calls", body)
    word = "s" * MAX_QUOTED
    assert (status, json.loads(raw)) == (
        400,
        _refused(f"'kind' is '{word}'…, not one of ('read', 'write')"),
    )
    status, _, raw = engine.call("POST", f"/_irimi/runs/{'x' * 5_000}/start", _start_body(1.0))
    error = json.loads(raw)["error"]["message"]
    assert status == 400 and error == "'" + "x" * (MAX_REFUSAL - 1)
    status, _, raw = engine.call("POST", _START, {**_start_body(1.0), "started_at": "s" * 1_000})
    assert (status, json.loads(raw)) == (400, _refused("'started_at' is a string, not a number"))


def test_a_head_request_gets_its_405_without_a_body_and_the_connection_stays_usable(engine):
    """A response to HEAD carries no content (RFC 9110), and mitmproxy sends whatever a response
    holds: a 405 that kept its JSON would be read as the start of the connection's next response,
    so `HEAD /_irimi/health` broke every later request on that connection.

    Over a raw socket, both requests in one write: `http.client` passes or fails this by when the
    stray bytes arrive."""
    host = f"127.0.0.1:{engine.port}"
    sent = "".join(f"{m} /_irimi/health HTTP/1.1\r\nhost: {host}\r\n\r\n" for m in ("HEAD", "GET"))
    with socket.create_connection(("127.0.0.1", engine.port), timeout=10) as sock:
        sock.sendall(sent.encode())
        received = b""
        while not received.endswith(b"}"):  # the GET's JSON is the last thing on the wire
            chunk = sock.recv(65536)
            assert chunk, f"the connection closed after {received!r}"
            received += chunk
    head, get = received.split(b"\r\n\r\n", 1)
    assert head.startswith(b"HTTP/1.1 405 ")
    assert b"\r\nirimi-answered-by: control\r\n" in head and b"\r\nallow: GET" in head
    # Nor a Content-Length: a HEAD answer may send one only if it is what a GET would get.
    assert b"content-length" not in head.lower()
    # The next byte after the 405's head is the GET's answer, not the 405's JSON.
    assert get.startswith(b"HTTP/1.1 200 ")
    assert json.loads(get.split(b"\r\n\r\n", 1)[1])["engine_version"] == __version__


# A field nested this deep is read by every Python's `json.loads` on any thread (3.12's stops near
# 10,000), so its refusal is the decoder's on every platform. A decoder fed one 100,000 deep
# is `tests/test_trace.py`'s.
_DEEP = 5_000


def _nested(doc: dict, field: str) -> bytes:
    """`doc` with `field` a list nested _DEEP times, where a string is expected."""
    deep = "[" * _DEEP + "]" * _DEEP
    return (
        json.dumps({**doc, field: None}).replace(f'"{field}": null', f'"{field}": {deep}').encode()
    )


@pytest.mark.parametrize(
    ("path", "body", "expected"),
    [
        (_START, b"[" * 1_000_000 + b"]" * 1_000_000, TOO_DEEP),
        (_START, _nested(_start_body(1.0), "sdk_version"), "'sdk_version' is a list, not a string"),
        (
            f"/_irimi/runs/{RUN}/tool-calls",
            _nested(_tool_call(1.0), "name"),
            "'name' is a list, not a string",
        ),
    ],
    ids=["the-body", "a-start-field", "a-tool-call-field"],
)
def test_a_body_nested_too_deeply_to_read_is_a_400_never_a_500(
    engine, caplog, path, body, expected
):
    """A mistyped field is a 400 however it is mistyped, never a 500 logged as irimi's failure
    for what is the agent's. A body `json.loads` cannot read, a million deep, is TOO_DEEP on
    every Python. A deeply nested field is refused by its decoder, which names the field's type
    and never quotes it: quoting it with `repr` raised RecursionError on some Pythons and stacks
    and not others, so the same body was answered differently on CI's runners."""
    with caplog.at_level(logging.ERROR, logger="irimi.engine.control"):
        status, headers, raw = engine.call("POST", path, body)
    assert (status, json.loads(raw)) == (400, _refused(expected))
    assert STAMP in headers
    assert [r for r in caplog.records if r.name == "irimi.engine.control"] == []
    engine.finish()
    assert not (engine.root / "runs").exists() and engine.tool_calls == []
