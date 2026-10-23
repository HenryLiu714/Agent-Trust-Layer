"""The trace store on disk: `DirectoryStore` writes, `StoreReader` reads back.

Every test drives the store through its public methods and asserts on what `StoreReader` and the
files under the root say, so a test here breaks only when what reaches disk changes. The real
`irimi shadow` over these same files is `tests/test_trace_e2e.py`'s.
"""

import dataclasses
import errno
import json
import logging
import math
import os
import threading
import time
from pathlib import Path

import pytest

from irimi import exchange, files, limits, redact, store, trace
from irimi.exchange import Exchange, Flag, Request, Response
from irimi.store import DirectoryStore, NullStore, RunNotFound, StoreReader, StoreStats
from irimi.trace import ErrorInfo, RunRecord, TelemetrySeen, ToolCall, TraceFormatError, Trigger

SECRET = "sk_live_StoreUnitSecret"


@pytest.fixture
def key(tmp_path) -> bytes:
    return redact.load_key(tmp_path / "home")


@pytest.fixture
def root(tmp_path) -> Path:
    return tmp_path / "store"


def _exchange(
    run_id: str, path: str = "/v1/things", *, body: bytes = b"", answer: bytes = b"", kind="read"
) -> Exchange:
    return Exchange(
        request=Request("POST", "https", "api.example.com", 443, path, "", (), body),
        response=Response(200, (("content-type", "application/octet-stream"),), answer),
        service="example",
        operation="things.create",
        kind=kind,
        answered_by="live",
        run_id=run_id,
        started_at=1.0,
        ended_at=2.0,
    )


def _record(run_id: str, started_at: float | None = 10.0, args=None) -> RunRecord:
    return RunRecord(
        schema_version=trace.SCHEMA_VERSION,
        run_id=run_id,
        mode="shadow",
        attribution="process",
        trigger=Trigger(name="agent", entrypoint=None, args=args, replayable=True),
        agent_version="1.2.3",
        engine_version="0.0.0",
        sdk_version=None,
        started_at=started_at,
    )


def _replays(record: RunRecord, replay_of: str) -> RunRecord:
    """`record` as a replay of `replay_of` builds it: `mode` set from `replay_of`."""
    return dataclasses.replace(record, mode="replay", replay_of=replay_of)


def _tool_call(run_id: str, args) -> ToolCall:
    return ToolCall("t1", run_id, "db.write", "write", "shadow", args, None, None, 1.0, 2.0)


class _StoreOs:
    """The `os` module as `irimi.store` sees it, with the functions a test sets replaced. Patching
    `os` itself would change it for every thread in the process, pytest's own included."""

    def __getattr__(self, name: str):
        return getattr(os, name)


@pytest.fixture
def store_os(monkeypatch) -> _StoreOs:
    proxy = _StoreOs()
    monkeypatch.setattr(store, "os", proxy)
    monkeypatch.setattr(files, "os", proxy)  # the store writes its files through `files`
    return proxy


def _drain(s: DirectoryStore, written: int) -> None:
    deadline = time.monotonic() + 10
    while s.stats().written < written or s.stats().queued:
        assert time.monotonic() < deadline, s.stats()
        time.sleep(0.005)


def _paths(stored) -> list[str]:
    return _paths_of(stored.events)


def _paths_of(events) -> list[str]:
    return [e.request.path for e in events]


# ---------------------------------------------------------------------------- the seam's contract


def test_null_store_takes_every_call_and_keeps_nothing():
    s = NullStore()
    s.start_run(_record("r1"))
    s.record(_exchange("r1"))
    s.record_tool_call(_tool_call("r1", None))
    s.end_run("r1", 3.0, "ok", exit_code=0)
    # The health endpoint reads these, so a store that keeps nothing reports nothing.
    assert s.stats() == StoreStats(queued=0, written=0, dropped=0)
    s.close()


def test_a_run_started_recorded_and_ended_reads_back_whole(root, key):
    s = DirectoryStore(root, key)
    s.start_run(_record("r1", args={"argv": ["agent", "--token", SECRET]}))
    s.record(_exchange("r1", "/a"))
    s.record(_exchange("r1", "/b"))
    s.end_run("r1", 3.0, "error", ErrorInfo("exit", "exited 2"), exit_code=2)
    s.close()

    run = StoreReader(root).load_run("r1")
    assert (run.record.attribution, run.record.agent_version) == ("process", "1.2.3")
    assert run.record.end == trace.RunEnd(3.0, "error", ErrorInfo("exit", "exited 2"), 2)
    assert run.record.dropped_events == 0
    assert _paths(run) == ["/a", "/b"]
    # A trigger's args are redacted before they reach disk, as a body is.
    assert run.record.trigger is not None
    assert trace.trigger_args(run.record.trigger) == {
        "argv": ["agent", "--token", redact.placeholder(key, SECRET)]
    }
    assert not any(SECRET.encode() in p.read_bytes() for p in root.rglob("*") if p.is_file())
    assert [json.loads(line)["seq"] for line in (root / "runs/r1/events.jsonl").open()] == [1, 2]


def test_the_first_event_of_a_run_with_no_start_creates_a_header_run(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r2"))
    s.close()
    record = StoreReader(root).load_run("r2").record
    # Started when its first event did, so it sorts among the runs that have a start.
    assert record == RunRecord.header("r2", started_at=1.0)
    assert (record.attribution, record.trigger, record.end) == ("header", None, None)


def test_a_start_for_a_header_run_rewrites_its_record(root, key):
    """A run whose events arrived before its start is a `header` run until the start arrives,
    which rewrites its record whole and keeps its events."""
    s = DirectoryStore(root, key)
    s.record(_exchange("r2"))
    s.start_run(dataclasses.replace(_record("r2"), attribution="sdk"))
    s.close()
    run = StoreReader(root).load_run("r2")
    assert run.record == dataclasses.replace(_record("r2"), attribution="sdk")
    assert _paths(run) == ["/v1/things"]
    assert s.stats().dropped == 0


def test_a_start_for_a_run_that_has_ended_is_dropped_and_the_run_kept_whole(root, key):
    """A start re-posted after its run's end rewrote run.json whole, so the run lost its end, its
    outcome and its first trigger, and listed `incomplete`. It is dropped instead, counted on
    the store alone, and the run's files are not rewritten: it lost nothing, so its `dropped_events`
    stays 0. So it is when the run ended in an earlier process, and the start reaches a store that
    has never seen it."""
    s = DirectoryStore(root, key)
    s.start_run(dataclasses.replace(_record("r1"), attribution="sdk"))
    s.record(_exchange("r1"))
    s.end_run("r1", 20.0, "ok")
    s.start_run(dataclasses.replace(_record("r1", started_at=30.0), attribution="sdk"))
    s.close()
    assert s.stats().dropped == 1
    assert "r1" not in s._runs  # forgotten again, as at its end: a serve holds only open runs
    run = StoreReader(root).load_run("r1")
    assert run.record == dataclasses.replace(
        _record("r1"), attribution="sdk", end=trace.RunEnd(20.0, "ok", None, None)
    )
    assert _paths(run) == ["/v1/things"]
    ended = {p.name: p.read_bytes() for p in (root / "runs" / "r1").iterdir()}
    s = DirectoryStore(root, key)
    s.start_run(dataclasses.replace(_record("r1", started_at=40.0), attribution="sdk"))
    s.close()
    assert s.stats().dropped == 1
    assert {p.name: p.read_bytes() for p in (root / "runs" / "r1").iterdir()} == ended


def test_every_run_a_replay_s_store_starts_is_that_replay_s(root, key):
    """The process run and an SDK run alike: each record's constructor names the replay, and the
    store checks it rather than stamping it, so a start that names no replay is dropped, counted
    on the store alone, with no run directory of its own."""
    trigger = Trigger("handle", "agent:handle", {}, True)
    s = DirectoryStore(root, key, replay_of="rec1")
    s.start_run(RunRecord.process("p1", ["agent"], None, 10.0, replay_of="rec1"))
    s.start_run(RunRecord.sdk("s1", trigger, None, "0.0.0", 10.0, replay_of="rec1"))
    s.start_run(_record("x1"))
    s.close()
    assert s.stats().dropped == 1
    reader = StoreReader(root)
    for run_id, attribution in (("p1", "process"), ("s1", "sdk")):
        record = reader.load_run(run_id).record
        assert (record.attribution, record.mode, record.replay_of) == (
            attribution,
            "replay",
            "rec1",
        )
    assert not (root / "runs" / "x1").exists()
    with pytest.raises(RunNotFound):
        reader.load_run("x1")
    s = DirectoryStore(root / "other", key)
    s.start_run(_record("p2"))
    s.close()
    record = StoreReader(root / "other").load_run("p2").record
    assert (record.mode, record.replay_of) == ("shadow", None)


def test_a_header_run_a_replay_makes_is_that_replay_s(root, key):
    """A store a replay writes names the run it replays, and so does each header run it makes:
    `mode: replay` and `replay_of`, by `trace.mode_for`, the rule the process run follows."""
    s = DirectoryStore(root, key, replay_of="rec1")
    s.record(_exchange("r2"))
    s.close()
    record = StoreReader(root).load_run("r2").record
    assert (record.mode, record.replay_of, record.attribution) == ("replay", "rec1", "header")
    assert record == RunRecord.header("r2", started_at=1.0, replay_of="rec1")


def test_a_replay_never_writes_into_the_run_it_replays(root, key):
    """An agent that names its runs itself - an `Irimi-Run` label, a run id its SDK reuses - names
    the recorded run again when it is replayed. Appending to that run merged two runs in one
    directory, and rewrote its run.json as a replay of itself: the recording changed under every
    later replay of it. The replay's events go to `unattributed/`, flagged, as a run id that names
    a directory another run holds does, and its start and end are dropped."""
    s = DirectoryStore(root, key)
    s.start_run(dataclasses.replace(_record("nightly"), attribution="sdk"))
    s.record(_exchange("nightly", "/recorded"))
    s.end_run("nightly", 20.0, "ok")
    s.close()
    recorded = {p.name: p.read_bytes() for p in (root / "runs" / "nightly").iterdir()}
    s = DirectoryStore(root, key, replay_of="nightly")
    s.start_run(_replays(dataclasses.replace(_record("nightly"), attribution="sdk"), "nightly"))
    s.record(_exchange("nightly", "/replayed"))
    s.end_run("nightly", 30.0, "ok")
    s.close()
    assert {p.name: p.read_bytes() for p in (root / "runs" / "nightly").iterdir()} == recorded
    (stray,) = StoreReader(root).load_run(trace.UNATTRIBUTED).events
    assert isinstance(stray, Exchange)
    assert (stray.request.path, Flag.BAD_RUN_ID in stray.flags) == ("/replayed", True)
    assert s.stats().dropped == 2


def test_a_replay_writes_only_the_runs_it_makes(root, key):
    """An agent that names its runs itself names every recorded run again, not only the one
    replayed: W9 `control_runs` posts five by name, and a replay of one appended to the other four
    and rewrote each record as the replay's. A replay's store writes only the runs it made:
    every run already stored is kept byte for byte, and what the replay said of it is filed in
    `unattributed/`, flagged, its start and end dropped. A run it starts, or makes from a header,
    is its own, and stays its own after it ends: a late event is still appended. A store that is
    no replay's resumes an existing run as it always has."""
    s = DirectoryStore(root, key)
    for run_id in ("replayed", "sibling"):
        s.start_run(dataclasses.replace(_record(run_id), attribution="sdk"))
        s.record(_exchange(run_id, "/recorded"))
        s.end_run(run_id, 20.0, "ok")
    s.close()
    recorded = {p.name: p.read_bytes() for p in (root / "runs" / "sibling").iterdir()}
    s = DirectoryStore(root, key, replay_of="replayed")
    s.start_run(_replays(dataclasses.replace(_record("sibling"), attribution="sdk"), "replayed"))
    s.record(_exchange("sibling", "/replayed"))
    s.end_run("sibling", 30.0, "ok")
    s.start_run(_replays(dataclasses.replace(_record("fresh"), attribution="sdk"), "replayed"))
    s.record(_exchange("fresh", "/replayed"))
    s.end_run("fresh", 30.0, "ok")
    s.record(_exchange("fresh", "/late"))
    s.record(_exchange("labelled", "/replayed"))
    s.close()
    assert {p.name: p.read_bytes() for p in (root / "runs" / "sibling").iterdir()} == recorded
    (stray,) = StoreReader(root).load_run(trace.UNATTRIBUTED).events
    assert isinstance(stray, Exchange)
    assert (stray.run_id, stray.request.path, Flag.BAD_RUN_ID in stray.flags) == (
        "sibling",
        "/replayed",
        True,
    )
    assert s.stats().dropped == 2
    reader = StoreReader(root)
    for run_id, paths in (("fresh", ["/replayed", "/late"]), ("labelled", ["/replayed"])):
        run = reader.load_run(run_id)
        assert (run.record.mode, run.record.replay_of) == ("replay", "replayed")
        assert [e.request.path for e in run.events if isinstance(e, Exchange)] == paths
    s = DirectoryStore(root, key)
    s.record(_exchange("sibling", "/resumed"))
    s.close()
    resumed = StoreReader(root).load_run("sibling")
    assert [e.request.path for e in resumed.events] == ["/recorded", "/resumed"]
    assert (resumed.record.mode, resumed.record.replay_of) == ("shadow", None)


def test_a_header_run_started_by_an_event_with_no_clock_has_no_start(root, key):
    s = DirectoryStore(root, key)
    s.record(dataclasses.replace(_exchange("r2"), started_at=0.0, ended_at=0.0))
    s.close()
    assert StoreReader(root).load_run("r2").record == RunRecord.header("r2")


def test_a_header_run_sorts_by_its_first_event_among_runs_that_started(root, key):
    """A header run once had no start and sorted after every run that had one, so `irimi runs
    list` hid an agent's labelled runs behind as many process runs as its limit."""
    s = DirectoryStore(root, key)
    for i in range(3):
        s.start_run(_record(f"p{i}", started_at=10.0 + i))
    s.record(dataclasses.replace(_exchange("labelled"), started_at=11.5, ended_at=11.6))
    s.close()
    assert [r.run_id for r in StoreReader(root).list_runs()] == ["p2", "labelled", "p1", "p0"]


def test_load_run_without_bodies_reads_no_blob_and_still_finds_a_missing_one(
    root, key, monkeypatch
):
    """`irimi runs list` counts events and reads no body: `bodies=False` opens no blob, but a
    missing or resized one is still damage."""
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.record(_exchange("r1", answer=b"a body only a blob holds"))
    s.close()
    reader = StoreReader(root)
    loaded = reader.load_run("r1").events[0]
    assert isinstance(loaded, Exchange) and loaded.response is not None and loaded.response.body

    real_read_bytes = Path.read_bytes

    def no_blob_reads(path):
        assert "blobs" not in path.parts, f"read a blob: {path}"
        return real_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", no_blob_reads)
    (bare,) = reader.load_run("r1", bodies=False).events
    assert isinstance(bare, Exchange) and bare.response is not None
    assert (bare.request.body, bare.response.body) == (b"", b"")
    assert (
        dataclasses.replace(
            loaded,
            request=dataclasses.replace(loaded.request, body=b""),
            response=dataclasses.replace(loaded.response, body=b""),
        )
        == bare
    )

    monkeypatch.undo()
    blob = next(p for p in (root / "blobs").rglob("*") if p.is_file())
    blob.write_bytes(blob.read_bytes() + b"x")
    with pytest.raises(TraceFormatError, match="bytes, not"):
        reader.load_run("r1", bodies=False)
    blob.unlink()
    with pytest.raises(TraceFormatError, match="missing"):
        reader.load_run("r1", bodies=False)


def test_a_late_event_after_the_run_ended_is_still_appended(root, key):
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.end_run("r1", 3.0, "ok", exit_code=0)
    s.record(_exchange("r1", "/late"))
    s.close()
    run = StoreReader(root).load_run("r1")
    assert (run.record.end.outcome, _paths(run)) == ("ok", ["/late"])


def test_a_tool_calls_args_and_result_are_redacted_and_share_the_runs_seq(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/a"))
    s.record_tool_call(_tool_call("r1", {"api_key": "plain", "note": SECRET}))
    s.close()
    lines = [json.loads(line) for line in (root / "runs/r1/events.jsonl").open()]
    assert [(line["seq"], line["type"]) for line in lines] == [(1, "exchange"), (2, "tool_call")]
    (_, call) = StoreReader(root).load_run("r1").events
    assert isinstance(call, ToolCall)
    assert call.args == {
        "api_key": redact.placeholder(key, "plain"),
        "note": redact.placeholder(key, SECRET),
    }


def test_an_error_and_a_trigger_name_are_redacted_before_they_reach_disk(root, key):
    """`ErrorInfo.from_exception` keeps `str(exc)`, and an exception's message may quote the key
    it refused; a trigger's name is the argv[0] its args repeat. A posted error's type is
    whatever the client sent, so it is redacted as its message is."""
    s = DirectoryStore(root, key)
    s.start_run(dataclasses.replace(_record("r1"), trigger=Trigger(SECRET, None, None, True)))
    call = _tool_call("r1", None)
    error = ErrorInfo(f"x.{SECRET}", f"refused {SECRET}")
    s.record_tool_call(dataclasses.replace(call, error=error))
    s.end_run("r1", 3.0, "error", ErrorInfo("x.Err", f"Invalid API key: {SECRET}"), exit_code=1)
    s.close()
    run = StoreReader(root).load_run("r1")
    hidden = redact.placeholder(key, SECRET)
    assert run.record.trigger is not None and run.record.trigger.name == hidden
    assert run.record.end.error == ErrorInfo("x.Err", f"Invalid API key: {hidden}")
    (stored,) = run.events
    assert isinstance(stored, ToolCall)
    assert stored.error == ErrorInfo(f"x.{hidden}", f"refused {hidden}")
    assert not any(SECRET.encode() in p.read_bytes() for p in root.rglob("*") if p.is_file())


def test_an_error_is_redacted_whole_and_only_then_cut(root, key):
    """A key straddling MAX_ERROR_TYPE and one straddling MAX_ERROR_MESSAGE, on a tool call and
    on an end. Cut first, each kept part is shorter than `ghp_`'s shape, which redaction would
    not find, so it reached disk. Redacted whole, the stored error is a prefix of the redacted
    whole, as a stored body is: each key is its whole placeholder, and no part of it is kept.
    """
    secret = "ghp_" + "S" * 36
    kind = "t" * (limits.MAX_ERROR_TYPE - 30) + "." + secret + ".Boom"
    message = "m" * (limits.MAX_ERROR_MESSAGE - 30) + " " + secret + " and more"
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.record_tool_call(dataclasses.replace(_tool_call("r1", None), error=ErrorInfo(kind, message)))
    s.end_run("r1", 3.0, "error", ErrorInfo(kind, message), exit_code=1)
    s.close()
    run = StoreReader(root).load_run("r1")
    hidden = redact.placeholder(key, secret)
    cut = ErrorInfo(
        kind.replace(secret, hidden)[: limits.MAX_ERROR_TYPE],
        message.replace(secret, hidden)[: limits.MAX_ERROR_MESSAGE],
    )
    assert cut.type.endswith(hidden + ".B") and cut.message.endswith(hidden + " a")
    (stored,) = run.events
    assert isinstance(stored, ToolCall)
    assert (run.record.end.error, stored.error) == (cut, cut)
    assert not any(b"ghp_" in p.read_bytes() for p in root.rglob("*") if p.is_file())


def test_a_run_json_says_the_version_its_lines_are_written_in(root, key):
    """A caller's record claiming another version would put this writer's lines under a run.json
    no reader of this version reads (docs/trace-format.md, "Versioning")."""
    s = DirectoryStore(root, key)
    s.start_run(dataclasses.replace(_record("r1"), schema_version=trace.SCHEMA_VERSION + 1))
    s.record(_exchange("r1"))
    s.close()
    assert StoreReader(root).load_run("r1").record.schema_version == trace.SCHEMA_VERSION


def test_telemetry_is_stored_as_a_host_and_a_time_and_no_body(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", body=b"an envelope", answer=b"ok", kind="telemetry"))
    s.close()
    run = StoreReader(root).load_run("r1")
    assert run.events == [TelemetrySeen("r1", "api.example.com", 1.0)]
    assert not (root / "blobs").exists()


# ----------------------------------------------------------------------------------- the bodies


def test_two_exchanges_with_the_same_response_body_share_one_blob(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/a", answer=b"the same bytes"))
    s.record(_exchange("r1", "/b", answer=b"the same bytes"))
    s.close()
    assert [p.name for p in (root / "blobs").iterdir()] == [
        trace.body_ref(b"the same bytes").sha256
    ]
    first, second = StoreReader(root).load_run("r1").events
    assert isinstance(first, Exchange) and isinstance(second, Exchange)
    assert first.response == second.response


def test_an_empty_body_stores_no_blob_and_a_null_ref(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r1"))
    s.close()
    (line,) = [json.loads(line) for line in (root / "runs/r1/events.jsonl").open()]
    assert (line["request"]["body"], line["response"]["body"]) == (None, None)
    assert not (root / "blobs").exists()


def test_a_9_mib_body_stores_its_first_8_mib_and_says_it_was_cut(root, key):
    # Not UTF-8, so redaction stores it unscanned and the test does not time a regex.
    body = b"\xff" * (9 * 1024 * 1024)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", answer=body))
    s.close()
    (line,) = [json.loads(line) for line in (root / "runs/r1/events.jsonl").open()]
    ref = line["response"]["body"]
    assert (ref["size"], ref["truncated"]) == (store.MAX_STORED_BODY, True)
    assert (root / "blobs" / ref["sha256"]).stat().st_size == store.MAX_STORED_BODY
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert ex.response.body == body[: store.MAX_STORED_BODY]
    assert Flag.BODY_TRUNCATED in ex.flags


def test_a_body_is_redacted_whole_before_it_is_cut(root, key, monkeypatch):
    """A secret straddling the cut is never stored as half a secret."""
    monkeypatch.setattr(store, "MAX_STORED_BODY", 20)
    body = b"012345678 " + SECRET.encode()  # the cut at 20 falls inside the key
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", answer=body))
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert ex.response.body == (b"012345678 " + redact.placeholder(key, SECRET).encode())[:20]
    assert not any(b"sk_live_" in p.read_bytes() for p in root.rglob("*") if p.is_file())


def _streamed(answer: bytes, chunks: tuple[int, ...]) -> Exchange:
    return dataclasses.replace(_exchange("r1", answer=answer), stream_chunks=chunks)


def test_a_streamed_body_is_stored_with_its_chunk_lengths(root, key):
    s = DirectoryStore(root, key)
    s.record(_streamed(b"aaaa" + b"bbbbbb", (4, 6)))
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert (ex.response.body, ex.stream_chunks) == (b"aaaabbbbbb", (4, 6))


def test_a_streamed_body_cut_at_the_maximum_keeps_the_chunks_of_what_is_stored(
    root, key, monkeypatch
):
    """`_put_body` keeps the first MAX_STORED_BODY bytes, and the chunk lengths are cut at the
    same byte, so they still split the stored body."""
    monkeypatch.setattr(store, "MAX_STORED_BODY", 20)
    s = DirectoryStore(root, key)
    s.record(_streamed(b"a" * 8 + b"b" * 8 + b"c" * 14, (8, 8, 14)))
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert (ex.response.body, ex.stream_chunks) == (b"a" * 8 + b"b" * 8 + b"c" * 4, (8, 8, 4))
    assert Flag.BODY_TRUNCATED in ex.flags


def test_a_stream_redaction_grows_past_the_maximum_is_a_prefix_of_its_redacted_whole(
    root, key, monkeypatch
):
    """The engine keeps at most MAX_STORED_BODY bytes of a stream, but a placeholder can be longer
    than its secret, so the redacted body can pass the cap again. It is cut like any body: a
    prefix of the redacted whole, never half a secret, its chunks cut with it, and both flags -
    the stream was not whole, and neither is the stored body."""
    secret = "sk_live_A"  # far shorter than its placeholder
    line = b'data: {"text":"' + secret.encode() + b'"}\n'
    redacted_line = line.replace(secret.encode(), redact.placeholder(key, secret).encode())
    sent = line * 10
    cap = 6 * len(redacted_line) + len(b'data: {"text":"<redac')  # inside the 7th placeholder
    assert len(sent) <= cap < len(redacted_line) * 10
    monkeypatch.setattr(store, "MAX_STORED_BODY", cap)
    streamed = dataclasses.replace(
        _streamed(sent, (len(line),) * 10), flags=(exchange.Flag.STREAM_TRUNCATED,)
    )
    s = DirectoryStore(root, key)
    s.record(streamed)
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert ex.response.body == (redacted_line * 10)[:cap]
    assert ex.stream_chunks == (len(redacted_line),) * 6 + (cap - 6 * len(redacted_line),)
    assert set(ex.flags) == {exchange.Flag.STREAM_TRUNCATED, Flag.BODY_TRUNCATED}
    assert not any(b"sk_live_" in p.read_bytes() for p in root.rglob("*") if p.is_file())


def test_a_request_body_too_big_to_redact_leaves_the_streamed_response_s_chunks(
    root, key, monkeypatch
):
    monkeypatch.setattr(store, "MAX_REDACTED_BODY", 10)
    s = DirectoryStore(root, key)
    streamed = _streamed(b"aaaa" + b"bbbbbb", (4, 6))
    big = dataclasses.replace(streamed.request, body=b"x" * 11)
    s.record(dataclasses.replace(streamed, request=big))
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert (ex.request.body, ex.response.body, ex.stream_chunks) == (b"", b"aaaabbbbbb", (4, 6))
    assert Flag.BODY_TRUNCATED in ex.flags


def test_a_streamed_body_too_big_to_redact_is_stored_with_no_chunks(root, key, monkeypatch):
    monkeypatch.setattr(store, "MAX_REDACTED_BODY", 10)
    s = DirectoryStore(root, key)
    s.record(_streamed(b"x" * 11, (5, 6)))
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert (ex.response.body, ex.stream_chunks) == (b"", ())
    assert Flag.BODY_TRUNCATED in ex.flags


# ---------------------------------------------------------------------------------- the run ids


def test_an_invalid_run_id_lands_in_unattributed_flagged_and_names_no_directory(
    root, key, tmp_path
):
    s = DirectoryStore(root, key)
    s.record(_exchange("../x"))
    s.record(_exchange(trace.UNATTRIBUTED, "/no-run"))
    s.close()
    assert not (tmp_path / "x").exists() and not (root / "x").exists()
    assert sorted(p.name for p in root.iterdir()) == ["unattributed"]
    bad, none = StoreReader(root).load_run(trace.UNATTRIBUTED).events
    assert isinstance(bad, Exchange) and isinstance(none, Exchange)
    assert (bad.run_id, Flag.BAD_RUN_ID in bad.flags) == ("../x", True)
    # `unattributed` is the name of "no run", not a bad name.
    assert (none.run_id, none.flags) == (trace.UNATTRIBUTED, ())


def test_an_invalid_run_id_never_starts_or_ends_a_run_or_makes_a_directory(root, key):
    s = DirectoryStore(root, key)
    s.start_run(_record("../x"))
    s.start_run(_record(trace.UNATTRIBUTED))
    s.end_run("../y", 3.0, "ok")
    s.end_run(trace.UNATTRIBUTED, 3.0, "ok")
    s.close()
    assert s.stats().dropped == 4
    # Not `unattributed/` either: nothing was stored there, and the drops have no run.json.
    assert os.listdir(root) == []


def test_two_run_ids_that_differ_only_in_case_never_share_a_directory(root, key):
    """`Run1` and `run1` are both valid run ids. On a case-insensitive filesystem they are one
    directory, so the second to arrive is stored unattributed and flagged; on a case-sensitive one
    each is a run of its own. Either way no directory holds two runs' events."""
    s = DirectoryStore(root, key)
    s.record(_exchange("Run1", "/first"))
    s.record(_exchange("run1", "/second"))
    s.close()
    reader = StoreReader(root)
    shared = sorted(os.listdir(root / "runs")) == ["Run1"]
    assert _paths(reader.load_run("Run1")) == ["/first"]
    if shared:
        (clash,) = reader.load_run(trace.UNATTRIBUTED).events
        assert isinstance(clash, Exchange)
        assert (clash.run_id, Flag.BAD_RUN_ID in clash.flags) == ("run1", True)
    else:
        assert _paths(reader.load_run("run1")) == ["/second"]


# ---------------------------------------------------------------------------------- concurrency


def test_eight_threads_recording_across_four_runs_store_every_event_once_in_seq_order(root, key):
    s = DirectoryStore(root, key)
    runs = [f"run{i}" for i in range(4)]

    def work(t: int) -> None:
        for n in range(500):
            s.record(_exchange(runs[n % 4], f"/t{t}/n{n}"))

    threads = [threading.Thread(target=work, args=(t,)) for t in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    s.close()

    assert s.stats().dropped == 0
    reader = StoreReader(root)
    seen = []
    for run_id in runs:
        lines = [json.loads(line) for line in (root / "runs" / run_id / "events.jsonl").open()]
        assert [line["seq"] for line in lines] == list(range(1, len(lines) + 1))
        seen += _paths(reader.load_run(run_id))
    assert sorted(seen) == sorted(f"/t{t}/n{n}" for t in range(8) for n in range(500))


# ------------------------------------------------------------------------------ crash and resume


def test_a_store_abandoned_without_close_leaves_an_incomplete_run_that_still_reads(root, key):
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    for n in range(5):
        s.record(_exchange("r1", f"/n{n}"))
    _drain(s, 5)  # the writer has flushed; the process then "dies" without close()

    run = StoreReader(root).load_run("r1")
    assert run.record.end is None  # incomplete
    assert _paths(run) == [f"/n{n}" for n in range(5)]

    with open(root / "runs/r1/events.jsonl", "a") as f:
        f.write('{"seq": 6, "type": "exchange", "run_id": "r1", "servi')  # a crash mid-write
    assert _paths(StoreReader(root).load_run("r1")) == [f"/n{n}" for n in range(5)]


def test_a_restarted_store_resumes_seq_after_truncating_a_half_line(root, key):
    first = DirectoryStore(root, key)
    first.record(_exchange("r1", "/a"))
    first.record(_exchange("r1", "/b"))
    first.close()
    with open(root / "runs/r1/events.jsonl", "a") as f:
        f.write('{"seq": 3, "ty')

    second = DirectoryStore(root, key)
    second.record(_exchange("r1", "/c"))
    second.close()
    lines = [json.loads(line) for line in (root / "runs/r1/events.jsonl").open()]
    assert [line["seq"] for line in lines] == [1, 2, 3]
    assert _paths(StoreReader(root).load_run("r1")) == ["/a", "/b", "/c"]


def test_a_damaged_last_seq_does_not_stop_a_restarted_store_writing_the_run(root, key):
    """`{"seq": 1e400}` parses as infinity, which `int()` refuses with an OverflowError: resuming
    skips that line as unreadable rather than drop every later event of the run."""
    first = DirectoryStore(root, key)
    first.record(_exchange("r1", "/a"))
    first.close()
    with open(root / "runs/r1/events.jsonl", "a") as f:
        f.write('{"seq": true}\n{"seq": 1e400}\n')
    second = DirectoryStore(root, key)
    second.record(_exchange("r1", "/b"))
    second.close()
    assert second.stats().dropped == 0
    last = (root / "runs/r1/events.jsonl").read_text().splitlines()[-1]
    assert (json.loads(last)["seq"], json.loads(last)["request"]["path"]) == (2, "/b")


def test_resuming_from_the_tail_truncates_a_half_line_at_its_offset_in_the_file(
    root, key, monkeypatch
):
    """The tail holds the last whole line and a crash's half line after it: the cut is made at the
    tail's offset plus the end of that line, not at an offset counted from the tail's start."""
    first = DirectoryStore(root, key)
    for n in range(3):
        first.record(_exchange("r1", f"/n{n}"))
    first.close()
    path = root / "runs/r1/events.jsonl"
    whole = path.read_bytes()
    last = whole.splitlines(keepends=True)[-1]
    with open(path, "ab") as f:
        f.write(b'{"seq": 4, "ty')
    monkeypatch.setattr(store, "RESUME_TAIL", len(last) + 30)
    second = DirectoryStore(root, key)
    second.record(_exchange("r1", "/n3"))
    second.close()
    lines = [json.loads(line) for line in path.open()]
    assert [line["seq"] for line in lines] == [1, 2, 3, 4]
    assert path.read_bytes().startswith(whole)


def test_a_run_json_is_on_disk_whole_before_it_is_renamed_into_place(root, key, store_os):
    """A run.json a crash left empty would read as no version at all, and a restarted store never
    appends to a run whose version it cannot read: so it is synced before it replaces the last."""
    synced: set[int] = set()  # inodes
    renamed: list[bool] = []  # per run.json rename: was the file synced first?
    store_os.fsync = lambda fd: (synced.add(os.fstat(fd).st_ino), os.fsync(fd))[1]

    def replace(src, dst) -> None:
        if Path(dst).name == store.RUN_FILE:
            renamed.append(os.stat(src).st_ino in synced)
        os.replace(src, dst)

    store_os.replace = replace
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.record(_exchange("h1"))  # a header run's run.json
    s.end_run("r1", 3.0, "ok", exit_code=0)
    s.close()
    assert renamed == [True, True, True]


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a file of mode 000")
def test_a_run_json_that_cannot_be_read_for_now_does_not_refuse_the_run_for_good(root, key):
    """Too many open files, say: that event is dropped, and the next one is written. Only a
    run.json that holds another version, or no version at all, closes its run to this writer."""
    first = DirectoryStore(root, key)
    first.start_run(_record("r1"))
    first.close()
    run_json = root / "runs/r1/run.json"
    run_json.chmod(0)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/lost"))
    deadline = time.monotonic() + 10
    while s.stats().dropped < 1:
        assert time.monotonic() < deadline, s.stats()
        time.sleep(0.005)
    run_json.chmod(0o600)
    s.record(_exchange("r1", "/kept"))
    s.close()
    run = StoreReader(root).load_run("r1")
    assert (_paths(run), run.record.attribution, run.record.dropped_events) == (
        ["/kept"],
        "process",
        1,
    )


def test_a_run_of_a_newer_schema_version_is_never_appended_to(root, key):
    (root / "runs/old").mkdir(parents=True)
    newer = trace.run_to_json(_record("old")) | {"schema_version": trace.SCHEMA_VERSION + 1}
    (root / "runs/old/run.json").write_text(json.dumps(newer))
    (root / "runs/old/events.jsonl").write_text('{"seq": 1, "ty')  # its own crash's half line
    s = DirectoryStore(root, key)
    s.record(_exchange("old"))
    s.close()
    assert (root / "runs/old/events.jsonl").read_text() == '{"seq": 1, "ty'  # not even truncated
    assert json.loads((root / "runs/old/run.json").read_text()) == newer
    assert s.stats().dropped == 1


@pytest.mark.parametrize(
    "damage",
    [
        {"mode": "replay", "replay_of": "a  b"},
        {"mode": "replay"},
        {"replay_of": "4f1c9a2b7d3e8a60"},
    ],
    ids=["replay-of-no-run-id", "replay-naming-no-run", "shadow-naming-a-run"],
)
def test_a_run_whose_run_json_cannot_be_decoded_is_never_appended_to(root, key, damage):
    """A store reopened on a run whose run.json it cannot decode - such as one whose
    `replay_of` is no run id or disagrees with its `mode` - drops that run's events and its end,
    as it drops a run of another version's: its lines would sit under a record no reader reads,
    and its end would write a header record over the damage, a replay's or a shadow run's by
    whichever field it trusted. Fail closed: the run is left as it was found, and each event is
    counted dropped."""
    (root / "runs/old").mkdir(parents=True)
    damaged = trace.run_to_json(_record("old")) | damage
    (root / "runs/old/run.json").write_text(json.dumps(damaged))
    s = DirectoryStore(root, key)
    s.record(_exchange("old"))
    s.end_run("old", 20.0, "ok")
    s.close()
    assert not (root / "runs/old/events.jsonl").exists()
    assert json.loads((root / "runs/old/run.json").read_text()) == damaged
    assert s.stats().dropped == 2


def test_a_run_of_an_older_schema_version_is_never_appended_to(root, key, monkeypatch):
    """The other direction: this irimi is newer than the run it finds (docs/trace-format.md,
    "Versioning"). Appending would mix two versions' lines under one run.json."""
    first = DirectoryStore(root, key)
    first.record(_exchange("old", "/before"))
    first.close()
    monkeypatch.setattr(store, "SCHEMA_VERSION", trace.SCHEMA_VERSION + 1)
    second = DirectoryStore(root, key)
    second.record(_exchange("old", "/after"))
    second.close()
    assert second.stats().dropped == 1
    assert len((root / "runs/old/events.jsonl").read_text().splitlines()) == 1


# ------------------------------------------------------------------------ never raise, never block


def test_a_failed_blob_write_drops_that_event_and_the_next_one_is_written(root, key, monkeypatch):
    real = store.write_private
    failed = []

    def once(path: Path, data: bytes, **kwargs) -> None:
        if path.parent.name == store.BLOBS_DIR and not failed:
            failed.append(path)
            raise OSError("disk full")
        real(path, data, **kwargs)

    monkeypatch.setattr(store, "write_private", once)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/lost", answer=b"first"))
    s.record(_exchange("r1", "/kept", answer=b"second"))
    s.close()
    assert failed and s.stats().dropped == 1
    run = StoreReader(root).load_run("r1")
    assert _paths(run) == ["/kept"]
    assert run.record.dropped_events == 1


def test_a_run_id_that_is_not_a_string_is_a_drop_and_the_writer_goes_on(root, key, monkeypatch):
    """The writer counts a failure under the event's run id. One that cannot be a dict key raised
    in that error path and ended the writer thread, silently losing every later event."""
    monkeypatch.setattr(store, "CLOSE_TIMEOUT_S", 2.0)
    s = DirectoryStore(root, key)
    s.record(dataclasses.replace(_exchange("r1"), run_id=["not", "a", "string"]))  # type: ignore[arg-type]
    s.record(_exchange("r1", "/after"))
    s.close()
    assert s.stats().dropped == 1
    assert _paths(StoreReader(root).load_run("r1")) == ["/after"]


def test_a_call_with_the_wrong_type_is_a_drop_not_a_raise(root, key):
    """Every public method is a mitmproxy hook's callee or the SDK's, and a raised hook forwards
    the flow, so a write escapes shadow mode: even a caller breaking the types gets a counted
    drop, not an exception."""
    s = DirectoryStore(root, key)
    s.record(None)  # type: ignore[arg-type]
    s.record_tool_call(None)  # type: ignore[arg-type]
    s.start_run(None)  # type: ignore[arg-type]
    s.record(_exchange("r1", "/after"))
    s.close()
    assert s.stats().dropped == 3
    assert _paths(StoreReader(root).load_run("r1")) == ["/after"]


def test_a_failed_write_never_cuts_off_a_line_another_process_appended(root, key, store_os):
    """`unattributed/` is shared by every process on a root. A `write` that raised wrote nothing,
    so there is nothing of this line to cut, and truncating the file to its size before the write
    would cut off a line another process appended in between."""
    other = _exchange(trace.UNATTRIBUTED, "/other-process")
    line = json.dumps(trace.event_to_json(1, other, lambda body: None)) + "\n"
    raced: list[int] = []

    def write(fd, data):
        if not raced:
            raced.append(fd)
            with open(root / "unattributed/events.jsonl", "a") as f:  # the other process
                f.write(line)
            raise OSError(errno.ENOSPC, "No space left on device")
        return os.write(fd, data)

    store_os.write = write
    s = DirectoryStore(root, key)
    s.record(_exchange(trace.UNATTRIBUTED, "/lost"))
    s.record(_exchange(trace.UNATTRIBUTED, "/kept"))
    s.close()
    assert raced and s.stats().dropped == 1
    events = StoreReader(root).load_run(trace.UNATTRIBUTED).events
    assert _paths_of(events) == ["/other-process", "/kept"]


def test_a_value_json_cannot_hold_is_a_drop_not_a_line_that_is_not_json(root, key):
    s = DirectoryStore(root, key)
    s.record_tool_call(_tool_call("r1", {"ratio": math.nan}))
    s.record(_exchange("r1", "/after"))
    s.close()
    assert s.stats().dropped == 1
    lines = (root / "runs/r1/events.jsonl").read_text().splitlines()
    assert [json.loads(line)["seq"] for line in lines] == [1]
    assert StoreReader(root).load_run("r1").record.dropped_events == 1


def test_a_full_queue_drops_without_blocking_and_the_run_counts_it(root, key, monkeypatch):
    monkeypatch.setattr(store, "QUEUE_SIZE", 3)
    taken, release = threading.Event(), threading.Event()
    real = DirectoryStore._handle

    def paused(self, item):
        taken.set()
        release.wait(10)
        real(self, item)

    monkeypatch.setattr(DirectoryStore, "_handle", paused)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/n0"))
    assert taken.wait(5)  # the writer holds /n0 and waits
    for n in range(1, 4):
        s.record(_exchange("r1", f"/n{n}"))
    assert (s.stats().queued, s.stats().dropped) == (3, 0)
    started = time.perf_counter()
    s.record(_exchange("r1", "/overflow"))
    assert time.perf_counter() - started < 0.010
    assert s.stats().dropped == 1
    release.set()
    s.close()
    run = StoreReader(root).load_run("r1")
    assert _paths(run) == [f"/n{n}" for n in range(4)]
    assert run.record.dropped_events == 1


def test_close_gives_up_on_a_stuck_writer_and_counts_what_it_left(root, key, monkeypatch):
    monkeypatch.setattr(store, "CLOSE_TIMEOUT_S", 0.2)
    release = threading.Event()
    real = DirectoryStore._handle

    def stuck(self, item):
        if isinstance(item, store._Record) and item.event.request.path == "/stuck":
            release.wait(10)
        real(self, item)

    monkeypatch.setattr(DirectoryStore, "_handle", stuck)
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.record(_exchange("r1", "/stuck"))
    s.record(_exchange("r1", "/left"))
    s.record(_exchange("r1", "/also-left"))
    started = time.monotonic()
    s.close()
    assert time.monotonic() - started < 2
    assert s.stats().dropped == 2
    assert StoreReader(root).load_run("r1").record.dropped_events == 2
    release.set()
    # The write in flight still lands; wait for it, so no later test's patched `os` meets it.
    deadline = time.monotonic() + 10
    while s.stats().written < 1:
        assert time.monotonic() < deadline, s.stats()
        time.sleep(0.005)


def test_an_abandoned_writer_stops_once_its_write_in_flight_lands(root, key, monkeypatch):
    """`close()` drained the queue, the sentinel included, when it gave up on the writer. The
    writer's idle wake-up must see that and end, not poll an empty queue for the process's life."""
    monkeypatch.setattr(store, "CLOSE_TIMEOUT_S", 0.2)
    monkeypatch.setattr(store, "SYNC_IDLE_S", 0.01)
    release = threading.Event()
    real = DirectoryStore._handle

    def stuck(self, item):
        release.wait(10)
        real(self, item)

    monkeypatch.setattr(DirectoryStore, "_handle", stuck)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/stuck"))
    s.close()
    release.set()
    s._thread.join(5)
    assert not s._thread.is_alive()
    assert s.stats().written == 1


def test_a_second_close_waits_for_the_writer_as_the_first_does(root, key, monkeypatch):
    """The engine and `irimi shadow` both close the store; whichever returns second must still
    find the files whole, so a second close does not return while the first is waiting."""
    taken, release = threading.Event(), threading.Event()
    real = DirectoryStore._handle

    def paused(self, item):
        taken.set()
        release.wait(10)
        real(self, item)

    monkeypatch.setattr(DirectoryStore, "_handle", paused)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/slow"))
    assert taken.wait(5)
    first = threading.Thread(target=s.close)
    first.start()
    deadline = time.monotonic() + 10
    while not s._closed:  # the first close has begun
        assert time.monotonic() < deadline
        time.sleep(0.001)
    threading.Timer(0.2, release.set).start()
    s.close()
    assert _paths(StoreReader(root).load_run("r1")) == ["/slow"]
    first.join()


def test_close_is_idempotent_and_an_event_after_it_is_dropped_not_raised(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r1"))
    s.close()
    s.close()
    s.record(_exchange("r1", "/after-close"))
    s.end_run("r1", 3.0, "ok")
    assert s.stats().dropped == 2
    assert _paths(StoreReader(root).load_run("r1")) == ["/v1/things"]


def test_the_queue_is_bounded_in_body_bytes_too(root, key, monkeypatch):
    """10,000 items of 8 MiB would be 80 GiB waiting on a slow disk. A body that would pass the
    byte budget is dropped, but one reaching an idle writer's empty queue is always taken."""
    monkeypatch.setattr(store, "MAX_QUEUED_BYTES", 100)
    taken, release = threading.Event(), threading.Event()
    real = DirectoryStore._handle

    def paused(self, item):
        taken.set()
        release.wait(10)
        real(self, item)

    monkeypatch.setattr(DirectoryStore, "_handle", paused)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/held", answer=b"x" * 500))  # over budget, but the queue is empty
    assert taken.wait(5)
    s.record(_exchange("r1", "/small", answer=b"y" * 60))
    s.record(_exchange("r1", "/too-big", answer=b"z" * 60))  # 60 + 60 > 100
    s.record(_exchange("r1", "/no-body"))
    assert s.stats().dropped == 1
    release.set()
    s.close()
    run = StoreReader(root).load_run("r1")
    assert _paths(run) == ["/held", "/small", "/no-body"]
    assert run.record.dropped_events == 1


def test_a_tool_calls_args_and_result_count_against_the_byte_budget(root, key, monkeypatch):
    """The control endpoint takes a tool call of up to 2 MiB, and 10,000 of them would be 20 GiB
    waiting: the call as JSON, its args and result included, counts as body bytes do."""
    size = len(json.dumps(trace.tool_call_to_json(_tool_call("r1", {"x": "y" * 400}))))
    monkeypatch.setattr(store, "MAX_QUEUED_BYTES", size + size // 2)
    taken, release = threading.Event(), threading.Event()
    real = DirectoryStore._handle

    def paused(self, item):
        taken.set()
        release.wait(10)
        real(self, item)

    monkeypatch.setattr(DirectoryStore, "_handle", paused)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/held"))  # the writer holds it, so the queue is empty again
    assert taken.wait(5)
    s.record_tool_call(_tool_call("r1", {"x": "y" * 400}))
    s.record_tool_call(_tool_call("r1", {"x": "z" * 400}))  # size + size > the budget
    s.record(_exchange("r1", "/no-body"))
    assert s.stats().dropped == 1
    release.set()
    s.close()
    run = StoreReader(root).load_run("r1")
    assert [e.args for e in run.events if isinstance(e, ToolCall)] == [{"x": "y" * 400}]
    assert run.record.dropped_events == 1


def test_a_tool_calls_name_and_id_count_against_the_byte_budget(root, key, monkeypatch):
    """A posted tool call's name and id are the client's to say too, and either may be nearly all
    of a 2 MiB post, which counting its args and result alone let through uncounted: the
    whole call as JSON counts, as a start's whole record does."""
    named = dataclasses.replace(_tool_call("r1", None), name="n" * 400)
    size = len(json.dumps(trace.tool_call_to_json(named)))
    monkeypatch.setattr(store, "MAX_QUEUED_BYTES", size + size // 2)
    taken, release = threading.Event(), threading.Event()
    real = DirectoryStore._handle

    def paused(self, item):
        taken.set()
        release.wait(10)
        real(self, item)

    monkeypatch.setattr(DirectoryStore, "_handle", paused)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/held"))  # the writer holds it, so the queue is empty again
    assert taken.wait(5)
    s.record_tool_call(named)
    s.record_tool_call(dataclasses.replace(named, tool_call_id="i" * 400, name="look_up"))
    s.record(_exchange("r1", "/no-body"))
    assert s.stats().dropped == 1
    release.set()
    s.close()
    run = StoreReader(root).load_run("r1")
    assert [e.name for e in run.events if isinstance(e, ToolCall)] == ["n" * 400]
    assert run.record.dropped_events == 1


def test_a_runs_start_counts_against_the_byte_budget(root, key, monkeypatch):
    """The control endpoint takes a start of up to 2 MiB, its trigger's args, and 10,000 of them
    would be 20 GiB waiting: its record as JSON counts as body bytes do. A start dropped so
    leaves its run to be made from its first event, a `header` run that says it dropped one."""
    started = _record("r1", args={"x": "y" * 400})
    size = len(json.dumps(trace.run_to_json(started)))
    monkeypatch.setattr(store, "MAX_QUEUED_BYTES", size + size // 2)
    taken, release = threading.Event(), threading.Event()
    real = DirectoryStore._handle

    def paused(self, item):
        taken.set()
        release.wait(10)
        real(self, item)

    monkeypatch.setattr(DirectoryStore, "_handle", paused)
    s = DirectoryStore(root, key)
    s.record(_exchange("r0", "/held"))  # the writer holds it, so the queue is empty again
    assert taken.wait(5)
    s.start_run(started)
    s.start_run(_record("r2", args={"x": "z" * 400}))  # size + size > the budget
    s.record(_exchange("r2", "/no-body"))
    assert s.stats().dropped == 1
    release.set()
    s.close()
    reader = StoreReader(root)
    assert reader.load_run("r1").record == started
    late = reader.load_run("r2")
    assert late.record == dataclasses.replace(RunRecord.header("r2", 1.0), dropped_events=1)
    assert _paths(late) == ["/no-body"]


def test_an_end_waits_with_its_error_cut_outside_secrets_and_is_never_dropped_for_bytes(
    root, key, monkeypatch
):
    """A posted error may be 2 MiB, and the writer cuts it only once redacted. So it waits
    cut outside secrets to MAX_QUEUED_ERROR, an end costs the byte budget nothing, and no run
    loses its end to it: a key across that cut is left out whole, and the stored error is still
    a prefix of the redacted whole."""
    monkeypatch.setattr(store, "MAX_QUEUED_BYTES", 100)
    held: list[object] = []
    taken, release = threading.Event(), threading.Event()
    real = DirectoryStore._handle

    def paused(self, item):
        held.append(item)
        taken.set()
        release.wait(10)
        real(self, item)

    monkeypatch.setattr(DirectoryStore, "_handle", paused)
    secret = "ghp_" + "Q" * 36
    long = "m" * (store.MAX_QUEUED_ERROR - 10) + " " + secret + " " + "m" * 2_000_000
    s = DirectoryStore(root, key)
    s.record(_exchange("r0", "/held", answer=b"x" * 500))  # over budget, so nothing else fits
    assert taken.wait(5)
    s.end_run("r1", 3.0, "error", ErrorInfo("t." * 1_000_000, long), exit_code=1)
    assert s.stats().dropped == 0
    release.set()
    s.close()
    [queued] = [item for item in held if isinstance(item, store._EndRun)]
    assert queued.error == ErrorInfo(
        "t." * (store.MAX_QUEUED_ERROR // 2), "m" * (store.MAX_QUEUED_ERROR - 10) + " "
    )
    record = StoreReader(root).load_run("r1").record
    assert (record.end.outcome, record.end.error) == (
        "error",
        ErrorInfo("t." * (limits.MAX_ERROR_TYPE // 2), "m" * limits.MAX_ERROR_MESSAGE),
    )


def test_an_end_whose_error_is_not_text_is_a_drop_on_its_run(root, key):
    """An end's error is cut as it is queued, so one that is not text - from a caller
    other than the control endpoint - is dropped there, never raised into the caller, and
    counted on its run."""
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.end_run("r1", 3.0, "error", ErrorInfo("x.Boom", None), exit_code=1)  # type: ignore[arg-type]
    s.close()
    assert s.stats().dropped == 1
    run = StoreReader(root).load_run("r1")
    assert (run.record.end, run.record.dropped_events) == (None, 1)


def test_a_start_that_cannot_be_measured_is_a_drop_on_its_run(root, key):
    """A start is measured as JSON on the caller's thread, so one that is not JSON - args no
    decoder could have made, from a caller other than the control endpoint - is dropped there,
    never raised into the caller, and counted on its run, which its first event makes a `header`
    run, as every dropped start leaves it."""
    s = DirectoryStore(root, key)
    s.start_run(_record("r1", args={"x": {1, 2}}))
    s.record(_exchange("r1"))
    s.close()
    assert s.stats().dropped == 1
    run = StoreReader(root).load_run("r1")
    assert run.record == dataclasses.replace(RunRecord.header("r1", 1.0), dropped_events=1)
    assert _paths(run) == ["/v1/things"]


def test_the_writer_fsyncs_in_batches_before_close(root, key, monkeypatch, store_os):
    synced: set[int] = set()  # inodes
    monkeypatch.setattr(store, "SYNC_EVERY", 3)
    monkeypatch.setattr(store, "SYNC_IDLE_S", 60.0)  # only the batch syncs before close
    store_os.fsync = lambda fd: (synced.add(os.fstat(fd).st_ino), os.fsync(fd))[1]
    s = DirectoryStore(root, key)
    for n in range(4):  # events.jsonl and a blob each: past 3 on the second event
        s.record(_exchange("r1", f"/n{n}", answer=f"body {n}".encode()))
    _drain(s, 4)
    events = root / "runs/r1/events.jsonl"
    assert events.stat().st_ino in synced, "events.jsonl was not fsynced before close"
    s.close()


def test_one_long_run_is_fsynced_every_sync_every_writes_not_only_when_it_ends(
    root, key, monkeypatch, store_os
):
    """Counted in distinct files, one run appending to its one events.jsonl never reached a batch,
    and a week-long run was synced only when it ended."""
    synced: set[int] = set()  # inodes
    monkeypatch.setattr(store, "SYNC_EVERY", 3)
    monkeypatch.setattr(store, "SYNC_IDLE_S", 60.0)
    store_os.fsync = lambda fd: (synced.add(os.fstat(fd).st_ino), os.fsync(fd))[1]
    s = DirectoryStore(root, key)
    for n in range(4):
        s.record(_exchange("r1", f"/n{n}"))
    _drain(s, 4)
    assert (root / "runs/r1/events.jsonl").stat().st_ino in synced
    s.close()


def test_an_idle_writer_fsyncs_what_it_wrote(root, key, monkeypatch, store_os):
    """A `serve` that goes quiet does not leave its last lines unsynced until the next event."""
    synced: set[int] = set()  # inodes
    monkeypatch.setattr(store, "SYNC_IDLE_S", 0.01)
    store_os.fsync = lambda fd: (synced.add(os.fstat(fd).st_ino), os.fsync(fd))[1]
    s = DirectoryStore(root, key)
    s.record(_exchange("r1"))
    _drain(s, 1)
    events = (root / "runs/r1/events.jsonl").stat().st_ino
    deadline = time.monotonic() + 10
    while events not in synced:
        assert time.monotonic() < deadline, "the idle writer never synced events.jsonl"
        time.sleep(0.005)
    s.close()


def test_stored_files_are_private_to_their_owner(root, key):
    """Redaction hides credentials, not customers or amounts: the store is 0700 and 0600."""
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.record(_exchange("r1", answer=b"a customer's data"))
    s.record(_exchange("../x"))
    s.close()
    for path in [root, *root.rglob("*")]:
        mode = path.stat().st_mode & 0o777
        assert mode == (0o700 if path.is_dir() else 0o600), (path, oct(mode))


def test_two_stores_on_one_root_never_interleave_inside_a_line(root, key):
    """Two processes on the default root share `unattributed/events.jsonl`: each line is one
    O_APPEND write, so however they race every line parses. Lines far past a buffer's size."""
    big = (("x-note", "n" * 20_000),)
    stores = [DirectoryStore(root, key) for _ in range(2)]

    def work(s: DirectoryStore, t: int) -> None:
        for n in range(200):
            ex = _exchange("../x", f"/s{t}/n{n}")
            s.record(dataclasses.replace(ex, request=dataclasses.replace(ex.request, headers=big)))

    threads = [threading.Thread(target=work, args=(s, t)) for t, s in enumerate(stores)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for s in stores:
        s.close()
    events = StoreReader(root).load_run(trace.UNATTRIBUTED).events
    assert sorted(_paths_of(events)) == sorted(f"/s{t}/n{n}" for t in range(2) for n in range(200))


def test_resuming_a_long_run_reads_only_its_tail(root, key, monkeypatch):
    monkeypatch.setattr(store, "RESUME_TAIL", 64)  # shorter than one line: falls back to it all
    first = DirectoryStore(root, key)
    for n in range(3):
        first.record(_exchange("r1", f"/n{n}"))
    first.close()
    second = DirectoryStore(root, key)
    second.record(_exchange("r1", "/n3"))
    second.close()
    lines = [json.loads(line) for line in (root / "runs/r1/events.jsonl").open()]
    assert [line["seq"] for line in lines] == [1, 2, 3, 4]


def test_a_line_the_disk_refused_half_way_is_cut_off_and_the_run_still_reads(root, key, store_os):
    failed = []

    def half(fd, data):
        # What a full disk does: a short write, then ENOSPC on the next one. A `write` that
        # raises has written nothing.
        if len(data) > 40 and not failed:
            failed.append(fd)
            return os.write(fd, bytes(data[:40]))
        if failed == [fd]:
            failed.append(fd)
            raise OSError(errno.ENOSPC, "No space left on device")
        return os.write(fd, data)

    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/a"))
    _drain(s, 1)
    store_os.write = half
    s.record(_exchange("r1", "/lost"))
    s.record(_exchange("r1", "/b"))
    s.close()
    assert failed and s.stats().dropped == 1
    run = StoreReader(root).load_run("r1")
    assert _paths(run) == ["/a", "/b"]
    assert run.record.dropped_events == 1


def test_a_torn_blob_is_written_again_rather_than_trusted(root, key):
    body = b"a common body"
    blob = root / "blobs" / trace.body_ref(body).sha256
    blob.parent.mkdir(parents=True)
    blob.write_bytes(body[:3])  # a crash tore it before any fsync
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", answer=body))
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None and ex.response.body == body


def test_a_body_too_big_to_redact_is_not_stored_and_its_exchange_says_so(root, key, monkeypatch):
    monkeypatch.setattr(store, "MAX_REDACTED_BODY", 10)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", body=b"small", answer=b"x" * 11))
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert (ex.request.body, ex.response.body) == (b"small", b"")
    assert Flag.BODY_TRUNCATED in ex.flags


def test_an_ended_run_is_forgotten_and_a_late_event_reopens_it_from_disk(root, key, monkeypatch):
    """A long `serve` holds only its open runs. A late event after `end_run` reopens the run:
    `seq` continues, the outcome stays, and a drop after the end is added to the stored count -
    once, and to the drop before the end, never counting that one twice."""
    real = store.write_private

    def refuse_late_blob(path: Path, data: bytes, **kwargs) -> None:
        if data in (b"early body", b"late body"):
            raise OSError("disk full")
        real(path, data, **kwargs)

    monkeypatch.setattr(store, "write_private", refuse_late_blob)
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.record(_exchange("r1", "/a"))
    s.record(_exchange("r1", "/lost-early", answer=b"early body"))
    s.end_run("r1", 3.0, "ok", exit_code=0)
    s.record(_exchange("r1", "/lost", answer=b"late body"))
    s.record(_exchange("r1", "/late"))
    s.close()
    run = StoreReader(root).load_run("r1")
    assert (run.record.end.outcome, run.record.dropped_events) == ("ok", 2)
    assert _paths(run) == ["/a", "/late"]
    lines = [json.loads(line) for line in (root / "runs/r1/events.jsonl").open()]
    assert [line["seq"] for line in lines] == [1, 2]


# ----------------------------------------------------------------------------------- the reader


def test_list_runs_is_newest_first_with_unstarted_runs_last(root, key):
    s = DirectoryStore(root, key)
    s.start_run(_record("old", started_at=1.0))
    s.start_run(_record("new", started_at=2.0))
    s.start_run(_record("unstarted", started_at=None))
    # A header run whose first event had no clock has no start either.
    s.record(dataclasses.replace(_exchange("headless"), started_at=0.0, ended_at=0.0))
    s.close()
    assert [r.run_id for r in StoreReader(root).list_runs()] == [
        "new",
        "old",
        "headless",
        "unstarted",
    ]


def test_an_unknown_or_invalid_run_id_is_run_not_found(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r1"))
    s.close()
    reader = StoreReader(root)
    for run_id in ("nope", "../runs/r1", ""):
        with pytest.raises(RunNotFound):
            reader.load_run(run_id)
    assert reader.load_run(trace.UNATTRIBUTED).events == []


def test_a_damaged_line_that_is_not_the_last_is_a_trace_format_error(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/a"))
    s.record(_exchange("r1", "/b"))
    s.close()
    path = root / "runs/r1/events.jsonl"
    first, second = path.read_text().splitlines()
    path.write_text(first[:20] + "\n" + second + "\n")
    with pytest.raises(TraceFormatError):
        StoreReader(root).load_run("r1")


def test_every_refusal_names_its_file_and_its_line(root, key):
    """A line that parses but does not decode is named by its file and line, as one that does not
    parse is: in a run of thousands of events, `'seq' is a string` alone cannot be found. A missing
    blob is named by the line whose event refers to it, and a run.json's refusal by its file.
    """
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.record(_exchange("r1", "/a"))
    s.record(_exchange("r1", "/b", answer=b"a body"))
    s.close()
    events = root / "runs/r1/events.jsonl"
    first, second = events.read_text().splitlines()
    events.write_text(first + "\n" + second.replace('"seq": 2', '"seq": "2"') + "\n")
    reader = StoreReader(root)
    with pytest.raises(TraceFormatError) as refused:
        reader.load_run("r1")
    assert str(refused.value) == f"{events}, line 2: 'seq' is a string, not an integer"
    events.write_text(first + "\n" + second + "\n")
    blob = root / "blobs" / trace.body_ref(b"a body").sha256
    blob.unlink()
    for bodies in (True, False):
        with pytest.raises(TraceFormatError) as refused:
            reader.load_run("r1", bodies=bodies)
        assert str(refused.value).startswith(f"{events}, line 2: blob {blob.name} is missing: ")
    record = root / "runs/r1/run.json"
    record.write_text(
        record.read_text().replace('"engine_version": "0.0.0"', '"engine_version": 0')
    )
    with pytest.raises(TraceFormatError) as refused:
        reader.load_run("r1")
    assert str(refused.value) == f"{record}: 'engine_version' is an integer, not a string"


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a directory whatever its mode")
def test_a_run_irimi_may_not_read_is_an_os_error_not_a_missing_run(root, key, caplog):
    """`Path.is_file` reads a run.json it may not stat as none, so a `chmod 0` run was `no run X`
    and vanished from `list_runs` without a word, and `Path.exists` read unattributed events it
    may not read as none. Each is an OSError, which `runs show` and `compare` print as
    `could not be read`, and `list_runs` skips the run with a warning, as it skips a damaged one."""
    s = DirectoryStore(root, key)
    s.start_run(_record("good"))
    s.start_run(_record("shut"))
    s.record(_exchange("not a run id"))
    s.close()
    shut, unattributed = root / "runs/shut", root / trace.UNATTRIBUTED
    reader = StoreReader(root)
    assert len(reader.load_run(trace.UNATTRIBUTED).events) == 1
    # The run's directory, and then its run.json alone.
    for closed in (shut, shut / "run.json"):
        closed.chmod(0)
        unattributed.chmod(0)
        caplog.clear()
        try:
            with caplog.at_level(logging.WARNING, logger="irimi.store"):
                assert [r.run_id for r in reader.list_runs()] == ["good"]
            assert caplog.messages == [
                f"irimi: skipping stored run shut: [Errno 13] Permission denied: '{shut}/run.json'"
            ]
            for run_id in ("shut", trace.UNATTRIBUTED):
                with pytest.raises(PermissionError):
                    reader.load_run(run_id)
        finally:
            closed.chmod(0o700 if closed == shut else 0o600)
            unattributed.chmod(0o700)
    # Still no run: no directory, a file where the directory would be, a run.json that is no file.
    (root / "runs/a-file").write_text("")
    (root / "runs/hollow/run.json").mkdir(parents=True)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="irimi.store"):
        assert [r.run_id for r in reader.list_runs()] == ["good", "shut"]
    assert caplog.messages == []
    for run_id in ("gone", "a-file", "hollow"):
        with pytest.raises(RunNotFound):
            reader.load_run(run_id)
    # The store itself: one with no `runs/` holds no run, and one irimi may not enter is an
    # OSError, where `Path.is_dir` read it as none, and `runs list` said `no runs in` it.
    assert StoreReader(root / "nowhere").list_runs() == []
    root.chmod(0)
    try:
        with pytest.raises(PermissionError):
            reader.list_runs()
    finally:
        root.chmod(0o700)


def test_a_run_id_is_never_answered_by_its_other_spelling(root, key):
    """On a case-insensitive filesystem `run1` opens `Run1/`: that is another run."""
    s = DirectoryStore(root, key)
    s.record(_exchange("Run1"))
    s.close()
    with pytest.raises(RunNotFound):
        StoreReader(root).load_run("run1")


def test_a_run_json_that_is_not_utf8_is_skipped_by_list_runs_not_raised(root, key):
    s = DirectoryStore(root, key)
    s.start_run(_record("good"))
    s.record(_exchange("bad"))
    s.close()
    (root / "runs/bad/run.json").write_bytes(b"\xff\xfe not utf-8")
    assert [r.run_id for r in StoreReader(root).list_runs()] == ["good"]
    with pytest.raises(TraceFormatError):
        StoreReader(root).load_run("bad")


def test_list_runs_skips_a_run_json_that_names_another_run(root, key):
    """`load_run` answers only the exact id its run.json names, so `list_runs` never lists a run
    that `load_run` would call not found."""
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.close()
    (root / "runs/copy").mkdir()
    (root / "runs/copy/run.json").write_bytes((root / "runs/r1/run.json").read_bytes())
    assert [r.run_id for r in StoreReader(root).list_runs()] == ["r1"]


def test_json_nested_past_the_parsers_depth_is_damage_not_a_recursion_error(root, key):
    s = DirectoryStore(root, key)
    s.start_run(_record("good"))
    s.record(_exchange("deep", "/a"))
    s.record(_exchange("deep", "/b"))
    s.close()
    deep = "[" * 1_000_000 + "]" * 1_000_000
    (root / "runs/good/events.jsonl").write_text(deep + "\n" + deep + "\n")
    (root / "runs/deep/run.json").write_text(deep)
    reader = StoreReader(root)
    assert [r.run_id for r in reader.list_runs()] == ["good"]
    for run_id in ("good", "deep"):
        with pytest.raises(TraceFormatError):
            reader.load_run(run_id)


def test_a_missing_blob_is_a_trace_format_error(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", answer=b"a body"))
    s.close()
    (root / "blobs" / trace.body_ref(b"a body").sha256).unlink()
    with pytest.raises(TraceFormatError, match="missing"):
        StoreReader(root).load_run("r1")
