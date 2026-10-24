"""The trace store on disk: `DirectoryStore` writes, `StoreReader` reads back.

Tests use only public methods and the files; `tests/e2e/test_trace_roundtrip.py` runs it for real.
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
from typing import cast

import pytest

from irimi.config import files
from irimi.core import exchange, limits
from irimi.core.exchange import Exchange, Flag, Response
from irimi.core.limits import MAX_QUEUED_ERROR
from irimi.protocol import trace
from irimi.protocol.trace import (
    ErrorInfo,
    RunRecord,
    TelemetrySeen,
    ToolCall,
    TraceFormatError,
    Trigger,
)
from irimi.record import redact
from irimi.record.store import StoreStats, layout, writer
from irimi.record.store.reader import RunNotFound, StoreReader
from irimi.record.store.writer import DirectoryStore
from tests.support import builders
from tests.support.credentials import STORE_SECRET, STORE_SECRET_SHORT
from tests.support.doubles import NullStore
from tests.support.waiting import eventually

SECRET = STORE_SECRET


@pytest.fixture
def key(tmp_path) -> bytes:
    return redact.load_key(tmp_path / "home")


@pytest.fixture
def root(tmp_path) -> Path:
    return tmp_path / "store"


def _exchange(
    run_id: str, path: str = "/v1/things", *, body: bytes = b"", answer: bytes = b"", kind="read"
) -> Exchange:
    """A POST to a service no map names, answered live with `answer` as raw bytes."""
    return builders.exchange(
        request=builders.request(method="POST", host="api.example.com", path=path, body=body),
        response=builders.response(
            headers=(("content-type", "application/octet-stream"),), body=answer
        ),
        service="example",
        operation="things.create",
        kind=kind,
        run_id=run_id,
        started_at=1.0,
        ended_at=2.0,
    )


def _record(run_id: str, started_at: float | None = 10.0, args=None) -> RunRecord:
    trigger = Trigger(name="agent", entrypoint=None, args=args, replayable=True)
    return builders.run_record(run_id, started_at, trigger=trigger)


def _replays(record: RunRecord, replay_of: str) -> RunRecord:
    """`record` as a replay of `replay_of` builds it: `mode` set from `replay_of`."""
    return dataclasses.replace(record, mode="replay", replay_of=replay_of)


def _tool_call(run_id: str, args) -> ToolCall:
    return builders.tool_call(tool_call_id="t1", run_id=run_id, name="db.write", args=args)


class _StoreOs:
    """The `os` module as the store's modules see it, with the functions a test sets replaced.

    Patching `os` itself would change it for every thread in the process, pytest's included."""

    def __getattr__(self, name: str):
        return getattr(os, name)


@pytest.fixture
def store_os(monkeypatch) -> _StoreOs:
    proxy = _StoreOs()
    monkeypatch.setattr(writer, "os", proxy)
    monkeypatch.setattr(layout, "os", proxy)
    monkeypatch.setattr(files, "os", proxy)  # the store writes its files through `files`
    return proxy


def _drain(s: DirectoryStore, written: int) -> None:
    """Wait until the writer has written `written` events and has none queued."""
    eventually(
        lambda: s.stats().written >= written and not s.stats().queued,
        interval=0.005,
        message="the writer never drained its queue",
    )


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
    assert [
        json.loads(line)["seq"] for line in (root / "runs/r1/events.jsonl").read_text().splitlines()
    ] == [1, 2]


def test_the_first_event_of_a_run_with_no_start_creates_a_header_run(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r2"))
    s.close()
    record = StoreReader(root).load_run("r2").record
    # Started when its first event did, so it sorts among the runs that have a start.
    assert record == RunRecord.header("r2", started_at=1.0)
    assert (record.attribution, record.trigger, record.end) == ("header", None, None)


def test_a_start_for_a_header_run_rewrites_its_record(root, key):
    """A late start rewrites a `header` run's record whole and keeps its events."""
    s = DirectoryStore(root, key)
    s.record(_exchange("r2"))
    s.start_run(dataclasses.replace(_record("r2"), attribution="sdk"))
    s.close()
    run = StoreReader(root).load_run("r2")
    assert run.record == dataclasses.replace(_record("r2"), attribution="sdk")
    assert _paths(run) == ["/v1/things"]
    assert s.stats().dropped == 0


def test_a_start_for_a_run_that_has_ended_is_dropped_and_the_run_kept_whole(root, key):
    """A start posted after its run ended is dropped, counted on the store, and changes nothing.

    The run keeps its end, outcome and first trigger, and its `dropped_events` stays 0, even when
    it ended in an earlier process."""
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
    """A replay's store drops a start that does not name the replay, process run or SDK run alike.

    The drop is counted on the store alone, and makes no run directory."""
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
    """Each header run a replay's store makes is `mode: replay` with the replayed run's id."""
    s = DirectoryStore(root, key, replay_of="rec1")
    s.record(_exchange("r2"))
    s.close()
    record = StoreReader(root).load_run("r2").record
    assert (record.mode, record.replay_of, record.attribution) == ("replay", "rec1", "header")
    assert record == RunRecord.header("r2", started_at=1.0, replay_of="rec1")


def test_a_replay_never_writes_into_the_run_it_replays(root, key):
    """A replay's events naming the run it replays go to `unattributed/`, flagged.

    An agent that names its own runs names the recorded one again on replay; appending would change
    the recording. The start and end are dropped."""
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
    """A replay's store writes only the runs it makes; every stored run is kept byte for byte.

    What the replay says of another stored run goes to `unattributed/`, flagged. A run it starts or
    makes from a header stays its own after it ends. A store that is no replay's resumes a run."""
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
    assert [cast(Exchange, e).request.path for e in resumed.events] == ["/recorded", "/resumed"]
    assert (resumed.record.mode, resumed.record.replay_of) == ("shadow", None)


def test_a_header_run_started_by_an_event_with_no_clock_has_no_start(root, key):
    s = DirectoryStore(root, key)
    s.record(dataclasses.replace(_exchange("r2"), started_at=0.0, ended_at=0.0))
    s.close()
    assert StoreReader(root).load_run("r2").record == RunRecord.header("r2")


def test_a_header_run_sorts_by_its_first_event_among_runs_that_started(root, key):
    """A header run sorts by its first event among runs that started, not after all of them."""
    s = DirectoryStore(root, key)
    for i in range(3):
        s.start_run(_record(f"p{i}", started_at=10.0 + i))
    s.record(dataclasses.replace(_exchange("labelled"), started_at=11.5, ended_at=11.6))
    s.close()
    assert [r.run_id for r in StoreReader(root).list_runs()] == ["p2", "labelled", "p1", "p0"]


def test_load_run_without_bodies_reads_no_blob_and_still_finds_a_missing_one(
    root, key, monkeypatch
):
    """`bodies=False` opens no blob, but a missing or resized one is still damage."""
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
    assert run.record.end is not None
    assert (run.record.end.outcome, _paths(run)) == ("ok", ["/late"])


def test_a_tool_calls_args_and_result_are_redacted_and_share_the_runs_seq(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/a"))
    s.record_tool_call(_tool_call("r1", {"api_key": "plain", "note": SECRET}))
    s.close()
    lines = [json.loads(line) for line in (root / "runs/r1/events.jsonl").read_text().splitlines()]
    assert [(line["seq"], line["type"]) for line in lines] == [(1, "exchange"), (2, "tool_call")]
    (_, call) = StoreReader(root).load_run("r1").events
    assert isinstance(call, ToolCall)
    assert call.args == {
        "api_key": redact.placeholder(key, "plain"),
        "note": redact.placeholder(key, SECRET),
    }


def test_an_error_and_a_trigger_name_are_redacted_before_they_reach_disk(root, key):
    """An error's type and message, and a trigger's name, are redacted before they reach disk.

    An exception's message may quote the key it refused; a posted error's type is the client's."""
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
    assert run.record.end is not None
    assert run.record.end.error == ErrorInfo("x.Err", f"Invalid API key: {hidden}")
    (stored,) = run.events
    assert isinstance(stored, ToolCall)
    assert stored.error == ErrorInfo(f"x.{hidden}", f"refused {hidden}")
    assert not any(SECRET.encode() in p.read_bytes() for p in root.rglob("*") if p.is_file())


def test_an_error_is_redacted_whole_and_only_then_cut(root, key):
    """An error is redacted whole and then cut, so a key across either limit leaves no part.

    Cut first, each kept part would be too short for redaction to find.
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
    assert run.record.end is not None
    assert (run.record.end.error, stored.error) == (cut, cut)
    assert not any(b"ghp_" in p.read_bytes() for p in root.rglob("*") if p.is_file())


def test_a_run_json_says_the_version_its_lines_are_written_in(root, key):
    """A run.json says the version its lines are written in, whatever the caller's record claims."""
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
    (line,) = [
        json.loads(line) for line in (root / "runs/r1/events.jsonl").read_text().splitlines()
    ]
    assert (line["request"]["body"], line["response"]["body"]) == (None, None)
    assert not (root / "blobs").exists()


def test_a_9_mib_body_stores_its_first_8_mib_and_says_it_was_cut(root, key):
    # Not UTF-8, so redaction stores it unscanned and the test does not time a regex.
    body = b"\xff" * (9 * 1024 * 1024)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", answer=body))
    s.close()
    (line,) = [
        json.loads(line) for line in (root / "runs/r1/events.jsonl").read_text().splitlines()
    ]
    ref = line["response"]["body"]
    assert (ref["size"], ref["truncated"]) == (limits.MAX_STORED_BODY, True)
    assert (root / "blobs" / ref["sha256"]).stat().st_size == limits.MAX_STORED_BODY
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert ex.response.body == body[: limits.MAX_STORED_BODY]
    assert Flag.BODY_TRUNCATED in ex.flags


def test_a_body_is_redacted_whole_before_it_is_cut(root, key, monkeypatch):
    """A secret straddling the cut is never stored as half a secret."""
    monkeypatch.setattr(writer, "MAX_STORED_BODY", 20)
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
    """A streamed body cut at MAX_STORED_BODY has its chunk lengths cut at the same byte."""
    monkeypatch.setattr(writer, "MAX_STORED_BODY", 20)
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
    """A stream that redaction grows past the cap is cut like any body, its chunks cut with it.

    A placeholder can be longer than its secret. Both flags are set: neither stream nor body is
    whole."""
    secret = STORE_SECRET_SHORT  # far shorter than its placeholder
    line = b'data: {"text":"' + secret.encode() + b'"}\n'
    redacted_line = line.replace(secret.encode(), redact.placeholder(key, secret).encode())
    sent = line * 10
    cap = 6 * len(redacted_line) + len(b'data: {"text":"<redac')  # inside the 7th placeholder
    assert len(sent) <= cap < len(redacted_line) * 10
    monkeypatch.setattr(writer, "MAX_STORED_BODY", cap)
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
    monkeypatch.setattr(writer, "MAX_REDACTED_BODY", 10)
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
    monkeypatch.setattr(writer, "MAX_REDACTED_BODY", 10)
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
    """Run ids differing only in case never share a directory, even on a case-insensitive disk.

    There the second is stored unattributed and flagged."""
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
        lines = [
            json.loads(line)
            for line in (root / "runs" / run_id / "events.jsonl").read_text().splitlines()
        ]
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
    lines = [json.loads(line) for line in (root / "runs/r1/events.jsonl").read_text().splitlines()]
    assert [line["seq"] for line in lines] == [1, 2, 3]
    assert _paths(StoreReader(root).load_run("r1")) == ["/a", "/b", "/c"]


def test_a_damaged_last_seq_does_not_stop_a_restarted_store_writing_the_run(root, key):
    """Resuming skips a last line whose `seq` is infinity, rather than drop later events."""
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
    """A crash's half line is cut at its offset in the file, not at one counted from the tail."""
    first = DirectoryStore(root, key)
    for n in range(3):
        first.record(_exchange("r1", f"/n{n}"))
    first.close()
    path = root / "runs/r1/events.jsonl"
    whole = path.read_bytes()
    last = whole.splitlines(keepends=True)[-1]
    with open(path, "ab") as f:
        f.write(b'{"seq": 4, "ty')
    monkeypatch.setattr(layout, "RESUME_TAIL", len(last) + 30)
    second = DirectoryStore(root, key)
    second.record(_exchange("r1", "/n3"))
    second.close()
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    assert [line["seq"] for line in lines] == [1, 2, 3, 4]
    assert path.read_bytes().startswith(whole)


def test_a_run_json_is_on_disk_whole_before_it_is_renamed_into_place(root, key, store_os):
    """A run.json is synced before it replaces the last, so a crash never leaves it empty."""
    synced: set[int] = set()  # inodes
    renamed: list[bool] = []  # per run.json rename: was the file synced first?

    def fsync(fd) -> None:
        synced.add(os.fstat(fd).st_ino)
        os.fsync(fd)

    store_os.fsync = fsync

    def replace(src, dst) -> None:
        if Path(dst).name == layout.RUN_FILE:
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
    """A run.json that cannot be read for now (too many open files) drops only that event.

    Only a run.json of another version, or none, closes its run to this writer."""
    first = DirectoryStore(root, key)
    first.start_run(_record("r1"))
    first.close()
    run_json = root / "runs/r1/run.json"
    run_json.chmod(0)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/lost"))
    eventually(lambda: s.stats().dropped >= 1, interval=0.005, message="nothing was dropped")
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
    """A run whose run.json cannot be decoded is left as found, its events and end dropped.

    Its lines would sit under a record no reader reads, and its end would overwrite the damage."""
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
    """A run of an older schema is never appended to, which would mix two versions' lines."""
    first = DirectoryStore(root, key)
    first.record(_exchange("old", "/before"))
    first.close()
    monkeypatch.setattr(writer, "SCHEMA_VERSION", trace.SCHEMA_VERSION + 1)
    second = DirectoryStore(root, key)
    second.record(_exchange("old", "/after"))
    second.close()
    assert second.stats().dropped == 1
    assert len((root / "runs/old/events.jsonl").read_text().splitlines()) == 1


# ------------------------------------------------------------------------ never raise, never block


def test_a_failed_blob_write_drops_that_event_and_the_next_one_is_written(root, key, monkeypatch):
    real = files.write_private
    failed: list[Path] = []

    def once(path: Path, data: bytes, **kwargs) -> None:
        if path.parent.name == layout.BLOBS_DIR and not failed:
            failed.append(path)
            raise OSError("disk full")
        real(path, data, **kwargs)

    monkeypatch.setattr(writer, "write_private", once)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/lost", answer=b"first"))
    s.record(_exchange("r1", "/kept", answer=b"second"))
    s.close()
    assert failed and s.stats().dropped == 1
    run = StoreReader(root).load_run("r1")
    assert _paths(run) == ["/kept"]
    assert run.record.dropped_events == 1


def test_a_run_id_that_is_not_a_string_is_a_drop_and_the_writer_goes_on(root, key, monkeypatch):
    """An event whose run id cannot be a dict key is a counted drop, and the writer goes on."""
    monkeypatch.setattr(writer, "CLOSE_TIMEOUT_S", 2.0)
    s = DirectoryStore(root, key)
    s.record(dataclasses.replace(_exchange("r1"), run_id=["not", "a", "string"]))  # type: ignore[arg-type]
    s.record(_exchange("r1", "/after"))
    s.close()
    assert s.stats().dropped == 1
    assert _paths(StoreReader(root).load_run("r1")) == ["/after"]


def test_a_call_with_the_wrong_type_is_a_drop_not_a_raise(root, key):
    """A call with the wrong types is a counted drop, not a raise (fail-closed hook rule)."""
    s = DirectoryStore(root, key)
    s.record(None)  # type: ignore[arg-type]
    s.record_tool_call(None)  # type: ignore[arg-type]
    s.start_run(None)  # type: ignore[arg-type]
    s.record(_exchange("r1", "/after"))
    s.close()
    assert s.stats().dropped == 3
    assert _paths(StoreReader(root).load_run("r1")) == ["/after"]


def test_a_failed_write_never_cuts_off_a_line_another_process_appended(root, key, store_os):
    """A failed write never truncates the shared `unattributed/` file another process appends to.

    A `write` that raised wrote nothing, so there is nothing of this line to cut."""
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
    monkeypatch.setattr(writer, "QUEUE_SIZE", 3)
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
    monkeypatch.setattr(writer, "CLOSE_TIMEOUT_S", 0.2)
    release = threading.Event()
    real = DirectoryStore._handle

    def stuck(self, item):
        if (
            isinstance(item, writer._QueuedEvent)
            and cast(Exchange, item.event).request.path == "/stuck"
        ):
            release.wait(30)
        real(self, item)

    monkeypatch.setattr(DirectoryStore, "_handle", stuck)
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.record(_exchange("r1", "/stuck"))
    s.record(_exchange("r1", "/left"))
    s.record(_exchange("r1", "/also-left"))
    started = time.monotonic()
    s.close()
    assert time.monotonic() - started < 10, "close waited for the stuck write"
    assert s.stats().dropped == 2
    assert StoreReader(root).load_run("r1").record.dropped_events == 2
    release.set()
    # The write in flight still lands; wait for it, so no later test's patched `os` meets it.
    eventually(lambda: s.stats().written >= 1, interval=0.005, message="nothing was written")


def test_an_abandoned_writer_stops_once_its_write_in_flight_lands(root, key, monkeypatch):
    """A writer `close()` gave up on ends once its write in flight lands, not polling forever."""
    monkeypatch.setattr(writer, "CLOSE_TIMEOUT_S", 0.2)
    monkeypatch.setattr(writer, "SYNC_IDLE_S", 0.01)
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
    """A second `close()` waits for the writer as the first does, so both find the files whole."""
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
    eventually(lambda: s._closed, interval=0.001)  # the first close has begun
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
    """A body that would pass the queue's byte budget is dropped, unless the queue is empty."""
    monkeypatch.setattr(writer, "MAX_QUEUED_BYTES", 100)
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
    """A tool call counts against the byte budget as JSON, its args and result included."""
    size = len(json.dumps(trace.tool_call_to_json(_tool_call("r1", {"x": "y" * 400}))))
    monkeypatch.setattr(writer, "MAX_QUEUED_BYTES", size + size // 2)
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
    """A tool call's name and id count against the byte budget too: either may be 2 MiB."""
    named = dataclasses.replace(_tool_call("r1", None), name="n" * 400)
    size = len(json.dumps(trace.tool_call_to_json(named)))
    monkeypatch.setattr(writer, "MAX_QUEUED_BYTES", size + size // 2)
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
    """A run's start counts against the byte budget as JSON.

    A start dropped for it leaves a `header` run that says it dropped one."""
    started = _record("r1", args={"x": "y" * 400})
    size = len(json.dumps(trace.run_to_json(started)))
    monkeypatch.setattr(writer, "MAX_QUEUED_BYTES", size + size // 2)
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
    """An end waits with its error cut outside secrets to MAX_QUEUED_ERROR, and is never dropped.

    An end costs the byte budget nothing, a key across the cut waits as its placeholder, and the
    stored error is a prefix of the redacted whole."""
    monkeypatch.setattr(writer, "MAX_QUEUED_BYTES", 100)
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
    long = "m" * (limits.MAX_QUEUED_ERROR - 10) + " " + secret + " " + "m" * 2_000_000
    s = DirectoryStore(root, key)
    s.record(_exchange("r0", "/held", answer=b"x" * 500))  # over budget, so nothing else fits
    assert taken.wait(5)
    s.end_run("r1", 3.0, "error", ErrorInfo("t." * 1_000_000, long), exit_code=1)
    assert s.stats().dropped == 0
    release.set()
    s.close()
    [queued] = [item for item in held if isinstance(item, writer._EndRun)]
    assert queued.error == ErrorInfo(
        "t." * (limits.MAX_QUEUED_ERROR // 2),
        "m" * (limits.MAX_QUEUED_ERROR - 10) + " " + redact.placeholder(key, secret),
    )
    record = StoreReader(root).load_run("r1").record
    assert record.end is not None
    assert (record.end.outcome, record.end.error) == (
        "error",
        ErrorInfo("t." * (limits.MAX_ERROR_TYPE // 2), "m" * limits.MAX_ERROR_MESSAGE),
    )


def test_an_end_whose_error_is_not_text_is_a_drop_on_its_run(root, key):
    """An end whose error is not text is dropped as it is queued and counted on its run."""
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.end_run("r1", 3.0, "error", ErrorInfo("x.Boom", None), exit_code=1)  # type: ignore[arg-type]
    s.close()
    assert s.stats().dropped == 1
    run = StoreReader(root).load_run("r1")
    assert (run.record.end, run.record.dropped_events) == (None, 1)


def test_a_start_that_cannot_be_measured_is_a_drop_on_its_run(root, key):
    """A start that is not JSON is dropped on the caller's thread, never raised, and counted.

    Its first event makes the run a `header` run, as every dropped start leaves it."""
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
    monkeypatch.setattr(writer, "SYNC_EVERY", 3)
    monkeypatch.setattr(writer, "SYNC_IDLE_S", 60.0)  # only the batch syncs before close

    def fsync(fd) -> None:
        synced.add(os.fstat(fd).st_ino)
        os.fsync(fd)

    store_os.fsync = fsync
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
    """One long run is fsynced every SYNC_EVERY writes, not only when it ends."""
    synced: set[int] = set()  # inodes
    monkeypatch.setattr(writer, "SYNC_EVERY", 3)
    monkeypatch.setattr(writer, "SYNC_IDLE_S", 60.0)

    def fsync(fd) -> None:
        synced.add(os.fstat(fd).st_ino)
        os.fsync(fd)

    store_os.fsync = fsync
    s = DirectoryStore(root, key)
    for n in range(4):
        s.record(_exchange("r1", f"/n{n}"))
    _drain(s, 4)
    assert (root / "runs/r1/events.jsonl").stat().st_ino in synced
    s.close()


def test_an_idle_writer_fsyncs_what_it_wrote(root, key, monkeypatch, store_os):
    """A `serve` that goes quiet does not leave its last lines unsynced until the next event."""
    synced: set[int] = set()  # inodes
    monkeypatch.setattr(writer, "SYNC_IDLE_S", 0.01)

    def fsync(fd) -> None:
        synced.add(os.fstat(fd).st_ino)
        os.fsync(fd)

    store_os.fsync = fsync
    s = DirectoryStore(root, key)
    s.record(_exchange("r1"))
    _drain(s, 1)
    events = (root / "runs/r1/events.jsonl").stat().st_ino
    eventually(
        lambda: events in synced,
        interval=0.005,
        message="the idle writer never synced events.jsonl",
    )
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
    """Two stores on one root never interleave inside a line, even lines far past a buffer.

    Each line in the shared `unattributed/events.jsonl` is one O_APPEND write."""
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
    monkeypatch.setattr(layout, "RESUME_TAIL", 64)  # shorter than one line: falls back to it all
    first = DirectoryStore(root, key)
    for n in range(3):
        first.record(_exchange("r1", f"/n{n}"))
    first.close()
    second = DirectoryStore(root, key)
    second.record(_exchange("r1", "/n3"))
    second.close()
    lines = [json.loads(line) for line in (root / "runs/r1/events.jsonl").read_text().splitlines()]
    assert [line["seq"] for line in lines] == [1, 2, 3, 4]


def test_a_line_the_disk_refused_half_way_is_cut_off_and_the_run_still_reads(root, key, store_os):
    failed: list[int] = []

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
    monkeypatch.setattr(writer, "MAX_REDACTED_BODY", 10)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", body=b"small", answer=b"x" * 11))
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert (ex.request.body, ex.response.body) == (b"small", b"")
    assert Flag.BODY_TRUNCATED in ex.flags


def test_an_ended_run_is_forgotten_and_a_late_event_reopens_it_from_disk(root, key, monkeypatch):
    """An ended run is forgotten, and a late event reopens it from disk.

    `seq` continues, the outcome stays, and a later drop is added to the stored count once."""
    real = files.write_private

    def refuse_late_blob(path: Path, data: bytes, **kwargs) -> None:
        if data in (b"early body", b"late body"):
            raise OSError("disk full")
        real(path, data, **kwargs)

    monkeypatch.setattr(writer, "write_private", refuse_late_blob)
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.record(_exchange("r1", "/a"))
    s.record(_exchange("r1", "/lost-early", answer=b"early body"))
    s.end_run("r1", 3.0, "ok", exit_code=0)
    s.record(_exchange("r1", "/lost", answer=b"late body"))
    s.record(_exchange("r1", "/late"))
    s.close()
    run = StoreReader(root).load_run("r1")
    assert run.record.end is not None
    assert (run.record.end.outcome, run.record.dropped_events) == ("ok", 2)
    assert _paths(run) == ["/a", "/late"]
    lines = [json.loads(line) for line in (root / "runs/r1/events.jsonl").read_text().splitlines()]
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
    """Every refusal names its file and line, a missing blob the line that refers to it."""
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
    """A run irimi may not read is an OSError, not a missing run.

    `runs show` and `compare` say `could not be read`; `list_runs` skips it with a warning."""
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
            with caplog.at_level(logging.WARNING, logger="irimi.record.store"):
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
    with caplog.at_level(logging.WARNING, logger="irimi.record.store"):
        assert [r.run_id for r in reader.list_runs()] == ["good", "shut"]
    assert caplog.messages == []
    for run_id in ("gone", "a-file", "hollow"):
        with pytest.raises(RunNotFound):
            reader.load_run(run_id)
    # The store itself: one with no `runs/` holds no run, and one irimi may not enter is an
    # OSError, not an empty store.
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
    """`list_runs` skips a run.json naming another run, which `load_run` would not find."""
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


def test_null_store_record_and_close_return_none():
    store = NullStore()
    faked = builders.exchange(
        response=Response(status=200, headers=(("content-type", "application/json"),), body=b"[]"),
        operation="POST /v1/charges",
        kind="unknown",
        answered_by="fake-L0",
    )
    assert store.record(faked) is None  # type: ignore[func-returns-value]
    assert store.close() is None  # type: ignore[func-returns-value]


KEY = b"k" * 32


LONG_KEY = "sk_live_" + "a" * (2 * MAX_QUEUED_ERROR)


def test_a_run_s_end_whose_error_is_one_long_key_is_stored_as_its_placeholder(tmp_path):
    """An end's error that is one long key is stored as its placeholder, never empty.

    The error waits cut before it is redacted; a key across the cut is hidden there, not dropped."""
    store = DirectoryStore(tmp_path / "store", KEY)
    store.start_run(RunRecord.process("ended01", ("python3",), None, 1.0))
    store.end_run("ended01", 2.0, "error", ErrorInfo(LONG_KEY, LONG_KEY), 1)
    store.close()
    end = StoreReader(tmp_path / "store").load_run("ended01").record.end
    assert end is not None and end.error is not None
    hidden = redact.placeholder(KEY, LONG_KEY)
    assert (end.error.type, end.error.message) == (hidden, hidden)


def test_an_end_s_error_is_a_prefix_of_its_redacted_whole_when_a_key_crosses_the_cut(tmp_path):
    store = DirectoryStore(tmp_path / "store", KEY)
    store.start_run(RunRecord.process("ended02", ("python3",), None, 1.0))
    message = f"refused {LONG_KEY} today"
    store.end_run("ended02", 2.0, "error", ErrorInfo("Boom", message), 1)
    store.close()
    end = StoreReader(tmp_path / "store").load_run("ended02").record.end
    assert end is not None and end.error is not None
    whole = redact.redact_error(ErrorInfo("Boom", message), KEY)
    assert whole is not None
    assert end.error.message == f"refused {redact.placeholder(KEY, LONG_KEY)}"
    assert whole.message.startswith(end.error.message)


@pytest.mark.parametrize("fails", ["write", "claim"])
def test_a_run_s_end_is_announced_even_when_it_cannot_be_written(tmp_path, monkeypatch, fails):
    """A run's end is announced even when it cannot be written, so its write log and keys go.

    A full disk, or a replay's store that may not write the run, would keep them all serve long."""
    ended: list[str] = []
    root = tmp_path / "store"
    replay_of = "recorded1" if fails == "claim" else None
    store = DirectoryStore(root, KEY, on_run_ended=ended.append, replay_of=replay_of)
    if fails == "write":

        def refuse(*args: object, **kwargs: object) -> None:
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(writer, "write_private", refuse)
    # A replay's store never writes the run it replays.
    store.end_run("recorded1" if fails == "claim" else "full0001", 2.0, "ok")
    store.close()
    assert ended == ["recorded1" if fails == "claim" else "full0001"]
    assert store.stats().dropped == 1
