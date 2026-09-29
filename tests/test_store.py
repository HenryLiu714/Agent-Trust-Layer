"""The trace store on disk (#70): `DirectoryStore` writes, `StoreReader` reads back.

Every test drives the store through its public methods and asserts on what `StoreReader` and the
files under the root say, so a test here breaks only when what reaches disk changes. The real
`irimi shadow` over these same files is `tests/test_trace_e2e.py`'s.
"""

import dataclasses
import json
import math
import os
import threading
import time
from pathlib import Path

import pytest

from irimi import redact, store, trace
from irimi.exchange import BAD_RUN_ID_FLAG, BODY_TRUNCATED_FLAG, Exchange, Request, Response
from irimi.store import DirectoryStore, NullStore, RunNotFound, StoreReader
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
        validation="unvalidated",
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
        ended_at=None,
        outcome=None,
        error=None,
        exit_code=None,
    )


def _tool_call(run_id: str, args) -> ToolCall:
    return ToolCall("t1", run_id, "db.write", "write", "shadow", args, None, None, 1.0, 2.0)


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
    assert (run.record.ended_at, run.record.outcome, run.record.exit_code) == (3.0, "error", 2)
    assert run.record.error == ErrorInfo("exit", "exited 2")
    assert run.record.dropped_events == 0
    assert _paths(run) == ["/a", "/b"]
    # A trigger's args are redacted before they reach disk, as a body is (#69).
    assert run.record.trigger is not None
    assert run.record.trigger.args == {
        "argv": ["agent", "--token", redact.placeholder(key, SECRET)]
    }
    assert not any(SECRET.encode() in p.read_bytes() for p in root.rglob("*") if p.is_file())
    assert [json.loads(line)["seq"] for line in (root / "runs/r1/events.jsonl").open()] == [1, 2]


def test_the_first_event_of_a_run_with_no_start_creates_a_header_run(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r2"))
    s.close()
    record = StoreReader(root).load_run("r2").record
    assert record == store.header_record("r2")
    assert (record.attribution, record.trigger, record.outcome) == ("header", None, None)


def test_a_late_event_after_the_run_ended_is_still_appended(root, key):
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.end_run("r1", 3.0, "ok", exit_code=0)
    s.record(_exchange("r1", "/late"))
    s.close()
    run = StoreReader(root).load_run("r1")
    assert (run.record.outcome, _paths(run)) == ("ok", ["/late"])


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
    # Not UTF-8, so redaction stores it unscanned (#69) and the test does not time a regex.
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
    assert BODY_TRUNCATED_FLAG in ex.flags


def test_a_body_is_redacted_whole_before_it_is_cut(root, key, monkeypatch):
    """A secret straddling the cut is never stored as half a secret (#69)."""
    monkeypatch.setattr(store, "MAX_STORED_BODY", 20)
    body = b"012345678 " + SECRET.encode()  # the cut at 20 falls inside the key
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", answer=body))
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert ex.response.body == (b"012345678 " + redact.placeholder(key, SECRET).encode())[:20]
    assert not any(b"sk_live_" in p.read_bytes() for p in root.rglob("*") if p.is_file())


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
    assert (bad.run_id, BAD_RUN_ID_FLAG in bad.flags) == ("../x", True)
    # `unattributed` is the name of "no run", not a bad name.
    assert (none.run_id, none.flags) == (trace.UNATTRIBUTED, ())


def test_an_invalid_run_id_never_starts_a_run(root, key):
    s = DirectoryStore(root, key)
    s.start_run(_record("../x"))
    s.start_run(_record(trace.UNATTRIBUTED))
    s.close()
    assert s.stats().dropped == 2
    assert not root.exists() or not (root / "runs").exists()


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
        assert (clash.run_id, BAD_RUN_ID_FLAG in clash.flags) == ("run1", True)
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
    assert run.record.outcome is None  # incomplete
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
    real = store._write_atomically
    failed = []

    def once(path: Path, data: bytes) -> None:
        if path.parent.name == store.BLOBS_DIR and not failed:
            failed.append(path)
            raise OSError("disk full")
        real(path, data)

    monkeypatch.setattr(store, "_write_atomically", once)
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/lost", answer=b"first"))
    s.record(_exchange("r1", "/kept", answer=b"second"))
    s.close()
    assert failed and s.stats().dropped == 1
    run = StoreReader(root).load_run("r1")
    assert _paths(run) == ["/kept"]
    assert run.record.dropped_events == 1


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
    assert s.stats().dropped >= 2
    assert StoreReader(root).load_run("r1").record.dropped_events >= 2
    release.set()


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


def test_the_writer_fsyncs_in_batches_before_close(root, key, monkeypatch):
    synced: list[int] = []
    real = os.fsync
    monkeypatch.setattr(store, "SYNC_EVERY", 3)
    monkeypatch.setattr(store.os, "fsync", lambda fd: (synced.append(fd), real(fd))[1])
    s = DirectoryStore(root, key)
    for n in range(4):  # run.json, events.jsonl and a blob each: past 3 on the first event
        s.record(_exchange("r1", f"/n{n}", answer=f"body {n}".encode()))
    _drain(s, 4)
    assert synced, "nothing was fsynced before close"
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


def test_a_line_the_disk_refused_half_way_is_cut_off_and_the_run_still_reads(
    root, key, monkeypatch
):
    real = os.write
    failed = []

    def half(fd, data):
        if not failed and len(data) > 40:
            failed.append(fd)
            real(fd, bytes(data[:40]))
            raise OSError(28, "No space left on device")
        return real(fd, data)

    s = DirectoryStore(root, key)
    s.record(_exchange("r1", "/a"))
    _drain(s, 1)
    monkeypatch.setattr(store.os, "write", half)
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
    assert BODY_TRUNCATED_FLAG in ex.flags


def test_an_ended_run_is_forgotten_and_a_late_event_reopens_it_from_disk(root, key, monkeypatch):
    """A long `serve` holds only its open runs. A late event after `end_run` reopens the run:
    `seq` continues, the outcome stays, and a drop after the end is added to the stored count -
    once, and to the drop before the end, never counting that one twice."""
    real = store._write_atomically

    def refuse_late_blob(path: Path, data: bytes) -> None:
        if data in (b"early body", b"late body"):
            raise OSError("disk full")
        real(path, data)

    monkeypatch.setattr(store, "_write_atomically", refuse_late_blob)
    s = DirectoryStore(root, key)
    s.start_run(_record("r1"))
    s.record(_exchange("r1", "/a"))
    s.record(_exchange("r1", "/lost-early", answer=b"early body"))
    s.end_run("r1", 3.0, "ok", exit_code=0)
    s.record(_exchange("r1", "/lost", answer=b"late body"))
    s.record(_exchange("r1", "/late"))
    s.close()
    run = StoreReader(root).load_run("r1")
    assert (run.record.outcome, run.record.dropped_events) == ("ok", 2)
    assert _paths(run) == ["/a", "/late"]
    lines = [json.loads(line) for line in (root / "runs/r1/events.jsonl").open()]
    assert [line["seq"] for line in lines] == [1, 2]


# ----------------------------------------------------------------------------------- the reader


def test_list_runs_is_newest_first_with_unstarted_runs_last(root, key):
    s = DirectoryStore(root, key)
    s.start_run(_record("old", started_at=1.0))
    s.start_run(_record("new", started_at=2.0))
    s.record(_exchange("headless"))
    s.close()
    assert [r.run_id for r in StoreReader(root).list_runs()] == ["new", "old", "headless"]


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


def test_a_missing_blob_is_a_trace_format_error(root, key):
    s = DirectoryStore(root, key)
    s.record(_exchange("r1", answer=b"a body"))
    s.close()
    (root / "blobs" / trace.body_ref(b"a body").sha256).unlink()
    with pytest.raises(TraceFormatError, match="missing"):
        StoreReader(root).load_run("r1")
