"""The trace store on disk (#70): `DirectoryStore` writes, `StoreReader` reads back.

Every test drives the store through its public methods and asserts on what `StoreReader` and the
files under the root say, so a test here breaks only when what reaches disk changes. The real
`irimi shadow` over these same files is `tests/test_trace_e2e.py`'s.
"""

import dataclasses
import errno
import json
import math
import os
import threading
import time
from pathlib import Path

import pytest

from irimi import exchange, redact, store, trace
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


class _StoreOs:
    """The `os` module as `irimi.store` sees it, with the functions a test sets replaced. Patching
    `os` itself would change it for every thread in the process, pytest's own included."""

    def __getattr__(self, name: str):
        return getattr(os, name)


@pytest.fixture
def store_os(monkeypatch) -> _StoreOs:
    proxy = _StoreOs()
    monkeypatch.setattr(store, "os", proxy)
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


def test_an_error_message_and_a_trigger_name_are_redacted_before_they_reach_disk(root, key):
    """`ErrorInfo.from_exception` keeps `str(exc)`, and an exception's message may quote the key
    it refused; a trigger's name is the argv[0] its args repeat (#69)."""
    s = DirectoryStore(root, key)
    s.start_run(dataclasses.replace(_record("r1"), trigger=Trigger(SECRET, None, None, True)))
    call = _tool_call("r1", None)
    s.record_tool_call(dataclasses.replace(call, error=ErrorInfo("x.Err", f"refused {SECRET}")))
    s.end_run("r1", 3.0, "error", ErrorInfo("x.Err", f"Invalid API key: {SECRET}"), exit_code=1)
    s.close()
    run = StoreReader(root).load_run("r1")
    hidden = redact.placeholder(key, SECRET)
    assert run.record.trigger is not None and run.record.trigger.name == hidden
    assert run.record.error == ErrorInfo("x.Err", f"Invalid API key: {hidden}")
    (stored,) = run.events
    assert isinstance(stored, ToolCall)
    assert stored.error == ErrorInfo("x.Err", f"refused {hidden}")
    assert not any(SECRET.encode() in p.read_bytes() for p in root.rglob("*") if p.is_file())


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
    same byte, so they still split the stored body (#71)."""
    monkeypatch.setattr(store, "MAX_STORED_BODY", 20)
    s = DirectoryStore(root, key)
    s.record(_streamed(b"a" * 8 + b"b" * 8 + b"c" * 14, (8, 8, 14)))
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert (ex.response.body, ex.stream_chunks) == (b"a" * 8 + b"b" * 8 + b"c" * 4, (8, 8, 4))
    assert BODY_TRUNCATED_FLAG in ex.flags


def test_a_stream_redaction_grows_past_the_maximum_is_a_prefix_of_its_redacted_whole(
    root, key, monkeypatch
):
    """The engine keeps at most MAX_STORED_BODY bytes of a stream, but a placeholder can be longer
    than its secret, so the redacted body can pass the cap again. It is cut like any body: a
    prefix of the redacted whole, never half a secret, its chunks cut with it, and both flags -
    the stream was not whole, and neither is the stored body (#71)."""
    secret = "sk_live_A"  # far shorter than its placeholder
    line = b'data: {"text":"' + secret.encode() + b'"}\n'
    redacted_line = line.replace(secret.encode(), redact.placeholder(key, secret).encode())
    sent = line * 10
    cap = 6 * len(redacted_line) + len(b'data: {"text":"<redac')  # inside the 7th placeholder
    assert len(sent) <= cap < len(redacted_line) * 10
    monkeypatch.setattr(store, "MAX_STORED_BODY", cap)
    streamed = dataclasses.replace(
        _streamed(sent, (len(line),) * 10), flags=(exchange.STREAM_TRUNCATED_FLAG,)
    )
    s = DirectoryStore(root, key)
    s.record(streamed)
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert ex.response.body == (redacted_line * 10)[:cap]
    assert ex.stream_chunks == (len(redacted_line),) * 6 + (cap - 6 * len(redacted_line),)
    assert set(ex.flags) == {exchange.STREAM_TRUNCATED_FLAG, BODY_TRUNCATED_FLAG}
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
    assert BODY_TRUNCATED_FLAG in ex.flags


def test_a_streamed_body_too_big_to_redact_is_stored_with_no_chunks(root, key, monkeypatch):
    monkeypatch.setattr(store, "MAX_REDACTED_BODY", 10)
    s = DirectoryStore(root, key)
    s.record(_streamed(b"x" * 11, (5, 6)))
    s.close()
    (ex,) = StoreReader(root).load_run("r1").events
    assert isinstance(ex, Exchange) and ex.response is not None
    assert (ex.response.body, ex.stream_chunks) == (b"", ())
    assert BODY_TRUNCATED_FLAG in ex.flags


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

    def once(path: Path, data: bytes, **kwargs) -> None:
        if path.parent.name == store.BLOBS_DIR and not failed:
            failed.append(path)
            raise OSError("disk full")
        real(path, data, **kwargs)

    monkeypatch.setattr(store, "_write_atomically", once)
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
    the flow (CLAUDE.md): even a caller breaking the types gets a counted drop, not an exception."""
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
    assert BODY_TRUNCATED_FLAG in ex.flags


def test_an_ended_run_is_forgotten_and_a_late_event_reopens_it_from_disk(root, key, monkeypatch):
    """A long `serve` holds only its open runs. A late event after `end_run` reopens the run:
    `seq` continues, the outcome stays, and a drop after the end is added to the stored count -
    once, and to the drop before the end, never counting that one twice."""
    real = store._write_atomically

    def refuse_late_blob(path: Path, data: bytes, **kwargs) -> None:
        if data in (b"early body", b"late body"):
            raise OSError("disk full")
        real(path, data, **kwargs)

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
