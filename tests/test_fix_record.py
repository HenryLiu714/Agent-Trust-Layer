"""The trace store's writer, its deletion of runs and its redaction of a run's end, at the edges a
store shared by several processes reaches."""

import os
import time
from pathlib import Path

import pytest

from irimi.cli import main
from irimi.config import paths
from irimi.core.exchange import Exchange, Request, Response
from irimi.core.limits import MAX_QUEUED_ERROR
from irimi.protocol.trace import ErrorInfo, RunRecord
from irimi.record import redact
from irimi.record.store import retention, writer
from irimi.record.store.lock import StoreBusy, WriterLock
from irimi.record.store.reader import StoreReader
from irimi.record.store.writer import DirectoryStore

KEY = b"k" * 32
# A long credential shape: an `sk_live_` key past the length an end's error waits cut to.
LONG_KEY = "sk_live_" + "a" * (2 * MAX_QUEUED_ERROR)


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path))
    return tmp_path


def _refund(run_id: str, started_at: float) -> Exchange:
    request = Request(
        "POST",
        "https",
        "api.stripe.com",
        443,
        "/v1/refunds",
        "",
        (("content-type", "application/x-www-form-urlencoded"),),
        f"charge=ch_{run_id}&amount=4900&currency=usd".encode(),
    )
    return Exchange(
        request,
        Response(200, (), b"{}"),
        "stripe",
        "refunds.create",
        "write",
        "fake-L1",
        run_id,
        started_at=started_at,
        ended_at=started_at + 0.1,
    )


def _store(home: Path) -> DirectoryStore:
    return DirectoryStore(home / paths.STORE_DIR_NAME, redact.load_key(home))


def _flush(store: DirectoryStore) -> None:
    """Wait for the writer to take everything queued."""
    deadline = time.monotonic() + 10
    while store.stats().queued and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.05)


def test_runs_rm_never_deletes_a_run_another_process_is_writing(home, capfd):
    """`irimi shadow` takes no `serve.lock`: its run has not ended, and `last` names it."""
    store = _store(home)
    root = home / paths.STORE_DIR_NAME
    store.start_run(RunRecord.process("live0001", ("python3", "agent.py"), None, time.time()))
    store.record(_refund("live0001", time.time()))
    _flush(store)
    try:
        assert main(["runs", "rm", "last"]) == 2
        assert capfd.readouterr() == (
            "",
            f"error: another irimi process is writing the trace store {root}; "
            "stop it to delete runs\n",
        )
        assert (root / "runs" / "live0001" / "events.jsonl").is_file()
    finally:
        store.close()
    assert main(["runs", "rm", "live0001"]) == 0
    assert capfd.readouterr().out.splitlines()[0] == "deleted run live0001"
    assert not (root / "runs" / "live0001").exists()


def test_runs_prune_never_deletes_an_idle_run_in_progress(home, capfd):
    """A run whose agent has not made a call for longer than `--older-than` is still in
    progress: its files are old, and it has not ended."""
    store = _store(home)
    root = home / paths.STORE_DIR_NAME
    long_ago = time.time() - 3 * 86400
    store.start_run(RunRecord.process("idle0001", ("python3", "agent.py"), None, long_ago))
    store.record(_refund("idle0001", long_ago))
    _flush(store)
    run_dir = root / "runs" / "idle0001"
    for name in ("run.json", "events.jsonl"):
        os.utime(run_dir / name, (long_ago, long_ago))
    try:
        for dry_run in ([], ["--dry-run"]):
            assert main(["runs", "prune", "--older-than", "1d", *dry_run]) == 2
            assert "another irimi process is writing" in capfd.readouterr().err
        assert run_dir.is_dir()
        assert len(list((root / "blobs").iterdir())) == 2
    finally:
        store.close()
    for name in ("run.json", "events.jsonl"):
        os.utime(run_dir / name, (long_ago, long_ago))
    assert main(["runs", "prune", "--older-than", "1d"]) == 0
    assert capfd.readouterr().out.splitlines()[0] == "deleted run idle0001"


def test_a_store_waits_for_a_removal_and_removal_waits_for_no_store(tmp_path):
    """The writer lock is shared among stores and exclusive to a removal."""
    root = tmp_path / "store"
    root.mkdir()
    one, two = WriterLock.share(root), WriterLock.share(root)
    with pytest.raises(StoreBusy):
        WriterLock.exclude(root)
    one.release()
    two.release()
    two.release()  # idempotent
    removing = WriterLock.exclude(root)
    with pytest.raises(StoreBusy):
        retention.remove(root, [])
    removing.release()
    assert retention.remove(root, []) == retention.Removal((), ())


def test_a_run_s_end_whose_error_is_one_long_key_is_stored_as_its_placeholder(tmp_path):
    """An end's error waits for the writer cut, before it is redacted. A key across that cut is
    hidden there, not dropped, so the stored text is the key's placeholder and never empty."""
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
    """The callback drops the run's write log and keys: a disk that refuses the end, or a replay's
    store that may not write the run, must not keep them for the life of a serve."""
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
