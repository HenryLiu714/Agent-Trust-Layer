"""What the engine hands its store: telemetry as a count only, and every other kind whole."""

import pytest

from irimi.core.exchange import Exchange
from irimi.decide.overlay import NoOverlay
from irimi.decide.policy import ShadowPolicy
from tests.support import builders
from tests.support.engine import engine_config


class _RecordingStore:
    """A RunStore that remembers what it was handed. NullStore cannot answer this question."""

    def __init__(self):
        self.recorded = []

    def record(self, exchange):
        self.recorded.append(exchange)

    def close(self):
        return None


def _addon(tmp_path, monkeypatch, store):
    """An IrimiAddon with no proxy under it. `session.record` is a plain method; driving a whole
    engine would only add a thread between the assertion and the thing asserted."""
    from irimi.engine.mitm import IrimiAddon

    seen: list[Exchange] = []
    addon = IrimiAddon(
        engine_config(tmp_path, monkeypatch),
        ShadowPolicy(),
        store,
        NoOverlay(),
        seen.append,
        lambda port, error: None,
    )
    return addon, seen


def _exchange(kind, answered_by="live"):
    """An exchange with Sentry's envelope endpoint, of `kind`, in the engine's run."""
    return builders.exchange(
        request=builders.request(
            method="POST", host="o0.ingest.sentry.io", path="/api/7/envelope/"
        ),
        service="sentry",
        operation="envelope.send",
        kind=kind,
        answered_by=answered_by,
        run_id="t3st",
    )


def test_telemetry_is_reported_and_stored_only_as_having_happened(tmp_path, monkeypatch):
    """Forwarded in every mode, counted in its own bucket, and stored only as having happened:
    one `TelemetrySeen` line with its host and time, so a stored run's summary can count
    it, and never its request or response - a recording of the agent's own observability traffic
    would re-emit someone else's events on replay. So no blob is written for its body."""
    from dataclasses import replace

    from irimi.protocol.trace import TelemetrySeen
    from irimi.record import redact
    from irimi.record.store.reader import StoreReader
    from irimi.record.store.writer import DirectoryStore

    store = DirectoryStore(tmp_path / "store", redact.load_key(tmp_path))
    addon, seen = _addon(tmp_path, monkeypatch, store)
    envelope = _exchange("telemetry")
    envelope = replace(envelope, request=replace(envelope.request, body=b"an envelope"))
    envelope = replace(envelope, started_at=1.5, ended_at=2.0)
    addon.session.record(envelope)
    store.close()
    assert [ex.kind for ex in seen] == ["telemetry"]
    stored = StoreReader(tmp_path / "store").load_run("t3st")
    assert stored.events == [
        TelemetrySeen(run_id="t3st", host="o0.ingest.sentry.io", started_at=1.5)
    ]
    assert stored.record.attribution == "header"
    assert not (tmp_path / "store" / "blobs").exists()


def test_a_store_that_raises_neither_escapes_the_hook_nor_hides_the_exchange(
    tmp_path, monkeypatch, caplog
):
    """A recording failure never affects traffic. `RunStore.record` may not raise, and
    `session.record` guards it anyway: a store that broke the contract escaped every hook that
    finishes a flow, and took the terminal line and the summary's count of the exchange with it."""

    class _Raising(_RecordingStore):
        def record(self, exchange):
            raise RuntimeError("the store exploded")

    addon, seen = _addon(tmp_path, monkeypatch, _Raising())
    with caplog.at_level("WARNING", logger="irimi.engine.session"):
        addon.session.record(_exchange("write", answered_by="fake-L0"))
    assert [ex.kind for ex in seen] == ["write"]
    # The type, never the message: a message may quote the value the store choked on.
    assert "the trace store raised RuntimeError" in caplog.text
    assert "exploded" not in caplog.text


@pytest.mark.parametrize("kind", ["read", "write", "llm", "unknown"])
def test_every_other_kind_is_still_recorded(tmp_path, monkeypatch, kind):
    store = _RecordingStore()
    addon, seen = _addon(tmp_path, monkeypatch, store)
    addon.session.record(_exchange(kind))
    assert [ex.kind for ex in store.recorded] == [kind]
    assert [ex.kind for ex in seen] == [kind]
