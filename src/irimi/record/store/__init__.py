"""The trace store: where runs, exchanges and tool calls are recorded.

Guarantees:
- Never raises into, or blocks, a mitmproxy hook: callers enqueue; one writer thread does I/O.
  A dropped event is counted on the store and on its run (`RunRecord.dropped_events`).
- Nothing unredacted on disk: every event passes `redact.for_disk` first, every record
  `redact.run_for_disk`.
- Files are 0600 and directories 0700: redaction hides credentials, not business data.
- A run id that cannot name a directory is stored in `unattributed/`.
- A failed write leaves no damage: a line the disk refused half-way is truncated, a torn blob is
  rewritten, and a run.json is synced before its rename. A crash's half line is skipped on read.
"""

from dataclasses import dataclass
from typing import Protocol

from irimi.core.exchange import Exchange
from irimi.protocol.trace import ErrorInfo, RunRecord, RunStatus, ToolCall


@dataclass(frozen=True)
class StoreStats:
    """What `RunStore.stats()` reports, as `GET /_irimi/health` returns it."""

    queued: int  # items waiting for the writer
    written: int  # event lines written
    dropped: int  # events dropped, for any reason, since the store was built


class RunStore(Protocol):
    """Where a run's record, exchanges and tool calls go. No method may raise."""

    def start_run(self, record: RunRecord) -> None: ...

    def record(self, exchange: Exchange) -> None: ...

    def record_tool_call(self, call: ToolCall) -> None: ...

    def end_run(
        self,
        run_id: str,
        ended_at: float,
        outcome: RunStatus,
        error: ErrorInfo | None = None,
        exit_code: int | None = None,
    ) -> None: ...

    def stats(self) -> StoreStats: ...

    def close(self) -> None: ...
