"""The trace store: where a run's record, its exchanges and its tool calls go.

`RunStore` is the seam. `writer.DirectoryStore` writes the layout `docs/trace-format.md`
describes (`layout`), `reader.StoreReader` reads it back, and `lock.ServeLock` keeps a root to one
`serve`.

WHAT IS STORED IS STILL PRIVATE. Redaction hides credentials, not the business data around them -
customers, amounts, messages - so every directory the store creates is 0700 and every file 0600, as
the CA's key is.
"""

from dataclasses import dataclass
from typing import Protocol

from irimi.core.exchange import Exchange
from irimi.protocol.trace import ErrorInfo, RunRecord, RunStatus, ToolCall


@dataclass(frozen=True)
class StoreStats:
    """What `RunStore.stats()` reports; the control endpoint's `GET /_irimi/health` answers
    with it."""

    queued: int  # items waiting for the writer
    written: int  # event lines written
    dropped: int  # events dropped, for any reason, since the store was built


class RunStore(Protocol):
    """Where a run's record, exchanges and tool calls go. No method may raise (see `writer`)."""

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
