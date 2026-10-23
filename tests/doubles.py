"""Test doubles for irimi's seams."""

from irimi.core.exchange import Exchange
from irimi.protocol.trace import ErrorInfo, RunRecord, RunStatus, ToolCall
from irimi.record.store import StoreStats


class NullStore:
    """A RunStore that keeps nothing."""

    def start_run(self, record: RunRecord) -> None:
        return None

    def record(self, exchange: Exchange) -> None:
        return None

    def record_tool_call(self, call: ToolCall) -> None:
        return None

    def end_run(
        self,
        run_id: str,
        ended_at: float,
        outcome: RunStatus,
        error: ErrorInfo | None = None,
        exit_code: int | None = None,
    ) -> None:
        return None

    def stats(self) -> StoreStats:
        return StoreStats(queued=0, written=0, dropped=0)

    def close(self) -> None:
        return None
