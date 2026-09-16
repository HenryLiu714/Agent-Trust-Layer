"""TraceStore: where finished exchanges go. Phase 3 replaces NullStore with a directory store."""

from typing import Protocol

from irimi.exchange import Exchange


class TraceStore(Protocol):
    def record(self, exchange: Exchange) -> None: ...

    def close(self) -> None: ...


class NullStore:
    def record(self, exchange: Exchange) -> None:
        return None

    def close(self) -> None:
        return None
