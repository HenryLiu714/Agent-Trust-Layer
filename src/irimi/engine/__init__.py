"""The Engine seam. Everything outside irimi.engine.mitm talks to this protocol only."""

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from irimi.ca import CAPaths
from irimi.exchange import Exchange
from irimi.servicemap import MapIndex

OnExchange = Callable[[Exchange], None]


class EngineStartError(RuntimeError):
    """The engine could not bind its listener (port in use, bad config) or was stopped first."""


@dataclass(frozen=True)
class EngineConfig:
    run_id: str
    ca: CAPaths
    confdir: Path
    listen_host: str
    listen_port: int  # 0 = pick a free port
    reverse_hosts: frozenset[str] = frozenset()  # hosts the reverse door may relay to; empty = none
    maps: MapIndex = field(default_factory=MapIndex)  # loaded service maps; empty = verb rule only


class Engine(Protocol):
    config: EngineConfig

    async def run(self) -> None:
        """Serve until shutdown() is called. Must be awaited inside an event loop."""
        ...

    def shutdown(self) -> None:
        """Thread-safe. Makes run() return."""
        ...

    async def wait_ready(self) -> None:
        """Returns once the listener is bound; listen_port() is valid afterwards."""
        ...

    def listen_port(self) -> int | None:
        """The bound port, or None before ready."""
        ...
