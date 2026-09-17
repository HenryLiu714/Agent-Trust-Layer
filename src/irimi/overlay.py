"""Overlay: applies the run's faked writes to a live read. Pure function so replay can reuse it."""

from collections.abc import Sequence
from typing import Protocol

from irimi.exchange import Exchange, Request, Response


class Overlay(Protocol):
    def __call__(
        self, write_log: Sequence[Exchange], read_request: Request, upstream_response: Response
    ) -> Response: ...


class NoOverlay:
    def __call__(
        self, write_log: Sequence[Exchange], read_request: Request, upstream_response: Response
    ) -> Response:
        return upstream_response
