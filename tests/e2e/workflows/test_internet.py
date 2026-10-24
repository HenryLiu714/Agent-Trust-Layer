"""The fake internet's resolver: a name it does not serve never resolves, whatever the lookup."""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import Any

import pytest

from tests.e2e.workflows.harness.internet import FakeInternet, Req, Resp, resolving_only


class _Stripe:
    hosts: tuple[str, ...] = ("api.stripe.com",)

    def handle(self, req: Req) -> Resp:
        return Resp()

    def is_write(self, req: Req) -> bool:
        return False


def test_resolving_refuses_every_lookup_of_a_name_it_does_not_serve():
    originals = (socket.getaddrinfo, socket.gethostbyname, socket.gethostbyname_ex)
    internet = FakeInternet([_Stripe()])
    lookups: dict[str, Callable[[str], Any]] = {
        "getaddrinfo": lambda host: socket.getaddrinfo(host, 443),
        # Looked up at each call, so the call reaches the resolver `resolving()` patches in.
        "gethostbyname": lambda host: socket.gethostbyname(host),  # noqa: PLW0108
        "gethostbyname_ex": lambda host: socket.gethostbyname_ex(host),  # noqa: PLW0108
    }
    with internet.resolving():
        for lookup in lookups.values():
            for name in ("example.com", "8.8.8.8"):
                with pytest.raises(socket.gaierror, match="not on the fake internet"):
                    lookup(name)
        assert socket.gethostbyname("API.Stripe.com.") == "127.0.0.1"
        assert socket.gethostbyname_ex("api.stripe.com")[2] == ["127.0.0.1"]
        assert {ai[4][0] for ai in socket.getaddrinfo("api.stripe.com", 443)} == {"127.0.0.1"}
        assert socket.gethostbyname("127.0.0.1") == "127.0.0.1"
    assert (socket.getaddrinfo, socket.gethostbyname, socket.gethostbyname_ex) == originals


def test_resolving_only_serves_the_names_it_is_given_whatever_their_case():
    """`resolving_only` resolves its names to loopback in any case, refuses others, and restores.

    It is the fake internet's resolver alone, for a test whose irimi reaches loopback stubs by
    their real names (`tests/e2e/test_replay_example.py`)."""
    originals = (socket.getaddrinfo, socket.gethostbyname, socket.gethostbyname_ex)
    # The block raises on purpose, to show the socket module is restored all the same.
    with (  # noqa: PT012
        pytest.raises(RuntimeError, match="block raised"),
        resolving_only(["Stub.Example"]),
    ):
        assert socket.gethostbyname("stub.example.") == "127.0.0.1"
        assert socket.gethostbyname(b"STUB.EXAMPLE") == "127.0.0.1"  # type: ignore[arg-type]
        assert {ai[4][0] for ai in socket.getaddrinfo("stub.example", 443)} == {"127.0.0.1"}
        with pytest.raises(socket.gaierror, match="not on the fake internet"):
            socket.gethostbyname("api.stripe.com")
        raise RuntimeError("block raised")
    assert (socket.getaddrinfo, socket.gethostbyname, socket.gethostbyname_ex) == originals
