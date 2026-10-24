"""Stopping the HTTP servers the tests start."""

from socketserver import BaseServer


def stop_server(server: BaseServer) -> None:
    """Stop `server` serving and close its listening socket, which `shutdown` alone leaves open."""
    server.shutdown()
    server.server_close()
