"""Bytes inside a CONNECT tunnel: what the engine relays, and what it refuses to."""

import contextlib
import socket
import ssl
import threading
import time

import pytest

from irimi.config import ca
from tests.support.engine import engine_config, open_tunnel, raw_tunnel, start_engine


def test_a_tunnelled_request_whose_method_mitmproxy_would_not_call_http_is_still_seen(
    engine, upstream
):
    """A tunnelled `M-SEARCH` is parsed as HTTP and faked, not relayed as raw TCP.

    mitmproxy's raw TCP mode would relay a first line that does not start with three letters."""
    eng, seen = engine
    s = raw_tunnel(eng.listen_port(), upstream)
    with s:
        s.sendall(b"M-SEARCH /hello HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n")
        assert s.recv(4096).startswith(b"HTTP/1.1 200")
    assert [(ex.request.method, ex.answered_by) for ex in seen] == [("M-SEARCH", "fake-L0")]
    assert upstream.seen == []


def test_bytes_in_a_tunnel_that_are_not_http_are_never_relayed(engine):
    """Unreadable tunnel bytes fail closed, and the destination is never dialled.

    irimi cannot read them, so it cannot know they are not a write."""
    eng, seen = engine
    with socket.create_server(("127.0.0.1", 0)) as destination:
        s = raw_tunnel(eng.listen_port(), destination.getsockname()[1])
        with s:
            s.sendall(b"\x00\x01 a write irimi cannot read\r\n\r\n")
            answer = b""
            with contextlib.suppress(TimeoutError, ConnectionError):
                while chunk := s.recv(4096):  # mitmproxy answers 400 and closes; a relay waits
                    answer += chunk
        destination.setblocking(False)
        with pytest.raises(BlockingIOError):
            destination.accept()
    assert answer.startswith(b"HTTP/1.1 400")
    assert seen == []


def test_a_tunnelled_request_whose_first_segment_is_short_is_still_seen(engine):
    """A `POST` split after two bytes is read whole and faked; the destination is never dialled.

    mitmproxy picks a tunnel's protocol from its first bytes, and raw TCP mode would relay this."""
    eng, seen = engine
    with socket.create_server(("127.0.0.1", 0)) as destination:
        s = raw_tunnel(eng.listen_port(), destination.getsockname()[1])
        with s:
            s.sendall(b"PO")
            time.sleep(0.2)  # its own segment, decided on before the rest arrives
            s.sendall(b"ST /charges HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 2\r\n\r\n{}")
            assert s.recv(4096).startswith(b"HTTP/1.1 200")
        destination.setblocking(False)
        with pytest.raises(BlockingIOError):
            destination.accept()
    assert [(ex.request.method, ex.answered_by) for ex in seen] == [("POST", "fake-L0")]


def test_a_connection_a_live_read_upgraded_is_closed_not_relayed(engine):
    """irimi closes a connection a live read upgraded to a non-WebSocket protocol after the `101`.

    Relayed as raw bytes, a write sent on it would reach the real service."""
    eng, seen = engine
    heard: list[bytes] = []
    with socket.create_server(("127.0.0.1", 0)) as service:

        def serve() -> None:
            conn, _ = service.accept()
            with conn:
                conn.settimeout(5)
                with contextlib.suppress(OSError):
                    while b"\r\n\r\n" not in b"".join(heard) and (chunk := conn.recv(4096)):
                        heard.append(chunk)
                    conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: foo\r\n\r\n")
                    while chunk := conn.recv(4096):
                        heard.append(chunk)

        server = threading.Thread(target=serve, daemon=True)
        server.start()
        port = service.getsockname()[1]
        with socket.create_connection(("127.0.0.1", eng.listen_port()), timeout=5) as s:
            s.sendall(
                f"GET http://127.0.0.1:{port}/feed HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                "Connection: Upgrade\r\nUpgrade: foo\r\n\r\n".encode()
            )
            assert s.recv(4096).startswith(b"HTTP/1.1 101")
            with contextlib.suppress(OSError):
                s.sendall(b"POST /charges HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
                assert s.recv(4096) == b""  # closed by irimi
        server.join(timeout=10)
    assert b"POST" not in b"".join(heard)
    assert [(ex.request.method, ex.answered_by) for ex in seen] == [("GET", "live")]


def test_a_client_that_refuses_irimi_s_ca_is_reported_by_the_host_it_asked_for(
    tmp_path, monkeypatch
):
    """The engine reports a handshake refused by a client that distrusts its CA, by SNI.

    No request is ever sent, so no exchange exists, and nothing raises."""
    cfg = engine_config(tmp_path, monkeypatch)
    refused = []
    reported = threading.Event()

    def on_tls_failure(host):
        refused.append(host)
        reported.set()

    eng, seen, stop = start_engine(cfg, on_tls_failure=on_tls_failure)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    other = ca.CAPaths(key=elsewhere / "ca.key", cert=elsewhere / "ca.pem")
    ca.generate_ca(other)  # a CA the client trusts, which is not irimi's
    try:
        tunnel = open_tunnel(eng.listen_port(), "api.distrustful.test", 443)
        context = ssl.create_default_context(cafile=str(other.cert))
        with pytest.raises(ssl.SSLCertVerificationError):
            context.wrap_socket(tunnel, server_hostname="api.distrustful.test")
        tunnel.close()
        assert reported.wait(10)
    finally:
        stop()
    assert refused == ["api.distrustful.test"]
    assert seen == []
