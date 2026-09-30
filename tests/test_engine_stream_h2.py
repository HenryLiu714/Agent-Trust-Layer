"""A streamed answer over HTTP/2 on both sides of the proxy (#71).

`tests/test_engine_mitm.py` streams over HTTP/1.1, plain and TLS, and the sample corpus has no
TLS at all. A real provider speaks HTTP/2 to a client that offers it, and mitmproxy offers the
upstream what the client offered, so an agent on an HTTP/2 client gets an HTTP/2 upstream. The
tee sits on mitmproxy's version-independent HTTP layer - one call per DATA frame - and this is
the test that says so. Both ends are written with `h2`, the library mitmproxy's own HTTP/2 layer
is built on, so the test needs nothing mitmproxy does not already install.
"""

import socket
import ssl
import threading
from pathlib import Path

from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import DataReceived, RequestReceived, ResponseReceived, StreamEnded

from tests.test_engine_mitm import (
    STREAM_MAP,
    THREE_CHUNKS,
    _config,
    _leaf_cert_for_loopback,
    _maps,
    _start,
)


def _h2_stream_server(tls_leaf: Path, chunks, gates, alpn: list):
    """An HTTPS upstream that speaks only HTTP/2 and answers one request with `chunks` as SSE,
    one DATA frame each, holding each one after the first until `gates[i - 1]` is set. The ALPN
    it agreed goes into `alpn`. The caller closes the returned listener."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(tls_leaf)
    ctx.set_alpn_protocols(["h2"])
    listener = socket.create_server(("127.0.0.1", 0))

    def serve() -> None:
        raw, _ = listener.accept()
        with ctx.wrap_socket(raw, server_side=True) as sock:
            alpn.append(sock.selected_alpn_protocol())
            conn = H2Connection(H2Configuration(client_side=False))
            conn.initiate_connection()
            sock.sendall(conn.data_to_send())
            while data := sock.recv(65535):
                for event in conn.receive_data(data):
                    if not isinstance(event, RequestReceived):
                        continue
                    headers = [(":status", "200"), ("content-type", "text/event-stream")]
                    conn.send_headers(event.stream_id, headers)
                    for i, chunk in enumerate(chunks):
                        if i:
                            gates[i - 1].wait(timeout=20)
                        last = i == len(chunks) - 1
                        conn.send_data(event.stream_id, chunk, end_stream=last)
                        sock.sendall(conn.data_to_send())
                sock.sendall(conn.data_to_send())

    threading.Thread(target=serve, daemon=True).start()
    return listener


def _h2_through_the_proxy(cfg, proxy_port: int, upstream_port: int, gates):
    """POST the chat route over HTTP/2 inside a CONNECT tunnel, trusting the irimi CA, and read
    the answer, setting `gates[i]` once chunk `i` has arrived. The ALPN the proxy agreed, the
    status and each non-empty DATA frame's bytes."""
    authority = f"127.0.0.1:{upstream_port}"
    sock = socket.create_connection(("127.0.0.1", proxy_port), timeout=5)
    sock.sendall(f"CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n".encode())
    reply = b""
    while b"\r\n\r\n" not in reply:
        reply += sock.recv(4096)
    assert reply.startswith(b"HTTP/1.1 200"), reply
    ctx = ssl.create_default_context(cafile=str(cfg.ca.cert))
    ctx.set_alpn_protocols(["h2"])
    status, frames = None, []
    with ctx.wrap_socket(sock, server_hostname="127.0.0.1") as tls:
        conn = H2Connection(H2Configuration(client_side=True, header_encoding="utf-8"))
        conn.initiate_connection()
        request = [
            (":method", "POST"),
            (":scheme", "https"),
            (":authority", authority),
            (":path", "/v1/chat/completions"),
            ("content-type", "application/json"),
        ]
        conn.send_headers(1, request)
        conn.send_data(1, b"{}", end_stream=True)
        tls.sendall(conn.data_to_send())
        ended = False
        while not ended:
            data = tls.recv(65535)
            assert data, "the proxy closed the connection before the stream ended"
            for event in conn.receive_data(data):
                if isinstance(event, ResponseReceived):
                    status = dict(event.headers)[":status"]
                elif isinstance(event, DataReceived):
                    conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                    if not event.data:  # mitmproxy ends the stream with an empty DATA frame
                        continue
                    frames.append(event.data)
                    if len(frames) <= len(gates):
                        gates[len(frames) - 1].set()
                elif isinstance(event, StreamEnded):
                    ended = True
            tls.sendall(conn.data_to_send())
        return tls.selected_alpn_protocol(), status, frames


def test_a_stream_over_http2_on_both_sides_is_recorded_as_its_chunks(tmp_path, monkeypatch):
    """Each chunk reaches the agent before the upstream sends the next, and the exchange holds
    the whole stream and each chunk's length, as it does over HTTP/1.1 (#71)."""
    cfg = _config(tmp_path, monkeypatch, maps=_maps(tmp_path, monkeypatch, STREAM_MAP))
    gates = [threading.Event(), threading.Event()]
    upstream_alpn: list = []
    listener = _h2_stream_server(
        _leaf_cert_for_loopback(cfg.ca, tmp_path / "leaf.pem"), THREE_CHUNKS, gates, upstream_alpn
    )
    eng, seen, stop = _start(cfg, trust_upstream_ca=cfg.ca.cert)
    try:
        port = eng.listen_port()
        assert port is not None
        agent_alpn, status, frames = _h2_through_the_proxy(
            cfg, port, listener.getsockname()[1], gates
        )
    finally:
        for gate in gates:
            gate.set()
        stop()
        listener.close()
    assert (agent_alpn, upstream_alpn) == ("h2", ["h2"])
    assert (status, frames) == ("200", THREE_CHUNKS)
    (ex,) = seen
    assert (ex.request.scheme, ex.kind, ex.answered_by, ex.flags) == ("https", "llm", "live", ())
    assert ex.response is not None and ex.response.body == b"".join(THREE_CHUNKS)
    assert ex.stream_chunks == tuple(len(chunk) for chunk in THREE_CHUNKS)
