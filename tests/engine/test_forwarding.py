"""The engine forwards reads live and fakes writes, through the forward proxy."""

import asyncio
import http.client
import json
import socket
from pathlib import Path

import pytest

from irimi.config import ca, paths
from irimi.core.exchange import Response
from irimi.decide.overlay import NoOverlay, Overlaid
from irimi.decide.policy import ShadowPolicy
from irimi.engine import EngineConfig, EngineStartError
from irimi.engine.mitm import MitmEngine
from tests.support.doubles import NullStore
from tests.support.engine import engine_config, start_engine, via_proxy
from tests.support.maps import SLACK_MAP, map_index


def test_a_get_is_forwarded_live(engine, upstream):
    eng, seen = engine
    status, data = via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    assert (status, data) == (200, b"hello from upstream")
    assert len(seen) == 1
    ex = seen[0]
    assert ex.answered_by == "live"
    assert ex.kind == "read"
    assert ex.service == "127.0.0.1"
    assert ex.operation == "GET /hello"
    assert ex.run_id == "t3st"
    assert ex.flags == ()
    assert ex.door == "forward"
    assert ex.request.port == upstream  # a loopback upstream on another port is not the listener
    assert ex.response.status == 200


def test_a_post_is_faked_with_an_echo(engine, upstream):
    eng, seen = engine
    status, data = via_proxy(
        eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b'{"a":1}'
    )
    assert status == 200
    body = json.loads(data)
    assert body["a"] == 1  # the echo reflects the request's own fields
    assert sorted(body) == ["a"]  # unmapped: nothing minted, and no other service's fields
    ex = seen[0]
    assert ex.answered_by == "fake-L0"
    assert ex.kind == "unknown"
    assert "unclassified" in ex.flags


def test_mapped_write_is_named_and_faked(tmp_path, monkeypatch, upstream):
    """A mapped write is classified by its route from the maps on EngineConfig, not by its verb."""
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch))
    eng, seen, stop = start_engine(cfg)
    try:
        status, data = via_proxy(
            eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b'{"a":1}'
        )
    finally:
        stop()
    assert status == 200
    assert json.loads(data)["a"] == 1  # the upstream's do_POST would have been a 500
    ex = seen[0]
    assert (ex.service, ex.operation, ex.kind) == ("demo", "things.create", "write")
    assert ex.answered_by == "fake-L0"
    assert ex.flags == ()


def test_mapped_read_is_named_and_forwarded(tmp_path, monkeypatch, upstream):
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch))
    eng, seen, stop = start_engine(cfg)
    try:
        status, data = via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    finally:
        stop()
    assert (status, data) == (200, b"hello from upstream")
    ex = seen[0]
    assert (ex.service, ex.operation, ex.kind) == ("demo", "things.list", "read")
    assert ex.answered_by == "live"
    assert ex.flags == ()


def test_a_slack_read_raises_the_ts_watermark(tmp_path, monkeypatch, upstream):
    """A Slack read raises the `ts` clock, so a minted `ts` sorts after messages already read."""
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch, doc=SLACK_MAP))
    eng, seen, stop = start_engine(cfg)
    try:
        status, _ = via_proxy(
            eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/slack-history"
        )
    finally:
        stop()
    assert status == 200
    assert (seen[0].service, seen[0].kind) == ("slack", "read")
    whole, _, fraction = eng.policy.slack_ts.mint().partition(".")
    assert (int(whole), int(fraction)) > (1_999_999_999, 500)


def test_an_upstream_error_is_flagged(engine, upstream):
    eng, seen = engine
    status, _ = via_proxy(eng.listen_port(), "GET", "http://127.0.0.1:1/x")
    assert status == 502
    assert seen[0].response is None
    assert "upstream-error" in seen[0].flags


def test_mitmproxys_ca_bundle_is_written_from_irimis_ca(engine, upstream):
    bundle = paths.mitm_dir() / "mitmproxy-ca.pem"
    assert bundle.is_file()
    assert ca.ca_paths().cert.read_bytes() in bundle.read_bytes()


# `rot13` is an encoding mitmproxy raises TypeError for rather than ValueError.
@pytest.mark.parametrize("content_encoding", ["gzip", "rot13"])
def test_undecodable_request_body_is_still_faked(engine, upstream, content_encoding):
    # A strict body decode raising inside the hook would make mitmproxy forward the write.
    eng, seen = engine
    status, data = via_proxy(
        eng.listen_port(),
        "POST",
        f"http://127.0.0.1:{upstream}/things",
        body=b'{"not":"gzip"}',
        extra_headers={"content-encoding": content_encoding},
    )
    assert status == 200  # the upstream answers every POST with 500, so it was not reached
    assert json.loads(data)["not"] == "gzip"  # reflected from the body we could not decode
    assert [ex.answered_by for ex in seen] == ["fake-L0"]
    assert seen[0].request.body == b'{"not":"gzip"}'


def test_undecodable_upstream_body_is_still_recorded(engine, upstream):
    eng, seen = engine
    status, data = via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/badgzip")
    assert (status, data) == (200, b"not-gzip")
    assert len(seen) == 1
    assert seen[0].answered_by == "live"
    assert seen[0].response.body == b"not-gzip"


@pytest.mark.parametrize("content_encoding", ["rot13", "utf-8"])
def test_an_upstream_body_in_an_encoding_that_is_not_a_decoding_is_still_recorded(
    engine, upstream, content_encoding
):
    """A body in a text codec (`rot13`, `utf-8`) is recorded as it came, like an undecodable one.

    mitmproxy raises TypeError for `rot13`, and `utf-8` decodes to a str, which is no body."""
    eng, seen = engine
    url = f"http://127.0.0.1:{upstream}/encoded/{content_encoding}"
    status, data = via_proxy(eng.listen_port(), "GET", url)
    assert (status, data) == (200, b"not-" + content_encoding.encode())
    (ex,) = seen
    assert (ex.answered_by, ex.flags) == ("live", ())
    assert ex.response is not None and ex.response.body == data


def test_an_overlays_answer_reaches_the_client(tmp_path, monkeypatch, upstream):
    class _Overlay:
        def __call__(
            self, write_log, classification, read_request, upstream_response, translated_from=None
        ):
            return Overlaid(Response(200, (("content-type", "text/plain"),), b"OVERLAID"))

        def rewrite(self, write_log, classification, read_request):
            return None

    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch), overlay=_Overlay())
    try:
        # The overlay is asked only once there is a write to show.
        via_proxy(eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")
        status, data = via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    finally:
        stop()
    assert (status, data) == (200, b"OVERLAID")
    assert seen[-1].response.body == b"OVERLAID"


@pytest.mark.parametrize("content_encoding", ["rot13", "utf-8"])
def test_an_overlaid_read_in_an_encoding_mitmproxy_cannot_apply_still_reaches_the_client(
    tmp_path, monkeypatch, upstream, content_encoding
):
    """An overlaid body goes out as the overlay built it, whatever `content-encoding` it keeps.

    mitmproxy would re-encode it per that header, and raises TypeError for a text codec."""

    class _Overlay:
        def __call__(
            self, write_log, classification, read_request, upstream_response, translated_from=None
        ):
            body = upstream_response.body + b"-OVERLAID"
            return Overlaid(Response(200, upstream_response.headers, body))

        def rewrite(self, write_log, classification, read_request):
            return None

    eng, seen, stop = start_engine(engine_config(tmp_path, monkeypatch), overlay=_Overlay())
    url = f"http://127.0.0.1:{upstream}/encoded/{content_encoding}"
    try:
        via_proxy(eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")
        status, data = via_proxy(eng.listen_port(), "GET", url)
    finally:
        stop()
    assert (status, data) == (200, b"not-" + content_encoding.encode() + b"-OVERLAID")
    assert len(seen) == 2 and seen[-1].response is not None
    assert (seen[-1].answered_by, seen[-1].response.body) == ("overlay", data)


def test_repeated_headers_survive_a_local_answer(tmp_path, monkeypatch, upstream):
    class _Overlay:
        def __call__(
            self, write_log, classification, read_request, upstream_response, translated_from=None
        ):
            return Overlaid(Response(200, (("set-cookie", "a=1"), ("set-cookie", "b=2")), b""))

        def rewrite(self, write_log, classification, read_request):
            return None

    eng, _seen, stop = start_engine(engine_config(tmp_path, monkeypatch), overlay=_Overlay())
    try:
        # The overlay is asked only once there is a write to show.
        via_proxy(eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")
        conn = http.client.HTTPConnection("127.0.0.1", eng.listen_port(), timeout=10)
        conn.request("GET", f"http://127.0.0.1:{upstream}/hello", headers={"host": "127.0.0.1"})
        resp = conn.getresponse()
        resp.read()
        cookies = resp.headers.get_all("set-cookie")
        conn.close()
    finally:
        stop()
    assert cookies == ["a=1", "b=2"]


def test_port_in_use_raises_engine_start_error(tmp_path, monkeypatch):
    """A taken port is reported in irimi's words, naming `--port` and never mitmproxy's `--mode`."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    port = sock.getsockname()[1]
    cfg = engine_config(tmp_path, monkeypatch, port=port)
    eng = MitmEngine(cfg, ShadowPolicy(), NullStore(), NoOverlay())
    said = (
        f"port {port} on 127.0.0.1 is in use; pick another with --port, or --port 0 for any "
        "free port"
    )

    async def go():
        task = asyncio.ensure_future(eng.run())
        with pytest.raises(EngineStartError) as refused:
            await asyncio.wait_for(eng.wait_ready(), 15)
        assert str(refused.value) == said
        with pytest.raises(EngineStartError):
            await asyncio.wait_for(task, 15)

    try:
        asyncio.run(go())
    finally:
        sock.close()
    assert eng.listen_port() is None


def test_shutdown_before_run_makes_run_return(tmp_path, monkeypatch):
    eng = MitmEngine(engine_config(tmp_path, monkeypatch), ShadowPolicy(), NullStore(), NoOverlay())

    async def go():
        task = asyncio.ensure_future(eng.run())
        eng.shutdown()
        await asyncio.wait_for(task, 15)
        with pytest.raises(EngineStartError, match="stopped before"):
            await eng.wait_ready()

    asyncio.run(go())


def test_the_engine_never_closes_the_store_even_when_setup_fails(tmp_path, monkeypatch):
    """The engine never closes the store, which the composition root opens and closes."""
    closed = []

    class _Store:
        def record(self, ex):
            pass

        def close(self):
            closed.append(True)

    cfg = engine_config(tmp_path, monkeypatch)
    cfg.ca.key.unlink()  # bundle write fails before mitmproxy starts
    eng = MitmEngine(cfg, ShadowPolicy(), _Store(), NoOverlay())  # type: ignore[arg-type]
    with pytest.raises(FileNotFoundError):
        asyncio.run(eng.run())
    assert closed == []


def test_faked_write_reflects_a_form_body_and_is_flagged(engine, upstream):
    """The echo of a form-encoded write, as stripe-python posts it, reaches the client."""
    eng, seen = engine
    status, data = via_proxy(
        eng.listen_port(),
        "POST",
        f"http://127.0.0.1:{upstream}/things",
        body=b"amount=4900&charge=ch_test",
        extra_headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert status == 200
    body = json.loads(data)
    assert body["amount"] == 4900
    assert body["charge"] == "ch_test"
    assert "created" not in body
    ex = seen[0]
    assert ex.answered_by == "fake-L0"


def test_live_read_carries_no_fidelity_flag(engine, upstream):
    eng, seen = engine
    via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    assert seen[0].answered_by == "live"
    assert seen[0].flags == ()


@pytest.mark.parametrize(
    ("cause", "said"),
    [
        (
            OSError(13, "http(s) proxy failed to listen on 127.0.0.1:1 with [Errno 13] denied"),
            "could not listen on 127.0.0.1:1: permission denied",
        ),
        (
            OSError("http(s) proxy failed to listen: [Errno None] no route"),
            "could not listen on 127.0.0.1:1",
        ),
        (None, "could not listen on 127.0.0.1:1"),
        (RuntimeError("loop closed"), "could not listen on 127.0.0.1:1: loop closed"),
    ],
)
def test_a_listener_that_did_not_bind_is_told_in_irimi_s_words(
    cause: BaseException | None, said: str
) -> None:
    """mitmproxy's strerror is its own sentence; only the OS's reason for the errno is irimi's."""
    from irimi.engine.mitm import _start_failure

    config = EngineConfig(
        run_id="t3st", ca=ca.ca_paths(), confdir=Path("."), listen_host="127.0.0.1", listen_port=1
    )
    assert _start_failure(config, cause) == said
