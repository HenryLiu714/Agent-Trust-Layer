"""The overlay through the engine: a run's reads show its own faked writes, and no one else's."""

from irimi.core.exchange import Flag, Response
from irimi.decide.overlay import Overlaid
from irimi.decide.policy import ShadowPolicy
from irimi.decide.writelog import Translation
from irimi.engine.mitm import IrimiAddon
from irimi.protocol.headers import ANSWERED_BY_HEADER, REWROTE_HEADER, RUN_HEADER
from tests.support.engine import engine_config, read_via_proxy, start_engine, via_proxy
from tests.support.maps import map_index


class _OverlayDouble:
    """An overlay whose answers a test chooses, counting how often the engine asked it."""

    def __init__(self, answer=None, rewrite=None):
        self._answer = answer or Overlaid
        self._rewrite = rewrite or (lambda request: None)
        self.calls = 0
        self.rewrites = 0
        self.translated_from: list[str | None] = []

    def __call__(
        self, write_log, classification, read_request, upstream_response, translated_from=None
    ):
        self.calls += 1
        self.translated_from.append(translated_from)
        return self._answer(upstream_response)

    def rewrite(self, write_log, classification, read_request):
        self.rewrites += 1
        return self._rewrite(read_request)


def _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay):
    """An engine over DEMO_MAP whose write log already holds one faked write."""
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch))
    eng, seen, stop = start_engine(cfg, overlay=overlay)
    try:
        addon = next(a for a in eng._master.addons.chain if isinstance(a, IrimiAddon))
        via_proxy(eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")
        assert addon.session.write_log.for_run("t3st"), (
            "the overlay is asked only once there is a write"
        )
    except BaseException:
        stop()
        raise
    return eng, seen, stop


def test_a_run_s_requests_see_its_own_writes_alone(tmp_path, monkeypatch, upstream):
    """A faked write reaches the policy and overlay for its own run's requests, never another's."""
    decided = []
    overlaid = []

    class _Seeing(ShadowPolicy):
        def decide(self, request, classification, write_log=(), run_id="", target=None):
            decided.append((run_id, request.method, tuple(write_log)))
            return super().decide(request, classification, write_log, run_id, target)

    class _Overlay(_OverlayDouble):
        def __call__(self, write_log, *args, **kwargs):
            overlaid.append(tuple(write_log))
            return super().__call__(write_log, *args, **kwargs)

    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch))
    eng, seen, stop = start_engine(cfg, overlay=_Overlay(), policy=_Seeing())
    base = f"http://127.0.0.1:{upstream}"
    try:
        for run, method, path in [
            ("run-a", "POST", "/things"),
            ("run-b", "GET", "/hello"),
            ("run-a", "GET", "/hello"),
            ("run-b", "POST", "/things"),
        ]:
            body = b"{}" if method == "POST" else None
            headers = {RUN_HEADER: run}
            via_proxy(eng.listen_port(), method, base + path, body=body, extra_headers=headers)
    finally:
        stop()
    write_a = seen[0]
    assert (write_a.run_id, write_a.kind) == ("run-a", "write")
    assert decided == [
        ("run-a", "POST", ()),
        ("run-b", "GET", ()),
        ("run-a", "GET", (write_a,)),
        ("run-b", "POST", ()),
    ]
    assert overlaid == [(write_a,)]


def _overlaid_read(tmp_path, monkeypatch, upstream, overlay):
    """One faked write, then one live read, through an engine using `overlay`."""
    eng, seen, stop = _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay)
    try:
        status, headers, data = read_via_proxy(
            eng.listen_port(), f"http://127.0.0.1:{upstream}/hello"
        )
    finally:
        stop()
    return status, headers, data, seen[-1]


def test_an_overlay_that_changes_a_read_stamps_it_overlay(tmp_path, monkeypatch, upstream):
    changed = Response(200, (("content-type", "application/json"),), b'{"overlaid": true}')
    overlay = _OverlayDouble(answer=lambda upstream: Overlaid(changed, "full"))
    status, headers, data, ex = _overlaid_read(tmp_path, monkeypatch, upstream, overlay)
    assert (status, data) == (200, b'{"overlaid": true}')
    assert headers[ANSWERED_BY_HEADER] == "overlay"
    assert ex.answered_by == "overlay"
    assert ex.overlay == "full"


def test_an_overlay_that_changes_nothing_leaves_the_read_live_and_unstamped(
    tmp_path, monkeypatch, upstream
):
    overlay = _OverlayDouble(answer=Overlaid)
    status, headers, data, ex = _overlaid_read(tmp_path, monkeypatch, upstream, overlay)
    assert overlay.calls == 1
    assert (status, data) == (200, b"hello from upstream")
    assert ANSWERED_BY_HEADER not in headers
    assert ex.answered_by == "live"
    assert ex.overlay is None
    assert ex.response.body == b"hello from upstream"


def test_an_overlay_may_flag_a_read_partial_without_touching_it(tmp_path, monkeypatch, upstream):
    overlay = _OverlayDouble(answer=lambda upstream: Overlaid(upstream, "partial"))
    status, headers, data, ex = _overlaid_read(tmp_path, monkeypatch, upstream, overlay)
    assert (status, data) == (200, b"hello from upstream")
    assert ANSWERED_BY_HEADER not in headers
    assert ex.answered_by == "live"
    assert ex.overlay == "partial"


def test_an_overlay_that_raises_does_not_break_the_read(tmp_path, monkeypatch, upstream):
    def boom(upstream):
        raise RuntimeError("the overlay exploded")

    overlay = _OverlayDouble(answer=boom)
    status, headers, data, ex = _overlaid_read(tmp_path, monkeypatch, upstream, overlay)
    assert overlay.calls == 1
    assert (status, data) == (200, b"hello from upstream")
    assert ANSWERED_BY_HEADER not in headers
    assert ex.answered_by == "live"
    assert ex.overlay == "partial"


def test_a_rewritten_read_reaches_the_upstream_translated_and_is_recorded_that_way(
    tmp_path, monkeypatch, upstream
):
    """A translated read reaches the service and the trace without any header of irimi's.

    What the translation dropped is handed to the overlay with the response."""
    from dataclasses import replace

    def translate(request):
        return Translation(replace(request, query="limit=1"), "starting_after=re_MINTED1")

    overlay = _OverlayDouble(rewrite=translate)
    eng, seen, stop = _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay)
    try:
        status, _, _ = read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello?limit=1&starting_after=re_MINTED1",
        )
    finally:
        stop()
    assert status == 200
    ((path, received),) = upstream.seen
    assert path == "/hello?limit=1"
    assert REWROTE_HEADER not in {k.lower() for k, _ in received}
    assert overlay.translated_from == ["starting_after=re_MINTED1"]
    ex = seen[-1]
    assert ex.request.query == "limit=1"
    assert REWROTE_HEADER not in {k for k, _ in ex.request.headers}


def test_a_rewrite_that_raises_forwards_the_read_unchanged(tmp_path, monkeypatch, upstream):
    def boom(request):
        raise RuntimeError("the rewrite exploded")

    overlay = _OverlayDouble(rewrite=boom)
    eng, seen, stop = _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay)
    try:
        status, _, data = read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello?limit=1&starting_after=re_MINTED1",
        )
    finally:
        stop()
    assert overlay.rewrites == 1
    assert (status, data) == (200, b"hello from upstream")
    assert [path for path, _ in upstream.seen] == ["/hello?limit=1&starting_after=re_MINTED1"]
    ex = seen[-1]
    assert ex.answered_by == "live"
    assert Flag.DECISION_FAILED not in ex.flags


def test_the_overlay_is_not_asked_about_a_read_before_any_write(tmp_path, monkeypatch, upstream):
    overlay = _OverlayDouble()
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch))
    eng, seen, stop = start_engine(cfg, overlay=overlay)
    try:
        status, _ = via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    finally:
        stop()
    assert status == 200
    # The rewrite gate stays closed until the write log holds a write: the engine strips the
    # header itself, so the overlay sees no read before then.
    assert (overlay.calls, overlay.rewrites) == (0, 0)
    assert seen[-1].overlay is None


def test_a_rewrote_header_the_agent_sent_is_not_forwarded_upstream(tmp_path, monkeypatch, upstream):
    """An agent's own `irimi-rewrote` is stripped before the overlay is asked about a read."""
    overlay = _OverlayDouble()
    eng, _seen, stop = _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay)
    try:
        status, _, _ = read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello",
            extra_headers={REWROTE_HEADER: "starting_after=re_FORGED"},
        )
    finally:
        stop()
    assert status == 200
    ((path, received),) = upstream.seen
    assert path == "/hello"
    assert REWROTE_HEADER not in {k.lower() for k, _ in received}


def test_a_rewrote_header_the_agent_sent_is_stripped_before_the_runs_first_write(
    tmp_path, monkeypatch, upstream
):
    """The engine strips every agent-sent `irimi-rewrote` even while the write log is empty.

    `NoOverlay` translates nothing, so only the engine can have removed both spellings."""
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch))
    eng, _seen, stop = start_engine(cfg)
    try:
        addon = next(a for a in eng._master.addons.chain if isinstance(a, IrimiAddon))
        status, _, _ = read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello",
            extra_headers={
                REWROTE_HEADER: "starting_after=re_FORGED",
                "Irimi-Rewrote": "starting_after=re_FORGED2",
            },
        )
        assert addon.session.write_log.for_run("t3st") == (), (
            "this test is about the empty-log path"
        )
    finally:
        stop()
    assert status == 200
    ((path, received),) = upstream.seen
    assert path == "/hello"
    assert REWROTE_HEADER not in {k.lower() for k, _ in received}


def test_a_rewrote_header_the_agent_sent_is_kept_out_of_the_recorded_request(
    tmp_path, monkeypatch, upstream
):
    """The recorded request omits an agent-sent `irimi-rewrote`, which irimi never sent."""
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch))
    eng, seen, stop = start_engine(cfg)
    try:
        read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello",
            extra_headers={REWROTE_HEADER: "starting_after=re_FORGED"},
        )
    finally:
        stop()
    assert [k for k, _ in seen[-1].request.headers if k == REWROTE_HEADER] == []


def test_no_rewrote_header_reaches_the_upstream_on_a_translated_read(
    tmp_path, monkeypatch, upstream
):
    """A translated read carries neither the agent's `irimi-rewrote` nor one of irimi's own.

    The overlay learns what was dropped from the engine, never from a header."""
    from dataclasses import replace

    def translate(request):
        assert all(k != REWROTE_HEADER for k, _ in request.headers), (
            "the engine strips the agent's before the overlay is asked"
        )
        return Translation(replace(request, query="limit=1"), "starting_after=re_MINTED1")

    overlay = _OverlayDouble(rewrite=translate)
    eng, seen, stop = _engine_with_a_write(tmp_path, monkeypatch, upstream, overlay)
    try:
        status, _, _ = read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/hello?limit=1&starting_after=re_MINTED1",
            extra_headers={REWROTE_HEADER: "starting_after=re_FORGED"},
        )
    finally:
        stop()
    assert status == 200
    ((path, received),) = upstream.seen
    assert path == "/hello?limit=1"
    assert [v for k, v in received if k.lower() == REWROTE_HEADER] == []
    assert [v for k, v in seen[-1].request.headers if k == REWROTE_HEADER] == []
    assert overlay.translated_from == ["starting_after=re_MINTED1"]


def test_a_write_carrying_a_rewrote_header_is_still_faked_and_records_none(
    tmp_path, monkeypatch, upstream
):
    """A faked write's recorded request omits an agent-sent `irimi-rewrote`."""
    cfg = engine_config(tmp_path, monkeypatch, maps=map_index(tmp_path, monkeypatch))
    eng, seen, stop = start_engine(cfg)
    try:
        status, headers, _ = read_via_proxy(
            eng.listen_port(),
            f"http://127.0.0.1:{upstream}/things",
            extra_headers={
                REWROTE_HEADER: "starting_after=re_FORGED",
                "content-type": "application/json",
            },
            method="POST",
            body=b"{}",
        )
    finally:
        stop()
    assert status == 200
    assert headers[ANSWERED_BY_HEADER] == "fake-L0"
    ex = seen[-1]
    assert ex.kind == "write"
    assert [k for k, _ in ex.request.headers if k == REWROTE_HEADER] == []
