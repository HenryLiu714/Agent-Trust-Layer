"""Every exchange is timed, a check read by its own reader call."""

import time

from tests.support.engine import post_refund, via_proxy
from tests.support.upstreams import SLOW_S


def test_a_live_read_a_faked_write_and_a_lost_upstream_are_each_timed(engine, upstream):
    """Each exchange is timed from the request hook to the hook that finishes it.

    That is `response` for the first two and `error` for the third."""
    eng, seen = engine
    before = time.time()
    via_proxy(eng.listen_port(), "GET", f"http://127.0.0.1:{upstream}/hello")
    via_proxy(eng.listen_port(), "POST", f"http://127.0.0.1:{upstream}/things", body=b"{}")
    via_proxy(eng.listen_port(), "GET", "http://127.0.0.1:1/x")
    after = time.time()
    read, write, lost = seen
    assert (read.answered_by, write.answered_by) == ("live", "fake-L0")
    assert "upstream-error" in lost.flags
    for ex in seen:
        assert 0 < before <= ex.started_at <= ex.ended_at <= after
    assert read.ended_at <= write.started_at <= write.ended_at <= lost.started_at


def test_the_precondition_read_is_timed_by_its_own_reader_call_inside_the_write(
    precondition_stub,
):
    """irimi's check read is timed by its `Reader` call, inside the write it checks.

    It lasts as long as `ch_SLOW1`'s stub takes, and is recorded first, in completion order."""
    proxy, stub, seen = precondition_stub
    status, _, _ = post_refund(proxy, stub, "ch_SLOW1")
    assert status == 200
    issued, write = seen
    assert (issued.issued_by, write.issued_by) == ("engine", "agent")
    assert write.started_at <= issued.started_at <= issued.ended_at <= write.ended_at
    assert issued.ended_at - issued.started_at >= SLOW_S * 0.9
