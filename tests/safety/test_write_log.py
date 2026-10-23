"""The write log holds only writes that would have happened: no precondition rejection, no
retry answered from the first try, no idempotency conflict, and no read irimi issued itself.
Every read the overlay edited sits in the summary under a write the overlay really held."""

import http.client
from dataclasses import replace

from irimi import servicemaps
from irimi.config import paths
from irimi.core.exchange import Exchange, Flag
from irimi.decide.overlay import Overlaid, ServiceOverlay
from irimi.decide.policy import ShadowPolicy
from irimi.protocol.trace import RunRecord
from irimi.report.model import RunView, for_summary
from irimi.report.summary import OVERLAY_MARKER, Live, summary_lines
from tests.support.builders import demo_exchange
from tests.support.engine import run_engine, via_proxy_stamped
from tests.support.invariants import (
    engine_issued_violations,
    overlay_line_violations,
    owes_overlay_line,
    write_log_violations,
)
from tests.support.upstreams import FakeStripe, serve

# The shipped Stripe map's two routes that L3 touches, claimed on loopback: the refund that names
# `charge_refundable`, and the charge read its precondition probe has to classify as.
STRIPE_MAP = """
version: 1
service: stripe
hosts:
  - 127.0.0.1
routes:
  - match:
      method: POST
      path: /v1/refunds
    operation: refunds.create
    kind: write
    human: refund {amount:money} on {charge}
    fixture: refund
    precondition: charge_refundable
    ids:
      id: re_
    fires:
      - refund.created
      - charge.refunded
  - match:
      method: GET
      path: /v1/charges/{charge}
    operation: charges.retrieve
    kind: read
    human: get charge {charge}
"""


def _charge(charge_id, refunded):
    return {
        "id": charge_id,
        "object": "charge",
        "amount": 4900,
        "amount_refunded": refunded,
        "refunded": refunded == 4900,
        "currency": "usd",
    }


# Real Stripe state for the precondition check: `ch_REAL1` is refundable, `ch_FULL1` is fully
# refunded.
CHARGES = {"ch_REAL1": _charge("ch_REAL1", 0), "ch_FULL1": _charge("ch_FULL1", 4900)}


class _LogCapturingOverlay:
    """Keeps each write log it was given, and hands every read back untouched - or to `inner`, the
    real overlay, when there is one. The addon's `write_log` is its own; what an overlay is handed
    is the only view of it a test should take."""

    def __init__(self, inner: ServiceOverlay | None = None) -> None:
        self.logs: list[tuple[Exchange, ...]] = []
        self.inner = inner

    def __call__(
        self, write_log, classification, read_request, upstream_response, translated_from=None
    ):
        self.logs.append(tuple(write_log))
        if self.inner is not None:
            return self.inner(
                write_log, classification, read_request, upstream_response, translated_from
            )
        return Overlaid(upstream_response)

    def rewrite(self, write_log, classification, read_request):
        self.logs.append(tuple(write_log))
        if self.inner is not None:
            return self.inner.rewrite(write_log, classification, read_request)
        return None


def _l3_run(tmp_path, monkeypatch):
    """A refund L3 passes, one it rejects, then a read, through an engine holding the real
    reader. Hands back the exchanges and the overlay that watched the write log."""
    seen, overlay, replies = _stripe_run(
        tmp_path, monkeypatch, [("ch_REAL1", 100, None), ("ch_FULL1", 100, None)]
    )
    assert replies == [200, 400, 200]
    return seen, overlay


def _stripe_run(tmp_path, monkeypatch, refunds, *, real_overlay=False):
    """Each `(charge, amount, idempotency key or None)` in `refunds` as a refund, then a charge
    read, through an engine holding the real reader. Hands back the exchanges, the overlay that
    watched the write log, and every status in order. With `real_overlay` that overlay passes each
    read on to the real `ServiceOverlay`, which applies the accepted refunds to the closing read."""
    from irimi.decide.reader import UpstreamReader

    monkeypatch.setenv(paths.IRIMI_HOME_ENV, str(tmp_path / "maps-home"))
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir(exist_ok=True)
    (maps_dir / "stripe.yaml").write_text(STRIPE_MAP)
    maps = servicemaps.load(cwd=tmp_path, maps_dir=maps_dir)
    stripe = serve(FakeStripe, threaded=True, charges=CHARGES)
    overlay = _LogCapturingOverlay(ServiceOverlay() if real_overlay else None)

    def refund(port, charge, amount, key):
        headers = {
            "host": f"127.0.0.1:{stripe}",
            "content-type": "application/x-www-form-urlencoded",
        }
        if key is not None:
            headers["idempotency-key"] = key
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        conn.request(
            "POST",
            f"http://127.0.0.1:{stripe}/v1/refunds",
            body=f"charge={charge}&amount={amount}".encode(),
            headers=headers,
        )
        resp = conn.getresponse()
        resp.read()
        conn.close()
        return resp.status

    def calls(port):
        return [
            *(refund(port, charge, amount, key) for charge, amount, key in refunds),
            via_proxy_stamped(port, "GET", f"http://127.0.0.1:{stripe}/v1/charges/ch_REAL1")[0],
        ]

    try:
        seen, replies = run_engine(
            tmp_path,
            monkeypatch,
            maps,
            calls,
            policy=ShadowPolicy(reader=UpstreamReader(), maps=maps),
            overlay=overlay,
        )
    finally:
        stripe.stop()
    return seen, overlay, replies


def test_a_rejected_write_is_never_in_the_write_log(tmp_path, monkeypatch):
    """L3 said the real service would have refused it, and the agent got that refusal. In the
    write log it would be replayed onto later reads - an effect for a write that happened in no
    world. Pinned at the log the overlay is handed, not at one of its downstream effects; a passed
    refund goes first so the log is not empty for the trivial reason."""
    seen, overlay = _l3_run(tmp_path, monkeypatch)
    assert [ex.precondition.outcome for ex in seen if ex.kind == "write"] == ["passed", "rejected"]
    assert overlay.logs, "the overlay was never handed the write log"
    for log in overlay.logs:
        assert [ex.precondition.outcome for ex in log] == ["passed"]


def test_a_replayed_write_is_never_in_the_write_log_twice(tmp_path, monkeypatch):
    """The agent's retry with the key it already sent is the same write. A second entry
    would have the overlay apply one refund twice."""
    seen, overlay, replies = _stripe_run(
        tmp_path, monkeypatch, [("ch_REAL1", 100, "k-1"), ("ch_REAL1", 100, "k-1")]
    )
    assert replies == [200, 200, 200]
    writes = [ex for ex in seen if ex.kind == "write"]
    assert [Flag.IDEMPOTENT_REPLAY in ex.flags for ex in writes] == [False, True]
    assert overlay.logs, "the overlay was never handed the write log"
    for log in overlay.logs:
        assert len(log) == 1
        assert Flag.IDEMPOTENT_REPLAY not in log[0].flags


def test_an_idempotency_conflict_is_never_in_the_write_log(tmp_path, monkeypatch):
    """A key reused for a different write was refused, as the real service would refuse it:
    nothing was minted and there is no effect to replay."""
    seen, overlay, replies = _stripe_run(
        tmp_path, monkeypatch, [("ch_REAL1", 100, "k-2"), ("ch_REAL1", 250, "k-2")]
    )
    assert replies == [200, 400, 200]
    writes = [ex for ex in seen if ex.kind == "write"]
    assert [Flag.IDEMPOTENCY_CONFLICT in ex.flags for ex in writes] == [False, True]
    assert overlay.logs, "the overlay was never handed the write log"
    for log in overlay.logs:
        assert len(log) == 1
        assert Flag.IDEMPOTENCY_CONFLICT not in log[0].flags


def test_only_a_write_that_would_have_happened_lists_its_webhooks(tmp_path, monkeypatch):
    """`would_fire` is a claim about a write irimi accepted, and the write log is the run's own
    record of which those are. Said once, as the one-directional property: an exchange that
    lists webhooks is one the overlay was handed as a write. The three that list nothing do so
    for three different reasons - L3 said the service would have refused it, the service would
    have refused the reused key, and a replay's events are already on the first write's own
    exchange."""
    seen, overlay, replies = _stripe_run(
        tmp_path,
        monkeypatch,
        [
            ("ch_REAL1", 100, None),  # accepted
            ("ch_FULL1", 100, None),  # L3 rejection
            ("ch_REAL1", 100, "k-1"),  # accepted
            ("ch_REAL1", 100, "k-1"),  # replay
            ("ch_REAL1", 100, "k-2"),  # accepted
            ("ch_REAL1", 250, "k-2"),  # conflict
        ],
    )
    assert replies == [200, 400, 200, 200, 200, 400, 200]
    writes = [ex for ex in seen if ex.kind == "write"]
    assert [ex.would_fire != () for ex in writes] == [True, False, True, False, True, False]
    for ex in writes:
        if ex.would_fire:
            assert ex.would_fire == ("refund.created", "charge.refunded")
    for ex in seen:
        refused_or_replayed = (
            Flag.IDEMPOTENT_REPLAY in ex.flags
            or Flag.IDEMPOTENCY_CONFLICT in ex.flags
            or ex.precondition.outcome == "rejected"
        )
        if refused_or_replayed or ex.kind == "read":
            assert ex.would_fire == (), (ex.kind, ex.flags, ex.precondition.outcome)

    assert overlay.logs, "the overlay was never handed the write log"
    logged = {(ex.request.path, ex.request.body) for ex in overlay.logs[-1]}
    for ex in writes:
        if ex.would_fire:
            assert (ex.request.path, ex.request.body) in logged


def test_the_write_log_check_passes_a_real_run_and_catches_a_forged_log(tmp_path, monkeypatch):
    """`write_log_violations` over a run holding every kind of write the log must leave out, and
    then over logs that break the rule each way it can be broken."""
    seen, overlay, replies = _stripe_run(
        tmp_path,
        monkeypatch,
        [
            ("ch_REAL1", 100, None),  # accepted
            ("ch_FULL1", 100, None),  # precondition rejection
            ("ch_REAL1", 100, "k-1"),  # accepted
            ("ch_REAL1", 100, "k-1"),  # retry
            ("ch_REAL1", 250, "k-1"),  # conflict
        ],
    )
    assert replies == [200, 400, 200, 200, 400, 200]
    log = overlay.logs[-1]
    assert write_log_violations(seen, log) == []

    writes = [ex for ex in seen if ex.kind == "write"]
    accepted, rejected, _, retry, conflict = writes
    read = next(ex for ex in seen if ex.kind == "read" and ex.issued_by == "agent")
    for forged in (read, rejected, retry, conflict, accepted):
        assert len(write_log_violations(seen, [*log, forged])) == 1, forged.flags
    assert len(write_log_violations(seen, log[1:])) == 1  # an accepted write left out


def test_every_overlaid_read_has_a_write_of_its_own_service_to_sit_under(tmp_path, monkeypatch):
    """An overlaid read shows the agent a write that never happened, and the summary's `↳` line
    is the only place a reader learns which of the agent's reads were shown one. The report
    files each under the latest earlier write of its own service and quietly skips one it cannot
    place; that skip is safe only because a real run never produces such a read - the overlay
    edits or flags a read only on the strength of a same-service write in the log, and every entry
    there is a printed write. And each is filed under a write the overlay really held: the run's
    L3 rejection falls between the accepted refund and the read, and a refused write is printed
    but never logged, so a `↳` under the `✗` would say the agent saw a refund that was refused.
    Held over a real run through the real overlay."""
    seen, overlay, replies = _stripe_run(
        tmp_path,
        monkeypatch,
        [("ch_REAL1", 100, None), ("ch_FULL1", 100, None)],
        real_overlay=True,
    )
    assert replies == [200, 400, 200]
    owed = [ex for ex in seen if owes_overlay_line(ex)]
    assert [ex.answered_by for ex in owed] == ["overlay"], "the run showed the agent no write"
    held = [ex for log in overlay.logs for ex in log]
    assert [ex.precondition.outcome for ex in held] == ["passed"] * len(held)
    assert overlay_line_violations(seen, held) == []
    view = RunView(RunRecord.header("t3st"), [*map(for_summary, seen)])
    lines = summary_lines(view, None, live=Live())
    assert sum(line.lstrip().startswith(OVERLAY_MARKER) for line in lines) == len(owed)

    # And the check can fail: a read before any write, one whose service wrote nothing, and one
    # filed under a write the overlay never held.
    early, elsewhere = replace(owed[0]), replace(owed[0], service="slack")
    assert len(overlay_line_violations([early, *seen, elsewhere], held)) == 2
    assert len(overlay_line_violations(seen, [])) == 1


def test_a_replayed_rejection_is_never_in_the_write_log(tmp_path, monkeypatch):
    """A retry of a write L3 rejected gets the same refusal back, and stays out of the log on
    both counts: it is a rejection and it is a replay. A passed refund goes first so
    the log is not empty for the trivial reason."""
    seen, overlay, replies = _stripe_run(
        tmp_path,
        monkeypatch,
        [("ch_REAL1", 100, None), ("ch_FULL1", 100, "k-3"), ("ch_FULL1", 100, "k-3")],
    )
    assert replies == [200, 400, 400, 200]
    writes = [ex for ex in seen if ex.kind == "write"]
    assert [ex.precondition.outcome for ex in writes] == ["passed", "rejected", "rejected"]
    assert Flag.IDEMPOTENT_REPLAY in writes[-1].flags
    assert overlay.logs, "the overlay was never handed the write log"
    for log in overlay.logs:
        assert [ex.precondition.outcome for ex in log] == ["passed"]


def test_an_engine_issued_exchange_is_never_a_write(tmp_path, monkeypatch):
    seen, _ = _l3_run(tmp_path, monkeypatch)
    assert [ex.issued_by for ex in seen].count("engine") == 2
    assert engine_issued_violations(seen) == []
    # And the check can fail: an engine-issued write is caught.
    forged = replace(demo_exchange("fake-L0", kind="write"), issued_by="engine")
    assert engine_issued_violations([forged]) != []
