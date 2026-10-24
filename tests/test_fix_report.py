"""Two writes checked at once each say why their own check failed; a 401's webhooks are left out."""

import pytest

from irimi.cli.compose import maps_for_stored_runs
from irimi.core.exchange import Exchange, Precondition, Request, Response
from irimi.protocol.trace import RunRecord
from irimi.report.model import RunView
from irimi.report.summary import summary_lines

FORM = (("content-type", "application/x-www-form-urlencoded"),)


def _refund(charge: str, started_at: float, ended_at: float) -> Exchange:
    body = f"charge={charge}&amount=500&currency=usd".encode()
    return Exchange(
        Request("POST", "https", "api.stripe.com", 443, "/v1/refunds", "", FORM, body),
        Response(200, (), b"{}"),
        "stripe",
        "refunds.create",
        "write",
        "fake-L1",
        "run1",
        precondition=Precondition("not_evaluable"),
        would_fire=(f"refund.created.{charge}",),
        started_at=started_at,
        ended_at=ended_at,
    )


def _check(charge: str, status: int, started_at: float) -> Exchange:
    path = f"/v1/charges/{charge}"
    return Exchange(
        Request("GET", "https", "api.stripe.com", 443, path, "", (), b""),
        Response(status, (), b"{}"),
        "stripe",
        "charges.retrieve",
        "read",
        "live",
        "run1",
        issued_by="engine",
        started_at=started_at,
        ended_at=started_at + 0.01,
    )


@pytest.mark.parametrize("a_checked_first", [True, False])
def test_two_writes_checked_at_once_each_take_their_own_check_read(a_checked_first):
    """Writes decided at once each take their own check read, in either order (429, then 401)."""
    a_at, b_at = (1.1, 1.2) if a_checked_first else (1.2, 1.1)
    events = sorted(
        [_check("ch_A1", 429, a_at), _check("ch_B2", 401, b_at)], key=lambda e: e.started_at
    )
    events += [_refund("ch_A1", 1.0, 2.0), _refund("ch_B2", 1.05, 2.1)]
    lines = summary_lines(RunView(RunRecord.header("run1"), events), maps_for_stored_runs())
    writes = [line.strip() for line in lines if "refund $5.00" in line]
    assert writes == [
        "○ stripe  refund $5.00 on ch_A1  faked; could not check: "
        "stripe answered irimi's check read with 429",
        "○ stripe  refund $5.00 on ch_B2  faked; could not check: "
        "stripe answered irimi's check read with 401",
    ]
    assert "  would have fired: refund.created.ch_A1" in lines
