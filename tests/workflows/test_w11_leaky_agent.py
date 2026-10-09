"""W11 `leaky_agent`: each escape's write really lands, and irimi never sees it.

LOOKS WRONG: every escape assertion here pins today's limit. When Phase 4's readiness checks land,
each flips from "irimi printed nothing" to "irimi flagged it", and the test says which check flagged
which escape.
"""

import pytest

from examples.workflows.w11_leaky_agent.agent import CHANNEL, CHARGE
from irimi.exchange import Exchange

W = "w11_leaky_agent"
NOTHING_SEEN = "0 exchanges · 0 live · 0 delegated · 0 virtualized"

ESCAPES = {
    "proxyless_client": ("POST", "api.stripe.com", "/v1/refunds"),
    "no_proxy_star": ("POST", "api.stripe.com", "/v1/refunds"),
    "raw_socket": ("POST", "slack.com", "/api/chat.postMessage"),
    "loopback_service": ("POST", "127.0.0.1", "/queue/jobs"),
}


@pytest.mark.parametrize("escape", sorted(ESCAPES))
def test_an_escape_writes_for_real_and_irimi_never_sees_it(run_workflow, escape):
    shadow = run_workflow(W, escape, "shadow")
    assert shadow.exit_code == 0
    assert [(r.method, r.host, r.path) for r in shadow.internet.writes()] == [ESCAPES[escape]]
    # No exchange line, and a summary that counts nothing and claims no write did not happen.
    assert shadow.exchange_lines() == []
    summary = "\n".join(shadow.summary())
    assert NOTHING_SEEN in summary
    assert "did not happen" not in summary
    # The agent got the real service's answer, unstamped: it cannot tell it escaped.
    assert [c["answered_by"] for c in shadow.calls()] == [None]
    # Nor can a stored run (#70): the escape's own run, which the SDK started and ended (#74),
    # and the process run are both `ok`, with no event at all. The escaped request carried no
    # `Irimi-Run` either (universal invariant 2): the SDK labels only what goes through irimi
    # (#75).
    reader = shadow.stored()
    runs = {r.attribution: r for r in reader.list_runs()}
    assert sorted(runs) == ["process", "sdk"]
    assert {r.outcome for r in runs.values()} == {"ok"}
    trigger = runs["sdk"].trigger
    assert trigger is not None and (trigger.name, trigger.args) == ("escape", {"escape": escape})
    assert shadow.stored_events() == []
    assert not [r for r in shadow.internet.requests() if "irimi-run" in r.headers]
    # The escape is a leak in both modes alike: shadow changed nothing about it.
    bare = run_workflow(W, escape, "bare")
    assert [(r.method, r.host, r.path) for r in bare.internet.writes()] == [ESCAPES[escape]]


def test_the_escaped_refund_really_changed_the_ledger(run_workflow):
    for escape in ("proxyless_client", "no_proxy_star"):
        stripe = run_workflow(W, escape, "shadow").world.stripe
        assert stripe.charges[CHARGE]["amount_refunded"] == 500
        assert [r["charge"] for r in stripe.refunds] == [CHARGE]


def test_the_raw_socket_post_is_in_the_channel(run_workflow):
    slack = run_workflow(W, "raw_socket", "shadow").world.slack
    assert [m["text"] for m in slack.messages[CHANNEL]] == ["posted around irimi"]


def test_irimis_own_no_proxy_default_exempts_a_loopback_sidecar(run_workflow):
    shadow = run_workflow(W, "loopback_service", "shadow")
    [sidecar] = [s for s in shadow.world.extra if s.hosts == ("127.0.0.1",)]
    assert sidecar.state["jobs"] == [{"job": "refund", "charge": CHARGE}]
    # It went straight there: not through any proxy, irimi's or the harness's.
    assert [r.via_proxy for r in shadow.internet.requests()] == [False]


def test_the_control_through_the_proxy_is_faked_and_the_ledger_is_untouched(run_workflow):
    shadow = run_workflow(W, "proxied", "shadow")
    # Through irimi, the refund is labelled with its run and stored there with irimi's L3 read of
    # the charge it names, and the process run holds nothing (#75).
    [run] = shadow.sdk_runs()
    events = shadow.stored().load_run(run.run_id).events
    assert sorted(
        (e.request.method, e.request.path) for e in events if isinstance(e, Exchange)
    ) == [
        ("GET", f"/v1/charges/{CHARGE}"),
        ("POST", "/v1/refunds"),
    ]
    assert shadow.stored().load_run(shadow.process_run_id()).events == []
    assert shadow.internet.writes() == []
    assert shadow.answered() == [("refund", 200, "fake-L1")]
    assert shadow.world.stripe.charges[CHARGE]["amount_refunded"] == 0
    assert "fake-L1   write     POST api.stripe.com/v1/refunds -> 200  [fidelity:L1]" in (
        shadow.exchange_lines()
    )
    assert "○ refund $5.00 on ch_LEAKY  unvalidated (L3 preconditions passed)" in "\n".join(
        shadow.summary()
    )


def test_a_redirect_off_irimis_route_is_labelled_on_each_hop_through_irimi_and_on_no_other(
    run_workflow,
):
    """`redirected`: urllib, `requests`, httpx and async httpx each read an export that redirects
    twice, inside one `sdk.run`: to `/export/v2` on the same host, through irimi, then to the
    sidecar on loopback, which irimi's `NO_PROXY` sends direct. The SDK labels a hop by where its
    own connection goes (#75): both hops through irimi carry the run and are stored in it, and the
    hop to the sidecar goes out with no `Irimi-Run`. httpx builds a redirect from the request it
    just sent, so the SDK must take its label off once sent, or the label rides on to the sidecar
    (universal invariant 2 and the check below).

    `requests` is the exception on the last hop: it keeps the first request's proxy across
    redirects whatever `NO_PROXY` says, so its hop to the sidecar goes through irimi too, labelled,
    and is stored in the run, stripped. That is `requests`' behaviour, pinned so a change in it
    shows; irimi does the right thing either way."""
    shadow = run_workflow(W, "redirected", "shadow")
    bare = run_workflow(W, "redirected", "bare")
    assert shadow.exit_code == bare.exit_code == 0
    for result in (shadow, bare):
        landed = [(e["client"], e["doc"], e.get("redirects")) for e in result.events("landed")]
        assert landed == [
            ("urllib", {"queued": 0}, None),
            ("requests", {"queued": 0}, 2),
            ("httpx", {"queued": 0}, 2),
            ("httpx-async", {"queued": 0}, 2),
        ]
    assert [(c["label"], c["status"], c["answered_by"]) for c in shadow.calls()] == [
        (f"export:{client}", 200, None) for client in ("urllib", "requests", "httpx", "httpx-async")
    ]
    # Nothing that reached a service carried the run: not the hops irimi stripped, nor the ones
    # that went around it.
    assert [(r.host, r.path) for r in shadow.internet.requests() if "irimi-run" in r.headers] == []
    assert [r.path for r in shadow.internet.requests("127.0.0.1")] == ["/queue/stats"] * 4
    # Every hop through irimi is stored in the escape's run, the process run holds none.
    [run] = shadow.sdk_runs()
    stored = [
        (e.request.host, e.request.path, e.response.status if e.response else None)
        for e in shadow.stored().load_run(run.run_id).events
        if isinstance(e, Exchange)
    ]
    hops = [("files.internal", "/export", 302), ("files.internal", "/export/v2", 302)]
    assert stored == hops * 2 + [("127.0.0.1", "/queue/stats", 200)] + hops * 2
    assert shadow.stored().load_run(shadow.process_run_id()).events == []
    # Bare, the same twelve requests: each client's three hops.
    assert len(bare.internet.requests()) == len(shadow.internet.requests()) == 12
