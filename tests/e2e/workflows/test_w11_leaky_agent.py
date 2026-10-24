"""W11 `leaky_agent`: each escape's write really lands, and irimi never sees it.

KNOWN BUG: irimi cannot see these escapes; `labelled_sidecar`'s write tool closes one of them."""

import pytest

from irimi.core.exchange import Exchange
from tests.e2e.workflows.agents.w11_leaky_agent.agent import CHANNEL, CHARGE
from tests.e2e.workflows.harness.compare import compare, rows_not_shown
from tests.e2e.workflows.harness.invariants import check_invariants

W = "w11_leaky_agent"
# Each test runs under `irimi shadow -- <cmd>` and under serve mode, its agent beside irimi.
MODES = ("shadow", "serve")
NOTHING_SEEN = "0 exchanges · 0 real · 0 answered by irimi (0 faked writes) · 0 sent to targets"

ESCAPES = {
    "proxyless_client": ("POST", "api.stripe.com", "/v1/refunds"),
    "no_proxy_star": ("POST", "api.stripe.com", "/v1/refunds"),
    "raw_socket": ("POST", "slack.com", "/api/chat.postMessage"),
    "loopback_service": ("POST", "127.0.0.1", "/queue/jobs"),
}


@pytest.mark.parametrize("escape", sorted(ESCAPES))
@pytest.mark.parametrize("mode", MODES)
def test_an_escape_writes_for_real_and_irimi_never_sees_it(run_workflow, mode, escape):
    shadow = run_workflow(W, escape, mode)
    assert shadow.exit_code == 0
    assert [(r.method, r.host, r.path) for r in shadow.internet.writes()] == [ESCAPES[escape]]
    # No exchange line, and a summary that counts nothing and claims no write did not happen.
    assert shadow.exchange_lines() == []
    summary = "\n".join(shadow.summaries())
    assert NOTHING_SEEN in summary
    assert "did not happen" not in summary
    # The agent got the real service's answer, unstamped: it cannot tell it escaped.
    assert [c["answered_by"] for c in shadow.calls()] == [None]
    # Nor can a stored run: every run is `ok` with no event. The escaped request carried no
    # `Irimi-Run` either, since the SDK labels only what goes through irimi.
    reader = shadow.stored()
    runs = {r.attribution: r for r in reader.list_runs()}
    assert sorted(runs) == (["process", "sdk"] if mode == "shadow" else ["sdk"])
    assert {r.end.outcome for r in runs.values()} == {"ok"}
    trigger = runs["sdk"].trigger
    assert trigger is not None and (trigger.name, trigger.args) == ("escape", {"escape": escape})
    assert shadow.stored_events() == []
    assert not [r for r in shadow.internet.requests() if "irimi-run" in r.headers]
    # Shadow changes nothing about the escape.
    bare = run_workflow(W, escape, "bare")
    assert [(r.method, r.host, r.path) for r in bare.internet.writes()] == [ESCAPES[escape]]


@pytest.mark.parametrize("mode", MODES)
def test_the_escaped_refund_really_changed_the_ledger(run_workflow, mode):
    for escape in ("proxyless_client", "no_proxy_star"):
        stripe = run_workflow(W, escape, mode).world.stripe
        assert stripe.charges[CHARGE]["amount_refunded"] == 500
        assert [r["charge"] for r in stripe.refunds] == [CHARGE]


@pytest.mark.parametrize("mode", MODES)
def test_the_raw_socket_post_is_in_the_channel(run_workflow, mode):
    slack = run_workflow(W, "raw_socket", mode).world.slack
    assert [m["text"] for m in slack.messages[CHANNEL]] == ["posted around irimi"]


@pytest.mark.parametrize("mode", MODES)
def test_irimis_own_no_proxy_default_exempts_a_loopback_sidecar(run_workflow, mode):
    shadow = run_workflow(W, "loopback_service", mode)
    [sidecar] = [s for s in shadow.world.extra if s.hosts == ("127.0.0.1",)]
    assert sidecar.state["jobs"] == [{"job": "refund", "charge": CHARGE}]
    # It went straight there: not through any proxy, irimi's or the harness's.
    assert [r.via_proxy for r in shadow.internet.requests()] == [False]


@pytest.mark.parametrize("mode", MODES)
def test_the_control_through_the_proxy_is_faked_and_the_ledger_is_untouched(run_workflow, mode):
    shadow = run_workflow(W, "proxied", mode)
    # Through irimi, the refund and irimi's check read are stored in the run; the process run
    # holds nothing.
    [run] = shadow.sdk_runs()
    events = shadow.stored().load_run(run.run_id).events
    assert sorted(
        (e.request.method, e.request.path) for e in events if isinstance(e, Exchange)
    ) == [
        ("GET", f"/v1/charges/{CHARGE}"),
        ("POST", "/v1/refunds"),
    ]
    assert shadow.stored().load_run(shadow.engine_run_id()).events == []
    assert shadow.internet.writes() == []
    assert shadow.answered() == [("refund", 200, "fake-L1")]
    assert shadow.world.stripe.charges[CHARGE]["amount_refunded"] == 0
    assert "faked         write     POST api.stripe.com/v1/refunds -> 200" in (
        shadow.exchange_lines()
    )
    assert (
        "○ stripe  refund $5.00 on ch_LEAKY  faked; checked against live state: would succeed"
        in "\n".join(shadow.summaries())
    )


@pytest.mark.parametrize("mode", MODES)
def test_a_redirect_off_irimis_route_is_labelled_on_each_hop_through_irimi_and_on_no_other(
    run_workflow,
    mode,
):
    """The SDK labels each redirect hop through irimi with the run, and no hop that goes around it.

    Four clients follow an export's redirect within the host, then to the loopback sidecar that
    `NO_PROXY` sends direct. httpx reuses the sent request, so the SDK unlabels it once sent.
    `requests` keeps its proxy across redirects, so its sidecar hop goes through irimi, stripped."""
    shadow = run_workflow(W, "redirected", mode)
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
    # Nothing that reached a service carried the run.
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
    assert shadow.stored().load_run(shadow.engine_run_id()).events == []
    # Bare, the same twelve requests: each client's three hops.
    assert len(bare.internet.requests()) == len(shadow.internet.requests()) == 12


@pytest.mark.parametrize("mode", MODES)
def test_a_sidecar_write_made_by_a_write_tool_is_stood_in_and_the_escape_closed(run_workflow, mode):
    """A sidecar write made by a write tool runs its fake instead, closing the escape.

    The sidecar receives nothing under shadow and the job bare. irimi stores the call in the run,
    and the summary says the write did not happen."""
    shadow = run_workflow(W, "labelled_sidecar", mode)
    bare = run_workflow(W, "labelled_sidecar", "bare")
    tool = "tests.e2e.workflows.agents.w11_leaky_agent.agent:enqueue_job"
    assert shadow.internet.requests() == []
    assert [(r.method, r.host, r.path) for r in bare.internet.writes()] == [
        ("POST", "127.0.0.1", "/queue/jobs")
    ]
    assert (shadow.tools(), bare.tools()) == ([(tool, "shadow")], [(tool, "real")])
    assert shadow.calls() == []
    assert [e["doc"] for e in shadow.events("queued")] == [{"queued": None, "stood_in": True}]
    assert [e["doc"] for e in bare.events("queued")] == [{"queued": 1}]
    [run] = shadow.sdk_runs()
    [call] = shadow.stored().load_run(run.run_id).events
    assert (call.name, call.ran, call.args) == (
        tool,
        "shadow",
        {"job": {"job": "refund", "charge": CHARGE}},
    )
    assert shadow.tool_call_lines() == [f"tool-fake     write     {tool} -> ok"]
    summary = shadow.summaries()
    start = summary.index("  None of these writes reached the real service:")
    assert summary[start + 1 : start + 3] == [f"  ○ tool  {tool}  fake ran instead", ""]
    assert f"  {NOTHING_SEEN}" in summary


# ---------------------------------------------------------------------- irimi compare

# Each escape is an `sdk.run` named `escape`, with no function to name its entrypoint.
ESCAPE = "escape"
FAKED_REFUND = (
    f"○ stripe  refund $5.00 on {CHARGE}  faked; checked against live state: would succeed"
)


def compared(shared_store_run, a: str, b: str, mode: str) -> tuple[str, str, list[str]]:
    """Return the run ids of `a` and `b` in one store and what `irimi compare` printed of them.

    It also checks that compare exited 1, wrote nothing to stderr, and showed each run's lines."""
    one = shared_store_run(W, a)
    other = shared_store_run(W, b, mode)
    assert check_invariants(other) == []
    code, out, err = compare(one, other)
    ids = [result.one("run.start")["run"] for result in (one, other)]
    assert (code, err) == (1, [])
    assert rows_not_shown(other, ids[0], ids[1], out) == []
    return ids[0], ids[1], out


@pytest.mark.parametrize("escape", sorted(ESCAPES))
@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_cannot_show_a_write_that_escaped(shared_store_run, escape, mode):
    """`irimi compare` shows an escape as a run that wrote nothing, against `proxied`'s fake.

    KNOWN BUG: a run that refunded for real compares as one that did not refund, since compare
    can only show what irimi stored, and irimi cannot see the escape."""
    a, b, out = compared(shared_store_run, "proxied", escape, mode)
    assert out == [
        f"irimi compare · {a} vs {b}",
        f"  A  {ESCAPE}",
        f"  B  {ESCAPE}",
        "  (the triggers' args differ)",
        "",
        f"  - {FAKED_REFUND}",
        "",
        "  0 same · 0 changed · 1 only in A · 0 only in B",
    ]


@pytest.mark.parametrize("mode", MODES)
def test_irimi_compare_shows_the_sidecar_write_a_write_tool_closed(shared_store_run, mode):
    """`irimi compare` shows the write tool's fake as B's one write, and the escape as nothing."""
    a, b, out = compared(shared_store_run, "loopback_service", "labelled_sidecar", mode)
    assert out == [
        f"irimi compare · {a} vs {b}",
        f"  A  {ESCAPE}",
        f"  B  {ESCAPE}",
        "  (the triggers' args differ)",
        "",
        "  + ○ tool  tests.e2e.workflows.agents.w11_leaky_agent.agent:enqueue_job  "
        "fake ran instead",
        "",
        "  0 same · 0 changed · 0 only in A · 1 only in B",
    ]


# ------------------------------------------------------------------------------- replay


@pytest.mark.parametrize(
    "scenario", ["proxyless_client", "no_proxy_star", "raw_socket", "loopback_service"]
)
def test_an_escape_escapes_its_replay_too(replayed, scenario):
    """An escaped write lands again on replay: its recording holds nothing to answer it from."""
    recorded, replay = replayed(W, scenario)
    escaped = [(r.method, r.host, r.path) for r in recorded.internet.writes()]
    assert escaped
    assert [(r.method, r.host, r.path) for r in replay.internet.writes()] == escaped
    seen = {(ex.request.method, ex.request.host, ex.request.path) for ex in replay.reported}
    assert not seen & set(escaped)
    assert not {
        (ex.request.method, ex.request.host, ex.request.path)
        for ex in recorded.stored_events()
        if isinstance(ex, Exchange)
    } & set(escaped)
