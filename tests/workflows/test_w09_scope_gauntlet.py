"""W9 `scope_gauntlet`: what irimi answers for each classification edge, pinned per call.

The labels are the calls in `examples/workflows/w09_scope_gauntlet/agent.py`.
"""

W = "w09_scope_gauntlet"


def answered(result):
    return {c["label"]: (c["status"], c["answered_by"]) for c in result.calls() if c.get("label")}


def test_the_scope_rule_fakes_every_method_no_route_names(run_workflow):
    shadow = run_workflow(W, "verbs", "shadow")
    assert answered(shadow) == {
        "get_charge": (200, None),
        "head_charges": (200, None),
        "options_charges": (404, None),
        "delete_customer": (200, "fake-L0"),
        "patch_customer": (200, "fake-L0"),
        "put_charge": (200, "fake-L0"),
        "unrouted_post": (200, "fake-L0"),
        "cancel_empty_body": (200, "fake-L1"),
    }
    lines = shadow.exchange_lines()
    for verb, path in [
        ("DELETE", "/v1/customers/cus_GAUNTLET"),
        ("PATCH", "/v1/customers/cus_GAUNTLET"),
        ("PUT", "/v1/charges/ch_GAUNTLET"),
        ("POST", "/v1/subscriptions"),
    ]:
        assert (
            f"fake-L0   unknown   {verb} api.stripe.com{path} -> 200  [unclassified, fidelity:L0]"
            in lines
        )
    # Bare, the same calls really reach Stripe; the fake refuses the ones it does not know.
    bare = run_workflow(W, "verbs", "bare")
    assert [(r.method, r.path) for r in bare.internet.writes()] == [
        ("DELETE", "/v1/customers/cus_GAUNTLET"),
        ("PATCH", "/v1/customers/cus_GAUNTLET"),
        ("PUT", "/v1/charges/ch_GAUNTLET"),
        ("POST", "/v1/subscriptions"),
        ("POST", "/v1/payment_intents/pi_GAUNTLET/cancel"),
    ]


def test_one_idempotency_key_is_one_write_and_a_reused_key_is_stripes_own_error(run_workflow):
    shadow = run_workflow(W, "idempotency", "shadow")
    bare = run_workflow(W, "idempotency", "bare")
    # The agent sees the same statuses either way: irimi's idempotency is Stripe's.
    assert [s for s, _ in answered(shadow).values()] == [s for s, _ in answered(bare).values()]
    assert answered(shadow)["refund_key_reused"] == (400, "fake-L1")
    refund = "fake-L1   write     POST api.stripe.com/v1/refunds"
    lines = shadow.exchange_lines()
    assert f"{refund} -> 200  [fidelity:L1, idempotent-replay]" in lines
    assert f"{refund} -> 400  [fidelity:L1, idempotency-conflict]" in lines
    # A replayed key issues no second L3 read; each keyless refund issues its own.
    engine_reads = [r for r in shadow.internet.requests() if r.path == "/v1/charges/ch_GAUNTLET"]
    assert len(engine_reads) == 3
    # The summary counts the two keyless refunds as two writes: the duplicate Phase 4 must flag.
    summary = "\n".join(shadow.summary())
    assert summary.count("refund $0.50 on ch_GAUNTLET") == 2
    assert "✗ refund 200 on ch_GAUNTLET  would fail: idempotency_error" in summary


def test_gzip_chunked_and_oversized_bodies_are_still_faked(run_workflow):
    shadow = run_workflow(W, "bodies", "shadow")
    assert answered(shadow) == {
        "refund_gzip": (200, "fake-L1"),
        "refund_chunked": (200, "fake-L1"),
        "customer_3mb": (200, "fake-L1"),
    }
    summary = "\n".join(shadow.summary())
    # irimi read the refund amount out of the gzip body and out of the chunked one.
    assert "refund $3.00 on ch_GAUNTLET" in summary
    assert "refund $4.00 on ch_GAUNTLET" in summary


def test_irimis_own_headers_sent_by_the_agent_never_leave(run_workflow):
    shadow = run_workflow(W, "headers", "shadow")
    assert answered(shadow) == {"forged_rewrote": (200, None), "bad_run_id": (200, None)}
    for req in shadow.internet.requests():
        assert "irimi-rewrote" not in req.headers
        assert "irimi-run" not in req.headers


def test_an_unmapped_hosts_posts_are_faked_even_when_they_are_reads(run_workflow):
    shadow = run_workflow(W, "unmapped_hosts", "shadow")
    assert answered(shadow) == {
        # A GraphQL query is a read, but irimi cannot tell it from a mutation: both are faked.
        "graphql_query": (200, "fake-L0"),
        "graphql_mutation": (200, "fake-L0"),
        "unmapped_get": (200, None),
    }
    assert (
        "fake-L0   unknown   POST slack.com/api/chat.delete -> 200  [unclassified, fidelity:L0]"
        in shadow.exchange_lines()
    )
    assert [r.path for r in shadow.internet.requests()] == ["/health"]


def test_a_get_that_writes_escapes_and_the_scenario_says_so(run_workflow):
    shadow = run_workflow(W, "get_that_writes", "shadow")
    assert [(r.method, r.host, r.path) for r in shadow.internet.writes()] == [
        ("GET", "legacy.internal", "/api/delete_user")
    ]
    assert (
        "live      read      GET legacy.internal/api/delete_user -> 200" in shadow.exchange_lines()
    )
