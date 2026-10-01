"""W9 `scope_gauntlet`: what irimi answers for each classification edge, pinned per call.

The labels are the calls in `examples/workflows/w09_scope_gauntlet/agent.py`.
"""

import pytest

from examples.workflows.w09_scope_gauntlet.agent import BIG_CHARGE
from examples.workflows.w09_scope_gauntlet.scenarios import BIG_DESCRIPTION_BYTES, WORKFLOW
from irimi.bodies import MAX_BODY_BYTES
from irimi.exchange import BODY_TRUNCATED_FLAG, Exchange
from irimi.store import MAX_STORED_BODY

W = "w09_scope_gauntlet"


def test_the_scope_rule_fakes_every_method_no_route_names(run_workflow):
    shadow = run_workflow(W, "verbs", "shadow")
    assert shadow.by_label() == {
        "get_charge": (200, None),
        "head_charges": (200, None),
        "options_charges": (404, None),
        "delete_customer": (200, "fake-L0"),
        "patch_customer": (200, "fake-L0"),
        "put_charge": (200, "fake-L0"),
        "unrouted_post": (200, "fake-L0"),
        "cancel_empty_body": (200, "fake-L1"),
        "reaction_no_fixture": (200, "fake-L0"),
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
    # The L0 floor: a mapped write whose route ships no `fixture:` is still a write, and faked.
    assert "fake-L0   write     POST slack.com/api/reactions.add -> 200  [fidelity:L0]" in lines
    assert "  ○ add :eyes: to a message in C0GAUNT  unvalidated (L0)" in shadow.summary()
    # Bare, the same calls really reach Stripe; the fake refuses the ones it does not know.
    bare = run_workflow(W, "verbs", "bare")
    assert [(r.method, r.path) for r in bare.internet.writes()] == [
        ("DELETE", "/v1/customers/cus_GAUNTLET"),
        ("PATCH", "/v1/customers/cus_GAUNTLET"),
        ("PUT", "/v1/charges/ch_GAUNTLET"),
        ("POST", "/v1/subscriptions"),
        ("POST", "/v1/payment_intents/pi_GAUNTLET/cancel"),
        ("POST", "/api/reactions.add"),
    ]


def test_one_idempotency_key_is_one_write_and_a_reused_key_is_stripes_own_error(run_workflow):
    shadow = run_workflow(W, "idempotency", "shadow")
    bare = run_workflow(W, "idempotency", "bare")
    # The agent sees the same statuses either way: irimi's idempotency is Stripe's.
    assert [s for s, _ in shadow.by_label().values()] == [s for s, _ in bare.by_label().values()]
    assert shadow.by_label()["refund_key_reused"] == (400, "fake-L1")
    # A replayed key hands back the first answer, minted id and all, and says it is a replay
    # (#46); a keyless retry mints a new id.
    ids = {e["label"]: (e["id"], e["replayed"]) for e in shadow.events("refund")}
    first_id = ids["refund_first"][0]
    assert first_id and first_id.startswith("re_")
    assert ids["refund_first"] == (first_id, None)
    assert ids["refund_same_key"] == (first_id, "true")
    assert ids["refund_key_reused"] == (None, None)
    assert len({ids["refund_first"][0], ids["refund_nokey_1"][0], ids["refund_nokey_2"][0]}) == 3
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
    assert shadow.by_label() == {
        "refund_gzip": (200, "fake-L1"),
        "refund_chunked": (200, "fake-L1"),
        "customer_3mb": (200, "fake-L1"),
    }
    summary = "\n".join(shadow.summary())
    # irimi read the refund amount out of the gzip body and out of the chunked one.
    assert "refund $3.00 on ch_GAUNTLET" in summary
    assert "refund $4.00 on ch_GAUNTLET" in summary
    # The 3 MB update and irimi's 3 MB fake of the customer are under MAX_STORED_BODY, so both
    # are stored whole, as blobs, and not flagged cut (#70).
    [update] = [
        e
        for e in shadow.stored_events()
        if isinstance(e, Exchange) and e.request.path.startswith("/v1/customers/")
    ]
    assert update.response is not None
    assert 3_000_000 < len(update.request.body) < MAX_STORED_BODY
    assert 3_000_000 < len(update.response.body) < MAX_STORED_BODY
    assert BODY_TRUNCATED_FLAG not in update.flags


def test_a_read_past_the_body_limit_leaves_the_check_and_the_overlay_unable_to_say(run_workflow):
    """A charge too big to parse (`MAX_BODY_BYTES`): irimi cannot check the refund against it (L2)
    or show the refund in it when the agent reads it back, and the summary says both."""
    bare = run_workflow(W, "big_reads", "bare")
    [charge_read] = [r for r in bare.internet.requests() if r.method == "GET"]
    assert charge_read.path == f"/v1/charges/{BIG_CHARGE}"
    shadow = run_workflow(W, "big_reads", "shadow")
    assert shadow.by_label() == {"refund_big": (200, "fake-L1"), "get_big": (200, None)}
    # Bare, the read-back shows the refund; under shadow the overlay could not add it.
    assert [e["amount_refunded"] for e in bare.events("big_charge")] == [700]
    assert [e["amount_refunded"] for e in shadow.events("big_charge")] == [0]
    assert shadow.exchange_lines() == [
        # LOOKS WRONG: the L3 read was answered 200, but irimi refused the body for its size and
        # records the read with no response and no flag, the same line as a read that got nothing.
        f"live      read      GET api.stripe.com/v1/charges/{BIG_CHARGE} -> -",
        "fake-L1   write     POST api.stripe.com/v1/refunds -> 200  [fidelity:L1]",
        f"live      read      GET api.stripe.com/v1/charges/{BIG_CHARGE} -> 200",
    ]
    summary = shadow.summary()
    assert f"  ○ refund 700 on {BIG_CHARGE}  unvalidated (L2)" in summary
    assert f"    ↳ GET /v1/charges/{BIG_CHARGE} did not show it  live (partial)" in summary
    # LOOKS WRONG: stored the same way (#70) - an engine read with no response and no flag, so a
    # stored run cannot tell "too big" apart from "no answer" either.
    assert engine_reads(shadow) == [(None, ())]


def engine_reads(result) -> list[tuple[object, tuple[str, ...]]]:
    """`(response, flags)` of each engine-issued read the store holds."""
    return [
        (e.response, e.flags)
        for e in result.stored_events()
        if isinstance(e, Exchange) and e.issued_by == "engine"
    ]


def test_the_big_charge_is_past_irimis_body_limit():
    """The scenario above means something only while its charge is past the limit."""
    assert BIG_DESCRIPTION_BYTES > MAX_BODY_BYTES


@pytest.mark.parametrize("scenario", sorted(WORKFLOW.scenarios))
def test_no_decision_ever_fails(run_workflow, scenario):
    """The never-raise hook: a decision that raised would answer 502 `decision-failed`. No edge
    in the gauntlet may reach that path, and every agent call got an HTTP answer."""
    shadow = run_workflow(W, scenario, "shadow")
    assert shadow.exchange_lines()
    assert not [line for line in shadow.exchange_lines() if "decision-failed" in line]
    assert all(isinstance(c.get("status"), int) for c in shadow.calls())


def test_irimis_own_headers_sent_by_the_agent_never_leave(run_workflow):
    # Bare, the agent's forged headers reach the service: the agent really sends them.
    bare = run_workflow(W, "headers", "bare")
    assert [
        sorted(h for h in r.headers if h.startswith("irimi-")) for r in bare.internet.requests()
    ] == [
        ["irimi-rewrote"],
        ["irimi-run"],
    ]
    shadow = run_workflow(W, "headers", "shadow")
    assert shadow.by_label() == {"forged_rewrote": (200, None), "bad_run_id": (200, None)}
    assert [r.path for r in shadow.internet.requests()] == [
        "/v1/refunds",
        "/v1/charges/ch_GAUNTLET",
    ]
    for req in shadow.internet.requests():
        assert "irimi-rewrote" not in req.headers
        assert "irimi-run" not in req.headers


def test_every_spelling_of_the_proxys_own_address_is_the_reverse_door(run_workflow):
    """`127.0.0.1` (sent direct, as irimi's NO_PROXY says), `127.1` and `0.0.0.0` (sent through the
    proxy to itself) all name irimi's listener, so each refund is taken through the reverse door
    and faked. Missing a spelling would forward the request to the proxy itself, or past it."""
    shadow = run_workflow(W, "self_addressed", "shadow")
    assert {k: v for k, v in shadow.by_label().items() if k.startswith("door_")} == {
        "door_127.0.0.1": (200, "fake-L1"),
        "door_127.1": (200, "fake-L1"),
        "door_0.0.0.0": (200, "fake-L1"),
    }
    assert shadow.internet.requests() == []
    # The door relays over https, and the fake internet speaks only http, so irimi's L3 read of
    # the charge gets no answer here and each refund is faked at L2. The failed engine read prints
    # with no response and no flag, as the oversized one does in `big_reads`.
    read = "live      read      GET api.stripe.com/v1/charges/ch_GAUNTLET -> -"
    write = "fake-L1   write     POST api.stripe.com/v1/refunds -> 200  [fidelity:L1]"
    assert shadow.exchange_lines() == [read, write] * 3
    refunds = [line for line in shadow.summary() if "○" in line]
    assert refunds == ["  ○ refund 600 on ch_GAUNTLET  unvalidated (L2)"] * 3
    assert engine_reads(shadow) == [(None, ())] * 3  # stored as `big_reads` stores its one (#70)


def test_the_control_endpoint_answers_at_every_spelling_and_is_never_an_exchange(run_workflow):
    """The same three spellings reach irimi's control endpoint (#73), direct and through the proxy
    to itself: health answers 200 and a path no route names 404, both stamped `control`. Neither
    is forwarded, decided or recorded, so the exchange lines above are the refunds' alone."""
    shadow = run_workflow(W, "self_addressed", "shadow")
    spellings = ("127.0.0.1", "127.1", "0.0.0.0")
    assert {k: v for k, v in shadow.by_label().items() if not k.startswith("door_")} == {
        **{f"health_{s}": (200, "control") for s in spellings},
        **{f"nope_{s}": (404, "control") for s in spellings},
    }
    assert not [line for line in shadow.exchange_lines() if "/_irimi/" in line]
    assert shadow.internet.requests() == []
    # Bare, there is no irimi: the same calls reach the fake internet's port, which serves no such
    # host, and the agent carries on to the same exit code.
    bare = run_workflow(W, "self_addressed", "bare")
    assert {k: v for k, v in bare.by_label().items() if not k.startswith("door_")} == {
        **{f"health_{s}": (502, None) for s in spellings},
        **{f"nope_{s}": (502, None) for s in spellings},
    }
    assert bare.exit_code == shadow.exit_code == 0


def test_an_unmapped_hosts_posts_are_faked_even_when_they_are_reads(run_workflow):
    shadow = run_workflow(W, "unmapped_hosts", "shadow")
    assert shadow.by_label() == {
        # A GraphQL query is a read, but irimi cannot tell it from a mutation: both are faked.
        "graphql_query": (200, "fake-L0"),
        "graphql_mutation": (200, "fake-L0"),
        "unmapped_get": (200, None),
        "slack_unrouted": (200, "fake-L0"),
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
