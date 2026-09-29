"""W9 `scope_gauntlet`: one call per classification edge, from a plain script with no SDK.

This is not an agent that does a job. It is the fastest check that irimi's safety rules hold on
the wire: THE SCOPE RULE, the L0 floor, idempotency, bodies irimi cannot read whole, irimi's own
headers arriving from the agent, and requests addressed to the proxy itself. Under `irimi shadow`
the run is attributed `process`.

    python -m examples.workflows.launch \\
        examples.workflows.w09_scope_gauntlet.agent <group>

Each group is one scenario in `scenarios.py`. Every call is logged with a label, which is what the
tests key their expectations on.
"""

from __future__ import annotations

import gzip
import os
import sys
from http.client import HTTPConnection
from urllib.parse import urlencode, urlsplit

from examples.workflows import agentkit
from examples.workflows.agentkit import base, http, slack, stripe

CHARGE = "ch_GAUNTLET"
CUSTOMER = "cus_GAUNTLET"
INTENT = "pi_GAUNTLET"
BIG_CHARGE = "ch_GAUNTLETBIG"


def verbs() -> None:
    """Every method on a mapped host, named by a route or not."""
    stripe("GET", f"/v1/charges/{CHARGE}", label="get_charge")
    stripe("HEAD", "/v1/charges", label="head_charges")
    stripe("OPTIONS", "/v1/charges", label="options_charges")
    # The Stripe map names GET and POST on /v1/customers/{customer}, never DELETE or PATCH, and
    # names no PUT anywhere. THE SCOPE RULE: none of these may be forwarded.
    stripe("DELETE", f"/v1/customers/{CUSTOMER}", label="delete_customer")
    stripe(
        "PATCH", f"/v1/customers/{CUSTOMER}", {"email": "x@example.test"}, label="patch_customer"
    )
    stripe("PUT", f"/v1/charges/{CHARGE}", {"amount": "1"}, label="put_charge")
    # A POST on a mapped host that no route names.
    stripe("POST", "/v1/subscriptions", {"customer": CUSTOMER}, label="unrouted_post")
    # A mapped write with an empty body: `payment_intents.cancel` posts nothing at all (#46).
    stripe("POST", f"/v1/payment_intents/{INTENT}/cancel", label="cancel_empty_body")
    # The L0 floor: a route that names a write but ships no `fixture:` is still faked, at L0, and
    # never forwarded. Slack's `reactions.add` is one.
    slack(
        "reactions.add",
        label="reaction_no_fixture",
        channel="C0GAUNT",
        timestamp="1790000000.000100",
        name="eyes",
    )


def idempotency() -> None:
    """One key, three uses: the first write, the same write again, and different parameters."""
    key = {"Idempotency-Key": "gauntlet-key-1"}
    refund = {"charge": CHARGE, "amount": "100"}
    _refund(refund, key, "refund_first")
    _refund(refund, key, "refund_same_key")
    _refund({**refund, "amount": "200"}, key, "refund_key_reused")
    # And with no key: the same write twice is two writes.
    _refund({"charge": CHARGE, "amount": "50"}, {}, "refund_nokey_1")
    _refund({"charge": CHARGE, "amount": "50"}, {}, "refund_nokey_2")


def _refund(form: dict[str, str], headers: dict[str, str], label: str) -> None:
    """A refund, logging the id it was answered with and whether Stripe called it a replay: a
    replayed key must hand back the first answer's id, not mint a second (#46)."""
    resp = stripe("POST", "/v1/refunds", form, headers=headers, label=label)
    agentkit.obs(
        "refund",
        label=label,
        id=(resp.json() or {}).get("id"),
        replayed=resp.headers.get("idempotent-replayed"),
    )


def bodies() -> None:
    """Bodies irimi cannot read as plain fields."""
    auth = {"Authorization": f"Bearer {agentkit.key('STRIPE_API_KEY')}"}
    form = urlencode({"charge": CHARGE, "amount": "300"}).encode()
    http(
        "POST",
        base("stripe") + "/v1/refunds",
        body=gzip.compress(form),
        headers={
            **auth,
            "Content-Type": "application/x-www-form-urlencoded",
            "Content-Encoding": "gzip",
        },
        label="refund_gzip",
    )
    _chunked_post(
        base("stripe") + "/v1/refunds", [b"charge=", CHARGE.encode(), b"&amount=400"], auth
    )
    big = urlencode({"email": "big@example.test", "description": "x" * 3_000_000}).encode()
    http(
        "POST",
        base("stripe") + f"/v1/customers/{CUSTOMER}",
        body=big,
        headers={**auth, "Content-Type": "application/x-www-form-urlencoded"},
        label="customer_3mb",
    )


def big_reads() -> None:
    """Reads whose answer is past `irimi.bodies.MAX_BODY_BYTES`: the charge a refund's L3 check
    reads, and the same charge read back after the refund, which the overlay would edit."""
    stripe("POST", "/v1/refunds", {"charge": BIG_CHARGE, "amount": "700"}, label="refund_big")
    doc = stripe("GET", f"/v1/charges/{BIG_CHARGE}", label="get_big").json() or {}
    agentkit.obs("big_charge", amount_refunded=doc.get("amount_refunded"))


def headers() -> None:
    """irimi's own vocabulary, sent by the agent: both must be stripped before anything leaves."""
    stripe(
        "GET",
        f"/v1/refunds?charge={CHARGE}",
        headers={"Irimi-Rewrote": "starting_after=re_forged"},
        label="forged_rewrote",
    )
    stripe("GET", f"/v1/charges/{CHARGE}", headers={"Irimi-Run": "../../etc"}, label="bad_run_id")


def self_addressed() -> None:
    """A refund sent to the proxy's own address, under three spellings of this machine: the
    reverse door (`/<host>/<path>`). Whatever the spelling, it must be recognised as addressed to
    irimi, and never forwarded to itself or anywhere else."""
    proxy = urlsplit(os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy") or "")
    auth = {"Authorization": f"Bearer {agentkit.key('STRIPE_API_KEY')}"}
    stripe_authority = urlsplit(base("stripe")).netloc
    for spelling in ("127.0.0.1", "127.1", "0.0.0.0"):
        http(
            "POST",
            f"http://{spelling}:{proxy.port}/{stripe_authority}/v1/refunds",
            form={"charge": CHARGE, "amount": "600"},
            headers=auth,
            label=f"door_{spelling}",
        )


def unmapped_hosts() -> None:
    """Hosts no map claims, and a mapped POST-only host's unrouted method."""
    gql = agentkit.internal("graphql.internal")
    http("POST", gql + "/graphql", json_body={"query": "{ orders { id } }"}, label="graphql_query")
    http(
        "POST",
        gql + "/graphql",
        json_body={"query": "mutation { cancelOrder(id: 7) { id } }"},
        label="graphql_mutation",
    )
    http("GET", gql + "/health", label="unmapped_get")
    slack("chat.delete", label="slack_unrouted", channel="C0GAUNT", ts="1790000000.000100")


def get_that_writes() -> None:
    """A write spelled as a GET. irimi forwards reads, so this one escapes, and the scenario says
    so: it is the case for a map entry, not a bug irimi can see."""
    legacy = agentkit.internal("legacy.internal")
    http("GET", legacy + "/api/delete_user?id=7", label="get_delete_user")


def _chunked_post(url: str, pieces: list[bytes], headers: dict[str, str]) -> None:
    """A chunked request body, which urllib cannot send: http.client, through the proxy by hand."""
    proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    assert proxy, "the gauntlet always runs behind a proxy"
    p = urlsplit(proxy)
    conn = HTTPConnection(p.hostname or "127.0.0.1", p.port, timeout=agentkit.timeout())
    sent = {**headers, "Content-Type": "application/x-www-form-urlencoded"}
    conn.request("POST", url, body=iter(pieces), headers=sent, encode_chunked=True)
    resp = conn.getresponse()
    resp.read()
    conn.close()
    answered_by = resp.getheader("Irimi-Answered-By")
    agentkit.obs_http("POST", url, "refund_chunked", status=resp.status, answered_by=answered_by)


GROUPS = {
    "verbs": verbs,
    "idempotency": idempotency,
    "bodies": bodies,
    "big_reads": big_reads,
    "headers": headers,
    "self_addressed": self_addressed,
    "unmapped_hosts": unmapped_hosts,
    "get_that_writes": get_that_writes,
}


def main(argv: list[str]) -> int:
    agentkit.start()
    if len(argv) != 1 or argv[0] not in GROUPS:
        print(f"usage: agent.py {{{','.join(GROUPS)}}}", file=sys.stderr)
        return 2
    GROUPS[argv[0]]()
    agentkit.obs("result", group=argv[0])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
