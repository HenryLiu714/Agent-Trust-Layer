"""W9 `scope_gauntlet`: one call per classification edge, from a plain script with no SDK.

This is not an agent that does a job. It is the fastest check that irimi's safety rules hold on
the wire: THE SCOPE RULE, the L0 floor, idempotency, bodies irimi cannot read whole, irimi's own
headers arriving from the agent, and requests addressed to the proxy itself. Under `irimi shadow`
the run is attributed `process`, but for the runs the control groups start over the control
endpoint (#73), doing by hand what the SDK (#74) does for the other workflows.

    python -m examples.workflows.launch \\
        examples.workflows.w09_scope_gauntlet.agent <group>

Each group is one scenario in `scenarios.py`. Every call is logged with a label, which is what the
tests key their expectations on.
"""

from __future__ import annotations

import gzip
import json
import math
import os
import sys
import threading
import time
import urllib.request
from collections.abc import Callable
from http.client import HTTPConnection
from typing import Any
from urllib.parse import urlencode, urlsplit

from examples.workflows import agentkit
from examples.workflows.agentkit import Response, base, http, slack, stripe

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
    irimi, and never forwarded to itself or anywhere else.

    At each spelling it also calls the control endpoint (#73): its health check, and a path under
    `/_irimi/` that names no route. Both are answered by irimi and are never an exchange."""
    proxy = urlsplit(agentkit.proxy() or "")
    auth = {"Authorization": f"Bearer {agentkit.key('STRIPE_API_KEY')}"}
    stripe_authority = urlsplit(base("stripe")).netloc
    for spelling in ("127.0.0.1", "127.1", "0.0.0.0"):
        listener = f"http://{spelling}:{proxy.port}"
        http(
            "POST",
            f"{listener}/{stripe_authority}/v1/refunds",
            form={"charge": CHARGE, "amount": "600"},
            headers=auth,
            label=f"door_{spelling}",
        )
        http("GET", f"{listener}/_irimi/health", label=f"health_{spelling}")
        http("GET", f"{listener}/_irimi/nope", label=f"nope_{spelling}")


# -- the control endpoint, called as the SDK will call it (#73) ------------------------------------
#
# These groups do the SDK's job by hand (#74 posts a run's start and end; #76 its tool calls), so
# `irimi shadow` itself is shown storing an `sdk` run from posts the test controls exactly: its
# trigger, its tool calls in order with the exchange labelled `Irimi-Run: <id>` between them, and
# its outcome, and every refusal the SDK itself never provokes.

# The trigger's entrypoint each run posts: this module, by the name `launch` keeps for it.
ENTRYPOINT = "examples.workflows.w09_scope_gauntlet.agent"
AGENT_VERSION = "w09-by-hand"
SDK_VERSION = "by-hand"
# Past `irimi.trace.MAX_ERROR_MESSAGE` (1000), the most of an error message a trace keeps.
LONG_MESSAGE_CHARS = 1500
# Past `irimi.control.MAX_CONTROL_BODY` (2 MiB), the most a control request may post.
OVERSIZED_BODY_BYTES = 2 * 1024 * 1024 + 1
# How long `_settle` waits for irimi's store to write what is queued.
SETTLE_S = 5.0


def control_runs() -> None:
    """The SDK's job, by hand: runs started, given tool calls and ended over the control endpoint,
    each with a Stripe read labelled with its run between two tool calls.

    One run posts direct to `IRIMI_CONTROL` (`127.0.0.1`, which NO_PROXY sends straight to the
    listener) and one through the proxy to itself (`127.1`). Two more run at once, on two threads
    kept in lockstep, so their posts and reads interleave on the wire and must not mix. One ends
    in error, its message past what a trace keeps. Then every refusal the endpoint documents, and
    the health check before and after."""
    control = _control_base()
    _health(control, "health_before")
    _run_by_hand(control, "ctl-direct", f"/v1/charges/{CHARGE}", "direct")
    _run_by_hand(_through_proxy(control), "ctl-proxied", f"/v1/charges/{CHARGE}", "proxied")
    lockstep = threading.Barrier(2, timeout=agentkit.timeout() * 3)
    threads = [
        threading.Thread(
            target=_run_by_hand,
            args=(control, run_id, path, tag),
            kwargs={"step": lockstep.wait},
        )
        for run_id, path, tag in (
            ("ctl-thread-1", f"/v1/charges/{CHARGE}", "thread_1"),
            ("ctl-thread-2", f"/v1/customers/{CUSTOMER}", "thread_2"),
        )
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    _start(control, "ctl-error", "error_start")
    _tool_call(
        control,
        "ctl-error",
        "charge_card",
        "error_tool",
        result=None,
        error={"type": "builtins.ValueError", "message": "card " + "x" * LONG_MESSAGE_CHARS},
    )
    _end(
        control,
        "ctl-error",
        "error_end",
        outcome="error",
        error={"type": "builtins.RuntimeError", "message": "run " + "y" * LONG_MESSAGE_CHARS},
    )
    _refusals(control)
    _health(control, "health_after")


def control_hazards() -> None:
    """Posts the SDK must never make, made, so what irimi does with each is pinned: a start
    re-posted for a run that has ended, and a start and an end for the process run's own id,
    `IRIMI_RUN`, which irimi starts and ends itself. Between those two, a tool call posted to the
    process run, which may hold one."""
    control = _control_base()
    _start(control, "ctl-restarted", "restarted_start")
    stripe(
        "GET",
        f"/v1/charges/{CHARGE}",
        headers={"Irimi-Run": "ctl-restarted"},
        label="restarted_read",
    )
    _end(control, "ctl-restarted", "restarted_end")
    _start(control, "ctl-restarted", "restarted_again", name="ctl-restarted again")
    # Bare, there is no process run; the calls are made to the same URL shape all the same.
    process_run = os.environ.get(agentkit.RUN_ENV) or "no-process-run"
    agentkit.obs("process_run", id=process_run)
    for label, resp in [
        ("process_start", _start(control, process_run, "process_start", name="takeover")),
        (
            "process_tool",
            _tool_call(control, process_run, "look_up", "process_tool", result={"run": "process"}),
        ),
        ("process_end", _end(control, process_run, "process_end")),
    ]:
        agentkit.obs("answer", label=label, body=resp.json())


def _control_base() -> str:
    """`IRIMI_CONTROL`, which `irimi shadow` gives its child. A bare run has none, so it posts to
    the same URL shape at its proxy's address, which is the fake internet: every call is made in
    both modes and the agent exits the same way."""
    given = os.environ.get(agentkit.CONTROL_ENV)
    agentkit.obs("control_base", given=given is not None)
    return given or (agentkit.proxy() or "").rstrip("/") + "/_irimi"


def _through_proxy(control: str) -> str:
    """The same endpoint spelled `127.1`, which is not in NO_PROXY: through the proxy to itself."""
    parts = urlsplit(control)
    return parts._replace(netloc=f"127.1:{parts.port}").geturl()


def _run_by_hand(
    control: str, run_id: str, read_path: str, tag: str, step: Callable[[], object] = lambda: None
) -> None:
    """One run as the SDK reports it: start, a tool call, a read labelled with the run, a second
    tool call, end ok. `step` is called between each, to keep two threads in lockstep."""
    _start(control, run_id, f"{tag}_start")
    step()
    _tool_call(control, run_id, "look_up", f"{tag}_tool_1", result={"found": read_path})
    step()
    stripe("GET", read_path, headers={"Irimi-Run": run_id}, label=f"{tag}_read")
    step()
    _tool_call(control, run_id, "summarise", f"{tag}_tool_2", result={"summary": tag})
    step()
    _end(control, run_id, f"{tag}_end")


def _start(control: str, run_id: str, label: str, *, name: str | None = None) -> Response:
    doc = {
        "trigger": {
            "name": name or run_id,
            "entrypoint": f"{ENTRYPOINT}:{run_id}",
            "args": {"run": run_id},
            "replayable": False,
        },
        "agent_version": AGENT_VERSION,
        "sdk_version": SDK_VERSION,
        "started_at": time.time(),
    }
    return _post(control, run_id, "start", doc, label)


def _tool_call(
    control: str,
    run_id: str,
    name: str,
    label: str,
    *,
    result: Any,
    error: dict[str, str] | None = None,
) -> Response:
    started_at = time.time()
    doc = {
        "tool_call_id": f"{run_id}-{name}",
        "name": name,
        "kind": "read",
        "ran": "real",
        "args": {"run": run_id},
        "result": result,
        "error": error,
        "started_at": started_at,
        "ended_at": time.time(),
    }
    return _post(control, run_id, "tool-calls", doc, label)


def _end(
    control: str,
    run_id: str,
    label: str,
    *,
    outcome: str = "ok",
    error: dict[str, str] | None = None,
) -> Response:
    doc = {"ended_at": time.time(), "outcome": outcome, "error": error}
    return _post(control, run_id, "end", doc, label)


def _post(control: str, run_id: str, action: str, doc: dict[str, Any], label: str) -> Response:
    """One control post, and what it posted, logged, so a test can compare the stored record with
    the posted one field by field."""
    agentkit.obs("posted", label=label, run=run_id, action=action, body=doc)
    return http("POST", f"{control}/runs/{run_id}/{action}", json_body=doc, label=label)


def _refusals(control: str) -> None:
    """Each documented refusal, once. A refused post must make no run directory, so each names a
    run id of its own that nothing else uses."""
    ended = {"ended_at": time.time(), "outcome": "ok", "error": None}
    started = {"agent_version": None, "sdk_version": SDK_VERSION, "started_at": time.time()}
    tool = {
        "tool_call_id": "t",
        "name": "look_up",
        "kind": "read",
        "ran": "real",
        "args": {},
        "result": None,
        "error": None,
        "started_at": time.time(),
        "ended_at": time.time(),
    }
    big = b'{"pad": "' + b"x" * OVERSIZED_BODY_BYTES + b'"}'
    refusals: list[tuple[str, str, str, dict[str, Any] | None, bytes | None]] = [
        ("refuse_get", "GET", "/runs/ctl-refused-get/start", None, None),
        ("refuse_route", "POST", "/runs/ctl-refused-route/nope", ended, None),
        ("refuse_dotdot", "POST", "/runs/../x/start", started, None),
        ("refuse_unattributed", "POST", "/runs/unattributed/end", ended, None),
        ("refuse_nan", "POST", "/runs/ctl-refused-nan/end", {**ended, "ended_at": math.nan}, None),
        ("refuse_missing", "POST", "/runs/ctl-refused-missing/start", started, None),
        (
            "refuse_run_id",
            "POST",
            "/runs/ctl-refused-run-id/tool-calls",
            {**tool, "run_id": "x"},
            None,
        ),
        ("refuse_big", "POST", "/runs/ctl-refused-big/start", None, big),
    ]
    for label, method, route, doc, body in refusals:
        headers = {"Content-Type": "application/json"} if body is not None else None
        resp = http(method, control + route, json_body=doc, body=body, headers=headers, label=label)
        agentkit.obs("refusal", label=label, body=resp.json())


def _health(control: str, label: str) -> None:
    """`GET /_irimi/health`, once irimi's store has written what it had queued, so its counters
    are exact."""
    _settle(control)
    resp = http("GET", f"{control}/health", label=label)
    agentkit.obs("health", label=label, body=resp.json())


def _settle(control: str) -> None:
    """Wait, unlogged, until the health check says the store has nothing queued. Every run here
    ends with a post that queues a run.json write, not an event line, so once the queue is empty
    every event line before it has been written. Bare, the fake internet answers, and there is
    nothing to wait for."""
    deadline = time.monotonic() + SETTLE_S
    request = urllib.request.Request(f"{control}/health")
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(request, timeout=agentkit.timeout()) as raw:
                doc = json.loads(raw.read())
        except (OSError, ValueError):  # a bare run's 502 is an HTTPError, an OSError
            return
        if doc["store"]["queued"] == 0:
            return
        time.sleep(0.01)
    # Logged, so a slow writer reads as itself rather than as a health pin's wrong count (#73).
    agentkit.obs("settle_timeout")


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
    proxy = agentkit.proxy()
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
    "control_runs": control_runs,
    "control_hazards": control_hazards,
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
