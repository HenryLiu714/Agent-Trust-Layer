"""W9 `scope_gauntlet`: one labelled call per classification edge, from a plain script with no SDK.

Its control groups post runs to the control endpoint by hand, doing what the SDK does elsewhere."""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import sys
import threading
import time
import urllib.request
from collections.abc import Callable
from http.client import HTTPConnection, HTTPException
from typing import Any
from urllib.parse import urlencode, urlsplit

from irimi.protocol.env import CONTROL_ENV, RUN_ENV
from tests.e2e.workflows.agents import agentkit
from tests.e2e.workflows.agents.agentkit import Response, base, http, slack, stripe

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
    # no PUT anywhere, so by the scope rule none of these may be forwarded.
    stripe("DELETE", f"/v1/customers/{CUSTOMER}", label="delete_customer")
    stripe(
        "PATCH", f"/v1/customers/{CUSTOMER}", {"email": "x@example.test"}, label="patch_customer"
    )
    stripe("PUT", f"/v1/charges/{CHARGE}", {"amount": "1"}, label="put_charge")
    # A POST on a mapped host that no route names.
    stripe("POST", "/v1/subscriptions", {"customer": CUSTOMER}, label="unrouted_post")
    # A mapped write with an empty body: `payment_intents.cancel` posts nothing at all.
    stripe("POST", f"/v1/payment_intents/{INTENT}/cancel", label="cancel_empty_body")
    # A write route with no `fixture:` is still faked, with an echo, and never forwarded. Slack's
    # `reactions.add` is one.
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
    """Refund and log the answer's id and `Idempotent-Replayed`; a retry returns the first id."""
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
    """Read a charge past `irimi.core.bodies.MAX_BODY_BYTES`, by the check and after a refund."""
    stripe("POST", "/v1/refunds", {"charge": BIG_CHARGE, "amount": "700"}, label="refund_big")
    doc = stripe("GET", f"/v1/charges/{BIG_CHARGE}", label="get_big").json() or {}
    agentkit.obs("big_charge", amount_refunded=doc.get("amount_refunded"))


def headers() -> None:
    """Send irimi's own headers from the agent; both must be stripped before anything leaves."""
    stripe(
        "GET",
        f"/v1/refunds?charge={CHARGE}",
        headers={"Irimi-Rewrote": "starting_after=re_forged"},
        label="forged_rewrote",
    )
    stripe("GET", f"/v1/charges/{CHARGE}", headers={"Irimi-Run": "../../etc"}, label="bad_run_id")


def self_addressed() -> None:
    """Send a base-URL refund (`/<host>/<path>`) to the proxy under three spellings of this machine.

    Each must be answered by irimi, never forwarded. Each spelling also calls the control
    endpoint's health check and an unknown `/_irimi/` path, which are never exchanges."""
    proxy = urlsplit(agentkit.proxy() or "")
    auth = {"Authorization": f"Bearer {agentkit.key('STRIPE_API_KEY')}"}
    stripe_authority = urlsplit(base("stripe")).netloc
    for spelling in ("127.0.0.1", "127.1", "0.0.0.0"):  # addresses, not binds
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


# The host `base_url_hosts` names with `--base-url-host`, and one no map or flag names.
ALLOWED_BASE_URL_HOST = "billing.example.test"
REFUSED_BASE_URL_HOST = "ledger.example.test"


def base_url_hosts() -> None:
    """Base-URL requests irimi relays and those it refuses before deciding anything.

    A write to a host `--base-url-host` names is relayed and faked. Every other one is answered by
    irimi itself, 403 with its JSON error: a host no map or flag names, a path that names no host
    or an empty one, and a port that is no port. Each answer's body is logged, so a test reads
    what irimi said."""
    listener = f"http://127.0.0.1:{urlsplit(agentkit.proxy() or '').port}"
    calls = (
        ("POST", f"/{ALLOWED_BASE_URL_HOST}/v1/invoices", "allowed_post"),
        ("POST", f"/{REFUSED_BASE_URL_HOST}/v1/entries", "unknown_host_post"),
        ("GET", f"/{REFUSED_BASE_URL_HOST}/v1/entries", "unknown_host_get"),
        ("POST", "/", "no_host_post"),
        ("POST", "//v1/entries", "empty_host_post"),
        ("POST", f"/{ALLOWED_BASE_URL_HOST}:99999/v1/invoices", "bad_port_post"),
    )
    for method, path, label in calls:
        form = {"amount": "700"} if method == "POST" else None
        resp = http(method, listener + path, form=form, label=label)
        agentkit.obs("answer", label=label, status=resp.status, body=resp.json())
    _head_then_get(listener)


# The host the HEAD in `base_url_hosts` names, which no map or flag names either.
HEAD_HOST = "evil.example.com"


def _head_then_get(listener: str) -> None:
    """Send a refused base-URL HEAD, then a GET on the same kept-alive connection.

    A body sent after the HEAD's answer would be read as the start of the GET's."""
    parts = urlsplit(listener)
    conn = HTTPConnection(parts.hostname or "127.0.0.1", parts.port, timeout=agentkit.timeout())
    try:
        for method, path, label in (
            ("HEAD", f"/{HEAD_HOST}/x", "head_unknown_host"),
            ("GET", f"/{REFUSED_BASE_URL_HOST}/v1/entries", "get_after_head"),
        ):
            conn.request(method, path)
            resp = conn.getresponse()
            body = resp.read()
            answered_by = resp.getheader("Irimi-Answered-By")
            agentkit.obs_http(
                method, listener + path, label, status=resp.status, answered_by=answered_by
            )
            agentkit.obs(
                "answer",
                label=label,
                status=resp.status,
                body=json.loads(body) if body else None,
                content_length=resp.getheader("Content-Length"),
            )
    except (OSError, HTTPException) as err:
        agentkit.obs("answer", label="keep_alive_broke", error=type(err).__name__)
    finally:
        conn.close()


# -- the control endpoint, called as the SDK will call it -----------------------------------------
#
# These groups post a run's start, tool calls and end by hand, so a test controls exactly what
# `irimi shadow` stores, and can provoke every refusal the SDK never does.

# The module half of each posted run's entrypoint: this module's importable name.
ENTRYPOINT = "tests.e2e.workflows.agents.w09_scope_gauntlet.agent"
AGENT_VERSION = "w09-by-hand"
SDK_VERSION = "by-hand"
# Past `irimi.core.limits.MAX_ERROR_MESSAGE` (1000), the most of an error message a trace keeps.
LONG_MESSAGE_CHARS = 1500
# A key in a posted error, across each cut a trace makes of one; `ghp_`'s shape needs 36
# characters, so a cut part of it would not match the shape.
ERROR_KEY = "ghp_" + "W9errorKey" + "0" * 26
# Where ERROR_KEY starts in a posted message: across the cut, its placeholder ending before it.
ERROR_KEY_AT = 966
# Past `irimi.core.limits.MAX_ERROR_TYPE` (300): a 657-character dotted path with ERROR_KEY
# among its names, across that cut.
LONG_ERROR_TYPE = "pkg" + ".sub" * 65 + f".{ERROR_KEY}" + ".sub" * 85 + ".RuntimeError"
# Past `irimi.engine.control.MAX_CONTROL_BODY` (2 MiB), the most a control request may post.
OVERSIZED_BODY_BYTES = 2 * 1024 * 1024 + 1
# How deep `refuse_deep` nests a tool call's name: deeper than `repr` goes on some Pythons, yet
# within what every Python's `json.loads` reads.
DEEP_FIELD_LEVELS = 5_000
# How long `_settle` waits for irimi's store to write what is queued.
SETTLE_S = 5.0


def control_runs() -> None:
    """Post runs by hand over the control endpoint, each with a labelled read between tool calls.

    One run posts direct, one through the proxy to itself, two in lockstep on two threads, and one
    ends in error with a key across each cut; then every documented refusal."""
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
        error={"type": "builtins.ValueError", "message": _keyed_message("card ", "x")},
    )
    _end(
        control,
        "ctl-error",
        "error_end",
        outcome="error",
        error={"type": LONG_ERROR_TYPE, "message": _keyed_message("run ", "y")},
    )
    _refusals(control)
    _health(control, "health_after")


def _keyed_message(head: str, filler: str) -> str:
    """Return a LONG_MESSAGE_CHARS message with ERROR_KEY at ERROR_KEY_AT, across the cut."""
    before = head + filler * (ERROR_KEY_AT - len(head) - 1) + " "
    after = " " + filler * (LONG_MESSAGE_CHARS - len(before) - len(ERROR_KEY) - 1)
    return before + ERROR_KEY + after


def control_hazards() -> None:
    """Make the posts the SDK never makes, so what irimi does with each is pinned.

    That is a start re-posted for an ended run, a start, tool call and end for irimi's own run, and
    a tool call for `unattributed`. The health check's `dropped` counts the re-posted start."""
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
    # irimi's own run: the process run under shadow, `unattributed` under serve; a bare run has
    # neither, but makes the same calls.
    process_run = os.environ.get(RUN_ENV) or (
        "unattributed" if os.environ.get(CONTROL_ENV) else "no-process-run"
    )
    agentkit.obs("process_run", id=process_run)
    for label, resp in [
        ("process_start", _start(control, process_run, "process_start", name="takeover")),
        (
            "process_tool",
            _tool_call(control, process_run, "look_up", "process_tool", result={"run": "process"}),
        ),
        ("process_end", _end(control, process_run, "process_end")),
        (
            "unattributed_tool",
            _tool_call(control, "unattributed", "look_up", "unattributed_tool", result=None),
        ),
    ]:
        agentkit.obs("answer", label=label, body=resp.json())
    _health(control, "health_after")


def _control_base() -> str:
    """Return `IRIMI_CONTROL`, or for a bare run the same URL shape on the fake internet."""
    given = os.environ.get(CONTROL_ENV)
    agentkit.obs("control_base", given=given is not None)
    return given or (agentkit.proxy() or "").rstrip("/") + "/_irimi"


def _through_proxy(control: str) -> str:
    """Return the endpoint spelled `127.1`, not in NO_PROXY, so posts go through the proxy."""
    parts = urlsplit(control)
    return parts._replace(netloc=f"127.1:{parts.port}").geturl()


def _run_by_hand(
    control: str, run_id: str, read_path: str, tag: str, step: Callable[[], object] = lambda: None
) -> None:
    """Post one run as the SDK would, calling `step` between posts to keep threads in lockstep."""
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
    return _post(control, run_id, "start", _start_doc(run_id, name), label)


def _start_doc(run_id: str, name: str | None = None) -> dict[str, Any]:
    return {
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
        "awaited": False,
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
    """Make one control post, logging the body so a test can compare it with the stored record."""
    agentkit.obs("posted", label=label, run=run_id, action=action, body=doc)
    return http("POST", f"{control}/runs/{run_id}/{action}", json_body=doc, label=label)


def _refusals(control: str) -> None:
    """Provoke each documented refusal once, each under a run id nothing else uses."""
    ended = {"ended_at": time.time(), "outcome": "ok", "error": None}
    started = {"agent_version": None, "sdk_version": SDK_VERSION, "started_at": time.time()}
    tool: dict[str, Any] = {
        "tool_call_id": "t",
        "name": "look_up",
        "kind": "read",
        "ran": "real",
        "args": {},
        "result": None,
        "error": None,
        "started_at": time.time(),
        "ended_at": time.time(),
        "awaited": False,
    }
    big = b'{"pad": "' + b"x" * OVERSIZED_BODY_BYTES + b'"}'
    nested = "[" * DEEP_FIELD_LEVELS + "]" * DEEP_FIELD_LEVELS
    deep = json.dumps({**tool, "name": None}).replace('"name": null', f'"name": {nested}').encode()
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
        ("refuse_deep", "POST", "/runs/ctl-refused-deep/tool-calls", None, deep),
    ]
    for label, method, route, doc, body in refusals:
        headers = {"Content-Type": "application/json"} if body is not None else None
        resp = http(method, control + route, json_body=doc, body=body, headers=headers, label=label)
        agentkit.obs("refusal", label=label, body=resp.json())
    # A start from an SDK that speaks another major version of the control protocol.
    label = "refuse_protocol"
    resp = http(
        "POST",
        f"{control}/runs/ctl-refused-protocol/start",
        json_body=started,
        headers={"Irimi-Protocol": "2"},
        label=label,
    )
    agentkit.obs("refusal", label=label, body=resp.json())
    agentkit.obs("protocol", label=label, spoken=resp.headers.get("irimi-protocol"))


def _health(control: str, label: str) -> None:
    """Log `GET /_irimi/health` once the store has written what it had queued."""
    _settle(control)
    resp = http("GET", f"{control}/health", label=label)
    agentkit.obs("health", label=label, body=resp.json())


def _settle(control: str) -> None:
    """Wait, unlogged, until the health check says the store has nothing queued."""
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
    # Logged, so a slow writer reads as itself rather than as a health pin's wrong count.
    agentkit.obs("settle_timeout")


# What each body `bombs` sends decodes to, past irimi's decoding cap and the fake internet's.
BOMB_PADDING_BYTES = 128 * 1024 * 1024
# Padding after a body's text: lines of blanks, which JSON ignores, or one endless line, which
# leaves irimi no whole line to keep.
BLANK_LINE = b" " * 15 + b"\n"


def bombs() -> None:
    """Send and read back bodies that decode to far more than they weigh.

    A refund, a live model call, a padded refunds read and three control starts; each body sent or
    read is logged with its length and SHA-256."""
    auth = {"Authorization": f"Bearer {agentkit.key('STRIPE_API_KEY')}"}
    form = urlencode({"charge": CHARGE, "amount": "800"}).encode() + b"&metadata[note]="
    _send_bomb(
        "refund_gzip_bomb",
        base("stripe") + "/v1/refunds",
        agentkit.gzip_bomb(form, b"x", BOMB_PADDING_BYTES),
        {**auth, "Content-Type": "application/x-www-form-urlencoded"},
    )
    message = {
        "model": "claude-sonnet-5",
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "hello"}],
    }
    _send_bomb(
        "llm_gzip_bomb",
        base("anthropic") + "/v1/messages",
        agentkit.gzip_bomb(json.dumps(message).encode() + b"\n", BLANK_LINE, BOMB_PADDING_BYTES),
        {
            "x-api-key": agentkit.key("ANTHROPIC_API_KEY"),
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
    )
    stripe("POST", "/v1/refunds", {"charge": CHARGE, "amount": "900"}, label="refund_plain")
    read = stripe("GET", f"/v1/refunds?charge={CHARGE}", label="refunds_padded")
    _log_bytes("received", "refunds_padded", read.body, read.headers.get("content-encoding"))
    control = _control_base()
    for label, run_id, after, line in (
        ("control_bomb_lines", "ctl-bomb-lines", b"\n", BLANK_LINE),
        ("control_bomb_after_a_line", "ctl-bomb-after-a-line", b"\n", b" "),
        ("control_bomb_one_line", "ctl-bomb-one-line", b"", b" "),
    ):
        doc = _start_doc(run_id)
        agentkit.obs("posted", label=label, run=run_id, action="start", body=doc)
        body = agentkit.gzip_bomb(json.dumps(doc).encode() + after, line, BOMB_PADDING_BYTES)
        resp = _send_bomb(
            label,
            f"{control}/runs/{run_id}/start",
            body,
            {"Content-Type": "application/json"},
        )
        agentkit.obs("refusal", label=label, body=resp.json())
    _health(control, "health_after")


def cut_refund() -> None:
    """Make `bombs`'s two refunds alone, the cut one for 700, not 800, for `irimi compare`."""
    _cut_write("/v1/refunds", {"charge": CHARGE, "amount": "700"})


def cut_update() -> None:
    """Do as `cut_refund` does, the cut write being a customer update instead."""
    _cut_write(f"/v1/customers/{CUSTOMER}", {"name": "Gauntlet Ltd"})


def _cut_write(path: str, fields: dict[str, str]) -> None:
    """Send a write that decodes to 128 MiB with no line break, then `bombs`'s plain refund."""
    form = urlencode(fields).encode() + b"&metadata[note]="
    _send_bomb(
        "cut_write",
        base("stripe") + path,
        agentkit.gzip_bomb(form, b"x", BOMB_PADDING_BYTES),
        {
            "Authorization": f"Bearer {agentkit.key('STRIPE_API_KEY')}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    stripe("POST", "/v1/refunds", {"charge": CHARGE, "amount": "900"}, label="refund_plain")


def _send_bomb(label: str, url: str, body: bytes, headers: dict[str, str]) -> Response:
    """POST `body` gzip-encoded, and log what was sent."""
    _log_bytes("sent", label, body, "gzip")
    return http(
        "POST", url, body=body, headers={**headers, "Content-Encoding": "gzip"}, label=label
    )


def _log_bytes(event: str, label: str, body: bytes, encoding: str | None) -> None:
    agentkit.obs(
        event,
        label=label,
        bytes=len(body),
        sha256=hashlib.sha256(body).hexdigest(),
        encoding=encoding,
    )


def unmapped_hosts() -> None:
    """Call hosts no map claims, and a mapped RPC host's unrouted method."""
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
    """Make a write spelled as a GET, which irimi forwards as a read, so it escapes."""
    legacy = agentkit.internal("legacy.internal")
    http("GET", legacy + "/api/delete_user?id=7", label="get_delete_user")


def _chunked_post(url: str, pieces: list[bytes], headers: dict[str, str]) -> None:
    """POST a chunked body, which urllib cannot send, with http.client through the proxy."""
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
    "base_url_hosts": base_url_hosts,
    "control_runs": control_runs,
    "control_hazards": control_hazards,
    "bombs": bombs,
    "cut_refund": cut_refund,
    "cut_update": cut_update,
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
