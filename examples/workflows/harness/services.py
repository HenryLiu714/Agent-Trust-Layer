"""The fake services behind the fake internet: Stripe, Slack, a scripted LLM, and generic JSON
services for a workflow's own internal APIs.

Each one is stateful in the way the real service is, so a BARE run of a workflow (no irimi) really
refunds, really posts, and really rejects a second refund. That bare run is the baseline a shadow
run is compared with: an agent that behaves the same under shadow while the fake records no write
is shadow mode working. Error bodies copy the real services' shapes, because the agents branch on
them and irimi's fakes copy the same shapes.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any

from examples.workflows.harness.internet import SAFE_METHODS, Req, Resp, json_error

# -- Stripe ---------------------------------------------------------------------------------------


def charge(
    charge_id: str,
    amount: int = 4900,
    *,
    currency: str = "usd",
    customer: str | None = None,
    amount_refunded: int = 0,
    created: int = 1_790_000_000,
    **extra: Any,
) -> dict[str, Any]:
    """A succeeded, paid charge, as `GET /v1/charges/{id}` answers it."""
    return {
        "id": charge_id,
        "object": "charge",
        "amount": amount,
        "amount_refunded": amount_refunded,
        "refunded": amount_refunded >= amount,
        "currency": currency,
        "customer": customer,
        "status": "succeeded",
        "paid": True,
        "created": created,
        "metadata": {},
        **extra,
    }


def _stripe_error(status: int, code: str, message: str, param: str | None = None) -> Resp:
    error: dict[str, Any] = {"type": "invalid_request_error", "code": code, "message": message}
    if param:
        error["param"] = param
    return json_error(status, error=error)


class FakeStripe:
    hosts = ("api.stripe.com",)

    def __init__(self) -> None:
        self.charges: dict[str, dict[str, Any]] = {}
        self.customers: dict[str, dict[str, Any]] = {}
        self.refunds: list[dict[str, Any]] = []
        self.payment_intents: dict[str, dict[str, Any]] = {}
        self.disputes: dict[str, dict[str, Any]] = {}
        self.idempotency: dict[str, tuple[str, Resp]] = {}
        self._ids = itertools.count(1)
        self._lock = threading.Lock()

    # seeding
    def add_charge(self, charge_id: str, amount: int = 4900, **kwargs: Any) -> dict[str, Any]:
        self.charges[charge_id] = charge(charge_id, amount, **kwargs)
        return self.charges[charge_id]

    def add_customer(self, customer_id: str, **fields: Any) -> dict[str, Any]:
        self.customers[customer_id] = {
            "id": customer_id,
            "object": "customer",
            "email": f"{customer_id}@example.test",
            "metadata": {},
            **fields,
        }
        return self.customers[customer_id]

    def add_payment_intent(self, pi_id: str, amount: int = 4900, **fields: Any) -> None:
        self.payment_intents[pi_id] = {
            "id": pi_id,
            "object": "payment_intent",
            "amount": amount,
            "currency": "usd",
            "status": "requires_capture",
            **fields,
        }

    def add_dispute(self, dispute_id: str, charge_id: str, amount: int, reason: str) -> None:
        self.disputes[dispute_id] = {
            "id": dispute_id,
            "object": "dispute",
            "charge": charge_id,
            "amount": amount,
            "currency": "usd",
            "reason": reason,
            "status": "needs_response",
        }

    # the Service protocol
    def is_write(self, req: Req) -> bool:
        return req.method not in SAFE_METHODS

    def handle(self, req: Req) -> Resp:
        auth = req.headers.get("authorization", "")
        if not auth.startswith("Bearer sk_test_"):
            return json_error(401, error={"type": "invalid_request_error", "code": "api_key"})
        with self._lock:
            key = req.headers.get("idempotency-key")
            if key and req.method == "POST":
                fingerprint = f"{req.method} {req.path} {sorted(req.params().items())}"
                held = self.idempotency.get(key)
                if held is not None:
                    if held[0] != fingerprint:
                        return _stripe_error(
                            400,
                            "idempotency_error",
                            "Keys for idempotent requests can only be used with the same "
                            "parameters they were first used with.",
                        )
                    # Stripe marks a replay so a client can tell it from a second write.
                    first = held[1]
                    return replace(first, headers={**first.headers, "idempotent-replayed": "true"})
                resp = self._route(req)
                self.idempotency[key] = (fingerprint, resp)
                return resp
            return self._route(req)

    def _route(self, req: Req) -> Resp:
        p, m = req.path, ("GET" if req.method == "HEAD" else req.method)
        if m == "GET" and p == "/v1/charges":
            return self._list(
                list(self.charges.values()), req, "/v1/charges", "customer", "customer"
            )
        if found := re.fullmatch(r"/v1/charges/([^/]+)", p):
            if m == "GET":
                return self._one(self.charges, found.group(1), "charge")
        if p == "/v1/refunds":
            if m == "POST":
                return self._refund(req.params())
            if m == "GET":
                return self._list(self.refunds, req, "/v1/refunds", "charge", "charge")
        if found := re.fullmatch(r"/v1/refunds/([^/]+)", p):
            refunds = {r["id"]: r for r in self.refunds}
            return self._one(refunds, found.group(1), "refund")
        if found := re.fullmatch(r"/v1/customers/([^/]+)", p):
            cid = found.group(1)
            if m == "GET":
                return self._one(self.customers, cid, "customer")
            if m == "POST":
                if cid not in self.customers:
                    return self._missing("customer", cid)
                customer = self.customers[cid]
                for name, value in req.params().items():
                    meta = re.fullmatch(r"metadata\[(.+)\]", name)
                    if meta:
                        customer["metadata"][meta.group(1)] = value
                    elif name in ("email", "name", "description"):
                        customer[name] = value
                return Resp(200, customer)
        if found := re.fullmatch(r"/v1/payment_intents/([^/]+)(/cancel)?", p):
            pid = found.group(1)
            if found.group(2) and m == "POST":
                if pid not in self.payment_intents:
                    return self._missing("payment_intent", pid)
                intent = self.payment_intents[pid]
                if intent["status"] == "canceled":
                    return _stripe_error(
                        400,
                        "payment_intent_unexpected_state",
                        "This PaymentIntent's status is canceled.",
                    )
                intent["status"] = "canceled"
                return Resp(200, intent)
            if not found.group(2) and m == "GET":
                return self._one(self.payment_intents, pid, "payment_intent")
        if (found := re.fullmatch(r"/v1/disputes/([^/]+)", p)) and m == "GET":
            return self._one(self.disputes, found.group(1), "dispute")
        if (found := re.fullmatch(r"/v1/balance_transactions/([^/]+)", p)) and m == "GET":
            return Resp(200, {"id": found.group(1), "object": "balance_transaction"})
        return _stripe_error(404, "resource_missing", f"Unrecognized request URL ({m}: {p}).")

    def _missing(self, kind: str, object_id: str) -> Resp:
        return _stripe_error(404, "resource_missing", f"No such {kind}: '{object_id}'", "id")

    def _one(self, table: dict[str, dict[str, Any]], object_id: str, kind: str) -> Resp:
        found = table.get(object_id)
        return Resp(200, found) if found is not None else self._missing(kind, object_id)

    def _list(
        self, items: list[dict[str, Any]], req: Req, url: str, param: str, field_name: str
    ) -> Resp:
        wanted = req.query.get(param)
        if wanted:
            items = [i for i in items if i.get(field_name) == wanted]
        # Newest first, as Stripe lists: later-created objects come first.
        items = sorted(items, key=lambda i: (i.get("created", 0), i["id"]), reverse=True)
        after = req.query.get("starting_after")
        if after:
            ids = [i["id"] for i in items]
            if after not in ids:
                return self._missing("object", after)
            items = items[ids.index(after) + 1 :]
        limit = int(req.query.get("limit") or 10)
        return Resp(
            200,
            {"object": "list", "url": url, "has_more": len(items) > limit, "data": items[:limit]},
        )

    def _refund(self, params: dict[str, Any]) -> Resp:
        charge_id = params.get("charge")
        if not charge_id or charge_id not in self.charges:
            return self._missing("charge", str(charge_id))
        ch = self.charges[charge_id]
        left = ch["amount"] - ch["amount_refunded"]
        if left <= 0:
            return _stripe_error(
                400, "charge_already_refunded", f"Charge {charge_id} has already been refunded."
            )
        amount = int(params.get("amount") or left)
        if amount > left:
            return _stripe_error(
                400,
                "amount_too_large",
                f"Refund amount ({amount}) is greater than unrefunded amount on charge ({left})",
                "amount",
            )
        ch["amount_refunded"] += amount
        ch["refunded"] = ch["amount_refunded"] >= ch["amount"]
        refund = {
            "id": f"re_fake{next(self._ids):06d}",
            "object": "refund",
            "amount": amount,
            "charge": charge_id,
            "currency": ch["currency"],
            "status": "succeeded",
            "reason": params.get("reason"),
            "created": int(time.time()),
            "metadata": {},
        }
        self.refunds.append(refund)
        return Resp(200, refund)


# -- Slack ----------------------------------------------------------------------------------------

# The Web API methods the fake treats as reads. Every other method is a write, including one the
# fake does not implement (`chat.delete`, `chat.update`): a Slack call is always a POST, so only a
# named read can be told apart, and an unnamed method that escaped must trip invariant 1.
SLACK_READS = frozenset(
    {
        "/api/auth.test",
        "/api/conversations.history",
        "/api/conversations.info",
        "/api/conversations.list",
        "/api/conversations.replies",
        "/api/users.info",
    }
)


class FakeSlack:
    hosts = ("slack.com", "hooks.slack.com", "files.slack.com")

    def __init__(self) -> None:
        self.channels: dict[str, dict[str, Any]] = {}
        self.messages: dict[str, list[dict[str, Any]]] = {}
        self.users: dict[str, dict[str, Any]] = {}
        self.scopes: set[str] = {"chat:write", "reactions:write", "files:write"}
        self.webhook_posts: list[dict[str, Any]] = []
        self._ts = itertools.count(1)
        self._lock = threading.Lock()

    def add_channel(
        self, channel_id: str, name: str, *, is_member: bool = True, is_archived: bool = False
    ) -> None:
        self.channels[channel_id] = {
            "id": channel_id,
            "name": name,
            "is_channel": True,
            "is_member": is_member,
            "is_archived": is_archived,
        }
        self.messages.setdefault(channel_id, [])

    def add_message(
        self, channel_id: str, ts: str, text: str, user: str = "U0HUMAN", **fields: Any
    ) -> dict[str, Any]:
        message = {"type": "message", "ts": ts, "user": user, "text": text, **fields}
        self.messages.setdefault(channel_id, []).append(message)
        return message

    def add_user(self, user_id: str, name: str, **fields: Any) -> None:
        self.users[user_id] = {"id": user_id, "name": name, "real_name": name.title(), **fields}

    def is_write(self, req: Req) -> bool:
        if req.host == "files.slack.com":  # file downloads: an ordinary HTTP host
            return req.method not in SAFE_METHODS
        return req.host == "hooks.slack.com" or req.path not in SLACK_READS

    def handle(self, req: Req) -> Resp:
        with self._lock:
            if req.host == "hooks.slack.com":
                self.webhook_posts.append(req.json() or {})
                return Resp(200, "ok")
            if not req.headers.get("authorization", "").startswith("Bearer xoxb-"):
                return Resp(200, {"ok": False, "error": "not_authed"})
            method = req.path.removeprefix("/api/")
            handler = getattr(self, "_" + method.replace(".", "_"), None)
            if handler is None:
                return Resp(200, {"ok": False, "error": "unknown_method"})
            return Resp(200, handler(req.params()))

    def _channel(self, spelled: Any) -> dict[str, Any] | None:
        spelled = str(spelled or "")
        if spelled in self.channels:
            return self.channels[spelled]
        name = spelled.lstrip("#")
        return next((c for c in self.channels.values() if c["name"] == name), None)

    def _auth_test(self, params: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True, "user_id": "U0BOT", "team_id": "T0TEAM"}

    def _conversations_info(self, params: dict[str, Any]) -> dict[str, Any]:
        found = self.channels.get(str(params.get("channel")))
        if found is None:
            return {"ok": False, "error": "channel_not_found"}
        return {"ok": True, "channel": found}

    def _conversations_list(self, params: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True, "channels": list(self.channels.values())}

    def _conversations_history(self, params: dict[str, Any]) -> dict[str, Any]:
        found = self.channels.get(str(params.get("channel")))
        if found is None:
            return {"ok": False, "error": "channel_not_found"}
        top = [m for m in self.messages[found["id"]] if m.get("thread_ts") in (None, m["ts"])]
        top.sort(key=lambda m: float(m["ts"]), reverse=True)
        limit = int(params.get("limit") or 100)
        return {"ok": True, "messages": top[:limit], "has_more": len(top) > limit}

    def _conversations_replies(self, params: dict[str, Any]) -> dict[str, Any]:
        found = self.channels.get(str(params.get("channel")))
        if found is None:
            return {"ok": False, "error": "channel_not_found"}
        ts = str(params.get("ts"))
        thread = [
            m for m in self.messages[found["id"]] if m["ts"] == ts or m.get("thread_ts") == ts
        ]
        if not thread:
            return {"ok": False, "error": "thread_not_found"}
        thread.sort(key=lambda m: float(m["ts"]))
        return {"ok": True, "messages": thread, "has_more": False}

    def _users_info(self, params: dict[str, Any]) -> dict[str, Any]:
        found = self.users.get(str(params.get("user")))
        if found is None:
            return {"ok": False, "error": "user_not_found"}
        return {"ok": True, "user": found}

    def _chat_postMessage(self, params: dict[str, Any]) -> dict[str, Any]:
        if "chat:write" not in self.scopes:
            return {"ok": False, "error": "missing_scope", "needed": "chat:write"}
        found = self._channel(params.get("channel"))
        if found is None:
            return {"ok": False, "error": "channel_not_found"}
        if found["is_archived"]:
            return {"ok": False, "error": "is_archived"}
        if not found["is_member"]:
            return {"ok": False, "error": "not_in_channel"}
        ts = f"{1_790_000_000 + next(self._ts)}.000100"
        message = {"type": "message", "ts": ts, "user": "U0BOT", "text": params.get("text", "")}
        if params.get("thread_ts"):
            message["thread_ts"] = params["thread_ts"]
        self.messages[found["id"]].append(message)
        return {"ok": True, "channel": found["id"], "ts": ts, "message": message}

    def _reactions_add(self, params: dict[str, Any]) -> dict[str, Any]:
        if "reactions:write" not in self.scopes:
            return {"ok": False, "error": "missing_scope", "needed": "reactions:write"}
        return {"ok": True}

    def _files_upload(self, params: dict[str, Any]) -> dict[str, Any]:
        if "files:write" not in self.scopes:
            return {"ok": False, "error": "missing_scope", "needed": "files:write"}
        return {"ok": True, "file": {"id": "F0FAKE", "name": params.get("filename", "")}}


# -- LLMs -----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class LlmCall:
    """One model call, provider-neutral: what a workflow's script decides from."""

    provider: str  # "anthropic" | "openai"
    system: str
    messages: list[dict[str, Any]]
    tools: list[str]  # tool names offered
    stream: bool

    def last_user_text(self) -> str:
        for message in reversed(self.messages):
            if message.get("role") in ("user", "tool"):
                return _flatten(message.get("content"))
        return ""

    def tool_results(self) -> list[str]:
        """Every tool result the conversation holds so far, as text, oldest first."""
        out = []
        for message in self.messages:
            if message.get("role") == "tool":
                out.append(_flatten(message.get("content")))
            content = message.get("content")
            if isinstance(content, list):
                out += [
                    _flatten(block.get("content"))
                    for block in content
                    if isinstance(block, dict) and block.get("type") == "tool_result"
                ]
        return out


@dataclass(frozen=True)
class LlmTurn:
    """The model's answer: text, tool calls, or both."""

    text: str = ""
    tool_calls: tuple[tuple[str, dict[str, Any]], ...] = ()


def _flatten(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            str(block.get("text") or block.get("content") or "")
            if isinstance(block, dict)
            else str(block)
            for block in content
        )
    return "" if content is None else str(content)


LlmScript = Callable[[LlmCall], LlmTurn]


def _echo_script(call: LlmCall) -> LlmTurn:
    return LlmTurn(text=f"ack: {call.last_user_text()[:60]}")


class ScriptedLLM:
    """Anthropic Messages and OpenAI chat completions, JSON or SSE, answered by a script.

    The script is a pure function of the call, so the same prompt always gets the same answer and a
    changed system prompt gets a different one. That is what makes replay divergence testable.
    `calls` records every call the fake received, for the "the model was asked N times" check.
    """

    hosts = ("api.anthropic.com", "api.openai.com")

    def __init__(self, script: LlmScript = _echo_script) -> None:
        self.script = script
        self.calls: list[LlmCall] = []
        self._ids = itertools.count(1)
        self._lock = threading.Lock()

    def is_write(self, req: Req) -> bool:
        return False

    def handle(self, req: Req) -> Resp:
        if req.host == "api.anthropic.com":
            if not req.headers.get("x-api-key", "").startswith("sk-ant-"):
                return json_error(401, type="error", error={"type": "authentication_error"})
            if req.path == "/v1/messages" and req.method == "POST":
                return self._anthropic(req.json() or {})
        else:
            if not req.headers.get("authorization", "").startswith("Bearer sk-"):
                return json_error(401, error={"type": "invalid_request_error"})
            if req.path == "/v1/chat/completions" and req.method == "POST":
                return self._openai(req.json() or {})
            if req.path == "/v1/embeddings" and req.method == "POST":
                return self._embeddings(req.json() or {})
        return json_error(404, error={"type": "not_found_error"})

    def _turn(self, call: LlmCall) -> tuple[LlmTurn, int]:
        with self._lock:
            self.calls.append(call)
            n = next(self._ids)
        return self.script(call), n

    # Anthropic
    def _anthropic(self, body: dict[str, Any]) -> Resp:
        system = body.get("system")
        call = LlmCall(
            "anthropic",
            _flatten(system),
            list(body.get("messages") or []),
            [t.get("name", "") for t in body.get("tools") or []],
            bool(body.get("stream")),
        )
        turn, n = self._turn(call)
        content: list[dict[str, Any]] = []
        if turn.text:
            content.append({"type": "text", "text": turn.text})
        for i, (name, args) in enumerate(turn.tool_calls):
            content.append(
                {"type": "tool_use", "id": f"toolu_{n:03d}_{i}", "name": name, "input": args}
            )
        stop = "tool_use" if turn.tool_calls else "end_turn"
        message = {
            "id": f"msg_fake{n:04d}",
            "type": "message",
            "role": "assistant",
            "model": body.get("model", "claude-sonnet-5"),
            "content": content,
            "stop_reason": stop,
            "usage": {"input_tokens": 10 * n, "output_tokens": 5},
        }
        if not call.stream:
            return Resp(200, message)
        events: list[tuple[str, dict[str, Any]]] = [
            ("message_start", {"type": "message_start", "message": {**message, "content": []}})
        ]
        for index, block in enumerate(content):
            if block["type"] == "text":
                events.append(
                    (
                        "content_block_start",
                        {
                            "type": "content_block_start",
                            "index": index,
                            "content_block": {"type": "text", "text": ""},
                        },
                    )
                )
                for piece in _pieces(block["text"]):
                    events.append(
                        (
                            "content_block_delta",
                            {
                                "type": "content_block_delta",
                                "index": index,
                                "delta": {"type": "text_delta", "text": piece},
                            },
                        )
                    )
            else:
                start = {**block, "input": {}}
                events.append(
                    (
                        "content_block_start",
                        {"type": "content_block_start", "index": index, "content_block": start},
                    )
                )
                for piece in _pieces(json.dumps(block["input"])):
                    events.append(
                        (
                            "content_block_delta",
                            {
                                "type": "content_block_delta",
                                "index": index,
                                "delta": {"type": "input_json_delta", "partial_json": piece},
                            },
                        )
                    )
            events.append(("content_block_stop", {"type": "content_block_stop", "index": index}))
        events.append(("message_delta", {"type": "message_delta", "delta": {"stop_reason": stop}}))
        events.append(("message_stop", {"type": "message_stop"}))
        chunks = [f"event: {name}\ndata: {json.dumps(data)}\n\n".encode() for name, data in events]
        return Resp(200, chunks=chunks, chunk_delay=0.005)

    # OpenAI
    def _openai(self, body: dict[str, Any]) -> Resp:
        messages = list(body.get("messages") or [])
        system = "".join(_flatten(m.get("content")) for m in messages if m.get("role") == "system")
        call = LlmCall(
            "openai",
            system,
            [m for m in messages if m.get("role") != "system"],
            [t.get("function", {}).get("name", "") for t in body.get("tools") or []],
            bool(body.get("stream")),
        )
        turn, n = self._turn(call)
        tool_calls = [
            {
                "id": f"call_{n:03d}_{i}",
                "type": "function",
                "function": {"name": name, "arguments": json.dumps(args)},
            }
            for i, (name, args) in enumerate(turn.tool_calls)
        ]
        finish = "tool_calls" if tool_calls else "stop"
        if not call.stream:
            message: dict[str, Any] = {"role": "assistant", "content": turn.text or None}
            if tool_calls:
                message["tool_calls"] = tool_calls
            return Resp(
                200,
                {
                    "id": f"chatcmpl-fake{n:04d}",
                    "object": "chat.completion",
                    "model": body.get("model", "gpt-5"),
                    "choices": [{"index": 0, "message": message, "finish_reason": finish}],
                    "usage": {"prompt_tokens": 10 * n, "completion_tokens": 5},
                },
            )
        chunks = []
        for piece in _pieces(turn.text):
            delta = {"choices": [{"index": 0, "delta": {"content": piece}, "finish_reason": None}]}
            chunks.append(f"data: {json.dumps(delta)}\n\n".encode())
        for i, tc in enumerate(tool_calls):
            delta = {
                "choices": [
                    {
                        "index": 0,
                        "delta": {"tool_calls": [{"index": i, **tc}]},
                        "finish_reason": None,
                    }
                ]
            }
            chunks.append(f"data: {json.dumps(delta)}\n\n".encode())
        end = {"choices": [{"index": 0, "delta": {}, "finish_reason": finish}]}
        chunks += [f"data: {json.dumps(end)}\n\n".encode(), b"data: [DONE]\n\n"]
        return Resp(200, chunks=chunks, chunk_delay=0.005)

    def _embeddings(self, body: dict[str, Any]) -> Resp:
        inputs = body.get("input")
        texts = inputs if isinstance(inputs, list) else [inputs]
        data = []
        for i, text in enumerate(texts):
            digest = hashlib.sha256(str(text).encode()).digest()
            data.append(
                {"object": "embedding", "index": i, "embedding": [b / 255 for b in digest[:8]]}
            )
        return Resp(200, {"object": "list", "data": data, "model": body.get("model", "")})


def _pieces(text: str, size: int = 12) -> list[str]:
    return [text[i : i + size] for i in range(0, len(text), size)] or [""]


# -- internal services ----------------------------------------------------------------------------

Route = Callable[[Req, dict[str, Any]], Resp]


class JsonService:
    """A workflow's own internal API: a table of `(method, path regex) -> handler(req, state)`.

    `writes` is the set of methods this service changes state on; by default every unsafe one.
    """

    def __init__(
        self,
        hosts: tuple[str, ...],
        routes: dict[tuple[str, str], Route],
        *,
        state: dict[str, Any] | None = None,
        is_write: Callable[[Req], bool] | None = None,
    ) -> None:
        self.hosts = hosts
        self.routes = routes
        self.state: dict[str, Any] = state if state is not None else {}
        self._is_write = is_write or (lambda req: req.method not in SAFE_METHODS)
        self._lock = threading.Lock()

    def is_write(self, req: Req) -> bool:
        return self._is_write(req)

    def handle(self, req: Req) -> Resp:
        with self._lock:
            for (method, pattern), route in self.routes.items():
                if method in (req.method, "*") and re.fullmatch(pattern, req.path):
                    return route(req, self.state)
        return json_error(404, error=f"no route {req.method} {req.path}")


@dataclass
class World:
    """Every fake a scenario can seed, before the fake internet starts."""

    stripe: FakeStripe = field(default_factory=FakeStripe)
    slack: FakeSlack = field(default_factory=FakeSlack)
    llm: ScriptedLLM = field(default_factory=ScriptedLLM)
    extra: list[Any] = field(default_factory=list)  # JsonService and the like

    def services(self) -> list[Any]:
        return [self.stripe, self.slack, self.llm, *self.extra]
