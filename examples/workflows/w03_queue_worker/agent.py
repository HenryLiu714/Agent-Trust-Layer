"""W3 `queue_worker`: an in-process queue consumer, one run per message, runs that must not mix.

Each message names a charge. `handle(message)` is the trigger: it reads the charge and refunds
part of it. The two calls go through a client chosen by the message's index, because the SDK has
to label a run's requests whichever client makes them (#75):

    0  urllib (`agentkit.http`)          2  a raw asyncio HTTP/1.1 client through the proxy
    1  `http.client` through the proxy   3  `requests`, when it is installed

Every call is labelled `read:<charge>` or `refund:<charge>`, so a test can check that run R's
calls name only R's own charge.

    python -m examples.workflows.w03_queue_worker.agent <scenario>
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
from typing import Any
from urllib.parse import urlencode, urlsplit

from examples.workflows import agentkit, sdk

CLIENTS = ("urllib", "http.client", "asyncio", "requests")


def charge_id(i: int) -> str:
    return f"ch_Q{i}"


# -- the four clients -----------------------------------------------------------------------------


def _target(url: str) -> tuple[str, int, str]:
    """Where to connect and what to put on the request line: the proxy and the absolute URL when
    one is configured, as every proxied client does; the host and the path otherwise."""
    proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    parts = urlsplit(proxy or url)
    if proxy:
        return parts.hostname or "127.0.0.1", parts.port or 80, url
    path = parts.path + (f"?{parts.query}" if parts.query else "")
    return parts.hostname or "", parts.port or 80, path


def _via_http_client(method: str, url: str, body: bytes | None, headers: dict[str, str]) -> Any:
    host, port, target = _target(url)
    conn = HTTPConnection(host, port, timeout=agentkit.timeout())
    try:
        conn.request(method, target, body=body, headers=headers)
        resp = conn.getresponse()
        resp.read()
        return resp.status, resp.getheader("Irimi-Answered-By")
    finally:
        conn.close()


async def _via_asyncio(method: str, url: str, body: bytes | None, headers: dict[str, str]) -> Any:
    host, port, target = _target(url)
    parts = urlsplit(url)
    reader, writer = await asyncio.open_connection(host, port)
    lines = [f"{method} {target} HTTP/1.1", f"Host: {parts.netloc}", "Connection: close"]
    lines += [f"{k}: {v}" for k, v in headers.items()]
    lines.append(f"Content-Length: {len(body or b'')}")
    writer.write(("\r\n".join(lines) + "\r\n\r\n").encode() + (body or b""))
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(), agentkit.timeout())
    writer.close()
    head = raw.split(b"\r\n\r\n", 1)[0].decode("latin-1").split("\r\n")
    status = int(head[0].split()[1])
    found = {k.strip().lower(): v.strip() for k, _, v in (h.partition(":") for h in head[1:])}
    return status, found.get("irimi-answered-by")


def _via_requests(method: str, url: str, body: bytes | None, headers: dict[str, str]) -> Any:
    import requests  # only in the `examples` group; the caller checks it is importable

    resp = requests.request(method, url, data=body, headers=headers, timeout=agentkit.timeout())
    return resp.status_code, resp.headers.get("Irimi-Answered-By")


def _has_requests() -> bool:
    try:
        import requests  # noqa: F401
    except ImportError:
        return False
    return True


def call(index: int, method: str, path: str, form: dict[str, Any] | None, label: str) -> Any:
    """One Stripe call through message `index`'s client, logged like `agentkit.http` logs.
    Returns the answer's JSON for the urllib client, None for the others."""
    client = CLIENTS[index % len(CLIENTS)]
    if client == "requests" and not _has_requests():
        client = "requests-unavailable"
    if client in ("urllib", "requests-unavailable"):
        return agentkit.stripe(method, path, form, label=label).json()
    url = agentkit.base("stripe") + path
    headers = {"Authorization": f"Bearer {agentkit.key('STRIPE_API_KEY')}"}
    body = None
    if form is not None:
        body = urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    run_id = sdk.current_run_id()
    if run_id is not None:
        # Only agentkit's urllib client labels a run's requests today; #75 patches the rest.
        headers["Irimi-Run"] = run_id
    if client == "http.client":
        status, answered_by = _via_http_client(method, url, body, headers)
    elif client == "asyncio":
        status, answered_by = asyncio.run(_via_asyncio(method, url, body, headers))
    else:
        status, answered_by = _via_requests(method, url, body, headers)
    parts = urlsplit(url)
    agentkit.obs(
        "http",
        method=method,
        url=f"{parts.hostname}{parts.path}",
        label=label,
        status=status,
        answered_by=answered_by,
        run=run_id,
        client=client,
    )
    return None


# -- the worker -----------------------------------------------------------------------------------


def read(message: dict[str, Any]) -> None:
    charge = message["charge"]
    doc = call(message["index"], "GET", f"/v1/charges/{charge}", None, f"read:{charge}")
    if isinstance(doc, dict):
        agentkit.obs("saw", charge=charge, amount_refunded=doc.get("amount_refunded"))


def refund(message: dict[str, Any]) -> None:
    charge = message["charge"]
    form = {"charge": charge, "amount": str(message["amount"])}
    call(message["index"], "POST", "/v1/refunds", form, f"refund:{charge}")


@sdk.trigger(name="audit")
def audit(message: dict[str, Any]) -> None:
    """A trigger called from inside another: it joins the run it is called in (#74)."""
    charge = message["charge"]
    call(message["index"], "GET", f"/v1/refunds?charge={charge}", None, f"audit:{charge}")


@sdk.trigger(name="handle")
def handle(message: dict[str, Any]) -> None:
    read(message)
    if message.get("poison"):
        raise ValueError(f"cannot refund {message['charge']}: poison message")
    how = message.get("refund_in", "inline")
    if how == "propagated_thread":
        with ThreadPoolExecutor(1) as pool:
            pool.submit(sdk.propagate(refund), message).result()
    elif how == "bare_thread":
        # No `sdk.propagate`: the thread starts with an empty context, so the refund belongs to
        # no run. Once #75 exists this is the request that lands in `unattributed/`.
        worker = threading.Thread(target=refund, args=(message,))
        worker.start()
        worker.join()
    else:
        refund(message)
    if message.get("audit"):
        audit(message)


@sdk.trigger(name="handle_async")
async def handle_async(message: dict[str, Any]) -> None:
    # `to_thread` copies the context, so the run follows the call into the thread.
    await asyncio.to_thread(read, message)
    await asyncio.to_thread(refund, message)


def consume(messages: list[dict[str, Any]], how: str) -> tuple[int, int]:
    """Process every message; one failure does not stop the queue. Returns (ok, failed)."""
    failed = 0

    def safe(message: dict[str, Any]) -> None:
        nonlocal failed
        try:
            handle(message)
        except ValueError as exc:
            failed += 1
            agentkit.obs("message.failed", charge=message["charge"], error=str(exc))

    if how == "threads":
        with ThreadPoolExecutor(4) as pool:
            list(pool.map(safe, messages))
    elif how == "asyncio":

        async def all_of() -> None:
            await asyncio.gather(*(handle_async(m) for m in messages))

        asyncio.run(all_of())
    else:
        for message in messages:
            safe(message)
    return len(messages) - failed, failed


SCENARIOS: dict[str, tuple[int, str, dict[str, Any]]] = {
    # name: (message count, how the queue is consumed, extra fields on every message)
    "threads_8": (8, "threads", {"refund_in": "propagated_thread"}),
    "asyncio_8": (8, "asyncio", {}),
    "unpropagated_thread": (2, "serial", {"refund_in": "bare_thread"}),
    "nested_trigger": (2, "serial", {"audit": True}),
    "one_message_fails": (4, "serial", {}),
    "shared_charge": (2, "serial", {}),
}
POISON = {"one_message_fails": 2}  # the message index that raises
SHARED_CHARGE = "ch_QSHARED"  # 4900; two runs each refund 3000 of it


def messages_for(scenario: str) -> list[dict[str, Any]]:
    count, _, extra = SCENARIOS[scenario]
    if scenario == "shared_charge":
        # Indexes 0 and 4: both through urllib, so both reads' bodies are logged (`saw`).
        return [{"charge": SHARED_CHARGE, "amount": 3000, "index": i} for i in (0, 4)]
    out = [{"charge": charge_id(i), "amount": 100 + i, "index": i, **extra} for i in range(count)]
    if scenario in POISON:
        out[POISON[scenario]]["poison"] = True
    return out


def main(argv: list[str]) -> int:
    agentkit.start()
    if len(argv) != 1 or argv[0] not in SCENARIOS:
        print(f"usage: agent.py {{{','.join(SCENARIOS)}}}", file=sys.stderr)
        return 2
    ok, failed = consume(messages_for(argv[0]), SCENARIOS[argv[0]][1])
    agentkit.obs("result", scenario=argv[0], ok=ok, failed=failed)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
