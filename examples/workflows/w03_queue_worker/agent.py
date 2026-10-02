"""W3 `queue_worker`: an in-process queue consumer, one run per message, runs that must not mix.

Each message names a charge. `handle(message)` is the trigger: it reads the charge and refunds
part of it. The two calls go through a client chosen by the message's index, because the SDK has
to label a run's requests whichever client makes them (#75):

    0  urllib (`agentkit.http`)          2  a raw asyncio HTTP/1.1 client through the proxy
    1  `http.client` through the proxy   3  `requests`, when it is installed

Every call is labelled `read:<charge>` or `refund:<charge>`, so a test can check that run R's
calls name only R's own charge.

A run is marked each way the SDK offers (#74): `@sdk.trigger` on a function, sync or async, and on
a `Worker`'s methods, and `async with sdk.run(...)` around a block.

    python -m examples.workflows.launch \\
        examples.workflows.w03_queue_worker.agent <scenario>
"""

from __future__ import annotations

import asyncio
import sys
import threading
from collections.abc import Coroutine
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
    proxy = agentkit.proxy()
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
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), agentkit.timeout())
    try:
        lines = [f"{method} {target} HTTP/1.1", f"Host: {parts.netloc}", "Connection: close"]
        lines += [f"{k}: {v}" for k, v in headers.items()]
        lines.append(f"Content-Length: {len(body or b'')}")
        writer.write(("\r\n".join(lines) + "\r\n\r\n").encode() + (body or b""))
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), agentkit.timeout())
    finally:
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
    agentkit.obs_http(method, url, label, status=status, answered_by=answered_by, client=client)
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
        # no run. Once #75 exists it falls back to the engine's own run: the process run under
        # `irimi shadow -- <cmd>`, and `unattributed` only in serve mode (#77).
        worker = threading.Thread(target=refund, args=(message,))
        worker.start()
        worker.join()
    else:
        refund(message)
    if message.get("audit"):
        audit(message)


# Set by `handle_async` once a stuck message is in flight, so the shutdown cancels it mid-run.
_in_flight: asyncio.Event | None = None


@sdk.trigger(name="handle_async")
async def handle_async(message: dict[str, Any]) -> None:
    # `to_thread` copies the context, so the run follows the call into the thread.
    await asyncio.to_thread(read, message)
    if message.get("stuck"):
        # Its refund waits on an upstream that never answers, until the worker's shutdown cancels
        # it: the run ends in CancelledError, a BaseException the SDK records and re-raises (#74).
        assert _in_flight is not None
        _in_flight.set()
        await asyncio.Event().wait()
    await asyncio.to_thread(refund, message)


class Worker:
    """The handlers as a worker object's methods, as a consumer framework's class has them. A
    trigger on a method records its class in the entrypoint and leaves `self` out of the args; the
    async one carries a name of its own, which its runs are stored by (#74)."""

    def __init__(self, queue: str) -> None:
        self.queue = queue

    @sdk.trigger
    def handle(self, message: dict[str, Any]) -> None:
        read(message)
        refund(message)

    @sdk.trigger(name="worker.handle_async")
    async def handle_async(self, message: dict[str, Any]) -> None:
        await asyncio.to_thread(read, message)
        await asyncio.to_thread(refund, message)


@sdk.trigger(name="enqueue")
def enqueue(message: dict[str, Any]) -> Coroutine[Any, Any, None]:
    """A sync handler that returns its work as a coroutine for the caller's loop to await, as a
    plain decorator over an `async def` does. Its body has not run when it returns, so the run
    goes with the coroutine, current while it runs and ended when it ends (#74): the nested
    `audit` it calls joins that run, and the poison message ends it in error."""
    return _process(message)


async def _process(message: dict[str, Any]) -> None:
    await asyncio.to_thread(read, message)
    if message.get("poison"):
        raise ValueError(f"cannot refund {message['charge']}: poison message")
    await asyncio.to_thread(audit, message)
    await asyncio.to_thread(refund, message)


async def handle_in_block(message: dict[str, Any]) -> None:
    """One message as a block that is one run, `async with sdk.run(...)`, with the message as its
    trigger (#74): no function names it, so the run has no entrypoint."""
    async with sdk.run(trigger=message, name="message"):
        await asyncio.to_thread(read, message)
        if message.get("poison"):
            raise ValueError(f"cannot refund {message['charge']}: poison message")
        await asyncio.to_thread(refund, message)


async def shut_down(messages: list[dict[str, Any]]) -> list[Any]:
    """Handle every message as a task, then shut down as a worker does on a deploy: once the rest
    are done, cancel what is still in flight. The stuck message is cancelled mid-run, past its read
    (#74)."""
    global _in_flight
    _in_flight = asyncio.Event()
    tasks = [asyncio.create_task(handle_async(m)) for m in messages]
    stuck = [t for t, m in zip(tasks, messages, strict=True) if m.get("stuck")]
    await _in_flight.wait()
    await asyncio.wait([t for t in tasks if t not in stuck])
    for task in stuck:
        task.cancel()
    return await asyncio.gather(*tasks, return_exceptions=True)


def tally(messages: list[dict[str, Any]], outcomes: list[Any]) -> list[bool]:
    """Each message's verdict from what `asyncio.gather` returned for it. A ValueError or a
    cancellation fails the message and is logged; anything else raised is the worker's own bug."""
    done = []
    for message, outcome in zip(messages, outcomes, strict=True):
        if isinstance(outcome, ValueError | asyncio.CancelledError):
            error = str(outcome) or type(outcome).__name__
            agentkit.obs("message.failed", charge=message["charge"], error=error)
        elif isinstance(outcome, BaseException):
            raise outcome
        done.append(outcome is None)
    return done


def consume(messages: list[dict[str, Any]], how: str) -> tuple[int, int]:
    """Process every message; one failure does not stop the queue. Returns (ok, failed)."""

    def safe(message: dict[str, Any]) -> bool:
        # Each worker returns its own verdict; a shared counter bumped from four threads would
        # race.
        try:
            handle(message)
        except ValueError as exc:
            agentkit.obs("message.failed", charge=message["charge"], error=str(exc))
            return False
        return True

    if how == "threads":
        with ThreadPoolExecutor(4) as pool:
            done = list(pool.map(safe, messages))
    elif how in ("asyncio", "async_run", "handoff"):
        handler = {"asyncio": handle_async, "async_run": handle_in_block, "handoff": enqueue}[how]

        async def all_of() -> list[Any]:
            runs = (handler(m) for m in messages)
            return await asyncio.gather(*runs, return_exceptions=True)

        done = tally(messages, asyncio.run(all_of()))
    elif how == "shutdown":
        done = tally(messages, asyncio.run(shut_down(messages)))
    elif how == "worker":
        # Even messages through the sync method, odd ones through the async one.
        worker = Worker("refunds")
        for message in messages:
            if message["index"] % 2:
                asyncio.run(worker.handle_async(message))
            else:
                worker.handle(message)
        done = [True] * len(messages)
    else:
        done = [safe(message) for message in messages]
    return done.count(True), done.count(False)


SCENARIOS: dict[str, tuple[int, str, dict[str, Any]]] = {
    # name: (message count, how the queue is consumed, extra fields on every message)
    "threads_8": (8, "threads", {"refund_in": "propagated_thread"}),
    "asyncio_8": (8, "asyncio", {}),
    "unpropagated_thread": (2, "serial", {"refund_in": "bare_thread"}),
    "nested_trigger": (2, "serial", {"audit": True}),
    "one_message_fails": (4, "serial", {}),
    "shared_charge": (2, "serial", {}),
    "async_run": (4, "async_run", {}),
    "cancelled_on_shutdown": (3, "shutdown", {}),
    "worker_methods": (4, "worker", {}),
    "coroutine_handoff": (4, "handoff", {}),
}
POISON = {
    "one_message_fails": 2,
    "async_run": 2,
    "coroutine_handoff": 2,
}  # the message index that raises
STUCK = {"cancelled_on_shutdown": 1}  # the message index still in flight at shutdown
SHARED_CHARGE = "ch_QSHARED"  # 4900; two runs each refund 3000 of it


def messages_for(scenario: str) -> list[dict[str, Any]]:
    count, _, extra = SCENARIOS[scenario]
    if scenario == "shared_charge":
        # Indexes 0 and 4: both through urllib, so both reads' bodies are logged (`saw`).
        return [{"charge": SHARED_CHARGE, "amount": 3000, "index": i} for i in (0, 4)]
    out = [{"charge": charge_id(i), "amount": 100 + i, "index": i, **extra} for i in range(count)]
    if scenario in POISON:
        out[POISON[scenario]]["poison"] = True
    if scenario in STUCK:
        out[STUCK[scenario]]["stuck"] = True
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
