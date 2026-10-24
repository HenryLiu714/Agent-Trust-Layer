"""W3 `queue_worker`: an in-process queue consumer, one run per message, runs that must not mix.

Each run reads and refunds its own charge, labelled `read:<charge>` and `refund:<charge>`."""

from __future__ import annotations

import asyncio
import socket
import sys
import threading
from collections.abc import Coroutine
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from http.client import HTTPConnection
from typing import Any
from urllib.parse import urlencode, urlsplit

import httpx
import requests

from irimi import sdk
from tests.e2e.workflows.agents import agentkit, observe

# The client message `i` uses is `CLIENTS[i % 6]`: the SDK labels every one but the raw asyncio
# client, which labels its requests itself, as an `aiohttp` user would.
CLIENTS = ("urllib", "http.client", "asyncio", "requests", "httpx", "httpx-async")
# The shared clients `one_pool_8` alternates between, each with a pool of one connection, so runs
# share a connection and only their `Irimi-Run` tells them apart.
POOLED = ("requests-pooled", "httpx-pooled")


def charge_id(i: int) -> str:
    return f"ch_Q{i}"


# -- the six clients ------------------------------------------------------------------------------


def _target(url: str) -> tuple[str, int, str]:
    """Return where to connect and the request target: the proxy and absolute URL, if configured."""
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
    resp = requests.request(method, url, data=body, headers=headers, timeout=agentkit.timeout())
    return resp.status_code, resp.headers.get("Irimi-Answered-By")


def _via_httpx(method: str, url: str, body: bytes | None, headers: dict[str, str]) -> Any:
    with httpx.Client(timeout=agentkit.timeout()) as client:
        resp = client.request(method, url, content=body, headers=headers)
    return resp.status_code, resp.headers.get("Irimi-Answered-By")


async def _via_httpx_async(
    method: str, url: str, body: bytes | None, headers: dict[str, str]
) -> Any:
    async with httpx.AsyncClient(timeout=agentkit.timeout()) as client:
        resp = await client.request(method, url, content=body, headers=headers)
    return resp.status_code, resp.headers.get("Irimi-Answered-By")


_pools: dict[str, Any] = {}
_pools_lock = threading.Lock()
# Each new connection a pooled client opens, by client: set per thread around a pooled call.
_opened: dict[str, int] = {}
_pooled_call = threading.local()


def _pool(client: str) -> Any:
    """Return `client`'s one shared instance, made on first use, with room for one connection."""
    with _pools_lock:
        if client not in _pools:
            if client == "requests-pooled":
                session = requests.Session()
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=1, pool_maxsize=1, pool_block=True
                )
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                _pools[client] = session
            else:
                limits = httpx.Limits(max_connections=1, max_keepalive_connections=1)
                _pools[client] = httpx.Client(timeout=agentkit.timeout(), limits=limits)
        return _pools[client]


def _count_connections() -> None:
    """Count each connection a pooled call opens, so `one_pool_8` can show its runs shared one."""
    connect = socket.socket.connect

    def counting(self: Any, address: Any) -> None:
        client = getattr(_pooled_call, "client", None)
        if client is not None:
            with _pools_lock:
                _opened[client] = _opened.get(client, 0) + 1
        connect(self, address)

    socket.socket.connect = counting  # type: ignore[method-assign]


def _via_pool(
    client: str, method: str, url: str, body: bytes | None, headers: dict[str, str]
) -> Any:
    _pooled_call.client = client
    try:
        if client == "requests-pooled":
            resp = _pool(client).request(
                method, url, data=body, headers=headers, timeout=agentkit.timeout()
            )
            return resp.status_code, resp.headers.get("Irimi-Answered-By")
        resp = _pool(client).request(method, url, content=body, headers=headers)
        return resp.status_code, resp.headers.get("Irimi-Answered-By")
    finally:
        _pooled_call.client = None


def call(
    index: int,
    method: str,
    path: str,
    form: dict[str, Any] | None,
    label: str,
    client: str | None = None,
) -> Any:
    """Make one Stripe call through `client`, or message `index`'s, logged as `agentkit.http` logs.

    Returns the answer's JSON for the urllib client, None for the others."""
    client = client or CLIENTS[index % len(CLIENTS)]
    if client == "urllib":
        return agentkit.stripe(method, path, form, label=label).json()
    url = agentkit.base("stripe") + path
    headers = {"Authorization": f"Bearer {agentkit.key('STRIPE_API_KEY')}"}
    body = None
    if form is not None:
        body = urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    run_id = sdk.current_run_id()
    if run_id is not None and client == "asyncio":
        # The SDK labels every other client's requests; this one it cannot see.
        headers["Irimi-Run"] = run_id
    if client == "http.client":
        status, answered_by = _via_http_client(method, url, body, headers)
    elif client == "asyncio":
        status, answered_by = asyncio.run(_via_asyncio(method, url, body, headers))
    elif client == "requests":
        status, answered_by = _via_requests(method, url, body, headers)
    elif client == "httpx":
        status, answered_by = _via_httpx(method, url, body, headers)
    elif client in POOLED:
        status, answered_by = _via_pool(client, method, url, body, headers)
    else:
        status, answered_by = asyncio.run(_via_httpx_async(method, url, body, headers))
    agentkit.obs_http(method, url, label, status=status, answered_by=answered_by, client=client)
    return None


# -- the ledger: the worker's own record of its refunds, as tools the proxy cannot see ------------

LEDGER = "ledger.sqlite3"


def seed_ledger() -> None:
    """Create the ledger tables; this is setup, not a tool, so it happens in every mode."""
    with agentkit.sqlite(LEDGER) as db:
        db.execute("CREATE TABLE IF NOT EXISTS refunds (charge TEXT, amount INTEGER)")
        db.execute("CREATE TABLE IF NOT EXISTS acks (charge TEXT, queue TEXT)")


def _refunded(charge: str) -> int:
    with agentkit.sqlite(LEDGER) as db:
        row = db.execute("SELECT COALESCE(SUM(amount), 0) FROM refunds WHERE charge = ?", (charge,))
        return int(row.fetchone()[0])


@observe.read_tool
def ledger_balance(charge: str) -> int:
    return _refunded(charge)


@observe.read_tool
async def ledger_balance_async(charge: str) -> int:
    await asyncio.sleep(0)  # lets the other messages' tasks run in between
    return _refunded(charge)


@observe.read_tool
def ledger_history(charge: str) -> Coroutine[Any, Any, list[int]]:
    """Return a coroutine from a sync read tool, whose call ends when the coroutine does."""
    return _history(charge)


async def _history(charge: str) -> list[int]:
    await asyncio.sleep(0)
    with agentkit.sqlite(LEDGER) as db:
        rows = db.execute("SELECT amount FROM refunds WHERE charge = ? ORDER BY amount", (charge,))
        return [int(r[0]) for r in rows]


def _insert_refund(charge: str, amount: int) -> None:
    with agentkit.sqlite(LEDGER) as db:
        db.execute("INSERT INTO refunds VALUES (?, ?)", (charge, amount))


def _record_stand_in(charge: str, amount: int) -> dict[str, Any]:
    return {"recorded": False, "charge": charge}


@observe.write_tool(fake=_record_stand_in)
def record_refund(charge: str, amount: int) -> dict[str, Any]:
    _insert_refund(charge, amount)
    return {"recorded": True, "charge": charge}


@observe.write_tool(fake=_record_stand_in)
def mark_refunded(charge: str, amount: int) -> dict[str, Any]:
    """`record_refund` under another name: the worker's second way to write the same row."""
    _insert_refund(charge, amount)
    return {"recorded": True, "charge": charge}


# `ledger_post_waits`: set as the first run's async ledger write ends, so the second run writes
# after it; None in every other scenario.
_ledger_written: asyncio.Event | None = None


def _ledger_written_now() -> None:
    if _ledger_written is not None:
        _ledger_written.set()


async def _record_async_stand_in(charge: str, amount: int) -> dict[str, Any]:
    await asyncio.sleep(0)
    _ledger_written_now()
    return {"recorded": False, "charge": charge}


@observe.write_tool(fake=_record_async_stand_in)
async def record_refund_async(charge: str, amount: int) -> dict[str, Any]:
    await asyncio.sleep(0)
    _insert_refund(charge, amount)
    _ledger_written_now()
    return {"recorded": True, "charge": charge}


# -- the worker -----------------------------------------------------------------------------------


def read(message: dict[str, Any]) -> None:
    charge = message["charge"]
    path = f"/v1/charges/{charge}"
    doc = call(message["index"], "GET", path, None, f"read:{charge}", message.get("client"))
    if isinstance(doc, dict):
        agentkit.obs("saw", charge=charge, amount_refunded=doc.get("amount_refunded"))


def refund(message: dict[str, Any]) -> None:
    charge = message["charge"]
    form = {"charge": charge, "amount": str(message["amount"])}
    call(message["index"], "POST", "/v1/refunds", form, f"refund:{charge}", message.get("client"))
    if message.get("ledger") == "sync":
        # In the refund's thread: a propagated one keeps the run, a bare one lands in the process
        # run. An odd message of `ledger_two_tools_8` writes through the second tool.
        two = message.get("two_tools") and message["index"] % 2
        record = mark_refunded if two else record_refund
        agentkit.obs("ledger", charge=charge, recorded=record(charge, message["amount"]))


@observe.trigger(name="audit")
def audit(message: dict[str, Any]) -> None:
    """Read the refunds: a trigger called inside another, so it joins that run."""
    charge = message["charge"]
    call(message["index"], "GET", f"/v1/refunds?charge={charge}", None, f"audit:{charge}")


@observe.trigger(name="handle")
def handle(message: dict[str, Any]) -> None:
    read(message)
    if message.get("ledger") == "sync":
        agentkit.obs("ledger", charge=message["charge"], before=ledger_balance(message["charge"]))
    if message.get("poison"):
        raise ValueError(f"cannot refund {message['charge']}: poison message")
    how = message.get("refund_in", "inline")
    if how == "propagated_thread":
        with ThreadPoolExecutor(1) as pool:
            pool.submit(sdk.propagate(refund), message).result()
    elif how == "bare_thread":
        # No `sdk.propagate`: the refund carries no `Irimi-Run`, so it lands in the engine's own
        # run, the process run under shadow and `unattributed` under serve.
        worker = threading.Thread(target=refund, args=(message,))
        worker.start()
        worker.join()
    elif how == "in_turn":
        refund_in_turn(message)
    else:
        refund(message)
    if message.get("audit"):
        audit(message)


@dataclass
class _Turns:
    """Order `concurrent_shared_charge`: message 0 refunds first, and no read until both have."""

    first_refunded: threading.Event
    both_refunded: threading.Barrier


# Set by `consume` for `concurrent_shared_charge`; None otherwise, and in a replay of one run.
_turns: _Turns | None = None


def refund_in_turn(message: dict[str, Any]) -> None:
    """Refund, then read the charge again once the other run has refunded too."""
    turns = _turns
    if turns is not None and message["index"] != 0:
        turns.first_refunded.wait(agentkit.timeout())
    refund(message)
    if turns is not None:
        turns.first_refunded.set()
        turns.both_refunded.wait(agentkit.timeout())
    read(message)


# Set by `handle_async` once a stuck message is in flight, so the shutdown cancels it mid-run.
_in_flight: asyncio.Event | None = None


@observe.trigger(name="handle_async")
async def handle_async(message: dict[str, Any]) -> None:
    # `to_thread` copies the context, so the run follows the call into the thread.
    await asyncio.to_thread(read, message)
    if message.get("stuck"):
        # It waits until the worker's shutdown cancels it: the run ends in CancelledError, which
        # the SDK records and re-raises.
        assert _in_flight is not None
        _in_flight.set()
        await asyncio.Event().wait()
    await asyncio.to_thread(refund, message)
    if message.get("ledger") == "async":
        # Each in the message's own task, eight at once.
        charge = message["charge"]
        before = await ledger_balance_async(charge)
        recorded = await record_refund_async(charge, message["amount"])
        history = await ledger_history(charge)
        agentkit.obs("ledger", charge=charge, before=before, recorded=recorded, history=history)


# `ledger_post_waits`' job on the loop's one executor thread, done once the second run has written.
_flushed = threading.Event()


@observe.trigger(name="handle_ledger")
async def handle_ledger(message: dict[str, Any]) -> None:
    """Write one ledger row so the two runs' posts reach irimi in reverse of the order they ended.

    The first run's async post waits behind a job on the loop's one executor thread; the second
    run's sync write posts at once and then lets the job finish. A replay just writes the row."""
    charge, amount = message["charge"], message["amount"]
    together = _ledger_written is not None
    if message["index"] == 0:
        loop = asyncio.get_running_loop()
        job = loop.run_in_executor(None, _flushed.wait, agentkit.timeout()) if together else None
        recorded = await record_refund_async(charge, amount)
        if job is not None:
            await job
    else:
        if _ledger_written is not None:
            await _ledger_written.wait()
        recorded = mark_refunded(charge, amount)
        _flushed.set()
    agentkit.obs("ledger", charge=charge, recorded=recorded)


async def post_waits(messages: list[dict[str, Any]]) -> list[Any]:
    """Run both messages at once on a loop whose default executor has one thread."""
    global _ledger_written  # noqa: PLW0603  # one scenario's knob, set once per process
    _ledger_written = asyncio.Event()
    asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(1))
    return await asyncio.gather(*(handle_ledger(m) for m in messages), return_exceptions=True)


def _journal_stand_in(cls: type, charge: str, net: int) -> dict[str, Any]:
    """Fake the class method; it is called as the class method is, with the class first."""
    return {"journaled": False, "by": cls.__name__}


def _ack_stand_in(worker: Worker, message: dict[str, Any]) -> dict[str, Any]:
    """Fake the method; it is called as the method is, with the instance first."""
    return {"acked": False, "queue": worker.queue}


class Worker:
    """The handlers as a worker's methods, as a consumer framework's class has them.

    A method trigger records its class in the entrypoint and leaves `self` out of the args."""

    def __init__(self, queue: str) -> None:
        self.queue = queue

    @observe.trigger
    def handle(self, message: dict[str, Any]) -> None:
        read(message)
        refund(message)
        self.book(message)

    @observe.trigger(name="worker.handle_async")
    async def handle_async(self, message: dict[str, Any]) -> None:
        await asyncio.to_thread(read, message)
        await asyncio.to_thread(refund, message)
        self.book(message)

    def book(self, message: dict[str, Any]) -> None:
        """Book the refund through method tools: a static fee, a class journal entry and an ack."""
        if message.get("ledger") != "worker":
            return
        charge, amount = message["charge"], message["amount"]
        fee = self.fee(amount)
        journal = Worker.journal(charge, amount - fee)
        ack = self.ack(message)
        agentkit.obs("ledger", charge=charge, fee=fee, journal=journal, ack=ack)

    @observe.read_tool
    @staticmethod
    def fee(amount: int) -> int:
        return amount // 10

    @observe.write_tool(fake=_journal_stand_in)
    @classmethod
    def journal(cls, charge: str, net: int) -> dict[str, Any]:
        _insert_refund(charge, net)
        return {"journaled": True, "by": cls.__name__}

    @observe.write_tool(fake=_ack_stand_in)
    def ack(self, message: dict[str, Any]) -> dict[str, Any]:
        with agentkit.sqlite(LEDGER) as db:
            db.execute("INSERT INTO acks VALUES (?, ?)", (message["charge"], self.queue))
        return {"acked": True, "queue": self.queue}


@observe.trigger(name="enqueue")
def enqueue(message: dict[str, Any]) -> Coroutine[Any, Any, None]:
    """Return the work as a coroutine for the caller's loop, as a decorator over `async def` does.

    The run goes with the coroutine and ends when it ends, so the nested `audit` joins it."""
    return _process(message)


async def _process(message: dict[str, Any]) -> None:
    await asyncio.to_thread(read, message)
    if message.get("poison"):
        raise ValueError(f"cannot refund {message['charge']}: poison message")
    await asyncio.to_thread(audit, message)
    await asyncio.to_thread(refund, message)


@observe.trigger(name="schedule")
def schedule(message: dict[str, Any]) -> asyncio.Task[None]:
    """Start the work as a task and return it; the run ends when `schedule` returns.

    The task was created inside the run, so its context keeps the run's id for its requests."""
    return asyncio.get_running_loop().create_task(_process_later(message))


async def _process_later(message: dict[str, Any]) -> None:
    await asyncio.to_thread(read, message)
    await asyncio.to_thread(refund, message)


async def handle_in_block(message: dict[str, Any]) -> None:
    """Handle one message in an `async with sdk.run(...)` block, a run with no entrypoint."""
    async with observe.run("message", inputs=message):
        await asyncio.to_thread(read, message)
        if message.get("poison"):
            raise ValueError(f"cannot refund {message['charge']}: poison message")
        await asyncio.to_thread(refund, message)


async def shut_down(messages: list[dict[str, Any]]) -> list[Any]:
    """Handle every message as a task, then cancel what is still in flight, as a deploy does."""
    global _in_flight  # noqa: PLW0603  # one scenario's knob, set once per process
    _in_flight = asyncio.Event()
    tasks = [asyncio.create_task(handle_async(m)) for m in messages]
    stuck = [t for t, m in zip(tasks, messages, strict=True) if m.get("stuck")]
    await _in_flight.wait()
    await asyncio.wait([t for t in tasks if t not in stuck])
    for task in stuck:
        task.cancel()
    return await asyncio.gather(*tasks, return_exceptions=True)


def tally(messages: list[dict[str, Any]], outcomes: list[Any]) -> list[bool]:
    """Return each message's verdict from `asyncio.gather`; a ValueError or cancel fails it."""
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
        # Each worker returns its own verdict; a shared counter would race.
        try:
            handle(message)
        except ValueError as exc:
            agentkit.obs("message.failed", charge=message["charge"], error=str(exc))
            return False
        return True

    if how in ("threads", "in_turn"):
        if how == "in_turn":
            global _turns  # noqa: PLW0603  # one scenario's knob, set once per process
            _turns = _Turns(threading.Event(), threading.Barrier(len(messages)))
        with ThreadPoolExecutor(4) as pool:
            done = list(pool.map(safe, messages))
    elif how in ("asyncio", "async_run", "handoff", "task"):
        handler = {
            "asyncio": handle_async,
            "async_run": handle_in_block,
            "handoff": enqueue,
            "task": schedule,
        }[how]

        async def all_of() -> list[Any]:
            runs = (handler(m) for m in messages)
            return await asyncio.gather(*runs, return_exceptions=True)

        done = tally(messages, asyncio.run(all_of()))
    elif how == "shutdown":
        done = tally(messages, asyncio.run(shut_down(messages)))
    elif how == "post_waits":
        done = tally(messages, asyncio.run(post_waits(messages)))
    elif how == "pooled":
        _count_connections()
        with ThreadPoolExecutor(4) as pool:
            done = list(pool.map(safe, messages))
        agentkit.obs("pool", opened=_opened)
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
    "concurrent_shared_charge": (2, "in_turn", {"refund_in": "in_turn"}),
    "async_run": (4, "async_run", {}),
    "cancelled_on_shutdown": (3, "shutdown", {}),
    "worker_methods": (4, "worker", {}),
    "coroutine_handoff": (4, "handoff", {}),
    "one_pool_8": (8, "pooled", {}),
    "task_outlives_trigger": (2, "task", {}),
    # The worker's ledger tools, in each place a run's identity has to follow them.
    "ledger_threads_8": (8, "threads", {"refund_in": "propagated_thread", "ledger": "sync"}),
    "ledger_two_tools_8": (
        8,
        "threads",
        {"refund_in": "propagated_thread", "ledger": "sync", "two_tools": True},
    ),
    "ledger_asyncio_8": (8, "asyncio", {"ledger": "async"}),
    "ledger_post_waits": (2, "post_waits", {}),
    "ledger_unpropagated": (2, "serial", {"refund_in": "bare_thread", "ledger": "sync"}),
    "ledger_worker": (4, "worker", {"ledger": "worker"}),
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
    if scenario in ("shared_charge", "concurrent_shared_charge"):
        # Indexes 0 and len(CLIENTS): both through urllib, so both reads' bodies are logged (`saw`).
        indexes = (0, len(CLIENTS))
        return [{"charge": SHARED_CHARGE, "amount": 3000, "index": i, **extra} for i in indexes]
    out = [{"charge": charge_id(i), "amount": 100 + i, "index": i, **extra} for i in range(count)]
    if scenario == "one_pool_8":
        for message in out:
            message["client"] = POOLED[message["index"] % len(POOLED)]
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
    if argv[0].startswith("ledger_"):
        seed_ledger()
    ok, failed = consume(messages_for(argv[0]), SCENARIOS[argv[0]][1])
    agentkit.obs("result", scenario=argv[0], ok=ok, failed=failed)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
