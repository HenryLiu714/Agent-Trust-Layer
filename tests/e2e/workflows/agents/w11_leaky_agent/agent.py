"""W11 `leaky_agent`: an agent that gets around irimi's proxy, one escape per scenario.

Each escape runs inside `sdk.run`; no run id may ride out on one, where nothing would strip it."""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import json
import os
import socket
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlencode

import httpx
import requests

from tests.e2e.workflows.agents import agentkit, observe

CHARGE = "ch_LEAKY"
CHANNEL = "C0LEAKY"
SERVED = frozenset({"api.stripe.com", "slack.com"})
# The export service `redirected` reads through, reached through the proxy like any named host.
FILES_HOST = "files.internal"
REDIRECT_CLIENTS = ("urllib", "requests", "httpx", "httpx-async")


def port() -> int:
    """The fake internet's port, which a raw socket dials by number."""
    return int(os.environ[agentkit.PORT_ENV])


@contextlib.contextmanager
def _local_dns() -> Iterator[None]:
    """Resolve the fake internet's names to loopback and fail every other, so no escape goes out."""
    real = socket.getaddrinfo

    def fake(host: Any, *args: Any, **kwargs: Any) -> Any:
        name = host.decode() if isinstance(host, bytes) else str(host)
        if name in SERVED:
            return real("127.0.0.1", *args, **kwargs)
        with contextlib.suppress(ValueError):
            if ipaddress.ip_address(name).is_loopback:
                return real(host, *args, **kwargs)
        raise socket.gaierror(socket.EAI_NONAME, f"{name} is not on the fake internet")

    socket.getaddrinfo = fake
    try:
        yield
    finally:
        socket.getaddrinfo = real


def _send(opener: urllib.request.OpenerDirector, label: str, url: str, **kw: Any) -> int:
    """Send one request through `opener`, logged as `agentkit.http` logs one."""
    request = urllib.request.Request(url, **kw)
    status: int
    try:
        with opener.open(request, timeout=agentkit.timeout()) as raw:
            status, headers = raw.status, raw.headers
            raw.read()
    except urllib.error.HTTPError as err:
        status, headers = err.code, err.headers
    answered_by = headers.get("Irimi-Answered-By")
    agentkit.obs_http(request.get_method(), url, label, status=status, answered_by=answered_by)
    return status


def _refund_request() -> dict[str, Any]:
    return {
        "data": urlencode({"charge": CHARGE, "amount": "500"}).encode(),
        "method": "POST",
        "headers": {
            "Authorization": f"Bearer {agentkit.key('STRIPE_API_KEY')}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    }


def proxied() -> None:
    """Refund through the proxy as configured: the control case."""
    agentkit.stripe("POST", "/v1/refunds", {"charge": CHARGE, "amount": "500"}, label="refund")


def proxyless_client() -> None:
    """Refund with no proxy handler, as `requests` does with `trust_env=False`."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with _local_dns():
        _send(opener, "refund", agentkit.base("stripe") + "/v1/refunds", **_refund_request())


def no_proxy_star() -> None:
    """Refund from a subprocess started with `NO_PROXY=*`, as a job runner might start one."""
    env = {**os.environ, "NO_PROXY": "*", "no_proxy": "*"}
    cmd = [
        sys.executable,
        "-m",
        "tests.e2e.workflows.agents.w11_leaky_agent.agent",
        "_child_refund",
    ]
    subprocess.run(cmd, env=env, check=True, timeout=30)


def _child_refund() -> None:
    """Refund in the `NO_PROXY=*` child with plain urllib, which honours it and goes direct."""
    with _local_dns():
        _send(
            urllib.request.build_opener(),
            "refund",
            agentkit.base("stripe") + "/v1/refunds",
            **_refund_request(),
        )


def raw_socket() -> None:
    """Post to Slack in HTTP/1.1 written by hand on a socket, which reads no proxy variables."""
    body = json.dumps({"channel": CHANNEL, "text": "posted around irimi"}).encode()
    head = (
        "POST /api/chat.postMessage HTTP/1.1\r\n"
        f"Host: slack.com:{port()}\r\n"
        f"Authorization: Bearer {agentkit.key('SLACK_BOT_TOKEN')}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    with socket.create_connection(("127.0.0.1", port()), timeout=agentkit.timeout()) as sock:
        sock.sendall(head + body)
        reply = b""
        while chunk := sock.recv(65536):
            reply += chunk
    head_lines = reply.split(b"\r\n\r\n", 1)[0].decode("latin-1").split("\r\n")
    status = int(head_lines[0].split(" ", 2)[1])
    fields = dict(line.split(":", 1) for line in head_lines[1:] if ":" in line)
    answered_by = {k.strip().lower(): v.strip() for k, v in fields.items()}.get("irimi-answered-by")
    url = f"http://slack.com:{port()}/api/chat.postMessage"
    agentkit.obs_http("POST", url, "post", status=status, answered_by=answered_by)


def loopback_service() -> None:
    """Write to a loopback sidecar, which irimi's own `NO_PROXY` sends direct."""
    agentkit.http(
        "POST",
        agentkit.internal("127.0.0.1") + "/queue/jobs",
        json_body={"job": "refund", "charge": CHARGE},
        label="enqueue",
    )


def _enqueue_stand_in(job: dict[str, Any]) -> dict[str, Any]:
    return {"queued": None, "stood_in": True}


@observe.write_tool(fake=_enqueue_stand_in)
def enqueue_job(job: dict[str, Any]) -> dict[str, Any]:
    """Make `loopback_service`'s write, labelled as a write tool."""
    resp = agentkit.http(
        "POST", agentkit.internal("127.0.0.1") + "/queue/jobs", json_body=job, label="enqueue"
    )
    return resp.json() or {}


def labelled_sidecar() -> None:
    """Make the sidecar write through a write tool, whose fake runs under shadow instead."""
    agentkit.obs("queued", doc=enqueue_job({"job": "refund", "charge": CHARGE}))


def redirected() -> None:
    """Read through two redirects to the loopback sidecar with each client the SDK labels.

    The last hop goes direct, except with `requests`, which keeps the first hop's proxy."""
    sidecar = agentkit.internal("127.0.0.1") + "/queue/stats"
    url = agentkit.internal(FILES_HOST) + "/export?" + urlencode({"next": sidecar})
    for client in REDIRECT_CLIENTS:
        if client == "urllib":
            resp = agentkit.http("GET", url, label=f"export:{client}")
            agentkit.obs("landed", client=client, doc=resp.json())
            continue
        if client == "requests":
            got: Any = requests.get(url, timeout=agentkit.timeout())
            status = got.status_code
        elif client == "httpx":
            with httpx.Client(timeout=agentkit.timeout(), follow_redirects=True) as c:
                got = c.get(url)
            status = got.status_code
        else:
            got = asyncio.run(_httpx_async_get(url))
            status = got.status_code
        doc, hops = got.json(), len(got.history)
        answered_by = got.headers.get("Irimi-Answered-By")
        agentkit.obs_http("GET", url, f"export:{client}", status=status, answered_by=answered_by)
        agentkit.obs("landed", client=client, doc=doc, redirects=hops)


async def _httpx_async_get(url: str) -> httpx.Response:
    async with httpx.AsyncClient(timeout=agentkit.timeout(), follow_redirects=True) as c:
        return await c.get(url)


ESCAPES = {
    "proxied": proxied,
    "proxyless_client": proxyless_client,
    "no_proxy_star": no_proxy_star,
    "raw_socket": raw_socket,
    "loopback_service": loopback_service,
    "labelled_sidecar": labelled_sidecar,
    "redirected": redirected,
}


def main(argv: list[str]) -> int:
    agentkit.start()
    if argv == ["_child_refund"]:
        _child_refund()
        return 0
    if len(argv) != 1 or argv[0] not in ESCAPES:
        print(f"usage: agent.py {{{','.join(ESCAPES)}}}", file=sys.stderr)
        return 2
    with observe.run("escape", inputs={"escape": argv[0]}):
        ESCAPES[argv[0]]()
    agentkit.obs("result", escape=argv[0])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
