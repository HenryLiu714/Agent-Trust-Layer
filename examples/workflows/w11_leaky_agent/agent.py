"""W11 `leaky_agent`: an agent that gets around irimi, one escape per scenario.

`irimi shadow` virtualizes only what is routed through its proxy, and says so in its banner
("hosts not routed through the proxy are NOT virtualized"). Each escape here is a real way agent
code ends up off that route. Under shadow every escape's write really lands on the fake service.
These are the cases Phase 4's readiness checks exist to flag.

Each escape runs inside `sdk.run`, so under shadow it is a run whose requests the SDK labels, but
only those that go through irimi (#75): a run's id must never ride out on an escape, where nothing
would strip it. Universal invariant 2 checks that no fake service ever saw `Irimi-Run`.

    python -m examples.workflows.launch \\
        examples.workflows.w11_leaky_agent.agent <escape>

Safety: no escape ever resolves a real host name. A client that bypasses the proxy resolves the
name itself, so this agent resolves the fake internet's names to 127.0.0.1 (`_local_dns`) and
refuses every other name. A raw socket and a loopback sidecar dial 127.0.0.1 directly.
"""

from __future__ import annotations

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

from examples.workflows import agentkit, sdk

CHARGE = "ch_LEAKY"
CHANNEL = "C0LEAKY"
SERVED = frozenset({"api.stripe.com", "slack.com"})


def port() -> int:
    """The fake internet's port, which a raw socket dials by number."""
    return int(os.environ[agentkit.PORT_ENV])


@contextlib.contextmanager
def _local_dns() -> Iterator[None]:
    """The fake internet's names resolve to loopback; every other name fails. Never the real
    internet, whatever the escape."""
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
    """One request through `opener`, logged the way `agentkit.http` logs one."""
    request = urllib.request.Request(url, **kw)
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
    """The control: the same refund, through the proxy as configured."""
    agentkit.stripe("POST", "/v1/refunds", {"charge": CHARGE, "amount": "500"}, label="refund")


def proxyless_client() -> None:
    """A client built with no proxy handler: what `requests` does with `trust_env=False`."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with _local_dns():
        _send(opener, "refund", agentkit.base("stripe") + "/v1/refunds", **_refund_request())


def no_proxy_star() -> None:
    """A subprocess started with `NO_PROXY=*`, as a job runner or a shell profile might."""
    env = {**os.environ, "NO_PROXY": "*", "no_proxy": "*"}
    cmd = [sys.executable, "-m", "examples.workflows.w11_leaky_agent.agent", "_child_refund"]
    subprocess.run(cmd, env=env, check=True, timeout=30)


def _child_refund() -> None:
    """In the `NO_PROXY=*` child: an ordinary urllib call, which honours NO_PROXY and goes
    direct."""
    with _local_dns():
        _send(
            urllib.request.build_opener(),
            "refund",
            agentkit.base("stripe") + "/v1/refunds",
            **_refund_request(),
        )


def raw_socket() -> None:
    """HTTP/1.1 written by hand on a TCP socket: no HTTP client, so no proxy variables at all."""
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
    """A sidecar on loopback (a queue, a database's HTTP API). `irimi shadow` itself sets
    `NO_PROXY=localhost,127.0.0.1`, so the default configuration sends this straight to it."""
    agentkit.http(
        "POST",
        agentkit.internal("127.0.0.1") + "/queue/jobs",
        json_body={"job": "refund", "charge": CHARGE},
        label="enqueue",
    )


ESCAPES = {
    "proxied": proxied,
    "proxyless_client": proxyless_client,
    "no_proxy_star": no_proxy_star,
    "raw_socket": raw_socket,
    "loopback_service": loopback_service,
}


def main(argv: list[str]) -> int:
    agentkit.start()
    if argv == ["_child_refund"]:
        _child_refund()
        return 0
    if len(argv) != 1 or argv[0] not in ESCAPES:
        print(f"usage: agent.py {{{','.join(ESCAPES)}}}", file=sys.stderr)
        return 2
    with sdk.run(trigger={"escape": argv[0]}, name="escape"):
        ESCAPES[argv[0]]()
    agentkit.obs("result", escape=argv[0])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
