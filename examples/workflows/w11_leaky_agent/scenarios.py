"""W11's scenarios: one escape each, and a control that goes through the proxy.

Every escape is marked `leaks`: under shadow its write really reaches the fake service, irimi
prints no exchange line for it, and its summary does not count it. That is irimi's documented
limit today ("hosts not routed through the proxy are NOT virtualized"). When Phase 4's readiness
checks land, each escape's assertion flips from "irimi never saw it" to "irimi flagged it".
"""

from __future__ import annotations

from examples.workflows.harness.internet import Req, Resp
from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import JsonService, World
from examples.workflows.w11_leaky_agent.agent import CHANNEL, CHARGE


def _services(world: World) -> None:
    world.stripe.add_charge(CHARGE, 5000)
    world.slack.add_channel(CHANNEL, "leaky")

    def enqueue(req: Req, state: dict) -> Resp:
        state.setdefault("jobs", []).append(req.json())
        return Resp(201, {"queued": len(state["jobs"])})

    # A sidecar on loopback, addressed as 127.0.0.1 the way agent config names one.
    world.extra.append(JsonService(("127.0.0.1",), {("POST", "/queue/jobs"): enqueue}))


WORKFLOW = Workflow(
    name="w11_leaky_agent",
    summary="An agent that gets around irimi on purpose: no proxy handler, NO_PROXY=*, a raw "
    "socket, a loopback sidecar. The cases Phase 4's readiness checks must flag.",
    scenarios={
        "proxied": Scenario(("proxied",), setup=_services, doc="control: the refund, faked"),
        "proxyless_client": Scenario(
            ("proxyless_client",),
            setup=_services,
            leaks=True,
            doc="a client with no proxy handler (requests' trust_env=False) refunds for real",
        ),
        "no_proxy_star": Scenario(
            ("no_proxy_star",),
            setup=_services,
            leaks=True,
            doc="a subprocess with NO_PROXY=* refunds for real",
        ),
        "raw_socket": Scenario(
            ("raw_socket",),
            setup=_services,
            leaks=True,
            doc="HTTP written on a raw socket posts to Slack for real",
        ),
        "loopback_service": Scenario(
            ("loopback_service",),
            setup=_services,
            leaks=True,
            doc="a loopback sidecar is exempt by irimi's own NO_PROXY default",
        ),
    },
)
