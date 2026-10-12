"""W11's scenarios: one escape each, and a control that goes through the proxy.

Every escape that writes is marked `leaks`: under shadow its write really reaches the fake service,
irimi prints no exchange line for it, and its summary does not count it. That is irimi's documented
limit today ("hosts not routed through the proxy are NOT virtualized"). When Phase 4's readiness
checks land, each escape's assertion flips from "irimi never saw it" to "irimi flagged it".
`redirected` writes nothing: a read whose redirects lead it off the proxied route, where irimi
never sees its last hop (#75).
"""

from __future__ import annotations

from urllib.parse import urlencode

from examples.workflows.harness.internet import Req, Resp
from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import JsonService, World
from examples.workflows.w11_leaky_agent.agent import CHANNEL, CHARGE, FILES_HOST


def _services(world: World) -> None:
    world.stripe.add_charge(CHARGE, 5000)
    world.slack.add_channel(CHANNEL, "leaky")

    def enqueue(req: Req, state: dict) -> Resp:
        state.setdefault("jobs", []).append(req.json())
        return Resp(201, {"queued": len(state["jobs"])})

    def stats(req: Req, state: dict) -> Resp:
        return Resp(200, {"queued": len(state.get("jobs", []))})

    # A sidecar on loopback, addressed as 127.0.0.1 the way agent config names one.
    world.extra.append(
        JsonService(
            ("127.0.0.1",), {("POST", "/queue/jobs"): enqueue, ("GET", "/queue/stats"): stats}
        )
    )

    # An export service that answers through two redirects: to its own `/export/v2`, through
    # irimi like the first hop, then to wherever `next` names, the loopback sidecar (#75).
    def export(req: Req, state: dict) -> Resp:
        return Resp(302, headers={"location": "/export/v2?" + urlencode(req.query)})

    def export_v2(req: Req, state: dict) -> Resp:
        return Resp(302, headers={"location": req.query["next"]})

    world.extra.append(
        JsonService((FILES_HOST,), {("GET", "/export"): export, ("GET", "/export/v2"): export_v2})
    )


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
        "redirected": Scenario(
            ("redirected",),
            setup=_services,
            doc="a read redirected through irimi and then off it, to a loopback sidecar, by every "
            "client the SDK labels: its last hop carries no Irimi-Run",
        ),
        "loopback_service": Scenario(
            ("loopback_service",),
            setup=_services,
            leaks=True,
            doc="a loopback sidecar is exempt by irimi's own NO_PROXY default",
        ),
    },
)
