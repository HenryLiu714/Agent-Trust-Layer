"""W3's scenarios: how the queue is consumed, and how a run's identity is (or is not) carried."""

from __future__ import annotations

from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import World
from examples.workflows.w03_queue_worker.agent import SCENARIOS, SHARED_CHARGE, charge_id


def _charges(world: World) -> None:
    for i in range(max(count for count, _, _ in SCENARIOS.values())):
        world.stripe.add_charge(charge_id(i), 10_000)
    world.stripe.add_charge(SHARED_CHARGE, 4900)


WORKFLOW = Workflow(
    name="w03_queue_worker",
    summary="A queue consumer, one @sdk.trigger run per message, over threads and asyncio and "
    "four HTTP clients: runs must not mix, and a thread without propagate loses its run.",
    scenarios={
        "threads_8": Scenario(
            ("threads_8",),
            setup=_charges,
            doc="8 messages on a 4-thread pool; each refund runs in a propagated thread",
        ),
        "asyncio_8": Scenario(
            ("asyncio_8",), setup=_charges, doc="8 async triggers under asyncio.gather"
        ),
        "unpropagated_thread": Scenario(
            ("unpropagated_thread",),
            setup=_charges,
            doc="the refund runs in a thread started without sdk.propagate: it has no run",
        ),
        "nested_trigger": Scenario(
            ("nested_trigger",), setup=_charges, doc="a trigger called inside a run joins it"
        ),
        "one_message_fails": Scenario(
            ("one_message_fails",),
            setup=_charges,
            doc="one message raises: only its run ends in error, the queue carries on",
        ),
        "shared_charge": Scenario(
            ("shared_charge",),
            setup=_charges,
            doc="two runs refund one charge: the second run's reads see the first run's fake",
        ),
    },
)
