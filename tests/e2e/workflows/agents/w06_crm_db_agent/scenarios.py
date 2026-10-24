"""W6's scenarios: labelled database and filesystem tools under shadow, and the gaps around them.

`read_after_write` and `unlabeled_write` pin gaps: no tool overlay; unlabelled writes are real."""

from __future__ import annotations

from typing import Any

from tests.e2e.workflows.harness.internet import Req, Resp
from tests.e2e.workflows.harness.scenario import Scenario, Workflow
from tests.e2e.workflows.harness.services import JsonService, World

# A replay's run id a shell left exported from an earlier replay.
STALE_REPLAY_RUN = "5a1e5a1e5a1e5a1e"


def _enrichment(world: World) -> None:
    def company(req: Req, state: dict[str, Any]) -> Resp:
        account = req.path.rsplit("/", 1)[1]
        return Resp(200, {"id": account, "industry": "manufacturing"})

    world.extra.append(JsonService(("enrich.internal",), {("GET", r"/v1/companies/\w+"): company}))


WORKFLOW = Workflow(
    name="w06_crm_db_agent",
    summary="Tools the proxy cannot see: SQLite and file writes labelled with @sdk.write_tool, "
    "replaced by their fakes under shadow, plus the gaps (no tool overlay, unlabelled writes).",
    scenarios={
        "enrich": Scenario(
            ("enrich", "smb"),
            setup=_enrichment,
            doc="every tool kind: upsert, delete, bulk update, CSV export, async notify",
        ),
        "read_after_write": Scenario(
            ("read_after_write", "smb"),
            # A stale exported `IRIMI_REPLAY_RUN`: shadow removes it from its child, a bare SDK
            # is inert, and a serve agent's lookups miss, as serve does not replay.
            env={"IRIMI_REPLAY_RUN": STALE_REPLAY_RUN},
            doc="a read tool after a faked write sees the old row (no tool overlay), and after "
            "an unlabelled one, which is real, the new last_seen; run with a stale "
            "IRIMI_REPLAY_RUN exported",
        ),
        "unlabeled_write": Scenario(
            ("unlabeled_write",),
            doc="a SQLite write with no @sdk.write_tool label happens for real under shadow",
        ),
        "tool_raises": Scenario(
            ("tool_raises",),
            doc="a read tool raises quoting a DSN, its key across the cut a trace makes of the "
            "message, given options whose repr holds a key across the cut the SDK makes of a "
            "repr, and is caught; another raises and is caught; a third raises and ends the run "
            "in error",
        ),
        "decoration_errors": Scenario(
            ("decoration_errors",),
            doc="each decoration-time TypeError the tool decorators raise, before any call",
        ),
        "merge_retry": Scenario(
            ("merge_retry",),
            doc="a write's fake raises and the agent, holding the very exception it raised, "
            "retries: both calls recorded, the first with its error",
        ),
        "merge_once": Scenario(
            ("merge_once",),
            doc="merge_retry's merge made right the first time: one call, its fake run, which "
            "`irimi compare` holds the retry against",
        ),
        "outside_run": Scenario(
            ("outside_run",),
            doc="tools called before the trigger, in no run: recorded in the process run "
            "(IRIMI_RUN), the trigger's own in its run; in serve mode, `unattributed`",
        ),
        "stand_in_http": Scenario(
            ("stand_in_http",),
            setup=_enrichment,
            doc="a write's fake makes an HTTP read: an ordinary exchange of the run, labelled",
        ),
        "overlapping_writes": Scenario(
            ("overlapping_writes",),
            doc="two async writes at once, the archive started first and ended last: stored, "
            "summarised and compared in the order they ended",
        ),
        "async_tools": Scenario(
            ("async_tools",),
            doc="an async read, a sync read that returns a coroutine, left pending while a sync "
            "read finds the segment, and a sync write that returns one: its call ends, and is "
            "recorded, when the coroutine does, after the read tool it awaits; then an async "
            "static method whose fake is a static method in the same class body",
        ),
    },
)
