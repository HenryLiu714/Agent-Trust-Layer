"""W6's scenarios: labelled database and filesystem tools under shadow, and the gaps around them.

Every assertion about a write tool is about which body ran. Under shadow the stand-in runs, so
the CRM and the state directory must be exactly as seeded. Two scenarios pin gaps rather than
guarantees: `read_after_write` (no tool overlay, so a read tool cannot see a stood-in write) and
`unlabeled_write` (a write with no `@sdk.tool` label happens for real; the case for Phase 4's
"production database URL" readiness check).
"""

from __future__ import annotations

from examples.workflows.harness.internet import Req, Resp
from examples.workflows.harness.run import Scenario, Workflow
from examples.workflows.harness.services import JsonService, World


def _enrichment(world: World) -> None:
    def company(req: Req, state: dict) -> Resp:
        account = req.path.rsplit("/", 1)[1]
        return Resp(200, {"id": account, "industry": "manufacturing"})

    world.extra.append(JsonService(("enrich.internal",), {("GET", r"/v1/companies/\w+"): company}))


WORKFLOW = Workflow(
    name="w06_crm_db_agent",
    summary="Tools the proxy cannot see: SQLite and file writes labelled with @sdk.tool, run as "
    "their stand-ins under shadow, plus the gaps (no tool overlay, unlabelled writes).",
    scenarios={
        "enrich": Scenario(
            ("enrich", "smb"),
            setup=_enrichment,
            doc="every tool kind: upsert, delete, bulk update, CSV export, async notify",
        ),
        "read_after_write": Scenario(
            ("read_after_write", "smb"),
            doc="a read tool after a stood-in write sees the old row (no tool overlay)",
        ),
        "unlabeled_write": Scenario(
            ("unlabeled_write",),
            doc="a SQLite write with no @sdk.tool label happens for real under shadow",
        ),
        "tool_raises": Scenario(
            ("tool_raises",),
            doc="a read tool raises and is caught; another raises and ends the run in error",
        ),
        "decoration_errors": Scenario(
            ("decoration_errors",),
            doc="each of #76's decoration-time TypeErrors, raised before any call",
        ),
    },
)
