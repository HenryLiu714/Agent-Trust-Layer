"""`python -m examples.llm_refund_agent TICKET_ID CHARGE MESSAGE`: the agent under its own module
name, `examples.llm_refund_agent.agent`, so its trigger's entrypoint is importable (#84, #85)."""

import sys

from examples.llm_refund_agent import agent

sys.exit(agent.main(sys.argv[1:]))
