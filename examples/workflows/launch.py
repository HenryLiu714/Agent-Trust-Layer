"""Run a workflow's agent under its real module name.

    python -m examples.workflows.launch examples.workflows.w01_ticket_triage.agent <args...>

`python -m <agent>` would run the agent as `__main__`. Then #74 records a trigger's entrypoint as
`__main__:handle_ticket`, which #84's replay cannot import, and #76 names every tool
`__main__.<fn>`. The harness launches every agent through this module instead, which imports the
agent by name and calls its `main(argv)`.
"""

import importlib
import sys


def main() -> int:
    module = importlib.import_module(sys.argv[1])
    return int(module.main(sys.argv[2:]))


if __name__ == "__main__":
    sys.exit(main())
