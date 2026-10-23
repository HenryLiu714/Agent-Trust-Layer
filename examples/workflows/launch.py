"""Run an agent module's `main(argv)` under its real module name.

    python -m examples.workflows.launch examples.workflows.w01_ticket_triage.agent <args...>

`python -m <agent>` would run the agent as `__main__`, and the SDK would record an entrypoint
(`__main__:handle_ticket`) that replay cannot import and tool names like `__main__.<fn>`. Imported
by name, the agent's entrypoint and tool names are `module:fn` and `module.fn`, so the harness
launches every agent through this module.
"""

import importlib
import sys


def main() -> int:
    if len(sys.argv) < 2:
        usage = "usage: python -m examples.workflows.launch <agent module> [args...]"
        print(usage, file=sys.stderr)
        return 2
    module = importlib.import_module(sys.argv[1])
    return int(module.main(sys.argv[2:]))


if __name__ == "__main__":
    sys.exit(main())
