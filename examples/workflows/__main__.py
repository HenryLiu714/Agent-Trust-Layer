"""Run a sample workflow by hand.

    uv run python -m examples.workflows                               # list workflows and scenarios
    uv run python -m examples.workflows w09_scope_gauntlet verbs      # bare, then shadow
    uv run python -m examples.workflows w09_scope_gauntlet verbs --mode shadow

Prints what the agent saw per call, what irimi printed, what reached the fake services, and the
universal invariants. Needs no keys and no network: every host is served by the fake internet.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

from examples.workflows.harness.run import MODES, check_invariants, run, workflows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m examples.workflows")
    parser.add_argument("workflow", nargs="?")
    parser.add_argument("scenario", nargs="?")
    parser.add_argument("--mode", choices=(*MODES, "both"), default="both")
    args = parser.parse_args(argv)
    catalog = workflows()
    if args.workflow is None:
        for name, wf in catalog.items():
            print(f"{name}  {wf.summary}")
            for scenario, spec in wf.scenarios.items():
                print(f"    {scenario:<24} {spec.doc}")
        return 0
    wf = catalog.get(args.workflow)
    if wf is None or args.scenario not in wf.scenarios:
        print("error: unknown workflow or scenario; run with no arguments to list them")
        return 2
    modes = MODES if args.mode == "both" else (args.mode,)
    results = {}
    with tempfile.TemporaryDirectory(prefix="irimi-workflow-") as tmp:
        for mode in modes:
            result = run(wf, args.scenario, mode, Path(tmp) / mode)
            results[mode] = result
            print(f"\n=== {mode}: agent exited {result.exit_code}")
            for method, url, status, answered_by in result.answered():
                print(f"  {method:<7} {url:<60} {status}  {answered_by or '(live)'}")
            if result.irimi:
                print("  --- irimi")
                print("\n".join(f"  {line}" for line in result.irimi))
            writes = result.internet.writes()
            print(f"  --- writes that reached the fake services: {len(writes)}")
            for req in writes:
                print(f"  {req.method} {req.host}{req.path}")
    if "shadow" in results:
        problems = check_invariants(results["shadow"], results.get("bare"))
        print("\ninvariants: " + ("all hold" if not problems else ""))
        for problem in problems:
            print(f"  BROKEN {problem}")
        return 1 if problems else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
