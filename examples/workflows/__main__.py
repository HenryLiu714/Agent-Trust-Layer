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
from typing import TextIO

from examples.workflows.harness.run import MODES, Workflow, check_invariants, run, workflows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m examples.workflows")
    parser.add_argument("workflow", nargs="?")
    parser.add_argument("scenario", nargs="?")
    parser.add_argument("--mode", choices=(*MODES, "both"), default="both")
    args = parser.parse_args(argv)
    catalog = workflows()
    if args.workflow is None:
        for wf in catalog.values():
            _list(wf, sys.stdout)
        return 0
    wf = catalog.get(args.workflow)
    if wf is None:
        print(f"error: no workflow {args.workflow!r}. Workflows:", file=sys.stderr)
        print("\n".join(f"  {name}" for name in catalog), file=sys.stderr)
        return 2
    if args.scenario not in wf.scenarios:
        wanted = "name a scenario" if args.scenario is None else f"no scenario {args.scenario!r}"
        print(f"error: {wanted} of {wf.name}:", file=sys.stderr)
        _list(wf, sys.stderr)
        return 2
    modes = MODES if args.mode == "both" else (args.mode,)
    results = {}
    with tempfile.TemporaryDirectory(prefix="irimi-workflow-") as tmp:
        for mode in modes:
            result = run(wf, args.scenario, mode, Path(tmp) / mode)
            results[mode] = result
            print(f"\n=== {mode}: agent exited {result.exit_code}")
            for call in result.calls():
                status = call.get("status", call.get("error"))
                answered_by = call.get("answered_by") or "(live)"
                print(f"  {call['method']:<7} {call['url']:<60} {status}  {answered_by}")
            if result.irimi:
                print("  --- irimi")
                print("\n".join(f"  {line}" for line in result.irimi))
            writes = result.internet.writes()
            print(f"  --- writes that reached the fake services: {len(writes)}")
            for req in writes:
                print(f"  {req.method} {req.host}{req.path}")
    if "shadow" in results:
        problems = check_invariants(results["shadow"], results.get("bare"))
        print(f"\ninvariants: {f'{len(problems)} broken' if problems else 'all hold'}")
        for problem in problems:
            print(f"  BROKEN {problem}")
        return 1 if problems else 0
    return 0


def _list(wf: Workflow, out: TextIO) -> None:
    print(f"{wf.name}  {wf.summary}", file=out)
    for scenario, spec in wf.scenarios.items():
        print(f"    {scenario:<24} {spec.doc}", file=out)


if __name__ == "__main__":
    sys.exit(main())
