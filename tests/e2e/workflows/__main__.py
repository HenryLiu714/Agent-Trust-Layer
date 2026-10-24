"""Run a sample workflow by hand, keyless and offline, and check the invariants.

Usage: `python -m tests.e2e.workflows [--all | WORKFLOW [SCENARIO] [--mode MODE]]`."""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path
from typing import TextIO

from tests.e2e.workflows.harness.execute import MODES, run
from tests.e2e.workflows.harness.invariants import check_invariants
from tests.e2e.workflows.harness.scenario import Workflow, workflows

PROG = "python -m tests.e2e.workflows"
# Three scenarios that show the most of irimi in one run each.
START_HERE = (
    ("w01_ticket_triage", "double_refund", "an LLM agent refunds twice; irimi fakes both"),
    ("w09_scope_gauntlet", "verbs", "every unsafe verb is faked, routed or not"),
    ("w04_slack_ops_bot", "own_thread", "a reply under a thread irimi minted reads back"),
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog=PROG)
    parser.add_argument("workflow", nargs="?")
    parser.add_argument("scenario", nargs="?")
    parser.add_argument("--mode", choices=(*MODES, "both"), default="both")
    parser.add_argument("--all", action="store_true", help="list every scenario of every workflow")
    args = parser.parse_args(argv)
    catalog = workflows()
    if args.workflow is None:
        for listed in catalog.values():
            _list(listed, sys.stdout, scenarios=args.all)
        if not args.all:
            print("\nstart here:")
            for name, scenario, why in START_HERE:
                print(f"  {PROG} {name} {scenario}\n      {why}")
            print(f"\nevery scenario: {PROG} --all")
        return 0
    wf = catalog.get(args.workflow)
    if wf is None:
        print(f"error: no workflow {args.workflow!r}. Workflows:", file=sys.stderr)
        print("\n".join(f"  {name}" for name in catalog), file=sys.stderr)
        return 2
    if args.scenario is None:
        _list(wf, sys.stdout, scenarios=True)
        return 0
    if args.scenario not in wf.scenarios:
        print(f"error: no scenario {args.scenario!r} of {wf.name}:", file=sys.stderr)
        _list(wf, sys.stderr, scenarios=True)
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
    broken = False
    for mode in ("shadow", "serve"):
        if mode not in results:
            continue
        problems = check_invariants(results[mode], results.get("bare"))
        print(f"\ninvariants ({mode}): {f'{len(problems)} broken' if problems else 'all hold'}")
        for problem in problems:
            print(f"  BROKEN {problem}")
        broken = broken or bool(problems)
    return 1 if broken else 0


def _list(wf: Workflow, out: TextIO, *, scenarios: bool) -> None:
    print(f"{wf.name}  {wf.summary}", file=out)
    if scenarios:
        for scenario, spec in wf.scenarios.items():
            print(f"    {scenario:<24} {spec.doc}", file=out)


if __name__ == "__main__":
    sys.exit(main())
