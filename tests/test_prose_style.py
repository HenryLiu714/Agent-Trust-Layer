"""Hold comments, docstrings and shipped map lines to CONTRIBUTING's style guide, as a ratchet.

Per-file counts in `tests/prose_baseline/<tree>.json` may only fall; a moved file keeps its entry.
"""

import ast
import io
import json
import re
import tokenize
from collections import Counter
from functools import cache
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BASELINES = Path(__file__).resolve().parent / "prose_baseline"

PATTERNS: dict[str, re.Pattern[str]] = {
    "phase": re.compile(r"Phase \d"),
    "design_ref": re.compile(r"design (doc )?(§|D\d)"),
    "section_sign": re.compile(r"§\d"),
    "decision_code": re.compile(r"\bD\d{1,2}\b"),
    "planning_id": re.compile(r"\bP\d-\d+\b"),
    "claude_md": re.compile(r"CLAUDE\.md"),
    "notion": re.compile(r"\b[Nn]otion\b"),
    "since_issue": re.compile(r"[Ss]ince #\d"),
    "issue_possessive": re.compile(r"#\d+'s"),
    "use_case": re.compile(r"\b[Uu]se case \d"),
    "trustme": re.compile(r"~/trustme"),
    "caps_run": re.compile(r"\b[A-Z]{2,}(?:'[A-Z]+)?\b(?:[ \t]+\b[A-Z]{2,}(?:'[A-Z]+)?\b){2,}"),
    "issue_ref": re.compile(r"(?<![&\w])#\d+"),
}


def _tree_files(tree: str) -> list[Path]:
    """The files a tree's baseline covers: its Python, plus the shipped maps for `src`."""
    files = sorted((ROOT / tree).rglob("*.py"))
    if tree == "src":
        files += sorted((ROOT / "src" / "irimi" / "maps").glob("*.yaml"))
    return files


def _python_prose(path: Path) -> list[tuple[int, str]]:
    """Every comment and docstring in `path`, each with the line it starts on."""
    source = path.read_text(encoding="utf-8")
    out: list[tuple[int, str]] = []
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.COMMENT:
            out.append((tok.start[0], tok.string.lstrip("#")))
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            doc = ast.get_docstring(node, clean=False)
            if doc is not None and node.body:
                out.append((node.body[0].lineno, doc))
    return out


def _yaml_prose(path: Path) -> list[tuple[int, str]]:
    return list(enumerate(path.read_text(encoding="utf-8").splitlines(), start=1))


@cache
def _scan(tree: str) -> dict[str, dict[str, list[tuple[int, str]]]]:
    """For each file in `tree` with any match: pattern name -> (line, matched text) per match."""
    found: dict[str, dict[str, list[tuple[int, str]]]] = {}
    for path in _tree_files(tree):
        prose = _yaml_prose(path) if path.suffix == ".yaml" else _python_prose(path)
        hits: dict[str, list[tuple[int, str]]] = {}
        for line, text in prose:
            for name, pattern in PATTERNS.items():
                for m in pattern.finditer(text):
                    hits.setdefault(name, []).append((line + text.count("\n", 0, m.start()), m[0]))
        if hits:
            found[path.relative_to(ROOT).as_posix()] = hits
    return found


def _counts(tree: str) -> dict[str, Counter[str]]:
    return {
        path: Counter({name: len(ms) for name, ms in hits.items()})
        for path, hits in _scan(tree).items()
    }


def _baseline(tree: str) -> dict[str, dict[str, int]]:
    data: dict[str, dict[str, int]] = json.loads((BASELINES / f"{tree}.json").read_text())
    return data


TREES = ["src", "tests", "examples"]


@pytest.mark.parametrize("tree", TREES)
def test_no_file_has_more_style_matches_than_its_baseline(tree):
    baseline = _baseline(tree)
    over = []
    for path, counts in sorted(_counts(tree).items()):
        allowed = baseline.get(path, {})
        for name, n in sorted(counts.items()):
            if n > allowed.get(name, 0):
                lines = ", ".join(f"{line}: {text!r}" for line, text in _scan(tree)[path][name])
                over.append(f"{path}: {name} {n} > baseline {allowed.get(name, 0)} ({lines})")
    assert over == [], (
        "comments or docstrings break CONTRIBUTING's style guide ('Writing comments, docstrings"
        " and messages'). Rewrite the new text; do not raise tests/prose_baseline/"
        f"{tree}.json:\n" + "\n".join(over)
    )


@pytest.mark.parametrize("tree", TREES)
def test_the_baseline_is_no_higher_than_the_tree(tree):
    baseline = _baseline(tree)
    current = _counts(tree)
    lowered: dict[str, dict[str, int] | None] = {}
    for path, allowed in sorted(baseline.items()):
        have = current.get(path, Counter())
        if any(n > have.get(name, 0) or n <= 0 for name, n in allowed.items()) or not allowed:
            kept = {
                name: min(n, have[name])
                for name, n in sorted(allowed.items())
                if n > 0 and have.get(name, 0) > 0
            }
            lowered[path] = kept or None
    assert lowered == {}, (
        f"tests/prose_baseline/{tree}.json is higher than the tree; lower it to these counts"
        " (null means delete the entry), or move the entry if the file moved:\n"
        + json.dumps(lowered, indent=2, ensure_ascii=False)
    )
