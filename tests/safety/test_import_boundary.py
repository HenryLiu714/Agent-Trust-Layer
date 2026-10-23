"""The import boundaries the package layout promises (docs/architecture.md, "Layers").

Both rules are mechanical, so a refactor cannot quietly undo them:

  1. mitmproxy is imported in exactly one module, `irimi.engine.mitm`. Everything else talks to
     the `Engine` protocol, which is what lets the decision, the store and the reports be tested
     without a proxy and lets the engine be swapped.
  2. A module imports only from below it. `LAYERS` lists irimi's packages lowest first, a layer
     holding one package or several side by side, and each package its modules lowest first. A
     module may import any module of an earlier layer, and a module listed before it in its own
     package; never a later one, nor another package of its own layer. A package nested in
     another (`services/stripe/`) is one entry of its parent, its modules importing each other
     freely. An import made only for type checking may name a later module of its own package,
     since it runs nothing. `cli` may import everything else, which is what makes it the
     composition root; only `__main__`, `python -m irimi`, and the public `irimi.testing` sit
     above it.

docs/architecture.md's layer table places the same packages in the same layers, and a test below
holds it to `LAYERS`, so the two cannot drift.
"""

import ast
import re
from pathlib import Path
from typing import cast

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "irimi"
DOCS = ROOT / "docs" / "architecture.md"
MITM_ALLOWED = {SRC / "engine" / "mitm.py"}

# Each layer maps its packages to their modules, lowest first; a top-level module that is not a
# package has no module list (None).
LAYERS: list[dict[str, list[str] | None]] = [
    # vocabulary
    {
        "core": [
            "__init__",
            "exchange",
            "netaddr",
            "secret_shapes",
            "money",
            "limits",
            "opener",
            "exitcodes",
            "calls",
            "bodies",
        ]
    },
    # contract
    {"protocol": ["__init__", "headers", "env", "trace", "entrypoints", "control", "replay_files"]},
    # machine and agent
    {
        "config": ["__init__", "paths", "files", "ca"],
        "sdk": [
            "_context",
            "_errors",
            "_capture",
            "_http_labels",
            "_control_client",
            "_state",
            "_runs",
            "_tools",
            "__init__",
        ],
    },
    # maps
    {"servicemaps": ["model", "rules", "loader", "__init__", "classify"]},
    # services
    {"services": ["model", "generic", "stripe", "slack", "__init__"]},
    # disk
    {"record": ["__init__", "redact", "store"]},
    # decision
    {
        "decide": [
            "__init__",
            "targets",
            "reader",
            "writelog",
            "idempotency",
            "preconditions",
            "overlay",
            "policy",
        ]
    },
    # proxy
    {
        "engine": [
            "__init__",
            "wire",
            "ingress",
            "decoding",
            "capture",
            "control",
            "session",
            "mitm",
        ]
    },
    # output
    {"report": ["__init__", "terminal", "model", "summary", "runs", "maps"]},
    # replay
    {"replay": ["__init__", "child", "index", "policy", "compare", "plan", "report"]},
    # commands
    {
        "cli": [
            "__init__",
            "options",
            "runner",
            "compose",
            "init",
            "maps",
            "runs",
            "shadow",
            "serve",
            "replay_cmd",
        ]
    },
    # entry points
    {"__main__": None, "testing": None},
]
LAYER_OF = {package: depth for depth, layer in enumerate(LAYERS) for package in layer}
ORDER = {
    package: {module: i for i, module in enumerate(modules)}
    for layer in LAYERS
    for package, modules in layer.items()
    if modules is not None
}

# (package, module); the module is None for a top-level module that is not a package.
Entry = tuple[str, str | None]


def _entry(dotted: str) -> Entry | None:
    """The entry an `irimi.<package>[.<module>[...]]` name falls in, or None for `irimi` itself
    (`__version__`), which imports nothing from irimi."""
    parts = dotted.split(".")
    if len(parts) < 2:
        return None
    if parts[1] in LAYER_OF and parts[1] not in ORDER:
        return (parts[1], None)
    return (parts[1], parts[2] if len(parts) > 2 else "__init__")


def _from_entries(module: str, names: list[str]) -> set[Entry]:
    """What `from <module> import <names>` imports: a name that is a module of the package
    `module` names is that module, and any other name comes from `module` itself."""
    here = _entry(module)
    out: set[Entry] = set()
    for name in names:
        named = _entry(f"{module}.{name}")
        if here is None:
            if named is not None and named[0] in LAYER_OF:
                out.add(named)
        elif here[1] == "__init__" and named is not None and named[1] in ORDER.get(here[0], {}):
            out.add(named)
        else:
            out.add(here)
    return out


def _typing_only(tree: ast.AST) -> set[ast.AST]:
    """The nodes under an `if TYPE_CHECKING:` block."""
    nodes: set[ast.AST] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and getattr(node.test, "id", None) == "TYPE_CHECKING":
            for child in node.body:
                nodes |= set(ast.walk(child))
    return nodes


def _irimi_imports(py: Path) -> set[tuple[Entry, bool]]:
    """Every irimi entry `py` imports, at any depth, deferred imports included, each with whether
    it is imported only for type checking."""
    tree = ast.parse(py.read_text())
    typing_only = _typing_only(tree)
    out: set[tuple[Entry, bool]] = set()
    for node in ast.walk(tree):
        entries: set[Entry] = set()
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            if node.module == "irimi" or node.module.startswith("irimi."):
                entries = _from_entries(node.module, [alias.name for alias in node.names])
        elif isinstance(node, ast.Import):
            entries = {
                entry
                for alias in node.names
                if alias.name.startswith("irimi.") and (entry := _entry(alias.name)) is not None
            }
        out |= {(entry, node in typing_only) for entry in entries}
    return out


def _entry_of(py: Path) -> Entry | None:
    """The entry the file `py` is, or None for irimi's own `__init__`."""
    parts = py.relative_to(SRC).with_suffix("").parts
    if parts == ("__init__",):
        return None
    if len(parts) == 1:
        return (parts[0], None)
    return (parts[0], parts[1])


def _mitmproxy_imports(py: Path) -> list[str]:
    """Every import of mitmproxy in `py`: an `import` or `from` of it, or an `import_module` or
    `__import__` call with its name as a literal."""
    found = []
    for node in ast.walk(ast.parse(py.read_text())):
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names = [node.module]
        elif isinstance(node, ast.Call) and node.args and isinstance(node.args[0], ast.Constant):
            called = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
            if called in {"import_module", "__import__"} and isinstance(node.args[0].value, str):
                names = [node.args[0].value]
        # Only an import or a call sets `names`, and each has a line.
        line = cast(ast.stmt | ast.expr, node)
        found += [f"line {line.lineno}: {n}" for n in names if n.split(".")[0] == "mitmproxy"]
    return found


def test_mitmproxy_is_imported_only_in_engine_mitm():
    offenders = [
        f"{py.relative_to(SRC)} {found}"
        for py in sorted(SRC.rglob("*.py"))
        if py not in MITM_ALLOWED
        for found in _mitmproxy_imports(py)
    ]
    assert offenders == [], "mitmproxy may only be imported in irimi/engine/mitm.py:\n" + "\n".join(
        offenders
    )


def test_every_module_is_assigned_a_layer():
    found = {entry for py in SRC.rglob("*.py") if (entry := _entry_of(py)) is not None}
    placed: set[Entry] = {
        (package, module)
        for layer in LAYERS
        for package, modules in layer.items()
        for module in (modules if modules is not None else [None])
    }
    assert found == placed, (
        "place each new module in LAYERS in this test and in docs/architecture.md's layer table"
    )


def test_no_module_imports_a_later_module_or_layer():
    offenders = []
    for py in sorted(SRC.rglob("*.py")):
        me = _entry_of(py)
        if me is None:
            continue
        package, module = me
        for (dep_package, dep_module), typing_only in sorted(_irimi_imports(py), key=str):
            if dep_package == package:
                if dep_module == module or typing_only:
                    continue
                order = ORDER[package]
                if module is not None and order[str(dep_module)] < order[module]:
                    continue
            elif LAYER_OF[dep_package] < LAYER_OF[package]:
                continue
            name = f"irimi.{dep_package}" + (f".{dep_module}" if dep_module else "")
            offenders.append(f"{py.relative_to(SRC)} imports {name}")
    assert offenders == [], "\n".join(offenders)


def _docs_layers() -> dict[int, set[str]]:
    """The packages docs/architecture.md's layer table places in each layer."""
    section = DOCS.read_text().split("\n## Layers\n", 1)[1].split("\n## ", 1)[0]
    layers: dict[int, set[str]] = {}
    for line in section.splitlines():
        row = re.match(r"\|\s*(\d+)\s*\|([^|]*)\|", line)
        if row:
            layers.setdefault(int(row[1]), set()).update(re.findall(r"`(\w+)/?`", row[2]))
    return layers


def test_the_architecture_docs_place_the_same_packages_in_the_same_layers():
    expected = {depth: set(layer) for depth, layer in enumerate(LAYERS)}
    assert _docs_layers() == expected, (
        "docs/architecture.md's layer table must place the packages as LAYERS in this test does"
    )


TESTS = ROOT / "tests"


def _test_module_imports(py: Path) -> list[str]:
    """Every test module `py` imports: a `tests...test_*` module, or `test_*` from a `tests`
    package."""
    found = []
    for node in ast.walk(ast.parse(py.read_text(), filename=str(py))):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            names = [node.module, *(f"{node.module}.{alias.name}" for alias in node.names)]
        else:
            continue
        found += [
            name
            for name in names
            if name.split(".")[0] == "tests" and name.split(".")[-1].startswith("test_")
        ]
    return found


def test_no_test_module_imports_another():
    """A helper two test modules share lives in `tests/support/`, which is not collected, so
    splitting or renaming one test file never breaks another."""
    offenders = [
        f"{py.relative_to(ROOT)} imports {name}"
        for py in sorted(TESTS.rglob("*.py"))
        for name in _test_module_imports(py)
    ]
    assert offenders == [], "move the shared helper into tests/support/:\n" + "\n".join(offenders)
