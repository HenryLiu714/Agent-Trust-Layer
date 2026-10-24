"""The import boundaries the package layout promises (`docs/architecture.md`, "Layers").

mitmproxy is imported only in `irimi.engine.mitm`, and a module imports only from below it.
"""

import ast
import re
from pathlib import Path
from typing import cast

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "irimi"
DOCS = ROOT / "docs" / "architecture.md"
MITM_ALLOWED = {SRC / "engine" / "mitm.py"}

# Each layer maps its packages to their modules, lowest first (None for a plain module). A module
# imports earlier layers and earlier modules of its package, a later one only under TYPE_CHECKING;
# a nested package (`services/stripe/`) is one entry.
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
    """The entry an `irimi.<package>[.<module>[...]]` name falls in, or None for `irimi` itself."""
    parts = dotted.split(".")
    if len(parts) < 2:
        return None
    if parts[1] in LAYER_OF and parts[1] not in ORDER:
        return (parts[1], None)
    return (parts[1], parts[2] if len(parts) > 2 else "__init__")


def _from_entries(module: str, names: list[str]) -> set[Entry]:
    """The entries `from <module> import <names>` imports, a submodule's name counting as itself."""
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


def _absolute(py: Path, node: ast.ImportFrom) -> str | None:
    """The module a `from` import in `py` names, a relative one resolved against `py`'s package."""
    if node.level == 0:
        return node.module
    package = list(py.relative_to(SRC.parent).parent.parts)
    base = package[: len(package) - node.level + 1]
    return ".".join([*base, *([node.module] if node.module else [])]) or None


def _irimi_imports(py: Path) -> set[tuple[Entry, bool]]:
    """Every irimi entry `py` imports at any depth, and whether only for type checking.

    Relative imports count, and so does each module reached through a package as an attribute.
    """
    tree = ast.parse(py.read_text())
    typing_only = _typing_only(tree)
    out: set[tuple[Entry, bool]] = set()
    bound: dict[str, str] = {}
    for node in ast.walk(tree):
        entries: set[Entry] = set()
        if isinstance(node, ast.ImportFrom) and (module := _absolute(py, node)):
            if module == "irimi" or module.startswith("irimi."):
                entries = _from_entries(module, [alias.name for alias in node.names])
                for alias in node.names:
                    bound[alias.asname or alias.name] = f"{module}.{alias.name}"
        elif isinstance(node, ast.Import):
            entries = {
                entry
                for alias in node.names
                if alias.name.startswith("irimi.") and (entry := _entry(alias.name)) is not None
            }
            for alias in node.names:
                if alias.name == "irimi" or alias.name.startswith("irimi."):
                    if alias.asname:
                        bound[alias.asname] = alias.name
                    else:
                        bound[alias.name.partition(".")[0]] = alias.name.partition(".")[0]
        out |= {(entry, node in typing_only) for entry in entries}
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and (entry := _reached(node, bound)) is not None:
            out.add((entry, node in typing_only))
    return out


def _reached(node: ast.Attribute, bound: dict[str, str]) -> Entry | None:
    """The module an attribute chain (`replay.policy.X`) reaches through a bound irimi name."""
    chain: list[str] = []
    current: ast.expr = node
    while isinstance(current, ast.Attribute):
        chain.insert(0, current.attr)
        current = current.value
    if not isinstance(current, ast.Name) or current.id not in bound:
        return None
    parts = [*bound[current.id].split("."), *chain]
    if len(parts) < 3 or parts[2] not in ORDER.get(parts[1], {}):
        return None
    return (parts[1], parts[2])


def _entry_of(py: Path) -> Entry | None:
    """The entry the file `py` is, or None for irimi's own `__init__`."""
    parts = py.relative_to(SRC).with_suffix("").parts
    if parts == ("__init__",):
        return None
    if len(parts) == 1:
        return (parts[0], None)
    return (parts[0], parts[1])


def _mitmproxy_imports(py: Path) -> list[str]:
    """Every import of mitmproxy in `py`, including `import_module` and `__import__` calls."""
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


def test_a_relative_import_is_held_to_the_layers_as_an_absolute_one():
    def imported(where: Path, source: str) -> str | None:
        node = ast.parse(source).body[0]
        assert isinstance(node, ast.ImportFrom)
        return _absolute(where, node)

    shadow = SRC / "cli" / "shadow.py"
    assert imported(shadow, "from . import compose") == "irimi.cli"
    assert imported(shadow, "from .runner import Child") == "irimi.cli.runner"
    assert imported(shadow, "from ..engine import Engine") == "irimi.engine"
    assert imported(SRC / "cli" / "__init__.py", "from .compose import Run") == "irimi.cli.compose"
    assert imported(SRC / "record" / "store" / "reader.py", "from ... import sdk") == "irimi"
    assert _from_entries("irimi.cli", ["compose"]) == {("cli", "compose")}


def test_a_module_reached_through_its_package_counts_as_imported(tmp_path):
    """`replay.policy` reached through `from irimi import replay` counts as importing it."""
    source = tmp_path / "x.py"
    for code, reached in (
        ("from irimi import replay\nreplay.policy.ReplayPolicy", ("replay", "policy")),
        ("import irimi.replay\nirimi.replay.plan.plan", ("replay", "plan")),
        ("import irimi.replay as r\nr.report", ("replay", "report")),
        ("from irimi import services\nservices.check", None),
    ):
        source.write_text(code)
        found = {entry for entry, _ in _irimi_imports(source)}
        assert reached is None or reached in found, code
        assert ("services", "check") not in found


TESTS = ROOT / "tests"


def _test_module_imports(py: Path) -> list[str]:
    """Every `tests...test_*` module `py` imports."""
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
    """No test module imports another; shared helpers live in `tests/support/`, never collected."""
    offenders = [
        f"{py.relative_to(ROOT)} imports {name}"
        for py in sorted(TESTS.rglob("*.py"))
        for name in _test_module_imports(py)
    ]
    assert offenders == [], "move the shared helper into tests/support/:\n" + "\n".join(offenders)
