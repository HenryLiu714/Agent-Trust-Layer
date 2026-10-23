"""The import boundaries the module layout promises (docs/architecture.md, "Layers").

Both rules are mechanical, so a refactor cannot quietly undo them:

  1. mitmproxy is imported in exactly one module, `irimi.engine.mitm`. Everything else talks to
     the `Engine` protocol, which is what lets the pipeline, the policy and the reports be tested
     without a proxy and lets the engine be swapped.
  2. A module imports only from layers below its own. The layers are listed lowest first; a
     module in one may import any module in an earlier layer and none in its own or a later one.
     `cli` may import everything else, which is what makes it the composition root; only
     `__main__`, `python -m irimi`, is above it.

A package is one layer, its modules importing each other freely, unless its submodules are
placed one by one with dotted entries (`replay.index`): each is then a layer entry of its own,
held to the rule like any module, and the package's `__init__` may import nothing from irimi.
"""

import ast
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src" / "irimi"
MITM_ALLOWED = {SRC / "engine" / "mitm.py"}

LAYERS: list[set[str]] = [
    {
        "exchange",
        "paths",
        "netaddr",
        "secret_shapes",
        "money",
        "limits",
        "opener",
        "files",
        "exitcodes",
    },
    {"ca", "servicemap", "bodies", "trace"},
    {"protocol"},
    {"classify", "wire", "redact", "sdk"},
    {"delegation", "services", "ingress", "reader"},
    {"writelog", "idempotency", "replay.index"},
    {"preconditions", "overlay", "store"},
    {"policy"},
    {"control", "session", "replay.policy"},
    {"engine"},
    {"report", "runner"},
    {"compare"},
    {"replay.plan", "replay.report"},
    {"cli"},
    {"__main__"},
]
LAYER_OF = {name: depth for depth, layer in enumerate(LAYERS) for name in layer}
# The packages whose submodules are placed one by one.
SPLIT = {name.partition(".")[0] for name in LAYER_OF if "." in name}


def _placed(module: str, names: list[str] | None = None) -> set[str]:
    """The layer entries an import of `module` names: `irimi.servicemap.loader` -> `servicemap`,
    `irimi.replay.index` -> `replay.index`, and `from irimi.replay import plan` (`names`) ->
    `replay.plan`. `irimi` alone, or a split package's `__init__`, is its own name."""
    parts = module.split(".")
    if len(parts) < 2 or parts[0] != "irimi":
        return {parts[0]}
    top = parts[1]
    if top not in SPLIT:
        return {top}
    if len(parts) > 2:
        return {f"{top}.{parts[2]}"}
    if names is not None:
        return {f"{top}.{n}" if f"{top}.{n}" in LAYER_OF else top for n in names}
    return {top}


def _irimi_imports(py: Path) -> set[str]:
    """Every irimi layer entry `py` imports, at any depth, deferred imports included."""
    out: set[str] = set()
    for node in ast.walk(ast.parse(py.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("irimi"):
            if node.module == "irimi":
                out |= {a.name for a in node.names if a.name in LAYER_OF or a.name in SPLIT}
            else:
                out |= _placed(node.module, [alias.name for alias in node.names])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("irimi."):
                    out |= _placed(alias.name)
    return out


def _module_of(py: Path) -> str:
    parts = py.relative_to(SRC).parts
    top = parts[0].removesuffix(".py")
    if top in SPLIT and len(parts) > 1 and parts[1] != "__init__.py":
        return f"{top}.{parts[1].removesuffix('.py')}"
    return top


def test_mitmproxy_is_imported_only_in_engine_mitm():
    offenders = []
    for py in SRC.rglob("*.py"):
        if py in MITM_ALLOWED:
            continue
        for line in py.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith(("import mitmproxy", "from mitmproxy")):
                offenders.append(f"{py.relative_to(SRC)}: {stripped}")
    assert offenders == [], "mitmproxy may only be imported in irimi/engine/mitm.py:\n" + "\n".join(
        offenders
    )


def test_every_module_is_assigned_a_layer():
    modules = {_module_of(py) for py in SRC.rglob("*.py")} - {"__init__"} - SPLIT
    assert modules == set(LAYER_OF), "add the new module to LAYERS in this test and to the docs"


def test_a_split_packages_init_imports_nothing_from_irimi():
    offenders = [
        f"{package}/__init__.py imports irimi.{dep}"
        for package in sorted(SPLIT)
        for dep in sorted(_irimi_imports(SRC / package / "__init__.py"))
    ]
    assert offenders == [], "\n".join(offenders)


def test_no_module_imports_its_own_layer_or_a_higher_one():
    offenders = []
    for py in sorted(SRC.rglob("*.py")):
        me = _module_of(py)
        if me == "__init__" or me in SPLIT:
            continue
        for dep in sorted(_irimi_imports(py)):
            # A split package's `__init__` imports nothing, so any module may import it.
            if dep != me and dep not in SPLIT and LAYER_OF[dep] >= LAYER_OF[me]:
                offenders.append(f"{py.relative_to(SRC)} imports irimi.{dep}")
    assert offenders == [], "\n".join(offenders)
