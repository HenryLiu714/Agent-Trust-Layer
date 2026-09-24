"""The import boundaries the module layout promises (docs/architecture.md, "Layers").

Both rules are mechanical, so a refactor cannot quietly undo them:

  1. mitmproxy is imported in exactly one module, `irimi.engine.mitm`. Everything else talks to
     the `Engine` protocol, which is what lets the pipeline, the policy and the reports be tested
     without a proxy and lets the engine be swapped.
  2. A module imports only from layers below its own. The layers are listed lowest first; a
     module in one may import any module in an earlier layer and none in its own or a later one.
     `cli` is the top and may import everything, which is what makes it the composition root.
"""

import ast
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src" / "irimi"
MITM_ALLOWED = {SRC / "engine" / "mitm.py"}

LAYERS: list[set[str]] = [
    {"exchange", "paths", "netaddr"},
    {"ca", "servicemap", "fixture"},
    {"pipeline", "reverse_door"},
    {"delegation", "echo", "services"},
    {"policy", "overlay", "store"},
    {"engine"},
    {"report", "runner"},
    {"cli"},
]
LAYER_OF = {name: depth for depth, layer in enumerate(LAYERS) for name in layer}


def _top_level(module: str) -> str:
    """`irimi.servicemap.loader` -> `servicemap`; `irimi` alone -> `irimi`."""
    parts = module.split(".")
    return parts[1] if len(parts) > 1 and parts[0] == "irimi" else parts[0]


def _irimi_imports(py: Path) -> set[str]:
    """Every top-level irimi module `py` imports, at any depth, deferred imports included."""
    out: set[str] = set()
    for node in ast.walk(ast.parse(py.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("irimi"):
            if node.module == "irimi":
                out |= {alias.name for alias in node.names if alias.name in LAYER_OF}
            else:
                out.add(_top_level(node.module))
        elif isinstance(node, ast.Import):
            out |= {_top_level(a.name) for a in node.names if a.name.startswith("irimi.")}
    return out


def _module_of(py: Path) -> str:
    return py.relative_to(SRC).parts[0].removesuffix(".py")


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
    modules = {_module_of(py) for py in SRC.rglob("*.py")} - {"__init__"}
    assert modules == set(LAYER_OF), "add the new module to LAYERS in this test and to the docs"


def test_no_module_imports_its_own_layer_or_a_higher_one():
    offenders = []
    for py in sorted(SRC.rglob("*.py")):
        me = _module_of(py)
        if me == "__init__":
            continue
        for dep in sorted(_irimi_imports(py)):
            if dep != me and LAYER_OF[dep] >= LAYER_OF[me]:
                offenders.append(f"{py.relative_to(SRC)} imports irimi.{dep}")
    assert offenders == [], "\n".join(offenders)
