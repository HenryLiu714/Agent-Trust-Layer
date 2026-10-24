"""The SDK's public surface is exactly its `__all__`, and it imports only what an agent can carry.

Every implementation module is private, and the SDK reaches only the stdlib and low modules."""

import ast
import json
import subprocess
import sys
from pathlib import Path

import pytest

import irimi.sdk
import irimi.testing

SDK = Path(irimi.sdk.__file__).parent

# The irimi modules and packages the SDK may import, its own included. Each is stdlib-only, never
# mitmproxy. `irimi` itself is allowed too, for the package's `__version__`.
ALLOWED = (
    "irimi.sdk",
    "irimi.core.calls",
    "irimi.core.limits",
    "irimi.core.opener",
    "irimi.core.secret_shapes",
    "irimi.protocol",
)


def test_the_public_names_are_exactly_all():
    public = {name for name in dir(irimi.sdk) if not name.startswith("_")}
    assert public == set(irimi.sdk.__all__)


def test_every_sdk_implementation_module_is_private():
    assert [py.name for py in SDK.glob("*.py") if not py.name.startswith("_")] == []


def _imports(py: Path) -> set[str]:
    out: set[str] = set()
    for node in ast.walk(ast.parse(py.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module)
        elif isinstance(node, ast.Import):
            out |= {alias.name for alias in node.names}
    return out


def _allowed(module: str) -> bool:
    if module == "irimi" or module.split(".")[0] in sys.stdlib_module_names:
        return True
    return any(module == name or module.startswith(f"{name}.") for name in ALLOWED)


def test_the_sdk_imports_only_the_stdlib_and_irimis_low_layers():
    offenders = [
        f"{py.relative_to(SDK.parent)} imports {module}"
        for py in sorted(SDK.glob("*.py"))
        for module in sorted(_imports(py))
        if not _allowed(module)
    ]
    assert offenders == [], "\n".join(offenders)


# ------------------------------------------------------ what importing it loads, at any depth


_LOADED = """
import json, sys
before = set(sys.modules)
import {module}
print(json.dumps(sorted(set(sys.modules) - before)))
"""


def _loaded_by(module: str) -> list[str]:
    """The modules `import <module>` adds to a fresh interpreter."""
    out = subprocess.run(
        [sys.executable, "-c", _LOADED.format(module=module)],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    loaded: list[str] = json.loads(out)
    return loaded


@pytest.mark.parametrize("module", ["irimi.sdk", "irimi.testing"])
def test_importing_it_loads_only_the_stdlib_and_irimi(module):
    foreign = [
        name
        for name in _loaded_by(module)
        if name.split(".")[0] not in sys.stdlib_module_names and name.split(".")[0] != "irimi"
    ]
    assert foreign == []


def test_importing_the_sdk_loads_no_proxy_side_of_irimi():
    """The proxy's packages need mitmproxy, yaml or cryptography; the SDK runs without them."""
    proxy_side = ("cli", "config", "decide", "engine", "record", "replay", "report", "servicemaps")
    loaded = [name for name in _loaded_by("irimi.sdk") if name.startswith("irimi.")]
    assert [name for name in loaded if name.split(".")[1] in proxy_side] == []


@pytest.mark.parametrize("module", [irimi.sdk, irimi.testing])
def test_every_name_all_promises_is_there(module):
    assert [name for name in module.__all__ if not hasattr(module, name)] == []
    namespace: dict[str, object] = {}
    exec(f"from {module.__name__} import *", namespace)
    assert {name for name in namespace if name != "__builtins__"} == set(module.__all__)
