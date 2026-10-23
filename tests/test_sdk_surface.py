"""The SDK's public surface is exactly its `__all__`, and it imports only what an agent can carry.

An agent imports `irimi.sdk` in its own process, so every implementation module is private and
the SDK reaches only the stdlib and irimi's low, dependency-free modules.
"""

import ast
import sys
from pathlib import Path

import irimi.sdk

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
