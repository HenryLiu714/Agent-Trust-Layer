"""The SDK as an agent's process meets it, in a fresh interpreter: what importing it loads, at
any depth, and what it says of a trigger no replay can import.

`tests/test_sdk_surface.py` checks the modules the SDK names; this checks what they pull in. An
agent's process gets the stdlib and irimi, and nothing it did not install itself.
"""

import json
import os
import subprocess
import sys

import pytest

import irimi.sdk
import irimi.testing
from irimi.core import limits, secret_shapes
from irimi.protocol.env import ENGINE_ACTIVE_ENV
from irimi.protocol.trace import ErrorInfo
from irimi.sdk import _runs

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


_PACKAGE_MAIN = """\
from irimi import sdk
from irimi.protocol import entrypoints

@sdk.trigger
def handle() -> None: ...

print(entrypoints.entrypoint(handle.__wrapped__))
"""


def test_a_trigger_in_a_package_s_main_is_warned_of_without_sending_the_user_to_python_m(
    tmp_path,
):
    """`python -m pkg` runs `pkg/__main__.py`, which a replay must not import again (it would run
    the agent's whole main), so its trigger records `__main__` as a script's does. The warning
    says what is wrong without telling a user who typed `python -m` to type it."""
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "__main__.py").write_text(_PACKAGE_MAIN)
    env = {k: v for k, v in os.environ.items() if not k.startswith("IRIMI_")}
    env[ENGINE_ACTIVE_ENV] = "1"
    done = subprocess.run(
        [sys.executable, "-m", "pkg"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout == "__main__:handle\n"
    assert done.stderr == (
        "irimi: trigger handle is defined in __main__, which a replay cannot import, so its runs "
        "cannot be replayed by entrypoint; define it in a module and start the agent with "
        "python -m <module>\n"
    )


def test_a_name_that_is_one_long_key_is_posted_marked_not_empty():
    """The SDK cuts before irimi redacts, with no key to make a placeholder: a secret the cut
    crosses becomes `secret_shapes.CUT_MARK`, so a name or a message that is one long key still
    says something was there, and nothing of the key is posted."""
    key = "sk_live_" + "k" * (2 * limits.MAX_POSTED_MESSAGE)
    assert _runs.label(_named, key, "@sdk.read_tool") == secret_shapes.CUT_MARK
    assert ErrorInfo.from_exception(ValueError(key)).message == secret_shapes.CUT_MARK
    for limit in (len(secret_shapes.CUT_MARK), 50):
        cut = secret_shapes.cut_marking_secrets("ok " + key, limit)
        assert len(cut) <= limit and "kkk" not in cut


def _named() -> None: ...


@pytest.mark.parametrize("limit", range(len(secret_shapes.CUT_MARK) + 2))
def test_a_cut_too_short_for_its_mark_stays_within_its_limit(limit):
    """A limit with no room for the mark drops the key unmarked: the cut never runs past it."""
    key = "sk_live_" + "k" * 40
    for text in (key, "a " + key, "abcdefghijkl " + key):
        cut = secret_shapes.cut_marking_secrets(text, limit)
        assert len(cut) <= limit and "sk_live" not in cut, (text, cut)
        assert cut == text[: len(cut)] or cut.endswith(secret_shapes.CUT_MARK)
