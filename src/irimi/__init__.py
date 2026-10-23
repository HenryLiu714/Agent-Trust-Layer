"""irimi: shadow mode for AI agents, where reads reach the real services and writes are faked.

Agent code imports `irimi.sdk`; tests that replay a stored run use `irimi.testing`; the proxy
itself is the `irimi` command.
"""

from importlib.metadata import version

__version__ = version("irimi")
