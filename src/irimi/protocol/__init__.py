"""The contract between irimi's engine and the SDK in an agent's process: the header names, the
environment variables, the control endpoint's routes and bodies, the files a replay's child and
`irimi.testing.replay` read, the trace format (`trace`) and how a function is named by its
`module:qualname` (`entrypoints`). The SDK and the engine both import this package and never each
other.
"""
