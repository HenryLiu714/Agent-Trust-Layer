"""The variables `irimi shadow` and `irimi replay` set in their child's environment for the SDK.

`runner.child_env` sets them; its docstring lists what each holds.
"""

# Set when a proxy is in front of the process, so the SDK is active.
ENGINE_ACTIVE_ENV = "IRIMI_ENGINE_ACTIVE"
# The process run's id.
RUN_ENV = "IRIMI_RUN"
# The control endpoint's base URL, `http://<host>:<port>/_irimi`.
CONTROL_ENV = "IRIMI_CONTROL"
# Set only in a replay's child, to the replay's own run id (not the run replayed).
REPLAY_RUN_ENV = "IRIMI_REPLAY_RUN"
# The trigger `irimi replay` asks its child to call, its args file, and the outcome file.
REPLAY_CALL_ENV = "IRIMI_REPLAY_CALL"
REPLAY_ARGS_FILE_ENV = "IRIMI_REPLAY_ARGS_FILE"
REPLAY_OUTCOME_FILE_ENV = "IRIMI_REPLAY_OUTCOME_FILE"
# Both spellings of the proxy variables, since clients disagree on which they read.
PROXY_ENVS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy")
# The agent's own version, recorded on each run.
AGENT_VERSION_ENV = "IRIMI_AGENT_VERSION"
