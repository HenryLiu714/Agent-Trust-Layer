"""The variables `irimi shadow` and `irimi replay` set in their child's environment for the SDK.

`runner.child_env` sets them; its docstring lists what each holds.
"""

# A proxy is in front of the process: the SDK is active.
ENGINE_ACTIVE_ENV = "IRIMI_ENGINE_ACTIVE"
# The process run's id.
RUN_ENV = "IRIMI_RUN"
# The control endpoint's base URL, `http://<host>:<port>/_irimi`, where the SDK reports what the
# wire cannot show.
CONTROL_ENV = "IRIMI_CONTROL"
# Set only for the child of an engine that replays a stored run, to the run the replay is: the
# engine's own, `IRIMI_RUN`, never the run replayed. The SDK reads it as "this is a replay"
# (`context.replay_run_id`). irimi removes it from a child it does not replay for.
REPLAY_RUN_ENV = "IRIMI_REPLAY_RUN"
# What `irimi replay` tells the child it starts to call a trigger: the function's
# `module:qualname`, a JSON file holding its captured args, and the file the child writes how the
# call ended to. irimi removes all three from every other child.
REPLAY_CALL_ENV = "IRIMI_REPLAY_CALL"
REPLAY_ARGS_FILE_ENV = "IRIMI_REPLAY_ARGS_FILE"
REPLAY_OUTCOME_FILE_ENV = "IRIMI_REPLAY_OUTCOME_FILE"
# The proxy variables `irimi shadow` points at its own listener, in both spellings, since clients
# disagree on which they read. The SDK reads the same names to tell a connection to irimi from one
# that bypasses it.
PROXY_ENVS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy")
# The agent's own version, if its deployment names one. `irimi shadow` records it on the process
# run and the SDK on each run it starts.
AGENT_VERSION_ENV = "IRIMI_AGENT_VERSION"
