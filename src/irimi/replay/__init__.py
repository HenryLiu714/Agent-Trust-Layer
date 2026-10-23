"""Replaying a stored run: re-running its agent and answering it from what the run recorded.

`replay.index` is the recording a replay answers from and the replay's state, `replay.policy`
the `AnswerPolicy` that asks it, `replay.plan` what `irimi replay` runs, and `replay.report` what
it says once the agent has run. The submodules sit in different layers, so this package imports
none of them.
"""
