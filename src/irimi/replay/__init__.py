"""Replaying a stored run: re-running its agent and answering it from what the run recorded.

`replay.child` is the process `irimi replay` starts to call a trigger again, `replay.index` the
recording a replay answers from and the replay's state, `replay.policy` the `AnswerPolicy` that
asks it, `replay.compare` two stored runs side by side, write by write, `replay.plan` what
`irimi replay` runs, and `replay.report` what it says once the agent has run.
"""
