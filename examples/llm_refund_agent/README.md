# An LLM agent recorded, its prompt edited, and replayed

Phase 3's exit, for use case 1: "a developer records a run of an LLM agent, edits its prompt,
replays the run, and sees where it diverged and which writes changed, with no write reaching a
real service and no substitute-labeled tool running for real" (#85).

`agent.py` is a small refund agent. `handle_ticket(ticket)` is the run (`@sdk.trigger`), and the
`Ticket` it takes is a dataclass `(ticket_id, charge, message)`:

1. it looks the customer up in its own SQLite through `lookup_customer`, an
   `@sdk.tool(kind="read")`;
2. it reads the charge from Stripe;
3. it asks Anthropic what to refund, with the system prompt read from `PROMPT_FILE` (`prompt.txt`
   by default), for `{"refund_amount": int, "reply": str}`;
4. it refunds that amount, when it is above zero;
5. it records the decision in its SQLite through `record_decision`, an `@sdk.tool(kind="write")`
   whose stand-in runs under irimi instead.

`prompt_half.txt` tells the model to refund half. Both calls go out with stdlib `urllib.request`,
which takes `HTTPS_PROXY` and `SSL_CERT_FILE` from the environment, so the agent needs no code to
go through irimi. Start it by module name, `python -m examples.llm_refund_agent`, so its trigger's
entrypoint is importable. It refuses a Stripe key that is not `sk_test_`.

## Two halves

- **Hermetic, in CI.** `tests/test_phase3_replay_exit.py` records the agent under the real
  `irimi shadow` against a loopback Stripe and a loopback Anthropic that decides from the system
  prompt alone. It replays the run unchanged with the agent's database deleted, and again with
  `PROMPT_FILE=prompt_half.txt`, through `irimi replay` and through `sdk.replay()`. It checks that
  the unchanged replay reaches neither service and that the edited one asks only the model again.
- **Live, by hand.** The steps below, against Stripe test mode and the real Anthropic API. It is
  not in CI.

## Running the live half

You need a Stripe test-mode key, an Anthropic API key and `uv`, with irimi's CA made
(`scripts/setup.sh` runs `irimi init`) and the `examples` group installed for the charge's seed
script (`uv sync --group examples`). From the repository root:

```
export STRIPE_API_KEY=sk_test_... ANTHROPIC_API_KEY=sk-ant-...
uv run python examples/refund_agent/seed.py        # outside irimi: prints "seeded charge ch_..."
uv run python -m examples.llm_refund_agent.seed ch_... "Ada Lovelace"
uv run irimi shadow -- python -m examples.llm_refund_agent T-1 ch_... "I was charged twice"
uv run irimi runs list                             # the handle_ticket run's id is the first column
PROMPT_FILE=examples/llm_refund_agent/prompt_half.txt uv run irimi replay <run id>
```

The model is `claude-opus-5`; set `ANTHROPIC_MODEL` to use another. The agent's database is
`llm_refunds.sqlite3` in the working directory; `REFUND_DB` moves it.

A pass looks like this:

- **The recording.** `irimi shadow` prints the customer lookup run for real, the charge read and
  the model call live, the refund `fake-L1`, and `record_decision`'s stand-in. The run id it
  prints is the process's; `irimi runs list` shows the run to replay as `sdk  handle_ticket`.
- **The replay.** It ends with a block that names the model call as where it diverged, and the
  refund as the write that changed:

  ```
  irimi replay · <run id> → <replay id> · - → -
    agent exited 0
    diverged at #3: live      llm       POST api.anthropic.com/v1/messages -> 200  [replay-miss]
    2 from the recording · 2 missed

    ~ A ○ refund $49.00 on ch_...  unvalidated (L3 preconditions passed)
      B ○ refund $24.50 on ch_...  unvalidated (L3 preconditions passed)
    = ○ tool examples.llm_refund_agent.agent.record_decision  shadow stand-in ran

    1 same · 1 changed · 0 only in A · 0 only in B
  ```

  The amounts are whatever the model decided; the exit code is 1, since a write changed.
  `irimi runs list` shows the replay as `replay of <run id>`.
- **Nothing happened.** The charge on the Stripe dashboard has no refund, and
  `llm_refunds.sqlite3` holds no row in `decisions`.

A replay with no prompt change says `no divergence: every call was answered from the recording`
and exits 0, and reaches neither Stripe nor Anthropic.
