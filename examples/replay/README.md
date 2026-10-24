# Replay: record an LLM agent, edit its prompt, see which write changed

A refund agent that asks Claude how much to refund. Record a run under `irimi shadow`, edit the
prompt, replay the run with `irimi replay`, and irimi shows where the replay diverged and which
writes changed. No write reaches Stripe, and the agent's own database is never written.

`agent.py` uses the official `anthropic` and `stripe` SDKs. `handle_ticket(ticket)` is the run
(`@sdk.trigger`), and the `Ticket` it takes is a dataclass `(ticket_id, charge, message)`:

1. it looks the customer up in its own SQLite through `db.lookup_customer`, an `@sdk.read_tool`;
2. it reads the charge from Stripe;
3. it asks Claude what to refund, with the system prompt read from `PROMPT_FILE` (`prompt.txt` by
   default);
4. it refunds that amount, when it is above zero;
5. it records the decision in its SQLite through `db.record_decision`, an `@sdk.write_tool` whose
   fake runs under irimi instead.

`prompt_half.txt` tells the model to refund half. The anthropic client finds irimi's proxy and CA
in the environment on its own; stripe-python is pointed at them by `connect()`. irimi does not yet
label the anthropic client's requests with their run by itself, so `ask_model` sends the
`Irimi-Run` header that puts the model call in the run. Start the agent with `python -m`, so that
`irimi replay` can import its trigger.

## Try it without keys

From the repository root, after `make setup`:

```
make demo EXAMPLE=replay
```

This records one run under `irimi shadow` against a fake Stripe and a fake Claude on loopback.

## Run it for real

You need a Stripe test-mode key, an Anthropic API key and the `examples` dependency group
(`uv sync --group examples`). The agent refuses a Stripe key that is not `sk_test_...`.

```
export STRIPE_API_KEY=sk_test_... ANTHROPIC_API_KEY=sk-ant-...
uv run python -m examples.seed_stripe          # outside irimi: prints "seeded charge ch_..."
uv run python -m examples.replay.seed ch_... "Ada Lovelace"
uv run irimi shadow -- python -m examples.replay.agent T-1 ch_... "I was charged twice"
PROMPT_FILE=examples/replay/prompt_half.txt uv run irimi replay <run id>
```

`irimi shadow` ends its summary with the `handle_ticket` run and a `replay with: irimi replay <run
id>` line; that is the run to replay. The model is `claude-opus-5-5`; set `ANTHROPIC_MODEL` to use
another. The recording and the edited replay each make one model call of a few hundred tokens each
way, a few US cents at most together; a replay with no prompt change is answered from the
recording and costs nothing, and Stripe test mode is free. The agent's database is
`llm_refunds.sqlite3` in the working directory; `REFUND_DB` moves it.

The replay ends with a block that names the model call as where it diverged, and the refund and
the recorded decision as the writes that changed:

```
irimi replay · <run id> -> <replay id>
  agent exited 0
  diverged at the agent's 3rd call: real          llm       POST api.anthropic.com/v1/messages -> 200  [replay-miss]
  2 from the recording · 2 missed

  ~ A ○ stripe  refund $49.00 on ch_...  faked; checked against live state: would succeed
    B ○ stripe  refund $24.50 on ch_...  faked; checked against live state: would succeed
  ~ A ○ tool  db.record_decision  fake ran instead
    B ○ tool  db.record_decision  fake ran instead
      args.decision.refund_amount: 4900 -> 2450
      args.decision.reply: "Refunded 4900." -> "Refunded 2450."

  0 same · 2 changed · 0 only in A · 0 only in B
```

The amounts are whatever the model decided, and the exit code is 1, since a write changed. A
replay with no prompt change says `no divergence: every call was answered from the recording`,
exits 0, and reaches neither Stripe nor Anthropic. Afterwards the charge on the Stripe dashboard
has no refund, and `llm_refunds.sqlite3` holds no row in `decisions`.

Tested by `tests/e2e/test_replay_example.py`.
