# Examples

Agents to run under irimi, smallest first. Each runs against the real services with the keys its
README names. All but the sidecar also run without keys against fake services on loopback
(`make demo EXAMPLE=<name>`, from the repository root after `make setup`); the sidecar's test does
the same for it. Start the packaged ones with `python -m` from the repository root, so that irimi
can import their trigger to replay a run.

| Example | What it shows | What it needs | Command |
| --- | --- | --- | --- |
| [`hello_agent.py`](hello_agent.py) | One real read and two writes irimi fakes, in stdlib Python | nothing | `uv run irimi shadow -- python3 examples/hello_agent.py` |
| [`quickstart/`](quickstart/README.md) | A stripe-python refund agent: the faked refund shown to the reads after it, and the retry refused | a Stripe test-mode key | `uv run irimi shadow -- python -m examples.quickstart.agent` |
| [`replay/`](replay/README.md) | An LLM agent on the `anthropic` SDK, with tools: record a run, edit the prompt, replay it and see which write changed | Stripe test-mode and Anthropic keys | `uv run irimi shadow -- python -m examples.replay.agent T-1 ch_... "..."`, then `uv run irimi replay <run id>` |
| [`sidecar/`](sidecar/README.md) | A server agent beside `irimi serve` in its own container: each request one run, two at once never mixed | Docker, a Stripe test-mode key | `docker compose up` in `examples/sidecar` |

The examples that call Stripe need a charge to refund in test mode. `uv run python -m
examples.seed_stripe` creates one; run it outside irimi, or the charge is faked too. Every example
refuses a Stripe key that is not `sk_test_...`.

The SDKs come from the `examples` dependency group: `uv sync --group examples`. irimi's own
end-to-end corpus of agents is in `tests/e2e/workflows/`, and `make demo EXAMPLE=w01` runs one of
its scenarios.
