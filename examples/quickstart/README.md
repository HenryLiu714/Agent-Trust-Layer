# Quickstart: a refund agent on stripe-python

`agent.py` is a refund agent of under 100 lines. It lists charges and picks one, refunds it in
full, lists the charge's refunds, re-reads the charge, and retries the refund. Run bare, the refund
is real. Run under `irimi shadow`, irimi fakes the refund, shows it to the two reads after it, and
refuses the retry as Stripe would, and Stripe never hears of the refund.

The run is one function marked `@sdk.trigger`, so irimi records it as a run you can replay.

## Try it without keys

From the repository root, after `make setup`:

```
make demo EXAMPLE=quickstart
```

This runs the agent under `irimi shadow` against a fake Stripe on loopback, seeded with one charge.

## Run it against Stripe test mode

You need a Stripe test-mode key and the `examples` dependency group (`uv sync --group examples`).
The agent refuses any key that is not `sk_test_...`.

```
export STRIPE_API_KEY=sk_test_...
uv run python -m examples.seed_stripe                       # outside irimi: a charge to refund
uv run irimi shadow -- python -m examples.quickstart.agent
```

The agent prints what it saw:

```
charge ch_...: 49.00 USD left to refund
refund re_...: 49.00 USD
refunds on ch_...: re_...
charge ch_...: 49.00 USD refunded
retry refused: charge_already_refunded
```

and irimi's summary names both refunds as faked: the first `checked against live state: would
succeed`, the retry `would fail: charge_already_refunded`. The Stripe dashboard shows the charge
with no refund. Run the agent without `irimi shadow` and the refund is real; seed another charge
before the next run.

## Going through irimi

stripe-python checks certificates against the CA bundle it ships, not `SSL_CERT_FILE`, so
`connect()` points it at irimi's proxy and CA when `IRIMI_ENGINE_ACTIVE` is set. Base-URL mode is
the other way in, for SDKs that ignore proxy settings: set `stripe.api_base` to the proxy's origin
followed by `/api.stripe.com`.

Tested by `tests/e2e/test_quickstart.py`, and against Stripe test mode by
`tests/live/test_stripe_test_mode.py` (`make live`).
