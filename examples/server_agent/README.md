# A server agent beside `irimi shadow --serve`

Phase 3's exit, for use case 0: "a server-style agent in a container produces attributed runs with
their triggers, and two concurrent requests don't mix" (#78).

`app.py` is a minimal refund agent on stdlib `ThreadingHTTPServer`. Each `POST /refund` with
`{"charge": "ch_...", "reason": "..."}` is one run, `handle_refund` (`@sdk.trigger`):

1. it reads the charge from Stripe, a live read;
2. it records the refund in its own SQLite through `mark_refunded`, an `@sdk.tool(kind="write")`
   whose stand-in runs under irimi;
3. it refunds the charge, a write irimi fakes;
4. it answers `200 {"refund": "re_..."}`.

It calls Stripe with `urllib.request`, which takes `HTTPS_PROXY` and `SSL_CERT_FILE` from the
environment, so it needs no code to go through irimi. Start it by module name,
`python -m examples.server_agent`, so the trigger's entrypoint is importable.

## Two halves

- **Hermetic, in CI.** `tests/test_phase3_exit.py` runs the real `irimi shadow --serve` and this
  app as two processes against a loopback Stripe, sends two refunds at once and checks that each
  is one run holding only its own charge.
- **Containers, by hand.** `check.sh` runs irimi and the app in two containers
  (`compose.yaml`) against Stripe test mode. It is not in CI.

## Running the container check

You need Docker with compose, `uv` (`check.sh` runs `uv run --group examples` for the Stripe SDK),
and a Stripe test-mode key:

```
export STRIPE_API_KEY=sk_test_...
examples/server_agent/check.sh
```

`check.sh` seeds two test-mode charges with `examples/refund_agent/seed.py`, builds and starts both
containers, sends two refunds at once, reads `irimi runs list` inside the irimi container, checks
that Stripe holds no refund for either charge, and tears everything down. A pass ends:

```
--- irimi runs list
30c26ae8c452baa9  2026-10-13 20:50:26  0.4s  ok  sdk  handle_refund  3 exchanges  1 write
4087dd7e4ab996b9  2026-10-13 20:50:26  0.4s  ok  sdk  handle_refund  3 exchanges  1 write
...
--- Stripe holds no refund for either charge
ch_irimiExample00000000001: amount_refunded=0 refunds=0
ch_irimiExample00000000002: amount_refunded=0 refunds=0
PASS: two concurrent refunds were two ok runs in irimi, and neither reached Stripe
```

Between those lines it prints irimi's own output: one line per exchange and tool call, and one
`--- run <id> ---` block per run, each naming its own charge.

To drive it yourself instead, from this directory, with `STRIPE_API_KEY` exported and a test-mode
charge id of your own (`uv run --group examples python examples/refund_agent/seed.py` from the repo
root makes one):

```
docker compose up --build --detach --wait
curl -X POST http://127.0.0.1:8080/refund -d '{"charge": "ch_...", "reason": "requested_by_customer"}'
docker compose exec irimi irimi runs list
```

`docker compose logs irimi` shows the run's summary, and `docker compose down -v` removes both
containers and irimi's store.

## How the containers are wired

- `irimi` runs `irimi init`, copies **only** `ca.pem` into the `ca-public` volume, and serves with
  `--listen-host 0.0.0.0`. Its `irimi-home` volume, which holds the CA key, the `mitm/` bundle and
  the trace store, is mounted on `irimi` alone. Its port is not published: the control endpoint
  has no authentication. To reach irimi from the host, publish its port unchanged (`4000:4000`):
  irimi knows a request is its own by the port it arrived on, and one remapped would be forwarded
  back to irimi (#126).
- `agent` mounts `ca-public` read-only and carries the README's "Serve mode" variables:
  `HTTPS_PROXY` and `HTTP_PROXY`, in both cases, naming `http://irimi:4000`, `SSL_CERT_FILE` and
  `REQUESTS_CA_BUNDLE` naming `/ca/ca.pem`, `IRIMI_ENGINE_ACTIVE=1` and
  `IRIMI_CONTROL=http://irimi:4000/_irimi`. `STRIPE_API_KEY` comes from your shell.
- `check.sh` sets `IRIMI_EXAMPLE_BARRIER=2`, which holds each refund after its charge read until
  both are there, so the two runs are open at once.
