# Sidecar: a server agent beside `irimi serve`

A refund agent that runs as a long-lived HTTP server, as a production agent does, with `irimi
serve` beside it in its own container. Each request is one run, irimi records it with its
trigger, and two requests at once are two runs that never mix.

`app.py` is a small server on stdlib `ThreadingHTTPServer`. Each `POST /refund` with
`{"charge": "ch_...", "reason": "..."}` is one run, `handle_refund` (`@sdk.trigger`):

1. it reads the charge from Stripe, a live read;
2. it refunds the charge, a write irimi fakes;
3. once Stripe has accepted the refund, it records it in its own SQLite through
   `db.mark_refunded`, an `@sdk.write_tool` whose fake runs under irimi;
4. it answers `200 {"refund": "re_..."}`.

It calls Stripe with `urllib.request`, which takes `HTTPS_PROXY` and `SSL_CERT_FILE` from the
environment, so it needs no code to go through irimi. It refuses a Stripe key that is not
`sk_test_...`. Start it with `python -m examples.sidecar.app`, so the trigger's entrypoint is
importable.

## Try it without keys

`tests/e2e/test_sidecar.py` runs the real `irimi serve` and this app as two processes against a
fake Stripe on loopback, sends two refunds at once, and checks that each is one run holding only
its own charge:

```
uv run pytest -q tests/e2e/test_sidecar.py
```

## Run it in containers

You need Docker with compose, the `examples` dependency group (`uv sync --group examples`) for the
seed script, and a Stripe test-mode key. From this directory, with `STRIPE_API_KEY` exported:

```
uv run python -m examples.seed_stripe          # from the repo root: prints "seeded charge ch_..."
docker compose up --build --detach --wait
curl -X POST http://127.0.0.1:8080/refund -d '{"charge": "ch_...", "reason": "requested_by_customer"}'
docker compose exec irimi irimi runs list
```

`docker compose logs irimi` shows the run's summary, and `docker compose down -v` removes both
containers and irimi's store. `scripts/live-checks/sidecar.sh` does all of this for two charges at
once and checks that Stripe holds no refund for either.

## How the containers are wired

- `irimi` runs `irimi init`, copies only `ca.pem` into the `ca-public` volume, and serves with
  `--bind 0.0.0.0`. Its `irimi-home` volume, which holds the CA key, the `mitm/` bundle and the
  trace store, is mounted on `irimi` alone. Its port is not published: the control endpoint has no
  authentication. To reach irimi from the host, publish its port unchanged (`4000:4000`): irimi
  knows a request is its own by the port it arrived on, and one remapped would be forwarded back
  to irimi.
- `agent` mounts `ca-public` read-only and carries the serve-mode variables: `HTTPS_PROXY` and
  `HTTP_PROXY`, in both cases, naming `http://irimi:4000`, `SSL_CERT_FILE` and
  `REQUESTS_CA_BUNDLE` naming `/ca/ca.pem`, `IRIMI_ENGINE_ACTIVE=1` and
  `IRIMI_CONTROL=http://irimi:4000/_irimi`. `STRIPE_API_KEY` comes from your shell.

Tested by `tests/e2e/test_sidecar.py`, and in containers by `scripts/live-checks/sidecar.sh`.
