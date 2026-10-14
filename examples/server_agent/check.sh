#!/usr/bin/env bash
# Phase 3's exit, the container half (#78): the server agent and `irimi shadow --serve` in two
# containers, two refunds at once against Stripe test mode, and Stripe left untouched.
#
#     STRIPE_API_KEY=sk_test_... examples/server_agent/check.sh
#
# Needs Docker with compose, uv, and a Stripe TEST-mode key. It seeds two charges (real, in test
# mode), refunds both through the agent under irimi, and checks that irimi stored two ok runs and
# that Stripe holds no refund for either charge. Not in CI: run it once by hand before Phase 3
# closes, as the Phase 2 live test was.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
compose=(docker compose -f "$here/compose.yaml" -p irimi-server-agent)

case "${STRIPE_API_KEY:-}" in
  sk_test_*) ;;
  *) echo "error: set STRIPE_API_KEY to a Stripe test-mode key (sk_test_...)" >&2; exit 2 ;;
esac

out="$(mktemp -d)"
cleanup() {
  status=$?
  if [ "$status" != 0 ]; then
    echo "--- FAILED (exit $status); irimi's and the agent's output:" >&2
    "${compose[@]}" logs --no-color >&2 || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$out"
}
trap cleanup EXIT
# Ctrl-C or a kill is a failure too: exit non-zero, so the trap prints the logs before tearing down.
trap 'exit 130' INT
trap 'exit 143' TERM

echo "--- seeding two test-mode charges"
charges=()
for _ in 1 2; do
  line="$(cd "$root" && uv run --group examples python examples/refund_agent/seed.py)"
  echo "$line"
  charge="$(echo "$line" | awk '{print $3}')"
  case "$charge" in
    ch_*) charges+=("$charge") ;;
    *) echo "error: seed.py printed no charge id" >&2; exit 1 ;;
  esac
done

echo "--- starting irimi and the agent"
# An earlier run killed before its trap could tear down would leave its runs in irimi's store.
"${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
# The barrier holds each refund after its charge read until both have arrived: two runs at once.
IRIMI_EXAMPLE_BARRIER=2 "${compose[@]}" up --build --detach --wait

echo "--- two refunds at once"
pids=()
for i in 0 1; do
  curl -sS --max-time 60 -X POST http://127.0.0.1:8080/refund \
    -H 'content-type: application/json' \
    -d "{\"charge\": \"${charges[$i]}\", \"reason\": \"requested_by_customer\"}" \
    -o "$out/$i.json" -w "%{http_code}\n" >"$out/$i.status" &
  pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done  # a curl that failed fails the check
for i in 0 1; do
  echo "${charges[$i]}: $(cat "$out/$i.status") $(cat "$out/$i.json")"
  grep -q '^200$' "$out/$i.status"
  grep -q '"refund": "re_' "$out/$i.json"
done

echo "--- irimi runs list"
runs=""
logs=""
# Each run is listed once its end is on disk, and its block printed just after, line by line, from
# the store's writer thread: wait for both blocks' last line too, or the counts below race it (#78).
for _ in $(seq 1 50); do
  runs="$("${compose[@]}" exec -T irimi irimi runs list)"
  logs="$("${compose[@]}" logs --no-log-prefix irimi)"
  [ "$(echo "$runs" | grep -c '  ok  sdk  handle_refund  ')" = 2 ] &&
    [ "$(echo "$logs" | grep -c '^  These writes did not happen\.')" = 2 ] && break
  sleep 0.2
done
echo "$runs"
[ "$(echo "$runs" | grep -c '  ok  sdk  handle_refund  ')" = 2 ]
[ "$(echo "$runs" | grep -c .)" = 2 ]

echo "--- irimi's output"
echo "$logs"
# One block per run, each with its faked refund and its stood-in tool, and each charge's refund
# in one block only: two requests at once that did not mix.
[ "$(echo "$logs" | grep -c '^--- run ')" = 2 ]
[ "$(echo "$logs" | grep -c '^  ○ refund ')" = 2 ]
[ "$(echo "$logs" | grep -c '^  ○ tool db.mark_refunded  shadow stand-in ran$')" = 2 ]
for charge in "${charges[@]}"; do
  [ "$(echo "$logs" | grep -c "^  ○ refund ? on $charge  ")" = 1 ]
done

echo "--- Stripe holds no refund for either charge"
(cd "$root" && uv run --group examples python - "${charges[@]}") <<'PY'
import os
import sys

import stripe

stripe.api_key = os.environ["STRIPE_API_KEY"]
for charge_id in sys.argv[1:]:
    charge = stripe.Charge.retrieve(charge_id)
    refunds = list(stripe.Refund.list(charge=charge_id).data)
    print(f"{charge_id}: amount_refunded={charge.amount_refunded} refunds={len(refunds)}")
    assert charge.amount_refunded == 0 and not charge.refunded and refunds == [], charge_id
PY

echo "PASS: two concurrent refunds were two ok runs in irimi, and neither reached Stripe"
