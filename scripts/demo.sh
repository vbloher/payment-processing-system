#!/usr/bin/env bash
# Runs one outbound payment through the local system started by `make up`.
set -euo pipefail

LEDGER_ADDR=${LEDGER_ADDR:-localhost:50051}
PAYMENT_URL=${PAYMENT_URL:-http://localhost:8080/api/v1}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-60}

if command -v grpcurl >/dev/null 2>&1; then
  grpcurl=(grpcurl)
else
  grpcurl=(go run github.com/fullstorydev/grpcurl/cmd/grpcurl@latest)
fi

ledger() {
  "${grpcurl[@]}" -plaintext -d "$2" "$LEDGER_ADDR" "ledger_v1.LedgerV1/$1"
}

json_field() {
  # json_field <name>: first string/number value of "<name>" in the JSON on stdin.
  sed -nE 's/.*"'"$1"'": *"?([^",}]*)"?.*/\1/p' | head -n1
}

echo "Opening a EUR account"
account_id=$(ledger OpenAccount '{"currency":"EUR"}' | json_field id)
[ -n "$account_id" ] || { echo "no account id returned" >&2; exit 1; }
echo "account: $account_id"

echo "Depositing 10000"
ledger Deposit "{\"account_id\":\"$account_id\",\"amount\":10000,\"currency\":\"EUR\",\"idempotency_key\":\"demo-$account_id\"}" >/dev/null

echo "Creating a payment of 3000 to DE-OK-1"
payment=$(curl -fsS -X POST "$PAYMENT_URL/payments" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: demo-$account_id" \
  -d "{\"account_id\":\"$account_id\",\"amount\":3000,\"currency\":\"EUR\",\"beneficiary\":\"DE-OK-1\",\"description\":\"demo\"}")
payment_id=$(printf '%s' "$payment" | json_field id)
[ -n "$payment_id" ] || { echo "no payment id returned: $payment" >&2; exit 1; }
echo "payment: $payment_id"

deadline=$((SECONDS + TIMEOUT_SECONDS))
while true; do
  payment=$(curl -fsS "$PAYMENT_URL/payments/$payment_id")
  status=$(printf '%s' "$payment" | json_field status)
  case "$status" in
    succeeded | failed) break ;;
  esac
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timeout: payment $payment_id is still '$status' after ${TIMEOUT_SECONDS}s" >&2
    echo "$payment" >&2
    exit 1
  fi
  sleep 1
done

echo "Final payment:"
echo "$payment"
echo "Balance:"
ledger GetBalance "{\"account_id\":\"$account_id\"}"
