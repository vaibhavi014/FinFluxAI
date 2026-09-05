#!/usr/bin/env bash
set -euo pipefail
API="${FINFLUX_API:-http://localhost:8080}"

now() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

post() {
  local id="$1" customer="$2" amount="$3" city="$4" category="$5" status="$6" type="$7"
  curl -sf -X POST "$API/api/v1/transactions" \
    -H "Content-Type: application/json" \
    -H "X-Correlation-Id: demo-$(date +%s)-$id" \
    -d "{
      \"transactionId\": \"$id\",
      \"customerId\": \"$customer\",
      \"accountId\": \"ACC-${customer#CUST-}\",
      \"amount\": $amount,
      \"currency\": \"INR\",
      \"merchantId\": \"MERCHANT-55\",
      \"merchantCategory\": \"$category\",
      \"deviceId\": \"DEVICE-77\",
      \"ipAddress\": \"192.168.1.10\",
      \"location\": \"$city\",
      \"timestamp\": \"$(now)\",
      \"type\": \"$type\",
      \"status\": \"$status\"
    }" >/dev/null
  echo "Posted $id ($amount INR, $city, $category, $status)"
}

mode="${1:-normal}"
suffix="$(date +%s)"

case "$mode" in
  normal)
    post "TXN-N1-$suffix" "CUST-101" 450 "Mumbai" "GROCERY" "SUCCESS" "PAYMENT"
    post "TXN-N2-$suffix" "CUST-102" 1200 "Bengaluru" "FOOD" "SUCCESS" "PAYMENT"
    post "TXN-N3-$suffix" "CUST-103" 800 "Delhi" "TRAVEL" "SUCCESS" "PAYMENT"
    ;;
  suspicious)
    for i in 1 2 3 4; do
      post "TXN-S0$i-$suffix" "CUST-101" 400 "Mumbai" "GROCERY" "SUCCESS" "PAYMENT"
    done
    post "TXN-S1-$suffix" "CUST-101" 95000 "Mumbai" "ELECTRONICS" "SUCCESS" "PAYMENT"
    post "TXN-S2-$suffix" "CUST-104" 1000 "Pune" "GROCERY" "SUCCESS" "PAYMENT"
    post "TXN-S3-$suffix" "CUST-104" 1100 "Delhi" "GROCERY" "SUCCESS" "PAYMENT"
    post "TXN-S4-$suffix" "CUST-105" 50 "Hyderabad" "GROCERY" "FAILED" "PAYMENT"
    post "TXN-S5-$suffix" "CUST-105" 50 "Hyderabad" "GROCERY" "FAILED" "PAYMENT"
    post "TXN-S6-$suffix" "CUST-105" 50 "Hyderabad" "CRYPTO" "FAILED" "PAYMENT"
    for i in 1 2 3 4 5; do
      post "TXN-SV$i-$suffix" "CUST-102" 300 "Bengaluru" "FOOD" "SUCCESS" "PAYMENT"
    done
    ;;
  *)
    echo "Usage: $0 [normal|suspicious]"
    exit 1
    ;;
esac

echo
echo "Inserted source rows. Debezium should publish CDC events shortly."
echo "Open http://localhost:8080 and watch processingStatus change from PENDING to SCORED."
