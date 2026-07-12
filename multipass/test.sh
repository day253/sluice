#!/bin/bash
# test.sh — smoke-test a running sluice cluster.
#
# Usage: ./multipass/test.sh <node-ip>
#        ./multipass/test.sh 192.168.64.2

set -euo pipefail

NODE="${1:-127.0.0.1}"
BASE="http://${NODE}:9090"

echo "=== Sluice Cluster Smoke Test ==="
echo "Target: $BASE"
echo ""

# ---- Health ----
echo "1. Health check"
curl -sf "${BASE}/api/v1/health" | jq .
echo ""

# ---- Tenants ----
echo "2. Create tenants"
curl -sf -X PUT "${BASE}/api/v1/admin/tenants/alice" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Alice Corp","max_workers":100}' | jq .

curl -sf -X PUT "${BASE}/api/v1/admin/tenants/bob" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Bob Ltd","max_workers":50}' | jq .

echo "3. List tenants"
curl -sf "${BASE}/api/v1/admin/tenants" | jq .
echo ""

# ---- Tasks ----
echo "4. Submit tasks"
TASK1=$(curl -sf -X POST "${BASE}/api/v1/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"alice","payload":"{\"url\":\"https://example.com/1\"}"}' | jq -r .task_id)
echo "  alice task: $TASK1"

TASK2=$(curl -sf -X POST "${BASE}/api/v1/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"bob","payload":"{\"url\":\"https://example.com/2\"}"}' | jq -r .task_id)
echo "  bob task:   $TASK2"

echo ""
echo "5. Wait for completion"
for task in "$TASK1" "$TASK2"; do
  echo -n "  $task ... "
  RESULT=$(curl -sf "${BASE}/api/v1/tasks/${task}/wait?timeout=5s" | jq -r .status)
  echo "$RESULT"
done

echo ""
echo "6. Cluster status"
curl -sf "${BASE}/api/v1/admin/nodes" | jq .
echo ""

echo "7. Allocations"
curl -sf "${BASE}/api/v1/admin/allocations" | jq .

echo ""
echo "=== Smoke test passed ==="
