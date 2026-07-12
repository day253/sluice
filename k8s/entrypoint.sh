#!/bin/sh
# entrypoint.sh — bootstrap or join Raft cluster based on pod ordinal.

set -e

API="${API:-0.0.0.0:9090}"
RAFT="${RAFT:-0.0.0.0:7000}"
DATA="${DATA:-/data}"
WORKERS="${WORKERS:-100}"
LOG_LEVEL="${LOG_LEVEL:-info}"

# Extract ordinal from StatefulSet pod name (e.g. sluice-2 → 2).
ORDINAL=$(echo "${POD_NAME:-unknown}" | grep -o '[0-9]*$' || echo "0")

if [ "$ORDINAL" = "0" ]; then
  echo "==> Bootstrapping cluster as sluice-0"
  exec sluice \
    --id="${POD_NAME}" \
    --api="${API}" \
    --raft="${RAFT}" \
    --data="${DATA}" \
    --workers="${WORKERS}" \
    --log-level="${LOG_LEVEL}" \
    --bootstrap
else
  # Wait for sluice-0 to be ready.
  JOIN="${JOIN_HOST:-sluice-0.sluice}:9090"
  echo "==> Joining cluster via ${JOIN} (attempting for up to 60s)"

  for i in $(seq 1 30); do
    if grpc_health_probe -addr="${JOIN}" 2>/dev/null; then
      break
    fi
    echo "   waiting for sluice-0... ${i}"
    sleep 2
  done

  exec sluice \
    --id="${POD_NAME}" \
    --api="${API}" \
    --raft="${RAFT}" \
    --data="${DATA}" \
    --workers="${WORKERS}" \
    --log-level="${LOG_LEVEL}" \
    --join="${JOIN}"
fi
