#!/bin/bash
# teardown.sh — stop and delete all sluice Multipass VMs.
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-sluice}"
NODES="${NODES:-3}"

echo "=== Tearing down sluice cluster ==="

for i in $(seq 0 $((NODES - 1))); do
  NAME="${CLUSTER_NAME}-${i}"
  if multipass info "$NAME" &>/dev/null; then
    echo "[$NAME] stopping + deleting..."
    multipass stop "$NAME" 2>/dev/null || true
    multipass delete "$NAME" 2>/dev/null || true
  fi
done

multipass purge 2>/dev/null || true
echo "Done."
