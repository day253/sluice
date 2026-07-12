#!/bin/bash
# setup.sh — launch 3-node sluice cluster in Multipass VMs.
#
# Prerequisites: multipass, go toolchain
#
# Usage:
#   make build              # build sluice binary first
#   ./multipass/setup.sh    # launch cluster

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-sluice}"
NODES="${NODES:-3}"
BINARY="${BINARY:-./bin/sluice}"
IMAGE="${IMAGE:-22.04}"  # Ubuntu 22.04 LTS

echo "=== Sluice Multipass Cluster ==="
echo "Nodes: $NODES | Image: $IMAGE | Binary: $BINARY"

if [ ! -f "$BINARY" ]; then
  echo "Binary not found at $BINARY. Run 'make build' first."
  exit 1
fi

# ---- Launch VMs ----
for i in $(seq 0 $((NODES - 1))); do
  NAME="${CLUSTER_NAME}-${i}"
  if multipass info "$NAME" &>/dev/null; then
    echo "[$NAME] already exists, skipping launch"
    continue
  fi
  echo "[$NAME] launching..."
  multipass launch "$IMAGE" --name "$NAME" --cpus 2 --memory 2G --disk 5G
done

# ---- Distribute binary ----
for i in $(seq 0 $((NODES - 1))); do
  NAME="${CLUSTER_NAME}-${i}"
  echo "[$NAME] uploading binary..."
  multipass transfer "$BINARY" "${NAME}:/home/ubuntu/sluice"
  multipass exec "$NAME" -- chmod +x /home/ubuntu/sluice
done

# ---- Get IPs ----
declare -a IPS
for i in $(seq 0 $((NODES - 1))); do
  NAME="${CLUSTER_NAME}-${i}"
  IP=$(multipass info "$NAME" --format json | grep -o '"ipv4[^"]*"[^"]*"[^"]*"' | head -1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1)
  IPS[$i]=$IP
  echo "[$NAME] IP: ${IPS[$i]}"
done

# ---- Start cluster ----
echo ""
echo "=== Starting Raft cluster ==="

# Node 0: bootstrap.
NAME0="${CLUSTER_NAME}-0"
echo "[$NAME0] bootstrapping cluster..."
multipass exec "$NAME0" -- /home/ubuntu/sluice \
  --id="${NAME0}" \
  --api="0.0.0.0:9090" \
  --raft="0.0.0.0:7000" \
  --data="/home/ubuntu/data" \
  --bootstrap \
  --workers=100 \
  --log-level=info &
PID0=$!

sleep 3

# Nodes 1..N-1: join.
for i in $(seq 1 $((NODES - 1))); do
  NAME="${CLUSTER_NAME}-${i}"
  echo "[$NAME] joining cluster via ${IPS[0]}..."
  multipass exec "$NAME" -- /home/ubuntu/sluice \
    --id="${NAME}" \
    --api="0.0.0.0:9090" \
    --raft="0.0.0.0:7000" \
    --data="/home/ubuntu/data" \
    --join="${IPS[0]}:9090" \
    --workers=100 \
    --log-level=info &
  PID[$i]=$!
done

echo ""
echo "=== Cluster running ==="
echo "  Bootstrap:  ${IPS[0]}:9090 (pid $PID0)"
for i in $(seq 1 $((NODES - 1))); do
  echo "  Follower $i:  ${IPS[$i]}:9090 (pid ${PID[$i]})"
done
echo ""
echo "Connect:"
echo "  grpcurl -plaintext ${IPS[0]}:9090 sluice.v1.Sluice/Health"
echo "  curl -s http://${IPS[0]}:9090/api/v1/health | jq"
echo ""
echo "Teardown: ./multipass/teardown.sh"
echo "Test:     ./multipass/test.sh ${IPS[0]}"

wait
