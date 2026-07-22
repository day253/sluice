#!/usr/bin/env bash

set -Eeuo pipefail

DEPLOY_HOST="${DEPLOY_HOST:-192.168.3.85}"
DEPLOY_USER="${DEPLOY_USER:-tiger}"
DEPLOY_DIR="${DEPLOY_DIR:-/home/tiger/Documents/distributed-rate-limiting}"
RELEASE="${RELEASE:-sluice}"
NAMESPACE="${NAMESPACE:-default}"
REGISTRY="${REGISTRY:-localhost:32000}"
# The five control replicas roll in order; 50 stateless Workers start in
# parallel. Keep enough time for the one-time 50-member Raft migration.
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-15m}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${DEPLOY_USER}@${DEPLOY_HOST}"
REVISION="$(git -C "${ROOT_DIR}" rev-parse --short HEAD 2>/dev/null || printf 'unknown')"
TAG="${IMAGE_TAG:-${REVISION}-$(date -u +%Y%m%d%H%M%S)}"

printf 'Syncing source to %s:%s\n' "${TARGET}" "${DEPLOY_DIR}"
rsync -az \
  --exclude='.git/' \
  --exclude='bin/' \
  --exclude='data/' \
  --exclude='*.out' \
  --exclude='.DS_Store' \
  "${ROOT_DIR}/" "${TARGET}:${DEPLOY_DIR}/"

printf 'Building and deploying image %s/sluice:%s\n' "${REGISTRY}" "${TAG}"
ssh "${TARGET}" bash -s -- \
  "${DEPLOY_DIR}" "${REGISTRY}" "${TAG}" "${RELEASE}" "${NAMESPACE}" "${ROLLOUT_TIMEOUT}" <<'REMOTE'
set -Eeuo pipefail

DEPLOY_DIR="$1"
REGISTRY="$2"
TAG="$3"
RELEASE="$4"
NAMESPACE="$5"
ROLLOUT_TIMEOUT="$6"
IMAGE="${REGISTRY}/sluice:${TAG}"
STATEFULSET="${RELEASE}-sluice"
WORKER_STATEFULSET="${RELEASE}-sluice-worker"

cd "${DEPLOY_DIR}"

for command in go docker microk8s; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    printf 'Required command not found on remote host: %s\n' "${command}" >&2
    exit 1
  fi
done

printf '\n==> Running tests\n'
go test ./...

printf '\n==> Validating Helm role split\n'
microk8s helm3 lint ./charts/sluice
microk8s helm3 template "${RELEASE}" ./charts/sluice \
  --namespace "${NAMESPACE}" \
  --set control.replicas=5 \
  --set worker.replicas=50 >/tmp/sluice-rendered.yaml
microk8s helm3 template "${RELEASE}" ./charts/sluice \
  --namespace "${NAMESPACE}" \
  --set control.replicas=5 \
  --set worker.autoscaling.enabled=true \
  --set worker.autoscaling.minReplicas=50 \
  --set worker.autoscaling.maxReplicas=100 \
  --set operator.enabled=true >/tmp/sluice-autoscaling-rendered.yaml
microk8s kubectl apply --dry-run=server --namespace "${NAMESPACE}" \
  -f /tmp/sluice-autoscaling-rendered.yaml >/dev/null

printf '\n==> Building container on remote host\n'
if ! docker build -t "${IMAGE}" .; then
  # Docker Hub is not required for subsequent deploys. Reuse the currently
  # running Alpine image and replace only the statically compiled binary when
  # base-image metadata lookup is temporarily unavailable.
  BASE_IMAGE="$(microk8s kubectl get statefulset "${STATEFULSET}" \
    --namespace "${NAMESPACE}" -o jsonpath='{.spec.template.spec.containers[0].image}')"
  if [ -z "${BASE_IMAGE}" ]; then
    printf 'No deployed image is available for offline build fallback\n' >&2
    exit 1
  fi
  printf 'Docker Hub unavailable; compiling locally and reusing %s\n' "${BASE_IMAGE}"
  mkdir -p bin
  CGO_ENABLED=0 go build -trimpath -o bin/sluice ./cmd/sluice
  CGO_ENABLED=0 go build -trimpath -o bin/sluice-operator ./cmd/operator
  offline_container="$(docker create "${BASE_IMAGE}")"
  if ! docker cp bin/sluice "${offline_container}:/usr/local/bin/sluice" || \
    ! docker cp bin/sluice-operator "${offline_container}:/usr/local/bin/sluice-operator"; then
    docker rm "${offline_container}" >/dev/null
    exit 1
  fi
  docker commit "${offline_container}" "${IMAGE}" >/dev/null
  docker rm "${offline_container}" >/dev/null
fi

printf '\n==> Publishing to the local MicroK8s registry\n'
docker push "${IMAGE}"

printf '\n==> Migrating existing Raft members to stable per-Pod ClusterIPs\n'
existing_pods="$(microk8s kubectl get pods \
  --namespace "${NAMESPACE}" \
  -l "app.kubernetes.io/name=sluice,app.kubernetes.io/instance=${RELEASE}" \
  -o name 2>/dev/null || true)"

if [ -n "${existing_pods}" ]; then
  printf 'Ensuring per-Pod ClusterIP services exist\n'
  for pod in ${existing_pods}; do
    pod_name="${pod#pod/}"
    service_name="${pod_name}-raft"
    if ! microk8s kubectl get service --namespace "${NAMESPACE}" "${service_name}" >/dev/null 2>&1; then
      microk8s kubectl create service clusterip "${service_name}" \
        --namespace "${NAMESPACE}" --tcp=9090:9090 --tcp=7000:7000
    fi
    microk8s kubectl annotate service --namespace "${NAMESPACE}" "${service_name}" \
      meta.helm.sh/release-name="${RELEASE}" \
      meta.helm.sh/release-namespace="${NAMESPACE}" --overwrite >/dev/null
    microk8s kubectl label service --namespace "${NAMESPACE}" "${service_name}" \
      app.kubernetes.io/managed-by=Helm \
      app.kubernetes.io/name=sluice \
      app.kubernetes.io/instance="${RELEASE}" --overwrite >/dev/null
    microk8s kubectl patch service --namespace "${NAMESPACE}" "${service_name}" \
      --type merge \
      -p "{\"spec\":{\"selector\":{\"app\":null,\"app.kubernetes.io/name\":\"sluice\",\"app.kubernetes.io/instance\":\"${RELEASE}\",\"statefulset.kubernetes.io/pod-name\":\"${pod_name}\"}}}" >/dev/null
  done

  probe_name="$(microk8s kubectl get pods \
    --namespace "${NAMESPACE}" \
    -l "app.kubernetes.io/name=sluice,app.kubernetes.io/instance=${RELEASE}" \
    -o jsonpath='{range .items[?(@.status.containerStatuses[0].ready==true)]}{.metadata.name}{"\n"}{end}' \
    2>/dev/null | head -n 1 || true)"

  if [ -z "${probe_name}" ]; then
    printf 'No Ready Sluice pod; skipping live Raft migration and continuing with Helm recovery.\n'
  else
    probe_pod="pod/${probe_name}"
    probe_ip="$(microk8s kubectl get service --namespace "${NAMESPACE}" "${probe_name}-raft" -o jsonpath='{.spec.clusterIP}')"
    health="$(microk8s kubectl exec --namespace "${NAMESPACE}" "${probe_pod}" -- \
      wget -qO- "http://${probe_ip}:9090/api/v1/health")"
    leader_raft="$(printf '%s' "${health}" | sed -n 's/.*"leader":"\([^"]*\)".*/\1/p')"
    leader_host="${leader_raft%:*}"

    if [ -z "${leader_host}" ]; then
      printf 'Could not discover the current Raft leader from: %s\n' "${health}" >&2
      exit 1
    fi

    if printf '%s' "${leader_host}" | grep -q '^sluice-'; then
      leader_pod="${leader_host%%.*}"
      leader_service_ip="$(microk8s kubectl get service --namespace "${NAMESPACE}" \
        "${leader_pod}-raft" -o jsonpath='{.spec.clusterIP}')"
    else
      # A leader reported as an IP is already directly reachable. It may be an
      # old Pod IP during migration or the stable per-Pod ClusterIP afterwards.
      leader_service_ip="${leader_host}"
    fi

    for pod in ${existing_pods}; do
      pod_name="${pod#pod/}"
      pod_service_ip="$(microk8s kubectl get service --namespace "${NAMESPACE}" \
        "${pod_name}-raft" -o jsonpath='{.spec.clusterIP}')"
      payload="$(printf '{\"node_id\":\"%s\",\"raft_address\":\"%s:7000\",\"http_address\":\"%s:9090\",\"total_workers\":100}' \
        "${pod_name}" "${pod_service_ip}" "${pod_service_ip}")"
      printf '%s: ' "${pod_name}"
      microk8s kubectl exec --namespace "${NAMESPACE}" "${probe_pod}" -- \
        wget -qO- --header='Content-Type: application/json' --post-data="${payload}" \
        "http://${leader_service_ip}:9090/api/v1/cluster/join"
      printf '\n'
    done
  fi
else
  printf 'No existing release found; migration is not needed.\n'
fi

printf '\n==> Upgrading Helm release\n'
microk8s helm3 upgrade --install "${RELEASE}" ./charts/sluice \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --set image.repository="${REGISTRY}/sluice" \
  --set-string image.tag="${TAG}" \
  --set image.pullPolicy=Always \
  --set control.replicas=5 \
  --set worker.replicas=50 \
  --set raftVoters=5 \
  --set affinity.enabled=false \
  --wait \
  --timeout "${ROLLOUT_TIMEOUT}"

printf '\n==> Waiting for StatefulSet rollout\n'
microk8s kubectl rollout status "statefulset/${STATEFULSET}" \
  --namespace "${NAMESPACE}" \
  --timeout "${ROLLOUT_TIMEOUT}"
microk8s kubectl rollout status "statefulset/${WORKER_STATEFULSET}" \
  --namespace "${NAMESPACE}" \
  --timeout "${ROLLOUT_TIMEOUT}"

printf '\n==> Verifying control and Worker topology\n'
pods="$(microk8s kubectl get pods \
  --namespace "${NAMESPACE}" \
  -l "app.kubernetes.io/instance=${RELEASE}" \
  -o name)"

if [ -z "${pods}" ]; then
  printf 'No Sluice pods found after deployment\n' >&2
  exit 1
fi

for pod in ${pods}; do
  pod_ip="$(microk8s kubectl get --namespace "${NAMESPACE}" "${pod}" -o jsonpath='{.status.podIP}')"
  printf '%s: ' "${pod#pod/}"
  microk8s kubectl exec --namespace "${NAMESPACE}" "${pod}" -- \
    wget -qO- "http://${pod_ip}:9090/api/v1/health"
  printf '\n'
done

control_count="$(microk8s kubectl get pods --namespace "${NAMESPACE}" \
  -l "app.kubernetes.io/name=sluice,app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=control" \
  --field-selector=status.phase=Running --no-headers | wc -l | tr -d ' ')"
worker_count="$(microk8s kubectl get pods --namespace "${NAMESPACE}" \
  -l "app.kubernetes.io/name=sluice-worker,app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=worker" \
  --field-selector=status.phase=Running --no-headers | wc -l | tr -d ' ')"
if [ "${control_count}" != "5" ] || [ "${worker_count}" != "50" ]; then
  printf 'Unexpected topology: controls=%s workers=%s\n' "${control_count}" "${worker_count}" >&2
  exit 1
fi

probe_control="$(microk8s kubectl get pods --namespace "${NAMESPACE}" \
  -l "app.kubernetes.io/name=sluice,app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=control" \
  -o jsonpath='{.items[0].metadata.name}')"
topology_ready=false
for _ in $(seq 1 60); do
  nodes_json="$(microk8s kubectl exec --namespace "${NAMESPACE}" "pod/${probe_control}" -- \
    wget -qO- 'http://127.0.0.1:9090/api/v1/admin/nodes')"
  allocations_json="$(microk8s kubectl exec --namespace "${NAMESPACE}" "pod/${probe_control}" -- \
    wget -qO- 'http://127.0.0.1:9090/api/v1/admin/allocations')"
  if NODES_JSON="${nodes_json}" ALLOCATIONS_JSON="${allocations_json}" python3 -c '
import json, os, sys
nodes = json.loads(os.environ["NODES_JSON"])["nodes"]
allocations = json.loads(os.environ["ALLOCATIONS_JSON"])["nodes"]
controls = [node for node in nodes if node.get("role") == "control"]
workers = [node for node in nodes if node.get("role") == "worker" and node.get("status") == "up"]
roles = {node["node_id"]: node.get("role") for node in nodes}
valid = (len(nodes) == 55 and len(controls) == 5 and len(workers) == 50 and
         all(node.get("total_workers") == 0 for node in controls) and
         sum(node.get("total_workers", 0) for node in workers) == 5000 and
         all(roles.get(allocation["node_id"]) == "worker" for allocation in allocations))
sys.exit(0 if valid else 1)
'; then
    topology_ready=true
    break
  fi
  sleep 1
done
if [ "${topology_ready}" != "true" ]; then
  printf 'FSM role/allocation topology did not converge: nodes=%s allocations=%s\n' \
    "${nodes_json}" "${allocations_json}" >&2
  exit 1
fi

raft_status="$(microk8s kubectl exec --namespace "${NAMESPACE}" "pod/${probe_control}" -- \
  wget -qO- 'http://127.0.0.1:9090/api/v1/admin/raft')"
voter_count="$(printf '%s' "${raft_status}" | sed -n 's/.*"voters":\[\([^]]*\)\].*/\1/p' | \
  awk -F, '{ if (length($0) == 0) print 0; else print NF }')"
if printf '%s' "${raft_status}" | grep -q '"nonvoters":null'; then
  nonvoter_count=0
else
  nonvoter_count="$(printf '%s' "${raft_status}" | sed -n 's/.*"nonvoters":\[\([^]]*\)\].*/\1/p' | \
    awk -F, '{ if (length($0) == 0) print 0; else print NF }')"
fi
if [ "${voter_count}" != "5" ] || [ "${nonvoter_count}" != "0" ]; then
  printf 'Unexpected Raft membership: %s\n' "${raft_status}" >&2
  exit 1
fi
printf 'Topology verified: controls=%s workers=%s, Raft=%s voter/%s nonvoter\n' \
  "${control_count}" "${worker_count}" "${voter_count}" "${nonvoter_count}"

printf '\nDeployed %s\n' "${IMAGE}"
microk8s kubectl get pods \
  --namespace "${NAMESPACE}" \
  -l "app.kubernetes.io/instance=${RELEASE}" \
  -o wide
REMOTE
