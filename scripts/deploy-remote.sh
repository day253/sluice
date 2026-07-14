#!/usr/bin/env bash

set -Eeuo pipefail

DEPLOY_HOST="${DEPLOY_HOST:-192.168.3.85}"
DEPLOY_USER="${DEPLOY_USER:-tiger}"
DEPLOY_DIR="${DEPLOY_DIR:-/home/tiger/Documents/distributed-rate-limiting}"
RELEASE="${RELEASE:-sluice}"
NAMESPACE="${NAMESPACE:-default}"
REGISTRY="${REGISTRY:-localhost:32000}"
# A 50-replica StatefulSet rolls in ordinal order and routinely needs more
# than five minutes even when every Pod becomes Ready normally.
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

cd "${DEPLOY_DIR}"

for command in go docker microk8s; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    printf 'Required command not found on remote host: %s\n' "${command}" >&2
    exit 1
  fi
done

printf '\n==> Running tests\n'
go test ./...

printf '\n==> Building container on remote host\n'
docker build -t "${IMAGE}" .

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
  --set affinity.enabled=false \
  --wait \
  --timeout "${ROLLOUT_TIMEOUT}"

printf '\n==> Waiting for StatefulSet rollout\n'
microk8s kubectl rollout status "statefulset/${STATEFULSET}" \
  --namespace "${NAMESPACE}" \
  --timeout "${ROLLOUT_TIMEOUT}"

printf '\n==> Verifying every Sluice pod\n'
pods="$(microk8s kubectl get pods \
  --namespace "${NAMESPACE}" \
  -l "app.kubernetes.io/name=sluice,app.kubernetes.io/instance=${RELEASE}" \
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

printf '\nDeployed %s\n' "${IMAGE}"
microk8s kubectl get pods \
  --namespace "${NAMESPACE}" \
  -l "app.kubernetes.io/name=sluice,app.kubernetes.io/instance=${RELEASE}" \
  -o wide
REMOTE
