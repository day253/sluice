#!/usr/bin/env bash

set -Eeuo pipefail

DEPLOY_HOST="${DEPLOY_HOST:-192.168.3.85}"
DEPLOY_USER="${DEPLOY_USER:-tiger}"
DEPLOY_DIR="${DEPLOY_DIR:-/home/tiger/Documents/distributed-rate-limiting}"
RELEASE="${RELEASE:-sluice}"
NAMESPACE="${NAMESPACE:-default}"
REGISTRY="${REGISTRY:-localhost:32000}"
# The five control replicas roll in order; stateless Workers roll in parallel.
# Keep enough time for the one-time legacy 50-member Raft migration.
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-15m}"
# Keep a warm five-Pod execution floor, but do not pin an idle demo cluster at
# the 50-Pod static fallback. In autoscaling mode Helm intentionally leaves
# spec.replicas to the scale owner. The chart's production default remains a
# conservative five-minute scale-down window; this remote demo uses one minute
# so the protected scale-down behavior is observable during an interactive
# session.
WORKER_STATIC_REPLICAS="${WORKER_STATIC_REPLICAS:-50}"
WORKER_MIN_REPLICAS="${WORKER_MIN_REPLICAS:-5}"
WORKER_MAX_REPLICAS="${WORKER_MAX_REPLICAS:-100}"
WORKER_SCALE_DOWN_STABILIZATION_SECONDS="${WORKER_SCALE_DOWN_STABILIZATION_SECONDS:-60}"

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
  "${DEPLOY_DIR}" "${REGISTRY}" "${TAG}" "${RELEASE}" "${NAMESPACE}" "${ROLLOUT_TIMEOUT}" \
  "${WORKER_STATIC_REPLICAS}" "${WORKER_MIN_REPLICAS}" "${WORKER_MAX_REPLICAS}" \
  "${WORKER_SCALE_DOWN_STABILIZATION_SECONDS}" <<'REMOTE'
set -Eeuo pipefail

DEPLOY_DIR="$1"
REGISTRY="$2"
TAG="$3"
RELEASE="$4"
NAMESPACE="$5"
ROLLOUT_TIMEOUT="$6"
WORKER_STATIC_REPLICAS="$7"
WORKER_MIN_REPLICAS="$8"
WORKER_MAX_REPLICAS="$9"
WORKER_SCALE_DOWN_STABILIZATION_SECONDS="${10}"
IMAGE="${REGISTRY}/sluice:${TAG}"
STATEFULSET="${RELEASE}-sluice"
WORKER_STATEFULSET="${RELEASE}-sluice-worker"

cd "${DEPLOY_DIR}"

for value in \
  "${WORKER_STATIC_REPLICAS}" \
  "${WORKER_MIN_REPLICAS}" \
  "${WORKER_MAX_REPLICAS}" \
  "${WORKER_SCALE_DOWN_STABILIZATION_SECONDS}"; do
  case "${value}" in
    ''|*[!0-9]*)
      printf 'Worker scaling values must be non-negative integers: %s\n' "${value}" >&2
      exit 1
      ;;
  esac
done
if [ "${WORKER_STATIC_REPLICAS}" -lt 1 ] ||
  [ "${WORKER_MIN_REPLICAS}" -lt 1 ] ||
  [ "${WORKER_MAX_REPLICAS}" -lt "${WORKER_MIN_REPLICAS}" ]; then
  printf 'Invalid Worker configuration: static=%s min=%s max=%s\n' \
    "${WORKER_STATIC_REPLICAS}" "${WORKER_MIN_REPLICAS}" "${WORKER_MAX_REPLICAS}" >&2
  exit 1
fi

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
  --set worker.replicas="${WORKER_STATIC_REPLICAS}" >/tmp/sluice-rendered.yaml
microk8s helm3 template "${RELEASE}" ./charts/sluice \
  --namespace "${NAMESPACE}" \
  --set control.replicas=5 \
  --set worker.autoscaling.enabled=true \
  --set worker.autoscaling.mode=workload \
  --set worker.autoscaling.minReplicas="${WORKER_MIN_REPLICAS}" \
  --set worker.autoscaling.maxReplicas="${WORKER_MAX_REPLICAS}" \
  --set worker.autoscaling.workload.scaleDownStabilizationSeconds="${WORKER_SCALE_DOWN_STABILIZATION_SECONDS}" \
  >/tmp/sluice-workload-autoscaling-rendered.yaml
microk8s kubectl apply --dry-run=server --namespace "${NAMESPACE}" \
  -f /tmp/sluice-workload-autoscaling-rendered.yaml >/dev/null
microk8s helm3 template "${RELEASE}" ./charts/sluice \
  --namespace "${NAMESPACE}" \
  --set control.replicas=5 \
  --set worker.autoscaling.enabled=true \
  --set worker.autoscaling.mode=hpa \
  --set worker.autoscaling.minReplicas="${WORKER_MIN_REPLICAS}" \
  --set worker.autoscaling.maxReplicas="${WORKER_MAX_REPLICAS}" \
  --set operator.enabled=true >/tmp/sluice-hpa-autoscaling-rendered.yaml
microk8s kubectl apply --dry-run=server --namespace "${NAMESPACE}" \
  -f /tmp/sluice-hpa-autoscaling-rendered.yaml >/dev/null

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
  CGO_ENABLED=0 go build -trimpath -o bin/sluice-autoscaler ./cmd/autoscaler
  offline_container="$(docker create "${BASE_IMAGE}")"
  if ! docker cp bin/sluice "${offline_container}:/usr/local/bin/sluice" || \
    ! docker cp bin/sluice-operator "${offline_container}:/usr/local/bin/sluice-operator" || \
    ! docker cp bin/sluice-autoscaler "${offline_container}:/usr/local/bin/sluice-autoscaler"; then
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
  -l "app.kubernetes.io/name=sluice,app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=control" \
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
    -l "app.kubernetes.io/name=sluice,app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=control" \
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
  --set worker.replicas="${WORKER_STATIC_REPLICAS}" \
  --set worker.autoscaling.enabled=true \
  --set worker.autoscaling.mode=workload \
  --set worker.autoscaling.minReplicas="${WORKER_MIN_REPLICAS}" \
  --set worker.autoscaling.maxReplicas="${WORKER_MAX_REPLICAS}" \
  --set worker.autoscaling.workload.scaleDownStabilizationSeconds="${WORKER_SCALE_DOWN_STABILIZATION_SECONDS}" \
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
microk8s kubectl rollout status "deployment/${RELEASE}-sluice-worker-autoscaler" \
  --namespace "${NAMESPACE}" \
  --timeout "${ROLLOUT_TIMEOUT}"

printf '\n==> Waiting for workload autoscaler minimum Worker capacity\n'
worker_min_ready=false
for _ in $(seq 1 180); do
  worker_desired="$(microk8s kubectl get "statefulset/${WORKER_STATEFULSET}" \
    --namespace "${NAMESPACE}" -o jsonpath='{.spec.replicas}')"
  worker_ready="$(microk8s kubectl get "statefulset/${WORKER_STATEFULSET}" \
    --namespace "${NAMESPACE}" -o jsonpath='{.status.readyReplicas}')"
  worker_ready="${worker_ready:-0}"
  if [ "${worker_desired}" -ge "${WORKER_MIN_REPLICAS}" ] &&
    [ "${worker_ready}" -ge "${WORKER_MIN_REPLICAS}" ]; then
    worker_min_ready=true
    break
  fi
  sleep 1
done
if [ "${worker_min_ready}" != "true" ]; then
  printf 'Workload autoscaler did not restore minimum capacity: desired=%s ready=%s\n' \
    "${worker_desired}" "${worker_ready}" >&2
  exit 1
fi

printf '\n==> Verifying control and Worker topology\n'
./scripts/verify-deployed-topology.sh \
  "${RELEASE}" "${NAMESPACE}" 5 \
  "${WORKER_MIN_REPLICAS}" "${WORKER_MAX_REPLICAS}" 100 \
  "${WORKER_SCALE_DOWN_STABILIZATION_SECONDS}"

printf '\nDeployed %s\n' "${IMAGE}"
microk8s kubectl get pods \
  --namespace "${NAMESPACE}" \
  -l "app.kubernetes.io/instance=${RELEASE}" \
  -o wide
REMOTE
