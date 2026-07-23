#!/usr/bin/env bash

set -Eeuo pipefail

RELEASE="${1:?release is required}"
NAMESPACE="${2:?namespace is required}"
EXPECTED_CONTROLS="${3:?expected control replicas are required}"
MIN_WORKERS="${4:?minimum Worker replicas are required}"
MAX_WORKERS="${5:?maximum Worker replicas are required}"
WORKERS_PER_POD="${6:-100}"
EXPECTED_SCALE_DOWN_STABILIZATION_SECONDS="${7:?expected scale-down stabilization is required}"
MICROK8S_BIN="${MICROK8S_BIN:-microk8s}"
VERIFY_ATTEMPTS="${TOPOLOGY_VERIFY_ATTEMPTS:-60}"
VERIFY_INTERVAL_SECONDS="${TOPOLOGY_VERIFY_INTERVAL_SECONDS:-1}"

STATEFULSET="${RELEASE}-sluice"
WORKER_STATEFULSET="${RELEASE}-sluice-worker"
validation_log="$(mktemp)"
trap 'rm -f "${validation_log}"' EXIT

last_control_ready=0
last_worker_ready=0
last_nodes_json=
last_allocations_json=
last_raft_status=
last_autoscaler_args=

# Worker scale-down is allowed to overlap deployment verification. Never cache
# a list of Worker Pod names: an ordinal may disappear between list and exec.
# Instead, retry an atomic-enough observation made from the current StatefulSet
# Ready counts and the current FSM topology.
for _ in $(seq 1 "${VERIFY_ATTEMPTS}"); do
  last_control_ready="$("${MICROK8S_BIN}" kubectl get "statefulset/${STATEFULSET}" \
    --namespace "${NAMESPACE}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
  last_worker_ready="$("${MICROK8S_BIN}" kubectl get "statefulset/${WORKER_STATEFULSET}" \
    --namespace "${NAMESPACE}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
  last_control_ready="${last_control_ready:-0}"
  last_worker_ready="${last_worker_ready:-0}"
  last_autoscaler_args="$("${MICROK8S_BIN}" kubectl get \
    "deployment/${RELEASE}-sluice-worker-autoscaler" \
    --namespace "${NAMESPACE}" \
    -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null || true)"

  if [ "${last_control_ready}" = "${EXPECTED_CONTROLS}" ] &&
    [ "${last_worker_ready}" -ge "${MIN_WORKERS}" ] &&
    [ "${last_worker_ready}" -le "${MAX_WORKERS}" ] &&
    printf '%s' "${last_autoscaler_args}" |
      grep -q -- "--scale-down-stabilization=${EXPECTED_SCALE_DOWN_STABILIZATION_SECONDS}s"; then
    probe_control="$("${MICROK8S_BIN}" kubectl get pods \
      --namespace "${NAMESPACE}" \
      -l "app.kubernetes.io/name=sluice,app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=control" \
      -o jsonpath='{range .items[?(@.status.containerStatuses[0].ready==true)]}{.metadata.name}{"\n"}{end}' \
      2>/dev/null | head -n 1 || true)"
    probe_worker="$("${MICROK8S_BIN}" kubectl get pods \
      --namespace "${NAMESPACE}" \
      -l "app.kubernetes.io/name=sluice-worker,app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=worker" \
      -o jsonpath='{range .items[?(@.status.containerStatuses[0].ready==true)]}{.metadata.name}{"\n"}{end}' \
      2>/dev/null | head -n 1 || true)"

    if [ -n "${probe_control}" ] && [ -n "${probe_worker}" ] &&
      "${MICROK8S_BIN}" kubectl exec --namespace "${NAMESPACE}" "pod/${probe_control}" -- \
        wget -qO- 'http://127.0.0.1:9090/api/v1/health' >/dev/null 2>&1 &&
      "${MICROK8S_BIN}" kubectl exec --namespace "${NAMESPACE}" "pod/${probe_worker}" -- \
        wget -qO- 'http://127.0.0.1:9090/api/v1/health' >/dev/null 2>&1; then
      last_nodes_json="$("${MICROK8S_BIN}" kubectl exec \
        --namespace "${NAMESPACE}" "pod/${probe_control}" -- \
        wget -qO- 'http://127.0.0.1:9090/api/v1/admin/nodes' 2>/dev/null || true)"
      last_allocations_json="$("${MICROK8S_BIN}" kubectl exec \
        --namespace "${NAMESPACE}" "pod/${probe_control}" -- \
        wget -qO- 'http://127.0.0.1:9090/api/v1/admin/allocations' 2>/dev/null || true)"
      worker_capacity="$((last_worker_ready * WORKERS_PER_POD))"
      if NODES_JSON="${last_nodes_json}" ALLOCATIONS_JSON="${last_allocations_json}" \
        python3 "$(dirname "$0")/validate-topology.py" \
          --controls "${EXPECTED_CONTROLS}" \
          --workers "${last_worker_ready}" \
          --worker-capacity "${worker_capacity}" \
          >"${validation_log}" 2>&1; then
        last_raft_status="$("${MICROK8S_BIN}" kubectl exec \
          --namespace "${NAMESPACE}" "pod/${probe_control}" -- \
          wget -qO- 'http://127.0.0.1:9090/api/v1/admin/raft' 2>/dev/null || true)"
        voter_count="$(printf '%s' "${last_raft_status}" |
          sed -n 's/.*"voters":\[\([^]]*\)\].*/\1/p' |
          awk -F, '{ if (length($0) == 0) print 0; else print NF }')"
        if printf '%s' "${last_raft_status}" | grep -q '"nonvoters":null'; then
          nonvoter_count=0
        else
          nonvoter_count="$(printf '%s' "${last_raft_status}" |
            sed -n 's/.*"nonvoters":\[\([^]]*\)\].*/\1/p' |
            awk -F, '{ if (length($0) == 0) print 0; else print NF }')"
        fi
        if [ "${voter_count}" = "${EXPECTED_CONTROLS}" ] &&
          [ "${nonvoter_count}" = "0" ]; then
          printf 'Topology verified: controls=%s workers=%s, Raft=%s voter/%s nonvoter\n' \
            "${last_control_ready}" "${last_worker_ready}" \
            "${voter_count}" "${nonvoter_count}"
          exit 0
        fi
      fi
    fi
  fi
  printf 'Topology not yet converged: controls=%s workers=%s; retrying\n' \
    "${last_control_ready}" "${last_worker_ready}"
  if [ "${VERIFY_INTERVAL_SECONDS}" != "0" ]; then
    sleep "${VERIFY_INTERVAL_SECONDS}"
  fi
done

printf 'Topology did not converge: controls=%s workers=%s autoscaler_args=%s nodes=%s allocations=%s Raft=%s\n' \
  "${last_control_ready}" "${last_worker_ready}" \
  "${last_autoscaler_args}" \
  "${last_nodes_json}" "${last_allocations_json}" "${last_raft_status}" >&2
if [ -s "${validation_log}" ]; then
  cat "${validation_log}" >&2
fi
exit 1
