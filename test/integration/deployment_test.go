package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestRemoteTopologyVerificationRetriesConcurrentWorkerScaleDown preserves
// HPA-010 at the production shell/Python boundary. The first observation sees
// 50 Ready replicas while the FSM already contains the post-scale 38 Workers;
// the verifier must retry, re-read the StatefulSet, and accept the converged
// 38-Worker topology instead of execing a cached, deleted Pod ordinal. One
// Worker also carries a durable 1000-slot capacity override, which must be
// validated from the FSM instead of overwritten as replicas*startup-default.
func TestRemoteTopologyVerificationRetriesConcurrentWorkerScaleDown(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test source")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), "../.."))
	tempDir := t.TempDir()
	statePath := filepath.Join(tempDir, "worker-ready-reads")
	fakeMicroK8s := filepath.Join(tempDir, "microk8s")
	fakeSource := `#!/bin/sh
set -eu
args="$*"
case "${args}" in
  *"get statefulset/sluice-sluice-worker "*"status.readyReplicas"*)
    reads=0
    if [ -f "${FAKE_STATE}" ]; then reads="$(cat "${FAKE_STATE}")"; fi
    reads=$((reads + 1))
    printf '%s' "${reads}" >"${FAKE_STATE}"
    if [ "${reads}" -eq 1 ]; then printf '50'; else printf '38'; fi
    ;;
  *"get statefulset/sluice-sluice "*"status.readyReplicas"*) printf '5' ;;
  *"get deployment/sluice-sluice-worker-autoscaler "*) printf '["--scale-down-stabilization=60s"]' ;;
  *"get pods "*"component=control"*) printf 'sluice-sluice-0\n' ;;
  *"get pods "*"component=worker"*) printf 'sluice-sluice-worker-0\n' ;;
  *"/api/v1/admin/nodes"*) printf '%s' "${FAKE_NODES_JSON}" ;;
  *"/api/v1/admin/allocations"*) printf '{"nodes":[]}' ;;
  *"/api/v1/admin/raft"*) printf '{"voters":["0","1","2","3","4"],"nonvoters":null}' ;;
  *"/api/v1/health"*) printf '{"status":"ok"}' ;;
  *) printf 'unexpected fake microk8s call: %s\n' "${args}" >&2; exit 1 ;;
esac
`
	if err := os.WriteFile(fakeMicroK8s, []byte(fakeSource), 0o755); err != nil {
		t.Fatal(err)
	}

	type topologyNode struct {
		NodeID       string `json:"node_id"`
		Role         string `json:"role"`
		Status       string `json:"status"`
		TotalWorkers int    `json:"total_workers"`
	}
	nodes := make([]topologyNode, 0, 43)
	for index := 0; index < 5; index++ {
		nodes = append(nodes, topologyNode{
			NodeID: fmt.Sprintf("control-%d", index),
			Role:   "control", Status: "up",
		})
	}
	for index := 0; index < 38; index++ {
		capacity := 100
		if index == 0 {
			capacity = 1000
		}
		nodes = append(nodes, topologyNode{
			NodeID: fmt.Sprintf("worker-%d", index),
			Role:   "worker", Status: "up", TotalWorkers: capacity,
		})
	}
	nodesJSON, err := json.Marshal(map[string]any{"nodes": nodes})
	if err != nil {
		t.Fatal(err)
	}

	command := exec.Command(
		filepath.Join(repositoryRoot, "scripts/verify-deployed-topology.sh"),
		"sluice", "default", "5", "5", "100", "1000", "60",
	)
	command.Dir = repositoryRoot
	command.Env = append(os.Environ(),
		"MICROK8S_BIN="+fakeMicroK8s,
		"FAKE_STATE="+statePath,
		"FAKE_NODES_JSON="+string(nodesJSON),
		"TOPOLOGY_VERIFY_ATTEMPTS=3",
		"TOPOLOGY_VERIFY_INTERVAL_SECONDS=0",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("scale-safe topology verification failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "controls=5 workers=50; retrying") ||
		!strings.Contains(
			string(output),
			"controls=5 workers=38 capacity=4700, Raft=5 voter/0 nonvoter",
		) {
		t.Fatalf("verification did not observe and recover from concurrent scale-down:\n%s", output)
	}
	reads, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(reads) != "2" {
		t.Fatalf("Worker Ready replicas read %s times, want exactly 2", reads)
	}
}

// TestRemoteTopologyVerificationAcceptsLargeAllocationSnapshot preserves
// DEPLOY-005 at the production shell/Python boundary. A large tenant allocation
// mirror must be streamed through files instead of copied into an exec
// environment, whose ARG_MAX limit made otherwise healthy deployments fail
// nondeterministically as the snapshot grew and shrank.
func TestRemoteTopologyVerificationAcceptsLargeAllocationSnapshot(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve integration test source")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), "../.."))
	tempDir := t.TempDir()
	fakeMicroK8s := filepath.Join(tempDir, "microk8s")
	fakeSource := `#!/bin/sh
set -eu
args="$*"
case "${args}" in
  *"get statefulset/sluice-sluice-worker "*"status.readyReplicas"*) printf '90' ;;
  *"get statefulset/sluice-sluice "*"status.readyReplicas"*) printf '5' ;;
  *"get deployment/sluice-sluice-worker-autoscaler "*) printf '["--scale-down-stabilization=60s"]' ;;
  *"get pods "*"component=control"*) printf 'sluice-sluice-0\n' ;;
  *"get pods "*"component=worker"*) printf 'sluice-sluice-worker-0\n' ;;
  *"/api/v1/admin/nodes"*) cat "${FAKE_NODES_FILE}" ;;
  *"/api/v1/admin/allocations"*) cat "${FAKE_ALLOCATIONS_FILE}" ;;
  *"/api/v1/admin/raft"*) printf '{"voters":["0","1","2","3","4"],"nonvoters":null}' ;;
  *"/api/v1/health"*) printf '{"status":"ok"}' ;;
  *) printf 'unexpected fake microk8s call: %s\n' "${args}" >&2; exit 1 ;;
esac
`
	if err := os.WriteFile(fakeMicroK8s, []byte(fakeSource), 0o755); err != nil {
		t.Fatal(err)
	}

	type topologyNode struct {
		NodeID       string `json:"node_id"`
		Role         string `json:"role"`
		Status       string `json:"status"`
		TotalWorkers int    `json:"total_workers"`
	}
	nodes := make([]topologyNode, 0, 95)
	for index := 0; index < 5; index++ {
		nodes = append(nodes, topologyNode{
			NodeID: fmt.Sprintf("control-%d", index),
			Role:   "control", Status: "up",
		})
	}
	for index := 0; index < 90; index++ {
		nodes = append(nodes, topologyNode{
			NodeID: fmt.Sprintf("worker-%d", index),
			Role:   "worker", Status: "up", TotalWorkers: 150,
		})
	}
	nodesJSON, err := json.Marshal(map[string]any{"nodes": nodes})
	if err != nil {
		t.Fatal(err)
	}
	nodesPath := filepath.Join(tempDir, "nodes.json")
	if err := os.WriteFile(nodesPath, nodesJSON, 0o600); err != nil {
		t.Fatal(err)
	}

	largeTenants := make(map[string]int, 120000)
	for index := 0; index < 120000; index++ {
		largeTenants[fmt.Sprintf("tenant-%06d-with-a-long-stable-name", index)] = 0
	}
	type allocation struct {
		NodeID  string         `json:"node_id"`
		Tenants map[string]int `json:"tenants,omitempty"`
	}
	allocations := make([]allocation, 0, 90)
	allocations = append(allocations, allocation{
		NodeID: "worker-0", Tenants: largeTenants,
	})
	for index := 1; index < 90; index++ {
		allocations = append(allocations, allocation{
			NodeID: fmt.Sprintf("worker-%d", index),
		})
	}
	allocationsJSON, err := json.Marshal(map[string]any{"nodes": allocations})
	if err != nil {
		t.Fatal(err)
	}
	if len(allocationsJSON) < 2*1024*1024 {
		t.Fatalf("large allocation fixture is only %d bytes", len(allocationsJSON))
	}
	allocationsPath := filepath.Join(tempDir, "allocations.json")
	if err := os.WriteFile(allocationsPath, allocationsJSON, 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(
		filepath.Join(repositoryRoot, "scripts/verify-deployed-topology.sh"),
		"sluice", "default", "5", "5", "90", "1000", "60",
	)
	command.Dir = repositoryRoot
	command.Env = append(os.Environ(),
		"MICROK8S_BIN="+fakeMicroK8s,
		"FAKE_NODES_FILE="+nodesPath,
		"FAKE_ALLOCATIONS_FILE="+allocationsPath,
		"TOPOLOGY_VERIFY_ATTEMPTS=1",
		"TOPOLOGY_VERIFY_INTERVAL_SECONDS=0",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("large topology verification failed: %v\n%s", err, output)
	}
	if !strings.Contains(
		string(output),
		"controls=5 workers=90 capacity=13500, Raft=5 voter/0 nonvoter",
	) {
		t.Fatalf("large topology verification reported the wrong result:\n%s", output)
	}
}
