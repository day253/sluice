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
// 38-Worker topology instead of execing a cached, deleted Pod ordinal.
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
		nodes = append(nodes, topologyNode{
			NodeID: fmt.Sprintf("worker-%d", index),
			Role:   "worker", Status: "up", TotalWorkers: 100,
		})
	}
	nodesJSON, err := json.Marshal(map[string]any{"nodes": nodes})
	if err != nil {
		t.Fatal(err)
	}

	command := exec.Command(
		filepath.Join(repositoryRoot, "scripts/verify-deployed-topology.sh"),
		"sluice", "default", "5", "5", "100", "100",
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
		!strings.Contains(string(output), "controls=5 workers=38, Raft=5 voter/0 nonvoter") {
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
