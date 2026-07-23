package charttest

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestRemoteTopologyValidationAllowsHPAReplicaHistory(t *testing.T) {
	type node struct {
		NodeID       string `json:"node_id"`
		Role         string `json:"role"`
		Status       string `json:"status"`
		TotalWorkers int    `json:"total_workers"`
	}
	type allocation struct {
		NodeID string `json:"node_id"`
	}

	controls := []node{
		{NodeID: "control-0", Role: "control", Status: "up"},
		{NodeID: "control-1", Role: "control", Status: "up"},
		{NodeID: "control-2", Role: "control", Status: "up"},
		{NodeID: "control-3", Role: "control", Status: "up"},
		{NodeID: "control-4", Role: "control", Status: "up"},
	}
	workers := []node{
		{NodeID: "worker-0", Role: "worker", Status: "up", TotalWorkers: 100},
		{NodeID: "worker-1", Role: "worker", Status: "up", TotalWorkers: 100},
	}
	downHistory := node{NodeID: "worker-2", Role: "worker", Status: "down", TotalWorkers: 100}

	run := func(t *testing.T, nodes []node, allocations []allocation, wantValid bool) {
		t.Helper()
		nodesJSON, err := json.Marshal(map[string]any{"nodes": nodes})
		if err != nil {
			t.Fatal(err)
		}
		allocationsJSON, err := json.Marshal(map[string]any{"nodes": allocations})
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("python3", "../../../scripts/validate-topology.py",
			"--controls", "5", "--workers", "2", "--worker-capacity", "200")
		cmd.Env = append(os.Environ(),
			"NODES_JSON="+string(nodesJSON),
			"ALLOCATIONS_JSON="+string(allocationsJSON),
		)
		err = cmd.Run()
		if wantValid && err != nil {
			t.Fatalf("expected topology to be valid: %v", err)
		}
		if !wantValid && err == nil {
			t.Fatal("expected topology to be rejected")
		}
	}

	t.Run("retained down identity is not current capacity", func(t *testing.T) {
		nodes := append(append(append([]node{}, controls...), workers...), downHistory)
		run(t, nodes, []allocation{{NodeID: "worker-0"}, {NodeID: "worker-1"}}, true)
	})
	t.Run("allocation cannot target retained down identity", func(t *testing.T) {
		nodes := append(append(append([]node{}, controls...), workers...), downHistory)
		run(t, nodes, []allocation{{NodeID: "worker-2"}}, false)
	})
	t.Run("retained identity cannot replace an up worker", func(t *testing.T) {
		nodes := append(append([]node{}, controls...), workers[0], downHistory)
		run(t, nodes, []allocation{{NodeID: "worker-0"}}, false)
	})
}

func TestWorkerEntrypointUsesStableServiceIPInsteadOfClusterDNS(t *testing.T) {
	data, err := os.ReadFile("../templates/configmap.yaml")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{
		"resolve_service_ip", "CONTROLLER_IP=$(resolve_service_ip", `--controller="${CONTROLLER_IP}:`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("worker entrypoint is missing stable service discovery %q", required)
		}
	}
	if strings.Contains(source, `--controller="{{ include "sluice.fullname" . }}:`) {
		t.Fatal("worker entrypoint still depends on cluster DNS, which resolves to a fake IP on the target host")
	}
}

func TestWorkerAutoscalingTargetsOnlyStatelessStatefulSet(t *testing.T) {
	hpaData, err := os.ReadFile("../templates/hpa.yaml")
	if err != nil {
		t.Fatal(err)
	}
	hpa := string(hpaData)
	for _, required := range []string{
		"apiVersion: autoscaling/v2", "kind: HorizontalPodAutoscaler",
		`name: {{ include "sluice.fullname" . }}-worker`,
		"kind: StatefulSet", "minReplicas:", "maxReplicas:", "metrics:", "behavior:",
	} {
		if !strings.Contains(hpa, required) {
			t.Fatalf("HPA template is missing %q", required)
		}
	}
	if strings.Contains(hpa, `name: {{ include "sluice.fullname" . }}\n`) {
		t.Fatal("HPA may not target the control/Raft StatefulSet")
	}

	workerData, err := os.ReadFile("../templates/worker-statefulset.yaml")
	if err != nil {
		t.Fatal(err)
	}
	worker := string(workerData)
	if !strings.Contains(worker, `if not .Values.worker.autoscaling.enabled`) {
		t.Fatal("Worker StatefulSet replicas must be omitted while HPA owns the scale subresource")
	}
	controlData, err := os.ReadFile("../templates/statefulset.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(controlData), "autoscaling") {
		t.Fatal("control/Raft StatefulSet must never be an HPA target")
	}
}

func TestWorkloadAutoscalingTargetsOnlyStatelessStatefulSet(t *testing.T) {
	data, err := os.ReadFile("../templates/workload-autoscaler.yaml")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{
		`eq (default "workload" .Values.worker.autoscaling.mode) "workload"`,
		`/usr/local/bin/sluice-autoscaler`,
		`--statefulset={{ include "sluice.fullname" . }}-worker`,
		`--sluice-service={{ include "sluice.fullname" . }}`,
		`--target-backlog-per-pod=`,
		`--target-worker-utilization=`,
		`resources: ["statefulsets"]`,
		`verbs: ["get", "list", "watch", "patch", "update"]`,
		`resources: ["services"]`,
		`verbs: ["get"]`,
		`resources: ["leases"]`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("workload autoscaler template is missing %q", required)
		}
	}
	if strings.Contains(source, `--statefulset={{ include "sluice.fullname" . }}"`) {
		t.Fatal("workload autoscaler may not target the control/Raft StatefulSet")
	}
	hpaData, err := os.ReadFile("../templates/hpa.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(
		string(hpaData),
		`eq (default "workload" .Values.worker.autoscaling.mode) "hpa"`,
	) {
		t.Fatal("native HPA and workload autoscaler modes are not mutually exclusive")
	}
}

func TestRemoteDeployWaitsForWorkloadAutoscalerMinimum(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/deploy-remote.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	wait := strings.Index(source, "Waiting for workload autoscaler minimum Worker capacity")
	verify := strings.Index(source, "Verifying control and Worker topology")
	for _, required := range []string{
		`worker_desired="$(microk8s kubectl get`,
		`worker_ready="$(microk8s kubectl get`,
		`if [ "${worker_desired}" -ge 50 ] && [ "${worker_ready}" -ge 50 ]`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("remote deployment is missing autoscaler convergence check %q", required)
		}
	}
	if wait < 0 || verify < 0 || wait >= verify {
		t.Fatal("minimum Worker capacity must converge before topology verification")
	}
}

func TestRemoteTopologyValidationAcceptsAutoscaledWorkerRange(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/deploy-remote.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{
		`[ "${worker_count}" -lt 50 ]`,
		`[ "${worker_count}" -gt 100 ]`,
		`worker_capacity="$((worker_count * 100))"`,
		`--controls 5 --workers "${worker_count}" --worker-capacity "${worker_capacity}"`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("remote topology gate is not autoscaling-aware: missing %q", required)
		}
	}
	if strings.Contains(source, `--controls 5 --workers 50 --worker-capacity 5000`) {
		t.Fatal("remote topology gate still requires the autoscaler to remain at its minimum")
	}
}

func TestRemoteRaftMigrationSelectsOnlyControlPods(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/deploy-remote.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	start := strings.Index(source, "Migrating existing Raft members")
	end := strings.Index(source, "Upgrading Helm release")
	if start < 0 || end <= start {
		t.Fatal("cannot locate remote Raft migration block")
	}
	migration := source[start:end]
	selector := `app.kubernetes.io/component=control`
	if strings.Count(migration, selector) < 2 {
		t.Fatalf("Raft migration selectors do not consistently require %q", selector)
	}
	if strings.Contains(migration, "worker-autoscaler") {
		t.Fatal("Raft migration must not name or select the workload autoscaler")
	}
}

func TestAutoscalingDefaultsProtectWorkerDrainAndReactToBacklog(t *testing.T) {
	data, err := os.ReadFile("../values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	values := string(data)
	for _, required := range []string{
		"autoscaling:", "enabled: false", "minReplicas: 5", "maxReplicas: 100",
		"mode: workload", "targetBacklogPerPod: 400", "targetWorkerUtilization: 70",
		"scaleUpPods: 10",
		"averageUtilization: 70", "stabilizationWindowSeconds: 300",
	} {
		if !strings.Contains(values, required) {
			t.Fatalf("autoscaling defaults are missing %q", required)
		}
	}
}

func TestChartAndStandaloneCRDsExposeWorkerAutoscaling(t *testing.T) {
	for _, path := range []string{"../templates/crd.yaml", "../../../config/crd/sluicecluster.yaml"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		source := string(data)
		for _, required := range []string{
			"workerReplicas:", "autoscaling:", "minReplicas:", "maxReplicas:",
			"enum: [hpa, workload]", "targetBacklogPerPod:",
			"targetWorkerUtilization:", "scaleDownStabilizationSeconds:",
			"x-kubernetes-preserve-unknown-fields: true", "desiredWorkerReplicas:",
		} {
			if !strings.Contains(source, required) {
				t.Fatalf("%s is missing CRD autoscaling field %q", path, required)
			}
		}
	}
}

func TestOptionalOperatorCanManageWorkerHPA(t *testing.T) {
	data, err := os.ReadFile("../templates/operator.yaml")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{
		".Values.operator.enabled", "/usr/local/bin/sluice-operator",
		`apiGroups: ["autoscaling"]`, `resources: ["horizontalpodautoscalers"]`,
		`apiGroups: ["apps"]`, `resources: ["statefulsets"]`,
		`apiGroups: ["coordination.k8s.io"]`, `resources: ["leases"]`,
		`--leader-elect=true`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("operator template is missing %q", required)
		}
	}
	dockerfile, err := os.ReadFile("../../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(dockerfile), "/usr/local/bin/sluice-operator") {
		t.Fatal("runtime image does not contain the CRD operator binary")
	}
	if !strings.Contains(string(dockerfile), "/usr/local/bin/sluice-autoscaler") {
		t.Fatal("runtime image does not contain the workload autoscaler binary")
	}
}
