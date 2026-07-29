package charttest

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestRemoteTopologyValidationAllowsHPAReplicaHistory(t *testing.T) {
	type node struct {
		NodeID       string `json:"node_id"`
		Role         string `json:"role"`
		Status       string `json:"status"`
		TotalWorkers int    `json:"total_workers"`
	}
	type allocation struct {
		NodeID  string         `json:"node_id"`
		Tenants map[string]int `json:"tenants,omitempty"`
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
		tempDir := t.TempDir()
		nodesPath := tempDir + "/nodes.json"
		allocationsPath := tempDir + "/allocations.json"
		if err := os.WriteFile(nodesPath, nodesJSON, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(allocationsPath, allocationsJSON, 0o600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("python3", "../../../scripts/validate-topology.py",
			"--nodes-file", nodesPath,
			"--allocations-file", allocationsPath,
			"--controls", "5", "--workers", "2", "--max-worker-capacity", "1000")
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
	t.Run("durable per-instance capacity override is authoritative", func(t *testing.T) {
		overriddenWorkers := append([]node{}, workers...)
		overriddenWorkers[0].TotalWorkers = 1000
		nodes := append(append([]node{}, controls...), overriddenWorkers...)
		run(t, nodes, []allocation{
			{NodeID: "worker-0", Tenants: map[string]int{"large": 1000}},
			{NodeID: "worker-1", Tenants: map[string]int{"small": 100}},
		}, true)
	})
	t.Run("allocation cannot exceed configured instance capacity", func(t *testing.T) {
		nodes := append(append([]node{}, controls...), workers...)
		run(t, nodes, []allocation{
			{NodeID: "worker-1", Tenants: map[string]int{"overflow": 101}},
		}, false)
	})
	t.Run("large allocation snapshot is read from files", func(t *testing.T) {
		tenants := make(map[string]int, 120000)
		for index := 0; index < 120000; index++ {
			tenants[fmt.Sprintf("tenant-%06d-with-a-long-stable-name", index)] = 0
		}
		nodes := append(append([]node{}, controls...), workers...)
		run(t, nodes, []allocation{
			{NodeID: "worker-0", Tenants: tenants},
			{NodeID: "worker-1"},
		}, true)
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

func TestControlEntrypointConfiguresBoundedSubmissionRaftIngress(t *testing.T) {
	type values struct {
		Control struct {
			SubmitApplyLimit int `json:"submitApplyLimit"`
		} `json:"control"`
	}
	valuesData, err := os.ReadFile("../values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var chartValues values
	if err := yaml.Unmarshal(valuesData, &chartValues); err != nil {
		t.Fatal(err)
	}
	if chartValues.Control.SubmitApplyLimit != 16 {
		t.Fatalf(
			"default control submitApplyLimit = %d, want 16",
			chartValues.Control.SubmitApplyLimit,
		)
	}

	configData, err := os.ReadFile("../templates/configmap.yaml")
	if err != nil {
		t.Fatal(err)
	}
	argument := `--submit-apply-limit={{ .Values.control.submitApplyLimit }}`
	if got := strings.Count(string(configData), argument); got != 3 {
		t.Fatalf(
			"control entrypoint submission limit occurrences = %d, want bootstrap/restart/join",
			got,
		)
	}
}

func TestDedicatedLoadGeneratorIsSingleStatelessPodOutsideRaft(t *testing.T) {
	type values struct {
		LoadGenerator struct {
			Enabled  bool `json:"enabled"`
			Replicas int  `json:"replicas"`
			APIPort  int  `json:"apiPort"`
		} `json:"loadGenerator"`
	}
	valuesData, err := os.ReadFile("../values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var chartValues values
	if err := yaml.Unmarshal(valuesData, &chartValues); err != nil {
		t.Fatal(err)
	}
	if !chartValues.LoadGenerator.Enabled ||
		chartValues.LoadGenerator.Replicas != 1 ||
		chartValues.LoadGenerator.APIPort != 9091 {
		t.Fatalf("loadGenerator defaults = %+v", chartValues.LoadGenerator)
	}

	deploymentData, err := os.ReadFile("../templates/load-generator.yaml")
	if err != nil {
		t.Fatal(err)
	}
	deployment := string(deploymentData)
	for _, required := range []string{
		"kind: Service",
		"kind: Deployment",
		"app.kubernetes.io/component: load-generator",
		`loadGenerator.replicas must be exactly 1`,
		`--role=loadgen`,
		`--controller="${CONTROLLER_IP}:`,
		"resolve_service_ip",
		"path: /api/v1/health",
	} {
		if !strings.Contains(deployment, required) {
			t.Fatalf("Load Generator template is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"kind: StatefulSet",
		"volumeClaimTemplates:",
		"--raft=",
		"--join=",
		"--bootstrap",
	} {
		if strings.Contains(deployment, forbidden) {
			t.Fatalf("stateless Load Generator contains %q", forbidden)
		}
	}

	configData, err := os.ReadFile("../templates/configmap.yaml")
	if err != nil {
		t.Fatal(err)
	}
	config := string(configData)
	if strings.Count(
		config,
		`--load-generator=http://${LOAD_GENERATOR_IP}:{{ .Values.loadGenerator.apiPort }}`,
	) != 1 ||
		strings.Count(config, `${LOAD_GENERATOR_ARG}`) != 3 {
		t.Fatal("bootstrap, restart and join control paths do not share one Load Generator proxy target")
	}

	deployData, err := os.ReadFile("../../../scripts/deploy-remote.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(
		string(deployData),
		`rollout status "deployment/${RELEASE}-sluice-load-generator"`,
	) {
		t.Fatal("remote deployment does not wait for the Load Generator Pod")
	}
}

func TestControlStatefulSetCanRecoverAllRaftVotersInParallel(t *testing.T) {
	data, err := os.ReadFile("../templates/statefulset.yaml")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{
		"podManagementPolicy: Parallel",
		"livenessProbe:",
		"tcpSocket:",
		"port: {{ .Values.raftPort }}",
		"readinessProbe:",
		"path: /api/v1/health",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("control StatefulSet recovery contract is missing %q", required)
		}
	}
	if strings.Contains(source, "podManagementPolicy: OrderedReady") {
		t.Fatal("persisted Raft voters can still deadlock behind OrderedReady")
	}
}

func TestControlAndWorkerResourceBudgetsAreIndependent(t *testing.T) {
	type resources struct {
		Requests map[string]string `json:"requests"`
		Limits   map[string]string `json:"limits"`
	}
	type values struct {
		Control struct {
			Resources resources `json:"resources"`
		} `json:"control"`
		Worker struct {
			Resources resources `json:"resources"`
		} `json:"worker"`
		Resources *resources `json:"resources"`
	}

	valuesData, err := os.ReadFile("../values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var chartValues values
	if err := yaml.Unmarshal(valuesData, &chartValues); err != nil {
		t.Fatal(err)
	}
	if chartValues.Resources != nil {
		t.Fatal("control and Worker still share one top-level resource budget")
	}
	if got := chartValues.Control.Resources.Limits["memory"]; got != "2Gi" {
		t.Fatalf("control memory limit = %q, want 2Gi for large FSM recovery", got)
	}
	if got := chartValues.Worker.Resources.Limits["memory"]; got != "512Mi" {
		t.Fatalf("Worker memory limit = %q, want independent 512Mi budget", got)
	}
	if chartValues.Control.Resources.Limits["memory"] ==
		chartValues.Worker.Resources.Limits["memory"] {
		t.Fatal("control and Worker memory limits are not independently sized")
	}

	controlData, err := os.ReadFile("../templates/statefulset.yaml")
	if err != nil {
		t.Fatal(err)
	}
	workerData, err := os.ReadFile("../templates/worker-statefulset.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(controlData), ".Values.control.resources") ||
		strings.Contains(string(controlData), ".Values.worker.resources") {
		t.Fatal("control StatefulSet does not use only the control resource budget")
	}
	if !strings.Contains(string(workerData), ".Values.worker.resources") ||
		strings.Contains(string(workerData), ".Values.control.resources") {
		t.Fatal("Worker StatefulSet does not use only the Worker resource budget")
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
		`--target-cpu-utilization=`,
		`--target-queue-drain=`,
		`--target-throughput-utilization=`,
		`--min-rate-utilization-percent=`,
		`--tolerance-percent=`,
		`--min-telemetry-coverage-percent=`,
		`targetCPUUtilization must be between 1 and 100`,
		`targetQueueDrainSeconds must be positive`,
		`targetThroughputUtilization must be between 1 and 100`,
		`minRateUtilizationPercent must be between 1 and 100`,
		`tolerancePercent must be between 0 and 100`,
		`minTelemetryCoveragePercent must be between 1 and 100`,
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
		`WORKER_MIN_REPLICAS="${WORKER_MIN_REPLICAS:-5}"`,
		`WORKER_SCALE_DOWN_STABILIZATION_SECONDS="${WORKER_SCALE_DOWN_STABILIZATION_SECONDS:-60}"`,
		`--set worker.autoscaling.minReplicas="${WORKER_MIN_REPLICAS}"`,
		`--set worker.autoscaling.workload.scaleDownStabilizationSeconds="${WORKER_SCALE_DOWN_STABILIZATION_SECONDS}"`,
		`worker_desired="$(microk8s kubectl get`,
		`worker_ready="$(microk8s kubectl get`,
		`if [ "${worker_desired}" -ge "${WORKER_MIN_REPLICAS}" ] &&`,
		`[ "${worker_ready}" -ge "${WORKER_MIN_REPLICAS}" ]`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("remote deployment is missing autoscaler convergence check %q", required)
		}
	}
	if wait < 0 || verify < 0 || wait >= verify {
		t.Fatal("minimum Worker capacity must converge before topology verification")
	}
	if strings.Contains(source, `--set worker.autoscaling.minReplicas=50`) ||
		strings.Contains(source, `worker_desired}" -ge 50`) {
		t.Fatal("remote deployment still pins an idle Worker pool to its 50-Pod rollout size")
	}
	if strings.Contains(
		source,
		`--set worker.autoscaling.scaleDownStabilizationSeconds=`,
	) {
		t.Fatal("remote deployment writes scale-down stabilization outside the workload config")
	}
}

func TestRemoteDeployMigratesControlPolicyWithoutDeletingPVCs(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/deploy-remote.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	migration := strings.Index(source, "Migrating control StatefulSet to parallel Raft recovery")
	upgrade := strings.Index(source, "Upgrading Helm release")
	for _, required := range []string{
		`control_policy="$(microk8s kubectl get statefulset`,
		`[ "${control_policy}" != "Parallel" ]`,
		`--cascade=orphan --wait=true`,
		`control_pods_before=`,
		`control_pods_after=`,
		`Control policy migration did not preserve Pods`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("remote control policy migration is missing %q", required)
		}
	}
	if migration < 0 || upgrade < 0 || migration >= upgrade {
		t.Fatal("immutable control policy must migrate before Helm upgrade")
	}
	if strings.Contains(source, `kubectl delete pvc`) ||
		strings.Contains(source, `--cascade=foreground`) {
		t.Fatal("control policy migration may not delete Raft PVCs or cascade to Pods")
	}
}

func TestRemoteDeployServerDryRunIsolatedFromLiveImmutableControl(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/deploy-remote.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{
		`VALIDATION_RELEASE="${RELEASE}-validation"`,
		`microk8s helm3 template "${VALIDATION_RELEASE}"`,
		`microk8s kubectl apply --dry-run=server`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("remote server-side validation is missing %q", required)
		}
	}
	if got := strings.Count(
		source,
		`microk8s helm3 template "${VALIDATION_RELEASE}"`,
	); got != 3 {
		t.Fatalf("isolated validation renders = %d, want all 3 chart variants", got)
	}
	if strings.Contains(source, `microk8s helm3 template "${RELEASE}"`) {
		t.Fatal("server-side preflight can still merge into the live immutable release")
	}
	validation := strings.Index(source, "Validating Helm role split")
	migration := strings.Index(source, "Migrating control StatefulSet to parallel Raft recovery")
	if validation < 0 || migration < 0 || validation >= migration {
		t.Fatal("isolated preflight must finish before mutating the live controller")
	}
}

func TestRemoteDeployRecreatesOrphanedControlPodsBeforeHelmUpgrade(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/deploy-remote.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{
		`StatefulSet RollingUpdate still waits for the highest old Pod`,
		`control_pvcs_before=`,
		`microk8s kubectl delete pods`,
		`app.kubernetes.io/component=control`,
		`control_pods_remaining=`,
		`[ "${control_pods_remaining}" -ne 0 ]`,
		`control_pvcs_after=`,
		`[ "${control_pvcs_after}" -ne "${control_pvcs_before}" ]`,
		`Control Pod recreation did not preserve PVCs`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("remote persisted-voter recreation is missing %q", required)
		}
	}
	orphan := strings.Index(source, `--cascade=orphan --wait=true`)
	recreateOffset := -1
	recreate := -1
	if orphan >= 0 {
		recreateOffset = strings.Index(source[orphan:], `microk8s kubectl delete pods`)
		if recreateOffset >= 0 {
			recreate = orphan + recreateOffset
		}
	}
	upgrade := strings.Index(source, "Upgrading Helm release")
	if orphan < 0 || recreateOffset < 0 || upgrade < 0 ||
		orphan >= recreate || recreate >= upgrade {
		t.Fatal("old control Pods must be orphaned, removed, then recreated by Helm")
	}
	if strings.Contains(source, `kubectl delete pvc`) {
		t.Fatal("persisted-voter recreation may not delete Raft PVCs")
	}
}

func TestRemoteDeployRejectsControlOOMAndRestartChurn(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/deploy-remote.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{
		`CONTROL_STABILITY_SECONDS="${CONTROL_STABILITY_SECONDS:-30}"`,
		`Verifying control recovery remains stable under persisted state`,
		`control_restarts_before=`,
		`control_restarts_now=`,
		`control_last_reasons=`,
		`grep -q '=OOMKilled$'`,
		`Control recovery was OOMKilled under the persisted FSM`,
		`[ "${control_ready}" -ne 5 ]`,
		`[ "${control_restarts_now}" != "${control_restarts_before}" ]`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("remote control recovery gate is missing %q", required)
		}
	}
	stability := strings.Index(source, "Verifying control recovery remains stable")
	topology := strings.Index(source, "Verifying control and Worker topology")
	if stability < 0 || topology < 0 || stability >= topology {
		t.Fatal("control stability must pass before final topology acceptance")
	}
}

func TestRemoteDeployRecreatesOOMLimitedParallelVotersBeforeUpgrade(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/deploy-remote.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{
		`CONTROL_MEMORY_LIMIT="${CONTROL_MEMORY_LIMIT:-2Gi}"`,
		`control_oom_count=`,
		`grep -c '^OOMKilled$'`,
		`[ "${control_policy}" = "Parallel" ]`,
		`Recovering OOM-limited persisted voters in parallel`,
		`kubectl set resources statefulset "${STATEFULSET}"`,
		`--limits="cpu=${CONTROL_CPU_LIMIT},memory=${CONTROL_MEMORY_LIMIT}"`,
		`kubectl delete pods`,
		`rollout status "statefulset/${STATEFULSET}"`,
		`--set control.resources.limits.memory="${CONTROL_MEMORY_LIMIT}"`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("remote OOM voter recovery is missing %q", required)
		}
	}
	recovery := strings.Index(source, "Recovering OOM-limited persisted voters in parallel")
	migration := strings.Index(source, "Migrating existing Raft members")
	upgrade := strings.Index(source, "Upgrading Helm release")
	if recovery < 0 || migration < 0 || upgrade < 0 ||
		recovery >= migration || migration >= upgrade {
		t.Fatal("OOM-limited voters must recover before Raft migration and Helm upgrade")
	}
}

func TestRemoteDeployReservesControlRolloutPodHeadroom(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/deploy-remote.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{
		`WORKER_MAX_REPLICAS="${WORKER_MAX_REPLICAS:-90}"`,
		`Reserving control-plane rollout capacity`,
		`kubectl scale deployment "${RELEASE}-sluice-worker-autoscaler"`,
		`--namespace "${NAMESPACE}" --replicas=0`,
		`kubectl scale statefulset "${WORKER_STATEFULSET}"`,
		`--namespace "${NAMESPACE}" --replicas="${WORKER_MIN_REPLICAS}"`,
		`worker_pods_remaining=`,
		`[ "${worker_pods_remaining}" -le "${WORKER_MIN_REPLICAS}" ]`,
		`Worker scale-down did not reserve control rollout capacity`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("remote rollout headroom gate is missing %q", required)
		}
	}
	reserve := strings.Index(source, "Reserving control-plane rollout capacity")
	migration := strings.Index(source, "Migrating existing Raft members")
	upgrade := strings.Index(source, "Upgrading Helm release")
	if reserve < 0 || migration < 0 || upgrade < 0 ||
		reserve >= migration || migration >= upgrade {
		t.Fatal("Worker rollout headroom must be reserved before Raft migration and Helm mutation")
	}
}

func TestRemoteTopologyValidationAcceptsAutoscaledWorkerRange(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/verify-deployed-topology.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	for _, required := range []string{
		`[ "${last_worker_ready}" -ge "${MIN_WORKERS}" ]`,
		`[ "${last_worker_ready}" -le "${MAX_WORKERS}" ]`,
		`MAX_WORKERS_PER_POD="${6:-1000}"`,
		`worker_capacity="$(cat "${validation_log}")"`,
		`--workers "${last_worker_ready}"`,
		`--max-worker-capacity "${MAX_WORKERS_PER_POD}"`,
		`--nodes-file "${nodes_snapshot}"`,
		`--allocations-file "${allocations_snapshot}"`,
		`--scale-down-stabilization=${EXPECTED_SCALE_DOWN_STABILIZATION_SECONDS}s`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("remote topology gate is not autoscaling-aware: missing %q", required)
		}
	}
	if strings.Contains(source, `--controls 5 --workers 50 --worker-capacity 5000`) {
		t.Fatal("remote topology gate still requires the autoscaler to remain at its minimum")
	}
	if strings.Contains(source, `last_worker_ready * WORKERS_PER_POD`) {
		t.Fatal("remote topology gate still overwrites durable per-instance capacity overrides")
	}
	if strings.Contains(source, `NODES_JSON="${last_nodes_json}"`) ||
		strings.Contains(source, `ALLOCATIONS_JSON="${last_allocations_json}"`) {
		t.Fatal("remote topology gate still passes unbounded snapshots through exec environment")
	}
}

func TestRemoteTopologyValidationRetriesConcurrentScaleDown(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/verify-deployed-topology.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	loop := strings.Index(source, `for _ in $(seq 1 "${VERIFY_ATTEMPTS}")`)
	workerRead := strings.Index(source, `last_worker_ready="$("${MICROK8S_BIN}" kubectl get`)
	topologyRead := strings.Index(
		source,
		`wget -qO- 'http://127.0.0.1:9090/api/v1/admin/nodes'`,
	)
	for _, required := range []string{
		"Never cache", "statefulset/${WORKER_STATEFULSET}",
		`Topology not yet converged`, `python3 "$(dirname "$0")/validate-topology.py"`,
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("concurrent scale-down verification is missing %q", required)
		}
	}
	if loop < 0 || workerRead < loop || topologyRead < workerRead {
		t.Fatal("each topology retry must re-read Worker Ready count before the FSM snapshot")
	}
	if strings.Contains(source, `for pod in ${pods}`) {
		t.Fatal("topology verification still iterates a stale autoscaled Pod list")
	}

	deployData, err := os.ReadFile("../../../scripts/deploy-remote.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(
		string(deployData),
		`./scripts/verify-deployed-topology.sh`,
	) {
		t.Fatal("remote deployment does not use the scale-safe topology verifier")
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
		"targetCPUUtilization: 70", "targetQueueDrainSeconds: 30",
		"targetThroughputUtilization: 80", "minRateUtilizationPercent: 50",
		"tolerancePercent: 10",
		"minTelemetryCoveragePercent: 80", "scaleUpPods: 10",
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
			"targetWorkerUtilization:", "targetCPUUtilization:",
			"targetQueueDrainSeconds:", "targetThroughputUtilization:",
			"minRateUtilizationPercent:", "tolerancePercent:",
			"minTelemetryCoveragePercent:",
			"scaleDownStabilizationSeconds:",
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
