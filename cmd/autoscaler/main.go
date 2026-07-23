// Command autoscaler runs the workload-aware Worker replica controller.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/day253/sluice/internal/autoscaler"
)

func main() {
	defaults := autoscaler.DefaultConfig()
	var (
		namespace, statefulSet, sluiceURL, sluiceService, probeAddress string
		interval, scaleDownStabilization, targetQueueDrain             time.Duration
		minReplicas, maxReplicas, workersPerPod, sluicePort            int
		targetWorkerUtilization, targetCPUUtilization                  int
		targetThroughputUtilization, tolerancePercent                  int
		minTelemetryCoveragePercent                                    int
		scaleUpPercent, scaleUpPods, scaleDownPercent                  int
		config                                                         = defaults
	)
	flag.StringVar(&namespace, "namespace", "default", "Namespace containing the Worker StatefulSet")
	flag.StringVar(&statefulSet, "statefulset", "", "Worker StatefulSet name")
	flag.StringVar(&sluiceURL, "sluice-url", "", "Fallback Sluice control URL")
	flag.StringVar(&sluiceService, "sluice-service", "", "Sluice control Kubernetes Service name")
	flag.IntVar(&sluicePort, "sluice-port", 9090, "Sluice control Service API port")
	flag.StringVar(&probeAddress, "health-probe-bind-address", ":8081", "Health probe endpoint")
	flag.DurationVar(&interval, "interval", 5*time.Second, "Workload signal polling interval")
	flag.IntVar(&minReplicas, "min-replicas", int(defaults.MinReplicas), "Minimum Worker replicas")
	flag.IntVar(&maxReplicas, "max-replicas", int(defaults.MaxReplicas), "Maximum Worker replicas")
	flag.IntVar(&workersPerPod, "workers-per-pod", int(defaults.WorkersPerPod), "Processor slots per Worker Pod")
	flag.Int64Var(&config.TargetBacklogPerPod, "target-backlog-per-pod", defaults.TargetBacklogPerPod, "Target unfinished tasks per Worker Pod")
	flag.IntVar(&targetWorkerUtilization, "target-worker-utilization", int(defaults.TargetWorkerUtilization), "Target executing Processor-slot percentage")
	flag.IntVar(&targetCPUUtilization, "target-cpu-utilization", int(defaults.TargetCPUUtilization), "Target average Worker process/container CPU percentage")
	flag.DurationVar(&targetQueueDrain, "target-queue-drain", defaults.TargetQueueDrainTime, "Target time to drain the current pending queue")
	flag.IntVar(&targetThroughputUtilization, "target-throughput-utilization", int(defaults.TargetThroughputUtilization), "Target fraction of measured completion throughput reserved for steady arrivals")
	flag.IntVar(&tolerancePercent, "tolerance-percent", int(defaults.TolerancePercent), "Replica recommendation deadband percentage")
	flag.IntVar(&minTelemetryCoveragePercent, "min-telemetry-coverage-percent", int(defaults.MinTelemetryCoveragePercent), "Minimum reporting Worker percentage required before scale-down")
	flag.IntVar(&scaleUpPercent, "scale-up-percent", int(defaults.ScaleUpPercent), "Maximum scale-up percentage per interval")
	flag.IntVar(&scaleUpPods, "scale-up-pods", int(defaults.ScaleUpPods), "Maximum absolute scale-up Pods per interval")
	flag.IntVar(&scaleDownPercent, "scale-down-percent", int(defaults.ScaleDownPercent), "Maximum scale-down percentage per period")
	flag.DurationVar(&scaleDownStabilization, "scale-down-stabilization", defaults.ScaleDownStabilization, "Continuous low-load window before scale-down")
	options := zap.Options{Development: false}
	options.BindFlags(flag.CommandLine)
	flag.Parse()
	config.MinReplicas, config.MaxReplicas = int32(minReplicas), int32(maxReplicas)
	config.WorkersPerPod = int32(workersPerPod)
	config.TargetWorkerUtilization = int32(targetWorkerUtilization)
	config.TargetCPUUtilization = int32(targetCPUUtilization)
	config.TargetQueueDrainTime = targetQueueDrain
	config.TargetThroughputUtilization = int32(targetThroughputUtilization)
	config.TolerancePercent = int32(tolerancePercent)
	config.MinTelemetryCoveragePercent = int32(minTelemetryCoveragePercent)
	config.ScaleUpPercent, config.ScaleUpPods = int32(scaleUpPercent), int32(scaleUpPods)
	config.ScaleDownPercent = int32(scaleDownPercent)
	config.ScaleUpPeriod = interval
	config.ScaleDownStabilization = scaleDownStabilization

	if statefulSet == "" || (sluiceURL == "" && sluiceService == "") {
		fmt.Fprintln(os.Stderr, "--statefulset and one of --sluice-url/--sluice-service are required")
		os.Exit(2)
	}
	if err := config.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&options)))
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(appsv1.AddToScheme(scheme))
	manager, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme, HealthProbeBindAddress: probeAddress,
		LeaderElection: true, LeaderElectionNamespace: namespace,
		LeaderElectionID:              statefulSet + ".autoscaler.sluice.day253.github.com",
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := manager.Add(&autoscaler.Runner{
		Client: manager.GetClient(), Namespace: namespace, StatefulSet: statefulSet,
		SluiceURL: sluiceURL, SluiceService: sluiceService, SluicePort: int32(sluicePort),
		Interval: interval, Policy: autoscaler.Policy{Config: config},
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
