package charttest

import (
	"os"
	"strings"
	"testing"
)

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
