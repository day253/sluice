package integration

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestHelmEntrypointRetriesInsteadOfStartingUnjoinedMember(t *testing.T) {
	t.Parallel()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test source")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), "../.."))
	template, err := os.ReadFile(
		filepath.Join(repositoryRoot, "charts/sluice/templates/configmap.yaml"),
	)
	if err != nil {
		t.Fatal(err)
	}

	entrypoint := controlEntrypointFromHelmTemplate(t, string(template))
	tempDir := t.TempDir()
	tracePath := filepath.Join(tempDir, "sluice.trace")
	writeExecutable(t, filepath.Join(tempDir, "cat"), `#!/bin/sh
case "$1" in
  */namespace) printf '%s\n' default ;;
  */token) printf '%s\n' test-token ;;
  *) /bin/cat "$@" ;;
esac
`)
	writeExecutable(t, filepath.Join(tempDir, "wget"), `#!/bin/sh
case "$*" in
  *"/services/"*)
    printf '%s\n' '{"spec":{"clusterIP":"10.0.0.10"}}'
    ;;
  *"/api/v1/health"*)
    if [ "${FAKE_LEADER_READY:-false}" = "true" ]; then
      printf '%s\n' '{"leader":"10.0.0.1:7000"}'
    else
      exit 1
    fi
    ;;
  *"/api/v1/cluster/join"*)
    printf '%s\n' '{"ok":true}'
    ;;
  *)
    exit 1
    ;;
esac
`)
	writeExecutable(t, filepath.Join(tempDir, "sluice"), `#!/bin/sh
printf '%s\n' "$*" >> "${SLUICE_TRACE}"
`)
	scriptPath := filepath.Join(tempDir, "entrypoint.sh")
	if err := os.WriteFile(scriptPath, []byte(entrypoint), 0o755); err != nil {
		t.Fatal(err)
	}

	baseEnvironment := append(os.Environ(),
		"PATH="+tempDir+":"+os.Getenv("PATH"),
		"POD_NAME=join-case-sluice-1",
		"POD_NAMESPACE=default",
		"KUBERNETES_TOKEN=test-token",
		"KUBERNETES_SERVICE_HOST=127.0.0.1",
		"KUBERNETES_SERVICE_PORT=443",
		"SLUICE_JOIN_ATTEMPTS=3",
		"SLUICE_JOIN_RETRY_DELAY=0",
		"SLUICE_TRACE="+tracePath,
	)

	failed := exec.Command("/bin/sh", scriptPath)
	failed.Env = append(baseEnvironment, "FAKE_LEADER_READY=false")
	failedOutput, failedErr := failed.CombinedOutput()
	if failedErr == nil {
		t.Fatalf("entrypoint started without joining:\n%s", failedOutput)
	}
	if !strings.Contains(string(failedOutput), "refusing to start an unjoined member") {
		t.Fatalf("entrypoint did not report join refusal:\n%s", failedOutput)
	}
	if _, err := os.Stat(tracePath); !os.IsNotExist(err) {
		t.Fatalf("sluice process was started before join succeeded: %v", err)
	}

	succeeded := exec.Command("/bin/sh", scriptPath)
	succeeded.Env = append(baseEnvironment, "FAKE_LEADER_READY=true")
	succeededOutput, succeededErr := succeeded.CombinedOutput()
	if succeededErr != nil {
		t.Fatalf("entrypoint did not start after a successful join: %v\n%s", succeededErr, succeededOutput)
	}
	trace, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"--id=join-case-sluice-1",
		"--role=control",
		"--workers=0",
	} {
		if !bytes.Contains(trace, []byte(required)) {
			t.Fatalf("started control args %q do not contain %q", trace, required)
		}
	}
}

func controlEntrypointFromHelmTemplate(t *testing.T, template string) string {
	t.Helper()
	const startMarker = "  entrypoint.sh: |\n"
	start := strings.Index(template, startMarker)
	if start < 0 {
		t.Fatal("Helm control ConfigMap has no entrypoint.sh")
	}
	start += len(startMarker)
	endOffset := strings.Index(template[start:], "\n---")
	if endOffset < 0 {
		t.Fatal("Helm control entrypoint has no document boundary")
	}
	var script strings.Builder
	for _, line := range strings.Split(template[start:start+endOffset], "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "{{-") {
			continue
		}
		if strings.HasPrefix(line, "    ") {
			line = strings.TrimPrefix(line, "    ")
		}
		script.WriteString(line)
		script.WriteByte('\n')
	}
	return script.String()
}

func writeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
		t.Fatal(err)
	}
}
