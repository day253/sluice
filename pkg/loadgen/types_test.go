package loadgen

import (
	"strings"
	"testing"
)

func TestNormalizeStartRequestBuildsBoundedRoundRobinServerWork(t *testing.T) {
	normalized, err := normalizeStartRequest(StartRequest{
		Name:      "Hundred tenants",
		Recipe:    "regression",
		Operation: "load",
		Options: Options{
			TenantCount:    100,
			TasksPerTenant: 2,
			Quota:          50,
			QuotaProfile:   "tiered",
			LoadShape:      "even",
			Delivery:       "waves",
			Waves:          3,
			SubmissionMode: "16",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !normalized.generatedPool || len(normalized.specs) != 100 ||
		normalized.totalTasks != 200 {
		t.Fatalf("normalized request = %+v", normalized)
	}
	if normalized.specs[0].ID != "load-lab-001" ||
		normalized.specs[99].ID != "load-lab-100" ||
		normalized.specs[0].MaxWorkers != 5 ||
		normalized.specs[1].MaxWorkers != 20 {
		t.Fatalf(
			"stable generated pool = first %+v second %+v last %+v",
			normalized.specs[0], normalized.specs[1], normalized.specs[99],
		)
	}
	jobs := buildRoundRobinJobs(normalized.specs)
	seen := make(map[string]bool, 100)
	for _, item := range jobs[:100] {
		if item.index != 0 || seen[item.tenantID] {
			t.Fatalf("first round is not one task per tenant: %+v", jobs[:100])
		}
		seen[item.tenantID] = true
	}
	waves := splitWaves(jobs, normalized.request.Options.Waves)
	if len(waves) != 3 ||
		len(waves[0])+len(waves[1])+len(waves[2]) != normalized.totalTasks {
		t.Fatalf("wave sizes = %d/%d/%d", len(waves[0]), len(waves[1]), len(waves[2]))
	}
}

func TestNormalizeStartRequestRejectsPerTenantAndTotalTaskExpansionAbovePodLimit(t *testing.T) {
	_, err := normalizeStartRequest(StartRequest{
		Operation: "load",
		Options: Options{
			TenantCount:    100,
			TasksPerTenant: 100,
			LoadShape:      "hotspot",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "per-tenant maximum is 5000") {
		t.Fatalf("oversized tenant error = %v", err)
	}
	_, err = normalizeStartRequest(StartRequest{
		Operation: "load",
		Options: Options{
			TenantCount:    100,
			TasksPerTenant: 2000,
			LoadShape:      "even",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "maximum is 100000") {
		t.Fatalf("oversized total load error = %v", err)
	}
}

func TestNormalizeStartRequestDeduplicatesAndBoundsExistingTenants(t *testing.T) {
	normalized, err := normalizeStartRequest(StartRequest{
		Operation: "load",
		TenantIDs: []string{" globex ", "acme", "globex"},
		Options: Options{
			TasksPerTenant: 3,
			SubmissionMode: "999",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if normalized.generatedPool ||
		len(normalized.specs) != 2 ||
		normalized.specs[0].ID != "globex" ||
		normalized.specs[1].ID != "acme" ||
		normalized.totalTasks != 6 ||
		normalized.request.Options.SubmissionMode != "auto" {
		t.Fatalf("existing tenant request = %+v", normalized)
	}
}
