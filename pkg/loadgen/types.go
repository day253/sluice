// Package loadgen runs bounded synthetic workloads from a dedicated process.
// It never owns Raft state: every accepted task still enters through the
// production tenant and task HTTP APIs.
package loadgen

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	MaxTenants        = 100
	MaxTasks          = 100000
	MaxTasksPerTenant = 5000
	MaxWaves          = 20
	DefaultBatchSize  = 1000
	tenantPrefix      = "load-lab-"
)

var (
	ErrRunActive   = errors.New("a load generation run is already active")
	ErrRunNotFound = errors.New("load generation run not found")
)

type Options struct {
	TenantCount    int    `json:"tenantCount"`
	TasksPerTenant int    `json:"tasksPerTenant"`
	Quota          int    `json:"quota"`
	QuotaProfile   string `json:"quotaProfile"`
	LoadShape      string `json:"loadShape"`
	Delivery       string `json:"delivery"`
	Waves          int    `json:"waves"`
	SubmissionMode string `json:"submissionMode"`
}

type StartRequest struct {
	Name      string   `json:"name"`
	Recipe    string   `json:"recipe"`
	Operation string   `json:"operation"`
	TenantIDs []string `json:"tenantIds,omitempty"`
	Options   Options  `json:"options"`
}

type TenantSpec struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	MaxWorkers int    `json:"maxWorkers"`
	TaskCount  int    `json:"taskCount"`
}

type Run struct {
	ID                       string       `json:"id"`
	Name                     string       `json:"name"`
	Recipe                   string       `json:"recipe"`
	Operation                string       `json:"operation"`
	Status                   string       `json:"status"`
	StartedAt                time.Time    `json:"startedAt"`
	SubmittedAt              *time.Time   `json:"submittedAt,omitempty"`
	EndedAt                  *time.Time   `json:"endedAt,omitempty"`
	TenantCount              int          `json:"tenantCount"`
	TotalTasks               int          `json:"totalTasks"`
	Prepared                 int          `json:"prepared"`
	Submitted                int          `json:"submitted"`
	Failed                   int          `json:"failed"`
	Remaining                int          `json:"remaining"`
	PeakBacklog              int          `json:"peakBacklog"`
	SubmissionMode           string       `json:"submissionMode"`
	SubmissionConcurrency    int          `json:"submissionConcurrency"`
	MaxSubmissionConcurrency int          `json:"maxSubmissionConcurrency"`
	SubmissionBackoffs       int          `json:"submissionBackoffs"`
	Options                  Options      `json:"options"`
	TenantSpecs              []TenantSpec `json:"tenantSpecs"`
	StopRequested            bool         `json:"stopRequested"`
	Message                  string       `json:"message"`
}

type normalizedRequest struct {
	request       StartRequest
	specs         []TenantSpec
	totalTasks    int
	generatedPool bool
}

func normalizeStartRequest(input StartRequest) (normalizedRequest, error) {
	input.Name = strings.TrimSpace(input.Name)
	if input.Name == "" {
		input.Name = "Synthetic workload"
	}
	input.Recipe = strings.TrimSpace(input.Recipe)
	if input.Recipe == "" {
		input.Recipe = "custom"
	}
	if input.Operation != "load" && input.Operation != "tenants" {
		return normalizedRequest{}, fmt.Errorf("operation must be load or tenants")
	}

	options := input.Options
	options.TenantCount = boundedDefault(options.TenantCount, 100, 1, MaxTenants)
	options.TasksPerTenant = boundedDefault(options.TasksPerTenant, 200, 1, MaxTasksPerTenant)
	options.Quota = boundedDefault(options.Quota, 50, 1, 100000)
	if !slices.Contains([]string{"equal", "tiered", "ramp"}, options.QuotaProfile) {
		options.QuotaProfile = "equal"
	}
	if !slices.Contains([]string{"even", "hotspot", "pyramid"}, options.LoadShape) {
		options.LoadShape = "even"
	}
	if options.Delivery != "waves" {
		options.Delivery = "burst"
	}
	options.Waves = boundedDefault(options.Waves, 5, 1, MaxWaves)
	if options.Delivery == "burst" {
		options.Waves = 1
	}
	options.SubmissionMode = normalizeSubmissionMode(options.SubmissionMode)
	input.Options = options

	tenantIDs, err := normalizeTenantIDs(input.TenantIDs)
	if err != nil {
		return normalizedRequest{}, err
	}
	input.TenantIDs = tenantIDs
	generatedPool := len(tenantIDs) == 0
	if !generatedPool && input.Operation == "tenants" {
		return normalizedRequest{}, fmt.Errorf("tenants operation cannot target existing tenant IDs")
	}

	var specs []TenantSpec
	if generatedPool {
		specs = make([]TenantSpec, options.TenantCount)
		for index := range specs {
			ordinal := index + 1
			specs[index] = TenantSpec{
				ID:         fmt.Sprintf("%s%03d", tenantPrefix, ordinal),
				Name:       fmt.Sprintf("Load Lab %03d · %s", ordinal, input.Name),
				MaxWorkers: quotaFor(options, index),
				TaskCount:  tasksFor(options, index),
			}
		}
	} else {
		input.Options.TenantCount = len(tenantIDs)
		input.Options.QuotaProfile = "equal"
		input.Options.LoadShape = "even"
		specs = make([]TenantSpec, len(tenantIDs))
		for index, tenantID := range tenantIDs {
			specs[index] = TenantSpec{
				ID: tenantID, Name: tenantID,
				TaskCount: input.Options.TasksPerTenant,
			}
		}
	}

	totalTasks := 0
	if input.Operation == "load" {
		for _, spec := range specs {
			if spec.TaskCount > MaxTasksPerTenant {
				return normalizedRequest{}, fmt.Errorf(
					"tenant %s contains %d tasks; per-tenant maximum is %d",
					spec.ID, spec.TaskCount, MaxTasksPerTenant,
				)
			}
			totalTasks += spec.TaskCount
			if totalTasks > MaxTasks {
				return normalizedRequest{}, fmt.Errorf(
					"load contains %d tasks; maximum is %d", totalTasks, MaxTasks,
				)
			}
		}
	}
	return normalizedRequest{
		request: input, specs: specs, totalTasks: totalTasks,
		generatedPool: generatedPool,
	}, nil
}

func normalizeTenantIDs(input []string) ([]string, error) {
	if len(input) > MaxTenants {
		return nil, fmt.Errorf("tenantIds contains %d entries; maximum is %d", len(input), MaxTenants)
	}
	seen := make(map[string]struct{}, len(input))
	result := make([]string, 0, len(input))
	for _, value := range input {
		id := strings.TrimSpace(value)
		if id == "" {
			return nil, fmt.Errorf("tenantIds cannot contain an empty ID")
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result, nil
}

func boundedDefault(value, fallback, minimum, maximum int) int {
	if value == 0 {
		value = fallback
	}
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func quotaFor(options Options, index int) int {
	switch options.QuotaProfile {
	case "tiered":
		return []int{5, 20, 100}[index%3]
	case "ramp":
		if options.TenantCount > 1 {
			low := max(1, options.Quota/4)
			return low + (options.Quota-low)*index/(options.TenantCount-1)
		}
	}
	return options.Quota
}

func tasksFor(options Options, index int) int {
	switch options.LoadShape {
	case "hotspot":
		if index == 0 {
			return options.TasksPerTenant * 100
		}
	case "pyramid":
		return options.TasksPerTenant * []int{1, 3, 8}[index%3]
	}
	return options.TasksPerTenant
}

func normalizeSubmissionMode(value string) string {
	switch value {
	case "4", "8", "16", "32":
		return value
	default:
		return "auto"
	}
}

type job struct {
	tenantID string
	index    int
}

func buildRoundRobinJobs(specs []TenantSpec) []job {
	total := 0
	for _, spec := range specs {
		total += spec.TaskCount
	}
	jobs := make([]job, 0, total)
	offsets := make([]int, len(specs))
	for len(jobs) < total {
		for index, spec := range specs {
			if offsets[index] >= spec.TaskCount {
				continue
			}
			jobs = append(jobs, job{tenantID: spec.ID, index: offsets[index]})
			offsets[index]++
		}
	}
	return jobs
}

func splitWaves(jobs []job, requested int) [][]job {
	if len(jobs) == 0 {
		return nil
	}
	count := min(max(1, requested), min(MaxWaves, len(jobs)))
	waves := make([][]job, 0, count)
	offset := 0
	for index := 0; index < count; index++ {
		remaining := len(jobs) - offset
		size := (remaining + count - index - 1) / (count - index)
		waves = append(waves, jobs[offset:offset+size])
		offset += size
	}
	return waves
}
