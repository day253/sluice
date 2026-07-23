package worker

import (
	"errors"
	"testing"
	"time"
)

func TestProcessCPULoadSamplerNormalizesCachesAndClampsUsage(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	usage := time.Duration(0)
	reads := 0
	sampler := newProcessCPULoadSampler(
		func() time.Time { return now },
		func() (time.Duration, error) {
			reads++
			return usage, nil
		},
		2,
		250*time.Millisecond,
	)

	if value, valid := sampler.Sample(); valid || value != 0 {
		t.Fatalf("baseline sample = %d/%v, want 0/false", value, valid)
	}
	now = now.Add(100 * time.Millisecond)
	usage = 80 * time.Millisecond
	if value, valid := sampler.Sample(); valid || value != 0 || reads != 1 {
		t.Fatalf("cached pre-window sample = %d/%v reads=%d", value, valid, reads)
	}

	now = now.Add(150 * time.Millisecond)
	usage = 250 * time.Millisecond
	if value, valid := sampler.Sample(); !valid || value != 500 {
		t.Fatalf("normalized sample = %d/%v, want 500/true", value, valid)
	}
	if value, valid := sampler.Sample(); !valid || value != 500 || reads != 2 {
		t.Fatalf("cached sample = %d/%v reads=%d, want 500/true/2", value, valid, reads)
	}

	now = now.Add(250 * time.Millisecond)
	usage += 750 * time.Millisecond
	if value, valid := sampler.Sample(); !valid || value != 1000 {
		t.Fatalf("clamped sample = %d/%v, want 1000/true", value, valid)
	}
}

func TestProcessCPULoadSamplerCachesFailureAndRequiresNewBaseline(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	reads := 0
	fail := true
	sampler := newProcessCPULoadSampler(
		func() time.Time { return now },
		func() (time.Duration, error) {
			reads++
			if fail {
				return 0, errors.New("usage unavailable")
			}
			return 10 * time.Millisecond, nil
		},
		1,
		250*time.Millisecond,
	)
	if _, valid := sampler.Sample(); valid {
		t.Fatal("failed usage sample unexpectedly valid")
	}
	now = now.Add(100 * time.Millisecond)
	if _, valid := sampler.Sample(); valid || reads != 1 {
		t.Fatalf("failed sample was not cached: valid=%v reads=%d", valid, reads)
	}
	fail = false
	now = now.Add(150 * time.Millisecond)
	if _, valid := sampler.Sample(); valid || reads != 2 {
		t.Fatalf("recovered source skipped baseline: valid=%v reads=%d", valid, reads)
	}
}
