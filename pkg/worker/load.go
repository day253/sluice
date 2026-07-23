package worker

import (
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const cpuSampleInterval = 250 * time.Millisecond

// CPULoadSampler returns normalized process/container CPU utilization in
// thousandths (0..1000). false means no reliable delta sample is available.
// Implementations must be concurrency-safe because every idle Processor slot
// may request a snapshot at the same time.
type CPULoadSampler interface {
	Sample() (utilizationMillis int32, valid bool)
}

type cpuUsageReader func() (time.Duration, error)

// ProcessCPULoadSampler amortizes process CPU sampling across all Processor
// goroutines. CPU time is normalized by the container quota when cgroup v2
// exposes one, otherwise by the Go process's available parallelism.
type ProcessCPULoadSampler struct {
	mu sync.Mutex

	now         func() time.Time
	readUsage   cpuUsageReader
	cpuCapacity float64
	interval    time.Duration

	initialized bool
	lastWall    time.Time
	lastUsage   time.Duration
	cached      int32
	valid       bool
}

func NewProcessCPULoadSampler() *ProcessCPULoadSampler {
	sampler := newProcessCPULoadSampler(
		time.Now,
		readProcessCPUUsage,
		detectCPUCapacity(),
		cpuSampleInterval,
	)
	// Establish the cumulative CPU baseline before Worker registration and
	// allocation. In production the first assignment normally arrives after
	// the sampling interval, so it already carries a real delta.
	_, _ = sampler.sampleLocked(time.Now())
	return sampler
}

func newProcessCPULoadSampler(
	now func() time.Time,
	readUsage cpuUsageReader,
	cpuCapacity float64,
	interval time.Duration,
) *ProcessCPULoadSampler {
	if cpuCapacity <= 0 {
		cpuCapacity = 1
	}
	return &ProcessCPULoadSampler{
		now: now, readUsage: readUsage, cpuCapacity: cpuCapacity, interval: interval,
	}
}

func (s *ProcessCPULoadSampler) Sample() (int32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sampleLocked(s.now())
}

func (s *ProcessCPULoadSampler) sampleLocked(now time.Time) (int32, bool) {
	if !s.lastWall.IsZero() && now.Sub(s.lastWall) < s.interval {
		return s.cached, s.valid
	}
	usage, err := s.readUsage()
	if err != nil {
		s.initialized = false
		s.lastWall = now
		s.cached = 0
		s.valid = false
		return 0, false
	}
	if !s.initialized {
		s.initialized = true
		s.lastWall = now
		s.lastUsage = usage
		s.valid = false
		return 0, false
	}
	wallDelta := now.Sub(s.lastWall)
	usageDelta := usage - s.lastUsage
	s.lastWall = now
	s.lastUsage = usage
	if wallDelta <= 0 || usageDelta < 0 {
		s.valid = false
		return 0, false
	}
	ratio := float64(usageDelta) / float64(wallDelta) / s.cpuCapacity
	millis := int32(math.Round(ratio * 1000))
	if millis < 0 {
		millis = 0
	}
	if millis > 1000 {
		millis = 1000
	}
	s.cached = millis
	s.valid = true
	return millis, true
}

func detectCPUCapacity() float64 {
	// In a cgroup-v2 container cpu.max is "<quota> <period>". "max" means
	// there is no quota and the process parallelism is the best local bound.
	if raw, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) == 2 && fields[0] != "max" {
			quota, quotaErr := strconv.ParseFloat(fields[0], 64)
			period, periodErr := strconv.ParseFloat(fields[1], 64)
			if quotaErr == nil && periodErr == nil && quota > 0 && period > 0 {
				return quota / period
			}
		}
	}
	if available := runtime.GOMAXPROCS(0); available > 0 {
		return float64(available)
	}
	return 1
}
