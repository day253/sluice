package metrics

import (
	"sync"
	"time"
)

// VarHistory stores a single metric with brpc-bvar-style multi-resolution
// automatic downsampling:
//
//	60 seconds at 1s granularity  (latest 1 min)
//	60 minutes at 1min granularity (latest 1 hour)
//	24 hours at 1h granularity    (latest 1 day)
//	30 days at 1d granularity     (latest 30 days)
//
// Total: 60 + 60 + 24 + 30 = 174 values per metric.
//
// Call Record(v) at any rate to feed samples.  Call Tick() once per second
// to advance the rings.  Tick is idempotent within the same second.
type VarHistory struct {
	mu sync.Mutex

	// Second ring — 60 most recent seconds.
	secRing [60]int64
	secIdx  int
	secCnt  int // samples this second, for averaging

	// Accumulator for the current second.
	secAcc int64

	// Minute ring — 60 most recent minutes (each = avg of 60 seconds).
	minRing [60]int64
	minIdx  int

	// Accumulator for the current minute (sum of second averages).
	minAcc int64
	minCnt int // number of seconds in current minute

	// Hour ring — 24 most recent hours (each = avg of 60 minutes).
	hourRing [24]int64
	hourIdx  int

	// Accumulator for the current hour.
	hourAcc int64
	hourCnt int

	// Day ring — 30 most recent days.
	dayRing [30]int64
	dayIdx  int
	dayAcc  int64
	dayCnt  int

	// Last tick time.
	lastTick time.Time
}

// Record feeds a sample.  Multiple calls within the same second are
// averaged by Tick.
func (v *VarHistory) Record(val int64) {
	v.mu.Lock()
	v.secAcc += val
	v.secCnt++
	v.mu.Unlock()
}

// Tick advances time windows.  Call once per second.
func (v *VarHistory) Tick() {
	v.tickAt(time.Now())
}

func (v *VarHistory) tickAt(now time.Time) {
	v.mu.Lock()
	defer v.mu.Unlock()

	now = now.Truncate(time.Second)
	if !v.lastTick.IsZero() && !now.After(v.lastTick) {
		return
	}
	v.lastTick = now

	// ---- Commit current second ----
	secAvg := int64(0)
	if v.secCnt > 0 {
		secAvg = v.secAcc / int64(v.secCnt)
	}
	v.secRing[v.secIdx] = secAvg
	v.secIdx = (v.secIdx + 1) % 60
	v.secAcc = 0
	v.secCnt = 0

	// Feed into minute accumulator.
	v.minAcc += secAvg
	v.minCnt++

	// ---- Every 60 seconds, commit a minute ----
	if v.secIdx == 0 {
		minAvg := int64(0)
		if v.minCnt > 0 {
			minAvg = v.minAcc / int64(v.minCnt)
		}
		v.minRing[v.minIdx] = minAvg
		v.minIdx = (v.minIdx + 1) % 60
		v.minAcc = 0
		v.minCnt = 0

		v.hourAcc += minAvg
		v.hourCnt++

		// ---- Every 60 minutes, commit an hour ----
		if v.minIdx == 0 {
			hourAvg := int64(0)
			if v.hourCnt > 0 {
				hourAvg = v.hourAcc / int64(v.hourCnt)
			}
			v.hourRing[v.hourIdx] = hourAvg
			v.hourIdx = (v.hourIdx + 1) % 24
			v.hourAcc = 0
			v.hourCnt = 0
			v.dayAcc += hourAvg
			v.dayCnt++

			// ---- Every 24 hours, commit a day ----
			if v.hourIdx == 0 {
				dayAvg := int64(0)
				if v.dayCnt > 0 {
					dayAvg = v.dayAcc / int64(v.dayCnt)
				}
				v.dayRing[v.dayIdx] = dayAvg
				v.dayIdx = (v.dayIdx + 1) % 30
				v.dayAcc = 0
				v.dayCnt = 0
			}
		}
	}
}

// Query returns all rings in chronological order.
func (v *VarHistory) Query() VarData {
	v.mu.Lock()
	defer v.mu.Unlock()

	return VarData{
		Secs:  ringChrono(v.secRing[:], v.secIdx),
		Mins:  ringChrono(v.minRing[:], v.minIdx),
		Hours: ringChrono(v.hourRing[:], v.hourIdx),
		Days:  ringChrono(v.dayRing[:], v.dayIdx),
	}
}

// VarData is the JSON response payload.
type VarData struct {
	Secs  []int64 `json:"secs"`
	Mins  []int64 `json:"mins"`
	Hours []int64 `json:"hours"`
	Days  []int64 `json:"days"`
}

func ringChrono(ring []int64, cursor int) []int64 {
	n := len(ring)
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		out[i] = ring[(cursor+i)%n]
	}
	return out
}
