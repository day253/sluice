// Package metrics provides server-side multi-resolution time-series
// storage for dashboard metrics.  Design adapted from brpc's bvar:
//   60 recent seconds + 60 recent minutes + 24 hours + 30 days = 174 pts.
package metrics

import (
	"sync"
	"time"
)

// VarHistory stores a single metric's history at multiple resolutions.
// Thread-safe.  Call Snapshot() every second.
type VarHistory struct {
	mu     sync.Mutex
	name   string
	labels map[string]string

	// Second ring: last 60 seconds at 1s resolution.
	secs     [60]int64
	secIdx   int

	// Minute ring: last 60 minutes (each is avg of its 60 seconds).
	mins     [60]int64
	minIdx   int
	minAcc   int64   // accumulating sum for current minute
	minCnt   int     // number of samples in current minute

	// Hour ring: last 24 hours (each is avg of its 60 minutes).
	hours    [24]int64
	hourIdx  int
	hourAcc  int64
	hourCnt  int

	// Day ring: last 30 days (each is avg of its 24 hours).
	days     [30]int64
	dayIdx   int
	dayAcc   int64
	dayCnt   int

	lastTick time.Time
}

func NewVarHistory(name string, labels map[string]string) *VarHistory {
	return &VarHistory{
		name:   name,
		labels: labels,
	}
}

// Record writes a sample.  Call this at any frequency.
func (v *VarHistory) Record(val int64) {
	v.mu.Lock()
	v.minAcc += val
	v.minCnt++
	v.hourAcc += val
	v.hourCnt++
	v.dayAcc += val
	v.dayCnt++
	v.mu.Unlock()
}

// Snapshot advances the time windows.  Call every 1 second.
func (v *VarHistory) Snapshot() {
	v.mu.Lock()
	defer v.mu.Unlock()

	now := time.Now().Truncate(time.Second)
	if !v.lastTick.IsZero() && now.Equal(v.lastTick) {
		return // already ticked this second
	}
	v.lastTick = now

	// ---- Second ring ----
	avg := int64(0)
	if v.minCnt > 0 {
		avg = v.minAcc / int64(v.minCnt)
	}
	v.secs[v.secIdx] = avg
	v.secIdx = (v.secIdx + 1) % 60

	// ---- Minute ring (on second rollover or every second we store the
	//      average of the last second into minAcc) ----
	v.minAcc += avg
	v.minCnt++

	// Every 60 seconds flush a minute.
	if v.secIdx == 0 {
		minAvg := int64(0)
		if v.minCnt > 0 {
			minAvg = v.minAcc / int64(v.minCnt)
		}
		v.mins[v.minIdx] = minAvg
		v.minIdx = (v.minIdx + 1) % 60
		v.minAcc = 0
		v.minCnt = 0

		v.hourAcc += minAvg
		v.hourCnt++

		// Every 60 minutes flush an hour.
		if v.minIdx == 0 {
			hourAvg := int64(0)
			if v.hourCnt > 0 {
				hourAvg = v.hourAcc / int64(v.hourCnt)
			}
			v.hours[v.hourIdx] = hourAvg
			v.hourIdx = (v.hourIdx + 1) % 24
			v.hourAcc = 0
			v.hourCnt = 0

			v.dayAcc += hourAvg
			v.dayCnt++

			// Every 24 hours flush a day.
			if v.hourIdx == 0 {
				dayAvg := int64(0)
				if v.dayCnt > 0 {
					dayAvg = v.dayAcc / int64(v.dayCnt)
				}
				v.days[v.dayIdx] = dayAvg
				v.dayIdx = (v.dayIdx + 1) % 30
				v.dayAcc = 0
				v.dayCnt = 0
			}
		}
	}
}

// Query returns the stored time-series data for the dashboard.
func (v *VarHistory) Query() VarHistoryData {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Build second array in chronological order.
	secs := make([]int64, 60)
	for i := 0; i < 60; i++ {
		secs[i] = v.secs[(v.secIdx+i)%60]
	}

	mins := make([]int64, 60)
	for i := 0; i < 60; i++ {
		mins[i] = v.mins[(v.minIdx+i)%60]
	}

	hours := make([]int64, 24)
	for i := 0; i < 24; i++ {
		hours[i] = v.hours[(v.hourIdx+i)%24]
	}

	days := make([]int64, 30)
	for i := 0; i < 30; i++ {
		days[i] = v.days[(v.dayIdx+i)%30]
	}

	return VarHistoryData{
		Name:   v.name,
		Labels: v.labels,
		Secs:   secs,
		Mins:   mins,
		Hours:  hours,
		Days:   days,
	}
}

// VarHistoryData is the JSON response for metrics queries.
type VarHistoryData struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Secs   []int64           `json:"secs"`
	Mins   []int64           `json:"mins"`
	Hours  []int64           `json:"hours"`
	Days   []int64           `json:"days"`
}
