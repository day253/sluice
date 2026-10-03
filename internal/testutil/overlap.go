package testutil

import (
	"sync"
	"time"
)

// OverlapGate turns "did these requests run at the same time?" from a wall-clock
// observation into a causal condition.
//
// The usual way to test rolling concurrency is to submit work, wait a while and
// then assert the peak number of in-flight requests. That peak is only
// observable when the machine is fast enough to schedule every worker before
// the first one finishes, so the assertion flakes on a loaded runner — a 2 vCPU
// CI runner under `-race` observed 15 of a configured 16 while the code under
// test was correct. Holding the first arrivals inside the gate until the
// expected overlap is actually present makes the peak a consequence of the code
// under test instead of the scheduler: a rolling ingress opens the gate as soon
// as the configured number of requests is in flight together, while a serial or
// wave-barrier ingress can only pass by letting the gate time out.
type OverlapGate struct {
	overlap  int
	total    int
	deadline time.Duration

	mu          sync.Mutex
	arrived     int
	overlapMet  bool
	totalMet    bool
	overlapDone chan struct{}
	totalDone   chan struct{}
	overlapMiss bool
	totalMiss   bool
}

// NewOverlapGate returns a gate that releases every arrival once overlap
// requests are in flight together, and that additionally holds arrival 0 until
// total requests have started. Both conditions are bounded by deadline.
func NewOverlapGate(overlap, total int, deadline time.Duration) *OverlapGate {
	return &OverlapGate{
		overlap:     overlap,
		total:       total,
		deadline:    deadline,
		overlapDone: make(chan struct{}),
		totalDone:   make(chan struct{}),
	}
}

// Arrive registers one started request and blocks until the gate conditions that
// are still reachable are satisfied. It reports whether the overlap condition
// (overlap requests in flight together) and the total condition (every one of
// total requests started before arrival 0 ended) were observed; false means the
// caller's concurrency is not rolling and the caller must fail.
//
// Arrivals that cannot contribute to a condition never block on it: once the
// overlap has been observed, or once a wait has already timed out, later
// arrivals return immediately. A gate therefore costs one deadline at most,
// even when the code under test is fully serial.
func (g *OverlapGate) Arrive(index int) (overlapMet, totalMet bool) {
	g.mu.Lock()
	g.arrived++
	if g.arrived >= g.overlap && !g.overlapMet {
		g.overlapMet = true
		close(g.overlapDone)
	}
	if g.arrived >= g.total && !g.totalMet {
		g.totalMet = true
		close(g.totalDone)
	}
	waitOverlap := !g.overlapMet && !g.overlapMiss
	waitTotal := index == 0 && !g.totalMet && !g.totalMiss
	g.mu.Unlock()

	overlapMet = true
	if waitOverlap {
		overlapMet = g.wait(g.overlapDone)
	}
	totalMet = true
	if waitTotal {
		totalMet = g.wait(g.totalDone)
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if waitOverlap && !overlapMet {
		g.overlapMiss = true
	}
	if waitTotal && !totalMet {
		g.totalMiss = true
	}
	return overlapMet, totalMet
}

// Missed reports which conditions had to time out. It lets a failing test
// distinguish "the ingress never reached the configured concurrency" from "the
// ingress never kept the pipeline full until the first request ended".
func (g *OverlapGate) Missed() (overlapMiss, totalMiss bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.overlapMiss, g.totalMiss
}

// Arrived reports how many requests reached the gate.
func (g *OverlapGate) Arrived() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.arrived
}

func (g *OverlapGate) wait(done <-chan struct{}) bool {
	timer := time.NewTimer(g.deadline)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
