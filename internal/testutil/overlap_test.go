package testutil

import (
	"sync"
	"testing"
	"time"
)

func TestOverlapGateReleasesEveryConcurrentArrivalWithoutTimingOut(t *testing.T) {
	const overlap, total = 6, 6
	gate := NewOverlapGate(overlap, total, 2*time.Second)

	released := make(chan int, total)
	var group sync.WaitGroup
	for index := 0; index < total; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			overlapMet, totalMet := gate.Arrive(index)
			if !overlapMet || !totalMet {
				t.Errorf(
					"arrival %d reported overlap=%v total=%v for %d concurrent requests",
					index, overlapMet, totalMet, overlap,
				)
			}
			released <- index
		}(index)
	}

	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent arrivals did not pass a gate whose overlap was reached")
	}
	if gate.Arrived() != total {
		t.Fatalf("gate saw %d arrivals, want %d", gate.Arrived(), total)
	}
	if overlapMiss, totalMiss := gate.Missed(); overlapMiss || totalMiss {
		t.Fatalf("concurrent gate reported misses: overlap=%v total=%v", overlapMiss, totalMiss)
	}
	for index := 0; index < total; index++ {
		select {
		case <-released:
		default:
			t.Fatalf("only %d of %d arrivals were released", index, total)
		}
	}
}

func TestOverlapGateReportsSerialArrivalAndNeverWaitsTwice(t *testing.T) {
	const deadline = 300 * time.Millisecond
	gate := NewOverlapGate(2, 8, deadline)

	started := time.Now()
	if overlapMet, _ := gate.Arrive(0); overlapMet {
		t.Fatal("a single arrival reported that two requests were in flight together")
	}
	if elapsed := time.Since(started); elapsed < deadline {
		t.Fatalf("arrival returned after %s, want it held until the %s deadline", elapsed, deadline)
	}
	overlapMiss, totalMiss := gate.Missed()
	if !overlapMiss || !totalMiss {
		t.Fatalf("misses after a serial arrival = overlap %v total %v, want both true", overlapMiss, totalMiss)
	}

	// A recorded miss must not make every later arrival pay the deadline again:
	// a serial pipeline would otherwise need total*deadline to fail.
	started = time.Now()
	gate.Arrive(1)
	if elapsed := time.Since(started); elapsed >= deadline/2 {
		t.Fatalf("second arrival waited %s after the first already timed out", elapsed)
	}
}

func TestOverlapGateHoldsFirstArrivalUntilEveryBatchStarted(t *testing.T) {
	gate := NewOverlapGate(2, 3, 5*time.Second)

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		if overlapMet, totalMet := gate.Arrive(0); !overlapMet || !totalMet {
			t.Errorf("first arrival reported overlap=%v total=%v", overlapMet, totalMet)
		}
	}()
	if overlapMet, _ := gate.Arrive(1); !overlapMet {
		t.Fatal("second arrival did not observe the overlap it completed")
	}

	// The first batch may only finish once all three batches have started, so it
	// must still be blocked while the third one has not arrived.
	select {
	case <-firstDone:
		t.Fatal("first arrival was released before every batch had started")
	case <-time.After(100 * time.Millisecond):
	}

	if _, totalMet := gate.Arrive(2); !totalMet {
		t.Fatal("third arrival reported that not every batch had started")
	}
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first arrival stayed blocked after every batch had started")
	}
	if overlapMiss, totalMiss := gate.Missed(); overlapMiss || totalMiss {
		t.Fatalf("rolling gate reported misses: overlap=%v total=%v", overlapMiss, totalMiss)
	}
}
