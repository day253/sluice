package metrics

import (
	"testing"
	"time"
)

func TestVarHistoryKeeps174MultiResolutionPoints(t *testing.T) {
	var history VarHistory
	start := time.Date(2026, time.July, 12, 0, 0, 0, 0, time.UTC)

	// A full day is enough to populate seconds, minutes, hours, and the
	// first daily aggregate. A constant sample makes every average exact.
	for i := 1; i <= 24*60*60; i++ {
		history.Record(12)
		history.tickAt(start.Add(time.Duration(i) * time.Second))
	}

	data := history.Query()
	if got := len(data.Secs) + len(data.Mins) + len(data.Hours) + len(data.Days); got != 174 {
		t.Fatalf("total history points = %d, want 174", got)
	}
	if data.Secs[len(data.Secs)-1] != 12 {
		t.Fatalf("latest second = %d, want 12", data.Secs[len(data.Secs)-1])
	}
	if data.Mins[len(data.Mins)-1] != 12 {
		t.Fatalf("latest minute = %d, want 12", data.Mins[len(data.Mins)-1])
	}
	if data.Hours[len(data.Hours)-1] != 12 {
		t.Fatalf("latest hour = %d, want 12", data.Hours[len(data.Hours)-1])
	}
	if data.Days[len(data.Days)-1] != 12 {
		t.Fatalf("latest day = %d, want 12", data.Days[len(data.Days)-1])
	}
}
