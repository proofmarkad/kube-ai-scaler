package finops

import (
	"testing"
	"time"
)

func TestSavingsTrackerZeroMaxSizeDoesNotPanic(t *testing.T) {
	tracker := NewSavingsTracker(0)
	tracker.Record(SavingsRecord{
		Timestamp: time.Now(),
		Workload:  "api",
		Namespace: "default",
		Type:      "rightsizing",
		Monthly:   42,
	})

	summary := tracker.Summary()
	if summary.EventCount != 1 {
		t.Fatalf("expected 1 event, got %d", summary.EventCount)
	}
	if summary.TotalMonthlySavings != 42 {
		t.Fatalf("expected total monthly savings 42, got %f", summary.TotalMonthlySavings)
	}
}
