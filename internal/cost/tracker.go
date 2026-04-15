package cost

import (
	"sync"
	"time"
)

// SavingsRecord captures a single cost saving event.
type SavingsRecord struct {
	Timestamp        time.Time
	Workload         string
	Namespace        string
	DeltaHourly      float64
	DeltaMonthly     float64
	PreviousReplicas int32
	NewReplicas      int32
}

// SavingsSummary aggregates cost savings.
type SavingsSummary struct {
	TotalSaved          float64
	TotalEvents         int
	AvgSavingPerEvent   float64
	LargestSingleSaving float64
}

// Tracker accumulates scaling cost savings over time.
type Tracker struct {
	mu      sync.Mutex
	records []SavingsRecord
	maxSize int
}

// NewTracker creates a tracker with a max history size.
func NewTracker(maxSize int) *Tracker {
	if maxSize <= 0 {
		maxSize = 1
	}
	return &Tracker{
		records: make([]SavingsRecord, 0, maxSize),
		maxSize: maxSize,
	}
}

// Record adds a new savings record.
func (t *Tracker) Record(record SavingsRecord) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.maxSize <= 0 {
		t.maxSize = 1
	}

	if len(t.records) >= t.maxSize {
		t.records = t.records[1:]
	}
	t.records = append(t.records, record)
}

// Summary returns aggregated savings data.
func (t *Tracker) Summary() *SavingsSummary {
	t.mu.Lock()
	defer t.mu.Unlock()

	summary := &SavingsSummary{TotalEvents: len(t.records)}
	for _, r := range t.records {
		if r.DeltaMonthly < 0 { // savings are negative deltas
			saving := -r.DeltaMonthly
			summary.TotalSaved += saving
			if saving > summary.LargestSingleSaving {
				summary.LargestSingleSaving = saving
			}
		}
	}
	if summary.TotalEvents > 0 {
		summary.AvgSavingPerEvent = summary.TotalSaved / float64(summary.TotalEvents)
	}
	return summary
}
