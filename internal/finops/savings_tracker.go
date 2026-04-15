package finops

import (
	"sync"
	"time"
)

// SavingsRecord captures a single savings event.
type SavingsRecord struct {
	Timestamp time.Time
	Workload  string
	Namespace string
	Type      string  // "horizontal", "vertical", "rightsizing"
	Monthly   float64 // estimated monthly savings
}

// SavingsSummary aggregates savings across all workloads.
type SavingsSummary struct {
	TotalMonthlySavings float64
	EventCount          int
	ByWorkload          map[string]float64
	ByType              map[string]float64
}

// SavingsTracker accumulates resource savings over time.
type SavingsTracker struct {
	mu      sync.Mutex
	records []SavingsRecord
	maxSize int
}

// NewSavingsTracker creates a savings tracker.
func NewSavingsTracker(maxSize int) *SavingsTracker {
	if maxSize <= 0 {
		maxSize = 1
	}
	return &SavingsTracker{
		records: make([]SavingsRecord, 0, maxSize),
		maxSize: maxSize,
	}
}

// Record adds a savings event.
func (st *SavingsTracker) Record(rec SavingsRecord) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.maxSize <= 0 {
		st.maxSize = 1
	}

	if len(st.records) >= st.maxSize {
		st.records = st.records[1:]
	}
	st.records = append(st.records, rec)
}

// Summary returns aggregated savings data.
func (st *SavingsTracker) Summary() *SavingsSummary {
	st.mu.Lock()
	defer st.mu.Unlock()

	summary := &SavingsSummary{
		ByWorkload: make(map[string]float64),
		ByType:     make(map[string]float64),
	}

	for _, r := range st.records {
		summary.TotalMonthlySavings += r.Monthly
		summary.EventCount++
		summary.ByWorkload[r.Workload] += r.Monthly
		summary.ByType[r.Type] += r.Monthly
	}

	return summary
}
