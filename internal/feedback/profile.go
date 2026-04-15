package feedback

import (
	"sync"
)

// WorkloadProfile captures learned characteristics of a workload.
type WorkloadProfile struct {
	mu              sync.RWMutex
	RPSPerReplica   float64
	BreakEvenCPU    float64
	AvgLatencyPerRPS float64
	TotalObservations int
}

// NewWorkloadProfile creates an empty workload profile.
func NewWorkloadProfile() *WorkloadProfile {
	return &WorkloadProfile{}
}

// Update incorporates a new observation into the profile using EMA.
func (wp *WorkloadProfile) Update(outcome DecisionOutcome) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	wp.TotalObservations++
	alpha := 0.1 // EMA smoothing factor
	if wp.TotalObservations < 10 {
		alpha = 0.3 // learn faster initially
	}

	// Update break-even CPU (typical CPU when workload is stable)
	if outcome.Effective {
		wp.BreakEvenCPU = wp.BreakEvenCPU*(1-alpha) + outcome.CPUAfter*alpha
	}
}

// GetRPSPerReplica returns the estimated RPS capacity per replica.
func (wp *WorkloadProfile) GetRPSPerReplica() float64 {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	return wp.RPSPerReplica
}

// GetBreakEvenCPU returns the learned baseline CPU.
func (wp *WorkloadProfile) GetBreakEvenCPU() float64 {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	return wp.BreakEvenCPU
}

// HasSufficientData returns true if enough observations have been collected.
func (wp *WorkloadProfile) HasSufficientData() bool {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	return wp.TotalObservations >= 10
}
