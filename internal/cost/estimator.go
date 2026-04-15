package cost

import (
	"fmt"
)

// CostEstimate holds the outcome of a cost estimation.
type CostEstimate struct {
	CurrentHourlyCost  float64
	ProposedHourlyCost float64
	DeltaHourlyCost    float64
	DeltaMonthlyCost   float64
	CostPerReplica     float64
	WasteReduction     float64
}

// Estimator estimates the cost impact of a scaling decision.
type Estimator struct{}

// NewEstimator creates a new cost estimator.
func NewEstimator() *Estimator {
	return &Estimator{}
}

// Estimate calculates cost deltas for a proposed replica change.
func (e *Estimator) Estimate(
	currentCost *WorkloadCost,
	currentReplicas int32,
	proposedReplicas int32,
) (*CostEstimate, error) {
	if currentReplicas <= 0 {
		return nil, fmt.Errorf("invalid current replicas: %d", currentReplicas)
	}

	costPerReplica := currentCost.TotalCost / float64(currentReplicas)
	proposedHourly := costPerReplica * float64(proposedReplicas)

	delta := proposedHourly - currentCost.TotalCost
	monthly := delta * 24 * 30

	wasteReduction := 0.0
	if currentCost.TotalEfficiency > 0 && proposedReplicas < currentReplicas {
		wasteReduction = (1 - currentCost.TotalEfficiency) * float64(currentReplicas-proposedReplicas) / float64(currentReplicas) * 100
	}

	return &CostEstimate{
		CurrentHourlyCost:  currentCost.TotalCost,
		ProposedHourlyCost: proposedHourly,
		DeltaHourlyCost:    delta,
		DeltaMonthlyCost:   monthly,
		CostPerReplica:     costPerReplica,
		WasteReduction:     wasteReduction,
	}, nil
}
