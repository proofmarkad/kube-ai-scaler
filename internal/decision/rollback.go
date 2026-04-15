package decision

import (
	"fmt"
)

// RollbackAction describes a recommended rollback.
type RollbackAction struct {
	Recommended     bool
	Reason          string
	TargetReplicas  int32
	PreviousReplicas int32
}

// RollbackManager evaluates whether a recent scaling decision should be reverted.
type RollbackManager struct{}

// NewRollbackManager creates a new rollback manager.
func NewRollbackManager() *RollbackManager {
	return &RollbackManager{}
}

// RollbackInput provides the data needed for rollback evaluation.
type RollbackInput struct {
	PreviousReplicas     int32
	CurrentReplicas      int32
	SLOsMetBefore        bool
	SLOsMetAfter         bool
	ErrorRateBefore      float64
	ErrorRateAfter       float64
	LatencyBefore        float64
	LatencyAfter         float64
	CrashLoopDetected    bool
	AutoRollbackEnabled  bool
	RollbackConditions   []string
}

// CheckForRollback evaluates whether conditions warrant a rollback.
func (rm *RollbackManager) CheckForRollback(input *RollbackInput) *RollbackAction {
	if !input.AutoRollbackEnabled {
		return &RollbackAction{Recommended: false}
	}

	for _, condition := range input.RollbackConditions {
		switch condition {
		case "sloViolationWithin":
			if input.SLOsMetBefore && !input.SLOsMetAfter {
				return &RollbackAction{
					Recommended:      true,
					Reason:           fmt.Sprintf("SLO violation detected after scaling from %d to %d", input.PreviousReplicas, input.CurrentReplicas),
					TargetReplicas:   input.PreviousReplicas,
					PreviousReplicas: input.CurrentReplicas,
				}
			}

		case "crashLoopDetected":
			if input.CrashLoopDetected {
				return &RollbackAction{
					Recommended:      true,
					Reason:           "crash loop detected after scaling",
					TargetReplicas:   input.PreviousReplicas,
					PreviousReplicas: input.CurrentReplicas,
				}
			}

		case "errorRateIncrease":
			if input.ErrorRateAfter > input.ErrorRateBefore*1.5 && input.ErrorRateAfter > 0.01 {
				return &RollbackAction{
					Recommended:      true,
					Reason:           fmt.Sprintf("error rate increased from %.2f%% to %.2f%% after scaling", input.ErrorRateBefore*100, input.ErrorRateAfter*100),
					TargetReplicas:   input.PreviousReplicas,
					PreviousReplicas: input.CurrentReplicas,
				}
			}

		case "latencyIncrease":
			if input.LatencyAfter > input.LatencyBefore*2 && input.LatencyAfter > 50 {
				return &RollbackAction{
					Recommended:      true,
					Reason:           fmt.Sprintf("latency increased from %.1fms to %.1fms after scaling", input.LatencyBefore, input.LatencyAfter),
					TargetReplicas:   input.PreviousReplicas,
					PreviousReplicas: input.CurrentReplicas,
				}
			}
		}
	}

	return &RollbackAction{Recommended: false}
}
