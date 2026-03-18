package decision

import (
	"fmt"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/llm"
)

// ValidationResult holds the outcome of a validation pass.
type ValidationResult struct {
	// OriginalReplicas is what the LLM suggested before clamping.
	OriginalReplicas int32
	// ValidatedReplicas is the safe value after all guardrails applied.
	ValidatedReplicas int32
	// Clamped is true if the validator changed the LLM's suggestion.
	Clamped bool
	// Reason describes why the value was clamped, if it was.
	Reason string
}

// Validator enforces hard scaling constraints regardless of LLM output.
// It never trusts the LLM blindly — every decision passes through here
// before being handed to the actuator.
type Validator struct{}

func NewValidator() *Validator {
	return &Validator{}
}

// Validate takes the LLM decision and the AIScaler policy and returns
// a safe replica count. The order of checks matters:
//  1. Step size — never move more than maxScaleStep in one cycle
//  2. Min/max bounds — never go outside the hard limits
func (v *Validator) Validate(
	decision *llm.ScalingDecision,
	current int32,
	policy *aiscalerv1.AIScaler,
) *ValidationResult {
	constraints := policy.Spec.Constraints
	target := decision.TargetReplicas
	original := target

	// Step 1: enforce max scale step
	// Clamp how far we can move from current in one cycle
	/*
		Scale up:
				Say current = 4, maxScaleStep = 3, and the LLM suggests target = 10.
				delta = target - current
				delta = 10 - 4 = 6
				6 > 3 (maxScaleStep) so we clamp:
				target = current + maxScaleStep
				target = 4 + 3 = 7
				We move 3 steps up instead of 6. Next reconcile cycle we'll move another 3, gradually approaching 10.

		Scale down:
			current = 10, maxScaleStep = 3, LLM suggests target = 2.
			delta = target - current
			delta = 2 - 10 = -8
			-8 < -3 so we clamp:
			target = current - maxScaleStep
			target = 10 - 3 = 7
	*/
	delta := target - current

	if delta > constraints.MaxScaleStep {
		target = current + constraints.MaxScaleStep
	} else if delta < -constraints.MaxScaleStep {
		target = current - constraints.MaxScaleStep
	}

	// Step 2: clamp to min/max
	if target < constraints.MinReplicas {
		target = constraints.MinReplicas
	} else if target > constraints.MaxReplicas {
		target = constraints.MaxReplicas
	}

	clamped := target != original
	reason := ""
	if clamped {
		reason = buildClampReason(original, target, current, constraints)
	}

	return &ValidationResult{
		OriginalReplicas:  original,
		ValidatedReplicas: target,
		Clamped:           clamped,
		Reason:            reason,
	}
}

// buildClampReason produces a human-readable explanation of why the
// validator changed the LLM's suggestion. This gets recorded in the
// AIScaler status for audit and debugging.
func buildClampReason(original, validated, current int32, c aiscalerv1.ScalingConstraints) string {
	delta := original - current
	switch {
	case delta > c.MaxScaleStep:
		return fmt.Sprintf(
			"LLM suggested %d but step size %d exceeds maxScaleStep %d — clamped to %d",
			original, delta, c.MaxScaleStep, validated,
		)
	case delta < -c.MaxScaleStep:
		return fmt.Sprintf(
			"LLM suggested %d but step size %d exceeds maxScaleStep %d — clamped to %d",
			original, -delta, c.MaxScaleStep, validated,
		)
	case original < c.MinReplicas:
		return fmt.Sprintf(
			"LLM suggested %d which is below minReplicas %d — clamped to %d",
			original, c.MinReplicas, validated,
		)
	case original > c.MaxReplicas:
		return fmt.Sprintf(
			"LLM suggested %d which is above maxReplicas %d — clamped to %d",
			original, c.MaxReplicas, validated,
		)
	default:
		return fmt.Sprintf("clamped from %d to %d", original, validated)
	}
}
