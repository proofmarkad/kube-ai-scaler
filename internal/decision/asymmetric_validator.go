package decision

import (
	"fmt"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/llm"
)

// AsymmetricValidationResult extends the basic validation result with direction info.
type AsymmetricValidationResult struct {
	ValidationResult
	Direction     string
	DirectionLimit int32
}

// ValidateAsymmetric applies different constraints for scale-up vs scale-down.
func ValidateAsymmetric(
	dec *llm.ScalingDecision,
	current int32,
	policy *aiscalerv1.AIScaler,
) *AsymmetricValidationResult {
	safety := policy.Spec.Safety
	if safety == nil {
		// Fall back to symmetric validation
		base := NewValidator().Validate(dec, current, policy)
		return &AsymmetricValidationResult{
			ValidationResult: *base,
			Direction:        directionOf(dec.TargetReplicas, current),
		}
	}

	target := dec.TargetReplicas
	original := target
	direction := directionOf(target, current)

	var dp *aiscalerv1.DirectionPolicy
	if direction == "up" && safety.ScaleUp != nil {
		dp = safety.ScaleUp
	} else if direction == "down" && safety.ScaleDown != nil {
		dp = safety.ScaleDown
	}

	dirLimit := int32(0)
	if dp != nil {
		dirLimit = dp.MaxStep
		// Enforce per-direction max step
		if dp.MaxStep > 0 {
			delta := target - current
			if delta > dp.MaxStep {
				target = current + dp.MaxStep
			} else if delta < -dp.MaxStep {
				target = current - dp.MaxStep
			}
		}

		// Enforce per-direction confidence requirement
		if dp.RequireConfidence > 0 && dec.Confidence < dp.RequireConfidence {
			target = current // don't scale if confidence too low for this direction
		}
	}

	// Still enforce global min/max
	constraints := policy.Spec.Constraints
	if target < constraints.MinReplicas {
		target = constraints.MinReplicas
	} else if target > constraints.MaxReplicas {
		target = constraints.MaxReplicas
	}

	clamped := target != original
	reason := ""
	if clamped {
		reason = fmt.Sprintf("asymmetric %s: clamped from %d to %d", direction, original, target)
	}

	return &AsymmetricValidationResult{
		ValidationResult: ValidationResult{
			OriginalReplicas:  original,
			ValidatedReplicas: target,
			Clamped:           clamped,
			Reason:            reason,
		},
		Direction:     direction,
		DirectionLimit: dirLimit,
	}
}

func directionOf(target, current int32) string {
	if target > current {
		return "up"
	} else if target < current {
		return "down"
	}
	return "none"
}
