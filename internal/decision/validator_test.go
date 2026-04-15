package decision

import (
	"testing"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/llm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newPolicy(min, max, step int32) *aiscalerv1.AIScaler {
	return &aiscalerv1.AIScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "test"},
		Spec: aiscalerv1.AIScalerSpec{
			Constraints: aiscalerv1.ScalingConstraints{
				MinReplicas:  min,
				MaxReplicas:  max,
				MaxScaleStep: step,
			},
		},
	}
}

func TestValidator_NoClamping(t *testing.T) {
	v := NewValidator()
	dec := &llm.ScalingDecision{TargetReplicas: 5, Confidence: 0.9}
	result := v.Validate(dec, 4, newPolicy(1, 10, 3))

	if result.Clamped {
		t.Error("expected no clamping")
	}
	if result.ValidatedReplicas != 5 {
		t.Errorf("expected 5, got %d", result.ValidatedReplicas)
	}
}

func TestValidator_ClampMaxStep_Up(t *testing.T) {
	v := NewValidator()
	dec := &llm.ScalingDecision{TargetReplicas: 10, Confidence: 0.9}
	result := v.Validate(dec, 4, newPolicy(1, 20, 3))

	if !result.Clamped {
		t.Error("expected clamping")
	}
	if result.ValidatedReplicas != 7 {
		t.Errorf("expected 7 (4+3), got %d", result.ValidatedReplicas)
	}
}

func TestValidator_ClampMaxStep_Down(t *testing.T) {
	v := NewValidator()
	dec := &llm.ScalingDecision{TargetReplicas: 2, Confidence: 0.9}
	result := v.Validate(dec, 10, newPolicy(1, 20, 3))

	if !result.Clamped {
		t.Error("expected clamping")
	}
	if result.ValidatedReplicas != 7 {
		t.Errorf("expected 7 (10-3), got %d", result.ValidatedReplicas)
	}
}

func TestValidator_ClampMin(t *testing.T) {
	v := NewValidator()
	dec := &llm.ScalingDecision{TargetReplicas: 0, Confidence: 0.9}
	result := v.Validate(dec, 2, newPolicy(2, 10, 5))

	if !result.Clamped {
		t.Error("expected clamping to min")
	}
	if result.ValidatedReplicas != 2 {
		t.Errorf("expected 2 (min), got %d", result.ValidatedReplicas)
	}
}

func TestValidator_ClampMax(t *testing.T) {
	v := NewValidator()
	dec := &llm.ScalingDecision{TargetReplicas: 15, Confidence: 0.9}
	result := v.Validate(dec, 9, newPolicy(1, 10, 10))

	if !result.Clamped {
		t.Error("expected clamping to max")
	}
	if result.ValidatedReplicas != 10 {
		t.Errorf("expected 10 (max), got %d", result.ValidatedReplicas)
	}
}

func TestValidator_StepThenBoundsClamp(t *testing.T) {
	// Step clamp would give 7 but max is 6, so final should be 6
	v := NewValidator()
	dec := &llm.ScalingDecision{TargetReplicas: 20, Confidence: 0.9}
	result := v.Validate(dec, 4, newPolicy(1, 6, 3))

	if !result.Clamped {
		t.Error("expected clamping")
	}
	if result.ValidatedReplicas != 6 {
		t.Errorf("expected 6, got %d", result.ValidatedReplicas)
	}
}
