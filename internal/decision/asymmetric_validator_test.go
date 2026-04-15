package decision

import (
	"testing"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/llm"
)

func TestValidateAsymmetric_NoSafetyConfig(t *testing.T) {
	dec := &llm.ScalingDecision{TargetReplicas: 10, Confidence: 0.9}
	policy := &aiscalerv1.AIScaler{}
	policy.Spec.Constraints.MinReplicas = 1
	policy.Spec.Constraints.MaxReplicas = 20
	policy.Spec.Constraints.MaxScaleStep = 5

	result := ValidateAsymmetric(dec, 5, policy)
	if result.Direction != "up" {
		t.Errorf("expected direction up, got %s", result.Direction)
	}
}

func TestValidateAsymmetric_ScaleUpMaxStep(t *testing.T) {
	dec := &llm.ScalingDecision{TargetReplicas: 20, Confidence: 0.9}
	policy := &aiscalerv1.AIScaler{}
	policy.Spec.Constraints.MinReplicas = 1
	policy.Spec.Constraints.MaxReplicas = 50
	policy.Spec.Safety = &aiscalerv1.SafetyConfig{
		ScaleUp: &aiscalerv1.DirectionPolicy{
			MaxStep: 3,
		},
	}

	result := ValidateAsymmetric(dec, 5, policy)
	if result.ValidatedReplicas != 8 { // 5 + 3
		t.Errorf("expected clamped to 8, got %d", result.ValidatedReplicas)
	}
}

func TestValidateAsymmetric_ScaleDownMaxStep(t *testing.T) {
	dec := &llm.ScalingDecision{TargetReplicas: 1, Confidence: 0.9}
	policy := &aiscalerv1.AIScaler{}
	policy.Spec.Constraints.MinReplicas = 1
	policy.Spec.Constraints.MaxReplicas = 20
	policy.Spec.Safety = &aiscalerv1.SafetyConfig{
		ScaleDown: &aiscalerv1.DirectionPolicy{
			MaxStep: 2,
		},
	}

	result := ValidateAsymmetric(dec, 10, policy)
	if result.ValidatedReplicas != 8 { // 10 - 2
		t.Errorf("expected clamped to 8, got %d", result.ValidatedReplicas)
	}
}

func TestValidateAsymmetric_ConfidenceGate(t *testing.T) {
	dec := &llm.ScalingDecision{TargetReplicas: 15, Confidence: 0.4}
	policy := &aiscalerv1.AIScaler{}
	policy.Spec.Constraints.MinReplicas = 1
	policy.Spec.Constraints.MaxReplicas = 20
	policy.Spec.Safety = &aiscalerv1.SafetyConfig{
		ScaleUp: &aiscalerv1.DirectionPolicy{
			MaxStep:            10,
			RequireConfidence:  0.7,
		},
	}

	result := ValidateAsymmetric(dec, 5, policy)
	if result.ValidatedReplicas != 5 { // should hold — confidence too low
		t.Errorf("expected held at 5 due to low confidence, got %d", result.ValidatedReplicas)
	}
}

func TestValidateAsymmetric_NoChange(t *testing.T) {
	dec := &llm.ScalingDecision{TargetReplicas: 5, Confidence: 0.9}
	policy := &aiscalerv1.AIScaler{}
	policy.Spec.Constraints.MinReplicas = 1
	policy.Spec.Constraints.MaxReplicas = 20

	result := ValidateAsymmetric(dec, 5, policy)
	if result.Direction != "none" {
		t.Errorf("expected direction none, got %s", result.Direction)
	}
}
