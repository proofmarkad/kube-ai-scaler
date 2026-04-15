package decision

import (
	"testing"
)

func int32Ptr(v int32) *int32 { return &v }

func TestPrecedenceResolver_SafetyWins(t *testing.T) {
	pr := NewPrecedenceResolver()
	inputs := &PrecedenceInputs{
		SafetyOverride: int32Ptr(2),
		SafetyReason:   "circuit breaker",
		LLMTarget:      int32Ptr(10),
		LLMReason:      "scale up",
		MinReplicas:    1,
		MaxReplicas:    20,
	}

	result := pr.Resolve(inputs)
	if result.TargetReplicas != 2 {
		t.Errorf("expected safety override 2, got %d", result.TargetReplicas)
	}
	if len(result.Conflicts) == 0 {
		t.Error("expected conflict between safety and LLM")
	}
}

func TestPrecedenceResolver_HumanOverridesLLM(t *testing.T) {
	pr := NewPrecedenceResolver()
	inputs := &PrecedenceInputs{
		HumanOverride: int32Ptr(5),
		HumanReason:   "manual override",
		LLMTarget:     int32Ptr(10),
		LLMReason:     "scale up",
		MinReplicas:   1,
		MaxReplicas:   20,
	}

	result := pr.Resolve(inputs)
	if result.TargetReplicas != 5 {
		t.Errorf("expected human override 5, got %d", result.TargetReplicas)
	}
}

func TestPrecedenceResolver_LLMOnly(t *testing.T) {
	pr := NewPrecedenceResolver()
	inputs := &PrecedenceInputs{
		LLMTarget:   int32Ptr(8),
		LLMReason:   "cpu high",
		MinReplicas: 1,
		MaxReplicas: 20,
	}

	result := pr.Resolve(inputs)
	if result.TargetReplicas != 8 {
		t.Errorf("expected LLM target 8, got %d", result.TargetReplicas)
	}
	if len(result.Conflicts) != 0 {
		t.Error("expected no conflicts with single layer")
	}
}

func TestPrecedenceResolver_CostCeiling(t *testing.T) {
	pr := NewPrecedenceResolver()
	inputs := &PrecedenceInputs{
		LLMTarget:          int32Ptr(15),
		LLMReason:          "scale up needed",
		CostConstrainedMax: int32Ptr(10),
		CostReason:         "budget limit",
		MinReplicas:        1,
		MaxReplicas:        20,
	}

	result := pr.Resolve(inputs)
	if result.TargetReplicas != 10 {
		t.Errorf("expected cost-capped target 10, got %d", result.TargetReplicas)
	}
}

func TestPrecedenceResolver_MinMaxBounds(t *testing.T) {
	pr := NewPrecedenceResolver()
	inputs := &PrecedenceInputs{
		LLMTarget:   int32Ptr(0),
		LLMReason:   "scale to zero",
		MinReplicas: 2,
		MaxReplicas: 20,
	}

	result := pr.Resolve(inputs)
	if result.TargetReplicas != 2 {
		t.Errorf("expected min-bound 2, got %d", result.TargetReplicas)
	}
}

func TestPrecedenceResolver_MaxBound(t *testing.T) {
	pr := NewPrecedenceResolver()
	inputs := &PrecedenceInputs{
		LLMTarget:   int32Ptr(100),
		LLMReason:   "massive scale up",
		MinReplicas: 1,
		MaxReplicas: 20,
	}

	result := pr.Resolve(inputs)
	if result.TargetReplicas != 20 {
		t.Errorf("expected max-bound 20, got %d", result.TargetReplicas)
	}
}

func TestPrecedenceResolver_NoInputs(t *testing.T) {
	pr := NewPrecedenceResolver()
	inputs := &PrecedenceInputs{
		MinReplicas: 1,
		MaxReplicas: 20,
	}

	result := pr.Resolve(inputs)
	if result.TargetReplicas != 1 {
		t.Errorf("expected min replicas 1 with no inputs, got %d", result.TargetReplicas)
	}
}

func TestPrecedenceResolver_AuditTrail(t *testing.T) {
	pr := NewPrecedenceResolver()
	inputs := &PrecedenceInputs{
		SafetyOverride: int32Ptr(3),
		SafetyReason:   "oscillation",
		LLMTarget:      int32Ptr(10),
		LLMReason:      "cpu spike",
		ReactiveTarget: int32Ptr(8),
		ReactiveReason: "rule fired",
		MinReplicas:    1,
		MaxReplicas:    20,
	}

	result := pr.Resolve(inputs)
	if len(result.Layers) != 3 {
		t.Errorf("expected 3 audit layers, got %d", len(result.Layers))
	}

	// First layer should be safety and should be applied
	if result.Layers[0].Layer != "safety" || !result.Layers[0].Applied {
		t.Error("expected safety layer to be first and applied")
	}
}

func TestPrecedenceResolver_ConflictMessageSupportsMultiDigitTargets(t *testing.T) {
	pr := NewPrecedenceResolver()
	inputs := &PrecedenceInputs{
		SafetyOverride: int32Ptr(3),
		SafetyReason:   "guardrail",
		LLMTarget:      int32Ptr(12),
		LLMReason:      "cpu spike",
		MinReplicas:    1,
		MaxReplicas:    20,
	}

	result := pr.Resolve(inputs)
	if len(result.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %d", len(result.Conflicts))
	}
	if result.Conflicts[0] != "llm: wanted 12 but overridden by safety" {
		t.Fatalf("unexpected conflict message: %q", result.Conflicts[0])
	}
}

func TestPrecedenceResolver_LLMCanOverrideReactiveWhenConfigured(t *testing.T) {
	pr := NewPrecedenceResolver()
	preferReactive := false
	inputs := &PrecedenceInputs{
		ReactiveTarget:           int32Ptr(10),
		ReactiveReason:           "rule fired",
		LLMTarget:                int32Ptr(6),
		LLMReason:                "moderate cpu",
		ReactiveRulesOverrideLLM: &preferReactive,
		MinReplicas:              1,
		MaxReplicas:              20,
	}

	result := pr.Resolve(inputs)
	if result.TargetReplicas != 6 {
		t.Fatalf("expected llm target 6, got %d", result.TargetReplicas)
	}
	if len(result.Conflicts) != 1 || result.Conflicts[0] != "reactive: wanted 10 but overridden by llm" {
		t.Fatalf("unexpected conflicts: %#v", result.Conflicts)
	}
}
