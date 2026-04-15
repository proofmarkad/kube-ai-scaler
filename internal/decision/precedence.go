package decision

import "fmt"

// PrecedenceInputs holds all the decision inputs from different layers.
type PrecedenceInputs struct {
	// Layer 1: Safety (hard limits, circuit breaker trips)
	SafetyOverride *int32
	SafetyReason   string

	// Layer 2: Human override (approval, annotation freeze)
	HumanOverride *int32
	HumanReason   string

	// Layer 3: Scheduled scaling
	ScheduleTarget *int32
	ScheduleReason string

	// Layer 4: SLO-driven
	SLOTarget *int32
	SLOReason string

	// Layer 5: Reactive rules
	ReactiveTarget *int32
	ReactiveReason string

	// Layer 6: LLM decision
	LLMTarget     *int32
	LLMReason     string
	LLMConfidence float64

	// Layer 7: Deterministic advisors (pod autoscaler, KEDA)
	DeterministicTarget *int32
	DeterministicReason string

	// Layer 8: Cost constraints
	CostConstrainedMax *int32
	CostReason         string

	// Global bounds
	MinReplicas int32
	MaxReplicas int32

	// Optional precedence behavior flags. Nil preserves the default ordering.
	ScheduleOverridesLLM     *bool
	ReactiveRulesOverrideLLM *bool
	SLOAlwaysWins            *bool
}

// AuditEntry records why a particular layer affected the outcome.
type AuditEntry struct {
	Layer   string
	Target  int32
	Reason  string
	Applied bool
}

// ResolvedDecision is the final output of precedence resolution.
type ResolvedDecision struct {
	TargetReplicas int32
	Layers         []AuditEntry
	Conflicts      []string
}

// PrecedenceResolver implements the 8-layer precedence hierarchy.
type PrecedenceResolver struct{}

// NewPrecedenceResolver creates a new resolver.
func NewPrecedenceResolver() *PrecedenceResolver {
	return &PrecedenceResolver{}
}

// Resolve applies the precedence hierarchy to produce a final decision.
// Layers (highest to lowest priority):
// 1. Safety 2. Human 3. Schedule 4. SLO 5. Reactive 6. LLM 7. Deterministic 8. Cost
func (pr *PrecedenceResolver) Resolve(inputs *PrecedenceInputs) *ResolvedDecision {
	rd := &ResolvedDecision{}

	type layer struct {
		name   string
		target *int32
		reason string
	}

	addLayer := func(layers []layer, name string, target *int32, reason string) []layer {
		return append(layers, layer{name: name, target: target, reason: reason})
	}

	scheduleFirst := boolFlagOrDefault(inputs.ScheduleOverridesLLM, true)
	reactiveFirst := boolFlagOrDefault(inputs.ReactiveRulesOverrideLLM, true)
	sloFirst := boolFlagOrDefault(inputs.SLOAlwaysWins, true)

	layers := make([]layer, 0, 7)
	layers = addLayer(layers, "safety", inputs.SafetyOverride, inputs.SafetyReason)
	layers = addLayer(layers, "human", inputs.HumanOverride, inputs.HumanReason)
	if scheduleFirst {
		layers = addLayer(layers, "schedule", inputs.ScheduleTarget, inputs.ScheduleReason)
	}
	if sloFirst {
		layers = addLayer(layers, "slo", inputs.SLOTarget, inputs.SLOReason)
	}
	if reactiveFirst {
		layers = addLayer(layers, "reactive", inputs.ReactiveTarget, inputs.ReactiveReason)
	}
	layers = addLayer(layers, "llm", inputs.LLMTarget, inputs.LLMReason)
	if !reactiveFirst {
		layers = addLayer(layers, "reactive", inputs.ReactiveTarget, inputs.ReactiveReason)
	}
	if !sloFirst {
		layers = addLayer(layers, "slo", inputs.SLOTarget, inputs.SLOReason)
	}
	if !scheduleFirst {
		layers = addLayer(layers, "schedule", inputs.ScheduleTarget, inputs.ScheduleReason)
	}
	layers = addLayer(layers, "deterministic", inputs.DeterministicTarget, inputs.DeterministicReason)

	var winner *struct {
		name   string
		target int32
		reason string
	}

	for _, layer := range layers {
		if layer.target == nil {
			continue
		}

		entry := AuditEntry{
			Layer:  layer.name,
			Target: *layer.target,
			Reason: layer.reason,
		}

		if winner == nil {
			winner = &struct {
				name   string
				target int32
				reason string
			}{layer.name, *layer.target, layer.reason}
			entry.Applied = true
		} else {
			// Record conflict if this layer disagrees
			if *layer.target != winner.target {
				rd.Conflicts = append(rd.Conflicts,
					fmt.Sprintf("%s: wanted %d but overridden by %s", layer.name, *layer.target, winner.name))
			}
		}

		rd.Layers = append(rd.Layers, entry)
	}

	if winner != nil {
		rd.TargetReplicas = winner.target
	}

	// Apply cost constraint as a ceiling
	if inputs.CostConstrainedMax != nil && rd.TargetReplicas > *inputs.CostConstrainedMax {
		rd.Conflicts = append(rd.Conflicts, "cost: capped from target to budget max")
		rd.TargetReplicas = *inputs.CostConstrainedMax
		rd.Layers = append(rd.Layers, AuditEntry{
			Layer:   "cost",
			Target:  *inputs.CostConstrainedMax,
			Reason:  inputs.CostReason,
			Applied: true,
		})
	}

	// Enforce global bounds
	if rd.TargetReplicas < inputs.MinReplicas {
		rd.TargetReplicas = inputs.MinReplicas
	}
	if inputs.MaxReplicas > 0 && rd.TargetReplicas > inputs.MaxReplicas {
		rd.TargetReplicas = inputs.MaxReplicas
	}

	return rd
}

func boolFlagOrDefault(flag *bool, defaultValue bool) bool {
	if flag == nil {
		return defaultValue
	}
	return *flag
}
