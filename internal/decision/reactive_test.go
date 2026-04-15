package decision

import (
	"testing"

	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

func TestEvaluateReactiveRules_NoRules(t *testing.T) {
	result := EvaluateReactiveRules(nil, &plugin.Bundle{})
	if result != nil {
		t.Error("expected nil for no rules")
	}
}

func TestEvaluateReactiveRules_RuleFires(t *testing.T) {
	rules := []plugin.ReactiveRule{
		{Metric: "error_rate", Operator: ">", Threshold: 5.0, Action: "scale_up", Amount: 2},
	}
	bundle := &plugin.Bundle{
		ErrorRate:       10.0,
		CurrentReplicas: 3,
	}

	result := EvaluateReactiveRules(rules, bundle)
	if result == nil {
		t.Fatal("expected rule to fire")
	}
	if !result.Fired {
		t.Error("expected Fired=true")
	}
	if result.Replicas != 5 {
		t.Errorf("expected 5 replicas, got %d", result.Replicas)
	}
}

func TestEvaluateReactiveRules_RuleDoesNotFire(t *testing.T) {
	rules := []plugin.ReactiveRule{
		{Metric: "error_rate", Operator: ">", Threshold: 5.0, Action: "scale_up", Amount: 2},
	}
	bundle := &plugin.Bundle{
		ErrorRate:       2.0,
		CurrentReplicas: 3,
	}

	result := EvaluateReactiveRules(rules, bundle)
	if result != nil {
		t.Error("expected rule not to fire")
	}
}

func TestEvaluateReactiveRules_FirstMatchWins(t *testing.T) {
	rules := []plugin.ReactiveRule{
		{Metric: "cpu_utilization", Operator: ">", Threshold: 80, Action: "scale_up", Amount: 1},
		{Metric: "error_rate", Operator: ">", Threshold: 5, Action: "scale_up", Amount: 5},
	}
	bundle := &plugin.Bundle{
		CPUUtilization:  90,
		ErrorRate:       10,
		CurrentReplicas: 3,
	}

	result := EvaluateReactiveRules(rules, bundle)
	if result == nil {
		t.Fatal("expected a rule to fire")
	}
	// First rule should win
	if result.Rule.Metric != "cpu_utilization" {
		t.Errorf("expected cpu_utilization rule to fire first, got %q", result.Rule.Metric)
	}
	if result.Replicas != 4 {
		t.Errorf("expected 4 replicas, got %d", result.Replicas)
	}
}

func TestEvalCondition(t *testing.T) {
	tests := []struct {
		actual    float64
		op        string
		threshold float64
		want      bool
	}{
		{10, ">", 5, true},
		{5, ">", 5, false},
		{5, ">=", 5, true},
		{3, "<", 5, true},
		{5, "<", 5, false},
		{5, "<=", 5, true},
		{5, "==", 5, true},
		{5, "==", 6, false},
		{5, "!=", 6, true},
		{5, "!=", 5, false},
		{5, "invalid", 5, false},
	}

	for _, tt := range tests {
		got := evalCondition(tt.actual, tt.op, tt.threshold)
		if got != tt.want {
			t.Errorf("evalCondition(%.1f, %q, %.1f) = %v, want %v",
				tt.actual, tt.op, tt.threshold, got, tt.want)
		}
	}
}

func TestComputeReactiveReplicas(t *testing.T) {
	tests := []struct {
		action  string
		amount  int32
		current int32
		want    int32
	}{
		{"scale_up", 2, 3, 5},
		{"scale_down", 1, 3, 2},
		{"scale_down", 5, 3, 1}, // clamp to 1
		{"set", 10, 3, 10},
		{"unknown", 5, 3, 3}, // no change
	}

	for _, tt := range tests {
		rule := plugin.ReactiveRule{Action: tt.action, Amount: tt.amount}
		got := computeReactiveReplicas(rule, tt.current)
		if got != tt.want {
			t.Errorf("computeReactiveReplicas(%q, %d, %d) = %d, want %d",
				tt.action, tt.amount, tt.current, got, tt.want)
		}
	}
}
