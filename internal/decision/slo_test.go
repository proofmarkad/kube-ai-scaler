package decision

import (
	"testing"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

func TestEvaluateSLOs_AllOK(t *testing.T) {
	slos := []aiscalerv1.SLO{
		{Name: "latency-slo", Metric: "p95_latency", Target: 200},
		{Name: "error-slo", Metric: "error_rate", Target: 1.0},
	}
	bundle := &plugin.Bundle{
		P95LatencyMs: 150,
		ErrorRate:    0.5,
	}

	statuses := EvaluateSLOs(slos, bundle)
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}
	for _, s := range statuses {
		if s.Breached {
			t.Errorf("expected SLO %q to not be breached", s.Name)
		}
		if s.Margin < 0 {
			t.Errorf("expected positive margin for %q, got %.2f", s.Name, s.Margin)
		}
	}
}

func TestEvaluateSLOs_Breached(t *testing.T) {
	slos := []aiscalerv1.SLO{
		{Name: "latency-slo", Metric: "p95_latency", Target: 200},
	}
	bundle := &plugin.Bundle{
		P95LatencyMs: 450,
	}

	statuses := EvaluateSLOs(slos, bundle)
	if len(statuses) != 1 {
		t.Fatalf("expected 1 status, got %d", len(statuses))
	}
	if !statuses[0].Breached {
		t.Error("expected SLO to be breached")
	}
	if statuses[0].Margin >= 0 {
		t.Errorf("expected negative margin, got %.2f", statuses[0].Margin)
	}
}

func TestEvaluateSLOs_CustomSignal(t *testing.T) {
	slos := []aiscalerv1.SLO{
		{Name: "custom-slo", Metric: "my_metric", Target: 100},
	}
	bundle := &plugin.Bundle{
		CustomSignals: map[string]float64{"my_metric": 50},
	}

	statuses := EvaluateSLOs(slos, bundle)
	if statuses[0].Breached {
		t.Error("expected custom SLO to not be breached")
	}
}

func TestAnyBreached(t *testing.T) {
	statuses := []SLOStatus{
		{Breached: false},
		{Breached: true},
	}
	if !AnyBreached(statuses) {
		t.Error("expected AnyBreached to return true")
	}
}

func TestAnyBreached_AllOK(t *testing.T) {
	statuses := []SLOStatus{
		{Breached: false},
		{Breached: false},
	}
	if AnyBreached(statuses) {
		t.Error("expected AnyBreached to return false")
	}
}

func TestFormatSLOContext_Empty(t *testing.T) {
	result := FormatSLOContext(nil)
	if result != "" {
		t.Errorf("expected empty string for nil statuses, got %q", result)
	}
}

func TestFormatSLOContext_WithBreaches(t *testing.T) {
	statuses := []SLOStatus{
		{Name: "slo1", Metric: "p95_latency", Target: 200, Actual: 450, Breached: true, Margin: -125},
		{Name: "slo2", Metric: "error_rate", Target: 0.1, Actual: 0.05, Breached: false, Margin: 50},
	}

	result := FormatSLOContext(statuses)
	if result == "" {
		t.Fatal("expected non-empty context")
	}
	if !contains(result, "BREACHED") {
		t.Error("expected BREACHED in context")
	}
	if !contains(result, "OK") {
		t.Error("expected OK in context")
	}
}

func TestComputeMargin_ZeroTarget(t *testing.T) {
	// target=0, actual=0 → 100 margin
	m := computeMargin("error_rate", 0, 0)
	if m != 100 {
		t.Errorf("expected 100 margin for zero/zero, got %.2f", m)
	}

	// target=0, actual>0 → -100 margin
	m = computeMargin("error_rate", 0, 5)
	if m != -100 {
		t.Errorf("expected -100 margin for zero/nonzero, got %.2f", m)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
