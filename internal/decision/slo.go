package decision

import (
	"fmt"
	"strings"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

// SLOStatus represents the evaluation result of a single SLO.
type SLOStatus struct {
	Name     string
	Metric   string
	Target   float64
	Actual   float64
	Breached bool
	Margin   float64 // percentage margin, negative means breached
	Priority int32
}

// EvaluateSLOs checks each SLO against current signal values and returns their status.
func EvaluateSLOs(slos []aiscalerv1.SLO, bundle *plugin.Bundle) []SLOStatus {
	results := make([]SLOStatus, 0, len(slos))

	for _, slo := range slos {
		actual := resolveMetric(slo.Metric, bundle)
		margin := computeMargin(slo.Metric, slo.Target, actual)
		breached := margin < 0

		results = append(results, SLOStatus{
			Name:     slo.Name,
			Metric:   slo.Metric,
			Target:   slo.Target,
			Actual:   actual,
			Breached: breached,
			Margin:   margin,
			Priority: slo.Priority,
		})
	}

	return results
}

// AnyBreached returns true if any SLO is breached.
func AnyBreached(statuses []SLOStatus) bool {
	for _, s := range statuses {
		if s.Breached {
			return true
		}
	}
	return false
}

// FormatSLOContext builds a human-readable SLO status block for the LLM prompt.
func FormatSLOContext(statuses []SLOStatus) string {
	if len(statuses) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("SLO Status:\n")
	for _, s := range statuses {
		status := "OK"
		if s.Breached {
			status = "BREACHED"
		}
		b.WriteString(fmt.Sprintf("  - %s (%s): target=%.2f actual=%.2f [%s] margin=%.1f%%\n",
			s.Name, s.Metric, s.Target, s.Actual, status, s.Margin))
	}
	b.WriteString("\nPrioritize keeping SLOs within target. If any SLO is breached, scale up.")
	return b.String()
}

// resolveMetric maps a metric name to its current value from the signal bundle.
func resolveMetric(metric string, bundle *plugin.Bundle) float64 {
	switch metric {
	case "p95_latency":
		return bundle.P95LatencyMs
	case "p99_latency":
		// Use p95 as fallback if p99 isn't separately tracked
		return bundle.P95LatencyMs
	case "error_rate":
		return bundle.ErrorRate
	case "cpu_utilization":
		return bundle.CPUUtilization
	case "memory_utilization":
		return bundle.MemoryUtilization
	case "queue_depth":
		return bundle.QueueDepth
	default:
		// Check custom signals
		if v, ok := bundle.CustomSignals[metric]; ok {
			return v
		}
		return 0
	}
}

// computeMargin calculates the percentage margin from the target.
// For "lower is better" metrics (latency, error_rate), margin = (target - actual) / target * 100.
// For "higher is better" metrics (availability), margin = (actual - target) / target * 100.
func computeMargin(metric string, target, actual float64) float64 {
	if target == 0 {
		if actual == 0 {
			return 100 // both zero, no breach
		}
		return -100 // target is zero but actual is not
	}

	switch metric {
	case "availability":
		// Higher is better
		return (actual - target) / target * 100
	default:
		// Lower is better (latency, error_rate, queue_depth, etc.)
		return (target - actual) / target * 100
	}
}
