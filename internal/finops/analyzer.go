package finops

import (
	"context"
	"fmt"
	"time"
)

// AnalysisResult holds the outcome of a workload resource analysis.
type AnalysisResult struct {
	Workload      string
	Namespace     string
	AnalyzedAt    time.Time
	Usage         *HistoricalUsage
	IsOverProvisioned bool
	WastePercent  float64
	Recommendation string
}

// Analyzer orchestrates per-workload resource analysis.
type Analyzer struct {
	collector *HistoricalCollector
}

// NewAnalyzer creates a FinOps analyzer.
func NewAnalyzer(prometheusURL string) *Analyzer {
	return &Analyzer{
		collector: NewHistoricalCollector(prometheusURL),
	}
}

// AnalyzeWorkload performs a resource utilization analysis for a workload.
func (a *Analyzer) AnalyzeWorkload(
	ctx context.Context,
	namespace string,
	workload string,
	window time.Duration,
) (*AnalysisResult, error) {
	usage, err := a.collector.QueryUsagePercentiles(ctx, namespace, workload, window)
	if err != nil {
		return nil, fmt.Errorf("collect historical usage: %w", err)
	}

	result := &AnalysisResult{
		Workload:   workload,
		Namespace:  namespace,
		AnalyzedAt: time.Now(),
		Usage:      usage,
	}

	// Determine over-provisioning
	// If P95 CPU is under 50% of request, workload is over-provisioned
	if usage.CPUP95 > 0 && usage.CPUP95 < 50 {
		result.IsOverProvisioned = true
		result.WastePercent = 100 - usage.CPUP95
		result.Recommendation = fmt.Sprintf(
			"CPU over-provisioned: P95 utilization is %.1f%%. Consider reducing CPU request by %.0f%%",
			usage.CPUP95, result.WastePercent*0.5)
	}

	if usage.MemoryP95 > 0 && usage.MemoryP95 < 40 {
		if !result.IsOverProvisioned {
			result.IsOverProvisioned = true
		}
		memWaste := 100 - usage.MemoryP95
		if memWaste > result.WastePercent {
			result.WastePercent = memWaste
		}
		result.Recommendation += fmt.Sprintf(
			"\nMemory over-provisioned: P95 utilization is %.1f%%. Consider reducing memory request by %.0f%%",
			usage.MemoryP95, memWaste*0.5)
	}

	return result, nil
}
