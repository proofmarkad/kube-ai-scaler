package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// Scaling decisions
	ScalingDecisions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aiscaler_scaling_decisions_total",
			Help: "Total scaling decisions by direction and provider",
		},
		[]string{"aiscaler", "provider", "direction"},
	)

	// Current replicas gauge
	CurrentReplicas = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_current_replicas",
			Help: "Current replica count per managed workload",
		},
		[]string{"aiscaler", "namespace", "deployment"},
	)

	// Desired replicas gauge
	DesiredReplicas = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_desired_replicas",
			Help: "Desired replica count from last decision",
		},
		[]string{"aiscaler", "namespace", "deployment"},
	)

	// LLM latency histogram
	LLMLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aiscaler_llm_latency_seconds",
			Help:    "LLM API call latency in seconds",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		},
		[]string{"provider", "model"},
	)

	// LLM errors counter
	LLMErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aiscaler_llm_errors_total",
			Help: "LLM API call errors by provider and error type",
		},
		[]string{"provider", "error_type"},
	)

	// LLM confidence histogram
	LLMConfidence = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aiscaler_llm_confidence",
			Help:    "LLM decision confidence score distribution",
			Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0},
		},
		[]string{"provider"},
	)

	// Validation clamp counter
	ValidationClamps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aiscaler_validation_clamps_total",
			Help: "Times validator clamped an LLM decision",
		},
		[]string{"aiscaler", "reason"},
	)

	// Reconcile duration histogram
	ReconcileDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aiscaler_reconcile_duration_seconds",
			Help:    "Total reconcile cycle duration",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
		[]string{"aiscaler"},
	)

	// Reconcile errors counter
	ReconcileErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aiscaler_reconcile_errors_total",
			Help: "Reconcile cycle errors by step and type",
		},
		[]string{"aiscaler", "step"},
	)

	// Signal plugin health gauge
	SignalPluginHealth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_signal_plugin_healthy",
			Help: "Health status of each signal plugin (1=healthy, 0=unhealthy)",
		},
		[]string{"aiscaler", "plugin"},
	)

	// Signal collection latency per plugin
	SignalCollectionLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aiscaler_signal_collection_seconds",
			Help:    "Signal collection latency per plugin",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
		},
		[]string{"plugin"},
	)

	// Circuit breaker state gauge
	CircuitBreakerState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_circuit_breaker_state",
			Help: "Circuit breaker state (0=closed, 1=half-open, 2=open)",
		},
		[]string{"provider"},
	)

	// Cost per hour gauge
	CostPerHour = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_cost_per_hour_dollars",
			Help: "Estimated hourly cost of managed workload",
		},
		[]string{"aiscaler", "namespace", "deployment"},
	)

	// SLO breach gauge
	SLOBreach = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_slo_breach",
			Help: "SLO breach status (1=breached, 0=ok)",
		},
		[]string{"aiscaler", "slo_name", "metric"},
	)

	// Cache hit/miss counter
	CacheHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aiscaler_llm_cache_hits_total",
			Help: "LLM response cache hits and misses",
		},
		[]string{"result"},
	)

	// DryRun decisions counter
	DryRunDecisions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aiscaler_dryrun_decisions_total",
			Help: "Scaling decisions made in dry-run mode",
		},
		[]string{"aiscaler", "direction"},
	)

	// Oscillation detected gauge
	OscillationDetected = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_oscillation_detected",
			Help: "Whether oscillation was detected (1=yes, 0=no)",
		},
		[]string{"aiscaler"},
	)

	// SLO headroom gauge
	SLOHeadroom = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_slo_headroom_percent",
			Help: "SLO headroom percentage per SLO target",
		},
		[]string{"aiscaler", "slo_name"},
	)

	// Workload waste percentage
	WorkloadWaste = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_workload_waste_percent",
			Help: "Estimated resource waste percentage",
		},
		[]string{"aiscaler", "namespace", "deployment"},
	)

	// Cost savings hourly
	CostSavingsHourly = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_cost_savings_hourly",
			Help: "Estimated hourly cost savings from scaling",
		},
		[]string{"aiscaler"},
	)

	// Approval pending gauge
	ApprovalPending = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_approval_pending",
			Help: "Number of pending approval requests",
		},
		[]string{"aiscaler"},
	)

	// Prediction confidence
	PredictionConfidence = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aiscaler_prediction_confidence",
			Help: "Confidence of the seasonal prediction",
		},
		[]string{"aiscaler", "metric"},
	)

	// Vertical resize counter
	VerticalResizes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aiscaler_vertical_resizes_total",
			Help: "Total vertical resize operations",
		},
		[]string{"aiscaler", "strategy"},
	)
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		ScalingDecisions, CurrentReplicas, DesiredReplicas,
		LLMLatency, LLMErrors, LLMConfidence,
		ValidationClamps, ReconcileDuration, ReconcileErrors,
		SignalPluginHealth, SignalCollectionLatency,
		CircuitBreakerState, CostPerHour, SLOBreach,
		CacheHits, DryRunDecisions,
		OscillationDetected, SLOHeadroom, WorkloadWaste,
		CostSavingsHourly, ApprovalPending,
		PredictionConfidence, VerticalResizes,
	)
}

// ClassifyError maps an error to a category for metrics.
func ClassifyError(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	switch {
	case contains(s, "timeout", "deadline"):
		return "timeout"
	case contains(s, "401", "403", "unauthorized", "forbidden"):
		return "auth"
	case contains(s, "parse", "unmarshal", "json"):
		return "parse"
	default:
		return "server"
	}
}

func contains(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
