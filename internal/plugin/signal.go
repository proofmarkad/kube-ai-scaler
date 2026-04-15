package plugin

import (
	"context"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

// Bundle holds all collected signals for one reconcile cycle.
type Bundle struct {
	// Core metrics (from metrics-server)
	CPUUtilization    float64
	MemoryUtilization float64
	CurrentReplicas   int32
	ReadyReplicas     int32
	DeploymentReady   bool

	// Prometheus / custom queries
	P95LatencyMs float64
	ErrorRate    float64

	// Queue-based signals
	QueueDepth float64

	// External scaler signals
	KEDADesiredReplicas int32

	// Cluster-level signals
	ClusterNodes        int32
	ClusterCPUAvailable float64
	ClusterMemAvailable float64

	// Cost signals
	CostPerHour    float64
	CostEfficiency float64

	// Custom signals (plugin-defined, key→value)
	CustomSignals map[string]float64

	// Annotation-based human intent
	Annotations AnnotationSignals

	// Metadata
	CollectedAt  time.Time
	SourceHealth map[string]bool // plugin-name → healthy
}

// AnnotationSignals holds human-intent signals from deployment annotations.
type AnnotationSignals struct {
	ExpectedTraffic     string
	ScaleConservatively bool
	FreezeUntil         *time.Time
	Note                string
	PeakHours           string
	ReactiveRules       []ReactiveRule
}

// ReactiveRule defines an annotation-based reactive scaling rule.
type ReactiveRule struct {
	Metric    string  `json:"metric"`
	Operator  string  `json:"operator"`
	Threshold float64 `json:"threshold"`
	Action    string  `json:"action"`
	Amount    int32   `json:"amount"`
}

// SignalPlugin is the interface every signal source must implement.
type SignalPlugin interface {
	// Name returns a unique identifier (e.g., "prometheus", "datadog", "aws-sqs").
	Name() string

	// Init is called once with the plugin's config block.
	Init(cfg map[string]string) error

	// Collect gathers signals and writes them into the bundle.
	Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *Bundle) error

	// Required returns true if this plugin's failure should abort the reconcile.
	Required() bool

	// Healthy returns the current health status of this plugin.
	Healthy() bool
}

// K8sPluginDeps holds shared Kubernetes dependencies for built-in plugins.
type K8sPluginDeps struct {
	Client        interface{} // client.Client
	MetricsClient interface{} // metricsclient.Interface
}

// K8sAwarePlugin is optionally implemented by plugins that need K8s clients.
type K8sAwarePlugin interface {
	SetK8sDeps(deps K8sPluginDeps)
}

// SecretReader is optionally implemented by plugins that need secrets.
type SecretReader interface {
	SetSecretData(data map[string][]byte)
}
