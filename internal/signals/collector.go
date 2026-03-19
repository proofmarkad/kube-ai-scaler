package signals

import (
	"context"
	"fmt"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// Bundle holds all signals collected for a single reconcile cycle.
// This is what gets passed to the LLM router as context.
type Bundle struct {
	// From metrics server
	CPUUtilization    float64 // percentage 0-100
	MemoryUtilization float64 // percentage 0-100

	// From prometheus
	P95LatencyMs float64 // p95 request latency in ms
	ErrorRate    float64 // // error rate as fraction 0-1

	// From Deployment
	CurrentReplicas int32
	ReadyReplicas   int32
	DeploymentReady bool

	// Time context
	CollectedAt time.Time

	// Human intent from annotations
	Annotations AnnotationSignals
}

// AnnotationSignals holds the fixed set of annotations we read from the target Deployment.
type AnnotationSignals struct {
	ExpectedTraffic     string // low | normal | high | critical
	ScaleConservatively bool
	FreezeUntil         *time.Time
	Note                string
	PeakHours           string // HH:MM-HH:MM TZ
}

// Collector orchestrates all signal fetching for a given AIScaler.
type Collector struct {
	metrics     *metricsCollector
	prometheus  *prometheusCollector
	annotations *annotationCollector
}

// NewCollector creates a new Collector.
func NewCollector(client client.Client) *Collector {
	cfg, err := config.GetConfig()
	if err != nil {
		panic(fmt.Sprintf("failed to get k8s config: %v", err))
	}

	metricsClient, err := metricsclient.NewForConfig(cfg)
	if err != nil {
		panic(fmt.Sprintf("failed to create metrics client: %v", err))
	}

	return &Collector{
		metrics:     &metricsCollector{client: client, metricsClient: metricsClient},
		prometheus:  &prometheusCollector{},
		annotations: &annotationCollector{client: client},
	}
}

// Collect gathers all signals for the given AIScaler and returns a Bundle.
// Prometheus failures are tolerated — the LLM works with whatever is available.
// Deployment fetch failures are fatal since replicas and health are core signals.
func (c *Collector) Collect(ctx context.Context, policy *aiscalerv1.AIScaler) (*Bundle, error) {
	bundle := &Bundle{
		CollectedAt: time.Now(),
	}

	// Deployment health + current replicas — fatal if this fails
	if err := c.metrics.collect(ctx, policy, bundle); err != nil {
		return nil, fmt.Errorf("failed to collect metrics: %w", err)
	}

	// Prometheus signals — non-fatal if Prometheus is unreachable
	if err := c.prometheus.collect(ctx, policy, bundle); err != nil {
		fmt.Printf("failed to collect prometheus signals: %v\n", err)
	}

	// Annotation signals — non-fatal
	if err := c.annotations.collect(ctx, policy, bundle); err != nil {
		fmt.Printf("failed to collect annotations: %v\n", err)
	}

	return bundle, nil
}
