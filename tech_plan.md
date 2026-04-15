# AI Scaler — Complete Technical Implementation Plan (v2)

> **Project Revamp**: This document is a ground-up redesign of the AI Scaler
> operator. It introduces a **plugin architecture** where every external
> dependency (SQS, Datadog, KEDA, Prometheus, OpenCost, etc.) is a
> configurable, swappable plugin. It adds comprehensive tests at every layer,
> optimized monitoring/alerting, and production-grade installation guides for
> **AWS EKS** and **GCP GKE**.

> Implementation status note: several items described below as target-state are already present in the repository, including the signal plugin registry, real unit coverage, precedence and rollback wiring, coordination gates, and production-oriented example manifests. Use the README and /examples for the current operator contract; keep this document as the larger redesign plan.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Plugin System Design](#plugin-system-design)
- [Phase 0 — Bug Fixes & Housekeeping](#phase-0--bug-fixes--housekeeping)
- [Phase 1 — Plugin Architecture & Signal Source Framework](#phase-1--plugin-architecture--signal-source-framework)
- [Phase 2 — Built-in Signal Plugins](#phase-2--built-in-signal-plugins)
- [Phase 3 — External Signal Plugins (SQS, Datadog, KEDA, etc.)](#phase-3--external-signal-plugins-sqs-datadog-keda-etc)
- [Phase 4 — Configurable LLM Provider System](#phase-4--configurable-llm-provider-system)
- [Phase 5 — Monitoring & Alerting (Optimized)](#phase-5--monitoring--alerting-optimized)
- [Phase 6 — SLO-First Scaling & Reactive Rules](#phase-6--slo-first-scaling--reactive-rules)
- [Phase 7 — Vertical Scaling & In-Place Resize](#phase-7--vertical-scaling--in-place-resize)
- [Phase 8 — Predictive & Seasonal Scaling](#phase-8--predictive--seasonal-scaling)
- [Phase 9 — Cost Optimization & FinOps](#phase-9--cost-optimization--finops)
- [Phase 10 — Safety, Guardrails & Production Hardening](#phase-10--safety-guardrails--production-hardening)
- [Phase 11 — Multi-Workload & Cluster-Wide Reasoning](#phase-11--multi-workload--cluster-wide-reasoning)
- [Phase 12 — Central Dashboard](#phase-12--central-dashboard)
- [Phase 13 — Comprehensive Test Suite](#phase-13--comprehensive-test-suite)
- [Phase 14 — Installation Guide (AWS EKS)](#phase-14--installation-guide-aws-eks)
- [Phase 15 — Installation Guide (GCP GKE)](#phase-15--installation-guide-gcp-gke)
- [Appendix A — File Index](#appendix-a--file-index)
- [Appendix B — CRD Reference](#appendix-b--crd-reference)
- [Appendix C — Configuration Reference](#appendix-c--configuration-reference)

---

## Architecture Overview

### Current State (Problems)

The current codebase has these fundamental issues:

1. **Tightly coupled** — Signal sources (metrics-server, Prometheus, annotations) are hard-coded in the collector
2. **No plugin system** — Adding a new signal source (SQS, Datadog, Kafka) requires editing core files
3. **No monitoring of the operator itself** — No Prometheus metrics exported, no alerting rules
4. **No tests** — Only scaffold tests, zero real coverage
5. **No install guide** — No IAM/RBAC documentation for cloud providers
6. **11 known bugs** — Critical issues in CPU calculation, RBAC markers, config parsing
7. **Single config pattern** — No per-source configuration, all or nothing

### Target Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                        AIScaler CRD (v1)                          │
│  spec.signals[] — dynamic list of signal plugin configs           │
│  spec.llm       — provider config (pluggable)                    │
│  spec.alerting  — alerting rules (pluggable)                     │
│  spec.cost      — cost engine config (pluggable)                 │
└─────────────────────────┬─────────────────────────────────────────┘
                          │
                ┌─────────▼─────────┐
                │   Reconciler      │
                │   (Step Chain)    │
                └─────────┬─────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
   ┌────▼────┐    ┌───────▼───────┐   ┌────▼────┐
   │ Signal  │    │  LLM Router   │   │Actuator │
   │ Manager │    │  (pluggable)  │   │  (SSA)  │
   └────┬────┘    └───────────────┘   └─────────┘
        │
   ┌────┴─────────────────────────────────┐
   │         Plugin Registry              │
   ├──────────┬──────────┬───────────┬────┤
   │metrics-  │prometheus│ datadog   │sqs │
   │server    │          │          │    │
   ├──────────┼──────────┼───────────┼────┤
   │keda      │cloudwatch│ kafka     │... │
   │          │          │ (custom)  │    │
   └──────────┴──────────┴───────────┴────┘
```

### Design Principles

1. **Plugin-first**: Every external dependency is a plugin implementing an interface
2. **Config-driven**: Everything configurable via CRD spec or operator config — zero code changes to add/remove integrations
3. **Observable**: Every component emits Prometheus metrics and structured logs
4. **Testable**: Every package has unit tests; integration tests use envtest; e2e tests use a real cluster
5. **Secure**: RBAC least-privilege, secrets never logged, IAM roles per cloud provider

---

## Plugin System Design

### Signal Source Plugin Interface

Every signal source (metrics-server, Prometheus, Datadog, SQS, KEDA, CloudWatch, custom) implements this interface:

```go
// internal/plugin/signal.go
package plugin

import (
    "context"
    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

// SignalPlugin is the interface every signal source must implement.
type SignalPlugin interface {
    // Name returns a unique identifier (e.g., "prometheus", "datadog", "aws-sqs").
    Name() string

    // Init is called once at startup with the plugin's config block.
    // Use it to create HTTP clients, SDK clients, etc.
    Init(cfg map[string]string) error

    // Collect gathers signals and writes them into the bundle.
    // Returning an error marks this source as unhealthy.
    Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *Bundle) error

    // Required returns true if this plugin's failure should abort the reconcile.
    Required() bool

    // Healthy returns the current health status of this plugin.
    Healthy() bool
}
```

### Plugin Registry

```go
// internal/plugin/registry.go
package plugin

import (
    "fmt"
    "sync"
)

// registry holds all registered signal plugins by name.
var (
    mu       sync.RWMutex
    registry = make(map[string]func() SignalPlugin)
)

// Register adds a plugin factory to the registry.
// Called in init() by each plugin package.
func Register(name string, factory func() SignalPlugin) {
    mu.Lock()
    defer mu.Unlock()
    if _, exists := registry[name]; exists {
        panic(fmt.Sprintf("signal plugin %q already registered", name))
    }
    registry[name] = factory
}

// Get returns a new instance of the named plugin.
func Get(name string) (SignalPlugin, error) {
    mu.RLock()
    defer mu.RUnlock()
    factory, ok := registry[name]
    if !ok {
        return nil, fmt.Errorf("unknown signal plugin %q", name)
    }
    return factory(), nil
}

// List returns all registered plugin names.
func List() []string {
    mu.RLock()
    defer mu.RUnlock()
    names := make([]string, 0, len(registry))
    for name := range registry {
        names = append(names, name)
    }
    return names
}
```

### CRD Signal Config (New Design)

```go
// In api/v1/aiscaler_types.go

type SignalSourceConfig struct {
    // Plugin name from the registry (e.g., "prometheus", "datadog", "aws-sqs")
    // +kubebuilder:validation:Required
    Name string `json:"name"`

    // Whether this source is required (failure aborts reconcile)
    // +kubebuilder:default=false
    Required bool `json:"required,omitempty"`

    // Plugin-specific configuration as key-value pairs
    // +optional
    Config map[string]string `json:"config,omitempty"`

    // Reference to a Secret for sensitive config (API keys, tokens)
    // +optional
    SecretRef *SecretRef `json:"secretRef,omitempty"`
}

type AIScalerSpec struct {
    // +kubebuilder:validation:Required
    TargetRef TargetRef `json:"targetRef"`

    // +kubebuilder:validation:Required
    Constraints ScalingConstraints `json:"constraints"`

    // +kubebuilder:validation:Required
    LLM LLMConfig `json:"llm"`

    // Signal sources — each entry enables a plugin
    // +optional
    Signals []SignalSourceConfig `json:"signals,omitempty"`

    // Alerting configuration
    // +optional
    Alerting *AlertingConfig `json:"alerting,omitempty"`

    // Cost/FinOps configuration
    // +optional
    Cost *CostConfig `json:"cost,omitempty"`

    // SLO definitions
    // +optional
    SLOs []SLO `json:"slos,omitempty"`

    // Vertical scaling configuration
    // +optional
    VerticalScaling *VerticalScalingConfig `json:"verticalScaling,omitempty"`

    // +kubebuilder:default="60s"
    EvaluationInterval metav1.Duration `json:"evaluationInterval,omitempty"`

    // +kubebuilder:default="300s"
    CooldownPeriod metav1.Duration `json:"cooldownPeriod,omitempty"`

    // +kubebuilder:default=false
    DryRun bool `json:"dryRun,omitempty"`

    // Require human approval for scaling decisions
    // +kubebuilder:default=false
    RequireApproval bool `json:"requireApproval,omitempty"`
}
```

### Example CRD with Plugins

```yaml
apiVersion: aiscaler.io/v1
kind: AIScaler
metadata:
  name: api-gateway
spec:
  targetRef:
    name: api-gateway
    namespace: production

  constraints:
    minReplicas: 2
    maxReplicas: 50
    maxScaleStep: 5
    minConfidence: 0.6

  llm:
    primary: anthropic
    fallback: gemini
    model: claude-sonnet-4-5-20250514

  # --- Plugin-based signal sources ---
  signals:
    - name: metrics-server        # Built-in: CPU/memory from metrics-server
      required: true

    - name: prometheus            # Built-in: PromQL queries
      required: false
      config:
        baseURL: "http://prometheus:9090"
        p95LatencyQuery: 'histogram_quantile(0.95, rate(http_request_duration_seconds_bucket{service="api-gateway"}[5m]))'
        errorRateQuery: 'sum(rate(http_requests_total{service="api-gateway",code=~"5.."}[5m])) / sum(rate(http_requests_total{service="api-gateway"}[5m]))'

    - name: datadog               # External: Datadog APM metrics
      required: false
      config:
        site: "datadoghq.com"
        query: 'avg:trace.http.request.duration{service:api-gateway}.rollup(avg, 60)'
      secretRef:
        name: datadog-credentials
        namespace: aiscaler-system
        key: api-key

    - name: aws-sqs               # External: SQS queue depth
      required: false
      config:
        queueURL: "https://sqs.us-east-1.amazonaws.com/123456789/orders-queue"
        region: "us-east-1"
        targetQueueLength: "10"

    - name: keda                  # External: KEDA ScaledObject recommendation
      required: false
      config:
        scaledObjectName: "api-gateway-scaledobject"

    - name: cloudwatch            # External: AWS CloudWatch metrics
      required: false
      config:
        region: "us-east-1"
        metricName: "CPUUtilization"
        namespace: "AWS/EC2"
        statistic: "Average"
        period: "300"

    - name: annotations           # Built-in: Deployment annotations
      required: false

  alerting:
    enabled: true
    rules:
      - name: high-error-rate
        condition: "error_rate > 5"
        severity: critical
        for: "2m"
      - name: high-latency
        condition: "p95_latency > 500"
        severity: warning
        for: "5m"

  slos:
    - name: latency-slo
      metric: p95_latency
      target: 200
      priority: 1
    - name: error-rate-slo
      metric: error_rate
      target: 0.1
      priority: 2

  cost:
    monthlyBudget: 5000
    openCostURL: "http://opencost:9003"
    optimizeForCost: true

  evaluationInterval: 30s
  cooldownPeriod: 120s
  dryRun: false
```

---

## Phase 0 — Bug Fixes & Housekeeping

> **Goal**: Fix all 11 known bugs before building new features.

### Task 0.1 — Fix CurrentReplicas Source Bug (CRITICAL)

**File**: `internal/controller/aiscaler_controller.go`

**Bug**: `fetchDecision()` uses `obj.Status.CurrentReplicas` instead of `b.CurrentReplicas`. On first reconcile, Status is empty so `CurrentReplicas = 0` is sent to the LLM.

**Fix**:
```go
// In fetchDecision(), change:
scalingRequest := llm.ScalingRequest{
    PolicyName: obj.Name, CurrentReplicas: b.CurrentReplicas, // FIX: use bundle
    // ... rest unchanged
}
```

**Test**: `internal/controller/aiscaler_controller_test.go` — verify ScalingRequest uses live bundle data.

---

### Task 0.2 — Fix CPU Utilization Calculation (CRITICAL)

**File**: `internal/signals/metrics.go`

**Bug**: `container.Usage.Cpu().Value()` returns whole cores (int64, rounds down). A pod using 250m CPU returns 0. Denominator is `totalCPUReq.MilliValue()`. Result: always 0%.

**Fix**:
```go
// Change in podUtilization():
sumCPU += float64(container.Usage.Cpu().MilliValue()) // was: .Value()
```

**Test**: `internal/signals/metrics_test.go` — mock pod with 250m CPU, request 500m, assert 50%.

---

### Task 0.3 — Fix Metrics Error Handling (IMPORTANT)

**File**: `internal/signals/metrics.go`

**Bug**: `podUtilization()` error kills entire reconcile despite being marked "non-fatal".

**Fix**: Return `nil` instead of `err` when metrics-server is unavailable. Log the error.

---

### Task 0.4 — Add Missing RBAC Markers (CRITICAL)

**File**: `internal/controller/aiscaler_controller.go`

**Add**:
```go
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=metrics.k8s.io,resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
```

Run `make manifests` after.

---

### Task 0.5 — Fix operator.yaml YAML Key Mismatch (IMPORTANT)

**File**: `config/operator.yaml`

**Bug**: `apiKey` in YAML but `api_key` in Go struct tag → silently empty.

**Fix**: Change all `apiKey` to `api_key` and `baseURL` to `base_url` in YAML.

---

### Task 0.6 — Fix Dockerfile Go Version (MINOR)

**File**: `Dockerfile` — change `golang:1.24` to `golang:1.25`.

---

### Task 0.7 — Wire apiKeySecret from CRD (IMPORTANT)

**File**: `internal/llm/router.go`

Pass `client.Client` to Router. In `callProvider()`, read Secret if `apiKeySecret` is set.

---

### Task 0.8 — Fix Collector panic() (MINOR)

**File**: `internal/signals/collector.go`

Change `NewCollector()` to return `(*Collector, error)` instead of panicking.

---

### Task 0.9 — Remove Duplicate CRD File (MINOR)

Delete `config/crd/bases/aiscaler.aiscaler.io_aiscalers.yaml`.

---

### Task 0.10 — Replace fmt.Printf with Structured Logging (MINOR)

**File**: `internal/signals/collector.go`

Replace `fmt.Printf` with `logf.FromContext(ctx).Info(...)`.

---

### Task 0.11 — Fix Dockerfile Go Version in Build Stage (MINOR)

Update `FROM golang:1.24` → `FROM golang:1.25` in `Dockerfile`.

---

## Phase 1 — Plugin Architecture & Signal Source Framework

> **Goal**: Refactor the signal collection system into a plugin architecture where each signal source is independently configurable and swappable.

### Task 1.1 — Create Plugin Package

**Create directory**: `internal/plugin/`

**Create file**: `internal/plugin/signal.go`
```go
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
    CollectedAt time.Time
    SourceHealth map[string]bool // plugin-name → healthy
}

type AnnotationSignals struct {
    ExpectedTraffic     string
    ScaleConservatively bool
    FreezeUntil         *time.Time
    Note                string
    PeakHours           string
    ReactiveRules       []ReactiveRule
}

type ReactiveRule struct {
    Metric    string  `json:"metric"`
    Operator  string  `json:"operator"`
    Threshold float64 `json:"threshold"`
    Action    string  `json:"action"`
    Amount    int32   `json:"amount"`
}

// SignalPlugin is the interface every signal source must implement.
type SignalPlugin interface {
    Name() string
    Init(cfg map[string]string) error
    Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *Bundle) error
    Required() bool
    Healthy() bool
}
```

**Create file**: `internal/plugin/registry.go`
```go
package plugin

import (
    "fmt"
    "sync"
)

var (
    mu       sync.RWMutex
    registry = make(map[string]func() SignalPlugin)
)

func Register(name string, factory func() SignalPlugin) {
    mu.Lock()
    defer mu.Unlock()
    if _, exists := registry[name]; exists {
        panic(fmt.Sprintf("signal plugin %q already registered", name))
    }
    registry[name] = factory
}

func Get(name string) (SignalPlugin, error) {
    mu.RLock()
    defer mu.RUnlock()
    factory, ok := registry[name]
    if !ok {
        return nil, fmt.Errorf("unknown signal plugin %q; available: %v", name, List())
    }
    return factory(), nil
}

func List() []string {
    mu.RLock()
    defer mu.RUnlock()
    names := make([]string, 0, len(registry))
    for name := range registry {
        names = append(names, name)
    }
    return names
}
```

### Task 1.2 — Create Signal Manager

The Signal Manager replaces the old Collector. It dynamically instantiates plugins based on the CRD `spec.signals[]` list.

**Create file**: `internal/plugin/manager.go`
```go
package plugin

import (
    "context"
    "fmt"
    "time"

    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
    logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Manager orchestrates all active signal plugins.
type Manager struct {
    plugins []pluginInstance
}

type pluginInstance struct {
    plugin   SignalPlugin
    required bool
}

// NewManager creates plugins from the CRD's signal config list.
// If spec.signals is empty, it creates default plugins (metrics-server + annotations).
func NewManager(signalConfigs []aiscalerv1.SignalSourceConfig, k8sPluginDeps K8sPluginDeps) (*Manager, error) {
    m := &Manager{}

    if len(signalConfigs) == 0 {
        // Default plugins when none specified
        signalConfigs = []aiscalerv1.SignalSourceConfig{
            {Name: "metrics-server", Required: true},
            {Name: "annotations", Required: false},
        }
    }

    for _, sc := range signalConfigs {
        p, err := Get(sc.Name)
        if err != nil {
            return nil, fmt.Errorf("failed to get plugin %q: %w", sc.Name, err)
        }

        // Merge K8s deps into config for built-in plugins
        cfg := make(map[string]string)
        for k, v := range sc.Config {
            cfg[k] = v
        }

        // Pass K8s dependencies if the plugin needs them
        if kp, ok := p.(K8sAwarePlugin); ok {
            kp.SetK8sDeps(k8sPluginDeps)
        }

        if err := p.Init(cfg); err != nil {
            return nil, fmt.Errorf("failed to init plugin %q: %w", sc.Name, err)
        }

        m.plugins = append(m.plugins, pluginInstance{
            plugin:   p,
            required: sc.Required,
        })
    }

    return m, nil
}

// K8sPluginDeps holds shared Kubernetes dependencies for built-in plugins.
type K8sPluginDeps struct {
    Client        interface{} // client.Client — use interface to avoid circular imports
    MetricsClient interface{} // metricsclient.Interface
}

// K8sAwarePlugin is optionally implemented by plugins that need K8s clients.
type K8sAwarePlugin interface {
    SetK8sDeps(deps K8sPluginDeps)
}

// Collect runs all plugins and returns the aggregated bundle.
func (m *Manager) Collect(ctx context.Context, policy *aiscalerv1.AIScaler) (*Bundle, error) {
    log := logf.FromContext(ctx)
    bundle := &Bundle{
        CollectedAt:   time.Now(),
        CustomSignals: make(map[string]float64),
        SourceHealth:  make(map[string]bool),
    }

    for _, pi := range m.plugins {
        err := pi.plugin.Collect(ctx, policy, bundle)
        bundle.SourceHealth[pi.plugin.Name()] = err == nil

        if err != nil {
            if pi.required {
                return nil, fmt.Errorf("required signal plugin %q failed: %w", pi.plugin.Name(), err)
            }
            log.Info("optional signal plugin failed (non-fatal)",
                "plugin", pi.plugin.Name(), "error", err)
        }
    }

    return bundle, nil
}

// HealthStatus returns the health of each active plugin.
func (m *Manager) HealthStatus() map[string]bool {
    status := make(map[string]bool)
    for _, pi := range m.plugins {
        status[pi.plugin.Name()] = pi.plugin.Healthy()
    }
    return status
}
```

### Task 1.3 — Update CRD Types

**File**: `api/v1/aiscaler_types.go`

Add `SignalSourceConfig` struct and `Signals` field to `AIScalerSpec` (as shown in Plugin System Design section above).

Run `make manifests generate` after.

### Task 1.4 — Migrate Existing Collectors to Plugins

Convert each existing collector into a plugin:

**File**: `internal/plugin/builtin/metrics_server.go`
```go
package builtin

import (
    "context"

    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
    "github.com/sanjbh/kube-scaling-agent/internal/plugin"
    appsv1 "k8s.io/api/apps/v1"
    corev1 "k8s.io/api/core/v1"
    "k8s.io/apimachinery/pkg/labels"
    "k8s.io/apimachinery/pkg/types"
    metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
    "sigs.k8s.io/controller-runtime/pkg/client"
    v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
    plugin.Register("metrics-server", func() plugin.SignalPlugin {
        return &metricsServerPlugin{}
    })
}

type metricsServerPlugin struct {
    client        client.Client
    metricsClient metricsclient.Interface
    healthy       bool
}

func (p *metricsServerPlugin) Name() string    { return "metrics-server" }
func (p *metricsServerPlugin) Required() bool  { return true }
func (p *metricsServerPlugin) Healthy() bool   { return p.healthy }

func (p *metricsServerPlugin) SetK8sDeps(deps plugin.K8sPluginDeps) {
    p.client = deps.Client.(client.Client)
    p.metricsClient = deps.MetricsClient.(metricsclient.Interface)
}

func (p *metricsServerPlugin) Init(cfg map[string]string) error {
    p.healthy = true
    return nil
}

func (p *metricsServerPlugin) Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
    deploy := &appsv1.Deployment{}
    key := types.NamespacedName{
        Namespace: policy.Spec.TargetRef.Namespace,
        Name:      policy.Spec.TargetRef.Name,
    }
    if err := p.client.Get(ctx, key, deploy); err != nil {
        p.healthy = false
        return err
    }

    bundle.CurrentReplicas = deploy.Status.Replicas
    bundle.ReadyReplicas = deploy.Status.ReadyReplicas
    bundle.DeploymentReady = deploy.Status.ReadyReplicas == deploy.Status.Replicas && deploy.Status.Replicas > 0

    cpuPct, memPct, err := p.podUtilization(ctx, deploy)
    if err != nil {
        // Non-fatal: metrics-server might be unavailable
        bundle.CPUUtilization = 0
        bundle.MemoryUtilization = 0
        p.healthy = false
        return nil // non-fatal
    }
    bundle.CPUUtilization = cpuPct
    bundle.MemoryUtilization = memPct
    p.healthy = true
    return nil
}

func (p *metricsServerPlugin) podUtilization(ctx context.Context, deploy *appsv1.Deployment) (cpuPct, memPct float64, err error) {
    sel, err := labels.ValidatedSelectorFromSet(deploy.Spec.Selector.MatchLabels)
    if err != nil {
        return 0, 0, err
    }

    podMetricsList, err := p.metricsClient.MetricsV1beta1().PodMetricses(deploy.Namespace).
        List(ctx, v1.ListOptions{LabelSelector: sel.String()})
    if err != nil {
        return 0, 0, err
    }
    if len(podMetricsList.Items) == 0 {
        return 0, 0, nil
    }

    var totalCPUMillis, totalMemBytes int64
    for _, container := range deploy.Spec.Template.Spec.Containers {
        if req, ok := container.Resources.Requests[corev1.ResourceCPU]; ok {
            totalCPUMillis += req.MilliValue()
        }
        if req, ok := container.Resources.Requests[corev1.ResourceMemory]; ok {
            totalMemBytes += req.Value()
        }
    }

    var sumCPUMillis, sumMemBytes float64
    for _, item := range podMetricsList.Items {
        for _, container := range item.Containers {
            sumCPUMillis += float64(container.Usage.Cpu().MilliValue()) // FIX: use MilliValue
            sumMemBytes += float64(container.Usage.Memory().Value())
        }
    }

    n := float64(len(podMetricsList.Items))
    if totalCPUMillis > 0 {
        cpuPct = (sumCPUMillis / n) / float64(totalCPUMillis) * 100
    }
    if totalMemBytes > 0 {
        memPct = (sumMemBytes / n) / float64(totalMemBytes) * 100
    }
    return cpuPct, memPct, nil
}
```

Similarly create:
- `internal/plugin/builtin/prometheus.go` — wraps existing `prometheusCollector`
- `internal/plugin/builtin/annotations.go` — wraps existing `annotationCollector`
- `internal/plugin/builtin/nodes.go` — cluster node resource awareness

Each plugin registers itself via `init()` and `plugin.Register()`.

### Task 1.5 — Import Plugins in cmd/main.go

```go
// cmd/main.go — import all plugins (blank imports trigger init() registration)
import (
    _ "github.com/sanjbh/kube-scaling-agent/internal/plugin/builtin"    // metrics-server, prometheus, annotations, nodes
    _ "github.com/sanjbh/kube-scaling-agent/internal/plugin/external"   // datadog, aws-sqs, keda, cloudwatch
)
```

### Task 1.6 — Update Reconciler to Use Signal Manager

Replace `Collector *signals.Collector` in the reconciler with `SignalManager *plugin.Manager`. The `collectSignals` step now calls `r.SignalManager.Collect()`.

---

## Phase 2 — Built-in Signal Plugins

> **Goal**: Refactor existing signal sources as plugins and add node-awareness.

### Task 2.1 — Prometheus Plugin

**File**: `internal/plugin/builtin/prometheus.go`

Configurable via `spec.signals[].config`:
```yaml
config:
  baseURL: "http://prometheus:9090"
  p95LatencyQuery: "..."
  errorRateQuery: "..."
  queueDepthQuery: "..."      # NEW
  customQueries: '{"rps":"sum(rate(http_requests_total[5m]))"}'  # NEW: JSON map
```

Features:
- Supports arbitrary PromQL queries via `customQueries` (JSON map, results go into `bundle.CustomSignals`)
- Automatically injects `{{namespace}}` and `{{deployment}}` template variables
- Configurable timeout (default 10s)
- TLS support via `tlsInsecureSkipVerify` config key
- Bearer token auth via `secretRef`

### Task 2.2 — Annotations Plugin

**File**: `internal/plugin/builtin/annotations.go`

Extended annotations:
```
aiscaler.io/expected-traffic: "high"
aiscaler.io/scale-conservatively: "true"
aiscaler.io/freeze-until: "2026-04-14T00:00:00Z"
aiscaler.io/note: "Deploy in progress"
aiscaler.io/peak-hours: "09:00-17:00 EST"
aiscaler.io/reactive-rules: '[{"metric":"error_rate","operator":">","threshold":5,"action":"scale_up","amount":2}]'
```

### Task 2.3 — Node Awareness Plugin

**File**: `internal/plugin/builtin/nodes.go`

Collects:
- Total cluster nodes (schedulable)
- Available CPU/memory percentage
- Node conditions (pressure)

### Task 2.4 — Tests for Built-in Plugins

**Create files**:
- `internal/plugin/builtin/metrics_server_test.go`
- `internal/plugin/builtin/prometheus_test.go`
- `internal/plugin/builtin/annotations_test.go`
- `internal/plugin/builtin/nodes_test.go`
- `internal/plugin/registry_test.go`
- `internal/plugin/manager_test.go`

Each test file:
1. Uses standard Go `testing` package
2. Mocks K8s clients using `fake.NewClientBuilder()`
3. Tests happy path, error path, and edge cases
4. Tests plugin registration and lifecycle

---

## Phase 3 — External Signal Plugins (SQS, Datadog, KEDA, etc.)

> **Goal**: Add configurable external signal sources. Each is a separate plugin that only activates when configured.

### Task 3.1 — AWS SQS Plugin

**File**: `internal/plugin/external/aws_sqs.go`

```go
package external

import (
    "context"
    "fmt"
    "strconv"

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
    "github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

func init() {
    plugin.Register("aws-sqs", func() plugin.SignalPlugin {
        return &sqsPlugin{}
    })
}

type sqsPlugin struct {
    queueURL          string
    region            string
    targetQueueLength float64
    client            *sqs.Client
    healthy           bool
}

func (p *sqsPlugin) Name() string    { return "aws-sqs" }
func (p *sqsPlugin) Required() bool  { return false }
func (p *sqsPlugin) Healthy() bool   { return p.healthy }

func (p *sqsPlugin) Init(cfg map[string]string) error {
    p.queueURL = cfg["queueURL"]
    if p.queueURL == "" {
        return fmt.Errorf("aws-sqs plugin requires 'queueURL' config")
    }
    p.region = cfg["region"]
    if p.region == "" {
        p.region = "us-east-1"
    }
    p.targetQueueLength = 5 // default
    if v, ok := cfg["targetQueueLength"]; ok {
        if f, err := strconv.ParseFloat(v, 64); err == nil {
            p.targetQueueLength = f
        }
    }

    // Uses IRSA (EKS) or environment credentials automatically
    awsCfg, err := config.LoadDefaultConfig(context.Background(),
        config.WithRegion(p.region),
    )
    if err != nil {
        return fmt.Errorf("failed to load AWS config: %w", err)
    }
    p.client = sqs.NewFromConfig(awsCfg)
    p.healthy = true
    return nil
}

func (p *sqsPlugin) Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
    out, err := p.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
        QueueUrl: &p.queueURL,
        AttributeNames: []string{
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        },
    })
    if err != nil {
        p.healthy = false
        return fmt.Errorf("failed to get SQS attributes: %w", err)
    }

    visible, _ := strconv.ParseFloat(out.Attributes["ApproximateNumberOfMessages"], 64)
    inflight, _ := strconv.ParseFloat(out.Attributes["ApproximateNumberOfMessagesNotVisible"], 64)

    bundle.QueueDepth = visible + inflight
    if bundle.CustomSignals == nil {
        bundle.CustomSignals = make(map[string]float64)
    }
    bundle.CustomSignals["sqs_visible_messages"] = visible
    bundle.CustomSignals["sqs_inflight_messages"] = inflight
    bundle.CustomSignals["sqs_target_ratio"] = (visible + inflight) / p.targetQueueLength

    p.healthy = true
    return nil
}
```

**Dependencies to add**:
```bash
go get github.com/aws/aws-sdk-go-v2/config
go get github.com/aws/aws-sdk-go-v2/service/sqs
```

**Authentication**: Uses AWS SDK default credential chain:
- EKS: IAM Roles for Service Accounts (IRSA) — see Phase 14
- Local: `~/.aws/credentials` or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars

---

### Task 3.2 — AWS CloudWatch Plugin

**File**: `internal/plugin/external/aws_cloudwatch.go`

```go
func init() {
    plugin.Register("cloudwatch", func() plugin.SignalPlugin {
        return &cloudWatchPlugin{}
    })
}
```

Config keys:
```yaml
config:
  region: "us-east-1"
  namespace: "AWS/ECS"          # CloudWatch namespace
  metricName: "CPUUtilization"
  dimensionName: "ServiceName"
  dimensionValue: "api-gateway"
  statistic: "Average"          # Average, Sum, Maximum, Minimum, SampleCount
  period: "300"                 # seconds
```

**Dependencies**:
```bash
go get github.com/aws/aws-sdk-go-v2/service/cloudwatch
```

---

### Task 3.3 — Datadog APM Plugin

**File**: `internal/plugin/external/datadog.go`

```go
func init() {
    plugin.Register("datadog", func() plugin.SignalPlugin {
        return &datadogPlugin{}
    })
}

type datadogPlugin struct {
    apiKey  string
    appKey  string
    site    string // e.g., "datadoghq.com", "datadoghq.eu"
    query   string // Datadog metrics query
    healthy bool
}
```

Config keys:
```yaml
config:
  site: "datadoghq.com"
  query: 'avg:trace.http.request.duration{service:api-gateway}.rollup(avg, 60)'
  appKeyField: "app-key"    # key name in the secret for APP_KEY
secretRef:
  name: datadog-credentials
  namespace: aiscaler-system
  key: api-key              # DD_API_KEY
```

Uses Datadog's [Metrics Query API](https://docs.datadoghq.com/api/latest/metrics/#query-timeseries-data):
```
GET https://api.datadoghq.com/api/v1/query?from=<now-5m>&to=<now>&query=<metric_query>
```

**Authentication**: API Key + App Key from referenced Secret.

**Dependencies**: No SDK needed — uses HTTP API directly. This keeps the binary lean and avoids pulling the full Datadog Go SDK.

---

### Task 3.4 — KEDA Integration Plugin

**File**: `internal/plugin/external/keda.go`

```go
func init() {
    plugin.Register("keda", func() plugin.SignalPlugin {
        return &kedaPlugin{}
    })
}
```

Reads KEDA `ScaledObject` status using unstructured client (no KEDA dependency):
```go
func (p *kedaPlugin) Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
    so := &unstructured.Unstructured{}
    so.SetGroupVersionKind(schema.GroupVersionKind{
        Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledObject",
    })
    // Read desired replicas from KEDA's status
    desiredReplicas, found, _ := unstructured.NestedInt64(so.Object, "status", "desiredReplicas")
    if found {
        bundle.KEDADesiredReplicas = int32(desiredReplicas)
    }
    return nil
}
```

Config:
```yaml
config:
  scaledObjectName: "my-scaledobject"
  # namespace defaults to targetRef.namespace
```

**RBAC** (add to controller markers):
```go
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects,verbs=get;list;watch
```

---

### Task 3.5 — GCP Pub/Sub Plugin

**File**: `internal/plugin/external/gcp_pubsub.go`

```go
func init() {
    plugin.Register("gcp-pubsub", func() plugin.SignalPlugin {
        return &gcpPubSubPlugin{}
    })
}
```

Config:
```yaml
config:
  projectID: "my-gcp-project"
  subscriptionID: "orders-sub"
  targetUnacked: "100"  # messages per pod
```

Uses GCP client library with Workload Identity (see Phase 15).

**Dependencies**:
```bash
go get cloud.google.com/go/pubsub
```

---

### Task 3.6 — GCP Stackdriver/Cloud Monitoring Plugin

**File**: `internal/plugin/external/gcp_monitoring.go`

Config:
```yaml
config:
  projectID: "my-gcp-project"
  metricType: "custom.googleapis.com/my_metric"
  filter: 'resource.type="k8s_container"'
```

---

### Task 3.7 — Kafka Plugin

**File**: `internal/plugin/external/kafka.go`

```go
func init() {
    plugin.Register("kafka", func() plugin.SignalPlugin {
        return &kafkaPlugin{}
    })
}
```

Config:
```yaml
config:
  brokers: "kafka-broker-1:9092,kafka-broker-2:9092"
  topic: "orders"
  consumerGroup: "order-processor"
  lagThreshold: "1000"
secretRef:
  name: kafka-credentials
  namespace: aiscaler-system
  key: sasl-password
```

Reads consumer group lag using Kafka admin client.

**Dependencies**:
```bash
go get github.com/IBM/sarama
```

---

### Task 3.8 — Custom Webhook Plugin

**File**: `internal/plugin/external/webhook.go`

For any custom signal source not covered by built-in plugins. The operator calls an HTTP endpoint and reads a JSON response.

Config:
```yaml
config:
  url: "https://my-service.internal/metrics"
  responseField: "queue_depth"     # JSON path to extract
  method: "GET"
  timeoutSeconds: "5"
secretRef:
  name: webhook-auth
  namespace: aiscaler-system
  key: bearer-token
```

---

### Task 3.9 — Tests for External Plugins

**Create files**:
- `internal/plugin/external/aws_sqs_test.go`
- `internal/plugin/external/aws_cloudwatch_test.go`
- `internal/plugin/external/datadog_test.go`
- `internal/plugin/external/keda_test.go`
- `internal/plugin/external/kafka_test.go`
- `internal/plugin/external/webhook_test.go`

Each test:
1. Uses `httptest.NewServer` for HTTP-based plugins
2. Uses mock AWS clients (aws-sdk-go-v2 has mock support)
3. Tests Init/Collect/Required/Healthy lifecycle
4. Tests error handling (connection failure, auth failure, malformed response)

---

## Phase 4 — Configurable LLM Provider System

> **Goal**: Make the LLM provider system fully configurable with secret management, circuit breakers, caching, and structured output.

### Task 4.1 — Enhanced LLM Config in CRD

```go
type LLMConfig struct {
    // +kubebuilder:validation:Required
    Primary LLMProvider `json:"primary"`
    // +optional
    Fallback *LLMProvider `json:"fallback,omitempty"`
    // +optional
    Model string `json:"model,omitempty"`
    // Secret containing API keys. Keys: primary-api-key, fallback-api-key
    // +optional
    APIKeySecret *SecretRef `json:"apiKeySecret,omitempty"`
    // +kubebuilder:default="http://localhost:11434"
    OllamaBaseURL string `json:"ollamaBaseURL,omitempty"`
    // Response caching TTL. Set to 0 to disable.
    // +kubebuilder:default="30s"
    CacheTTL metav1.Duration `json:"cacheTTL,omitempty"`
    // Circuit breaker: number of consecutive failures before opening
    // +kubebuilder:default=3
    CircuitBreakerThreshold int32 `json:"circuitBreakerThreshold,omitempty"`
    // Circuit breaker reset timeout
    // +kubebuilder:default="60s"
    CircuitBreakerTimeout metav1.Duration `json:"circuitBreakerTimeout,omitempty"`
}
```

### Task 4.2 — Circuit Breaker

**Create file**: `internal/llm/circuit_breaker.go`

Standard circuit breaker pattern: closed → open (after N failures) → half-open (after timeout) → closed/open.

```go
type CircuitBreaker struct {
    mu           sync.Mutex
    failureCount int
    lastFailure  time.Time
    threshold    int
    resetTimeout time.Duration
    state        string // "closed", "open", "half-open"
}

func (cb *CircuitBreaker) Allow() bool    { ... }
func (cb *CircuitBreaker) RecordSuccess() { ... }
func (cb *CircuitBreaker) RecordFailure() { ... }
func (cb *CircuitBreaker) State() string  { ... }
```

### Task 4.3 — Response Cache

**Create file**: `internal/llm/cache.go`

SHA256 hash of request metrics as cache key. TTL-based eviction.

### Task 4.4 — Structured Output (JSON Schema)

For providers supporting it (OpenAI GPT-4o, Gemini), use `response_format: json_object`.

### Task 4.5 — Provider Accuracy Tracking

Track per-provider stats:
- Total calls, success rate
- Average latency
- Clamping rate (how often the validator overrides)

### Task 4.6 — LLM Tests

**Create files**:
- `internal/llm/router_test.go` — mock HTTP server returning JSON decisions
- `internal/llm/circuit_breaker_test.go` — test state transitions
- `internal/llm/cache_test.go` — test TTL, key generation, eviction
- `internal/llm/prompt_test.go` — test prompt generation with various inputs

---

## Phase 5 — Monitoring & Alerting (Optimized)

> **Goal**: Make the operator fully observable with Prometheus metrics, structured logging, alerting rules, and Grafana dashboards.

### Task 5.1 — Prometheus Metrics Exporter

**Create file**: `internal/metrics/exporter.go`

```go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
    // Scaling decisions
    ScalingDecisions = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "aiscaler_scaling_decisions_total",
            Help: "Total scaling decisions by direction and provider",
        },
        []string{"aiscaler", "provider", "direction"}, // up, down, none
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
        []string{"provider", "error_type"}, // timeout, auth, parse, server
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
        []string{"aiscaler", "reason"}, // max_step, min_replicas, max_replicas, confidence
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
        []string{"result"}, // hit, miss
    )

    // DryRun decisions counter
    DryRunDecisions = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "aiscaler_dryrun_decisions_total",
            Help: "Scaling decisions made in dry-run mode",
        },
        []string{"aiscaler", "direction"},
    )
)

func init() {
    metrics.Registry.MustRegister(
        ScalingDecisions, CurrentReplicas, DesiredReplicas,
        LLMLatency, LLMErrors, LLMConfidence,
        ValidationClamps, ReconcileDuration, ReconcileErrors,
        SignalPluginHealth, SignalCollectionLatency,
        CircuitBreakerState, CostPerHour, SLOBreach,
        CacheHits, DryRunDecisions,
    )
}
```

### Task 5.2 — Instrument the Reconciler

In the controller's `Reconcile()`, wrap each step with timing:
```go
start := time.Now()
// ... run step ...
metrics.ReconcileDuration.WithLabelValues(obj.Name).Observe(time.Since(start).Seconds())
```

In `fetchDecision`:
```go
start := time.Now()
decision, provider, err := r.Router.Decide(ctx, state.obj, scalingRequest)
metrics.LLMLatency.WithLabelValues(string(provider), model).Observe(time.Since(start).Seconds())
if err != nil {
    metrics.LLMErrors.WithLabelValues(string(provider), classifyError(err)).Inc()
}
metrics.LLMConfidence.WithLabelValues(string(provider)).Observe(decision.Confidence)
```

### Task 5.3 — PrometheusRule CRD for Alerting

**Create file**: `config/prometheus/alerting-rules.yaml`

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: aiscaler-alerts
  namespace: aiscaler-system
  labels:
    release: prometheus  # match your Prometheus Operator selector
spec:
  groups:
    - name: aiscaler.rules
      rules:
        # --- Operator Health ---
        - alert: AIScalerReconcileErrors
          expr: rate(aiscaler_reconcile_errors_total[5m]) > 0
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "AIScaler {{ $labels.aiscaler }} has reconcile errors"
            description: "Step {{ $labels.step }} is failing at {{ $value | printf \"%.2f\" }}/s"

        - alert: AIScalerReconcileSlow
          expr: histogram_quantile(0.95, rate(aiscaler_reconcile_duration_seconds_bucket[5m])) > 30
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "AIScaler reconcile cycles are slow (p95 > 30s)"

        # --- LLM Health ---
        - alert: AIScalerLLMHighErrorRate
          expr: rate(aiscaler_llm_errors_total[5m]) / rate(aiscaler_scaling_decisions_total[5m]) > 0.1
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "LLM provider {{ $labels.provider }} error rate > 10%"

        - alert: AIScalerLLMHighLatency
          expr: histogram_quantile(0.95, rate(aiscaler_llm_latency_seconds_bucket[5m])) > 10
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "LLM provider {{ $labels.provider }} p95 latency > 10s"

        - alert: AIScalerCircuitBreakerOpen
          expr: aiscaler_circuit_breaker_state == 2
          for: 1m
          labels:
            severity: critical
          annotations:
            summary: "Circuit breaker for {{ $labels.provider }} is OPEN"

        # --- Signal Health ---
        - alert: AIScalerSignalPluginUnhealthy
          expr: aiscaler_signal_plugin_healthy == 0
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Signal plugin {{ $labels.plugin }} for {{ $labels.aiscaler }} is unhealthy"

        # --- SLO ---
        - alert: AIScalerSLOBreach
          expr: aiscaler_slo_breach == 1
          for: 2m
          labels:
            severity: critical
          annotations:
            summary: "SLO {{ $labels.slo_name }} breached for {{ $labels.aiscaler }}"

        # --- Scaling ---
        - alert: AIScalerFrequentScaling
          expr: increase(aiscaler_scaling_decisions_total{direction!="none"}[30m]) > 10
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "{{ $labels.aiscaler }} scaled {{ $value }} times in 30min — possible flapping"

        - alert: AIScalerAtMaxReplicas
          expr: aiscaler_current_replicas >= on(aiscaler) group_left() aiscaler_desired_replicas
          for: 30m
          labels:
            severity: warning
          annotations:
            summary: "{{ $labels.aiscaler }} pegged at max replicas for 30min"

        # --- Validation ---
        - alert: AIScalerHighClampRate
          expr: rate(aiscaler_validation_clamps_total[1h]) / rate(aiscaler_scaling_decisions_total[1h]) > 0.5
          for: 1h
          labels:
            severity: warning
          annotations:
            summary: "Validator clamping >50% of decisions for {{ $labels.aiscaler }}"
```

### Task 5.4 — Grafana Dashboard JSON

**Create file**: `config/grafana/aiscaler-dashboard.json`

Panels:
1. **Overview** — Replicas (current vs desired) over time
2. **LLM Performance** — Latency histogram, error rate, confidence distribution
3. **Signal Health** — Plugin health matrix (green/red per plugin)
4. **Scaling Activity** — Decision timeline (up/down/none)
5. **SLO Status** — SLO breach timeline
6. **Cost** — Hourly cost trend
7. **Circuit Breaker** — State timeline
8. **Cache** — Hit/miss ratio

### Task 5.5 — Alerting Config in CRD

```go
type AlertingConfig struct {
    // +kubebuilder:default=true
    Enabled bool `json:"enabled"`

    // Custom alert rules evaluated by the operator (not Prometheus)
    // +optional
    Rules []AlertRule `json:"rules,omitempty"`

    // Webhook URL for alert notifications (Slack, PagerDuty, etc.)
    // +optional
    WebhookURL string `json:"webhookURL,omitempty"`

    // Secret containing webhook auth token
    // +optional
    WebhookSecret *SecretRef `json:"webhookSecret,omitempty"`
}

type AlertRule struct {
    // +kubebuilder:validation:Required
    Name string `json:"name"`

    // Condition expression: "error_rate > 5" or "p95_latency > 200"
    // +kubebuilder:validation:Required
    Condition string `json:"condition"`

    // +kubebuilder:validation:Enum=info;warning;critical
    Severity string `json:"severity"`

    // Duration the condition must hold before firing
    // +kubebuilder:default="2m"
    For metav1.Duration `json:"for,omitempty"`
}
```

### Task 5.6 — In-Operator Alert Evaluator

**Create file**: `internal/alerting/evaluator.go`

Evaluates CRD-defined alert rules against the signal bundle. Fires webhook notifications.

```go
type Evaluator struct {
    webhookURL    string
    webhookToken  string
    activeAlerts  map[string]time.Time // name → first-seen
}

func (e *Evaluator) Evaluate(bundle *plugin.Bundle, rules []aiscalerv1.AlertRule) []FiredAlert { ... }
func (e *Evaluator) Notify(ctx context.Context, alerts []FiredAlert) error { ... }
```

### Task 5.7 — Structured Logging Standards

All log lines must include:
- `aiscaler` — the AIScaler name
- `step` — the reconcile step name
- `plugin` — the signal plugin name (if applicable)
- `provider` — the LLM provider (if applicable)

Use `logf.FromContext(ctx).WithValues(...)` for consistent structured logs.

### Task 5.8 — Monitoring Tests

**Create files**:
- `internal/metrics/exporter_test.go` — verify all metrics are registered
- `internal/alerting/evaluator_test.go` — test rule evaluation, webhook firing

---

## Phase 6 — SLO-First Scaling & Reactive Rules

> **Goal**: Let users define SLOs and reactive rules as the primary scaling triggers.

### Task 6.1 — SLO Spec in CRD

```go
type SLO struct {
    Name     string  `json:"name"`
    Metric   string  `json:"metric"`   // p95_latency, p99_latency, error_rate, availability
    Target   float64 `json:"target"`
    Priority int32   `json:"priority,omitempty"` // lower = higher priority
}
```

### Task 6.2 — SLO Evaluation Engine

**Create file**: `internal/decision/slo.go`

```go
func EvaluateSLOs(slos []aiscalerv1.SLO, bundle *plugin.Bundle) []SLOStatus { ... }
```

### Task 6.3 — Reactive Rules via Annotations

Parse `aiscaler.io/reactive-rules` JSON annotation. Evaluate before LLM call. If a rule fires, skip the LLM and apply immediately.

### Task 6.4 — Add SLO Context to LLM Prompt

When SLOs are defined, inject their status into the prompt:
```
SLO Status:
  - latency-slo (p95_latency): target=200ms actual=450ms [BREACHED] margin=-125%
  - error-rate-slo (error_rate): target=0.1% actual=0.05% [OK] margin=50%

Prioritize keeping SLOs within target. If any SLO is breached, scale up.
```

### Task 6.5 — SLO Tests

- `internal/decision/slo_test.go`
- `internal/decision/reactive_test.go`

---

## Phase 7 — Vertical Scaling & In-Place Resize

> **Goal**: Add CPU/memory request/limit adjustments.

### Task 7.1 — VerticalScalingConfig in CRD

```go
type VerticalScalingConfig struct {
    Enabled        bool     `json:"enabled"`
    ContainerNames []string `json:"containerNames,omitempty"`
    MinCPU         string   `json:"minCPU,omitempty"`
    MinMemory      string   `json:"minMemory,omitempty"`
    MaxCPU         string   `json:"maxCPU,omitempty"`
    MaxMemory      string   `json:"maxMemory,omitempty"`
    // +kubebuilder:validation:Enum=InPlace;Recreate
    // +kubebuilder:default="Recreate"
    UpdateMode     string   `json:"updateMode,omitempty"`
}
```

### Task 7.2 — Extend LLM Response for Resource Recommendations

```go
type ResourceRecommendation struct {
    ContainerName string `json:"container_name"`
    CPURequest    string `json:"cpu_request"`
    MemoryRequest string `json:"memory_request"`
}
```

### Task 7.3 — Vertical Actuator

**Create file**: `internal/actuator/vertical.go`

Patches container resource requests using SSA.

### Task 7.4 — Tests

- `internal/actuator/vertical_test.go`

---

## Phase 8 — Predictive & Seasonal Scaling

> **Goal**: Use historical metrics to predict load and pre-scale.

### Task 8.1 — History Store

**Create file**: `internal/history/store.go`

In-memory ring buffer per deployment. Records metrics after each reconcile.

### Task 8.2 — Historical Context in Prompt

Pass last 10 data points + same-time-last-week data to the LLM prompt.

### Task 8.3 — Tests

- `internal/history/store_test.go`

---

## Phase 9 — Cost Optimization & FinOps

> **Goal**: Integrate cost awareness into scaling decisions.

### Task 9.1 — OpenCost Integration (as a Plugin)

**File**: `internal/plugin/external/opencost.go`

```go
func init() {
    plugin.Register("opencost", func() plugin.SignalPlugin {
        return &openCostPlugin{}
    })
}
```

Config:
```yaml
config:
  baseURL: "http://opencost:9003"
  window: "1h"
```

### Task 9.2 — CostConfig in CRD

```go
type CostConfig struct {
    MonthlyBudget   float64 `json:"monthlyBudget,omitempty"`
    OpenCostURL     string  `json:"openCostURL,omitempty"`
    OptimizeForCost bool    `json:"optimizeForCost,omitempty"`
}
```

### Task 9.3 — FinOps Right-Sizing

**Create file**: `internal/finops/rightsizer.go`

Uses P95 CPU + 20% headroom, P99 memory + 10% headroom.

### Task 9.4 — ResourceRecommendation CRD

Records right-sizing recommendations as K8s resources.

### Task 9.5 — Tests

- `internal/finops/rightsizer_test.go`
- `internal/plugin/external/opencost_test.go`

---

## Phase 10 — Safety, Guardrails & Production Hardening

> **Goal**: Add confidence gating, approval workflows, and scaling event audit trail.

### Task 10.1 — Confidence Threshold Gating

Add `minConfidence` to `ScalingConstraints`. Skip scaling if LLM confidence < threshold.

### Task 10.2 — ScalingEvent CRD (Audit Trail)

**Create file**: `api/v1/scalingevent_types.go`

Records every scaling decision with full context (metrics, decision, validation, provider).

### Task 10.3 — Human-in-the-Loop Approval

**Create file**: `api/v1/scalingapproval_types.go`

When `spec.requireApproval: true`, create a `ScalingApproval` instead of applying.

### Task 10.4 — Tests

- `internal/decision/validator_test.go`
- Approval controller test

---

## Phase 11 — Multi-Workload & Cluster-Wide Reasoning

> **Goal**: One AIScaler can manage multiple deployments as a group.

### Task 11.1 — MultiTarget Support

```go
type MultiTargetRef struct {
    Targets  []TargetRef              `json:"targets,omitempty"`
    Selector *metav1.LabelSelector    `json:"selector,omitempty"`
}
```

### Task 11.2 — Grouped Prompt

Send all deployment signals in a single LLM prompt. LLM returns per-deployment decisions.

---

## Phase 12 — Central Dashboard

> **Goal**: Web-based UI for visualizing scaling activity.

### Task 12.1 — Dashboard API Server

**File**: `internal/dashboard/server.go`

Endpoints: `/api/v1/aiscalers`, `/api/v1/events`, `/api/v1/metrics`, `/api/v1/recommendations`

### Task 12.2 — Frontend

React or static HTML+Chart.js. Pages: Overview, Detail, Metrics, Audit, FinOps, Settings.

### Task 12.3 — Deployment Manifests

`config/dashboard/deployment.yaml`, `config/dashboard/service.yaml`

---

## Phase 13 — Comprehensive Test Suite

> **Goal**: Full test coverage at every layer.

### Test Matrix

| Layer | Type | Runner | Directory |
|-------|------|--------|-----------|
| Plugin Interface | Unit | `go test` | `internal/plugin/*_test.go` |
| Signal Plugins | Unit | `go test` | `internal/plugin/builtin/*_test.go`, `internal/plugin/external/*_test.go` |
| LLM Router | Unit | `go test` | `internal/llm/*_test.go` |
| Decision Validator | Unit | `go test` | `internal/decision/*_test.go` |
| Actuator | Unit | `go test` | `internal/actuator/*_test.go` |
| History Store | Unit | `go test` | `internal/history/*_test.go` |
| FinOps | Unit | `go test` | `internal/finops/*_test.go` |
| Alerting | Unit | `go test` | `internal/alerting/*_test.go` |
| Metrics | Unit | `go test` | `internal/metrics/*_test.go` |
| Config | Unit | `go test` | `internal/config/*_test.go` |
| Controller | Integration | envtest | `internal/controller/*_test.go` |
| E2E | E2E | Ginkgo | `test/e2e/*_test.go` |

### Task 13.1 — Unit Tests: Plugin Registry & Manager

**File**: `internal/plugin/registry_test.go`
```go
package plugin_test

import (
    "testing"

    "github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

func TestRegisterAndGet(t *testing.T) {
    // Register a mock plugin
    plugin.Register("test-plugin", func() plugin.SignalPlugin {
        return &mockPlugin{name: "test-plugin"}
    })

    p, err := plugin.Get("test-plugin")
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if p.Name() != "test-plugin" {
        t.Errorf("expected name 'test-plugin', got %q", p.Name())
    }
}

func TestGetUnknown(t *testing.T) {
    _, err := plugin.Get("nonexistent")
    if err == nil {
        t.Fatal("expected error for unknown plugin")
    }
}

type mockPlugin struct {
    name string
}

func (m *mockPlugin) Name() string                   { return m.name }
func (m *mockPlugin) Init(cfg map[string]string) error { return nil }
func (m *mockPlugin) Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
    return nil
}
func (m *mockPlugin) Required() bool  { return false }
func (m *mockPlugin) Healthy() bool   { return true }
```

**File**: `internal/plugin/manager_test.go`
```go
func TestManagerCollect_AllPluginsSucceed(t *testing.T) { ... }
func TestManagerCollect_OptionalPluginFails(t *testing.T) { ... }
func TestManagerCollect_RequiredPluginFails(t *testing.T) { ... }
func TestManagerDefaultPlugins(t *testing.T) { ... }
```

### Task 13.2 — Unit Tests: Validator

**File**: `internal/decision/validator_test.go`
```go
func TestValidate_NoClamp(t *testing.T) {
    v := NewValidator()
    d := &llm.ScalingDecision{TargetReplicas: 5}
    policy := makePolicy(1, 10, 3)
    result := v.Validate(d, 4, policy)
    assertEqual(t, result.ValidatedReplicas, int32(5))
    assertEqual(t, result.Clamped, false)
}

func TestValidate_ClampMaxStep(t *testing.T) {
    v := NewValidator()
    d := &llm.ScalingDecision{TargetReplicas: 10}
    policy := makePolicy(1, 20, 3)
    result := v.Validate(d, 3, policy)
    assertEqual(t, result.ValidatedReplicas, int32(6)) // 3+3
    assertEqual(t, result.Clamped, true)
}

func TestValidate_ClampMinReplicas(t *testing.T) { ... }
func TestValidate_ClampMaxReplicas(t *testing.T) { ... }
func TestValidate_ScaleDown_ClampStep(t *testing.T) { ... }
func TestValidate_NoChange(t *testing.T) { ... }
```

### Task 13.3 — Unit Tests: LLM Router

**File**: `internal/llm/router_test.go`
```go
func TestParseDecision_ValidJSON(t *testing.T) {
    raw := `{"target_replicas": 5, "reasoning": "high CPU", "confidence": 0.85}`
    d, err := parseDecision(raw)
    if err != nil { t.Fatal(err) }
    assertEqual(t, d.TargetReplicas, int32(5))
    assertEqual(t, d.Confidence, 0.85)
}

func TestParseDecision_MarkdownWrapped(t *testing.T) {
    raw := "```json\n{\"target_replicas\": 3, \"reasoning\": \"ok\", \"confidence\": 0.7}\n```"
    d, err := parseDecision(raw)
    if err != nil { t.Fatal(err) }
    assertEqual(t, d.TargetReplicas, int32(3))
}

func TestParseDecision_Invalid(t *testing.T) {
    _, err := parseDecision("not json")
    if err == nil { t.Fatal("expected error") }
}

func TestCallProvider_MockServer(t *testing.T) {
    // Start httptest.NewServer that returns a valid completion response
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        resp := openai.ChatCompletionResponse{
            Choices: []openai.ChatCompletionChoice{
                {Message: openai.ChatCompletionMessage{
                    Content: `{"target_replicas": 5, "reasoning": "test", "confidence": 0.9}`,
                }},
            },
        }
        json.NewEncoder(w).Encode(resp)
    }))
    defer server.Close()

    cfg := &config.Config{
        LLM: config.LLMConfig{
            Providers: []config.ProviderConfig{
                {Name: "test", BaseURL: server.URL, Model: "test-model", APIKey: "test-key"},
            },
        },
    }
    router := NewRouter(cfg)
    // ... test router.Decide()
}

func TestRouter_FallbackOnPrimaryFailure(t *testing.T) { ... }
func TestRouter_BothProvidersFail(t *testing.T) { ... }
```

### Task 13.4 — Unit Tests: Actuator

**File**: `internal/actuator/actuator_test.go`
```go
func TestApply_ScaleUp(t *testing.T) {
    // Use fake client with an existing deployment
    deploy := makeDeployment("test-deploy", "default", 3)
    fakeClient := fake.NewClientBuilder().
        WithScheme(scheme).
        WithObjects(deploy).
        Build()

    actuator := NewActuator(fakeClient)
    result, err := actuator.Apply(ctx, makePolicy("test-deploy", "default", false),
        &decision.ValidationResult{ValidatedReplicas: 5})
    if err != nil { t.Fatal(err) }
    assertEqual(t, result.PreviousReplicas, int32(3))
    assertEqual(t, result.AppliedReplicas, int32(5))
    assertEqual(t, result.DryRun, false)
}

func TestApply_DryRun(t *testing.T) { ... }
func TestApply_NoChange(t *testing.T) { ... }
func TestApply_DeploymentNotFound(t *testing.T) { ... }
```

### Task 13.5 — Unit Tests: Config

**File**: `internal/config/config_test.go`
```go
func TestLoad_ValidConfig(t *testing.T) { ... }
func TestLoad_MissingProviders(t *testing.T) { ... }
func TestLoad_EnvironmentVariableExpansion(t *testing.T) { ... }
func TestLLMProvider_Found(t *testing.T) { ... }
func TestLLMProvider_NotFound(t *testing.T) { ... }
```

### Task 13.6 — Unit Tests: Signal Plugins

**File**: `internal/plugin/builtin/prometheus_test.go`
```go
func TestPrometheusPlugin_Collect(t *testing.T) {
    // Mock Prometheus API server
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        resp := `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"42.5"]}]}}`
        w.Write([]byte(resp))
    }))
    defer server.Close()

    p := &prometheusPlugin{}
    p.Init(map[string]string{
        "baseURL":         server.URL,
        "p95LatencyQuery": "histogram_quantile(0.95,...)",
    })

    bundle := &plugin.Bundle{}
    err := p.Collect(ctx, makePolicy(), bundle)
    if err != nil { t.Fatal(err) }
    assertEqual(t, bundle.P95LatencyMs, 42.5)
}
```

**File**: `internal/plugin/external/aws_sqs_test.go`
```go
func TestSQSPlugin_Collect(t *testing.T) {
    // Use mock SQS client
    // Assert bundle.QueueDepth from visible+inflight messages
}

func TestSQSPlugin_Init_MissingQueueURL(t *testing.T) {
    p := &sqsPlugin{}
    err := p.Init(map[string]string{})
    if err == nil { t.Fatal("expected error") }
}
```

### Task 13.7 — Integration Tests: Controller

**File**: `internal/controller/aiscaler_controller_test.go`

Uses envtest (real API server, no mocks):
```go
var _ = Describe("AIScaler Controller", func() {
    Context("When creating an AIScaler", func() {
        It("Should set phase to Initializing", func() {
            // Create AIScaler CR
            // Assert status.phase == "Initializing"
        })
    })

    Context("When signals are collected", func() {
        It("Should call LLM and apply decision", func() {
            // Create Deployment + AIScaler
            // Mock LLM server
            // Assert deployment scaled
        })
    })

    Context("During cooldown", func() {
        It("Should skip scaling and requeue", func() { ... })
    })

    Context("With dry-run enabled", func() {
        It("Should not change deployment replicas", func() { ... })
    })

    Context("With freeze annotation", func() {
        It("Should skip scaling until freeze expires", func() { ... })
    })
})
```

### Task 13.8 — E2E Tests

**File**: `test/e2e/e2e_test.go`

Requires a real cluster:
```go
var _ = Describe("AIScaler E2E", func() {
    It("Should scale a deployment based on CPU load", func() {
        // 1. Deploy a sample workload
        // 2. Create AIScaler CR pointing to it
        // 3. Generate CPU load
        // 4. Wait for AIScaler to scale up
        // 5. Remove load
        // 6. Wait for AIScaler to scale down
    })

    It("Should handle SQS queue depth signal", func() {
        // 1. Deploy workload + AIScaler with SQS plugin
        // 2. Push messages to SQS
        // 3. Assert scaling happens
    })

    It("Should respect cooldown period", func() { ... })
    It("Should respect dry-run mode", func() { ... })
})
```

### Task 13.9 — Makefile Test Targets

```makefile
.PHONY: test
test: ## Run unit tests
	go test ./internal/... ./api/... -coverprofile=cover.out

.PHONY: test-integration
test-integration: ## Run integration tests (requires envtest)
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use -p path)" go test ./internal/controller/... -tags=integration

.PHONY: test-e2e
test-e2e: ## Run E2E tests (requires cluster)
	go test ./test/e2e/... -v -count=1

.PHONY: test-all
test-all: test test-integration test-e2e ## Run all tests

.PHONY: coverage
coverage: test ## Generate coverage report
	go tool cover -html=cover.out -o coverage.html
```

---

## Phase 14 — Installation Guide (AWS EKS)

> **For EKS clusters with IAM Roles for Service Accounts (IRSA)**

### Prerequisites

| Component | Version | Notes |
|-----------|---------|-------|
| EKS Cluster | 1.28+ | With OIDC provider enabled |
| kubectl | 1.28+ | Configured for EKS |
| Helm | 3.12+ | For deploying dependencies |
| AWS CLI | 2.x | With admin access |
| metrics-server | 0.6+ | Required for CPU/memory signals |
| Prometheus | 2.45+ | Optional: for PromQL signals |
| KEDA | 2.12+ | Optional: for KEDA integration |

### Step 1: Enable OIDC Provider

```bash
# Check if OIDC provider exists
aws eks describe-cluster --name $CLUSTER_NAME \
  --query "cluster.identity.oidc.issuer" --output text

# If not, enable it
eksctl utils associate-iam-oidc-provider \
  --cluster $CLUSTER_NAME \
  --approve
```

### Step 2: Create IAM Policy

Create a file `aiscaler-iam-policy.json`:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SQSReadAccess",
            "Effect": "Allow",
            "Action": [
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
                "sqs:ListQueues"
            ],
            "Resource": "arn:aws:sqs:*:ACCOUNT_ID:*"
        },
        {
            "Sid": "CloudWatchReadAccess",
            "Effect": "Allow",
            "Action": [
                "cloudwatch:GetMetricData",
                "cloudwatch:GetMetricStatistics",
                "cloudwatch:ListMetrics",
                "cloudwatch:DescribeAlarms"
            ],
            "Resource": "*"
        },
        {
            "Sid": "AutoScalingReadAccess",
            "Effect": "Allow",
            "Action": [
                "autoscaling:DescribeAutoScalingGroups",
                "autoscaling:DescribeAutoScalingInstances"
            ],
            "Resource": "*"
        }
    ]
}
```

> **Least Privilege**: Scope `sqs:*` resources to only the queues your AIScaler monitors. Replace `ACCOUNT_ID` with your AWS account ID.

```bash
aws iam create-policy \
  --policy-name AIScalerPolicy \
  --policy-document file://aiscaler-iam-policy.json
```

### Step 3: Create IAM Role with IRSA

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
OIDC_PROVIDER=$(aws eks describe-cluster --name $CLUSTER_NAME \
  --query "cluster.identity.oidc.issuer" --output text | sed 's|https://||')

# Create trust policy
cat > trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::${ACCOUNT_ID}:oidc-provider/${OIDC_PROVIDER}"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "${OIDC_PROVIDER}:sub": "system:serviceaccount:aiscaler-system:aiscaler-controller-manager",
          "${OIDC_PROVIDER}:aud": "sts.amazonaws.com"
        }
      }
    }
  ]
}
EOF

aws iam create-role \
  --role-name AIScalerRole \
  --assume-role-policy-document file://trust-policy.json

aws iam attach-role-policy \
  --role-name AIScalerRole \
  --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/AIScalerPolicy
```

### Step 4: Create Namespace and Secrets

```bash
kubectl create namespace aiscaler-system

# LLM API keys
kubectl create secret generic aiscaler-api-keys \
  -n aiscaler-system \
  --from-literal=ANTHROPIC_API_KEY='your-anthropic-key' \
  --from-literal=GEMINI_API_KEY='your-gemini-key' \
  --from-literal=DEEPSEEK_API_KEY='your-deepseek-key'

# (Optional) Datadog credentials
kubectl create secret generic datadog-credentials \
  -n aiscaler-system \
  --from-literal=api-key='your-dd-api-key' \
  --from-literal=app-key='your-dd-app-key'
```

### Step 5: Annotate Service Account for IRSA

```bash
kubectl annotate serviceaccount aiscaler-controller-manager \
  -n aiscaler-system \
  eks.amazonaws.com/role-arn=arn:aws:iam::${ACCOUNT_ID}:role/AIScalerRole
```

### Step 6: Install the Operator

```bash
# Install CRDs
make install

# Deploy operator
make deploy IMG=your-registry/aiscaler:latest
```

Or using kustomize directly:
```bash
cd config/default
kustomize build . | kubectl apply -f -
```

### Step 7: Install Dependencies (Optional)

```bash
# metrics-server (if not already installed)
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

# Prometheus (via Helm)
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace

# KEDA (if using KEDA plugin)
helm repo add kedacore https://kedacore.github.io/charts
helm install keda kedacore/keda -n keda --create-namespace
```

### Step 8: Create Your AIScaler Resource

```yaml
# aiscaler-sample.yaml
apiVersion: aiscaler.io/v1
kind: AIScaler
metadata:
  name: my-api
spec:
  targetRef:
    name: my-api-deployment
    namespace: production
  constraints:
    minReplicas: 2
    maxReplicas: 20
    maxScaleStep: 3
    minConfidence: 0.6
  llm:
    primary: anthropic
    model: claude-sonnet-4-5-20250514
    apiKeySecret:
      name: aiscaler-api-keys
      namespace: aiscaler-system
      key: ANTHROPIC_API_KEY
  signals:
    - name: metrics-server
      required: true
    - name: prometheus
      config:
        baseURL: "http://prometheus-kube-prometheus-prometheus.monitoring:9090"
        p95LatencyQuery: 'histogram_quantile(0.95, rate(http_request_duration_seconds_bucket{namespace="production",service="my-api"}[5m]))'
    - name: aws-sqs
      config:
        queueURL: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue"
        region: "us-east-1"
  evaluationInterval: 30s
  cooldownPeriod: 120s
```

```bash
kubectl apply -f aiscaler-sample.yaml
```

### Step 9: Verify

```bash
# Check operator logs
kubectl logs -n aiscaler-system -l control-plane=controller-manager -f

# Check AIScaler status
kubectl get aiscalers
kubectl describe aiscaler my-api

# Check metrics
kubectl port-forward -n aiscaler-system svc/aiscaler-controller-manager-metrics-service 8443:8443
curl -k https://localhost:8443/metrics | grep aiscaler_
```

### AWS RBAC Summary

| K8s RBAC | Purpose |
|----------|---------|
| `aiscaler.io/*` | Full control over AIScaler CRDs |
| `apps/deployments` | Get, List, Watch, Patch (for scaling) |
| `metrics.k8s.io/pods` | Get, List (CPU/memory metrics) |
| `""/secrets` | Get, List, Watch (API key Secrets) |
| `""/events` | Create, Patch (K8s events) |
| `""/nodes` | Get, List, Watch (cluster capacity) |
| `keda.sh/scaledobjects` | Get, List, Watch (if KEDA plugin used) |

| AWS IAM | Purpose |
|---------|---------|
| `sqs:GetQueueAttributes` | Read SQS queue depth |
| `sqs:GetQueueUrl` | Resolve queue names |
| `sqs:ListQueues` | Discover queues |
| `cloudwatch:GetMetricData` | Read CloudWatch metrics |
| `cloudwatch:GetMetricStatistics` | Read CloudWatch statistics |
| `cloudwatch:ListMetrics` | Discover metrics |

### Troubleshooting (AWS)

| Problem | Cause | Fix |
|---------|-------|-----|
| `AccessDenied` on SQS | IRSA not configured | Check SA annotation `eks.amazonaws.com/role-arn` |
| Empty LLM response | API key not set | Verify Secret exists and key name matches |
| `forbidden: User "system:serviceaccount:..."` | Missing RBAC | Run `make manifests && make install` |
| CPU always 0% | metrics-server missing | Install metrics-server |
| Prometheus queries fail | Wrong baseURL | Verify Prometheus service URL |

---

## Phase 15 — Installation Guide (GCP GKE)

> **For GKE clusters with Workload Identity Federation**

### Prerequisites

| Component | Version | Notes |
|-----------|---------|-------|
| GKE Cluster | 1.28+ | Autopilot or Standard with Workload Identity enabled |
| kubectl | 1.28+ | Configured for GKE |
| Helm | 3.12+ | For deploying dependencies |
| gcloud CLI | Latest | With project admin access |
| metrics-server | Built-in | GKE includes metrics-server |
| Prometheus | 2.45+ | Optional: GMP (Google Managed Prometheus) or self-managed |

### Step 1: Enable Required APIs

```bash
gcloud services enable \
  container.googleapis.com \
  iam.googleapis.com \
  iamcredentials.googleapis.com \
  monitoring.googleapis.com \
  pubsub.googleapis.com  # if using Pub/Sub plugin
```

### Step 2: Enable Workload Identity Federation

```bash
# For existing clusters
gcloud container clusters update $CLUSTER_NAME \
  --location=$LOCATION \
  --workload-pool=$PROJECT_ID.svc.id.goog

# For new clusters, it's enabled by default in Autopilot
```

### Step 3: Create GCP IAM Service Account

```bash
gcloud iam service-accounts create aiscaler-sa \
  --description="AI Scaler operator service account" \
  --display-name="AI Scaler"
```

### Step 4: Grant IAM Roles

```bash
PROJECT_ID=$(gcloud config get-value project)

# Monitoring read access (for GCP Monitoring/Stackdriver plugin)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:aiscaler-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/monitoring.viewer"

# Pub/Sub read access (if using Pub/Sub plugin)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:aiscaler-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/pubsub.viewer"

# Cloud Storage read (if using GCS-based config)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:aiscaler-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"
```

### Step 5: Bind K8s SA to GCP SA (Workload Identity)

```bash
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")

# Method 1: IAM principal identifier (recommended for GKE 1.28+)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --role="roles/iam.workloadIdentityUser" \
  --member="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/aiscaler-system/sa/aiscaler-controller-manager" \
  --condition=None

# Method 2: Legacy annotation-based (for compatibility)
gcloud iam service-accounts add-iam-policy-binding \
  aiscaler-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:${PROJECT_ID}.svc.id.goog[aiscaler-system/aiscaler-controller-manager]"
```

### Step 6: Create Namespace and Secrets

```bash
kubectl create namespace aiscaler-system

# LLM API keys
kubectl create secret generic aiscaler-api-keys \
  -n aiscaler-system \
  --from-literal=ANTHROPIC_API_KEY='your-anthropic-key' \
  --from-literal=GEMINI_API_KEY='your-gemini-key'
```

### Step 7: Annotate K8s Service Account

```bash
# If using legacy method (Method 2 above)
kubectl annotate serviceaccount aiscaler-controller-manager \
  -n aiscaler-system \
  iam.gke.io/gcp-service-account=aiscaler-sa@${PROJECT_ID}.iam.gserviceaccount.com
```

### Step 8: Install the Operator

```bash
make install
make deploy IMG=your-registry/aiscaler:latest
```

### Step 9: Configure with GCP-Specific Signals

```yaml
apiVersion: aiscaler.io/v1
kind: AIScaler
metadata:
  name: my-api
spec:
  targetRef:
    name: my-api-deployment
    namespace: production
  constraints:
    minReplicas: 2
    maxReplicas: 20
    maxScaleStep: 3
  llm:
    primary: gemini
    model: gemini-2.0-flash
    apiKeySecret:
      name: aiscaler-api-keys
      namespace: aiscaler-system
      key: GEMINI_API_KEY
  signals:
    - name: metrics-server
      required: true
    - name: prometheus
      config:
        # Google Managed Prometheus (GMP) frontend
        baseURL: "http://frontend.default.svc:9090"
        p95LatencyQuery: 'histogram_quantile(0.95, rate(http_request_duration_seconds_bucket{namespace="production"}[5m]))'
    - name: gcp-pubsub
      config:
        projectID: "my-project"
        subscriptionID: "orders-sub"
        targetUnacked: "100"
    - name: gcp-monitoring
      config:
        projectID: "my-project"
        metricType: "custom.googleapis.com/http/request_count"
  evaluationInterval: 30s
```

### Step 10: Verify

```bash
kubectl get aiscalers
kubectl describe aiscaler my-api
kubectl logs -n aiscaler-system -l control-plane=controller-manager -f
```

### GCP IAM Summary

| GCP IAM Role | Purpose |
|-------------|---------|
| `roles/monitoring.viewer` | Read GCP Cloud Monitoring metrics |
| `roles/pubsub.viewer` | Read Pub/Sub subscription metrics |
| `roles/storage.objectViewer` | Read GCS objects (if needed) |
| `roles/iam.workloadIdentityUser` | Allow K8s SA to impersonate GCP SA |

| K8s RBAC | Purpose |
|----------|---------|
| Same as AWS table above | Same permissions needed in-cluster |

### Troubleshooting (GCP)

| Problem | Cause | Fix |
|---------|-------|-----|
| `Permission denied` on GCP APIs | Workload Identity not configured | Check SA annotation `iam.gke.io/gcp-service-account` |
| `Could not detect credentials` | OIDC token not refreshed | Ensure node pool has `--workload-metadata=GKE_METADATA` |
| GMP queries return empty | Wrong frontend URL | Use `http://frontend.default.svc:9090` for GMP |
| Pub/Sub: `subscription not found` | Wrong project or sub ID | Verify project ID and subscription exist |

---

## Appendix A — File Index (Revised)

| File | Purpose |
|------|---------|
| **CRD Types** | |
| `api/v1/aiscaler_types.go` | AIScaler CRD with plugin-based signals |
| `api/v1/scalingevent_types.go` | ScalingEvent audit trail CRD |
| `api/v1/scalingapproval_types.go` | Human approval CRD |
| `api/v1/resourcerecommendation_types.go` | FinOps recommendation CRD |
| **Plugin Framework** | |
| `internal/plugin/signal.go` | SignalPlugin interface + Bundle |
| `internal/plugin/registry.go` | Plugin registration |
| `internal/plugin/manager.go` | Signal Manager (orchestrator) |
| **Built-in Plugins** | |
| `internal/plugin/builtin/metrics_server.go` | K8s metrics-server |
| `internal/plugin/builtin/prometheus.go` | Prometheus PromQL |
| `internal/plugin/builtin/annotations.go` | Deployment annotations |
| `internal/plugin/builtin/nodes.go` | Cluster node awareness |
| **External Plugins** | |
| `internal/plugin/external/aws_sqs.go` | AWS SQS queue depth |
| `internal/plugin/external/aws_cloudwatch.go` | AWS CloudWatch metrics |
| `internal/plugin/external/datadog.go` | Datadog APM metrics |
| `internal/plugin/external/keda.go` | KEDA ScaledObject integration |
| `internal/plugin/external/gcp_pubsub.go` | GCP Pub/Sub |
| `internal/plugin/external/gcp_monitoring.go` | GCP Cloud Monitoring |
| `internal/plugin/external/kafka.go` | Kafka consumer lag |
| `internal/plugin/external/webhook.go` | Custom HTTP webhook |
| `internal/plugin/external/opencost.go` | OpenCost integration |
| **Core** | |
| `internal/controller/aiscaler_controller.go` | Main reconciler |
| `internal/llm/router.go` | LLM provider routing |
| `internal/llm/prompt.go` | Prompt construction |
| `internal/llm/circuit_breaker.go` | Circuit breaker |
| `internal/llm/cache.go` | Response cache |
| `internal/decision/validator.go` | Hard guardrails |
| `internal/decision/slo.go` | SLO evaluation |
| `internal/decision/reactive.go` | Reactive rule evaluation |
| `internal/actuator/actuator.go` | Horizontal SSA actuator |
| `internal/actuator/vertical.go` | Vertical resource actuator |
| `internal/config/config.go` | YAML config loader |
| **Monitoring** | |
| `internal/metrics/exporter.go` | Prometheus metrics |
| `internal/alerting/evaluator.go` | Alert rule evaluator |
| `config/prometheus/alerting-rules.yaml` | PrometheusRule alerts |
| `config/grafana/aiscaler-dashboard.json` | Grafana dashboard |
| **Supporting** | |
| `internal/history/store.go` | Historical metrics store |
| `internal/finops/rightsizer.go` | Resource right-sizing |
| `internal/finops/savings.go` | Savings calculator |
| `internal/dashboard/server.go` | Dashboard API |
| `cmd/main.go` | Entrypoint |

---

## Appendix B — CRD Reference

### AIScaler (primary CRD)

```yaml
apiVersion: aiscaler.io/v1
kind: AIScaler                    # cluster-scoped
spec:
  targetRef:                      # deployment to manage
    name: string
    namespace: string
  constraints:
    minReplicas: int32            # default: 1
    maxReplicas: int32            # default: 10
    maxScaleStep: int32           # default: 3
    minConfidence: float64        # default: 0.5 (0-1)
  llm:
    primary: enum                 # anthropic|gemini|ollama|deepseek
    fallback: enum                # optional
    model: string                 # optional override
    apiKeySecret: SecretRef       # optional per-CRD secret
    cacheTTL: duration            # default: 30s
    circuitBreakerThreshold: int  # default: 3
    circuitBreakerTimeout: dur    # default: 60s
  signals:                        # plugin list
    - name: string                # plugin name from registry
      required: bool              # abort if fails?
      config: map[string]string   # plugin config
      secretRef: SecretRef        # sensitive config
  alerting:
    enabled: bool
    rules: []AlertRule
    webhookURL: string
    webhookSecret: SecretRef
  slos: []SLO
  cost: CostConfig
  verticalScaling: VerticalConfig
  evaluationInterval: duration    # default: 60s
  cooldownPeriod: duration        # default: 300s
  dryRun: bool                    # default: false
  requireApproval: bool           # default: false
```

### ScalingEvent (audit)

```yaml
apiVersion: aiscaler.io/v1
kind: ScalingEvent
spec:
  aiScalerName: string
  timestamp: Time
  provider: string
  replicas: {before: int, after: int}
  metrics: {cpu, mem, p95, errorRate}
  decision: {target, reasoning, confidence}
  validation: {original, validated, clamped, reason}
  dryRun: bool
```

---

## Appendix C — Configuration Reference

### Operator Config (`config/operator.yaml`)

```yaml
llm:
  providers:
    - name: anthropic
      base_url: "https://api.anthropic.com/v1"
      api_key: "${ANTHROPIC_API_KEY}"         # env var expanded
      model: "claude-sonnet-4-5-20250514"
    - name: gemini
      base_url: "https://generativelanguage.googleapis.com/v1beta/openai"
      api_key: "${GEMINI_API_KEY}"
      model: "gemini-2.0-flash"
    - name: ollama
      base_url: "http://localhost:11434/v1"
      model: "qwen3:8b-q4_K_M"
    - name: deepseek
      base_url: "https://api.deepseek.com/v1"
      api_key: "${DEEPSEEK_API_KEY}"
      model: "deepseek-chat"

prometheus:
  baseURL: "http://prometheus:9090"

operator:
  leaderElection: true
  metricsBindAddress: ":8443"
  healthProbeBindAddress: ":8081"
```

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `ANTHROPIC_API_KEY` | Anthropic Claude API key | If using Anthropic |
| `GEMINI_API_KEY` | Google Gemini API key | If using Gemini |
| `DEEPSEEK_API_KEY` | DeepSeek API key | If using DeepSeek |
| `CONFIG_PATH` | Path to operator.yaml | Default: `config/operator.yaml` |

### Signal Plugin Configuration Reference

| Plugin | Config Keys | Required Keys | Optional Keys |
|--------|------------|---------------|---------------|
| `metrics-server` | — | — | — (auto-configured) |
| `prometheus` | `baseURL`, `p95LatencyQuery`, `errorRateQuery`, `queueDepthQuery`, `customQueries` (JSON), `timeoutSeconds`, `tlsInsecureSkipVerify` | `baseURL` | all others |
| `annotations` | — | — | — (reads deployment annotations) |
| `nodes` | — | — | — (reads cluster node list) |
| `aws-sqs` | `queueURL`, `region`, `targetQueueLength`, `scaleOnInFlight` | `queueURL` | `region` (default: us-east-1), `targetQueueLength` (default: 5) |
| `cloudwatch` | `region`, `namespace`, `metricName`, `dimensionName`, `dimensionValue`, `statistic`, `period` | `region`, `namespace`, `metricName` | `statistic` (default: Average), `period` (default: 300) |
| `datadog` | `site`, `query`, `appKeyField` | `query` | `site` (default: datadoghq.com) |
| `keda` | `scaledObjectName` | `scaledObjectName` | — |
| `gcp-pubsub` | `projectID`, `subscriptionID`, `targetUnacked` | `projectID`, `subscriptionID` | `targetUnacked` (default: 100) |
| `gcp-monitoring` | `projectID`, `metricType`, `filter` | `projectID`, `metricType` | `filter` |
| `kafka` | `brokers`, `topic`, `consumerGroup`, `lagThreshold` | `brokers`, `topic`, `consumerGroup` | `lagThreshold` (default: 1000) |
| `webhook` | `url`, `responseField`, `method`, `timeoutSeconds` | `url`, `responseField` | `method` (default: GET), `timeoutSeconds` (default: 5) |
| `opencost` | `baseURL`, `window` | `baseURL` | `window` (default: 1h) |

---

## Implementation Priority & Order

| Priority | Phase | Complexity | Dependencies |
|----------|-------|------------|--------------|
| **P0** | Phase 0 (Bug Fixes) | Low | None |
| **P0** | Phase 1 (Plugin Architecture) | High | Phase 0 |
| **P1** | Phase 2 (Built-in Plugins) | Medium | Phase 1 |
| **P1** | Phase 5 (Monitoring & Alerting) | Medium | Phase 1 |
| **P1** | Phase 13 (Tests — unit) | Medium | Phase 1-2 |
| **P2** | Phase 3 (External Plugins) | Medium | Phase 1 |
| **P2** | Phase 4 (LLM Enhancements) | Medium | Phase 0 |
| **P2** | Phase 6 (SLO & Reactive Rules) | Medium | Phase 2 |
| **P2** | Phase 10 (Safety, Guardrails) | Medium | Phase 4 |
| **P3** | Phase 7 (Vertical Scaling) | High | Phase 2 |
| **P3** | Phase 8 (Predictive Scaling) | High | Phase 2, 5 |
| **P3** | Phase 9 (Cost/FinOps) | Medium | Phase 3 |
| **P4** | Phase 11 (Multi-Workload) | High | Phase 2, 6 |
| **P4** | Phase 12 (Dashboard) | High | Phase 5, 10 |
| **P4** | Phase 14-15 (Install Guides) | Low | Phase 3 |
| **P4** | Phase 13 (Tests — e2e) | Medium | Phase 3 |

---

## Development Workflow

### For Each Task

1. **Branch**: `git checkout -b phase-X/task-Y.Z-description`
2. **Test first** (TDD): write failing tests
3. **Implement**: minimum code to pass
4. **Run all tests**: `make test`
5. **Regenerate manifests**: `make manifests` (if CRD/RBAC changed)
6. **Build**: `make docker-build`
7. **PR**: open PR with clear description

### Useful Commands

```bash
make test              # Unit tests
make test-integration  # Integration tests (envtest)
make test-e2e          # E2E tests (needs cluster)
make test-all          # Everything
make coverage          # HTML coverage report
make manifests         # Regenerate CRD/RBAC YAML
make generate          # Regenerate deepcopy
make docker-build      # Build container
make run               # Run locally
make dev               # Install CRDs + run locally
```
