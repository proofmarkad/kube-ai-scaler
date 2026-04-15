# AI Scaler — Detailed Technical Implementation Plan

> This document breaks every phase into step-by-step implementation tasks so that
> **any engineer** — including a junior one — can pick up a task and ship it.
> Each task lists the **exact files to touch**, the **function signatures** to add
> or change, the **code snippets** to write, and the **tests** to prove it works.

---

## Table of Contents

- [Phase 0 — Bug Fixes & Housekeeping](#phase-0--bug-fixes--housekeeping)
- [Phase 1 — Extended Signal Sources](#phase-1--extended-signal-sources)
- [Phase 2 — Natural-Language Scheduling & Reactive Rules](#phase-2--natural-language-scheduling--reactive-rules)
- [Phase 3 — Vertical Scaling & In-Place Resize](#phase-3--vertical-scaling--in-place-resize)
- [Phase 4 — Cost Optimization Engine](#phase-4--cost-optimization-engine)
- [Phase 5 — SLO-First Multi-Signal Scaling](#phase-5--slo-first-multi-signal-scaling)
- [Phase 6 — Predictive & Seasonal Scaling](#phase-6--predictive--seasonal-scaling)
- [Phase 7 — Node-Aware Cluster-Level Intelligence](#phase-7--node-aware-cluster-level-intelligence)
- [Phase 8 — KEDA Integration & Composability](#phase-8--keda-integration--composability)
- [Phase 9 — Safety, Guardrails & Production Hardening](#phase-9--safety-guardrails--production-hardening)
- [Phase 10 — Observability, Audit & Replay](#phase-10--observability-audit--replay)
- [Phase 11 — Multi-Workload & Cluster-Wide Reasoning](#phase-11--multi-workload--cluster-wide-reasoning)
- [Phase 12 — LLM Reliability & Provider Routing](#phase-12--llm-reliability--provider-routing)
- [Phase 13 — FinOps Resource Analysis & Right-Sizing](#phase-13--finops-resource-analysis--right-sizing)
- [Phase 14 — Central Dashboard](#phase-14--central-dashboard)

---

## Tech Stack Reference

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Go | 1.25 |
| Framework | Kubebuilder / controller-runtime | v0.22.4 |
| K8s client libs | k8s.io/api, k8s.io/apimachinery, k8s.io/client-go | v0.35.2 |
| Metrics server | k8s.io/metrics | v0.35.2 |
| LLM SDK | github.com/sashabaranov/go-openai | v1.41.2 |
| Testing | Ginkgo v2 / Gomega | latest |
| Integration Test | controller-runtime/pkg/envtest | v0.22.4 |
| Container | gcr.io/distroless/static:nonroot | — |
| Module path | github.com/sanjbh/kube-scaling-agent | — |

---

## Phase 0 — Bug Fixes & Housekeeping

> **Goal**: Fix all 11 known bugs documented in plan.md Section 2. These must be
> fixed before building new features because they affect correctness and
> deployability.

### Task 0.1 — Fix CurrentReplicas Source Bug (CRITICAL)

**Problem**: In `fetchDecision()`, the `ScalingRequest.CurrentReplicas` is set
from `obj.Status.CurrentReplicas` instead of `state.bundle.CurrentReplicas`.
On the very first reconcile, Status is empty so `CurrentReplicas = 0` is sent
to the LLM, which may cause it to scale from "0 replicas" when the deployment
actually has pods running.

**File**: `internal/controller/aiscaler_controller.go`

**Current code** (~line 178 in `fetchDecision`):
```go
scalingRequest := llm.ScalingRequest{
    PolicyName:          obj.Name,
    CurrentReplicas:     obj.Status.CurrentReplicas, // BUG: uses status
    // ...
}
```

**Fix**: Change to use the freshly-collected bundle:
```go
scalingRequest := llm.ScalingRequest{
    PolicyName:          obj.Name,
    CurrentReplicas:     b.CurrentReplicas, // FIX: use live data from bundle
    // ...
}
```

**Test**: In `internal/controller/aiscaler_controller_test.go`, write a test
that creates an AIScaler with no prior Status and a Deployment with 3 replicas.
Assert that the ScalingRequest passed to the Router contains
`CurrentReplicas == 3`.

---

### Task 0.2 — Fix CPU Utilization Calculation Bug (CRITICAL)

**Problem**: In `podUtilization()`, CPU usage is fetched via
`container.Usage.Cpu().Value()` which returns **whole cores** (int64, rounds
down). A pod using 250m CPU shows `Value() == 0`. The denominator is
`totalCPUReq.MilliValue()` (milliCPU). Dividing 0 by 500 yields 0%.

**File**: `internal/signals/metrics.go`

**Current code** (~line 110):
```go
for _, container := range item.Containers {
    sumCPU += float64(container.Usage.Cpu().Value())    // BUG: whole cores
    sumMem += float64(container.Usage.Memory().Value())
}
```

**Fix**: Use `MilliValue()` for CPU, keep `Value()` for memory (bytes are fine):
```go
for _, container := range item.Containers {
    sumCPU += float64(container.Usage.Cpu().MilliValue()) // FIX: milliCPU
    sumMem += float64(container.Usage.Memory().Value())
}
```

**Test**: Create a unit test in `internal/signals/metrics_test.go`:
- Mock a PodMetrics object with `Cpu: resource.MustParse("250m")`
- Mock a Deployment with container request `cpu: 500m`
- Assert `cpuPct == 50.0`

---

### Task 0.3 — Fix Metrics Error Handling (IMPORTANT)

**Problem**: `metricsCollector.collect()` returns an error when
`podUtilization()` fails, even though the comment says "Non-fatal".  
`Collector.Collect()` propagates this as a fatal error, killing the entire
reconcile cycle.

**File**: `internal/signals/metrics.go`

**Current code** (~line 58):
```go
cpuUtil, memUtil, err := m.podUtilization(ctx, deploy)
if err != nil {
    bundle.CPUUtilization = 0
    bundle.MemoryUtilization = 0
    return err // BUG: makes metrics-server failure fatal
}
```

**Fix**: Log the error and return nil:
```go
cpuUtil, memUtil, err := m.podUtilization(ctx, deploy)
if err != nil {
    // metrics-server unavailable — non-fatal, LLM operates without CPU/mem
    bundle.CPUUtilization = 0
    bundle.MemoryUtilization = 0
    return nil // FIX: non-fatal
}
```

Also replace `fmt.Printf` calls in `collector.go` with structured logging:

**File**: `internal/signals/collector.go`

Replace:
```go
fmt.Printf("failed to collect prometheus signals: %v\n", err)
```
With:
```go
log := logf.FromContext(ctx)
log.Info("prometheus signals unavailable (non-fatal)", "error", err)
```

Add the import:
```go
logf "sigs.k8s.io/controller-runtime/pkg/log"
```

Repeat for the annotations `fmt.Printf` on the next line.

---

### Task 0.4 — Add Missing RBAC Markers (CRITICAL)

**Problem**: The controller reads Deployments and PodMetrics, but only has RBAC
markers for `aiscaler.io` resources. The generated `role.yaml` won't grant
access to Deployments or PodMetrics, causing runtime permission errors.

**File**: `internal/controller/aiscaler_controller.go`

**Add these markers** right below the existing ones (before `Reconcile`):
```go
// +kubebuilder:rbac:groups=aiscaler.io,resources=aiscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=aiscaler.io,resources=aiscalers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=aiscaler.io,resources=aiscalers/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=metrics.k8s.io,resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
```

**Then regenerate RBAC**:
```bash
make manifests
```

Verify that `config/rbac/role.yaml` now contains rules for `apps/deployments`,
`metrics.k8s.io/pods`, and `secrets`.

---

### Task 0.5 — Fix operator.yaml YAML Key Mismatch (IMPORTANT)

**Problem**: `config/operator.yaml` uses `apiKey` and `baseURL`, but the Go
struct in `config.go` uses YAML tags `api_key` and `base_url`. Unmarshaling
silently produces empty strings.

**File**: `config/operator.yaml`

**Current**:
```yaml
providers:
  anthropic:
    baseURL: "https://api.anthropic.com/v1"
    apiKey: "${ANTHROPIC_API_KEY}"
    model: "claude-sonnet-4-5-20250514"
```

**Fix** — change all providers to use snake_case keys matching struct tags:
```yaml
providers:
  anthropic:
    base_url: "https://api.anthropic.com/v1"
    api_key: "${ANTHROPIC_API_KEY}"
    model: "claude-sonnet-4-5-20250514"
```

Do the same for `gemini`, `ollama`, and `deepseek` entries.

**Also fix**: `config/manager/operator-config.yaml` (the in-cluster ConfigMap
version) — same key rename.

**Test**: After the fix, run the operator locally and check logs confirm the
API key is non-empty (you'll see successful LLM calls instead of auth errors).

---

### Task 0.6 — Fix Dockerfile Go Version (MINOR)

**Problem**: `Dockerfile` uses `golang:1.24` but `go.mod` requires `go 1.25`.

**File**: `Dockerfile`

**Fix**: Change the build stage:
```dockerfile
FROM golang:1.25 AS builder
```

---

### Task 0.7 — Wire apiKeySecret from CRD (IMPORTANT)

**Problem**: The `LLMConfig` struct has an `APIKeySecret` field (`SecretRef`)
that is never read. The router always uses keys from the central config.

**File**: `internal/llm/router.go`

**Change `callProvider` signature** to accept a `client.Client` and an optional `SecretRef`:
```go
func (r *Router) callProvider(
    ctx context.Context,
    k8sClient client.Client,
    provider aiscalerv1.LLMProvider,
    modelOverride string,
    secretRef *aiscalerv1.SecretRef,
    req *ScalingRequest,
) (*ScalingDecision, error) {
```

Inside `callProvider`, after `buildClient`:
```go
if secretRef != nil {
    // Read API key from the referenced Secret
    secret := &corev1.Secret{}
    key := types.NamespacedName{
        Namespace: secretRef.Namespace,
        Name:      secretRef.Name,
    }
    if err := k8sClient.Get(ctx, key, secret); err != nil {
        return nil, fmt.Errorf("failed to read apiKeySecret %s/%s: %w",
            secretRef.Namespace, secretRef.Name, err)
    }
    apiKeyBytes, ok := secret.Data[secretRef.Key]
    if !ok {
        return nil, fmt.Errorf("key %q not found in secret %s/%s",
            secretRef.Key, secretRef.Namespace, secretRef.Name)
    }
    // Override the API key from central config
    apiKey = string(apiKeyBytes)
}
```

**Update Router struct** to hold a `client.Client`:
```go
type Router struct {
    cfg    *config.Config
    client client.Client
}

func NewRouter(cfg *config.Config, client client.Client) *Router {
    return &Router{cfg: cfg, client: client}
}
```

**Update `cmd/main.go`** to pass the client:
```go
Router: llm.NewRouter(cfg, k8sClient),
```

**Update `Decide`** to pass `r.client` and `policy.Spec.LLM.APIKeySecret` to
`callProvider`.

---

### Task 0.8 — Fix Collector panic() on Startup (MINOR)

**Problem**: `NewCollector()` calls `panic()` if it can't get the K8s config.
This crashes the operator on startup without a useful error message.

**File**: `internal/signals/collector.go`

**Fix**: Change the constructor to return `(*Collector, error)`:
```go
func NewCollector(client client.Client) (*Collector, error) {
    cfg, err := config.GetConfig()
    if err != nil {
        return nil, fmt.Errorf("failed to get k8s config: %w", err)
    }
    metricsClient, err := metricsclient.NewForConfig(cfg)
    if err != nil {
        return nil, fmt.Errorf("failed to create metrics client: %w", err)
    }
    return &Collector{
        metrics:     &metricsCollector{client: client, metricsClient: metricsClient},
        prometheus:  &prometheusCollector{},
        annotations: &annotationCollector{client: client},
    }, nil
}
```

**Update `cmd/main.go`**:
```go
collector, err := signals.NewCollector(k8sClient)
if err != nil {
    setupLog.Error(err, "unable to create signal collector")
    os.Exit(1)
}
// ... use collector in reconciler
```

---

### Task 0.9 — Remove Duplicate CRD File (MINOR)

**Problem**: There are two CRD files in `config/crd/bases/`:
- `aiscaler.aiscaler.io_aiscalers.yaml`  
- `aiscaler.io_aiscalers.yaml`

Only one should exist.

**Fix**: Delete the one that doesn't match the current `groupversion_info.go`
GroupVersion. The group is `aiscaler.io`, so keep `aiscaler.io_aiscalers.yaml`
and delete `aiscaler.aiscaler.io_aiscalers.yaml`.

```bash
rm config/crd/bases/aiscaler.aiscaler.io_aiscalers.yaml
```

Update `config/crd/kustomization.yaml` if it references the deleted file.

---

### Task 0.10 — Fix PROJECT File Scope (MINOR)

**Problem**: The `PROJECT` file may have stale metadata after CRD renames.

**Fix**: Run `make manifests` and verify the `PROJECT` file's `group` field
matches `aiscaler.io`.

---

### Task 0.11 — Add Real Unit Tests (NICE-TO-HAVE)

**Problem**: The existing tests in `internal/controller/` are scaffold-only
(generated by kubebuilder, don't test real logic).

**Files to create**:
- `internal/signals/metrics_test.go`
- `internal/signals/prometheus_test.go`
- `internal/decision/validator_test.go`
- `internal/llm/router_test.go`
- `internal/actuator/actuator_test.go`

Each test file should:
1. Use standard Go `testing` package (not Ginkgo) for unit tests
2. Mock external dependencies (K8s client, HTTP server, LLM API)
3. Test both happy path and error paths

**Example** — `internal/decision/validator_test.go`:
```go
package decision

import (
    "testing"

    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
    "github.com/sanjbh/kube-scaling-agent/internal/llm"
)

func TestValidate_NoClamp(t *testing.T) {
    v := NewValidator()
    d := &llm.ScalingDecision{TargetReplicas: 5}
    policy := &aiscalerv1.AIScaler{
        Spec: aiscalerv1.AIScalerSpec{
            Constraints: aiscalerv1.ScalingConstraints{
                MinReplicas:  1,
                MaxReplicas:  10,
                MaxScaleStep: 3,
            },
        },
    }
    result := v.Validate(d, 4, policy)
    if result.Clamped {
        t.Errorf("expected no clamp, got clamped to %d", result.ValidatedReplicas)
    }
    if result.ValidatedReplicas != 5 {
        t.Errorf("expected 5, got %d", result.ValidatedReplicas)
    }
}

func TestValidate_ClampMaxStep(t *testing.T) {
    v := NewValidator()
    d := &llm.ScalingDecision{TargetReplicas: 10}
    policy := &aiscalerv1.AIScaler{
        Spec: aiscalerv1.AIScalerSpec{
            Constraints: aiscalerv1.ScalingConstraints{
                MinReplicas:  1,
                MaxReplicas:  20,
                MaxScaleStep: 3,
            },
        },
    }
    result := v.Validate(d, 3, policy)
    if !result.Clamped {
        t.Error("expected clamp")
    }
    if result.ValidatedReplicas != 6 {
        t.Errorf("expected 6 (3+3), got %d", result.ValidatedReplicas)
    }
}

func TestValidate_ClampMinReplicas(t *testing.T) {
    v := NewValidator()
    d := &llm.ScalingDecision{TargetReplicas: 0}
    policy := &aiscalerv1.AIScaler{
        Spec: aiscalerv1.AIScalerSpec{
            Constraints: aiscalerv1.ScalingConstraints{
                MinReplicas:  2,
                MaxReplicas:  10,
                MaxScaleStep: 5,
            },
        },
    }
    result := v.Validate(d, 3, policy)
    if result.ValidatedReplicas != 2 {
        t.Errorf("expected 2, got %d", result.ValidatedReplicas)
    }
}
```

---

## Phase 1 — Extended Signal Sources

> **Goal**: Make the signal collector pluggable so new metric sources can be
> added without modifying existing code. Add queue depth, custom PromQL
> queries, and event-based signals.

### Task 1.1 — Define the SignalSource Interface

**Create file**: `internal/signals/source.go`

```go
package signals

import (
    "context"

    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

// SignalSource is the interface for any pluggable signal provider.
// Each source populates relevant fields in the Bundle.
// Returning an error marks the source as "unavailable" — it does NOT
// fail the entire reconcile cycle.
type SignalSource interface {
    // Name returns a human-readable name for logging and conditions.
    Name() string

    // Collect fetches signals and writes them into the bundle.
    Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *Bundle) error

    // Required returns true if this source's failure should abort the cycle.
    Required() bool
}
```

### Task 1.2 — Refactor Collector to Use SignalSource

**File**: `internal/signals/collector.go`

Replace the three hard-coded sub-collectors with a slice of `SignalSource`:

```go
type Collector struct {
    sources []SignalSource
}

func NewCollector(client client.Client) (*Collector, error) {
    cfg, err := config.GetConfig()
    if err != nil {
        return nil, fmt.Errorf("failed to get k8s config: %w", err)
    }
    mc, err := metricsclient.NewForConfig(cfg)
    if err != nil {
        return nil, fmt.Errorf("failed to create metrics client: %w", err)
    }

    return &Collector{
        sources: []SignalSource{
            &metricsCollector{client: client, metricsClient: mc},  // Required
            &prometheusCollector{},                                 // Optional
            &annotationCollector{client: client},                   // Optional
        },
    }, nil
}

func (c *Collector) Collect(ctx context.Context, policy *aiscalerv1.AIScaler) (*Bundle, error) {
    log := logf.FromContext(ctx)
    bundle := &Bundle{CollectedAt: time.Now()}

    for _, src := range c.sources {
        if err := src.Collect(ctx, policy, bundle); err != nil {
            if src.Required() {
                return nil, fmt.Errorf("required source %s failed: %w", src.Name(), err)
            }
            log.Info("optional signal source unavailable",
                "source", src.Name(), "error", err)
        }
    }
    return bundle, nil
}
```

Make each existing collector implement `SignalSource`:

```go
// In metrics.go
func (m *metricsCollector) Name() string    { return "metrics-server" }
func (m *metricsCollector) Required() bool  { return true }

// In prometheus.go
func (p *prometheusCollector) Name() string   { return "prometheus" }
func (p *prometheusCollector) Required() bool { return false }

// In annotations.go
func (a *annotationCollector) Name() string   { return "annotations" }
func (a *annotationCollector) Required() bool { return false }
```

### Task 1.3 — Add Queue Depth Signal Source

**Create file**: `internal/signals/queue.go`

```go
package signals

import (
    "context"

    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

// queueCollector reads queue depth from Prometheus using a configurable
// PromQL query provided in the AIScaler spec.
type queueCollector struct{}

func (q *queueCollector) Name() string    { return "queue-depth" }
func (q *queueCollector) Required() bool  { return false }

func (q *queueCollector) Collect(
    ctx context.Context,
    policy *aiscalerv1.AIScaler,
    bundle *Bundle,
) error {
    // Read the queue-depth PromQL query from the spec
    // (requires adding QueueDepthQuery to PrometheusConfig — see Task 1.4)
    query := policy.Spec.Prometheus.QueueDepthQuery
    if query == "" {
        return nil
    }
    baseURL := policy.Spec.Prometheus.BaseURL
    if baseURL == "" {
        return nil
    }

    pc := &prometheusCollector{}
    val, err := pc.query(ctx, baseURL, query)
    if err != nil {
        return err
    }
    bundle.QueueDepth = val
    return nil
}
```

### Task 1.4 — Extend Bundle and CRD for New Signals

**File**: `internal/signals/collector.go` — add to `Bundle`:
```go
type Bundle struct {
    // ... existing fields ...

    // Queue depth (from Prometheus PromQL)
    QueueDepth float64

    // Custom PromQL signals — user-defined key→value pairs
    CustomSignals map[string]float64
}
```

**File**: `api/v1/aiscaler_types.go` — add to `PrometheusConfig`:
```go
type PrometheusConfig struct {
    // ... existing fields ...

    // +optional
    QueueDepthQuery string `json:"queueDepthQuery,omitempty"`

    // +optional
    CustomQueries map[string]string `json:"customQueries,omitempty"`
}
```

Run `make manifests` to regenerate CRD and deepcopy.

### Task 1.5 — Add Custom PromQL Signal Source

**Create file**: `internal/signals/custom_promql.go`

```go
package signals

import (
    "context"
    "fmt"

    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

type customPromQLCollector struct{}

func (c *customPromQLCollector) Name() string    { return "custom-promql" }
func (c *customPromQLCollector) Required() bool  { return false }

func (c *customPromQLCollector) Collect(
    ctx context.Context,
    policy *aiscalerv1.AIScaler,
    bundle *Bundle,
) error {
    if len(policy.Spec.Prometheus.CustomQueries) == 0 {
        return nil
    }
    baseURL := policy.Spec.Prometheus.BaseURL
    if baseURL == "" {
        return nil
    }
    if bundle.CustomSignals == nil {
        bundle.CustomSignals = make(map[string]float64)
    }
    pc := &prometheusCollector{}
    for name, query := range policy.Spec.Prometheus.CustomQueries {
        val, err := pc.query(ctx, baseURL, query)
        if err != nil {
            return fmt.Errorf("custom query %q failed: %w", name, err)
        }
        bundle.CustomSignals[name] = val
    }
    return nil
}
```

Register both new sources in `NewCollector`.

### Task 1.6 — Update the LLM Prompt with New Signals

**File**: `internal/llm/prompt.go`

Add to `ScalingRequest`:
```go
type ScalingRequest struct {
    // ... existing fields ...
    QueueDepth    float64
    CustomSignals map[string]float64
}
```

Add to `buildPrompt()` after the existing metrics section:
```go
if req.QueueDepth > 0 {
    userPrompt += fmt.Sprintf("- Queue depth: %.0f\n", req.QueueDepth)
}
for name, val := range req.CustomSignals {
    userPrompt += fmt.Sprintf("- %s: %.2f\n", name, val)
}
```

Update `fetchDecision()` in the controller to pass these new fields from the
bundle to the `ScalingRequest`.

---

## Phase 2 — Natural-Language Scheduling & Reactive Rules

> **Goal**: Allow users to define scaling schedules and reactive rules in
> natural language via CRD annotations or a new `ScheduledScaling` CRD.

### Task 2.1 — Create ScheduledScaling CRD

**File**: `api/v1/scheduledscaling_types.go`

```go
package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type ScheduleRule struct {
    // Cron expression (standard 5-field)
    // +kubebuilder:validation:Required
    Cron string `json:"cron"`

    // Target replica count for this schedule window
    // +kubebuilder:validation:Minimum=0
    TargetReplicas int32 `json:"targetReplicas"`

    // Duration of this override (e.g., "2h")
    // +kubebuilder:validation:Required
    Duration metav1.Duration `json:"duration"`
}

type ScheduledScalingSpec struct {
    // Reference to the AIScaler this schedule applies to
    // +kubebuilder:validation:Required
    AIScalerRef string `json:"aiScalerRef"`

    // List of scheduled scaling rules
    // +kubebuilder:validation:MinItems=1
    Rules []ScheduleRule `json:"rules"`
}

type ScheduledScalingStatus struct {
    // +optional
    ActiveRule *string `json:"activeRule,omitempty"`
    // +optional
    NextTrigger *metav1.Time `json:"nextTrigger,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=ss
type ScheduledScaling struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Spec              ScheduledScalingSpec   `json:"spec,omitempty"`
    Status            ScheduledScalingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ScheduledScalingList struct {
    metav1.TypeMeta `json:",inline"`
    metav1.ListMeta `json:"metadata,omitempty"`
    Items           []ScheduledScaling `json:"items"`
}

func init() {
    SchemeBuilder.Register(&ScheduledScaling{}, &ScheduledScalingList{})
}
```

### Task 2.2 — Implement the Schedule Controller

**Create file**: `internal/controller/scheduledscaling_controller.go`

This controller:
1. Lists all `ScheduledScaling` resources
2. Uses `robfig/cron/v3` to parse cron expressions
3. Checks if any rule is currently "active" (cron triggered within duration)
4. If active, overrides the AIScaler's min/max replicas for that window
5. Requeues at the next cron trigger time

**Dependencies to add**:
```bash
go get github.com/robfig/cron/v3
```

**Key function**:
```go
func (r *ScheduledScalingReconciler) Reconcile(
    ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
    // 1. Fetch ScheduledScaling
    // 2. For each rule, check if cron means "now is in the active window"
    // 3. If active, update referenced AIScaler's constraints
    // 4. Compute next trigger time, requeue
}
```

### Task 2.3 — Add Reactive Rule Annotations

Reactive rules let users express conditions like "if error rate > 5%, scale up
by 2 immediately". Implement via annotations on the Deployment:

**Add annotation keys** to `internal/signals/annotations.go`:
```go
const (
    // ... existing annotations ...
    annotationReactiveRules = "aiscaler.io/reactive-rules"
)
```

**Add to AnnotationSignals**:
```go
type AnnotationSignals struct {
    // ... existing fields ...
    ReactiveRules []ReactiveRule
}

type ReactiveRule struct {
    Metric    string  `json:"metric"`    // cpu, memory, error_rate, p95_latency
    Operator  string  `json:"operator"`  // >, <, >=, <=
    Threshold float64 `json:"threshold"`
    Action    string  `json:"action"`    // scale_up, scale_down
    Amount    int32   `json:"amount"`    // +/- N replicas
}
```

Parse the JSON annotation in `annotationCollector.collect()`.

### Task 2.4 — Implement Reactive Rule Evaluation

**Create file**: `internal/decision/reactive.go`

```go
package decision

import (
    "github.com/sanjbh/kube-scaling-agent/internal/signals"
)

// EvaluateReactiveRules checks reactive rules against the signal bundle.
// If any rule fires, it returns a recommended replica delta.
// Returns (delta, firedRuleName, fired).
func EvaluateReactiveRules(
    bundle *signals.Bundle,
    rules []signals.ReactiveRule,
) (int32, string, bool) {
    for _, rule := range rules {
        val := metricValue(bundle, rule.Metric)
        if matches(val, rule.Operator, rule.Threshold) {
            switch rule.Action {
            case "scale_up":
                return rule.Amount, rule.Metric, true
            case "scale_down":
                return -rule.Amount, rule.Metric, true
            }
        }
    }
    return 0, "", false
}

func metricValue(b *signals.Bundle, metric string) float64 {
    switch metric {
    case "cpu":
        return b.CPUUtilization
    case "memory":
        return b.MemoryUtilization
    case "error_rate":
        return b.ErrorRate
    case "p95_latency":
        return b.P95LatencyMs
    case "queue_depth":
        return b.QueueDepth
    default:
        return 0
    }
}

func matches(val float64, op string, threshold float64) bool {
    switch op {
    case ">":
        return val > threshold
    case "<":
        return val < threshold
    case ">=":
        return val >= threshold
    case "<=":
        return val <= threshold
    default:
        return false
    }
}
```

### Task 2.5 — Integrate Reactive Rules into the Reconcile Loop

In `aiscaler_controller.go`, add a new step **between** `collectSignals` and
`fetchDecision`:

```go
{Name: "evaluateReactiveRules", Run: r.evaluateReactiveRules},
```

```go
func (r *AIScalerReconciler) evaluateReactiveRules(
    ctx context.Context, state *reconcileState,
) (*ctrl.Result, error) {
    rules := state.bundle.Annotations.ReactiveRules
    if len(rules) == 0 {
        return nil, nil
    }
    delta, metric, fired := decision.EvaluateReactiveRules(state.bundle, rules)
    if !fired {
        return nil, nil
    }
    log := logf.FromContext(ctx)
    log.Info("reactive rule fired", "metric", metric, "delta", delta)

    target := state.bundle.CurrentReplicas + delta
    state.decision = &llm.ScalingDecision{
        TargetReplicas: target,
        Reasoning:      fmt.Sprintf("reactive rule: %s triggered", metric),
        Confidence:     1.0,
    }
    state.provider = "reactive-rule"
    // Skip LLM call — jump to validation
    return nil, nil // next step will be validateDecision
}
```

Modify the step chain so that `fetchDecision` is skipped if `state.decision` is
already set by the reactive rule step.

---

## Phase 3 — Vertical Scaling & In-Place Resize

> **Goal**: Add the ability to adjust CPU/memory requests and limits on pods,
> using Kubernetes 1.33+ in-place pod resize when available.

### Task 3.1 — Add VPA Spec to CRD

**File**: `api/v1/aiscaler_types.go`

```go
type VerticalScalingConfig struct {
    // +kubebuilder:default=false
    Enabled bool `json:"enabled"`

    // Container names to target. Empty = all containers.
    // +optional
    ContainerNames []string `json:"containerNames,omitempty"`

    // Minimum resource requests
    // +optional
    MinCPU string `json:"minCPU,omitempty"` // e.g., "100m"
    // +optional
    MinMemory string `json:"minMemory,omitempty"` // e.g., "128Mi"

    // Maximum resource requests
    // +optional
    MaxCPU string `json:"maxCPU,omitempty"` // e.g., "4"
    // +optional
    MaxMemory string `json:"maxMemory,omitempty"` // e.g., "8Gi"

    // UpdateMode: "InPlace" for K8s 1.33+, "Recreate" for older clusters
    // +kubebuilder:validation:Enum=InPlace;Recreate
    // +kubebuilder:default="Recreate"
    UpdateMode string `json:"updateMode,omitempty"`
}
```

Add to `AIScalerSpec`:
```go
type AIScalerSpec struct {
    // ... existing fields ...
    // +optional
    VerticalScaling *VerticalScalingConfig `json:"verticalScaling,omitempty"`
}
```

### Task 3.2 — Extend LLM Response for Vertical Decisions

**File**: `internal/llm/router.go`

Add vertical recommendations to `ScalingDecision`:
```go
type ResourceRecommendation struct {
    ContainerName string `json:"container_name"`
    CPURequest    string `json:"cpu_request"`    // e.g., "500m"
    MemoryRequest string `json:"memory_request"` // e.g., "256Mi"
}

type ScalingDecision struct {
    TargetReplicas int32                    `json:"target_replicas"`
    Reasoning      string                   `json:"reasoning"`
    Confidence     float64                  `json:"confidence"`
    Resources      []ResourceRecommendation `json:"resources,omitempty"`
}
```

### Task 3.3 — Update the Prompt to Request Vertical Decisions

**File**: `internal/llm/prompt.go`

When `policy.Spec.VerticalScaling.Enabled` is true, add to the system prompt:

```
You may also recommend resource changes for containers.
If you recommend resource changes, include a "resources" array in your JSON response:
{
  "target_replicas": <integer>,
  "reasoning": "<brief explanation>",
  "confidence": <float 0-1>,
  "resources": [
    {"container_name": "app", "cpu_request": "500m", "memory_request": "256Mi"}
  ]
}
```

Add current resource requests/limits to the user prompt so the LLM knows the
current state.

### Task 3.4 — Implement Vertical Actuator

**Create file**: `internal/actuator/vertical.go`

```go
package actuator

import (
    "context"
    "fmt"

    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
    "github.com/sanjbh/kube-scaling-agent/internal/llm"
    appsv1 "k8s.io/api/apps/v1"
    corev1 "k8s.io/api/core/v1"
    "k8s.io/apimachinery/pkg/api/resource"
    "k8s.io/apimachinery/pkg/types"
    "sigs.k8s.io/controller-runtime/pkg/client"
)

// ApplyVertical patches container resource requests on the target Deployment.
func (a *Actuator) ApplyVertical(
    ctx context.Context,
    policy *aiscalerv1.AIScaler,
    recommendations []llm.ResourceRecommendation,
) error {
    if policy.Spec.VerticalScaling == nil || !policy.Spec.VerticalScaling.Enabled {
        return nil
    }
    if len(recommendations) == 0 {
        return nil
    }

    deploy := &appsv1.Deployment{}
    key := types.NamespacedName{
        Namespace: policy.Spec.TargetRef.Namespace,
        Name:      policy.Spec.TargetRef.Name,
    }
    if err := a.client.Get(ctx, key, deploy); err != nil {
        return fmt.Errorf("failed to get deployment: %w", err)
    }

    // Build patch
    for _, rec := range recommendations {
        for i := range deploy.Spec.Template.Spec.Containers {
            c := &deploy.Spec.Template.Spec.Containers[i]
            if c.Name != rec.ContainerName {
                continue
            }
            if c.Resources.Requests == nil {
                c.Resources.Requests = corev1.ResourceList{}
            }
            if rec.CPURequest != "" {
                c.Resources.Requests[corev1.ResourceCPU] = resource.MustParse(rec.CPURequest)
            }
            if rec.MemoryRequest != "" {
                c.Resources.Requests[corev1.ResourceMemory] = resource.MustParse(rec.MemoryRequest)
            }
        }
    }

    // Validate against min/max from vertical config
    // ... (clamp values within MinCPU/MaxCPU and MinMemory/MaxMemory)

    // Apply using SSA
    return a.client.Patch(ctx, deploy,
        client.Apply,
        client.FieldOwner("aiscaler"),
        client.ForceOwnership,
    )
}
```

### Task 3.5 — Add Vertical Step to Reconcile Chain

In the controller, add after `actuate`:
```go
{Name: "actuateVertical", Run: r.actuateVertical},
```

---

## Phase 4 — Cost Optimization Engine

> **Goal**: Integrate with OpenCost (or kubecost) to add cost awareness to
> scaling decisions.

### Task 4.1 — Add Cost Signal Source

**Create file**: `internal/signals/cost.go`

```go
package signals

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "time"

    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

type costCollector struct{}

func (c *costCollector) Name() string    { return "cost" }
func (c *costCollector) Required() bool  { return false }

type openCostResponse struct {
    TotalCost   float64 `json:"totalCost"`
    CPUCost     float64 `json:"cpuCost"`
    MemoryCost  float64 `json:"memoryCost"`
    Efficiency  float64 `json:"efficiency"` // 0-1
}

func (c *costCollector) Collect(
    ctx context.Context,
    policy *aiscalerv1.AIScaler,
    bundle *Bundle,
) error {
    // OpenCost API endpoint — configured via annotation or CRD field
    baseURL := policy.GetAnnotations()["aiscaler.io/opencost-url"]
    if baseURL == "" {
        return nil
    }

    endpoint := fmt.Sprintf(
        "%s/allocation/compute?window=1h&namespace=%s&filterControllers=%s",
        baseURL,
        policy.Spec.TargetRef.Namespace,
        policy.Spec.TargetRef.Name,
    )

    httpClient := &http.Client{Timeout: 10 * time.Second}
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
    if err != nil {
        return err
    }
    resp, err := httpClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    var result openCostResponse
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return err
    }

    bundle.CostPerHour = result.TotalCost
    bundle.CostEfficiency = result.Efficiency
    return nil
}
```

### Task 4.2 — Add Cost Fields to Bundle

```go
type Bundle struct {
    // ... existing fields ...
    CostPerHour    float64
    CostEfficiency float64 // 0-1 (resource utilization efficiency)
}
```

### Task 4.3 — Add Cost Context to LLM Prompt

**File**: `internal/llm/prompt.go`

```go
if req.CostPerHour > 0 {
    userPrompt += fmt.Sprintf("\nCost: $%.4f/hour (efficiency: %.0f%%)",
        req.CostPerHour, req.CostEfficiency*100)
    userPrompt += "\nInstruction: Consider cost efficiency when scaling."
}
```

### Task 4.4 — Add Cost Spec to CRD

```go
type CostConfig struct {
    // +optional
    MonthlyBudget float64 `json:"monthlyBudget,omitempty"`
    // +optional
    OpenCostURL string `json:"openCostURL,omitempty"`
    // +kubebuilder:default=false
    OptimizeForCost bool `json:"optimizeForCost,omitempty"`
}
```

Add to `AIScalerSpec`:
```go
// +optional
Cost *CostConfig `json:"cost,omitempty"`
```

---

## Phase 5 — SLO-First Multi-Signal Scaling

> **Goal**: Let users define SLOs (e.g., "p99 < 200ms", "error rate < 0.1%")
> and make the LLM use them as the primary scaling trigger.

### Task 5.1 — Add SLO Spec to CRD

```go
type SLO struct {
    // +kubebuilder:validation:Required
    Name string `json:"name"`

    // The metric to track: p95_latency, p99_latency, error_rate, availability
    // +kubebuilder:validation:Required  
    Metric string `json:"metric"`

    // The threshold value (unit depends on metric)
    // +kubebuilder:validation:Required
    Target float64 `json:"target"`

    // Priority relative to other SLOs (lower = higher priority)
    // +kubebuilder:default=100
    Priority int32 `json:"priority,omitempty"`
}
```

Add to `AIScalerSpec`:
```go
// +optional
SLOs []SLO `json:"slos,omitempty"`
```

### Task 5.2 — SLO Evaluation Engine

**Create file**: `internal/decision/slo.go`

```go
package decision

import (
    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
    "github.com/sanjbh/kube-scaling-agent/internal/signals"
)

type SLOStatus struct {
    Name      string
    Metric    string
    Target    float64
    Actual    float64
    Breached  bool
    Margin    float64 // percentage above/below target
}

// EvaluateSLOs checks all SLOs against the signal bundle.
func EvaluateSLOs(
    slos []aiscalerv1.SLO,
    bundle *signals.Bundle,
) []SLOStatus {
    var results []SLOStatus
    for _, slo := range slos {
        actual := metricValueForSLO(bundle, slo.Metric)
        breached := actual > slo.Target
        margin := 0.0
        if slo.Target > 0 {
            margin = (slo.Target - actual) / slo.Target * 100
        }
        results = append(results, SLOStatus{
            Name:     slo.Name,
            Metric:   slo.Metric,
            Target:   slo.Target,
            Actual:   actual,
            Breached: breached,
            Margin:   margin,
        })
    }
    return results
}

func metricValueForSLO(b *signals.Bundle, metric string) float64 {
    switch metric {
    case "p95_latency":
        return b.P95LatencyMs
    case "error_rate":
        return b.ErrorRate * 100
    default:
        return 0
    }
}
```

### Task 5.3 — Add SLO Status to Prompt

In the user prompt, before "What should the replica count be?":
```go
if len(sloStatuses) > 0 {
    userPrompt += "\nSLO Status:\n"
    for _, s := range sloStatuses {
        status := "OK"
        if s.Breached {
            status = "BREACHED"
        }
        userPrompt += fmt.Sprintf("  - %s (%s): target=%.2f actual=%.2f [%s] margin=%.1f%%\n",
            s.Name, s.Metric, s.Target, s.Actual, status, s.Margin)
    }
    userPrompt += "\nPrioritize keeping SLOs within target. If any SLO is breached, scale up."
}
```

---

## Phase 6 — Predictive & Seasonal Scaling

> **Goal**: Store historical metrics and use them to predict upcoming load,
> allowing the operator to pre-scale before traffic spikes.

### Task 6.1 — Historical Metrics Storage

**Create file**: `internal/history/store.go`

```go
package history

import (
    "sync"
    "time"
)

type MetricPoint struct {
    Timestamp       time.Time
    CPUUtilization  float64
    MemoryUtilization float64
    P95LatencyMs    float64
    ErrorRate       float64
    Replicas        int32
}

// Store is a simple in-memory ring buffer for metric history.
// In production, this would be backed by a database.
type Store struct {
    mu      sync.RWMutex
    points  map[string][]MetricPoint // keyed by deployment name
    maxSize int
}

func NewStore(maxSize int) *Store {
    return &Store{
        points:  make(map[string][]MetricPoint),
        maxSize: maxSize,
    }
}

func (s *Store) Record(deployName string, point MetricPoint) {
    s.mu.Lock()
    defer s.mu.Unlock()
    pts := s.points[deployName]
    if len(pts) >= s.maxSize {
        pts = pts[1:]
    }
    s.points[deployName] = append(pts, point)
}

func (s *Store) LastN(deployName string, n int) []MetricPoint {
    s.mu.RLock()
    defer s.mu.RUnlock()
    pts := s.points[deployName]
    if len(pts) <= n {
        return pts
    }
    return pts[len(pts)-n:]
}

// SameTimeLastWeek returns metrics from the same hour one week ago (±30min).
func (s *Store) SameTimeLastWeek(deployName string) []MetricPoint {
    s.mu.RLock()
    defer s.mu.RUnlock()
    now := time.Now()
    target := now.AddDate(0, 0, -7)
    window := 30 * time.Minute
    var results []MetricPoint
    for _, pt := range s.points[deployName] {
        if pt.Timestamp.After(target.Add(-window)) &&
            pt.Timestamp.Before(target.Add(window)) {
            results = append(results, pt)
        }
    }
    return results
}
```

### Task 6.2 — Record Metrics After Each Reconcile

In the controller's `updateStatus` step, call:
```go
r.History.Record(state.obj.Spec.TargetRef.Name, history.MetricPoint{
    Timestamp:         time.Now(),
    CPUUtilization:    state.bundle.CPUUtilization,
    MemoryUtilization: state.bundle.MemoryUtilization,
    P95LatencyMs:      state.bundle.P95LatencyMs,
    ErrorRate:         state.bundle.ErrorRate,
    Replicas:          state.bundle.CurrentReplicas,
})
```

### Task 6.3 — Add Historical Context to the LLM Prompt

Pass the last 10 data points and the same-time-last-week data to the prompt:

```go
if len(recentHistory) > 0 {
    userPrompt += "\nRecent metric history (last 10 cycles):\n"
    for _, pt := range recentHistory {
        userPrompt += fmt.Sprintf("  %s: cpu=%.1f%% mem=%.1f%% p95=%.1fms replicas=%d\n",
            pt.Timestamp.Format("15:04"), pt.CPUUtilization,
            pt.MemoryUtilization, pt.P95LatencyMs, pt.Replicas)
    }
}
if len(lastWeekHistory) > 0 {
    userPrompt += "\nSame time last week:\n"
    for _, pt := range lastWeekHistory {
        userPrompt += fmt.Sprintf("  %s: cpu=%.1f%% p95=%.1fms replicas=%d\n",
            pt.Timestamp.Format("2006-01-02 15:04"),
            pt.CPUUtilization, pt.P95LatencyMs, pt.Replicas)
    }
    userPrompt += "\nConsider seasonal patterns when making your decision."
}
```

---

## Phase 7 — Node-Aware Cluster-Level Intelligence

> **Goal**: Add node resource awareness so the LLM can consider available cluster
> capacity when making scaling decisions.

### Task 7.1 — Node Metrics Signal Source

**Create file**: `internal/signals/nodes.go`

```go
package signals

import (
    "context"
    "fmt"

    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
    corev1 "k8s.io/api/core/v1"
    "sigs.k8s.io/controller-runtime/pkg/client"
)

type nodeCollector struct {
    client client.Client
}

func (n *nodeCollector) Name() string    { return "node-resources" }
func (n *nodeCollector) Required() bool  { return false }

func (n *nodeCollector) Collect(
    ctx context.Context,
    policy *aiscalerv1.AIScaler,
    bundle *Bundle,
) error {
    nodeList := &corev1.NodeList{}
    if err := n.client.List(ctx, nodeList); err != nil {
        return fmt.Errorf("failed to list nodes: %w", err)
    }

    var totalCPU, allocCPU, totalMem, allocMem int64
    for _, node := range nodeList.Items {
        // Skip unschedulable nodes
        if node.Spec.Unschedulable {
            continue
        }
        totalCPU += node.Status.Allocatable.Cpu().MilliValue()
        totalMem += node.Status.Allocatable.Memory().Value()
        allocCPU += node.Status.Capacity.Cpu().MilliValue() -
            node.Status.Allocatable.Cpu().MilliValue()
        allocMem += node.Status.Capacity.Memory().Value() -
            node.Status.Allocatable.Memory().Value()
    }

    bundle.ClusterNodes = int32(len(nodeList.Items))
    if totalCPU > 0 {
        bundle.ClusterCPUAvailable = float64(totalCPU-allocCPU) / float64(totalCPU) * 100
    }
    if totalMem > 0 {
        bundle.ClusterMemAvailable = float64(totalMem-allocMem) / float64(totalMem) * 100
    }
    return nil
}
```

### Task 7.2 — Add Cluster Fields to Bundle

```go
type Bundle struct {
    // ... existing fields ...
    ClusterNodes        int32
    ClusterCPUAvailable float64 // percentage of total cluster CPU free
    ClusterMemAvailable float64 // percentage of total cluster memory free
}
```

### Task 7.3 — Add RBAC for Nodes

```go
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
```

### Task 7.4 — Add Cluster Context to Prompt

```go
if bundle.ClusterNodes > 0 {
    userPrompt += fmt.Sprintf("\nCluster: %d nodes, %.1f%% CPU available, %.1f%% memory available\n",
        bundle.ClusterNodes, bundle.ClusterCPUAvailable, bundle.ClusterMemAvailable)
    userPrompt += "Consider available cluster capacity. If cluster is nearly full, prefer conservative scaling."
}
```

---

## Phase 8 — KEDA Integration & Composability

> **Goal**: Allow the AIScaler to work alongside KEDA ScaledObjects, either
> as the primary scaler with KEDA as a signal source, or as an intelligent
> override on top of KEDA.

### Task 8.1 — KEDA Signal Source

**Create file**: `internal/signals/keda.go`

```go
package signals

import (
    "context"
    "fmt"

    aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
    "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
    "k8s.io/apimachinery/pkg/types"
    "sigs.k8s.io/controller-runtime/pkg/client"
)

type kedaCollector struct {
    client client.Client
}

func (k *kedaCollector) Name() string    { return "keda" }
func (k *kedaCollector) Required() bool  { return false }

func (k *kedaCollector) Collect(
    ctx context.Context,
    policy *aiscalerv1.AIScaler,
    bundle *Bundle,
) error {
    // Check for annotation pointing to a KEDA ScaledObject
    kedaRef := policy.GetAnnotations()["aiscaler.io/keda-scaledobject"]
    if kedaRef == "" {
        return nil
    }

    // Read the ScaledObject as unstructured (no KEDA dependency needed)
    so := &unstructured.Unstructured{}
    so.SetGroupVersionKind(schema.GroupVersionKind{
        Group:   "keda.sh",
        Version: "v1alpha1",
        Kind:    "ScaledObject",
    })
    key := types.NamespacedName{
        Namespace: policy.Spec.TargetRef.Namespace,
        Name:      kedaRef,
    }
    if err := k.client.Get(ctx, key, so); err != nil {
        return fmt.Errorf("failed to get KEDA ScaledObject %s: %w", kedaRef, err)
    }

    // Extract KEDA's desired replicas from status
    desiredReplicas, found, _ := unstructured.NestedInt64(so.Object,
        "status", "desiredReplicas")
    if found {
        bundle.KEDADesiredReplicas = int32(desiredReplicas)
    }
    return nil
}
```

### Task 8.2 — Add KEDA Fields to Bundle

```go
type Bundle struct {
    // ... existing fields ...
    KEDADesiredReplicas int32 // 0 means KEDA not in use
}
```

### Task 8.3 — Add KEDA Context to the Prompt

```go
if req.KEDADesiredReplicas > 0 {
    userPrompt += fmt.Sprintf("\nKEDA recommends %d replicas based on external metrics.\n",
        req.KEDADesiredReplicas)
    userPrompt += "Consider KEDA's recommendation as an additional signal."
}
```

---

## Phase 9 — Safety, Guardrails & Production Hardening

> **Goal**: Add circuit breakers, rate limiting, approval workflows, and
> confidence-based gating.

### Task 9.1 — Confidence Threshold Gating

The LLM returns a `confidence` field (0-1) that is currently parsed but never
used in decision-making.

**File**: `api/v1/aiscaler_types.go`

Add to `ScalingConstraints`:
```go
type ScalingConstraints struct {
    // ... existing fields ...

    // MinConfidence is the minimum confidence score from the LLM to act.
    // Decisions below this threshold are logged but not applied.
    // +kubebuilder:validation:Minimum=0
    // +kubebuilder:validation:Maximum=1
    // +kubebuilder:default=0.5
    // +optional
    MinConfidence float64 `json:"minConfidence,omitempty"`
}
```

**File**: `internal/controller/aiscaler_controller.go`

In `fetchDecision`, after getting the decision:
```go
if scalingDecision.Confidence < obj.Spec.Constraints.MinConfidence {
    log.Info("LLM confidence below threshold, skipping",
        "confidence", scalingDecision.Confidence,
        "threshold", obj.Spec.Constraints.MinConfidence)
    return &ctrl.Result{
        RequeueAfter: obj.Spec.EvaluationInterval.Duration,
    }, nil
}
```

### Task 9.2 — Circuit Breaker for LLM Failures

**Create file**: `internal/llm/circuit_breaker.go`

```go
package llm

import (
    "sync"
    "time"
)

type CircuitBreaker struct {
    mu              sync.Mutex
    failureCount    int
    lastFailure     time.Time
    threshold       int           // failures before opening
    resetTimeout    time.Duration // how long to wait before trying again
    state           string        // "closed", "open", "half-open"
}

func NewCircuitBreaker(threshold int, resetTimeout time.Duration) *CircuitBreaker {
    return &CircuitBreaker{
        threshold:    threshold,
        resetTimeout: resetTimeout,
        state:        "closed",
    }
}

func (cb *CircuitBreaker) Allow() bool {
    cb.mu.Lock()
    defer cb.mu.Unlock()

    switch cb.state {
    case "closed":
        return true
    case "open":
        if time.Since(cb.lastFailure) > cb.resetTimeout {
            cb.state = "half-open"
            return true
        }
        return false
    case "half-open":
        return true
    }
    return false
}

func (cb *CircuitBreaker) RecordSuccess() {
    cb.mu.Lock()
    defer cb.mu.Unlock()
    cb.failureCount = 0
    cb.state = "closed"
}

func (cb *CircuitBreaker) RecordFailure() {
    cb.mu.Lock()
    defer cb.mu.Unlock()
    cb.failureCount++
    cb.lastFailure = time.Now()
    if cb.failureCount >= cb.threshold {
        cb.state = "open"
    }
}
```

### Task 9.3 — Integrate Circuit Breaker into Router

In `Router.Decide()`:
```go
if !r.circuitBreaker.Allow() {
    return nil, "", fmt.Errorf("circuit breaker open: too many LLM failures")
}

decision, err := r.callProvider(...)
if err != nil {
    r.circuitBreaker.RecordFailure()
    // ... fallback logic ...
} else {
    r.circuitBreaker.RecordSuccess()
}
```

### Task 9.4 — Human-in-the-Loop Approval (Optional)

For production-critical workloads, add an approval step.

**Create CRD**: `api/v1/scalingapproval_types.go`

```go
type ScalingApprovalSpec struct {
    AIScalerName    string `json:"aiScalerName"`
    ProposedReplicas int32  `json:"proposedReplicas"`
    CurrentReplicas  int32  `json:"currentReplicas"`
    Reasoning        string `json:"reasoning"`
    Provider         string `json:"provider"`
    Confidence       float64 `json:"confidence"`
}

type ScalingApprovalStatus struct {
    Approved  *bool      `json:"approved,omitempty"`
    ApprovedBy string    `json:"approvedBy,omitempty"`
    ApprovedAt *metav1.Time `json:"approvedAt,omitempty"`
}
```

When `spec.requireApproval: true`, the controller creates a `ScalingApproval`
instead of applying immediately. A separate approval controller or webhook
watches for approved approvals and triggers the actuation.

---

## Phase 10 — Observability, Audit & Replay

> **Goal**: Record every scaling decision for debugging, auditing, and
> replaying.

### Task 10.1 — Scaling Event CRD

**Create file**: `api/v1/scalingevent_types.go`

```go
type ScalingEventSpec struct {
    AIScalerName    string              `json:"aiScalerName"`
    Timestamp       metav1.Time         `json:"timestamp"`
    Provider        aiscalerv1.LLMProvider `json:"provider"`
    Replicas        ReplicaChange       `json:"replicas"`
    Metrics         MetricSnapshot      `json:"metrics"`
    Decision        DecisionSnapshot    `json:"decision"`
    Validation      ValidationSnapshot  `json:"validation"`
    DryRun          bool                `json:"dryRun"`
}

type ReplicaChange struct {
    Before int32 `json:"before"`
    After  int32 `json:"after"`
}

type MetricSnapshot struct {
    CPUUtilization    float64 `json:"cpuUtilization"`
    MemoryUtilization float64 `json:"memoryUtilization"`
    P95LatencyMs      float64 `json:"p95LatencyMs"`
    ErrorRate         float64 `json:"errorRate"`
}

type DecisionSnapshot struct {
    TargetReplicas int32   `json:"targetReplicas"`
    Reasoning      string  `json:"reasoning"`
    Confidence     float64 `json:"confidence"`
    RawResponse    string  `json:"rawResponse,omitempty"`
}

type ValidationSnapshot struct {
    OriginalReplicas  int32  `json:"originalReplicas"`
    ValidatedReplicas int32  `json:"validatedReplicas"`
    Clamped           bool   `json:"clamped"`
    Reason            string `json:"reason,omitempty"`
}
```

### Task 10.2 — Record Events in the Controller

After `updateStatus`, create a `ScalingEvent`:
```go
func (r *AIScalerReconciler) recordEvent(
    ctx context.Context, state *reconcileState,
) (*ctrl.Result, error) {
    event := &aiscalerv1.ScalingEvent{
        ObjectMeta: metav1.ObjectMeta{
            GenerateName: state.obj.Name + "-",
            Namespace:    state.obj.Namespace,
        },
        Spec: aiscalerv1.ScalingEventSpec{
            AIScalerName: state.obj.Name,
            Timestamp:    metav1.Now(),
            Provider:     state.provider,
            Replicas: aiscalerv1.ReplicaChange{
                Before: state.bundle.CurrentReplicas,
                After:  state.validation.ValidatedReplicas,
            },
            Metrics: aiscalerv1.MetricSnapshot{
                CPUUtilization:    state.bundle.CPUUtilization,
                MemoryUtilization: state.bundle.MemoryUtilization,
                P95LatencyMs:      state.bundle.P95LatencyMs,
                ErrorRate:         state.bundle.ErrorRate,
            },
            Decision: aiscalerv1.DecisionSnapshot{
                TargetReplicas: state.decision.TargetReplicas,
                Reasoning:      state.decision.Reasoning,
                Confidence:     state.decision.Confidence,
            },
            Validation: aiscalerv1.ValidationSnapshot{
                OriginalReplicas:  state.validation.OriginalReplicas,
                ValidatedReplicas: state.validation.ValidatedReplicas,
                Clamped:           state.validation.Clamped,
                Reason:            state.validation.Reason,
            },
            DryRun: state.obj.Spec.DryRun,
        },
    }
    return nil, r.Create(ctx, event)
}
```

### Task 10.3 — Prometheus Metrics Exporter

**Create file**: `internal/metrics/exporter.go`

Register custom Prometheus metrics:
```go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
    ScalingDecisions = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "aiscaler_scaling_decisions_total",
            Help: "Total scaling decisions made",
        },
        []string{"aiscaler", "provider", "direction"}, // direction: up, down, none
    )

    LLMLatency = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "aiscaler_llm_latency_seconds",
            Help:    "LLM API call latency",
            Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
        },
        []string{"provider"},
    )

    LLMErrors = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "aiscaler_llm_errors_total",
            Help: "LLM API call errors",
        },
        []string{"provider"},
    )

    ValidationClamps = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "aiscaler_validation_clamps_total",
            Help: "Times validator overrode LLM decision",
        },
        []string{"aiscaler", "reason"},
    )
)

func init() {
    metrics.Registry.MustRegister(
        ScalingDecisions,
        LLMLatency,
        LLMErrors,
        ValidationClamps,
    )
}
```

### Task 10.4 — Instrument the Router and Controller

In `Router.callProvider()`, wrap the LLM call with timing:
```go
start := time.Now()
res, err := client.CreateChatCompletion(ctx, ...)
duration := time.Since(start)
metrics.LLMLatency.WithLabelValues(string(provider)).Observe(duration.Seconds())
if err != nil {
    metrics.LLMErrors.WithLabelValues(string(provider)).Inc()
}
```

In the controller after actuation:
```go
direction := "none"
if state.validation.ValidatedReplicas > state.bundle.CurrentReplicas {
    direction = "up"
} else if state.validation.ValidatedReplicas < state.bundle.CurrentReplicas {
    direction = "down"
}
metrics.ScalingDecisions.WithLabelValues(
    state.obj.Name, string(state.provider), direction,
).Inc()
```

---

## Phase 11 — Multi-Workload & Cluster-Wide Reasoning

> **Goal**: Allow one AIScaler to manage multiple deployments as a group,
> with the LLM reasoning about resource allocation across the entire group.

### Task 11.1 — Extend TargetRef to Support Multiple Targets

```go
type TargetRef struct {
    // Single deployment (existing)
    // +optional
    Name string `json:"name,omitempty"`
    // +optional
    Namespace string `json:"namespace,omitempty"`
}

type MultiTargetRef struct {
    // +optional
    Targets []TargetRef `json:"targets,omitempty"`

    // Label selector to auto-discover deployments
    // +optional
    Selector *metav1.LabelSelector `json:"selector,omitempty"`
}
```

### Task 11.2 — Collect Signals for Multiple Deployments

Modify the Collector to iterate over all targets and produce a per-deployment
Bundle. The LLM receives all bundles in a single prompt.

### Task 11.3 — Extend the LLM Response for Multi-Workload

```go
type MultiScalingDecision struct {
    Decisions []WorkloadDecision `json:"decisions"`
    Reasoning string             `json:"reasoning"`
}

type WorkloadDecision struct {
    Name           string `json:"name"`
    Namespace      string `json:"namespace"`
    TargetReplicas int32  `json:"target_replicas"`
}
```

---

## Phase 12 — LLM Reliability & Provider Routing

> **Goal**: Add structured output schemas, response caching, token budgeting,
> and smart provider selection based on historical accuracy.

### Task 12.1 — Structured Output Schemas

For providers that support it (OpenAI, Gemini), use JSON schema mode:

```go
func (r *Router) callProviderWithSchema(
    ctx context.Context,
    provider aiscalerv1.LLMProvider,
    model string,
    req *ScalingRequest,
) (*ScalingDecision, error) {
    client, model, err := r.buildClient(provider, model)
    if err != nil {
        return nil, err
    }

    // Use response_format if provider supports it
    request := openai.ChatCompletionRequest{
        Model: model,
        Messages: []openai.ChatCompletionMessage{
            {Role: openai.ChatMessageRoleSystem, Content: systemPrompt},
            {Role: openai.ChatMessageRoleUser, Content: buildPrompt(req)},
        },
    }

    // OpenAI and Gemini support response_format
    if provider == aiscalerv1.ProviderGemini || model == "gpt-4o" {
        request.ResponseFormat = &openai.ChatCompletionResponseFormat{
            Type: openai.ChatCompletionResponseFormatTypeJSONObject,
        }
    }

    // ... rest of the call
}
```

### Task 12.2 — Response Caching

**Create file**: `internal/llm/cache.go`

```go
package llm

import (
    "crypto/sha256"
    "encoding/hex"
    "sync"
    "time"
)

type cacheEntry struct {
    decision  *ScalingDecision
    provider  string
    timestamp time.Time
}

type ResponseCache struct {
    mu      sync.RWMutex
    entries map[string]cacheEntry
    ttl     time.Duration
}

func NewResponseCache(ttl time.Duration) *ResponseCache {
    return &ResponseCache{
        entries: make(map[string]cacheEntry),
        ttl:     ttl,
    }
}

func (c *ResponseCache) Key(req *ScalingRequest) string {
    // Hash the request to create a cache key
    // Only cache when metrics haven't changed significantly
    data := fmt.Sprintf("%d-%d-%.0f-%.0f-%.0f-%.2f",
        req.CurrentReplicas, req.MinReplicas,
        req.CPUUtilization, req.MemoryUtilization,
        req.P95LatencyMs, req.ErrorRate)
    h := sha256.Sum256([]byte(data))
    return hex.EncodeToString(h[:])
}

func (c *ResponseCache) Get(key string) (*ScalingDecision, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    entry, ok := c.entries[key]
    if !ok || time.Since(entry.timestamp) > c.ttl {
        return nil, false
    }
    return entry.decision, true
}

func (c *ResponseCache) Set(key string, decision *ScalingDecision) {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.entries[key] = cacheEntry{
        decision:  decision,
        timestamp: time.Now(),
    }
}
```

### Task 12.3 — Provider Accuracy Tracking

Track each provider's decision quality over time:
```go
type ProviderStats struct {
    mu           sync.Mutex
    totalCalls   int
    successCalls int  // no clamping needed
    avgLatency   time.Duration
    lastError    time.Time
}
```

Use these stats to prefer the most accurate/fastest provider for non-critical
decisions.

---

## Phase 13 — FinOps Resource Analysis & Right-Sizing

> **Goal**: Analyze historical resource usage to generate CPU/memory
> right-sizing recommendations and calculate potential savings.

### Task 13.1 — ResourceRecommendation CRD

**Create file**: `api/v1/resourcerecommendation_types.go`

```go
type ContainerRecommendation struct {
    ContainerName string `json:"containerName"`
    CurrentCPU    string `json:"currentCPU"`
    CurrentMemory string `json:"currentMemory"`
    RecommendedCPU    string `json:"recommendedCPU"`
    RecommendedMemory string `json:"recommendedMemory"`
    CPUSavings    string `json:"cpuSavings"`
    MemorySavings string `json:"memorySavings"`
    Confidence    float64 `json:"confidence"`
}

type ResourceRecommendationSpec struct {
    TargetRef   TargetRef                `json:"targetRef"`
    Containers  []ContainerRecommendation `json:"containers"`
    MonthlySavingsEstimate float64       `json:"monthlySavingsEstimate"`
    AnalysisWindow string                 `json:"analysisWindow"` // e.g., "7d"
}
```

### Task 13.2 — Historical Resource Collector

**Create file**: `internal/finops/collector.go`

Collect resource usage percentiles (p50, p95, p99, max) over a configurable
window by querying Prometheus:

```go
package finops

import (
    "context"
    "fmt"
)

type ResourceUsageStats struct {
    P50CPU  float64
    P95CPU  float64
    P99CPU  float64
    MaxCPU  float64
    P50Mem  float64
    P95Mem  float64
    P99Mem  float64
    MaxMem  float64
}

func CollectResourceStats(
    ctx context.Context,
    prometheusURL string,
    namespace string,
    deployment string,
    window string, // e.g., "7d"
) (*ResourceUsageStats, error) {
    // Query Prometheus for CPU percentiles:
    // quantile_over_time(0.50, container_cpu_usage_seconds_total{namespace="X",pod=~"deploy-.*"}[7d])
    // ... similar for memory
    return nil, fmt.Errorf("not implemented")
}
```

### Task 13.3 — Right-Sizing Recommendation Engine

**Create file**: `internal/finops/rightsizer.go`

```go
package finops

import "k8s.io/apimachinery/pkg/api/resource"

type RightSizeResult struct {
    ContainerName     string
    CurrentCPUReq     resource.Quantity
    CurrentMemReq     resource.Quantity
    RecommendedCPUReq resource.Quantity
    RecommendedMemReq resource.Quantity
    Confidence        float64
}

// RightSize computes recommended resource requests based on historical usage.
// Strategy: p95 usage + 20% headroom for CPU, p99 usage + 10% for memory.
func RightSize(stats *ResourceUsageStats, headroomCPU, headroomMem float64) *RightSizeResult {
    recCPU := stats.P95CPU * (1 + headroomCPU)
    recMem := stats.P99Mem * (1 + headroomMem)
    // ... convert to resource.Quantity
    return &RightSizeResult{
        RecommendedCPUReq: *resource.NewMilliQuantity(int64(recCPU*1000), resource.DecimalSI),
        RecommendedMemReq: *resource.NewQuantity(int64(recMem), resource.BinarySI),
    }
}
```

### Task 13.4 — Savings Calculator

**Create file**: `internal/finops/savings.go`

```go
package finops

// EstimateMonthlySavings calculates the dollar savings from right-sizing.
func EstimateMonthlySavings(
    currentCPU, recommendedCPU float64, // in cores
    currentMem, recommendedMem float64, // in GiB
    cpuPricePerCoreHour float64,  // e.g., $0.034 for on-demand
    memPricePerGiBHour float64,   // e.g., $0.004
) float64 {
    hoursPerMonth := 730.0
    cpuSaved := (currentCPU - recommendedCPU) * cpuPricePerCoreHour * hoursPerMonth
    memSaved := (currentMem - recommendedMem) * memPricePerGiBHour * hoursPerMonth
    total := cpuSaved + memSaved
    if total < 0 {
        return 0 // recommendation increases cost — no savings
    }
    return total
}
```

### Task 13.5 — FinOps Reconciler

Create a separate controller that runs on a longer interval (e.g., daily)
to generate `ResourceRecommendation` objects for all managed deployments.

---

## Phase 14 — Central Dashboard

> **Goal**: Build a web-based dashboard for visualizing scaling activity,
> metrics trends, LLM decision audit trail, and cost analysis.

### Task 14.1 — Dashboard API Server

**Create directory**: `internal/dashboard/`

Build a lightweight HTTP API server that runs as a sidecar or separate
deployment:

**File**: `internal/dashboard/server.go`

```go
package dashboard

import (
    "encoding/json"
    "net/http"

    "sigs.k8s.io/controller-runtime/pkg/client"
)

type Server struct {
    client client.Client
    mux    *http.ServeMux
}

func NewServer(client client.Client) *Server {
    s := &Server{client: client, mux: http.NewServeMux()}
    s.mux.HandleFunc("/api/v1/aiscalers", s.listAIScalers)
    s.mux.HandleFunc("/api/v1/events", s.listScalingEvents)
    s.mux.HandleFunc("/api/v1/metrics", s.getMetrics)
    s.mux.HandleFunc("/api/v1/recommendations", s.listRecommendations)
    return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    s.mux.ServeHTTP(w, r)
}

func (s *Server) listAIScalers(w http.ResponseWriter, r *http.Request) {
    // List all AIScaler resources and return as JSON
    // Include status, last decision, metrics summary
}

func (s *Server) listScalingEvents(w http.ResponseWriter, r *http.Request) {
    // Query ScalingEvent CRDs with pagination and filtering
    // Support ?aiscaler=name&limit=50&since=2024-01-01
}

func (s *Server) getMetrics(w http.ResponseWriter, r *http.Request) {
    // Return time-series data for frontend charts
    // Aggregate from ScalingEvent CRDs or Prometheus
}

func (s *Server) listRecommendations(w http.ResponseWriter, r *http.Request) {
    // List ResourceRecommendation CRDs
}
```

### Task 14.2 — Dashboard Frontend

Use a lightweight framework (React, or static HTML+Chart.js for simplicity):

**Key pages**:
1. **Overview** — List all AIScalers with current phase, replicas, last provider
2. **Detail** — Timeline of scaling events for one AIScaler
3. **Metrics** — CPU/memory/latency charts over time
4. **Audit** — Full log of LLM decisions with reasoning
5. **FinOps** — Cost analysis and right-sizing recommendations
6. **Settings** — Provider configuration, thresholds

### Task 14.3 — Dashboard Deployment

**Create file**: `config/dashboard/deployment.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: aiscaler-dashboard
  namespace: aiscaler-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: aiscaler-dashboard
  template:
    metadata:
      labels:
        app: aiscaler-dashboard
    spec:
      serviceAccountName: aiscaler-dashboard
      containers:
      - name: dashboard
        image: aiscaler-dashboard:latest
        ports:
        - containerPort: 8080
        env:
        - name: KUBERNETES_SERVICE_HOST
          value: "" # auto-detected
```

---

## Implementation Priority & Order

| Priority | Phase | Est. Complexity | Dependencies |
|----------|-------|----------------|--------------|
| P0 | Phase 0 (Bug Fixes) | Low | None |
| P1 | Phase 1 (Extended Signals) | Medium | Phase 0 |
| P1 | Phase 9 (Safety) | Medium | Phase 0 |
| P1 | Phase 10 (Observability) | Medium | Phase 0 |
| P2 | Phase 2 (Scheduling) | Medium | Phase 1 |
| P2 | Phase 5 (SLO) | Medium | Phase 1 |
| P2 | Phase 12 (LLM Reliability) | Medium | Phase 0 |
| P3 | Phase 3 (Vertical) | High | Phase 1 |
| P3 | Phase 4 (Cost) | Medium | Phase 1 |
| P3 | Phase 6 (Predictive) | High | Phase 1, 10 |
| P4 | Phase 7 (Node-Aware) | Medium | Phase 1 |
| P4 | Phase 8 (KEDA) | Medium | Phase 1 |
| P4 | Phase 11 (Multi-Workload) | High | Phase 1, 10 |
| P5 | Phase 13 (FinOps) | High | Phase 4, 10 |
| P5 | Phase 14 (Dashboard) | High | Phase 10, 13 |

---

## Development Workflow

### For Each Task

1. **Read** the task description completely
2. **Create a branch**: `git checkout -b phase-X/task-Y.Z-description`
3. **Write tests first** (TDD): create the test file, write failing tests
4. **Implement**: write the minimum code to pass the tests
5. **Run all tests**: `make test`
6. **Regenerate manifests**: `make manifests` (if CRD/RBAC changed)
7. **Build**: `make docker-build`
8. **Local test**: `make install && make run` (uses envtest)
9. **PR**: open a pull request with a clear description

### Useful Commands

```bash
# Run unit tests
make test

# Run e2e tests (requires a cluster)
make test-e2e

# Regenerate CRD manifests, RBAC, deepcopy
make manifests generate

# Build and push Docker image
make docker-build docker-push IMG=myregistry/aiscaler:latest

# Install CRDs into cluster
make install

# Deploy operator to cluster
make deploy IMG=myregistry/aiscaler:latest

# Run locally against cluster
make run
```

---

## Appendix A — File Index

| File | Purpose |
|------|---------|
| `api/v1/aiscaler_types.go` | CRD type definitions |
| `api/v1/groupversion_info.go` | GVK registration |
| `api/v1/zz_generated.deepcopy.go` | Auto-generated DeepCopy methods |
| `cmd/main.go` | Entrypoint, manager wiring |
| `internal/controller/aiscaler_controller.go` | Main reconciler (7-step chain) |
| `internal/signals/collector.go` | Signal orchestrator |
| `internal/signals/metrics.go` | Deployment health + CPU/memory |
| `internal/signals/prometheus.go` | Prometheus HTTP queries |
| `internal/signals/annotations.go` | Annotation-based human intent |
| `internal/llm/router.go` | LLM provider routing + fallback |
| `internal/llm/prompt.go` | System/user prompt construction |
| `internal/decision/validator.go` | Hard guardrails on LLM decisions |
| `internal/actuator/actuator.go` | SSA-based deployment patching |
| `internal/config/config.go` | YAML config with env var expansion |
| `config/operator.yaml` | Central operator config (providers) |
| `config/rbac/role.yaml` | Generated RBAC ClusterRole |
| `Dockerfile` | Multi-stage build (distroless) |
| `Makefile` | Build, test, deploy targets |

---

## Appendix B — Key Design Decisions

1. **All LLM providers use OpenAI-compatible API**: No provider-specific SDKs.
   Everything goes through `sashabaranov/go-openai` with a custom `BaseURL`.

2. **Server-Side Apply (SSA)** for actuation: The operator only owns the
   `replicas` field. Other controllers (GitOps, etc.) can manage the rest.

3. **Step-chain reconciler pattern**: Each step returns `(*ctrl.Result, error)`.
   A non-nil Result short-circuits. This makes the flow easy to follow and test.

4. **Cluster-scoped CRD**: `AIScaler` is cluster-scoped (not namespaced) so it
   can target deployments in any namespace.

5. **Confidence score**: The LLM returns 0-1 confidence. Below the threshold,
   no action is taken. This provides a safety net for uncertain decisions.

6. **Ring-buffer history**: Simple in-memory store for now. Can be replaced with
   a persistent store (PostgreSQL, ClickHouse) for production.
