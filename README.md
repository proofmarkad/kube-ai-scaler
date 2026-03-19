# kube-scaling-agent

A Kubernetes operator that uses LLM reasoning to make autoscaling decisions. Instead of threshold-based rules, it feeds real-time signals — CPU, memory, p95 latency, error rate, and human intent via annotations — to an LLM and applies its replica recommendation with hard safety guardrails.

Built in Go using kubebuilder. Works with any LLM provider that exposes an OpenAI-compatible API — Anthropic, Gemini, DeepSeek, and Ollama supported out of the box.

---

## Why this exists

HPA scales on a single metric against a fixed threshold. That works until you need to reason about multiple signals simultaneously — high CPU but low latency probably doesn't need scaling; low CPU but rising error rate probably does. An LLM can reason across that signal space and explain its decision in plain English. This operator explores what that looks like in production-grade Go.

---

## Demo

Scaling triggered by human intent annotation on a live k0s cluster, with Ollama running locally as the LLM provider:

```bash
kubectl annotate deployment nginx-test \
  aiscaler.io/expected-traffic=critical \
  aiscaler.io/note="Black Friday load test in progress, scale up aggressively"
```

The operator's next reconcile cycle scaled from 1 → 3 replicas. The LLM's reasoning is stored directly in the `AIScaler` status:

```
Critical traffic spike requires aggressive scaling. Current replicas (1) are
below max (5) and max_step (2) allows immediate increase to 3. Metrics show
low utilization but expected load justifies proactive scaling.
```

```bash
kubectl get ais
NAME                PHASE     CURRENT   DESIRED   PROVIDER   AGE
nginx-test-scaler   Scaling   1         3         ollama     65s
```

No threshold rules. No manual intervention. The decision is auditable, in plain English, stored in the cluster.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Reconcile cycle                          │
│                                                              │
│  ensureFinalizer → checkCooldown → collectSignals            │
│       → fetchDecision → validateDecision → actuate           │
│       → updateStatus                                         │
└─────────────────────────────────────────────────────────────┘
```

### Step chain reconciler

The controller uses a step chain pattern — a slice of named `StepFunc` executed in order, each returning `(*ctrl.Result, error)`. A non-nil result short-circuits the chain. This keeps each concern isolated and independently testable without the sprawling if-else trees common in monolithic reconcilers.

```go
type StepFunc struct {
    Name string
    Run  func(ctx context.Context, state *reconcileState) (*ctrl.Result, error)
}
```

State is carried between steps via `reconcileState` — a plain struct holding the `AIScaler` object, the collected signal bundle, the LLM decision, and the validation result. No globals, no channels.

Each step is logged on entry, completion, and short-circuit — making the chain visible at runtime:

```
step started         step=ensureFinalizer
step completed       step=ensureFinalizer
step started         step=checkCooldown
step short-circuited step=checkCooldown
in cooldown          remaining=1m39s
```

### Signal collection

Three collectors run per reconcile cycle:

- **metricsCollector** — fetches `Deployment` health, replica counts, and CPU/memory utilization via the `metrics.k8s.io/v1beta1` API (metrics-server). Uses a typed `metricsclient` directly — not registered in the controller-runtime cache scheme — because metrics-server does not support the watch verb. Utilization is computed as a percentage of resource requests, not limits.
- **prometheusCollector** — queries p95 latency and error rate via the Prometheus HTTP API using PromQL defined in the `AIScaler` spec. Non-fatal if Prometheus is unreachable.
- **annotationCollector** — reads human intent from the target Deployment's annotations. Operators can signal expected traffic levels, freeze scaling windows, or add freeform notes that go directly into the LLM prompt.

All signals are collected into a `Bundle` and passed to the LLM router.

### LLM router

A single `Router` handles all providers via `sashabaranov/go-openai`. Every provider — Anthropic, Gemini, DeepSeek, Ollama — is accessed through an OpenAI-compatible base URL. No provider-specific SDKs.

```go
cfg := openai.DefaultConfig(apiKey)
cfg.BaseURL = settings.BaseURL  // provider-specific URL, uniform client
```

The router tries the primary provider first, falls back to the configured fallback on failure. The LLM is prompted with structured signal data and constrained to respond with a JSON object:

```json
{
  "target_replicas": 4,
  "reasoning": "CPU at 78% with rising p95 latency suggests scaling up one step",
  "confidence": 0.85
}
```

### Validator

Every LLM decision passes through a validator before the actuator sees it. Two guardrails, applied in order:

1. **MaxScaleStep** — caps how far replicas can move in a single cycle. Prevents the LLM from making dramatic jumps.
2. **Min/max bounds** — clamps to the hard limits in the spec.

The validator never trusts the LLM blindly. If the decision is clamped, the reason is recorded in the `AIScaler` status for audit.

### Actuator

Uses Server-Side Apply with `FieldOwner("aiscaler")` — the operator owns only the `replicas` field. Other controllers (HPA, Argo Rollouts, etc.) can manage the rest of the Deployment freely without conflict.

```go
client.Patch(ctx, deployment, client.Apply, client.FieldOwner("aiscaler"), client.ForceOwnership)
```

Supports dry-run mode via `spec.dryRun: true` — the decision is computed and logged but never applied. Useful for validating LLM behaviour before committing to live scaling.

### Spec validation

Cross-field constraints (`minReplicas <= maxReplicas`, `maxScaleStep <= maxReplicas`, `primary != fallback`) are enforced in `ValidateSpec()` at the start of each reconcile cycle. Invalid specs set the `Ready` condition to `False` with a descriptive reason and stop requeuing until the spec is corrected — no webhook dependency required.

---

## CRD

```yaml
apiVersion: aiscaler.io/v1
kind: AIScaler
metadata:
  name: my-service-scaler
spec:
  targetRef:
    name: my-service
    namespace: production

  constraints:
    minReplicas: 2
    maxReplicas: 20
    maxScaleStep: 3

  llm:
    primary: anthropic
    fallback: ollama
    model: claude-sonnet-4-5         # optional override
    apiKeySecret:
      name: anthropic-key
      namespace: production
      key: api-key

  prometheus:
    baseURL: "http://prometheus:9090"
    p95LatencyQuery: 'histogram_quantile(0.95, rate(http_request_duration_seconds_bucket{job="my-service"}[5m]))'
    errorRateQuery: 'rate(http_requests_total{job="my-service",status=~"5.."}[5m])'

  evaluationInterval: 60s
  cooldownPeriod: 300s
  dryRun: false
```

### Human intent via annotations

Operators can influence scaling decisions without touching the spec:

```yaml
# on the target Deployment
annotations:
  aiscaler.io/expected-traffic: "high"           # low | normal | high | critical
  aiscaler.io/scale-conservatively: "true"
  aiscaler.io/freeze-until: "2026-03-20T06:00:00Z"
  aiscaler.io/peak-hours: "09:00-18:00 IST"
  aiscaler.io/note: "Release going out at 14:00, keep headroom"
```

These feed directly into the LLM prompt as natural language context. The LLM can act on human intent even when raw metrics look healthy — as demonstrated in the demo above.

### Status

```bash
kubectl get ais
NAME                PHASE     CURRENT   DESIRED   PROVIDER   AGE
nginx-test-scaler   Scaling   1         3         ollama     65s
```

```yaml
status:
  phase: Scaling
  currentReplicas: 1
  desiredReplicas: 3
  lastProvider: ollama
  lastScaleTime: "2026-03-19T13:24:41Z"
  lastDecisionReason: >
    Critical traffic spike requires aggressive scaling. Current replicas (1)
    are below max (5) and max_step (2) allows immediate increase to 3.
    Metrics show low utilization but expected load justifies proactive scaling.
  conditions:
    - type: Ready
      status: "True"
    - type: SignalsReady
      status: "True"
    - type: LLMReady
      status: "True"
```

---

## Local setup

### Prerequisites

- Go 1.24+
- kubectl configured against a cluster (k0s, kind, minikube)
- metrics-server installed in the cluster
- API keys for your chosen LLM providers (Ollama needs none)

### Run

```bash
# Install CRDs
make install

# Export API keys (only needed for cloud providers)
export ANTHROPIC_API_KEY=sk-ant-...
export GEMINI_API_KEY=...
export DEEPSEEK_API_KEY=...

# Run the operator locally (out-of-cluster)
make run
```

### Apply a sample

```bash
kubectl apply -f config/samples/aiscaler_v1_aiscaler.yaml
kubectl get ais -w
```

### Or use the dev shortcut

```bash
make dev   # installs CRDs + runs the operator in one command
```

---

## In-cluster deployment

```bash
# Create API key secret (never commit real keys)
kubectl create secret generic operator-api-keys \
  --namespace=system \
  --from-literal=ANTHROPIC_API_KEY=sk-ant-... \
  --from-literal=GEMINI_API_KEY=... \
  --from-literal=DEEPSEEK_API_KEY=...

# Build and push image
make docker-build docker-push IMG=<your-registry>/kube-scaling-agent:v0.1.0

# Deploy
make deploy IMG=<your-registry>/kube-scaling-agent:v0.1.0
```

---

## Configuration

`config/operator.yaml` is the central config file, mounted as a ConfigMap in-cluster. Environment variables in `api_key` fields are expanded at load time via `os.ExpandEnv`.

```yaml
llm:
  providers:
    - name: anthropic
      base_url: "https://api.anthropic.com/v1"
      api_key: "${ANTHROPIC_API_KEY}"
      model: "claude-sonnet-4-5"
    - name: gemini
      base_url: "https://generativelanguage.googleapis.com/v1beta/openai"
      api_key: "${GEMINI_API_KEY}"
      model: "gemini-2.0-flash"
    - name: ollama
      base_url: "http://localhost:11434/v1"
      model: "qwen2.5-coder:7b"      # no key needed for local Ollama
    - name: deepseek
      base_url: "https://api.deepseek.com/v1"
      api_key: "${DEEPSEEK_API_KEY}"
      model: "deepseek-chat"

prometheus:
  base_url: "http://prometheus:9090"

operator:
  leaderElection: false
  metricsBindAddress: ":8080"
  healthProbeBindAddress: ":8081"
```

---

## Key design decisions

**OpenAI-compatible API for all providers** — `sashabaranov/go-openai` pointed at different base URLs. Adding a new provider is a config entry, not a code change.

**SSA over strategic merge patch** — the operator owns only the `replicas` field. Coexists safely with other controllers managing the same Deployment.

**Typed metrics client, not scheme registration** — `metrics.k8s.io` does not support the watch verb. Registering `PodMetrics` in the controller-runtime scheme causes a failed watch on startup. Using `metricsclient.NewForConfig` directly bypasses the cache entirely, which is correct — pod metrics are point-in-time readings and should never be cached.

**`ValidateSpec()` for cross-field validation** — constraints like `minReplicas <= maxReplicas` are enforced at the start of each reconcile cycle, setting the `Ready` condition to `False` with a descriptive reason. No webhook dependency required.

**Non-fatal signal collection** — Prometheus failures don't abort the reconcile. The LLM reasons with whatever signals are available and the prompt makes gaps explicit.

**Step chain over monolithic reconciler** — each step is a named, isolated function. The chain is easy to extend, each step is independently testable, and the named logging makes runtime behaviour transparent.

---

## Limitations

This is a portfolio project — it is not production-ready as-is.

- Targets `Deployment` only — `StatefulSet`, `DaemonSet`, and custom workloads are not supported
- One `AIScaler` per Deployment — no fan-out or multi-target support
- LLM cold-start latency — Ollama model load time adds several seconds to the first reconcile after startup
- No multi-tenancy RBAC — the operator has cluster-wide patch access on all Deployments
- Prometheus queries are user-supplied PromQL with no validation — a malformed query silently returns zero

---

## Tested with

- k0s v1.35.1 (single-node)
- metrics-server v0.7.x
- Ollama with `qwen2.5-coder:7b` (local, no API key)
- Anthropic `claude-sonnet-4-5` (cloud)

---

## Project structure

```
.
├── api/v1/                  # AIScaler CRD types, deepcopy, scheme
├── cmd/main.go              # Manager wiring
├── config/
│   ├── crd/                 # Generated CRD manifests
│   ├── manager/             # Deployment, ConfigMap, Secret template
│   ├── rbac/                # ClusterRole, bindings
│   ├── samples/             # Example AIScaler resource
│   └── operator.yaml        # Central operator config
└── internal/
    ├── actuator/            # SSA patch applier
    ├── config/              # Config loader with env var expansion
    ├── controller/          # Step chain reconciler
    ├── decision/            # Validator with MaxScaleStep + bounds
    ├── llm/                 # Router, prompt builder, response parser
    └── signals/             # Collector, metrics, prometheus, annotations
```

---

## Makefile targets

| Target | Description |
|--------|-------------|
| `make run` | Run operator locally against current kubeconfig |
| `make dev` | Install CRDs + run locally in one step |
| `make install` | Install CRDs into cluster |
| `make deploy` | Full in-cluster deploy via kustomize |
| `make manifests` | Regenerate CRD + RBAC from markers |
| `make generate` | Regenerate deepcopy methods |
| `make test` | Run unit tests with envtest |
| `make lint` | Run golangci-lint |
| `make sample` | Apply sample AIScaler resource |
| `make build` | Compile manager binary |
| `make docker-build` | Build container image |
