# kube-scaling-agent

A Kubernetes operator that combines runtime metrics, human intent, and LLM reasoning to make autoscaling decisions with explicit guardrails.

The repository is a single-cluster operator built with Go, kubebuilder, and controller-runtime. It uses OpenAI-compatible APIs for Anthropic, Gemini, Ollama, and DeepSeek, and it layers those LLM recommendations with reactive rules, SLO protection, cost ceilings, approval holds, rollback checks, and coordination gates.

Scope note: the broader architecture documents in `plan.md` and `tech_plan.md` remain roadmap documents. This README describes the code that is live in this repository today.

## Current capabilities

- Multi-signal decisioning from metrics-server, Prometheus, annotations, queue signals, and custom plugin inputs.
- Provider routing with primary and fallback LLMs.
- Precedence resolution across reactive rules, LLM output, KEDA advisory input, SLO protection, approval holds, rollback safety decisions, and cost ceilings.
- Asymmetric validation for scale-up vs scale-down confidence and step limits.
- Process-local coordination gates for dependency deferral, max concurrent live scaling, and optional cluster cost ceilings.
- Audit history, feedback summaries, alert rule evaluation, and vertical resource recommendations.

## Reconcile flow

The live step chain is:

```text
ensureFinalizer
  -> checkCooldown
  -> collectSignals
  -> enrichContext
  -> fetchDecision
  -> checkOscillation
  -> checkRollback
  -> resolveTentativeDecision
  -> estimateCost
  -> checkApproval
  -> resolveFinalDecision
  -> validateDecision
  -> checkCoordination
  -> actuate
  -> evaluateAlerts
  -> recordAudit
  -> updateStatus
```

The important behavior changes versus the earlier simpler controller are:

- `collectSignals` and `enrichContext` build a richer prompt context, including SLO state, cost context, node context, vertical state, and feedback summaries.
- `fetchDecision` can produce multiple candidate decisions in one reconcile: reactive rule output, LLM output, and deterministic KEDA advice.
- `checkRollback`, `checkApproval`, and `resolveFinalDecision` can override or hold the original recommendation before actuation.
- `checkCoordination` can defer a live change when a dependency is already scaling, the controller has exhausted its concurrency budget, or a cluster budget ceiling would be crossed.
- `recordAudit` persists the pre- and post-decision context used by rollback and feedback evaluation.

## Decision precedence

The controller no longer treats the LLM output as the only decision source. The final desired replica count is resolved from multiple layers.

Ordered roughly from strongest override to weakest:

1. Safety override, including auto-rollback.
2. Human approval hold, when approval policy blocks the proposed change.
3. SLO protection, when current objectives are breached.
4. Reactive annotation rules, when configured to outrank the LLM.
5. LLM recommendation.
6. Deterministic advisory input, currently KEDA desired replicas.
7. Cost ceiling clamp.

Per-policy precedence flags live under `spec.precedence`.

## Signals and plugins

Signal sources are configured through `spec.signals[]`. If you omit the list entirely, the operator defaults to `metrics-server` and `annotations`.

Built-in examples in this repo exercise these plugins:

- `metrics-server`
- `annotations`
- `prometheus`
- `keda`
- `opencost`
- `aws-sqs`
- `datadog`
- `gcp-pubsub`
- `gcp-monitoring`

Additional registered plugins in the codebase include `nodes`, `cloudwatch`, `webhook`, and `kafka`.

## Minimal policy

The minimal working path is a Deployment target, constraints, an LLM provider, and either explicit `signals[]` or the default signal set.

```yaml
apiVersion: aiscaler.io/v1
kind: AIScaler
metadata:
  name: web-basic-scaler
spec:
  targetRef:
    name: web
    namespace: default
    kind: Deployment
    apiVersion: apps/v1

  constraints:
    minReplicas: 2
    maxReplicas: 12
    maxScaleStep: 3
    minConfidence: 0.65

  llm:
    primary: ollama
    model: qwen3:8b-q4_K_M

  signals:
    - name: metrics-server
      required: true
    - name: annotations

  evaluationInterval: 60s
  cooldownPeriod: 300s
```

The shipped baseline sample is `config/samples/aiscaler_v1_aiscaler.yaml`. Broader scenario manifests live under `examples/`.

## Human intent and reactive annotations

The annotations plugin reads workload annotations from the target Deployment. These become prompt context or direct reactive rules.

Supported human-intent annotations:

- `aiscaler.io/expected-traffic`
- `aiscaler.io/scale-conservatively`
- `aiscaler.io/freeze-until`
- `aiscaler.io/peak-hours`
- `aiscaler.io/note`
- `aiscaler.io/reactive-rules`

Reactive rules are JSON and are evaluated before the LLM call. First match wins.

```bash
kubectl annotate deployment payments-api \
  aiscaler.io/reactive-rules='[{"metric":"queue_depth","operator":">","threshold":75,"action":"scale_up","amount":3}]' \
  --overwrite
```

SLO metric names currently recognized by the controller are:

- `p95_latency`
- `p99_latency`
- `error_rate`
- `cpu_utilization`
- `memory_utilization`
- `queue_depth`
- any key emitted into `bundle.CustomSignals`

Alert rule conditions support `error_rate`, `p95_latency`, `cpu`, `memory`, `queue_depth`, and custom signal names.

## Local setup

Prerequisites:

- Go 1.25+
- a Kubernetes cluster reachable through `kubectl`
- metrics-server installed in the cluster
- optional Prometheus, OpenCost, KEDA, or external signal backends depending on the example you want to run

Run locally:

```bash
make install

# only needed for cloud LLM providers
export ANTHROPIC_API_KEY=...
export GEMINI_API_KEY=...
export DEEPSEEK_API_KEY=...

make run
```

Apply the baseline sample:

```bash
kubectl apply -f config/samples/aiscaler_v1_aiscaler.yaml
kubectl get aiscaler -A
```

Or use one of the richer scenarios:

```bash
kubectl apply -f examples/01-basic-ollama.yaml
kubectl apply -f examples/03-cost-approval-rollback.yaml
```

`make dev` still installs CRDs and runs the operator in one command.

## In-cluster deployment

```bash
# LLM provider keys if you are not using Ollama only
kubectl create secret generic operator-api-keys \
  --namespace=system \
  --from-literal=ANTHROPIC_API_KEY=... \
  --from-literal=GEMINI_API_KEY=... \
  --from-literal=DEEPSEEK_API_KEY=...

# Optional runtime integrations
export SLACK_WEBHOOK_URL=...
export OPENCOST_ENDPOINT=http://opencost.opencost.svc.cluster.local:9003

make docker-build docker-push IMG=<your-registry>/kube-scaling-agent:v0.1.0
make deploy IMG=<your-registry>/kube-scaling-agent:v0.1.0
```

## Operator configuration

`config/operator.yaml` is loaded before manager creation, so manager-level settings in that file now affect the running controller.

The default operator block is:

```yaml
operator:
  leaderElection: true
  metricsBindAddress: ":8080"
  healthProbeBindAddress: ":8081"
  alertWebhookURL: ""
  alertWebhookToken: ""
  coordinationMaxConcurrentScaling: 2
  coordinationRequeueSeconds: 15
  clusterMaxHourlyCost: 0
```

Production guidance:

1. Keep `leaderElection: true` when running more than one manager pod.
2. Set a real non-zero `clusterMaxHourlyCost` before relying on cluster budget gating.
3. Set `alertWebhookURL` and `alertWebhookToken` if you want `spec.alerting.rules` to notify an external system.
4. Set `SLACK_WEBHOOK_URL` if you want approval, rollback, or budget events delivered by the controller process.
5. Set `OPENCOST_ENDPOINT` if you want cost delta estimation, workload budget checks, or cluster budget coordination.

LLM provider configuration stays centralized in `config/operator.yaml`. If you set `spec.llm.model`, that override applies to the primary provider. When a different fallback provider is used, the controller now falls back to that provider's configured default model.

## Examples

The repository now includes a dedicated `examples/` folder covering:

- baseline Ollama operation
- reactive annotations plus SLO protection
- cost ceilings, approval holds, and rollback
- vertical recommendations plus feedback summaries
- multi-workload coordination
- KEDA advisory input
- AWS plus Datadog external signals
- GCP Pub/Sub plus Cloud Monitoring signals

See `examples/README.md` for scenario notes and prerequisites.

## Limitations and current caveats

- Treat the current end-to-end operator as Deployment-focused. Some CRD fields and helper code point toward broader workload support, but the live signal collection path still assumes Deployment-backed workloads.
- Vertical scaling patches Deployment pod resources only.
- Alert delivery currently uses operator-level webhook settings from `config/operator.yaml`; per-policy webhook fields in the CRD are not the active delivery path yet.
- Approval policy evaluation is live, but notification delivery is currently process-level and environment-driven rather than sourced from `spec.approval.channels`.
- Predictive context is learned automatically from observed CPU and queue history. Some forward-looking CRD fields, including parts of the predictive and scheduling surface, are not yet active decision inputs.
- Prometheus and custom signal queries are user-supplied. They are not schema-validated beyond runtime execution.

## Project structure

```text
.
├── api/v1/                  # AIScaler CRD types
├── cmd/main.go              # Manager setup and dependency wiring
├── config/
│   ├── crd/                 # Generated CRD manifests
│   ├── manager/             # Deployment and config templates
│   ├── rbac/                # RBAC manifests
│   ├── samples/             # Baseline sample manifest
│   └── operator.yaml        # Central operator configuration
├── examples/                # Scenario manifests aligned to the live controller
└── internal/
    ├── actuator/            # Horizontal and vertical apply paths
    ├── alerting/            # Alert rule evaluation and webhook delivery
    ├── approval/            # Approval policy evaluation
    ├── audit/               # Decision audit persistence
    ├── config/              # Config loader
    ├── controller/          # Step-chain reconciler
    ├── coordinator/         # Dependency and concurrency gating
    ├── cost/                # Cost estimation and budget checks
    ├── decision/            # Validation, precedence, rollback, SLOs
    ├── feedback/            # Post-decision outcome evaluation
    ├── llm/                 # Router, prompt builder, cache, circuit breakers
    ├── metrics/             # Operator metrics exporter
    ├── node/                # Node feasibility and cluster context
    ├── notification/        # Slack and PagerDuty notifiers
    ├── plugin/              # Signal plugin registry and manager
    └── prediction/          # Seasonal baseline predictor
```

## Make targets

| Target | Description |
| --- | --- |
| `make run` | Run the operator locally against the current kubeconfig |
| `make dev` | Install CRDs and run locally |
| `make install` | Install CRDs into the current cluster |
| `make deploy` | Deploy the controller in-cluster via kustomize |
| `make test` | Run the Go test suite |
| `make build` | Build the manager binary |
| `make docker-build` | Build the controller image |
| `make sample` | Apply the baseline sample manifest |
