# Examples

These manifests are aligned to the controller paths that are live in this repository today. They intentionally avoid forward-looking CRD fields that are not yet enforced end-to-end.

| File | Scenario | Notes |
| --- | --- | --- |
| `01-basic-ollama.yaml` | Baseline LLM-driven scaling with metrics, annotations, and Prometheus | Best starting point for local or lab clusters |
| `02-reactive-slo-annotations.yaml` | Annotation-driven reactive rules with SLO protection and precedence | Shows the exact workload annotations the controller evaluates |
| `03-cost-approval-rollback.yaml` | Cost ceiling, approval holds, alert rules, and auto-rollback | Requires OpenCost access for cost delta enforcement |
| `04-vertical-feedback.yaml` | Horizontal plus vertical recommendations with feedback summaries | Vertical changes patch Deployment pod resources |
| `05-coordination-multi-workload.yaml` | Dependency-aware coordination across multiple workloads | Exercises the live dependency deferral path |
| `06-keda-advisory.yaml` | KEDA desired replicas as a deterministic advisory input | Uses `signals[].config.scaledObjectName` as the active lookup key |
| `07-external-signals-aws-datadog.yaml` | Queue-driven scaling with AWS SQS and Datadog | Shows `secretRef` usage for external signal plugins |
| `08-external-signals-gcp.yaml` | Queue and latency signals from GCP Monitoring / PubSub | Assumes GKE Workload Identity or other ADC wiring |

Usage:

```bash
kubectl apply -f examples/01-basic-ollama.yaml
kubectl get aiscaler -A
```

Operational notes:

- Replace deployment names, PromQL, queue identifiers, and secret names before applying to a live cluster.
- Cost delta estimates, budget ceilings, and cluster budget coordination require the operator to start with `OPENCOST_ENDPOINT` set.
- Approval, rollback, and budget notifications currently route through the operator process. Set `SLACK_WEBHOOK_URL` if you want those events delivered.
- Alert rules are defined per `AIScaler`, but alert delivery uses `operator.alertWebhookURL` and `operator.alertWebhookToken` from `config/operator.yaml`.
- The predictor and feedback loop learn from observed runtime data. There is no per-policy historical bootstrap config required today.

Reactive rule annotation example:

```bash
kubectl annotate deployment payments-api \
  aiscaler.io/reactive-rules='[{"metric":"queue_depth","operator":">","threshold":75,"action":"scale_up","amount":3}]' \
  --overwrite
```