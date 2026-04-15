package llm

import (
	"fmt"
	"sort"
	"strings"
)

// systemPrompt instructs the LLM on its role and expected output format.
const systemPrompt = `You are a Kubernetes autoscaling advisor.
You analyze deployment metrics, SLO status, cost data, predictions, and cluster state to decide how many replicas a service should have and whether resource requests/limits should change.
You must respond with a JSON object only — no markdown, no explanation outside the JSON.

Response format:
{
  "target_replicas": <integer>,
  "reasoning": "<brief explanation>",
  "confidence": <float 0-1>,
  "action_type": "<scale_up|scale_down|no_change|vertical_resize>",
  "urgency": "<low|medium|high|critical>",
  "reason_codes": ["<tag1>", "<tag2>"],
  "vertical_changes": {
    "cpu_request": "<e.g. 500m>",
    "memory_request": "<e.g. 256Mi>",
    "cpu_limit": "<e.g. 1000m>",
    "memory_limit": "<e.g. 512Mi>",
    "resize_strategy": "<InPlace|Recreate>"
  }
}

Notes:
- vertical_changes is optional; include it only when you recommend resource changes.
- reason_codes are structured tags like: high_cpu, high_latency, slo_breach, cost_saving, predictive_scale, queue_backlog, low_utilization.
- urgency reflects how quickly the change should be applied.`

// buildPrompt constructs the user prompt from a ScalingRequest.
func buildPrompt(req *ScalingRequest) string {
	var b strings.Builder

	fmt.Fprintf(&b, "Deployment: %s (namespace: %s)\n", req.PolicyName, req.Namespace)
	fmt.Fprintf(&b, "Current replicas: %d\n", req.CurrentReplicas)
	fmt.Fprintf(&b, "Constraints: min=%d, max=%d, max_step=%d\n",
		req.MinReplicas, req.MaxReplicas, req.MaxScaleStep)
	fmt.Fprintf(&b, "Deployment healthy: %v\n", req.DeploymentReady)

	b.WriteString("\nMetrics:\n")
	fmt.Fprintf(&b, "- CPU utilization: %.1f%%\n", req.CPUUtilization)
	fmt.Fprintf(&b, "- Memory utilization: %.1f%%\n", req.MemoryUtilization)
	fmt.Fprintf(&b, "- p95 latency: %.1fms\n", req.P95LatencyMs)
	fmt.Fprintf(&b, "- Error rate: %.2f%%\n", req.ErrorRate*100)

	if req.QueueDepth > 0 {
		fmt.Fprintf(&b, "- Queue depth: %.0f\n", req.QueueDepth)
	}

	if len(req.CustomSignals) > 0 {
		b.WriteString("\nCustom Signals:\n")
		// Sort keys for deterministic output
		keys := make([]string, 0, len(req.CustomSignals))
		for k := range req.CustomSignals {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "- %s: %.2f\n", k, req.CustomSignals[k])
		}
	}

	if req.ExpectedTraffic != "" {
		fmt.Fprintf(&b, "\nExpected traffic: %s", req.ExpectedTraffic)
	}
	if req.ScaleConservatively {
		b.WriteString("\nInstruction: Scale conservatively, prefer fewer replicas.")
	}
	if req.PeakHours != "" {
		fmt.Fprintf(&b, "\nPeak hours: %s", req.PeakHours)
	}
	if req.Note != "" {
		fmt.Fprintf(&b, "\nOperator note: %s", req.Note)
	}
	if req.SLOContext != "" {
		fmt.Fprintf(&b, "\n\n%s", req.SLOContext)
	}
	if req.CostContext != "" {
		fmt.Fprintf(&b, "\n\n%s", req.CostContext)
	}
	if req.PredictiveContext != "" {
		fmt.Fprintf(&b, "\n\n%s", req.PredictiveContext)
	}
	if req.NodeContext != "" {
		fmt.Fprintf(&b, "\n\n%s", req.NodeContext)
	}
	if req.VerticalContext != "" {
		fmt.Fprintf(&b, "\n\n%s", req.VerticalContext)
	}
	if req.FeedbackContext != "" {
		fmt.Fprintf(&b, "\n\n%s", req.FeedbackContext)
	}

	b.WriteString("\n\nWhat should the replica count be?")

	return b.String()
}
