package llm

import "fmt"

// systemPrompt instructs the LLM on its role and expected output format.
const systemPrompt = `You are a Kubernetes autoscaling advisor.
You analyze deployment metrics and decide how many replicas a service should have.
You must respond with a JSON object only — no markdown, no explanation outside the JSON.

Response format:
{
  "target_replicas": <integer>,
  "reasoning": "<brief explanation>",
  "confidence": <float 0-1>
}`

// buildPrompt constructs the user prompt from a ScalingRequest.
func buildPrompt(req *ScalingRequest) string {
	userPromptTemplate := `
Deployment: %s (namespace: %s)
Current replicas: %d
Constraints: min=%d, max=%d, max_step=%d
Deployment healthy: %v

Metrics:
- CPU utilization: %.1f%%
- Memory utilization: %.1f%%
- p95 latency: %.1fms
- Error rate: %.2f%%
`

	userPrompt := fmt.Sprintf(userPromptTemplate,
		req.PolicyName,
		req.Namespace,
		req.CurrentReplicas,
		req.MinReplicas,
		req.MaxReplicas,
		req.MaxScaleStep,
		req.DeploymentReady,
		req.CPUUtilization,
		req.MemoryUtilization,
		req.P95LatencyMs,
		req.ErrorRate*100)

	if req.ExpectedTraffic != "" {
		userPrompt += fmt.Sprintf("\nExpected traffic: %s", req.ExpectedTraffic)
	}

	if req.ScaleConservatively {
		userPrompt += "\nInstruction: Scale conservatively, prefer fewer replicas."
	}

	if req.PeakHours != "" {
		userPrompt += fmt.Sprintf("\nPeak hours: %s", req.PeakHours)
	}

	if req.Note != "" {
		userPrompt += fmt.Sprintf("\nOperator note: %s", req.Note)
	}

	userPrompt += "\n\nWhat should the replica count be?"

	return userPrompt

}
