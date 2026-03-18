package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/config"
	"github.com/sashabaranov/go-openai"
)

type ScalingRequest struct {
	PolicyName        string
	Namespace         string
	CurrentReplicas   int32
	MinReplicas       int32
	MaxReplicas       int32
	MaxScaleStep      int32
	CPUUtilization    float64
	MemoryUtilization float64
	P95LatencyMs      float64
	ErrorRate         float64
	DeploymentReady   bool
	// Human intent
	ExpectedTraffic     string
	ScaleConservatively bool
	Note                string
	PeakHours           string
}

// ScalingDecision is the structured response we parse from the LLM.
type ScalingDecision struct {
	TargetReplicas int32   `json:"target_replicas"`
	Reasoning      string  `json:"reasoning"`
	Confidence     float64 `json:"confidence"` // 0-1
}

// Router selects the appropriate LLM provider and handles fallback.
// All providers are called via the OpenAI-compatible API —
// no provider-specific SDKs needed.
type Router struct {
	cfg *config.Config
}

// NewRouter creates a Router from the central operator config.
func NewRouter(cfg *config.Config) *Router {
	return &Router{cfg: cfg}
}

// Decide routes the request to the primary provider, falling back
// to the fallback provider if the primary fails.
// Returns the decision, the provider that served it, and any error.
func (r *Router) Decide(
	ctx context.Context,
	policy *aiscalerv1.AIScaler,
	req ScalingRequest,
) (
	*ScalingDecision,
	aiscalerv1.LLMProvider,
	error) {

	decision, err := r.callProvider(ctx, policy.Spec.LLM.Primary, policy.Spec.LLM.Model, &req)
	if err == nil {
		return decision, policy.Spec.LLM.Primary, nil
	}

	// Primary failed — try fallback if configured
	if policy.Spec.LLM.Fallback == nil {
		return nil, "", fmt.Errorf("primary provider %s failed: %w", policy.Spec.LLM.Primary, err)
	}

	decision, ferr := r.callProvider(ctx, *policy.Spec.LLM.Fallback, policy.Spec.LLM.Model, &req)
	if ferr != nil {
		return nil, "", fmt.Errorf(
			"primary %s failed (%w) and fallback %s also failed: %v",
			policy.Spec.LLM.Primary, err, *policy.Spec.LLM.Fallback, ferr,
		)
	}
	return decision, *policy.Spec.LLM.Fallback, nil
}

// callProvider builds an OpenAI-compatible client for the given provider
// and makes a single chat completion call.
func (r *Router) callProvider(
	ctx context.Context,
	provider aiscalerv1.LLMProvider,
	modelOverride string,
	req *ScalingRequest,
) (*ScalingDecision, error) {

	client, model, err := r.buildClient(provider, modelOverride)
	if err != nil {
		return nil, err
	}

	res, err := client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model: model,
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleSystem,
				Content: systemPrompt,
			},
			{
				Role:    openai.ChatMessageRoleUser,
				Content: buildPrompt(req),
			},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("completion failed for %s: %w", provider, err)
	}

	if len(res.Choices) == 0 {
		return nil, fmt.Errorf("empty response from %s", provider)
	}

	return parseDecision(res.Choices[0].Message.Content)
}

// buildClient constructs an openai.Client pointed at the correct base URL
// for the given provider. This is the only place provider differences live.
func (r *Router) buildClient(
	provider aiscalerv1.LLMProvider,
	modelOverride string,
) (*openai.Client, string, error) {

	settings, err := r.cfg.LLMProvider(string(provider))
	if err != nil {
		return nil, "", err
	}

	apiKey := settings.APIKey
	if apiKey == "" {
		apiKey = "unused"
	}

	model := modelOverride

	if model == "" {
		model = settings.Model
	}

	cfg := openai.DefaultConfig(apiKey)
	cfg.BaseURL = settings.BaseURL
	return openai.NewClientWithConfig(cfg), model, nil
}

// parseDecision parses the LLM JSON response into a ScalingDecision.
// Handles markdown-wrapped JSON defensively since some models wrap
// their output in ```json fences.
func parseDecision(raw string) (*ScalingDecision, error) {
	cleaned := strings.TrimSpace(raw)
	cleaned = strings.TrimPrefix(cleaned, "```json")
	cleaned = strings.TrimSuffix(cleaned, "```")
	cleaned = strings.TrimSpace(cleaned)

	var decision ScalingDecision

	if err := json.Unmarshal([]byte(cleaned), &decision); err != nil {
		return nil, fmt.Errorf("failed to parse LLM response: %w\nraw: %s", err, raw)
	}

	return &decision, nil
}
