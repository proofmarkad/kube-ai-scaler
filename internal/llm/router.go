package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/config"
	aiscalermetrics "github.com/sanjbh/kube-scaling-agent/internal/metrics"
	"github.com/sashabaranov/go-openai"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	QueueDepth        float64
	DeploymentReady   bool
	// Human intent
	ExpectedTraffic     string
	ScaleConservatively bool
	Note                string
	PeakHours           string
	// Custom signals
	CustomSignals map[string]float64
	// SLO context
	SLOContext string
	// Cost context
	CostContext string
	// Predictive context
	PredictiveContext string
	// Node/cluster context
	NodeContext string
	// Vertical scaling context (current resource requests/limits)
	VerticalContext string
	// Feedback context (recent decision outcomes)
	FeedbackContext string
}

// ScalingDecision is the structured response we parse from the LLM.
type ScalingDecision struct {
	TargetReplicas int32   `json:"target_replicas"`
	Reasoning      string  `json:"reasoning"`
	Confidence     float64 `json:"confidence"` // 0-1
	// Extended fields
	ActionType       string            `json:"action_type,omitempty"`       // scale_up, scale_down, no_change, vertical_resize
	Urgency          string            `json:"urgency,omitempty"`           // low, medium, high, critical
	ReasonCodes      []string          `json:"reason_codes,omitempty"`      // structured reason tags
	VerticalChanges  *VerticalProposal `json:"vertical_changes,omitempty"`
}

// VerticalProposal is the LLM's proposal for resource changes.
type VerticalProposal struct {
	CPURequest     string `json:"cpu_request,omitempty"`
	MemoryRequest  string `json:"memory_request,omitempty"`
	CPULimit       string `json:"cpu_limit,omitempty"`
	MemoryLimit    string `json:"memory_limit,omitempty"`
	ResizeStrategy string `json:"resize_strategy,omitempty"`
}

// Router selects the appropriate LLM provider and handles fallback.
type Router struct {
	cfg             *config.Config
	k8sClient       client.Client
	cache           *Cache
	circuitBreakers map[string]*CircuitBreaker
}

// NewRouter creates a Router from the central operator config.
func NewRouter(cfg *config.Config, opts ...RouterOption) *Router {
	r := &Router{
		cfg:             cfg,
		circuitBreakers: make(map[string]*CircuitBreaker),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// RouterOption configures the Router.
type RouterOption func(*Router)

// WithK8sClient sets the Kubernetes client for secret reading.
func WithK8sClient(c client.Client) RouterOption {
	return func(r *Router) { r.k8sClient = c }
}

// WithCache sets the response cache.
func WithCache(cache *Cache) RouterOption {
	return func(r *Router) { r.cache = cache }
}

// WithCircuitBreaker sets up circuit breakers per provider.
func WithCircuitBreaker(threshold int, timeout time.Duration) RouterOption {
	return func(r *Router) {
		for _, p := range r.cfg.LLM.Providers {
			r.circuitBreakers[p.Name] = NewCircuitBreaker(threshold, timeout)
		}
	}
}

// Decide routes the request to the primary provider, falling back
// to the fallback provider if the primary fails.
func (r *Router) Decide(
	ctx context.Context,
	policy *aiscalerv1.AIScaler,
	req ScalingRequest,
) (*ScalingDecision, aiscalerv1.LLMProvider, error) {

	// Check cache
	if r.cache != nil {
		cacheKey := BuildKey(&req)
		if decision, provider, ok := r.cache.Get(cacheKey); ok {
			aiscalermetrics.CacheHits.WithLabelValues("hit").Inc()
			return decision, aiscalerv1.LLMProvider(provider), nil
		}
		aiscalermetrics.CacheHits.WithLabelValues("miss").Inc()
	}

	// Read API key from secret if configured
	secretAPIKey := ""
	if policy.Spec.LLM.APIKeySecret != nil && r.k8sClient != nil {
		key, err := r.readSecret(ctx, policy.Spec.LLM.APIKeySecret)
		if err == nil {
			secretAPIKey = key
		}
	}

	decision, err := r.callProviderWithCB(
		ctx,
		policy.Spec.LLM.Primary,
		effectiveModelOverride(policy.Spec.LLM.Primary, policy.Spec.LLM.Primary, policy.Spec.LLM.Model),
		&req,
		secretAPIKey,
	)
	if err == nil {
		r.cacheResult(&req, decision, string(policy.Spec.LLM.Primary))
		return decision, policy.Spec.LLM.Primary, nil
	}

	// Primary failed — try fallback if configured
	if policy.Spec.LLM.Fallback == nil {
		return nil, "", fmt.Errorf("primary provider %s failed: %w", policy.Spec.LLM.Primary, err)
	}

	decision, ferr := r.callProviderWithCB(
		ctx,
		*policy.Spec.LLM.Fallback,
		effectiveModelOverride(policy.Spec.LLM.Primary, *policy.Spec.LLM.Fallback, policy.Spec.LLM.Model),
		&req,
		secretAPIKey,
	)
	if ferr != nil {
		return nil, "", fmt.Errorf(
			"primary %s failed (%w) and fallback %s also failed: %v",
			policy.Spec.LLM.Primary, err, *policy.Spec.LLM.Fallback, ferr,
		)
	}
	r.cacheResult(&req, decision, string(*policy.Spec.LLM.Fallback))
	return decision, *policy.Spec.LLM.Fallback, nil
}

func (r *Router) callProviderWithCB(
	ctx context.Context,
	provider aiscalerv1.LLMProvider,
	modelOverride string,
	req *ScalingRequest,
	secretAPIKey string,
) (*ScalingDecision, error) {
	cb := r.circuitBreakers[string(provider)]
	if cb != nil && !cb.Allow() {
		r.updateCBMetric(string(provider), cb)
		return nil, fmt.Errorf("circuit breaker open for %s", provider)
	}

	decision, err := r.callProvider(ctx, provider, modelOverride, req, secretAPIKey)
	if err != nil {
		if cb != nil {
			cb.RecordFailure()
			r.updateCBMetric(string(provider), cb)
		}
		return nil, err
	}
	if cb != nil {
		cb.RecordSuccess()
		r.updateCBMetric(string(provider), cb)
	}
	return decision, nil
}

func (r *Router) updateCBMetric(provider string, cb *CircuitBreaker) {
	stateVal := 0.0 // closed
	switch cb.State() {
	case "half-open":
		stateVal = 1.0
	case "open":
		stateVal = 2.0
	}
	aiscalermetrics.CircuitBreakerState.WithLabelValues(provider).Set(stateVal)
}

func (r *Router) cacheResult(req *ScalingRequest, decision *ScalingDecision, provider string) {
	if r.cache != nil {
		r.cache.Set(BuildKey(req), decision, provider)
	}
}

func effectiveModelOverride(primary, provider aiscalerv1.LLMProvider, override string) string {
	if override == "" {
		return ""
	}
	if provider != primary {
		return ""
	}
	return override
}

// callProvider builds an OpenAI-compatible client for the given provider
// and makes a single chat completion call.
func (r *Router) callProvider(
	ctx context.Context,
	provider aiscalerv1.LLMProvider,
	modelOverride string,
	req *ScalingRequest,
	secretAPIKey string,
) (*ScalingDecision, error) {

	oaiClient, model, err := r.buildClient(provider, modelOverride, secretAPIKey)
	if err != nil {
		return nil, err
	}

	res, err := oaiClient.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model: model,
		ResponseFormat: &openai.ChatCompletionResponseFormat{
			Type: openai.ChatCompletionResponseFormatTypeJSONObject,
		},
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

// buildClient constructs an openai.Client pointed at the correct base URL.
func (r *Router) buildClient(
	provider aiscalerv1.LLMProvider,
	modelOverride string,
	secretAPIKey string,
) (*openai.Client, string, error) {

	settings, err := r.cfg.LLMProvider(string(provider))
	if err != nil {
		return nil, "", err
	}

	apiKey := settings.APIKey
	if secretAPIKey != "" {
		apiKey = secretAPIKey
	}
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

// readSecret reads a secret key value from the cluster.
func (r *Router) readSecret(ctx context.Context, ref *aiscalerv1.SecretRef) (string, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}
	if err := r.k8sClient.Get(ctx, key, secret); err != nil {
		return "", fmt.Errorf("failed to read secret %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	val, ok := secret.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %s/%s", ref.Key, ref.Namespace, ref.Name)
	}
	return string(val), nil
}

// parseDecision parses the LLM JSON response into a ScalingDecision.
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

// GetCircuitBreakerState returns the current state of a provider's circuit breaker.
func (r *Router) GetCircuitBreakerState(provider string) string {
	cb, ok := r.circuitBreakers[provider]
	if !ok {
		return "closed"
	}
	return cb.State()
}
