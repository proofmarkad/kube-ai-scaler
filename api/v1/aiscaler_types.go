/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum=anthropic;gemini;ollama;deepseek
type LLMProvider string

const (
	ProviderAnthropic LLMProvider = "anthropic"
	ProviderGemini    LLMProvider = "gemini"
	ProviderOllama    LLMProvider = "ollama"
	ProviderDeepseek  LLMProvider = "deepseek"
)

type ScalingPhase string

const (
	PhaseInitializing ScalingPhase = "Initializing"
	PhaseObserving    ScalingPhase = "Observing"
	PhaseScaling      ScalingPhase = "Scaling"
	PhaseCoolingDown  ScalingPhase = "CoolingDown"
	PhaseFailed       ScalingPhase = "Failed"
)

const (
	ConditionReady        = "Ready"
	ConditionSignalsReady = "SignalsReady"
	ConditionLLMReady     = "LLMReady"
	ConditionScaling      = "Scaling"
)

type TargetRef struct {
	// +kubebuilder:validation:Required
	Name string `json:"name"`
	// +kubebuilder:validation:Required
	Namespace string `json:"namespace"`
	// +kubebuilder:default="apps/v1"
	// +optional
	APIVersion string `json:"apiVersion,omitempty"`
	// +kubebuilder:default="Deployment"
	// +kubebuilder:validation:Enum=Deployment;StatefulSet;Rollout
	// +optional
	Kind string `json:"kind,omitempty"`
}

type ScalingConstraints struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	MinReplicas int32 `json:"minReplicas"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=10
	MaxReplicas int32 `json:"maxReplicas"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	MaxScaleStep int32 `json:"maxScaleStep"`
	// Minimum confidence score from LLM to apply scaling (0-1)
	// +kubebuilder:default=0.5
	// +optional
	MinConfidence float64 `json:"minConfidence,omitempty"`
}

type SecretRef struct {
	// +kubebuilder:validation:Required
	Name string `json:"name"`
	// +kubebuilder:validation:Required
	Namespace string `json:"namespace"`
	// +kubebuilder:validation:Required
	Key string `json:"key"`
}

// SignalSourceConfig configures a single signal plugin.
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

type LLMConfig struct {
	// +kubebuilder:validation:Required
	Primary LLMProvider `json:"primary"`
	// +optional
	Fallback *LLMProvider `json:"fallback,omitempty"`
	// +optional
	Model string `json:"model,omitempty"`
	// +optional
	APIKeySecret *SecretRef `json:"apiKeySecret,omitempty"`
	// +kubebuilder:default="http://localhost:11434"
	// +optional
	OllamaBaseURL string `json:"ollamaBaseURL,omitempty"`
	// Response caching TTL. Set to 0 to disable.
	// +kubebuilder:default="30s"
	// +optional
	CacheTTL metav1.Duration `json:"cacheTTL,omitempty"`
	// Circuit breaker: consecutive failures before opening
	// +kubebuilder:default=3
	// +optional
	CircuitBreakerThreshold int32 `json:"circuitBreakerThreshold,omitempty"`
	// Circuit breaker reset timeout
	// +kubebuilder:default="60s"
	// +optional
	CircuitBreakerTimeout metav1.Duration `json:"circuitBreakerTimeout,omitempty"`
}

// AlertingConfig configures in-operator alerting.
type AlertingConfig struct {
	// +kubebuilder:default=true
	Enabled bool `json:"enabled"`
	// Custom alert rules
	// +optional
	Rules []AlertRule `json:"rules,omitempty"`
	// Webhook URL for alert notifications
	// +optional
	WebhookURL string `json:"webhookURL,omitempty"`
	// Secret containing webhook auth token
	// +optional
	WebhookSecret *SecretRef `json:"webhookSecret,omitempty"`
}

// AlertRule defines a single alert condition.
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
	// +optional
	For metav1.Duration `json:"for,omitempty"`
}

// SLO defines a Service Level Objective.
type SLO struct {
	Name     string  `json:"name"`
	Metric   string  `json:"metric"`
	Target   float64 `json:"target"`
	Priority int32   `json:"priority,omitempty"`
}

// CostConfig configures cost optimization.
type CostConfig struct {
	MonthlyBudget   float64 `json:"monthlyBudget,omitempty"`
	OpenCostURL     string  `json:"openCostURL,omitempty"`
	OptimizeForCost bool    `json:"optimizeForCost,omitempty"`
}

// CostConstraints defines hard cost limits for a workload.
type CostConstraints struct {
	MaxHourlyCost  float64 `json:"maxHourlyCost,omitempty"`
	MaxMonthlyCost float64 `json:"maxMonthlyCost,omitempty"`
	// +kubebuilder:default="USD"
	Currency string `json:"currency,omitempty"`
	// +kubebuilder:validation:Enum=hard;soft
	// +kubebuilder:default="soft"
	Enforcement string `json:"enforcement,omitempty"`
}

// --- Phase 3: Vertical Scaling ---

// VerticalConstraints defines resource limits for vertical scaling.
type VerticalConstraints struct {
	MinCPURequest       resource.Quantity `json:"minCPURequest,omitempty"`
	MaxCPURequest       resource.Quantity `json:"maxCPURequest,omitempty"`
	MinMemoryRequest    resource.Quantity `json:"minMemoryRequest,omitempty"`
	MaxMemoryRequest    resource.Quantity `json:"maxMemoryRequest,omitempty"`
	MaxCPUStepPercent   int32             `json:"maxCPUStepPercent,omitempty"`
	MaxMemoryStepPercent int32            `json:"maxMemoryStepPercent,omitempty"`
}

// ContainerVerticalConfig configures per-container vertical scaling.
type ContainerVerticalConfig struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
}

// VerticalScalingConfig configures vertical (resource) scaling.
type VerticalScalingConfig struct {
	Enabled bool `json:"enabled"`
	// +kubebuilder:validation:Enum=InPlace;Recreate;InPlaceOrRecreate;RecommendOnly
	// +kubebuilder:default="RecommendOnly"
	ResizePolicy string              `json:"resizePolicy,omitempty"`
	Constraints  VerticalConstraints `json:"constraints,omitempty"`
	// +optional
	Containers []ContainerVerticalConfig `json:"containers,omitempty"`
}

// ApplicationProfile describes workload lifecycle characteristics.
type ApplicationProfile struct {
	ColdStartDuration        metav1.Duration `json:"coldStartDuration,omitempty"`
	WarmUpDuration           metav1.Duration `json:"warmUpDuration,omitempty"`
	GracefulShutdownDuration metav1.Duration `json:"gracefulShutdownDuration,omitempty"`
}

// ResourceSet defines a set of resource requests.
type ResourceSet struct {
	CPURequest    resource.Quantity `json:"cpuRequest,omitempty"`
	MemoryRequest resource.Quantity `json:"memoryRequest,omitempty"`
}

// ResourceProfile defines a named resource profile with an optional schedule.
type ResourceProfile struct {
	Name      string      `json:"name"`
	Schedule  string      `json:"schedule,omitempty"`
	Resources ResourceSet `json:"resources"`
}

// ResourceStatus tracks the current resource state.
type ResourceStatus struct {
	CPURequest         resource.Quantity `json:"cpuRequest,omitempty"`
	MemoryRequest      resource.Quantity `json:"memoryRequest,omitempty"`
	CPULimit           resource.Quantity `json:"cpuLimit,omitempty"`
	MemoryLimit        resource.Quantity `json:"memoryLimit,omitempty"`
	LastResizeTime     *metav1.Time      `json:"lastResizeTime,omitempty"`
	LastResizeStrategy string            `json:"lastResizeStrategy,omitempty"`
}

// VerticalDecision is the LLM's resource change proposal.
type VerticalDecision struct {
	CPURequest     resource.Quantity `json:"cpuRequest,omitempty"`
	MemoryRequest  resource.Quantity `json:"memoryRequest,omitempty"`
	CPULimit       resource.Quantity `json:"cpuLimit,omitempty"`
	MemoryLimit    resource.Quantity `json:"memoryLimit,omitempty"`
	ResizeStrategy string            `json:"resizeStrategy,omitempty"`
}

// --- Phase 6: Predictive Scaling ---

// PredictiveEvent defines a known future event affecting load.
type PredictiveEvent struct {
	Name                    string      `json:"name"`
	Start                   metav1.Time `json:"start"`
	End                     metav1.Time `json:"end"`
	ExpectedLoadMultiplier  float64     `json:"expectedLoadMultiplier,omitempty"`
	PreScaleMinutes         int32       `json:"preScaleMinutes,omitempty"`
}

// PredictiveScalingConfig configures predictive (seasonal) scaling.
type PredictiveScalingConfig struct {
	Enabled             bool             `json:"enabled"`
	LookaheadWindow     metav1.Duration  `json:"lookaheadWindow,omitempty"`
	HistoryDays         int32            `json:"historyDays,omitempty"`
	ConfidenceThreshold float64          `json:"confidenceThreshold,omitempty"`
	// +optional
	Events []PredictiveEvent `json:"events,omitempty"`
}

// PredictionStatus tracks current predictions.
type PredictionStatus struct {
	PredictedCPU30m      float64 `json:"predictedCPU30m,omitempty"`
	PredictedReplicas30m float64 `json:"predictedReplicas30m,omitempty"`
	Confidence           float64 `json:"confidence,omitempty"`
}

// --- Phase 9: Safety & Guardrails ---

// DirectionPolicy defines asymmetric scaling constraints per direction.
type DirectionPolicy struct {
	MaxStep            int32           `json:"maxStep,omitempty"`
	Cooldown           metav1.Duration `json:"cooldown,omitempty"`
	RequireConfidence  float64         `json:"requireConfidence,omitempty"`
	RequireSLOHeadroom bool            `json:"requireSLOHeadroom,omitempty"`
}

// CircuitBreakerConfig configures the safety circuit breaker.
type CircuitBreakerConfig struct {
	Enabled        bool            `json:"enabled"`
	TripAfter      int32           `json:"tripAfter,omitempty"`
	ResetAfter     metav1.Duration `json:"resetAfter,omitempty"`
	TripConditions []string        `json:"tripConditions,omitempty"`
}

// AutoRollbackConfig configures automatic rollback on degradation.
type AutoRollbackConfig struct {
	Enabled    bool     `json:"enabled"`
	Conditions []string `json:"conditions,omitempty"`
}

// SafetyConfig defines safety guardrails.
type SafetyConfig struct {
	ScaleUp                     *DirectionPolicy      `json:"scaleUp,omitempty"`
	ScaleDown                   *DirectionPolicy      `json:"scaleDown,omitempty"`
	CircuitBreaker              *CircuitBreakerConfig  `json:"circuitBreaker,omitempty"`
	AutoRollback                *AutoRollbackConfig    `json:"autoRollback,omitempty"`
	MaxReplicaChangePerMinute   int32                  `json:"maxReplicaChangePerMinute,omitempty"`
	MaxWorkloadsChangedPerMinute int32                 `json:"maxWorkloadsChangedPerMinute,omitempty"`
}

// LLMRateLimit configures rate limiting for LLM API calls.
type LLMRateLimit struct {
	MaxCallsPerMinute    int32 `json:"maxCallsPerMinute,omitempty"`
	MaxCallsPerHour      int32 `json:"maxCallsPerHour,omitempty"`
	MaxConcurrentCalls   int32 `json:"maxConcurrentCalls,omitempty"`
}

// --- Phase 10: Observability / Audit ---

// LLMTelemetry tracks LLM usage metrics in status.
type LLMTelemetry struct {
	TotalCallsToday int32    `json:"totalCallsToday,omitempty"`
	AverageLatencyMs float64 `json:"averageLatencyMs,omitempty"`
	ErrorRate       float64  `json:"errorRate,omitempty"`
	FallbackRate    float64  `json:"fallbackRate,omitempty"`
}

// --- Phase 11: Multi-Workload ---

// CoscaleRef links two workloads that should scale together.
type CoscaleRef struct {
	TargetRef TargetRef `json:"targetRef"`
	Ratio     float64   `json:"ratio,omitempty"`
}

// DependencyConfig defines workload scaling dependencies.
type DependencyConfig struct {
	UpstreamOf   []TargetRef  `json:"upstreamOf,omitempty"`
	DownstreamOf []TargetRef  `json:"downstreamOf,omitempty"`
	CoscalesWith []CoscaleRef `json:"coscalesWith,omitempty"`
}

// --- SLO Enhancements ---

// SLOViolationBudget tracks allowed SLO violation windows.
type SLOViolationBudget struct {
	Window              metav1.Duration `json:"window"`
	MaxViolationPercent float64         `json:"maxViolationPercent"`
}

// --- Approval Workflow ---

// ApprovalTrigger defines when human approval is required.
type ApprovalTrigger struct {
	// +kubebuilder:validation:Enum=ReplicaChangeExceeds;CostIncreaseExceeds;ConfidenceBelow;Always
	Type      string  `json:"type"`
	Threshold float64 `json:"threshold,omitempty"`
}

// NotificationChannel defines a channel for notifications.
type NotificationChannel struct {
	// +kubebuilder:validation:Enum=slack;pagerduty;webhook;email
	Type string `json:"type"`
	// URL or endpoint
	Endpoint string `json:"endpoint,omitempty"`
	// Secret for authentication
	// +optional
	SecretRef *SecretRef `json:"secretRef,omitempty"`
}

// ApprovalConfig configures human-in-the-loop approval.
type ApprovalConfig struct {
	Enabled                 bool                  `json:"enabled"`
	RequireApprovalWhen     []ApprovalTrigger     `json:"requireApprovalWhen,omitempty"`
	Channels                []NotificationChannel `json:"channels,omitempty"`
	ApprovalTimeout         metav1.Duration       `json:"approvalTimeout,omitempty"`
	AutoApproveAfterTimeout bool                  `json:"autoApproveAfterTimeout,omitempty"`
}

// --- Precedence ---

// PrecedenceConfig controls how multiple decision sources are resolved.
type PrecedenceConfig struct {
	// +kubebuilder:validation:Enum=soft;hard
	CostEnforcement        string `json:"costEnforcement,omitempty"`
	ScheduleOverridesLLM   bool   `json:"scheduleOverridesLLM,omitempty"`
	ReactiveRulesOverrideLLM bool `json:"reactiveRulesOverrideLLM,omitempty"`
	SLOAlwaysWins          bool   `json:"sloAlwaysWins,omitempty"`
	LogConflicts           bool   `json:"logConflicts,omitempty"`
}

// PrecedenceConflict records a conflict between decision layers.
type PrecedenceConflict struct {
	Layer   string `json:"layer"`
	Message string `json:"message"`
}

// ActiveOverride records an active override.
type ActiveOverride struct {
	Layer     string `json:"layer"`
	ExpiresAt string `json:"expiresAt,omitempty"`
}

// PrecedenceStatus tracks precedence resolution.
type PrecedenceStatus struct {
	ResolvedReplicas int32                `json:"resolvedReplicas,omitempty"`
	Conflicts        []PrecedenceConflict `json:"conflicts,omitempty"`
	ActiveOverrides  []ActiveOverride     `json:"activeOverrides,omitempty"`
}

// --- Feedback Loop ---

// FeedbackConfig configures the decision feedback loop.
type FeedbackConfig struct {
	Enabled         bool            `json:"enabled"`
	EvaluationDelay metav1.Duration `json:"evaluationDelay,omitempty"`
	HistoryDepth    int32           `json:"historyDepth,omitempty"`
	IncludeInPrompt int32           `json:"includeInPrompt,omitempty"`
}

// FeedbackStatus tracks decision effectiveness.
type FeedbackStatus struct {
	TotalDecisions int32   `json:"totalDecisions,omitempty"`
	EffectiveRate  float64 `json:"effectiveRate,omitempty"`
}

// --- StatefulSet Policy ---

// StatefulSetPolicy defines StatefulSet-specific scaling behavior.
type StatefulSetPolicy struct {
	// +kubebuilder:validation:Enum=oneAtATime;maxUnavailable
	ScaleDownPolicy string          `json:"scaleDownPolicy,omitempty"`
	DrainTimeout    metav1.Duration `json:"drainTimeout,omitempty"`
	PVCRetention    string          `json:"pvcRetention,omitempty"`
}

// --- KEDA Integration ---

// KEDAIntegration configures KEDA ScaledObject awareness.
type KEDAIntegration struct {
	Enabled        bool      `json:"enabled"`
	ScaledObjectRef TargetRef `json:"scaledObjectRef,omitempty"`
	// +kubebuilder:default="advisory"
	Mode string `json:"mode,omitempty"`
}

// CostStatus tracks cost status in the AIScaler status.
type CostStatus struct {
	CurrentHourlyCost  float64 `json:"currentHourlyCost,omitempty"`
	CurrentMonthlyCost float64 `json:"currentMonthlyCost,omitempty"`
	WastePercent       float64 `json:"wastePercent,omitempty"`
	BudgetUtilization  float64 `json:"budgetUtilization,omitempty"`
}

type PrometheusConfig struct {
	// +kubebuilder:validation:Required
	BaseURL string `json:"baseURL"`
	// +optional
	P95LatencyQuery string `json:"p95LatencyQuery,omitempty"`
	// +optional
	ErrorRateQuery string `json:"errorRateQuery,omitempty"`
}

type AIScalerSpec struct {
	// +kubebuilder:validation:Required
	TargetRef TargetRef `json:"targetRef"`
	// +kubebuilder:validation:Required
	Constraints ScalingConstraints `json:"constraints"`
	// +kubebuilder:validation:Required
	LLM LLMConfig `json:"llm"`
	// Prometheus config (legacy — prefer using signals[].name=prometheus)
	// +optional
	Prometheus PrometheusConfig `json:"prometheus,omitempty"`
	// Signal sources — each entry enables a signal plugin
	// +optional
	Signals []SignalSourceConfig `json:"signals,omitempty"`
	// Alerting configuration
	// +optional
	Alerting *AlertingConfig `json:"alerting,omitempty"`
	// Cost/FinOps configuration
	// +optional
	Cost *CostConfig `json:"cost,omitempty"`
	// Hard cost constraints
	// +optional
	CostConstraints *CostConstraints `json:"costConstraints,omitempty"`
	// SLO definitions
	// +optional
	SLOs []SLO `json:"slos,omitempty"`
	// SLO violation budget
	// +optional
	SLOViolationBudget *SLOViolationBudget `json:"sloViolationBudget,omitempty"`
	// Vertical scaling configuration
	// +optional
	VerticalScaling *VerticalScalingConfig `json:"verticalScaling,omitempty"`
	// Application lifecycle profile
	// +optional
	ApplicationProfile *ApplicationProfile `json:"applicationProfile,omitempty"`
	// Resource profiles (time-based resource configurations)
	// +optional
	ResourceProfiles []ResourceProfile `json:"resourceProfiles,omitempty"`
	// Predictive scaling configuration
	// +optional
	PredictiveScaling *PredictiveScalingConfig `json:"predictiveScaling,omitempty"`
	// Safety guardrails
	// +optional
	Safety *SafetyConfig `json:"safety,omitempty"`
	// LLM rate limiting
	// +optional
	LLMRateLimit *LLMRateLimit `json:"llmRateLimit,omitempty"`
	// Workload tier for priority scheduling
	// +kubebuilder:validation:Enum=critical;standard;best-effort
	// +optional
	Tier string `json:"tier,omitempty"`
	// Workload dependencies
	// +optional
	Dependencies *DependencyConfig `json:"dependencies,omitempty"`
	// KEDA integration
	// +optional
	KEDAIntegration *KEDAIntegration `json:"kedaIntegration,omitempty"`
	// Human-in-the-loop approval
	// +optional
	Approval *ApprovalConfig `json:"approval,omitempty"`
	// Decision precedence configuration
	// +optional
	Precedence *PrecedenceConfig `json:"precedence,omitempty"`
	// Decision feedback loop
	// +optional
	Feedback *FeedbackConfig `json:"feedback,omitempty"`
	// StatefulSet-specific policy
	// +optional
	StatefulSetPolicy *StatefulSetPolicy `json:"statefulSetPolicy,omitempty"`
	// +kubebuilder:default="60s"
	// +optional
	EvaluationInterval metav1.Duration `json:"evaluationInterval,omitempty"`
	// +kubebuilder:default="300s"
	// +optional
	CooldownPeriod metav1.Duration `json:"cooldownPeriod,omitempty"`
	// +kubebuilder:default=false
	// +optional
	DryRun bool `json:"dryRun,omitempty"`
	// Require human approval for scaling decisions (simple flag, use Approval for advanced)
	// +kubebuilder:default=false
	// +optional
	RequireApproval bool `json:"requireApproval,omitempty"`
}

type AIScalerStatus struct {
	// +optional
	Phase ScalingPhase `json:"phase,omitempty"`
	// +optional
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`
	// +optional
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`
	// +optional
	LastDecisionReason string `json:"lastDecisionReason,omitempty"`
	// +optional
	LastProvider LLMProvider `json:"lastProvider,omitempty"`
	// +optional
	LastDecisionID string `json:"lastDecisionID,omitempty"`
	// Vertical scaling resource status
	// +optional
	CurrentResources *ResourceStatus `json:"currentResources,omitempty"`
	// Prediction status
	// +optional
	Prediction *PredictionStatus `json:"prediction,omitempty"`
	// Cost tracking
	// +optional
	Cost *CostStatus `json:"cost,omitempty"`
	// LLM usage telemetry
	// +optional
	LLMTelemetry *LLMTelemetry `json:"llmTelemetry,omitempty"`
	// Precedence resolution
	// +optional
	Precedence *PrecedenceStatus `json:"precedence,omitempty"`
	// Decision feedback
	// +optional
	Feedback *FeedbackStatus `json:"feedback,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=ais
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Current",type=integer,JSONPath=`.status.currentReplicas`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.status.desiredReplicas`
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.status.lastProvider`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type AIScaler struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              AIScalerSpec   `json:"spec,omitempty"`
	Status            AIScalerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type AIScalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AIScaler `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AIScaler{}, &AIScalerList{})
}

func (a *AIScalerSpec) ValidateSpec() error {
	if a.Constraints.MinReplicas > a.Constraints.MaxReplicas {
		return fmt.Errorf("minReplicas (%d) must be <= maxReplicas (%d)",
			a.Constraints.MinReplicas, a.Constraints.MaxReplicas)
	}
	if a.Constraints.MaxScaleStep > a.Constraints.MaxReplicas {
		return fmt.Errorf("maxScaleStep (%d) must be <= maxReplicas (%d)",
			a.Constraints.MaxScaleStep, a.Constraints.MaxReplicas)
	}

	if a.LLM.Fallback != nil && a.LLM.Primary == *a.LLM.Fallback {
		return fmt.Errorf("primary and fallback LLMs must be different")
	}
	return nil
}
