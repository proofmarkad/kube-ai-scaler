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
}

type SecretRef struct {
	// +kubebuilder:validation:Required
	Name string `json:"name"`
	// +kubebuilder:validation:Required
	Namespace string `json:"namespace"`
	// +kubebuilder:validation:Required
	Key string `json:"key"`
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
	// +kubebuilder:validation:Required
	Prometheus PrometheusConfig `json:"prometheus"`
	// +kubebuilder:default="60s"
	// +optional
	EvaluationInterval metav1.Duration `json:"evaluationInterval,omitempty"`
	// +kubebuilder:default="300s"
	// +optional
	CooldownPeriod metav1.Duration `json:"cooldownPeriod,omitempty"`
	// +kubebuilder:default=false
	// +optional
	DryRun bool `json:"dryRun,omitempty"`
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
