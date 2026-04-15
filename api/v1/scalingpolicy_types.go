/*
Copyright 2026.
Licensed under the Apache License, Version 2.0.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ScalingPolicyDefaults defines cluster-wide default settings.
type ScalingPolicyDefaults struct {
	Safety         *SafetyConfig      `json:"safety,omitempty"`
	CostConstraints *CostConstraints  `json:"costConstraints,omitempty"`
	SLOs           []SLO              `json:"slos,omitempty"`
	LLMRateLimit   *LLMRateLimit      `json:"llmRateLimit,omitempty"`
}

// ScalingPolicySpec defines the desired state of ScalingPolicy.
type ScalingPolicySpec struct {
	// Defaults applied to all AIScalers unless overridden
	// +kubebuilder:validation:Required
	Defaults ScalingPolicyDefaults `json:"defaults"`
	// Namespace selector — which namespaces this policy applies to
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`
}

// ScalingPolicyStatus defines the observed state.
type ScalingPolicyStatus struct {
	AppliedTo int32  `json:"appliedTo,omitempty"`
	Phase     string `json:"phase,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=sp
// +kubebuilder:printcolumn:name="Applied",type=integer,JSONPath=`.status.appliedTo`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ScalingPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ScalingPolicySpec   `json:"spec,omitempty"`
	Status            ScalingPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ScalingPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ScalingPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ScalingPolicy{}, &ScalingPolicyList{})
}
