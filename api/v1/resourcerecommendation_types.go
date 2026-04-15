/*
Copyright 2026.
Licensed under the Apache License, Version 2.0.
*/

package v1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AnalysisConfig configures how resource analysis is performed.
type AnalysisConfig struct {
	// Window over which to analyze usage
	// +kubebuilder:default="168h"
	Window metav1.Duration `json:"window,omitempty"`
	// Cron schedule for periodic analysis
	// +optional
	Schedule string `json:"schedule,omitempty"`
	// Minimum sample percentage to trust analysis
	// +kubebuilder:default=80
	MinSamplePercent float64 `json:"minSamplePercent,omitempty"`
	// Buffer strategy for headroom
	// +kubebuilder:validation:Enum=fixed;percent;adaptive
	// +kubebuilder:default="percent"
	BufferStrategy string `json:"bufferStrategy,omitempty"`
	// Buffer percent to add on top of recommended resources
	// +kubebuilder:default=15
	BufferPercent float64 `json:"bufferPercent,omitempty"`
}

// ResourceRecommendationSpec defines the desired state.
type ResourceRecommendationSpec struct {
	// +kubebuilder:validation:Required
	TargetRef TargetRef `json:"targetRef"`
	// +kubebuilder:validation:Required
	Analysis AnalysisConfig `json:"analysis"`
	// Auto-apply recommendations
	// +kubebuilder:default=false
	AutoApply bool `json:"autoApply,omitempty"`
}

// UsagePercentiles captures percentile utilization data.
type UsagePercentiles struct {
	CPUP50    float64 `json:"cpuP50,omitempty"`
	CPUP95    float64 `json:"cpuP95,omitempty"`
	CPUP99    float64 `json:"cpuP99,omitempty"`
	CPUMax    float64 `json:"cpuMax,omitempty"`
	MemoryP50 float64 `json:"memoryP50,omitempty"`
	MemoryP95 float64 `json:"memoryP95,omitempty"`
	MemoryP99 float64 `json:"memoryP99,omitempty"`
	MemoryMax float64 `json:"memoryMax,omitempty"`
}

// RecommendedResources holds the recommended resource settings.
type RecommendedResources struct {
	CPURequest    resource.Quantity `json:"cpuRequest,omitempty"`
	MemoryRequest resource.Quantity `json:"memoryRequest,omitempty"`
	CPULimit      resource.Quantity `json:"cpuLimit,omitempty"`
	MemoryLimit   resource.Quantity `json:"memoryLimit,omitempty"`
}

// ResourceRecommendationStatus defines the observed state.
type ResourceRecommendationStatus struct {
	Phase                string                `json:"phase,omitempty"` // Pending, Analyzed, Applied, Rejected
	RecommendedResources *RecommendedResources `json:"recommendedResources,omitempty"`
	Usage                *UsagePercentiles     `json:"usage,omitempty"`
	EstimatedMonthlySavings float64            `json:"estimatedMonthlySavings,omitempty"`
	Risk                 string                `json:"risk,omitempty"` // low, medium, high
	LastAnalyzedAt       *metav1.Time          `json:"lastAnalyzedAt,omitempty"`
	Message              string                `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=rr
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Savings",type=number,JSONPath=`.status.estimatedMonthlySavings`
// +kubebuilder:printcolumn:name="Risk",type=string,JSONPath=`.status.risk`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ResourceRecommendation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ResourceRecommendationSpec   `json:"spec,omitempty"`
	Status            ResourceRecommendationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ResourceRecommendationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ResourceRecommendation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ResourceRecommendation{}, &ResourceRecommendationList{})
}
