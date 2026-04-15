/*
Copyright 2026.
Licensed under the Apache License, Version 2.0.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ScheduleType defines the type of schedule.
// +kubebuilder:validation:Enum=Recurrence;OneShot;NaturalLanguage
type ScheduleType string

const (
	ScheduleRecurrence    ScheduleType = "Recurrence"
	ScheduleOneShot       ScheduleType = "OneShot"
	ScheduleNaturalLang   ScheduleType = "NaturalLanguage"
)

// ScheduleSpec defines when this schedule activates.
type ScheduleSpec struct {
	// +kubebuilder:validation:Required
	Type ScheduleType `json:"type"`
	// Timezone (IANA), default UTC
	// +kubebuilder:default="UTC"
	Timezone string `json:"timezone,omitempty"`
	// Cron expression (for Recurrence)
	// +optional
	Recurrence string `json:"recurrence,omitempty"`
	// One-shot activation window
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`
	// Natural language expression (compiled by LLM)
	// +optional
	Expression string `json:"expression,omitempty"`
}

// SchedulePolicy defines what happens when the schedule is active.
type SchedulePolicy struct {
	MinReplicas    *int32  `json:"minReplicas,omitempty"`
	MaxReplicas    *int32  `json:"maxReplicas,omitempty"`
	TargetReplicas *int32  `json:"targetReplicas,omitempty"`
}

// ScheduledScalingSpec defines the desired state of ScheduledScaling.
type ScheduledScalingSpec struct {
	// +kubebuilder:validation:Required
	TargetRef TargetRef `json:"targetRef"`
	// +kubebuilder:validation:Required
	Schedule ScheduleSpec `json:"schedule"`
	// +kubebuilder:validation:Required
	Policy SchedulePolicy `json:"policy"`
	// TTL after which completed one-shot schedules are cleaned up
	// +optional
	TTLAfterCompletion *metav1.Duration `json:"ttlAfterCompletion,omitempty"`
}

// ScheduledScalingStatus defines the observed state.
type ScheduledScalingStatus struct {
	Active          bool         `json:"active,omitempty"`
	Phase           string       `json:"phase,omitempty"`
	NextActivation  *metav1.Time `json:"nextActivation,omitempty"`
	LastActivation  *metav1.Time `json:"lastActivation,omitempty"`
	// Compiled schedule (only for NaturalLanguage type)
	CompiledFrom    string  `json:"compiledFrom,omitempty"`
	CompilationConf float64 `json:"compilationConfidence,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=ss
// +kubebuilder:printcolumn:name="Active",type=boolean,JSONPath=`.status.active`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ScheduledScaling struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ScheduledScalingSpec   `json:"spec,omitempty"`
	Status            ScheduledScalingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ScheduledScalingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ScheduledScaling `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ScheduledScaling{}, &ScheduledScalingList{})
}
