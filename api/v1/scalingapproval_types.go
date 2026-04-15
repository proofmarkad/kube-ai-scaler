/*
Copyright 2026.
Licensed under the Apache License, Version 2.0.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProposedAction describes the scaling action pending approval.
type ProposedAction struct {
	Type              string  `json:"type"`                        // horizontal, vertical
	CurrentReplicas   int32   `json:"currentReplicas"`
	TargetReplicas    int32   `json:"targetReplicas"`
	Reasoning         string  `json:"reasoning"`
	Confidence        float64 `json:"confidence"`
	EstimatedCostDelta float64 `json:"estimatedCostDelta,omitempty"`
	TriggerReason     string  `json:"triggerReason,omitempty"`
}

// ScalingApprovalSpec defines the desired state of ScalingApproval.
type ScalingApprovalSpec struct {
	// +kubebuilder:validation:Required
	AIScalerRef TargetRef `json:"aiScalerRef"`
	// +kubebuilder:validation:Required
	ProposedAction ProposedAction `json:"proposedAction"`
	// +optional
	ExpiresAt *metav1.Time `json:"expiresAt,omitempty"`
}

// ScalingApprovalStatus defines the observed state.
type ScalingApprovalStatus struct {
	// +kubebuilder:validation:Enum=Pending;Approved;Rejected;Expired;Applied
	Phase      string       `json:"phase,omitempty"`
	ApprovedBy string       `json:"approvedBy,omitempty"`
	ApprovedAt *metav1.Time `json:"approvedAt,omitempty"`
	Message    string       `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=sa
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Target",type=integer,JSONPath=`.spec.proposedAction.targetReplicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ScalingApproval struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ScalingApprovalSpec   `json:"spec,omitempty"`
	Status            ScalingApprovalStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ScalingApprovalList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ScalingApproval `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ScalingApproval{}, &ScalingApprovalList{})
}
