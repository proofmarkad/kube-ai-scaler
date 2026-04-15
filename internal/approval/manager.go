package approval

import (
	"fmt"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/llm"
)

// Manager handles the approval workflow for scaling decisions.
type Manager struct{}

// NewManager creates a new approval manager.
func NewManager() *Manager { return &Manager{} }

// ApprovalRequest holds a pending approval.
type ApprovalRequest struct {
	Workload       string
	Namespace      string
	CurrentReplicas int32
	TargetReplicas int32
	Reasoning      string
	Confidence     float64
	CostDelta      float64
}

// NeedsApproval checks the policy to determine if this decision requires human approval.
func (m *Manager) NeedsApproval(
	policy *aiscalerv1.AIScaler,
	dec *llm.ScalingDecision,
	currentReplicas int32,
	costDelta float64,
) bool {
	// Simple flag check
	if policy.Spec.RequireApproval {
		return true
	}

	// Advanced approval config
	approval := policy.Spec.Approval
	if approval == nil || !approval.Enabled {
		return false
	}

	replicaChange := abs32(dec.TargetReplicas - currentReplicas)

	for _, trigger := range approval.RequireApprovalWhen {
		switch trigger.Type {
		case "ReplicaChangeExceeds":
			if float64(replicaChange) > trigger.Threshold {
				return true
			}
		case "CostIncreaseExceeds":
			if costDelta > trigger.Threshold {
				return true
			}
		case "ConfidenceBelow":
			if dec.Confidence < trigger.Threshold {
				return true
			}
		case "Always":
			return true
		}
	}

	return false
}

// CreateRequest builds an approval request from a decision.
func (m *Manager) CreateRequest(
	policy *aiscalerv1.AIScaler,
	dec *llm.ScalingDecision,
	currentReplicas int32,
	costDelta float64,
) *ApprovalRequest {
	return &ApprovalRequest{
		Workload:        policy.Spec.TargetRef.Name,
		Namespace:       policy.Spec.TargetRef.Namespace,
		CurrentReplicas: currentReplicas,
		TargetReplicas:  dec.TargetReplicas,
		Reasoning:       dec.Reasoning,
		Confidence:      dec.Confidence,
		CostDelta:       costDelta,
	}
}

// FormatMessage creates a human-readable message for the approval request.
func (ar *ApprovalRequest) FormatMessage() string {
	direction := "scale up"
	if ar.TargetReplicas < ar.CurrentReplicas {
		direction = "scale down"
	}
	return fmt.Sprintf(
		"[AIScaler] Approval required to %s %s/%s from %d to %d replicas (confidence: %.0f%%, cost delta: $%.2f/hr)\nReason: %s",
		direction, ar.Namespace, ar.Workload,
		ar.CurrentReplicas, ar.TargetReplicas,
		ar.Confidence*100, ar.CostDelta,
		ar.Reasoning,
	)
}

func abs32(x int32) int32 {
	if x < 0 {
		return -x
	}
	return x
}
