package approval

import (
	"testing"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/llm"
)

func TestManager_NoApprovalNeeded(t *testing.T) {
	m := NewManager()
	policy := &aiscalerv1.AIScaler{}
	dec := &llm.ScalingDecision{TargetReplicas: 5, Confidence: 0.9}

	if m.NeedsApproval(policy, dec, 3, 0.0) {
		t.Error("expected no approval needed with defaults")
	}
}

func TestManager_RequireApprovalFlag(t *testing.T) {
	m := NewManager()
	policy := &aiscalerv1.AIScaler{}
	policy.Spec.RequireApproval = true
	dec := &llm.ScalingDecision{TargetReplicas: 5, Confidence: 0.9}

	if !m.NeedsApproval(policy, dec, 3, 0.0) {
		t.Error("expected approval needed with RequireApproval=true")
	}
}

func TestManager_ReplicaChangeExceeds(t *testing.T) {
	m := NewManager()
	policy := &aiscalerv1.AIScaler{}
	policy.Spec.Approval = &aiscalerv1.ApprovalConfig{
		Enabled: true,
		RequireApprovalWhen: []aiscalerv1.ApprovalTrigger{
			{Type: "ReplicaChangeExceeds", Threshold: 5},
		},
	}

	// Change of 10 replicas should require approval
	dec := &llm.ScalingDecision{TargetReplicas: 15, Confidence: 0.9}
	if !m.NeedsApproval(policy, dec, 5, 0.0) {
		t.Error("expected approval for large replica change")
	}

	// Change of 2 replicas should not require approval
	dec2 := &llm.ScalingDecision{TargetReplicas: 7, Confidence: 0.9}
	if m.NeedsApproval(policy, dec2, 5, 0.0) {
		t.Error("expected no approval for small replica change")
	}
}

func TestManager_ConfidenceBelow(t *testing.T) {
	m := NewManager()
	policy := &aiscalerv1.AIScaler{}
	policy.Spec.Approval = &aiscalerv1.ApprovalConfig{
		Enabled: true,
		RequireApprovalWhen: []aiscalerv1.ApprovalTrigger{
			{Type: "ConfidenceBelow", Threshold: 0.7},
		},
	}

	dec := &llm.ScalingDecision{TargetReplicas: 5, Confidence: 0.5}
	if !m.NeedsApproval(policy, dec, 3, 0.0) {
		t.Error("expected approval for low confidence")
	}

	dec2 := &llm.ScalingDecision{TargetReplicas: 5, Confidence: 0.9}
	if m.NeedsApproval(policy, dec2, 3, 0.0) {
		t.Error("expected no approval for high confidence")
	}
}

func TestManager_CostIncreaseExceeds(t *testing.T) {
	m := NewManager()
	policy := &aiscalerv1.AIScaler{}
	policy.Spec.Approval = &aiscalerv1.ApprovalConfig{
		Enabled: true,
		RequireApprovalWhen: []aiscalerv1.ApprovalTrigger{
			{Type: "CostIncreaseExceeds", Threshold: 5.0},
		},
	}

	dec := &llm.ScalingDecision{TargetReplicas: 10, Confidence: 0.9}
	if !m.NeedsApproval(policy, dec, 3, 10.0) {
		t.Error("expected approval for high cost delta")
	}

	if m.NeedsApproval(policy, dec, 3, 2.0) {
		t.Error("expected no approval for low cost delta")
	}
}

func TestManager_CreateRequest(t *testing.T) {
	m := NewManager()
	policy := &aiscalerv1.AIScaler{}
	policy.Spec.TargetRef.Name = "web-api"
	policy.Spec.TargetRef.Namespace = "production"

	dec := &llm.ScalingDecision{
		TargetReplicas: 10,
		Confidence:     0.8,
		Reasoning:      "traffic spike",
	}

	req := m.CreateRequest(policy, dec, 5, 3.5)
	if req.Workload != "web-api" {
		t.Errorf("expected workload web-api, got %s", req.Workload)
	}
	if req.CurrentReplicas != 5 || req.TargetReplicas != 10 {
		t.Errorf("unexpected replicas: %d→%d", req.CurrentReplicas, req.TargetReplicas)
	}
}
