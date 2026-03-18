package actuator

import (
	"context"
	"fmt"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/decision"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Actuator applies scaling decisions to the target Deployment.
// It uses Server-Side Apply so it only owns the replicas field —
// other controllers can manage the rest of the Deployment freely.
type Actuator struct {
	client client.Client
}

// NewActuator creates a new Actuator.
func NewActuator(client client.Client) *Actuator {
	return &Actuator{
		client: client,
	}
}

// ApplyResult holds the outcome of an apply operation.
type ApplyResult struct {
	// PreviousReplicas is the replica count before this apply.
	PreviousReplicas int32

	// AppliedReplicas is the replica count after this apply.
	AppliedReplicas int32

	// AppliedAt is when the scale event was applied.
	AppliedAt time.Time

	// DryRun indicates the decision was computed but not applied.
	DryRun bool
}

// Apply patches the target Deployment's replica count using SSA.
// If policy.Spec.DryRun is true, it validates the patch without applying it.
func (a *Actuator) Apply(
	ctx context.Context,
	policy *aiscalerv1.AIScaler,
	result *decision.ValidationResult,
) (*ApplyResult, error) {
	// Fetch current replica count for the result record
	currentReplicas, err := a.currentReplicas(ctx, policy)
	if err != nil {
		return nil, err
	}

	// No-op if replica count is already correct
	if currentReplicas == result.ValidatedReplicas {
		return &ApplyResult{
			PreviousReplicas: currentReplicas,
			AppliedReplicas:  currentReplicas,
			AppliedAt:        time.Now(),
			DryRun:           policy.Spec.DryRun,
		}, nil
	}

	// Build a minimal Deployment with only the replicas field set.
	// SSA means we only declare what we own — everything else is
	// left untouched by other controllers.
	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: policy.Spec.TargetRef.Namespace,
			Name:      policy.Spec.TargetRef.Name,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &result.ValidatedReplicas,
		},
	}

	// Build patch options
	opts := []client.PatchOption{
		client.FieldOwner("aiscaler"),
		client.ForceOwnership,
	}
	if policy.Spec.DryRun {
		opts = append(opts, client.DryRunAll)
	}

	// Apply patch
	if err := a.client.Patch(ctx, deployment, client.Apply, opts...); err != nil {
		return nil, fmt.Errorf("SSA patch failed for %s/%s: %w",
			policy.Spec.TargetRef.Namespace,
			policy.Spec.TargetRef.Name,
			err,
		)
	}

	return &ApplyResult{
		PreviousReplicas: currentReplicas,
		AppliedReplicas:  result.ValidatedReplicas,
		AppliedAt:        time.Now(),
		DryRun:           policy.Spec.DryRun,
	}, nil
}

// currentReplicas fetches the current replica count of the target Deployment.
func (a *Actuator) currentReplicas(
	ctx context.Context,
	policy *aiscalerv1.AIScaler,
) (int32, error) {
	deployment := &appsv1.Deployment{}
	key := types.NamespacedName{
		Namespace: policy.Spec.TargetRef.Namespace,
		Name:      policy.Spec.TargetRef.Name,
	}

	if err := a.client.Get(ctx, key, deployment); err != nil {
		return 0, fmt.Errorf("failed to get deployment %s/%s: %w",
			policy.Spec.TargetRef.Namespace,
			policy.Spec.TargetRef.Name,
			err,
		)
	}

	if deployment.Spec.Replicas == nil {
		return 1, nil
	}

	return *deployment.Spec.Replicas, nil
}
