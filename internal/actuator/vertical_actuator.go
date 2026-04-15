package actuator

import (
	"context"
	"fmt"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// VerticalActuator applies resource (CPU/memory) changes to workload containers.
type VerticalActuator struct {
	client client.Client
}

// NewVerticalActuator creates a new VerticalActuator.
func NewVerticalActuator(c client.Client) *VerticalActuator {
	return &VerticalActuator{client: c}
}

// VerticalApplyResult holds the outcome of a vertical scaling operation.
type VerticalApplyResult struct {
	ContainerName  string
	PreviousCPU    resource.Quantity
	PreviousMemory resource.Quantity
	AppliedCPU     resource.Quantity
	AppliedMemory  resource.Quantity
	Strategy       string
	Applied        bool
	RecommendOnly  bool
	DryRun         bool
}

// Apply patches container resource requests/limits on the target workload.
func (v *VerticalActuator) Apply(
	ctx context.Context,
	policy *aiscalerv1.AIScaler,
	vd *aiscalerv1.VerticalDecision,
) (*VerticalApplyResult, error) {
	log := logf.FromContext(ctx)

	vc := policy.Spec.VerticalScaling
	if vc == nil || !vc.Enabled {
		return nil, fmt.Errorf("vertical scaling not enabled")
	}

	// Validate the proposed change against constraints
	if err := ValidateVerticalChange(vd, &vc.Constraints); err != nil {
		return nil, fmt.Errorf("vertical validation failed: %w", err)
	}

	// Fetch the existing deployment
	existing := &appsv1.Deployment{}
	key := types.NamespacedName{
		Namespace: policy.Spec.TargetRef.Namespace,
		Name:      policy.Spec.TargetRef.Name,
	}
	if err := v.client.Get(ctx, key, existing); err != nil {
		return nil, fmt.Errorf("failed to get deployment: %w", err)
	}

	if len(existing.Spec.Template.Spec.Containers) == 0 {
		return nil, fmt.Errorf("deployment has no containers")
	}

	container := &existing.Spec.Template.Spec.Containers[0]
	result := &VerticalApplyResult{
		ContainerName:  container.Name,
		PreviousCPU:    *container.Resources.Requests.Cpu(),
		PreviousMemory: *container.Resources.Requests.Memory(),
		Strategy:       vc.ResizePolicy,
		DryRun:         policy.Spec.DryRun,
	}

	if vc.ResizePolicy == "RecommendOnly" {
		result.RecommendOnly = true
		result.AppliedCPU = vd.CPURequest
		result.AppliedMemory = vd.MemoryRequest
		log.Info("vertical scaling recommendation (not applied)",
			"container", container.Name,
			"cpuRequest", vd.CPURequest.String(),
			"memoryRequest", vd.MemoryRequest.String(),
		)
		return result, nil
	}

	// Build the patch
	patch := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: policy.Spec.TargetRef.Namespace,
			Name:      policy.Spec.TargetRef.Name,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: existing.Spec.Selector,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: container.Name,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    vd.CPURequest,
									corev1.ResourceMemory: vd.MemoryRequest,
								},
							},
						},
					},
				},
			},
		},
	}

	// Set limits if provided
	if !vd.CPULimit.IsZero() || !vd.MemoryLimit.IsZero() {
		limits := corev1.ResourceList{}
		if !vd.CPULimit.IsZero() {
			limits[corev1.ResourceCPU] = vd.CPULimit
		}
		if !vd.MemoryLimit.IsZero() {
			limits[corev1.ResourceMemory] = vd.MemoryLimit
		}
		patch.Spec.Template.Spec.Containers[0].Resources.Limits = limits
	}

	opts := []client.PatchOption{
		client.FieldOwner("aiscaler-vertical"),
		client.ForceOwnership,
	}
	if policy.Spec.DryRun {
		opts = append(opts, client.DryRunAll)
	}

	if err := v.client.Patch(ctx, patch, client.Apply, opts...); err != nil {
		return nil, fmt.Errorf("vertical SSA patch failed: %w", err)
	}

	result.AppliedCPU = vd.CPURequest
	result.AppliedMemory = vd.MemoryRequest
	result.Applied = true

	log.Info("vertical scaling applied",
		"container", container.Name,
		"cpuRequest", vd.CPURequest.String(),
		"memoryRequest", vd.MemoryRequest.String(),
		"strategy", vc.ResizePolicy,
	)

	return result, nil
}

// ValidateVerticalChange checks proposed resources against constraints.
func ValidateVerticalChange(vd *aiscalerv1.VerticalDecision, constraints *aiscalerv1.VerticalConstraints) error {
	if !constraints.MinCPURequest.IsZero() && vd.CPURequest.Cmp(constraints.MinCPURequest) < 0 {
		return fmt.Errorf("proposed CPU %s below minimum %s",
			vd.CPURequest.String(), constraints.MinCPURequest.String())
	}
	if !constraints.MaxCPURequest.IsZero() && vd.CPURequest.Cmp(constraints.MaxCPURequest) > 0 {
		return fmt.Errorf("proposed CPU %s above maximum %s",
			vd.CPURequest.String(), constraints.MaxCPURequest.String())
	}
	if !constraints.MinMemoryRequest.IsZero() && vd.MemoryRequest.Cmp(constraints.MinMemoryRequest) < 0 {
		return fmt.Errorf("proposed memory %s below minimum %s",
			vd.MemoryRequest.String(), constraints.MinMemoryRequest.String())
	}
	if !constraints.MaxMemoryRequest.IsZero() && vd.MemoryRequest.Cmp(constraints.MaxMemoryRequest) > 0 {
		return fmt.Errorf("proposed memory %s above maximum %s",
			vd.MemoryRequest.String(), constraints.MaxMemoryRequest.String())
	}
	return nil
}
