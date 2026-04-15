package actuator

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DeploymentAccessor implements WorkloadAccessor for Deployments.
type DeploymentAccessor struct {
	client    client.Client
	namespace string
	name      string
}

// NewDeploymentAccessor creates a Deployment accessor.
func NewDeploymentAccessor(c client.Client, namespace, name string) *DeploymentAccessor {
	return &DeploymentAccessor{client: c, namespace: namespace, name: name}
}

func (d *DeploymentAccessor) Kind() string { return "Deployment" }

func (d *DeploymentAccessor) GetReplicas(ctx context.Context) (int32, error) {
	dep := &appsv1.Deployment{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: d.namespace, Name: d.name}, dep); err != nil {
		return 0, err
	}
	if dep.Spec.Replicas == nil {
		return 1, nil
	}
	return *dep.Spec.Replicas, nil
}

func (d *DeploymentAccessor) SetReplicas(ctx context.Context, replicas int32, dryRun bool) error {
	dep := &appsv1.Deployment{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: d.namespace, Name: d.name}, dep); err != nil {
		return err
	}

	patch := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Namespace: d.namespace, Name: d.name},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas, Selector: dep.Spec.Selector},
	}

	opts := []client.PatchOption{client.FieldOwner("aiscaler"), client.ForceOwnership}
	if dryRun {
		opts = append(opts, client.DryRunAll)
	}
	return d.client.Patch(ctx, patch, client.Apply, opts...)
}

func (d *DeploymentAccessor) GetResources(ctx context.Context) ([]corev1.Container, error) {
	dep := &appsv1.Deployment{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: d.namespace, Name: d.name}, dep); err != nil {
		return nil, err
	}
	return dep.Spec.Template.Spec.Containers, nil
}

func (d *DeploymentAccessor) GetSelector(ctx context.Context) (*metav1.LabelSelector, error) {
	dep := &appsv1.Deployment{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: d.namespace, Name: d.name}, dep); err != nil {
		return nil, err
	}
	return dep.Spec.Selector, nil
}

func (d *DeploymentAccessor) IsReady(ctx context.Context) (bool, error) {
	dep := &appsv1.Deployment{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: d.namespace, Name: d.name}, dep); err != nil {
		return false, err
	}
	if dep.Spec.Replicas == nil {
		return dep.Status.ReadyReplicas >= 1, nil
	}
	return dep.Status.ReadyReplicas >= *dep.Spec.Replicas, nil
}

func (d *DeploymentAccessor) SupportsInPlaceResize() bool { return false }

// StatefulSetAccessor implements WorkloadAccessor for StatefulSets.
type StatefulSetAccessor struct {
	client    client.Client
	namespace string
	name      string
}

// NewStatefulSetAccessor creates a StatefulSet accessor.
func NewStatefulSetAccessor(c client.Client, namespace, name string) *StatefulSetAccessor {
	return &StatefulSetAccessor{client: c, namespace: namespace, name: name}
}

func (s *StatefulSetAccessor) Kind() string { return "StatefulSet" }

func (s *StatefulSetAccessor) GetReplicas(ctx context.Context) (int32, error) {
	ss := &appsv1.StatefulSet{}
	if err := s.client.Get(ctx, types.NamespacedName{Namespace: s.namespace, Name: s.name}, ss); err != nil {
		return 0, err
	}
	if ss.Spec.Replicas == nil {
		return 1, nil
	}
	return *ss.Spec.Replicas, nil
}

func (s *StatefulSetAccessor) SetReplicas(ctx context.Context, replicas int32, dryRun bool) error {
	ss := &appsv1.StatefulSet{}
	if err := s.client.Get(ctx, types.NamespacedName{Namespace: s.namespace, Name: s.name}, ss); err != nil {
		return err
	}

	patch := &appsv1.StatefulSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{Namespace: s.namespace, Name: s.name},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas, Selector: ss.Spec.Selector, ServiceName: ss.Spec.ServiceName},
	}

	opts := []client.PatchOption{client.FieldOwner("aiscaler"), client.ForceOwnership}
	if dryRun {
		opts = append(opts, client.DryRunAll)
	}
	return s.client.Patch(ctx, patch, client.Apply, opts...)
}

func (s *StatefulSetAccessor) GetResources(ctx context.Context) ([]corev1.Container, error) {
	ss := &appsv1.StatefulSet{}
	if err := s.client.Get(ctx, types.NamespacedName{Namespace: s.namespace, Name: s.name}, ss); err != nil {
		return nil, err
	}
	return ss.Spec.Template.Spec.Containers, nil
}

func (s *StatefulSetAccessor) GetSelector(ctx context.Context) (*metav1.LabelSelector, error) {
	ss := &appsv1.StatefulSet{}
	if err := s.client.Get(ctx, types.NamespacedName{Namespace: s.namespace, Name: s.name}, ss); err != nil {
		return nil, err
	}
	return ss.Spec.Selector, nil
}

func (s *StatefulSetAccessor) IsReady(ctx context.Context) (bool, error) {
	ss := &appsv1.StatefulSet{}
	if err := s.client.Get(ctx, types.NamespacedName{Namespace: s.namespace, Name: s.name}, ss); err != nil {
		return false, err
	}
	if ss.Spec.Replicas == nil {
		return ss.Status.ReadyReplicas >= 1, nil
	}
	return ss.Status.ReadyReplicas >= *ss.Spec.Replicas, nil
}

func (s *StatefulSetAccessor) SupportsInPlaceResize() bool { return false }

// NewWorkloadAccessor creates the appropriate accessor for a workload kind.
func NewWorkloadAccessor(c client.Client, kind, namespace, name string) (WorkloadAccessor, error) {
	switch kind {
	case "Deployment", "":
		return NewDeploymentAccessor(c, namespace, name), nil
	case "StatefulSet":
		return NewStatefulSetAccessor(c, namespace, name), nil
	default:
		return nil, fmt.Errorf("unsupported workload kind: %s", kind)
	}
}
