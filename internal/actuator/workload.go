package actuator

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkloadAccessor abstracts access to different workload types.
type WorkloadAccessor interface {
	GetReplicas(ctx context.Context) (int32, error)
	SetReplicas(ctx context.Context, replicas int32, dryRun bool) error
	GetResources(ctx context.Context) ([]corev1.Container, error)
	GetSelector(ctx context.Context) (*metav1.LabelSelector, error)
	IsReady(ctx context.Context) (bool, error)
	SupportsInPlaceResize() bool
	Kind() string
}
