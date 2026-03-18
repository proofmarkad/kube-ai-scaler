package signals

import (
	"context"
	"fmt"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	annotationExpectedTraffic     = "aiscaler.io/expected-traffic"
	annotationScaleConservatively = "aiscaler.io/scale-conservatively"
	annotationFreezeUntil         = "aiscaler.io/freeze-until"
	annotationNote                = "aiscaler.io/note"
	annotationPeakHours           = "aiscaler.io/peak-hours"
)

type annotationCollector struct {
	client client.Client
}

func (a *annotationCollector) collect(
	ctx context.Context,
	policy *aiscalerv1.AIScaler,
	bundle *Bundle) error {

	deploy := &appsv1.Deployment{}
	key := types.NamespacedName{
		Namespace: policy.Spec.TargetRef.Namespace,
		Name:      policy.Spec.TargetRef.Name,
	}

	if err := a.client.Get(ctx, key, deploy); err != nil {
		return fmt.Errorf(
			"failed to get deployment %s/%s: %w",
			policy.Spec.TargetRef.Namespace,
			policy.Spec.TargetRef.Name,
			err,
		)
	}

	ann := deploy.GetAnnotations()
	if ann == nil {
		return nil
	}

	bundle.Annotations.ExpectedTraffic = ann[annotationExpectedTraffic]
	bundle.Annotations.ScaleConservatively = ann[annotationScaleConservatively] == "true"
	bundle.Annotations.Note = ann[annotationNote]
	bundle.Annotations.PeakHours = ann[annotationPeakHours]

	if raw, ok := ann[annotationFreezeUntil]; ok {
		freezeUntil, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return fmt.Errorf("invalid freeze-until timestamp %q: %w", raw, err)
		}
		bundle.Annotations.FreezeUntil = &freezeUntil
	}

	return nil
}
