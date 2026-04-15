package builtin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
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
	annotationReactiveRules       = "aiscaler.io/reactive-rules"
)

func init() {
	plugin.Register("annotations", func() plugin.SignalPlugin {
		return &annotationsPlugin{}
	})
}

type annotationsPlugin struct {
	client  client.Client
	healthy bool
}

func (p *annotationsPlugin) Name() string   { return "annotations" }
func (p *annotationsPlugin) Required() bool { return false }
func (p *annotationsPlugin) Healthy() bool  { return p.healthy }

func (p *annotationsPlugin) SetK8sDeps(deps plugin.K8sPluginDeps) {
	p.client = deps.Client.(client.Client)
}

func (p *annotationsPlugin) Init(cfg map[string]string) error {
	p.healthy = true
	return nil
}

func (p *annotationsPlugin) Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	deploy := &appsv1.Deployment{}
	key := types.NamespacedName{
		Namespace: policy.Spec.TargetRef.Namespace,
		Name:      policy.Spec.TargetRef.Name,
	}
	if err := p.client.Get(ctx, key, deploy); err != nil {
		p.healthy = false
		return fmt.Errorf("failed to get deployment %s/%s: %w", key.Namespace, key.Name, err)
	}

	ann := deploy.GetAnnotations()
	if ann == nil {
		p.healthy = true
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

	// Parse reactive rules JSON
	if raw, ok := ann[annotationReactiveRules]; ok && raw != "" {
		var rules []plugin.ReactiveRule
		if err := json.Unmarshal([]byte(raw), &rules); err != nil {
			return fmt.Errorf("invalid reactive-rules JSON: %w", err)
		}
		bundle.Annotations.ReactiveRules = rules
	}

	p.healthy = true
	return nil
}
