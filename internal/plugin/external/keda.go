package external

import (
	"context"
	"fmt"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func init() {
	plugin.Register("keda", func() plugin.SignalPlugin {
		return &kedaPlugin{}
	})
}

type kedaPlugin struct {
	client           client.Client
	scaledObjectName string
	healthy          bool
}

func (p *kedaPlugin) Name() string   { return "keda" }
func (p *kedaPlugin) Required() bool { return false }
func (p *kedaPlugin) Healthy() bool  { return p.healthy }

func (p *kedaPlugin) SetK8sDeps(deps plugin.K8sPluginDeps) {
	p.client = deps.Client.(client.Client)
}

func (p *kedaPlugin) Init(cfg map[string]string) error {
	p.scaledObjectName = cfg["scaledObjectName"]
	if p.scaledObjectName == "" {
		return fmt.Errorf("keda plugin requires 'scaledObjectName' config")
	}
	p.healthy = true
	return nil
}

func (p *kedaPlugin) Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	so := &unstructured.Unstructured{}
	so.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledObject",
	})

	ns := policy.Spec.TargetRef.Namespace
	key := types.NamespacedName{Namespace: ns, Name: p.scaledObjectName}
	if err := p.client.Get(ctx, key, so); err != nil {
		p.healthy = false
		return fmt.Errorf("failed to get KEDA ScaledObject %s/%s: %w", ns, p.scaledObjectName, err)
	}

	desiredReplicas, found, _ := unstructured.NestedInt64(so.Object, "status", "desiredReplicas")
	if found {
		bundle.KEDADesiredReplicas = int32(desiredReplicas)
	}

	if bundle.CustomSignals == nil {
		bundle.CustomSignals = make(map[string]float64)
	}
	bundle.CustomSignals["keda_desired_replicas"] = float64(desiredReplicas)

	p.healthy = true
	return nil
}
