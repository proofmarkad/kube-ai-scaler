package builtin

import (
	"context"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func init() {
	plugin.Register("nodes", func() plugin.SignalPlugin {
		return &nodesPlugin{}
	})
}

type nodesPlugin struct {
	client  client.Client
	healthy bool
}

func (p *nodesPlugin) Name() string   { return "nodes" }
func (p *nodesPlugin) Required() bool { return false }
func (p *nodesPlugin) Healthy() bool  { return p.healthy }

func (p *nodesPlugin) SetK8sDeps(deps plugin.K8sPluginDeps) {
	p.client = deps.Client.(client.Client)
}

func (p *nodesPlugin) Init(cfg map[string]string) error {
	p.healthy = true
	return nil
}

func (p *nodesPlugin) Collect(ctx context.Context, _ *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	nodeList := &corev1.NodeList{}
	if err := p.client.List(ctx, nodeList); err != nil {
		p.healthy = false
		return err
	}

	var schedulable int32
	var totalCPUMillis, totalMemBytes int64
	var allocCPUMillis, allocMemBytes int64

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		if node.Spec.Unschedulable {
			continue
		}
		schedulable++

		if cpu := node.Status.Allocatable.Cpu(); cpu != nil {
			totalCPUMillis += cpu.MilliValue()
		}
		if mem := node.Status.Allocatable.Memory(); mem != nil {
			totalMemBytes += mem.Value()
		}

		// Use capacity - allocatable as a rough proxy for system reserved
		if cpu := node.Status.Capacity.Cpu(); cpu != nil {
			allocCPUMillis += cpu.MilliValue()
		}
		if mem := node.Status.Capacity.Memory(); mem != nil {
			allocMemBytes += mem.Value()
		}
	}

	bundle.ClusterNodes = schedulable
	if allocCPUMillis > 0 {
		bundle.ClusterCPUAvailable = float64(totalCPUMillis) / float64(allocCPUMillis) * 100.0
	}
	if allocMemBytes > 0 {
		bundle.ClusterMemAvailable = float64(totalMemBytes) / float64(allocMemBytes) * 100.0
	}

	p.healthy = true
	return nil
}
