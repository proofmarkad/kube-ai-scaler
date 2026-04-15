package builtin

import (
	"context"
	"fmt"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func init() {
	plugin.Register("metrics-server", func() plugin.SignalPlugin {
		return &metricsServerPlugin{}
	})
}

type metricsServerPlugin struct {
	client        client.Client
	metricsClient metricsclient.Interface
	healthy       bool
}

func (p *metricsServerPlugin) Name() string   { return "metrics-server" }
func (p *metricsServerPlugin) Required() bool { return true }
func (p *metricsServerPlugin) Healthy() bool  { return p.healthy }

func (p *metricsServerPlugin) SetK8sDeps(deps plugin.K8sPluginDeps) {
	p.client = deps.Client.(client.Client)
	p.metricsClient = deps.MetricsClient.(metricsclient.Interface)
}

func (p *metricsServerPlugin) Init(cfg map[string]string) error {
	p.healthy = true
	return nil
}

func (p *metricsServerPlugin) Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	deploy := &appsv1.Deployment{}
	key := types.NamespacedName{
		Namespace: policy.Spec.TargetRef.Namespace,
		Name:      policy.Spec.TargetRef.Name,
	}
	if err := p.client.Get(ctx, key, deploy); err != nil {
		p.healthy = false
		return fmt.Errorf("failed to get deployment %s/%s: %w", key.Namespace, key.Name, err)
	}

	bundle.CurrentReplicas = deploy.Status.Replicas
	bundle.ReadyReplicas = deploy.Status.ReadyReplicas
	bundle.DeploymentReady = deploy.Status.ReadyReplicas == deploy.Status.Replicas && deploy.Status.Replicas > 0

	cpuPct, memPct, err := p.podUtilization(ctx, deploy)
	if err != nil {
		// Non-fatal: metrics-server might be unavailable
		bundle.CPUUtilization = 0
		bundle.MemoryUtilization = 0
		p.healthy = false
		return nil
	}
	bundle.CPUUtilization = cpuPct
	bundle.MemoryUtilization = memPct
	p.healthy = true
	return nil
}

func (p *metricsServerPlugin) podUtilization(ctx context.Context, deploy *appsv1.Deployment) (cpuPct, memPct float64, err error) {
	sel, err := labels.ValidatedSelectorFromSet(deploy.Spec.Selector.MatchLabels)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid deployment selector: %w", err)
	}

	podMetricsList, err := p.metricsClient.MetricsV1beta1().PodMetricses(deploy.Namespace).
		List(ctx, v1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list pod metrics: %w", err)
	}
	if len(podMetricsList.Items) == 0 {
		return 0, 0, nil
	}

	var totalCPUMillis, totalMemBytes int64
	for _, container := range deploy.Spec.Template.Spec.Containers {
		if req, ok := container.Resources.Requests[corev1.ResourceCPU]; ok {
			totalCPUMillis += req.MilliValue()
		}
		if req, ok := container.Resources.Requests[corev1.ResourceMemory]; ok {
			totalMemBytes += req.Value()
		}
	}

	var sumCPUMillis, sumMemBytes float64
	for _, item := range podMetricsList.Items {
		for _, container := range item.Containers {
			sumCPUMillis += float64(container.Usage.Cpu().MilliValue())
			sumMemBytes += float64(container.Usage.Memory().Value())
		}
	}

	n := float64(len(podMetricsList.Items))
	if totalCPUMillis > 0 {
		cpuPct = (sumCPUMillis / n) / float64(totalCPUMillis) * 100.0
	}
	if totalMemBytes > 0 {
		memPct = (sumMemBytes / n) / float64(totalMemBytes) * 100.0
	}
	return cpuPct, memPct, nil
}
