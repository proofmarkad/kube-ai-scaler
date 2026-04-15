package signals

import (
	"context"
	"fmt"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// metricsCollector reads Deployment health and replica counts.
// CPU/memory utilization will be added once we wire the metrics-server client.
type metricsCollector struct {
	client        client.Client
	metricsClient metricsclient.Interface
}

func (m *metricsCollector) collect(
	ctx context.Context,
	policy *aiscalerv1.AIScaler,
	bundle *Bundle) error {

	deploy := &appsv1.Deployment{}
	key := types.NamespacedName{
		Namespace: policy.Spec.TargetRef.Namespace,
		Name:      policy.Spec.TargetRef.Name,
	}
	if err := m.client.Get(ctx, key, deploy); err != nil {
		return fmt.Errorf(
			"failed to get deployment %s/%s: %w",
			policy.Spec.TargetRef.Namespace,
			policy.Spec.TargetRef.Name,
			err,
		)
	}

	bundle.CurrentReplicas = deploy.Status.Replicas
	bundle.ReadyReplicas = deploy.Status.ReadyReplicas
	bundle.DeploymentReady =
		deploy.Status.ReadyReplicas == deploy.Status.Replicas && deploy.Status.Replicas > 0

	// Collect CPU/memory via metrics-server
	cpuUtil, memUtil, err := m.podUtilization(ctx, deploy)
	if err != nil {
		// Non-fatal — operator still functions, LLM gets 0 for these signals
		// and the prompt makes clear they are unavailable
		bundle.CPUUtilization = 0
		bundle.MemoryUtilization = 0
		return nil // non-fatal: metrics-server may be unavailable
	}

	bundle.CPUUtilization = cpuUtil
	bundle.MemoryUtilization = memUtil

	return nil
}

// podUtilization computes average CPU and memory utilization across all pods
// matched by the deployment's selector, as a percentage of their requests.
func (m *metricsCollector) podUtilization(
	ctx context.Context,
	deploy *appsv1.Deployment,
) (cpuPct, memPct float64, err error) {

	sel, err := labels.ValidatedSelectorFromSet(deploy.Spec.Selector.MatchLabels)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid deployment selector: %w", err)
	}

	podMetricsList, err := m.metricsClient.
		MetricsV1beta1().
		PodMetricses(deploy.Namespace).
		List(ctx, v1.ListOptions{LabelSelector: sel.String()})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list pod metrics: %w", err)
	}

	if len(podMetricsList.Items) == 0 {
		return 0, 0, nil
	}

	// Sum resource requests from the pod template spec for % calculation
	var totalCPUReq, totalMemReq resource.Quantity

	for _, container := range deploy.Spec.Template.Spec.Containers {
		if req, ok := container.Resources.Requests[corev1.ResourceCPU]; ok {
			totalCPUReq.Add(req)
		}

		if req, ok := container.Resources.Requests[corev1.ResourceMemory]; ok {
			totalMemReq.Add(req)
		}
	}

	var sumCPU, sumMem float64

	for _, item := range podMetricsList.Items {
		for _, container := range item.Containers {
			sumCPU += float64(container.Usage.Cpu().MilliValue())
			sumMem += float64(container.Usage.Memory().Value())
		}
	}

	n := float64(len(podMetricsList.Items))

	avgCpu := sumCPU / n
	avgMem := sumMem / n

	cpuReqMilli := float64(totalCPUReq.MilliValue())
	memReqBytes := float64(totalMemReq.Value())

	if cpuReqMilli > 0 {
		cpuPct = avgCpu / cpuReqMilli * 100.0
	}

	if memReqBytes > 0 {
		memPct = avgMem / memReqBytes * 100
	}

	return cpuPct, memPct, nil

}
