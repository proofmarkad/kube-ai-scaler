package node

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NodeContext aggregates cluster node/pool information.
type NodeContext struct {
	TotalNodes          int32
	ReadyNodes          int32
	ClusterCPURequested float64
	ClusterMemRequested float64
	ClusterCPUCapacity  float64
	ClusterMemCapacity  float64
	PendingPods         int32
	NodePools           []NodePoolInfo
}

// NodePoolInfo describes a node pool.
type NodePoolInfo struct {
	Name           string
	InstanceType   string
	CurrentNodes   int32
	CPUAllocatable float64
	MemAllocatable float64
	IsSpot         bool
	CostPerHour    float64
}

// Collector gathers node-level context from the Kubernetes API.
type Collector struct {
	client client.Client
}

// NewCollector creates a new node collector.
func NewCollector(c client.Client) *Collector {
	return &Collector{client: c}
}

// Collect gathers cluster node context.
func (c *Collector) Collect(ctx context.Context) (*NodeContext, error) {
	nodeList := &corev1.NodeList{}
	if err := c.client.List(ctx, nodeList); err != nil {
		return nil, err
	}

	nc := &NodeContext{
		TotalNodes: int32(len(nodeList.Items)),
	}

	poolMap := make(map[string]*NodePoolInfo)

	for _, node := range nodeList.Items {
		// Count ready nodes
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
				nc.ReadyNodes++
				break
			}
		}

		// Aggregate capacity
		cpu := node.Status.Allocatable.Cpu()
		mem := node.Status.Allocatable.Memory()
		if cpu != nil {
			nc.ClusterCPUCapacity += cpu.AsApproximateFloat64()
		}
		if mem != nil {
			nc.ClusterMemCapacity += mem.AsApproximateFloat64()
		}

		// Pool detection (common labels)
		poolName := "default"
		if v, ok := node.Labels["node.kubernetes.io/instance-type"]; ok {
			poolName = v
		}
		if v, ok := node.Labels["cloud.google.com/gke-nodepool"]; ok {
			poolName = v
		}
		if v, ok := node.Labels["eks.amazonaws.com/nodegroup"]; ok {
			poolName = v
		}

		pool, exists := poolMap[poolName]
		if !exists {
			pool = &NodePoolInfo{
				Name:         poolName,
				InstanceType: node.Labels["node.kubernetes.io/instance-type"],
			}
			// Detect spot instances
			if _, ok := node.Labels["cloud.google.com/gke-spot"]; ok {
				pool.IsSpot = true
			}
			if v, ok := node.Labels["eks.amazonaws.com/capacityType"]; ok && v == "SPOT" {
				pool.IsSpot = true
			}
			poolMap[poolName] = pool
		}
		pool.CurrentNodes++
		if cpu != nil {
			pool.CPUAllocatable += cpu.AsApproximateFloat64()
		}
		if mem != nil {
			pool.MemAllocatable += mem.AsApproximateFloat64()
		}
	}

	for _, pool := range poolMap {
		nc.NodePools = append(nc.NodePools, *pool)
	}

	// Count pending pods
	podList := &corev1.PodList{}
	if err := c.client.List(ctx, podList); err == nil {
		for _, pod := range podList.Items {
			if pod.Status.Phase == corev1.PodPending {
				nc.PendingPods++
				continue
			}
			if pod.Spec.NodeName == "" || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				continue
			}

			for _, container := range pod.Spec.Containers {
				if cpu := container.Resources.Requests.Cpu(); cpu != nil {
					nc.ClusterCPURequested += cpu.AsApproximateFloat64()
				}
				if mem := container.Resources.Requests.Memory(); mem != nil {
					nc.ClusterMemRequested += mem.AsApproximateFloat64()
				}
			}
		}
	}

	return nc, nil
}
