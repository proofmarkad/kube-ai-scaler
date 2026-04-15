package node

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

// FeasibilityResult holds the scheduling feasibility check result.
type FeasibilityResult struct {
	CanSchedule            bool
	Reason                 string
	RemainingCPU           float64
	RemainingMemory        float64
	ConsolidationPossible  bool
	ConsolidationNodeCount int32
}

// CanSchedule checks if the cluster can schedule additional pods with given resource requirements.
func CanSchedule(
	nc *NodeContext,
	additionalPods int32,
	cpuPerPod resource.Quantity,
	memPerPod resource.Quantity,
) *FeasibilityResult {
	cpuNeeded := cpuPerPod.AsApproximateFloat64() * float64(additionalPods)
	memNeeded := memPerPod.AsApproximateFloat64() * float64(additionalPods)

	remainingCPU := nc.ClusterCPUCapacity - nc.ClusterCPURequested
	remainingMem := nc.ClusterMemCapacity - nc.ClusterMemRequested

	result := &FeasibilityResult{
		RemainingCPU:    remainingCPU,
		RemainingMemory: remainingMem,
	}

	if cpuNeeded > remainingCPU {
		result.CanSchedule = false
		result.Reason = "insufficient cluster CPU capacity"
		return result
	}

	if memNeeded > remainingMem {
		result.CanSchedule = false
		result.Reason = "insufficient cluster memory capacity"
		return result
	}

	result.CanSchedule = true
	return result
}

// ConsolidationOpportunity checks if removing pods could free up entire nodes.
func ConsolidationOpportunity(
	nc *NodeContext,
	removedPods int32,
	cpuPerPod resource.Quantity,
	memPerPod resource.Quantity,
) *FeasibilityResult {
	cpuFreed := cpuPerPod.AsApproximateFloat64() * float64(removedPods)

	result := &FeasibilityResult{}

	// Check if freed resources could empty out a node
	for _, pool := range nc.NodePools {
		if pool.CurrentNodes < 2 {
			continue
		}
		avgCPUPerNode := pool.CPUAllocatable / float64(pool.CurrentNodes)
		if cpuFreed >= avgCPUPerNode {
			possibleNodes := int32(cpuFreed / avgCPUPerNode)
			result.ConsolidationPossible = true
			result.ConsolidationNodeCount = possibleNodes
			break
		}
	}

	return result
}
