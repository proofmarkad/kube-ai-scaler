package coordinator

import (
	"sync"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

// DependencyGraph tracks workload dependencies for coordinated scaling.
type DependencyGraph struct {
	mu    sync.RWMutex
	graph map[string]*WorkloadNode
}

// WorkloadNode represents a workload in the dependency graph.
type WorkloadNode struct {
	Name       string
	Upstream   []string // services this workload depends on
	Downstream []string // services that depend on this workload
	Coscales   []CoscaleEntry
}

// CoscaleEntry links a co-scaling workload with a ratio.
type CoscaleEntry struct {
	Workload string
	Ratio    float64
}

// NewDependencyGraph creates an empty dependency graph.
func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{
		graph: make(map[string]*WorkloadNode),
	}
}

// BuildFromPolicies populates the graph from AIScaler specs.
func (dg *DependencyGraph) BuildFromPolicies(policies []aiscalerv1.AIScaler) {
	dg.mu.Lock()
	defer dg.mu.Unlock()

	dg.graph = make(map[string]*WorkloadNode)

	for _, p := range policies {
		workloadName := p.Spec.TargetRef.Name
		if workloadName == "" {
			workloadName = p.Name
		}

		node := &WorkloadNode{Name: workloadName}

		if p.Spec.Dependencies != nil {
			for _, up := range p.Spec.Dependencies.UpstreamOf {
				node.Upstream = append(node.Upstream, up.Name)
			}
			for _, down := range p.Spec.Dependencies.DownstreamOf {
				node.Downstream = append(node.Downstream, down.Name)
			}
			for _, co := range p.Spec.Dependencies.CoscalesWith {
				node.Coscales = append(node.Coscales, CoscaleEntry{
					Workload: co.TargetRef.Name,
					Ratio:    co.Ratio,
				})
			}
		}

		dg.graph[workloadName] = node
	}
}

// ShouldDefer checks if a workload should defer scaling because
// an upstream or downstream dependency is currently scaling.
func (dg *DependencyGraph) ShouldDefer(workload string, activeScaling map[string]bool) bool {
	dg.mu.RLock()
	defer dg.mu.RUnlock()

	node, ok := dg.graph[workload]
	if !ok {
		return false
	}

	// Don't scale if an upstream dependency is being scaled
	for _, up := range node.Upstream {
		if activeScaling[up] {
			return true
		}
	}

	// Don't scale if a downstream dependency is being scaled
	for _, down := range node.Downstream {
		if activeScaling[down] {
			return true
		}
	}

	return false
}

// GetCoscaleTargets returns workloads that should co-scale with the given workload.
func (dg *DependencyGraph) GetCoscaleTargets(workload string) []CoscaleEntry {
	dg.mu.RLock()
	defer dg.mu.RUnlock()

	node, ok := dg.graph[workload]
	if !ok {
		return nil
	}
	return node.Coscales
}
