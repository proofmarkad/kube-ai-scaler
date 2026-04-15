package coordinator

import (
	"fmt"
	"sync"
	"time"
)

// ClusterCoordinator manages cluster-wide scaling budgets and
// prevents too many workloads from scaling simultaneously.
type ClusterCoordinator struct {
	mu            sync.Mutex
	activeOps     map[string]time.Time
	totalBudget   float64
	currentSpend  float64
	maxConcurrent int
}

// NewClusterCoordinator creates a coordinator with the specified limits.
func NewClusterCoordinator(maxConcurrent int, totalBudget float64) *ClusterCoordinator {
	return &ClusterCoordinator{
		activeOps:     make(map[string]time.Time),
		maxConcurrent: maxConcurrent,
		totalBudget:   totalBudget,
	}
}

// AcquireScalingSlot attempts to acquire a scaling slot for a workload.
// Returns an error if the cluster-wide concurrency limit is exceeded.
func (c *ClusterCoordinator) AcquireScalingSlot(workload string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked()

	if _, exists := c.activeOps[workload]; exists {
		c.activeOps[workload] = time.Now()
		return nil
	}

	if c.maxConcurrent > 0 && len(c.activeOps) >= c.maxConcurrent {
		return fmt.Errorf("max concurrent scaling operations reached (%d), deferring %s", c.maxConcurrent, workload)
	}

	c.activeOps[workload] = time.Now()
	return nil
}

// ReleaseScalingSlot releases a scaling slot.
func (c *ClusterCoordinator) ReleaseScalingSlot(workload string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.activeOps, workload)
}

// ActiveOperations returns the count of active scaling operations.
func (c *ClusterCoordinator) ActiveOperations() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked()
	return len(c.activeOps)
}

// ActiveWorkloads returns a snapshot of workloads currently performing scaling operations.
func (c *ClusterCoordinator) ActiveWorkloads() map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanupLocked()

	active := make(map[string]bool, len(c.activeOps))
	for workload := range c.activeOps {
		active[workload] = true
	}
	return active
}

// CheckBudget verifies that the proposed cost delta is within the cluster budget.
func (c *ClusterCoordinator) CheckBudget(deltaCost float64) (bool, float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.totalBudget <= 0 {
		return true, 0 // no budget limit
	}

	remaining := c.totalBudget - c.currentSpend
	if deltaCost > remaining {
		return false, remaining
	}
	return true, remaining - deltaCost
}

// RecordSpend records a cost expenditure against the cluster budget.
func (c *ClusterCoordinator) RecordSpend(amount float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.currentSpend += amount
}

func (c *ClusterCoordinator) cleanupLocked() {
	cutoff := time.Now().Add(-5 * time.Minute)
	for workload, startedAt := range c.activeOps {
		if startedAt.Before(cutoff) {
			delete(c.activeOps, workload)
		}
	}
}
