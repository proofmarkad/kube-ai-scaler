package llm

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"time"
)

// Cache provides a TTL-based cache for LLM responses.
type Cache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	ttl     time.Duration
}

type cacheEntry struct {
	decision  *ScalingDecision
	provider  string
	createdAt time.Time
}

// NewCache creates a new response cache.
func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		entries: make(map[string]*cacheEntry),
		ttl:     ttl,
	}
}

// Get retrieves a cached decision if available and not expired.
func (c *Cache) Get(key string) (*ScalingDecision, string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[key]
	if !ok {
		return nil, "", false
	}
	if time.Since(entry.createdAt) > c.ttl {
		return nil, "", false
	}
	return entry.decision, entry.provider, true
}

// Set stores a decision in the cache.
func (c *Cache) Set(key string, decision *ScalingDecision, provider string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict expired entries periodically (simple approach)
	if len(c.entries) > 100 {
		c.evictExpired()
	}

	c.entries[key] = &cacheEntry{
		decision:  decision,
		provider:  provider,
		createdAt: time.Now(),
	}
}

// BuildKey generates a cache key from request metrics.
func BuildKey(req *ScalingRequest) string {
	data := fmt.Sprintf("%s:%d:%.1f:%.1f:%.1f:%.2f:%v",
		req.PolicyName,
		req.CurrentReplicas,
		req.CPUUtilization,
		req.MemoryUtilization,
		req.P95LatencyMs,
		req.ErrorRate,
		req.DeploymentReady,
	)
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash[:16])
}

func (c *Cache) evictExpired() {
	for k, v := range c.entries {
		if time.Since(v.createdAt) > c.ttl {
			delete(c.entries, k)
		}
	}
}
