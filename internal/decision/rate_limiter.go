package decision

import (
	"sync"
	"time"
)

// RateLimiter implements a token bucket rate limiter for LLM API calls.
type RateLimiter struct {
	mu               sync.Mutex
	maxPerMinute     int32
	maxPerHour       int32
	maxConcurrent    int32
	minuteTokens     int32
	hourTokens       int32
	concurrent       int32
	lastMinuteReset  time.Time
	lastHourReset    time.Time
}

// NewRateLimiter creates a rate limiter with the specified limits.
func NewRateLimiter(maxPerMinute, maxPerHour, maxConcurrent int32) *RateLimiter {
	now := time.Now()
	return &RateLimiter{
		maxPerMinute:    maxPerMinute,
		maxPerHour:      maxPerHour,
		maxConcurrent:   maxConcurrent,
		minuteTokens:    maxPerMinute,
		hourTokens:      maxPerHour,
		lastMinuteReset: now,
		lastHourReset:   now,
	}
}

// Allow checks if a call is permitted under the rate limits.
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()

	// Reset minute bucket
	if now.Sub(rl.lastMinuteReset) >= time.Minute {
		rl.minuteTokens = rl.maxPerMinute
		rl.lastMinuteReset = now
	}

	// Reset hour bucket
	if now.Sub(rl.lastHourReset) >= time.Hour {
		rl.hourTokens = rl.maxPerHour
		rl.lastHourReset = now
	}

	// Check concurrent limit
	if rl.maxConcurrent > 0 && rl.concurrent >= rl.maxConcurrent {
		return false
	}

	// Check minute limit
	if rl.maxPerMinute > 0 && rl.minuteTokens <= 0 {
		return false
	}

	// Check hour limit
	if rl.maxPerHour > 0 && rl.hourTokens <= 0 {
		return false
	}

	// Consume tokens
	if rl.maxPerMinute > 0 {
		rl.minuteTokens--
	}
	if rl.maxPerHour > 0 {
		rl.hourTokens--
	}
	rl.concurrent++

	return true
}

// Release marks a concurrent call as complete.
func (rl *RateLimiter) Release() {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.concurrent > 0 {
		rl.concurrent--
	}
}
