package llm

import (
	"sync"
	"time"
)

// CircuitBreaker implements a standard circuit breaker pattern.
// States: closed → open (after N failures) → half-open (after timeout) → closed/open.
type CircuitBreaker struct {
	mu           sync.Mutex
	failureCount int
	lastFailure  time.Time
	threshold    int
	resetTimeout time.Duration
	state        string // "closed", "open", "half-open"
}

// NewCircuitBreaker creates a circuit breaker with the given threshold and reset timeout.
func NewCircuitBreaker(threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		resetTimeout: resetTimeout,
		state:        "closed",
	}
}

// Allow returns true if the circuit breaker allows a request.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case "closed":
		return true
	case "open":
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = "half-open"
			return true
		}
		return false
	case "half-open":
		return true
	default:
		return true
	}
}

// RecordSuccess resets the circuit breaker to closed state.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount = 0
	cb.state = "closed"
}

// RecordFailure records a failure. Opens the circuit if threshold reached.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount++
	cb.lastFailure = time.Now()
	if cb.failureCount >= cb.threshold {
		cb.state = "open"
	}
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	// Check if open should transition to half-open
	if cb.state == "open" && time.Since(cb.lastFailure) > cb.resetTimeout {
		cb.state = "half-open"
	}
	return cb.state
}
