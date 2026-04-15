package llm

import (
	"testing"
	"time"
)

func TestCircuitBreaker_StartsCloseed(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)
	if cb.State() != "closed" {
		t.Errorf("expected closed, got %q", cb.State())
	}
	if !cb.Allow() {
		t.Error("expected Allow() to return true when closed")
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != "closed" {
		t.Error("should still be closed after 2 failures")
	}

	cb.RecordFailure() // hits threshold
	if cb.State() != "open" {
		t.Errorf("expected open after 3 failures, got %q", cb.State())
	}
	if cb.Allow() {
		t.Error("expected Allow() to return false when open")
	}
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)

	cb.RecordFailure()
	if cb.State() != "open" {
		t.Fatal("expected open")
	}

	time.Sleep(60 * time.Millisecond)
	if cb.State() != "half-open" {
		t.Errorf("expected half-open after timeout, got %q", cb.State())
	}
	if !cb.Allow() {
		t.Error("expected Allow() to return true when half-open")
	}
}

func TestCircuitBreaker_ClosesOnSuccess(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)

	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond) // now half-open

	cb.RecordSuccess()
	if cb.State() != "closed" {
		t.Errorf("expected closed after success in half-open, got %q", cb.State())
	}
}

func TestCircuitBreaker_ReopensOnFailureInHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(1, 50*time.Millisecond)

	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond) // now half-open

	cb.RecordFailure()
	if cb.State() != "open" {
		t.Errorf("expected open after failure in half-open, got %q", cb.State())
	}
}

func TestCircuitBreaker_SuccessResetsClosed(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)

	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess() // should reset failure count

	cb.RecordFailure() // only 1 failure now
	if cb.State() != "closed" {
		t.Error("expected closed after success reset")
	}
}
