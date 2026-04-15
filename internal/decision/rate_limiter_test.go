package decision

import (
	"testing"
)

func TestRateLimiter_AllowWithinLimits(t *testing.T) {
	rl := NewRateLimiter(10, 100, 5)
	for i := 0; i < 5; i++ {
		if !rl.Allow() {
			t.Errorf("expected Allow() on call %d", i)
		}
	}
}

func TestRateLimiter_MaxConcurrent(t *testing.T) {
	rl := NewRateLimiter(100, 1000, 2)

	// Acquire 2 concurrent slots
	if !rl.Allow() {
		t.Error("expected first Allow()")
	}
	if !rl.Allow() {
		t.Error("expected second Allow()")
	}
	if rl.Allow() {
		t.Error("expected third Allow() to be denied — max concurrent reached")
	}

	// Release one
	rl.Release()
	if !rl.Allow() {
		t.Error("expected Allow() after Release()")
	}
}

func TestRateLimiter_PerMinuteLimit(t *testing.T) {
	rl := NewRateLimiter(3, 1000, 100)
	for i := 0; i < 3; i++ {
		if !rl.Allow() {
			t.Errorf("expected Allow() on call %d", i)
		}
		rl.Release()
	}
	// 4th should fail
	if rl.Allow() {
		t.Error("expected 4th call to be denied — per-minute limit reached")
	}
}
