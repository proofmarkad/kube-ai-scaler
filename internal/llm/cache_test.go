package llm

import (
	"testing"
	"time"
)

func TestCache_SetAndGet(t *testing.T) {
	c := NewCache(5 * time.Minute)

	decision := &ScalingDecision{TargetReplicas: 5, Reasoning: "test", Confidence: 0.9}
	c.Set("key1", decision, "openai")

	got, provider, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got.TargetReplicas != 5 {
		t.Errorf("expected 5 replicas, got %d", got.TargetReplicas)
	}
	if provider != "openai" {
		t.Errorf("expected provider openai, got %q", provider)
	}
}

func TestCache_Miss(t *testing.T) {
	c := NewCache(5 * time.Minute)

	_, _, ok := c.Get("nonexistent")
	if ok {
		t.Error("expected cache miss")
	}
}

func TestCache_Expiry(t *testing.T) {
	c := NewCache(50 * time.Millisecond)

	decision := &ScalingDecision{TargetReplicas: 5}
	c.Set("key1", decision, "openai")

	time.Sleep(60 * time.Millisecond)

	_, _, ok := c.Get("key1")
	if ok {
		t.Error("expected cache miss after TTL expiry")
	}
}

func TestBuildKey_Deterministic(t *testing.T) {
	req := &ScalingRequest{
		PolicyName:      "test",
		CurrentReplicas: 3,
		CPUUtilization:  75.0,
	}

	key1 := BuildKey(req)
	key2 := BuildKey(req)

	if key1 != key2 {
		t.Error("expected BuildKey to be deterministic")
	}
}

func TestBuildKey_DifferentInputs(t *testing.T) {
	req1 := &ScalingRequest{PolicyName: "a", CurrentReplicas: 3}
	req2 := &ScalingRequest{PolicyName: "b", CurrentReplicas: 3}

	if BuildKey(req1) == BuildKey(req2) {
		t.Error("expected different keys for different inputs")
	}
}
