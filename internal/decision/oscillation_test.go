package decision

import (
	"testing"
	"time"
)

func TestOscillationDetector_NoEvents(t *testing.T) {
	d := NewOscillationDetector(30*time.Minute, 100)
	if d.IsOscillating() {
		t.Error("expected no oscillation with zero events")
	}
}

func TestOscillationDetector_SingleDirection(t *testing.T) {
	d := NewOscillationDetector(30*time.Minute, 100)
	for i := 0; i < 5; i++ {
		d.Record(ScalingEvent{
			Timestamp: time.Now(),
			Direction: "up",
			From:      int32(3 + i),
			To:        int32(4 + i),
		})
	}
	if d.IsOscillating() {
		t.Error("expected no oscillation with only scale-up events")
	}
}

func TestOscillationDetector_Oscillating(t *testing.T) {
	d := NewOscillationDetector(30*time.Minute, 100)
	directions := []string{"up", "down", "up", "down", "up"}
	for i, dir := range directions {
		d.Record(ScalingEvent{
			Timestamp: time.Now(),
			Direction: dir,
			From:      int32(3 + i%2),
			To:        int32(4 - i%2),
		})
	}
	if !d.IsOscillating() {
		t.Error("expected oscillation with alternating up/down events")
	}
}

func TestOscillationDetector_OldEventsExpire(t *testing.T) {
	d := NewOscillationDetector(1*time.Second, 100)
	directions := []string{"up", "down", "up", "down"}
	for _, dir := range directions {
		d.Record(ScalingEvent{
			Timestamp: time.Now().Add(-2 * time.Second),
			Direction: dir,
			From:      3,
			To:        5,
		})
	}
	if d.IsOscillating() {
		t.Error("expected no oscillation — events should have expired")
	}
}

func TestOscillationDetector_MaxSize(t *testing.T) {
	d := NewOscillationDetector(30*time.Minute, 3)
	for i := 0; i < 10; i++ {
		d.Record(ScalingEvent{
			Timestamp: time.Now(),
			Direction: "up",
			From:      int32(i),
			To:        int32(i + 1),
		})
	}
	d.mu.Lock()
	count := len(d.events)
	d.mu.Unlock()
	if count > 3 {
		t.Errorf("expected max 3 events, got %d", count)
	}
}
