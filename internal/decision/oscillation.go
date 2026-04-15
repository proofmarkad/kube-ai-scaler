package decision

import (
	"sync"
	"time"
)

// ScalingEvent records a single scaling action for oscillation detection.
type ScalingEvent struct {
	Timestamp time.Time
	Direction string // "up" or "down"
	From      int32
	To        int32
}

// OscillationDetector detects rapid up-down-up-down scaling patterns.
type OscillationDetector struct {
	mu       sync.Mutex
	events   []ScalingEvent
	maxSize  int
	window   time.Duration
}

// NewOscillationDetector creates a detector with a sliding window.
func NewOscillationDetector(window time.Duration, maxSize int) *OscillationDetector {
	return &OscillationDetector{
		events:  make([]ScalingEvent, 0, maxSize),
		maxSize: maxSize,
		window:  window,
	}
}

// Record adds a new scaling event.
func (o *OscillationDetector) Record(event ScalingEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.events) >= o.maxSize {
		o.events = o.events[1:]
	}
	o.events = append(o.events, event)
}

// IsOscillating returns true if there's a rapid up-down pattern within the window.
// An oscillation is 3+ direction changes in the window.
func (o *OscillationDetector) IsOscillating() bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	cutoff := time.Now().Add(-o.window)
	var recent []ScalingEvent
	for _, e := range o.events {
		if e.Timestamp.After(cutoff) {
			recent = append(recent, e)
		}
	}

	if len(recent) < 3 {
		return false
	}

	// Count direction changes
	changes := 0
	for i := 1; i < len(recent); i++ {
		if recent[i].Direction != recent[i-1].Direction {
			changes++
		}
	}

	return changes >= 2
}

// RecentEvents returns events within the detection window.
func (o *OscillationDetector) RecentEvents() []ScalingEvent {
	o.mu.Lock()
	defer o.mu.Unlock()

	cutoff := time.Now().Add(-o.window)
	var recent []ScalingEvent
	for _, e := range o.events {
		if e.Timestamp.After(cutoff) {
			recent = append(recent, e)
		}
	}
	return recent
}
