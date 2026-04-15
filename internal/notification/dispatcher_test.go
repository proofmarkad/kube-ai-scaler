package notification

import (
	"sync"
	"testing"
	"time"
)

type mockNotifier struct {
	mu     sync.Mutex
	events []Event
}

func (m *mockNotifier) Send(event Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
	return nil
}

func (m *mockNotifier) Type() string { return "mock" }

func (m *mockNotifier) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

func TestDispatcher_BroadcastToAll(t *testing.T) {
	n1 := &mockNotifier{}
	n2 := &mockNotifier{}
	d := NewDispatcher(n1, n2)

	d.Dispatch(Event{
		Type:      EventScalingApplied,
		Severity:  SeverityInfo,
		Workload:  "web",
		Namespace: "prod",
		Message:   "scaled up",
		Timestamp: time.Now(),
	})

	if n1.count() != 1 {
		t.Errorf("expected n1 to receive 1 event, got %d", n1.count())
	}
	if n2.count() != 1 {
		t.Errorf("expected n2 to receive 1 event, got %d", n2.count())
	}
}

func TestDispatcher_FilteredRouting(t *testing.T) {
	nAll := &mockNotifier{}
	nCritical := &mockNotifier{}

	d := NewDispatcher(nAll)
	d.RegisterFilter(EventCircuitBreakerTrip, nCritical)

	// Circuit breaker event should go ONLY to nCritical (filter overrides broadcast)
	d.Dispatch(Event{
		Type:      EventCircuitBreakerTrip,
		Severity:  SeverityCritical,
		Workload:  "api",
		Namespace: "prod",
		Message:   "circuit breaker tripped",
		Timestamp: time.Now(),
	})

	if nCritical.count() != 1 {
		t.Errorf("expected nCritical to receive 1 event, got %d", nCritical.count())
	}
	if nAll.count() != 0 {
		t.Errorf("expected nAll to receive 0 events (filtered), got %d", nAll.count())
	}

	// Unfiltered event should broadcast to all
	d.Dispatch(Event{
		Type:      EventScalingApplied,
		Severity:  SeverityInfo,
		Workload:  "web",
		Namespace: "prod",
		Message:   "scaled",
		Timestamp: time.Now(),
	})

	if nAll.count() != 1 {
		t.Errorf("expected nAll to receive 1 broadcast event, got %d", nAll.count())
	}
}

func TestDispatcher_Empty(t *testing.T) {
	d := NewDispatcher()
	// Should not panic with no notifiers
	d.Dispatch(Event{
		Type:      EventScalingApplied,
		Severity:  SeverityInfo,
		Message:   "test",
		Timestamp: time.Now(),
	})
}
