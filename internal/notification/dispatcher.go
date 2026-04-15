package notification

import (
	"sync"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Notifier is the interface for sending notifications.
type Notifier interface {
	Send(event Event) error
	Type() string
}

// Dispatcher routes events to configured notification channels.
type Dispatcher struct {
	mu        sync.RWMutex
	notifiers []Notifier
	filters   map[EventType][]Notifier
}

// NewDispatcher creates a dispatcher with the given notifiers.
func NewDispatcher(notifiers ...Notifier) *Dispatcher {
	return &Dispatcher{
		notifiers: notifiers,
		filters:   make(map[EventType][]Notifier),
	}
}

// RegisterFilter routes specific event types to specific notifiers.
func (d *Dispatcher) RegisterFilter(eventType EventType, notifier Notifier) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.filters[eventType] = append(d.filters[eventType], notifier)
}

// Dispatch sends an event to all matching notifiers.
func (d *Dispatcher) Dispatch(event Event) {
	log := logf.Log.WithName("notification")

	d.mu.RLock()
	defer d.mu.RUnlock()

	// Check if there are specific notifiers for this event type
	notifiers, hasFilter := d.filters[event.Type]
	if !hasFilter {
		notifiers = d.notifiers // broadcast to all
	}

	for _, n := range notifiers {
		if err := n.Send(event); err != nil {
			log.Error(err, "failed to send notification",
				"type", event.Type, "notifier", n.Type())
		}
	}
}
