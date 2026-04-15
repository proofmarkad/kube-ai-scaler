package prediction

import (
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

// EventPredictor checks if any configured predictive events are upcoming.
type EventPredictor struct{}

// NewEventPredictor creates a new event predictor.
func NewEventPredictor() *EventPredictor { return &EventPredictor{} }

// UpcomingEvent represents an event that requires pre-scaling.
type UpcomingEvent struct {
	Name                   string
	StartsIn               time.Duration
	ExpectedLoadMultiplier float64
}

// Check returns upcoming events within the lookahead window.
func (e *EventPredictor) Check(events []aiscalerv1.PredictiveEvent, lookahead time.Duration) []UpcomingEvent {
	now := time.Now()
	var upcoming []UpcomingEvent

	for _, ev := range events {
		preScale := time.Duration(ev.PreScaleMinutes) * time.Minute
		effectiveStart := ev.Start.Time.Add(-preScale)

		// Event is upcoming if its effective start is within lookahead
		if effectiveStart.After(now) && effectiveStart.Before(now.Add(lookahead)) {
			upcoming = append(upcoming, UpcomingEvent{
				Name:                   ev.Name,
				StartsIn:               effectiveStart.Sub(now),
				ExpectedLoadMultiplier: ev.ExpectedLoadMultiplier,
			})
		}

		// Event is currently active
		if now.After(ev.Start.Time) && now.Before(ev.End.Time) {
			upcoming = append(upcoming, UpcomingEvent{
				Name:                   ev.Name + " (ACTIVE)",
				StartsIn:               0,
				ExpectedLoadMultiplier: ev.ExpectedLoadMultiplier,
			})
		}
	}

	return upcoming
}
