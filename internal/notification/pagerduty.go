package notification

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// PagerDutyNotifier sends notifications to PagerDuty via Events API v2.
type PagerDutyNotifier struct {
	routingKey string
	client     *http.Client
}

// NewPagerDutyNotifier creates a PagerDuty notifier.
func NewPagerDutyNotifier(routingKey string) *PagerDutyNotifier {
	return &PagerDutyNotifier{
		routingKey: routingKey,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (p *PagerDutyNotifier) Type() string { return "pagerduty" }

type pdPayload struct {
	RoutingKey  string    `json:"routing_key"`
	EventAction string    `json:"event_action"`
	Payload     pdContent `json:"payload"`
}

type pdContent struct {
	Summary  string `json:"summary"`
	Source   string `json:"source"`
	Severity string `json:"severity"`
}

// Send triggers a PagerDuty event.
func (p *PagerDutyNotifier) Send(event Event) error {
	severity := "info"
	switch event.Severity {
	case SeverityWarning:
		severity = "warning"
	case SeverityCritical:
		severity = "critical"
	}

	payload := pdPayload{
		RoutingKey:  p.routingKey,
		EventAction: "trigger",
		Payload: pdContent{
			Summary:  fmt.Sprintf("[AIScaler] %s/%s: %s", event.Namespace, event.Workload, event.Message),
			Source:   fmt.Sprintf("aiscaler/%s", event.Workload),
			Severity: severity,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal pagerduty payload: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost,
		"https://events.pagerduty.com/v2/enqueue",
		bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build pagerduty request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("pagerduty request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("pagerduty returned %d", resp.StatusCode)
	}
	return nil
}
