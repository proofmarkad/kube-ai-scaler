package notification

import "time"

// EventType categorizes notification events.
type EventType string

const (
	EventScalingApplied      EventType = "scaling_applied"
	EventSLOViolation        EventType = "slo_violation"
	EventCircuitBreakerTrip  EventType = "circuit_breaker_tripped"
	EventBudgetExceeded      EventType = "budget_exceeded"
	EventApprovalRequired    EventType = "approval_required"
	EventRollbackTriggered   EventType = "rollback_triggered"
	EventOscillationDetected EventType = "oscillation_detected"
	EventLLMFailure          EventType = "llm_failure"
)

// Severity levels.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Event describes a single notification event.
type Event struct {
	Type      EventType         `json:"type"`
	Severity  Severity          `json:"severity"`
	Workload  string            `json:"workload"`
	Namespace string            `json:"namespace"`
	Cluster   string            `json:"cluster,omitempty"`
	Message   string            `json:"message"`
	Details   map[string]string `json:"details,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}
