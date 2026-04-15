package audit

import (
	"time"

	"github.com/sanjbh/kube-scaling-agent/internal/decision"
	"github.com/sanjbh/kube-scaling-agent/internal/llm"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

// DecisionRecord captures a complete audit trail for one scaling decision.
type DecisionRecord struct {
	// Unique identifier
	ID string `json:"id"`

	// Timing
	Timestamp time.Time `json:"timestamp"`

	// Workload identity
	Workload  string `json:"workload"`
	Namespace string `json:"namespace"`

	// Input signals
	Signals *plugin.Bundle `json:"signals,omitempty"`

	// LLM interaction
	PromptUsed   string                `json:"promptUsed,omitempty"`
	RawResponse  string                `json:"rawResponse,omitempty"`
	Provider     string                `json:"provider"`
	Model        string                `json:"model,omitempty"`
	LLMLatencyMs float64               `json:"llmLatencyMs"`

	// Decision
	ParsedDecision *llm.ScalingDecision `json:"parsedDecision,omitempty"`

	// Validation
	PreValidation  *decision.ValidationResult `json:"preValidation,omitempty"`
	PostValidation *decision.ValidationResult `json:"postValidation,omitempty"`
	WasClamped     bool                        `json:"wasClamped"`

	// State
	PreviousReplicas int32 `json:"previousReplicas"`
	NewReplicas      int32 `json:"newReplicas"`

	// Flags
	Applied bool `json:"applied"`
	DryRun  bool `json:"dryRun"`

	// Cost
	CostDeltaHourly  float64 `json:"costDeltaHourly,omitempty"`
	CostDeltaMonthly float64 `json:"costDeltaMonthly,omitempty"`
}
