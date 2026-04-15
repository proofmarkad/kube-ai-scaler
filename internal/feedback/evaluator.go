package feedback

import (
	"context"
	"time"

	"github.com/sanjbh/kube-scaling-agent/internal/audit"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

// DecisionOutcome evaluates whether a past decision was effective.
type DecisionOutcome struct {
	RecordID      string
	Effective     bool
	SLOsMetBefore bool
	SLOsMetAfter  bool
	CPUBefore     float64
	CPUAfter      float64
	LatencyBefore float64
	LatencyAfter  float64
	EvaluatedAt   time.Time
}

// OutcomeEvaluator evaluates past decisions after an observation window.
type OutcomeEvaluator struct {
	auditStore audit.Store
}

// NewOutcomeEvaluator creates an outcome evaluator.
func NewOutcomeEvaluator(store audit.Store) *OutcomeEvaluator {
	return &OutcomeEvaluator{
		auditStore: store,
	}
}

// Evaluate checks recent decisions and compares pre/post state.
func (oe *OutcomeEvaluator) Evaluate(
	ctx context.Context,
	workload string,
	currentBundle *plugin.Bundle,
	evaluationDelay time.Duration,
	limit int,
) ([]DecisionOutcome, error) {
	if currentBundle == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}

	records, err := oe.auditStore.List(ctx, workload, limit)
	if err != nil {
		return nil, err
	}

	var outcomes []DecisionOutcome
	now := time.Now()

	for _, rec := range records {
		// Only evaluate records older than the evaluation delay
		if now.Sub(rec.Timestamp) < evaluationDelay {
			continue
		}

		outcome := DecisionOutcome{
			RecordID:    rec.ID,
			EvaluatedAt: now,
		}

		// Compare signals from when the decision was made to current
		if rec.Signals != nil {
			outcome.CPUBefore = rec.Signals.CPUUtilization
			outcome.LatencyBefore = rec.Signals.P95LatencyMs
		}
		outcome.CPUAfter = currentBundle.CPUUtilization
		outcome.LatencyAfter = currentBundle.P95LatencyMs

		// Simple effectiveness heuristic:
		// If we scaled up and CPU/latency went down, it was effective
		// If we scaled down and metrics stayed acceptable, it was effective
		if rec.NewReplicas > rec.PreviousReplicas {
			outcome.Effective = outcome.CPUAfter <= outcome.CPUBefore || outcome.LatencyAfter <= outcome.LatencyBefore
		} else if rec.NewReplicas < rec.PreviousReplicas {
			outcome.Effective = outcome.CPUAfter < 80 && outcome.LatencyAfter < outcome.LatencyBefore*1.5
		} else {
			outcome.Effective = true // no change was made
		}

		outcomes = append(outcomes, outcome)
	}

	return outcomes, nil
}
