package decision

import (
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

// ReactiveResult holds the outcome of evaluating reactive rules.
type ReactiveResult struct {
	Fired   bool
	Rule    plugin.ReactiveRule
	Actual  float64
	Replicas int32
}

// EvaluateReactiveRules checks annotation-based reactive rules against the signal bundle.
// If a rule fires, it returns immediately with the action to take, skipping the LLM.
// Rules are evaluated in order; first match wins.
func EvaluateReactiveRules(rules []plugin.ReactiveRule, bundle *plugin.Bundle) *ReactiveResult {
	for _, rule := range rules {
		actual := resolveMetric(rule.Metric, bundle)
		if !evalCondition(actual, rule.Operator, rule.Threshold) {
			continue
		}

		replicas := computeReactiveReplicas(rule, bundle.CurrentReplicas)
		return &ReactiveResult{
			Fired:    true,
			Rule:     rule,
			Actual:   actual,
			Replicas: replicas,
		}
	}
	return nil
}

// evalCondition evaluates "actual <op> threshold".
func evalCondition(actual float64, op string, threshold float64) bool {
	switch op {
	case ">":
		return actual > threshold
	case ">=":
		return actual >= threshold
	case "<":
		return actual < threshold
	case "<=":
		return actual <= threshold
	case "==":
		return actual == threshold
	case "!=":
		return actual != threshold
	default:
		return false
	}
}

// computeReactiveReplicas applies the rule's action to compute new replica count.
func computeReactiveReplicas(rule plugin.ReactiveRule, current int32) int32 {
	switch rule.Action {
	case "scale_up":
		return current + rule.Amount
	case "scale_down":
		result := current - rule.Amount
		if result < 1 {
			return 1
		}
		return result
	case "set":
		return rule.Amount
	default:
		return current
	}
}
