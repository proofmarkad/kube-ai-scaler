package cost

import "fmt"

// BudgetEnforcer checks scaling decisions against cost budgets.
type BudgetEnforcer struct{}

// NewBudgetEnforcer creates a new budget enforcer.
func NewBudgetEnforcer() *BudgetEnforcer {
	return &BudgetEnforcer{}
}

// BudgetCheckResult holds the result of a budget check.
type BudgetCheckResult struct {
	Allowed   bool
	Reason    string
	Remaining float64
}

// Check evaluates whether a proposed cost change is within budget.
func (b *BudgetEnforcer) Check(
	maxHourly float64,
	maxMonthly float64,
	enforcement string,
	estimate *CostEstimate,
) *BudgetCheckResult {
	// Check hourly budget
	if maxHourly > 0 && estimate.ProposedHourlyCost > maxHourly {
		msg := fmt.Sprintf("proposed hourly cost $%.2f exceeds budget $%.2f", estimate.ProposedHourlyCost, maxHourly)
		if enforcement == "hard" {
			return &BudgetCheckResult{Allowed: false, Reason: msg, Remaining: maxHourly - estimate.ProposedHourlyCost}
		}
		// soft enforcement — allow but warn
		return &BudgetCheckResult{Allowed: true, Reason: "WARNING: " + msg, Remaining: maxHourly - estimate.ProposedHourlyCost}
	}

	// Check monthly budget
	if maxMonthly > 0 {
		projectedMonthly := estimate.ProposedHourlyCost * 24 * 30
		if projectedMonthly > maxMonthly {
			msg := fmt.Sprintf("projected monthly cost $%.2f exceeds budget $%.2f", projectedMonthly, maxMonthly)
			if enforcement == "hard" {
				return &BudgetCheckResult{Allowed: false, Reason: msg, Remaining: maxMonthly - projectedMonthly}
			}
			return &BudgetCheckResult{Allowed: true, Reason: "WARNING: " + msg, Remaining: maxMonthly - projectedMonthly}
		}
	}

	return &BudgetCheckResult{Allowed: true, Remaining: maxHourly - estimate.ProposedHourlyCost}
}
