package cost

import (
	"testing"
	"time"
)

func TestEstimator_ScaleUp(t *testing.T) {
	e := NewEstimator()
	wc := &WorkloadCost{TotalCost: 1.0, TotalEfficiency: 0.7}
	est, err := e.Estimate(wc, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if est.CostPerReplica != 0.5 {
		t.Errorf("expected cost per replica 0.5, got %f", est.CostPerReplica)
	}
	if est.ProposedHourlyCost != 2.0 {
		t.Errorf("expected proposed hourly 2.0, got %f", est.ProposedHourlyCost)
	}
	if est.DeltaHourlyCost != 1.0 {
		t.Errorf("expected delta hourly 1.0, got %f", est.DeltaHourlyCost)
	}
}

func TestEstimator_ScaleDown(t *testing.T) {
	e := NewEstimator()
	wc := &WorkloadCost{TotalCost: 2.0, TotalEfficiency: 0.5}
	est, err := e.Estimate(wc, 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if est.DeltaHourlyCost >= 0 {
		t.Errorf("expected negative delta for scale-down, got %f", est.DeltaHourlyCost)
	}
	if est.WasteReduction <= 0 {
		t.Errorf("expected positive waste reduction, got %f", est.WasteReduction)
	}
}

func TestEstimator_InvalidReplicas(t *testing.T) {
	e := NewEstimator()
	wc := &WorkloadCost{TotalCost: 1.0}
	_, err := e.Estimate(wc, 0, 2)
	if err == nil {
		t.Error("expected error for zero current replicas")
	}
}

func TestBudgetEnforcer_WithinBudget(t *testing.T) {
	b := NewBudgetEnforcer()
	est := &CostEstimate{ProposedHourlyCost: 5.0}
	result := b.Check(10.0, 0, "hard", est)
	if !result.Allowed {
		t.Error("expected allowed within budget")
	}
}

func TestBudgetEnforcer_HardExceeded(t *testing.T) {
	b := NewBudgetEnforcer()
	est := &CostEstimate{ProposedHourlyCost: 15.0}
	result := b.Check(10.0, 0, "hard", est)
	if result.Allowed {
		t.Error("expected denied for hard enforcement over budget")
	}
}

func TestBudgetEnforcer_SoftExceeded(t *testing.T) {
	b := NewBudgetEnforcer()
	est := &CostEstimate{ProposedHourlyCost: 15.0}
	result := b.Check(10.0, 0, "soft", est)
	if !result.Allowed {
		t.Error("expected allowed for soft enforcement (with warning)")
	}
}

func TestBudgetEnforcer_MonthlyExceeded(t *testing.T) {
	b := NewBudgetEnforcer()
	est := &CostEstimate{ProposedHourlyCost: 100.0} // 100 * 24 * 30 = $72000
	result := b.Check(0, 1000.0, "hard", est)
	if result.Allowed {
		t.Error("expected denied — projected monthly exceeds budget")
	}
}

func TestTrackerZeroMaxSizeDoesNotPanic(t *testing.T) {
	tracker := NewTracker(0)
	tracker.Record(SavingsRecord{
		Timestamp:        time.Now(),
		Workload:         "api",
		DeltaMonthly:     -25,
		PreviousReplicas: 4,
		NewReplicas:      2,
	})

	summary := tracker.Summary()
	if summary.TotalEvents != 1 {
		t.Fatalf("expected 1 event, got %d", summary.TotalEvents)
	}
	if summary.TotalSaved != 25 {
		t.Fatalf("expected total saved 25, got %f", summary.TotalSaved)
	}
}
