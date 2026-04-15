package prediction

import (
	"testing"
	"time"
)

func TestSeasonalPredictor_Empty(t *testing.T) {
	p := NewSeasonalPredictor()
	val, conf := p.Predict("cpu", 1*time.Hour)
	if val != 0 || conf != 0 {
		t.Errorf("expected (0, 0) for empty predictor, got (%f, %f)", val, conf)
	}
}

func TestSeasonalPredictor_UpdateAndPredict(t *testing.T) {
	p := NewSeasonalPredictor()

	// Feed several observations
	for i := 0; i < 10; i++ {
		p.UpdateBaseline("cpu", 75.0)
	}

	// Predict for the same hour slot (0 lookahead gives current slot)
	val, conf := p.Predict("cpu", 0)
	if val == 0 {
		t.Error("expected non-zero prediction after updates")
	}
	if conf == 0 {
		t.Error("expected non-zero confidence after updates")
	}
}

func TestSeasonalPredictor_SetBaselines(t *testing.T) {
	p := NewSeasonalPredictor()

	baselines := [168]float64{}
	for i := range baselines {
		baselines[i] = float64(i)
	}
	p.SetBaselines("test_metric", baselines)

	// Predict
	val, conf := p.Predict("test_metric", 0)
	if val == 0 && conf == 0 {
		t.Error("expected non-zero prediction after SetBaselines")
	}
}

func TestSeasonalPredictor_TimeToSlot(t *testing.T) {
	// Sunday 00:00 should be slot 0 (Weekday=0)
	sunday := time.Date(2025, 1, 5, 0, 0, 0, 0, time.UTC) // Jan 5 2025 is a Sunday
	slot := timeToSlot(sunday)
	if slot != 0 {
		t.Errorf("expected slot 0 for Sunday 00:00, got %d", slot)
	}

	// Monday 00:00 should be slot 24 (Weekday=1)
	monday := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)
	slotMon := timeToSlot(monday)
	if slotMon != 24 {
		t.Errorf("expected slot 24 for Monday 00:00, got %d", slotMon)
	}

	// Monday 23:00 should be slot 47
	monday23 := time.Date(2025, 1, 6, 23, 0, 0, 0, time.UTC)
	slot23 := timeToSlot(monday23)
	if slot23 != 47 {
		t.Errorf("expected slot 47 for Monday 23:00, got %d", slot23)
	}

	// Saturday 23:00 should be slot 167 (Weekday=6, 6*24+23=167)
	saturday := time.Date(2025, 1, 11, 23, 0, 0, 0, time.UTC) // Saturday
	slotSat := timeToSlot(saturday)
	if slotSat != 167 {
		t.Errorf("expected slot 167 for Saturday 23:00, got %d", slotSat)
	}
}
