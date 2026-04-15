package prediction

import (
	"math"
	"sync"
	"time"
)

// SeasonalPredictor models weekly seasonal patterns using a [168]float64 array (168 = 24h * 7 days).
type SeasonalPredictor struct {
	mu        sync.RWMutex
	baselines map[string][168]float64
	counts    map[string][168]int64
}

// NewSeasonalPredictor creates a predictor with empty baselines.
func NewSeasonalPredictor() *SeasonalPredictor {
	return &SeasonalPredictor{
		baselines: make(map[string][168]float64),
		counts:    make(map[string][168]int64),
	}
}

// UpdateBaseline feeds a new observation into the exponential moving average.
func (s *SeasonalPredictor) UpdateBaseline(metric string, value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	slot := timeToSlot(time.Now())
	bl := s.baselines[metric]
	cn := s.counts[metric]

	cn[slot]++
	alpha := 1.0 / math.Min(float64(cn[slot]), 100) // EMA smoothing
	bl[slot] = bl[slot]*(1-alpha) + value*alpha

	s.baselines[metric] = bl
	s.counts[metric] = cn
}

// Predict returns the predicted value at now + lookahead.
func (s *SeasonalPredictor) Predict(metric string, lookahead time.Duration) (float64, float64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bl, ok := s.baselines[metric]
	if !ok {
		return 0, 0
	}

	futureSlot := timeToSlot(time.Now().Add(lookahead))
	predicted := bl[futureSlot]

	// confidence based on sample count
	cn := s.counts[metric]
	confidence := math.Min(float64(cn[futureSlot])/20.0, 1.0)

	return predicted, confidence
}

// CurrentBaseline returns the baseline value for the current slot.
func (s *SeasonalPredictor) CurrentBaseline(metric string) float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bl, ok := s.baselines[metric]
	if !ok {
		return 0
	}
	return bl[timeToSlot(time.Now())]
}

// SetBaselines sets pre-computed baselines (e.g. from Prometheus historical data).
func (s *SeasonalPredictor) SetBaselines(metric string, baselines [168]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.baselines[metric] = baselines
	var cn [168]int64
	for i := range cn {
		if baselines[i] > 0 {
			cn[i] = 20 // assume reasonable confidence for pre-loaded data
		}
	}
	s.counts[metric] = cn
}

// timeToSlot maps a time to a 0-167 slot (hour-of-week).
func timeToSlot(t time.Time) int {
	return int(t.Weekday())*24 + t.Hour()
}
