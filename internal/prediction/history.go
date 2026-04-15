package prediction

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// HistoryStore queries Prometheus for historical metric averages
// to populate seasonal baselines.
type HistoryStore struct {
	prometheusURL string
	client        *http.Client
}

// NewHistoryStore creates a history store backed by Prometheus.
func NewHistoryStore(prometheusURL string) *HistoryStore {
	return &HistoryStore{
		prometheusURL: prometheusURL,
		client:        &http.Client{Timeout: 30 * time.Second},
	}
}

type promRangeResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Values [][]interface{}   `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// QueryHourlyAverages returns hourly averages for a metric over the past N days,
// keyed by hour-of-week (0-167).
func (h *HistoryStore) QueryHourlyAverages(
	ctx context.Context,
	query string,
	days int,
) (map[int]float64, error) {
	// Use Prometheus range query with 1h step over the requested days
	end := time.Now()
	start := end.Add(-time.Duration(days) * 24 * time.Hour)

	endpoint := strings.TrimRight(h.prometheusURL, "/") + "/api/v1/query_range"
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.Unix(), 10)},
		"end":   {strconv.FormatInt(end.Unix(), 10)},
		"step":  {"3600"},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+params.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("prometheus query: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("prometheus status %d: %s", resp.StatusCode, body)
	}

	var result promRangeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode prometheus response: %w", err)
	}
	if result.Status != "success" {
		if result.Error != "" {
			return nil, fmt.Errorf("prometheus query failed: %s (%s)", result.Error, result.ErrorType)
		}
		return nil, fmt.Errorf("prometheus query failed with status %q", result.Status)
	}

	if len(result.Data.Result) == 0 {
		return map[int]float64{}, nil
	}

	// Aggregate values by hour-of-week
	sums := make(map[int]float64)
	counts := make(map[int]int)

	for _, series := range result.Data.Result {
		for _, pair := range series.Values {
			if len(pair) < 2 {
				continue
			}
			ts, ok := pair[0].(float64)
			if !ok {
				continue
			}
			valStr, ok := pair[1].(string)
			if !ok {
				continue
			}
			val, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				continue
			}

			t := time.Unix(int64(ts), 0)
			slot := int(t.Weekday())*24 + t.Hour()
			sums[slot] += val
			counts[slot]++
		}
	}

	averages := make(map[int]float64, len(sums))
	for slot, sum := range sums {
		if counts[slot] > 0 {
			averages[slot] = sum / float64(counts[slot])
		}
	}

	return averages, nil
}

// BuildBaselines converts hourly averages into a [168]float64 array.
func BuildBaselines(averages map[int]float64) [168]float64 {
	var baselines [168]float64
	for slot, avg := range averages {
		if slot >= 0 && slot < 168 {
			baselines[slot] = avg
		}
	}
	return baselines
}
