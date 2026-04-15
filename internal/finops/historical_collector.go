package finops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// HistoricalUsage holds percentile utilization data.
type HistoricalUsage struct {
	CPUP50    float64
	CPUP95    float64
	CPUP99    float64
	CPUMax    float64
	MemoryP50 float64
	MemoryP95 float64
	MemoryP99 float64
	MemoryMax float64
	OOMKills  int
	Throttled float64
}

// HistoricalCollector queries Prometheus for historical usage data.
type HistoricalCollector struct {
	prometheusURL string
	client        *http.Client
}

// NewHistoricalCollector creates a collector.
func NewHistoricalCollector(prometheusURL string) *HistoricalCollector {
	return &HistoricalCollector{
		prometheusURL: prometheusURL,
		client:        &http.Client{Timeout: 30 * time.Second},
	}
}

type promQueryResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Values [][]interface{} `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

// QueryUsagePercentiles fetches CPU and memory usage percentiles from Prometheus.
func (hc *HistoricalCollector) QueryUsagePercentiles(
	ctx context.Context,
	namespace string,
	workload string,
	window time.Duration,
) (*HistoricalUsage, error) {
	usage := &HistoricalUsage{}

	// CPU utilization
	cpuQuery := fmt.Sprintf(
		`rate(container_cpu_usage_seconds_total{namespace="%s",pod=~"%s-.*"}[5m])`,
		namespace, workload)
	cpuValues, err := hc.queryRange(ctx, cpuQuery, window)
	if err == nil && len(cpuValues) > 0 {
		usage.CPUP50 = percentile(cpuValues, 0.50) * 100
		usage.CPUP95 = percentile(cpuValues, 0.95) * 100
		usage.CPUP99 = percentile(cpuValues, 0.99) * 100
		usage.CPUMax = percentile(cpuValues, 1.0) * 100
	}

	// Memory utilization
	memQuery := fmt.Sprintf(
		`container_memory_working_set_bytes{namespace="%s",pod=~"%s-.*"}`,
		namespace, workload)
	memValues, err := hc.queryRange(ctx, memQuery, window)
	if err == nil && len(memValues) > 0 {
		usage.MemoryP50 = percentile(memValues, 0.50)
		usage.MemoryP95 = percentile(memValues, 0.95)
		usage.MemoryP99 = percentile(memValues, 0.99)
		usage.MemoryMax = percentile(memValues, 1.0)
	}

	return usage, nil
}

func (hc *HistoricalCollector) queryRange(
	ctx context.Context,
	query string,
	window time.Duration,
) ([]float64, error) {
	end := time.Now()
	start := end.Add(-window)

	endpoint := strings.TrimRight(hc.prometheusURL, "/") + "/api/v1/query_range"
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(start.Unix(), 10)},
		"end":   {strconv.FormatInt(end.Unix(), 10)},
		"step":  {"300"},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := hc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("prometheus status %d: %s", resp.StatusCode, body)
	}

	var result promQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if result.Status != "success" {
		if result.Error != "" {
			return nil, fmt.Errorf("prometheus query failed: %s (%s)", result.Error, result.ErrorType)
		}
		return nil, fmt.Errorf("prometheus query failed with status %q", result.Status)
	}

	var values []float64
	for _, r := range result.Data.Result {
		for _, pair := range r.Values {
			if len(pair) >= 2 {
				if valStr, ok := pair[1].(string); ok {
					if v, err := strconv.ParseFloat(valStr, 64); err == nil {
						values = append(values, v)
					}
				}
			}
		}
	}

	return values, nil
}

// percentile calculates the p-th percentile from a sorted slice.
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	idx := p * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}

	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}
