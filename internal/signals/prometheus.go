package signals

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

type prometheusCollector struct{}

// prometheusResponse is the envelope returned by the Prometheus HTTP API.
type prometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value [2]interface{} `json:"value"` // [timestamp, value_string]
		} `json:"result"`
	} `json:"data"`
}

func (p *prometheusCollector) collect(
	ctx context.Context,
	policy *aiscalerv1.AIScaler,
	bundle *Bundle) error {

	baseUrl := policy.Spec.Prometheus.BaseURL
	if baseUrl == "" {
		return nil
	}

	if policy.Spec.Prometheus.P95LatencyQuery != "" {
		val, err := p.query(ctx, baseUrl, policy.Spec.Prometheus.P95LatencyQuery)
		if err != nil {
			return fmt.Errorf("failed to query p95 latency: %w", err)
		}
		bundle.P95LatencyMs = val
	}

	if policy.Spec.Prometheus.ErrorRateQuery != "" {
		val, err := p.query(ctx, baseUrl, policy.Spec.Prometheus.ErrorRateQuery)
		if err != nil {
			return fmt.Errorf("failed to query error rate: %w", err)
		}
		bundle.ErrorRate = val
	}

	return nil
}

func (p *prometheusCollector) query(
	ctx context.Context,
	baseUrl string,
	promql string) (float64, error) {

	endpoint := fmt.Sprintf("%s/api/v1/query", baseUrl)
	params := url.Values{
		"query": {promql},
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+params.Encode(), nil)
	if err != nil {
		return 0, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result prometheusResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode prometheus response: %w", err)
	}

	if result.Status != "success" {
		return 0, fmt.Errorf("prometheus returned status %q", result.Status)
	}
	if len(result.Data.Result) == 0 {
		return 0, nil
	}

	raw, ok := result.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("unexpected value type in prometheus response")
	}

	return strconv.ParseFloat(raw, 64)
}
