package builtin

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

func init() {
	plugin.Register("prometheus", func() plugin.SignalPlugin {
		return &prometheusPlugin{}
	})
}

type prometheusPlugin struct {
	baseURL         string
	p95LatencyQuery string
	errorRateQuery  string
	queueDepthQuery string
	customQueries   map[string]string // name→PromQL
	timeout         time.Duration
	bearerToken     string
	tlsSkipVerify   bool
	healthy         bool
}

func (p *prometheusPlugin) Name() string   { return "prometheus" }
func (p *prometheusPlugin) Required() bool { return false }
func (p *prometheusPlugin) Healthy() bool  { return p.healthy }

func (p *prometheusPlugin) Init(cfg map[string]string) error {
	p.baseURL = cfg["baseURL"]
	if p.baseURL == "" {
		return fmt.Errorf("prometheus plugin requires 'baseURL' config")
	}
	p.p95LatencyQuery = cfg["p95LatencyQuery"]
	p.errorRateQuery = cfg["errorRateQuery"]
	p.queueDepthQuery = cfg["queueDepthQuery"]
	p.timeout = 10 * time.Second
	if v, ok := cfg["timeoutSeconds"]; ok {
		if secs, err := strconv.Atoi(v); err == nil {
			p.timeout = time.Duration(secs) * time.Second
		}
	}

	// Parse custom queries JSON
	if raw, ok := cfg["customQueries"]; ok && raw != "" {
		p.customQueries = make(map[string]string)
		if err := json.Unmarshal([]byte(raw), &p.customQueries); err != nil {
			return fmt.Errorf("invalid customQueries JSON: %w", err)
		}
	}

	// TLS / auth options
	p.bearerToken = cfg["bearerToken"]
	if v, ok := cfg["tlsInsecureSkipVerify"]; ok && v == "true" {
		p.tlsSkipVerify = true
	}

	p.healthy = true
	return nil
}

func (p *prometheusPlugin) Collect(ctx context.Context, policy *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	if p.p95LatencyQuery != "" {
		q := p.expandVars(p.p95LatencyQuery, policy)
		val, err := p.query(ctx, q)
		if err != nil {
			p.healthy = false
			return fmt.Errorf("p95 latency query failed: %w", err)
		}
		bundle.P95LatencyMs = val
	}

	if p.errorRateQuery != "" {
		q := p.expandVars(p.errorRateQuery, policy)
		val, err := p.query(ctx, q)
		if err != nil {
			p.healthy = false
			return fmt.Errorf("error rate query failed: %w", err)
		}
		bundle.ErrorRate = val
	}

	if p.queueDepthQuery != "" {
		q := p.expandVars(p.queueDepthQuery, policy)
		val, err := p.query(ctx, q)
		if err != nil {
			p.healthy = false
			return fmt.Errorf("queue depth query failed: %w", err)
		}
		bundle.QueueDepth = val
	}

	// Custom queries
	for name, promql := range p.customQueries {
		q := p.expandVars(promql, policy)
		val, err := p.query(ctx, q)
		if err != nil {
			continue // non-fatal for custom queries
		}
		if bundle.CustomSignals == nil {
			bundle.CustomSignals = make(map[string]float64)
		}
		bundle.CustomSignals[name] = val
	}

	p.healthy = true
	return nil
}

func (p *prometheusPlugin) expandVars(query string, policy *aiscalerv1.AIScaler) string {
	r := strings.NewReplacer(
		"{{namespace}}", policy.Spec.TargetRef.Namespace,
		"{{deployment}}", policy.Spec.TargetRef.Name,
	)
	return r.Replace(query)
}

// prometheusResponse is the envelope returned by the Prometheus HTTP API.
type prometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value [2]interface{} `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func (p *prometheusPlugin) query(ctx context.Context, promql string) (float64, error) {
	endpoint := fmt.Sprintf("%s/api/v1/query", p.baseURL)
	params := url.Values{"query": {promql}}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if p.tlsSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // user-configured
	}
	httpClient := &http.Client{Timeout: p.timeout, Transport: transport}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+params.Encode(), nil)
	if err != nil {
		return 0, err
	}
	if p.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+p.bearerToken)
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
