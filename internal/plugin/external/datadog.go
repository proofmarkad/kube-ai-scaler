package external

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

func init() {
	plugin.Register("datadog", func() plugin.SignalPlugin {
		return &datadogPlugin{}
	})
}

type datadogPlugin struct {
	apiKey  string
	appKey  string
	site    string
	query   string
	healthy bool
}

func (p *datadogPlugin) Name() string   { return "datadog" }
func (p *datadogPlugin) Required() bool { return false }
func (p *datadogPlugin) Healthy() bool  { return p.healthy }

func (p *datadogPlugin) SetSecretData(data map[string][]byte) {
	if v, ok := data["api-key"]; ok {
		p.apiKey = string(v)
	}
	if v, ok := data["app-key"]; ok {
		p.appKey = string(v)
	}
}

func (p *datadogPlugin) Init(cfg map[string]string) error {
	p.site = cfg["site"]
	if p.site == "" {
		p.site = "datadoghq.com"
	}
	p.query = cfg["query"]
	if p.query == "" {
		return fmt.Errorf("datadog plugin requires 'query' config")
	}
	// API keys can come from SetSecretData or directly
	if v, ok := cfg["apiKey"]; ok {
		p.apiKey = v
	}
	if v, ok := cfg["appKey"]; ok {
		p.appKey = v
	}
	p.healthy = true
	return nil
}

// datadogQueryResponse represents the Datadog metrics query API response.
type datadogQueryResponse struct {
	Series []struct {
		Pointlist [][]interface{} `json:"pointlist"`
	} `json:"series"`
}

func (p *datadogPlugin) Collect(ctx context.Context, _ *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	if p.apiKey == "" {
		p.healthy = false
		return fmt.Errorf("datadog API key not configured")
	}

	now := time.Now().Unix()
	from := now - 300 // last 5 minutes

	endpoint := fmt.Sprintf("https://api.%s/api/v1/query", p.site)
	params := url.Values{
		"from":  {strconv.FormatInt(from, 10)},
		"to":    {strconv.FormatInt(now, 10)},
		"query": {p.query},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+params.Encode(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("DD-API-KEY", p.apiKey)
	if p.appKey != "" {
		req.Header.Set("DD-APPLICATION-KEY", p.appKey)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		p.healthy = false
		return fmt.Errorf("datadog query failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.healthy = false
		return fmt.Errorf("datadog API returned status %d", resp.StatusCode)
	}

	var result datadogQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		p.healthy = false
		return fmt.Errorf("failed to decode datadog response: %w", err)
	}

	if len(result.Series) > 0 && len(result.Series[0].Pointlist) > 0 {
		for idx := len(result.Series[0].Pointlist) - 1; idx >= 0; idx-- {
			point := result.Series[0].Pointlist[idx]
			if len(point) < 2 || point[1] == nil {
				continue
			}

			val, err := parseDatadogPointValue(point[1])
			if err != nil {
				continue
			}

			if bundle.CustomSignals == nil {
				bundle.CustomSignals = make(map[string]float64)
			}
			bundle.CustomSignals["datadog_metric"] = val
			break
		}
	}

	p.healthy = true
	return nil
}

func parseDatadogPointValue(raw interface{}) (float64, error) {
	switch v := raw.(type) {
	case float64:
		return v, nil
	case json.Number:
		return v.Float64()
	case string:
		return strconv.ParseFloat(v, 64)
	case nil:
		return 0, fmt.Errorf("datadog point value is null")
	default:
		return 0, fmt.Errorf("unsupported datadog point value type %T", raw)
	}
}
