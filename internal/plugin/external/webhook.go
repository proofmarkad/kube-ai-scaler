package external

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

func init() {
	plugin.Register("webhook", func() plugin.SignalPlugin {
		return &webhookPlugin{}
	})
}

type webhookPlugin struct {
	url           string
	responseField string
	method        string
	timeout       time.Duration
	bearerToken   string
	healthy       bool
}

func (p *webhookPlugin) Name() string   { return "webhook" }
func (p *webhookPlugin) Required() bool { return false }
func (p *webhookPlugin) Healthy() bool  { return p.healthy }

func (p *webhookPlugin) SetSecretData(data map[string][]byte) {
	if v, ok := data["bearer-token"]; ok {
		p.bearerToken = string(v)
	}
}

func (p *webhookPlugin) Init(cfg map[string]string) error {
	p.url = cfg["url"]
	if p.url == "" {
		return fmt.Errorf("webhook plugin requires 'url' config")
	}
	p.responseField = cfg["responseField"]
	if p.responseField == "" {
		return fmt.Errorf("webhook plugin requires 'responseField' config")
	}
	p.method = cfg["method"]
	if p.method == "" {
		p.method = http.MethodGet
	}
	p.timeout = 5 * time.Second
	if v, ok := cfg["timeoutSeconds"]; ok {
		if secs, err := strconv.Atoi(v); err == nil {
			p.timeout = time.Duration(secs) * time.Second
		}
	}
	p.healthy = true
	return nil
}

func (p *webhookPlugin) Collect(ctx context.Context, _ *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	req, err := http.NewRequestWithContext(ctx, p.method, p.url, nil)
	if err != nil {
		return err
	}
	if p.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+p.bearerToken)
	}

	client := &http.Client{Timeout: p.timeout}
	resp, err := client.Do(req)
	if err != nil {
		p.healthy = false
		return fmt.Errorf("webhook request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.healthy = false
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB limit
	if err != nil {
		p.healthy = false
		return fmt.Errorf("failed to read webhook response: %w", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		p.healthy = false
		return fmt.Errorf("failed to decode webhook JSON: %w", err)
	}

	val, ok := data[p.responseField]
	if !ok {
		p.healthy = false
		return fmt.Errorf("webhook response missing field %q", p.responseField)
	}

	var numVal float64
	switch v := val.(type) {
	case float64:
		numVal = v
	case string:
		numVal, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("webhook field %q is not numeric: %w", p.responseField, err)
		}
	default:
		return fmt.Errorf("webhook field %q has unsupported type", p.responseField)
	}

	if bundle.CustomSignals == nil {
		bundle.CustomSignals = make(map[string]float64)
	}
	bundle.CustomSignals["webhook_"+p.responseField] = numVal

	p.healthy = true
	return nil
}
