package external

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
)

func init() {
	plugin.Register("opencost", func() plugin.SignalPlugin {
		return &opencostPlugin{}
	})
}

// opencostPlugin reads workload cost data from the OpenCost API.
type opencostPlugin struct {
	endpoint  string
	namespace string
	window    string
	client    *http.Client
	healthy   bool
}

func (o *opencostPlugin) Name() string   { return "opencost" }
func (o *opencostPlugin) Required() bool { return false }
func (o *opencostPlugin) Healthy() bool  { return o.healthy }

func (o *opencostPlugin) Init(cfg map[string]string) error {
	o.endpoint = cfg["endpoint"]
	if o.endpoint == "" {
		return fmt.Errorf("opencost plugin requires 'endpoint' config (e.g. http://opencost.opencost:9003)")
	}
	o.namespace = cfg["namespace"]
	if o.namespace == "" {
		o.namespace = "default"
	}
	o.window = cfg["window"]
	if o.window == "" {
		o.window = "1h"
	}
	o.client = &http.Client{Timeout: 15 * time.Second}
	o.healthy = true
	return nil
}

// opencostResponse models the top-level allocation response.
type opencostResponse struct {
	Code int                                 `json:"code"`
	Data []map[string]opencostAllocationItem `json:"data"`
}

type opencostAllocationItem struct {
	TotalCost            float64 `json:"totalCost"`
	CPUCost              float64 `json:"cpuCost"`
	MemoryCost           float64 `json:"memoryCost"`
	CPUEfficiency        float64 `json:"cpuEfficiency"`
	RAMEfficiency        float64 `json:"ramEfficiency"`
	TotalEfficiency      float64 `json:"totalEfficiency"`
}

func (o *opencostPlugin) Collect(ctx context.Context, obj *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	// Build the allocation query URL
	// The OpenCost allocation API: /allocation/compute?window=1h&aggregate=namespace&filterNamespaces=<ns>
	url := fmt.Sprintf("%s/allocation/compute?window=%s&aggregate=namespace&filterNamespaces=%s",
		o.endpoint, o.window, o.namespace)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("opencost: failed to build request: %w", err)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		o.healthy = false
		return fmt.Errorf("opencost: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		o.healthy = false
		return fmt.Errorf("opencost: unexpected status %d", resp.StatusCode)
	}

	var result opencostResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		o.healthy = false
		return fmt.Errorf("opencost: failed to decode response: %w", err)
	}

	if len(result.Data) == 0 {
		o.healthy = true
		return nil // no cost data yet
	}

	// Find our namespace in the first window result
	for _, window := range result.Data {
		if alloc, ok := window[o.namespace]; ok {
			bundle.CostPerHour = alloc.TotalCost
			bundle.CostEfficiency = alloc.TotalEfficiency

			if bundle.CustomSignals == nil {
				bundle.CustomSignals = make(map[string]float64)
			}
			bundle.CustomSignals["opencost_cpu_cost"] = alloc.CPUCost
			bundle.CustomSignals["opencost_memory_cost"] = alloc.MemoryCost
			bundle.CustomSignals["opencost_cpu_efficiency"] = alloc.CPUEfficiency
			bundle.CustomSignals["opencost_ram_efficiency"] = alloc.RAMEfficiency
			bundle.CustomSignals["opencost_total_cost"] = alloc.TotalCost

			o.healthy = true
			return nil
		}
	}

	o.healthy = true
	return nil
}
