package cost

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client reads workload cost data from the OpenCost allocation API.
type Client struct {
	endpoint string
	client   *http.Client
}

// NewClient creates a cost client pointing to the OpenCost service.
func NewClient(endpoint string) *Client {
	return &Client{
		endpoint: endpoint,
		client:   &http.Client{Timeout: 15 * time.Second},
	}
}

// WorkloadCost holds cost data for a single workload.
type WorkloadCost struct {
	TotalCost       float64
	CPUCost         float64
	MemoryCost      float64
	CPUEfficiency   float64
	RAMEfficiency   float64
	TotalEfficiency float64
}

type allocationResponse struct {
	Code int                          `json:"code"`
	Data []map[string]allocationEntry `json:"data"`
}

type allocationEntry struct {
	TotalCost       float64 `json:"totalCost"`
	CPUCost         float64 `json:"cpuCost"`
	MemoryCost      float64 `json:"memoryCost"`
	CPUEfficiency   float64 `json:"cpuEfficiency"`
	RAMEfficiency   float64 `json:"ramEfficiency"`
	TotalEfficiency float64 `json:"totalEfficiency"`
}

// GetWorkloadCost fetches cost data for a namespace from OpenCost.
func (c *Client) GetWorkloadCost(ctx context.Context, namespace string) (*WorkloadCost, error) {
	url := fmt.Sprintf("%s/allocation/compute?window=1h&aggregate=namespace&filterNamespaces=%s",
		c.endpoint, namespace)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opencost request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("opencost status %d", resp.StatusCode)
	}

	var result allocationResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	for _, window := range result.Data {
		if entry, ok := window[namespace]; ok {
			return &WorkloadCost{
				TotalCost:       entry.TotalCost,
				CPUCost:         entry.CPUCost,
				MemoryCost:      entry.MemoryCost,
				CPUEfficiency:   entry.CPUEfficiency,
				RAMEfficiency:   entry.RAMEfficiency,
				TotalEfficiency: entry.TotalEfficiency,
			}, nil
		}
	}

	return &WorkloadCost{}, nil
}
