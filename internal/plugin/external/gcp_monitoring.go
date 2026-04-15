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
	plugin.Register("gcp-monitoring", func() plugin.SignalPlugin {
		return &gcpMonitoringPlugin{}
	})
}

// gcpMonitoringPlugin reads metrics from GCP Cloud Monitoring (Stackdriver).
// Uses the v3 timeSeries API with Workload Identity for auth.
type gcpMonitoringPlugin struct {
	projectID  string
	metricType string
	filter     string
	signalName string
	timeout    time.Duration
	healthy    bool
}

func (g *gcpMonitoringPlugin) Name() string   { return "gcp-monitoring" }
func (g *gcpMonitoringPlugin) Required() bool { return false }
func (g *gcpMonitoringPlugin) Healthy() bool  { return g.healthy }

func (g *gcpMonitoringPlugin) Init(cfg map[string]string) error {
	g.projectID = cfg["projectID"]
	if g.projectID == "" {
		return fmt.Errorf("gcp-monitoring plugin requires 'projectID' config")
	}
	g.metricType = cfg["metricType"]
	if g.metricType == "" {
		return fmt.Errorf("gcp-monitoring plugin requires 'metricType' config")
	}
	g.filter = cfg["filter"] // optional additional filter
	g.signalName = cfg["signalName"]
	if g.signalName == "" {
		g.signalName = "gcp_metric"
	}
	g.timeout = 10 * time.Second
	g.healthy = true
	return nil
}

func (g *gcpMonitoringPlugin) Collect(ctx context.Context, _ *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	filterExpr := fmt.Sprintf(`metric.type="%s"`, g.metricType)
	if g.filter != "" {
		filterExpr += " AND " + g.filter
	}

	client, err := newGCPMonitoringClient(ctx, g.timeout)
	if err != nil {
		g.healthy = false
		return err
	}

	end := time.Now()
	start := end.Add(-5 * time.Minute)
	req, err := newGCPTimeSeriesRequest(ctx, g.projectID, filterExpr, start, end)
	if err != nil {
		g.healthy = false
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		g.healthy = false
		return fmt.Errorf("gcp monitoring request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		g.healthy = false
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("gcp monitoring returned status %d: %s", resp.StatusCode, body)
	}

	var result struct {
		TimeSeries []struct {
			Points []struct {
				Value struct {
					DoubleValue *float64 `json:"doubleValue"`
					Int64Value  string   `json:"int64Value"`
				} `json:"value"`
			} `json:"points"`
		} `json:"timeSeries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		g.healthy = false
		return fmt.Errorf("failed to decode gcp monitoring response: %w", err)
	}

	if len(result.TimeSeries) > 0 && len(result.TimeSeries[0].Points) > 0 {
		point := result.TimeSeries[0].Points[0]
		var val float64
		if point.Value.DoubleValue != nil {
			val = *point.Value.DoubleValue
		} else {
			val, _ = strconv.ParseFloat(point.Value.Int64Value, 64)
		}
		if bundle.CustomSignals == nil {
			bundle.CustomSignals = make(map[string]float64)
		}
		bundle.CustomSignals[g.signalName] = val
	}

	g.healthy = true
	return nil
}
