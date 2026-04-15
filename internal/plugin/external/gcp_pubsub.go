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
	plugin.Register("gcp-pubsub", func() plugin.SignalPlugin {
		return &gcpPubSubPlugin{}
	})
}

// gcpPubSubPlugin reads unacked message count from a GCP Pub/Sub subscription
// via the Cloud Monitoring API (metrics.googleapis.com). Uses Workload Identity
// for auth when running on GKE.
type gcpPubSubPlugin struct {
	projectID      string
	subscriptionID string
	targetDepth    float64
	timeout        time.Duration
	healthy        bool
}

func (g *gcpPubSubPlugin) Name() string   { return "gcp-pubsub" }
func (g *gcpPubSubPlugin) Required() bool { return false }
func (g *gcpPubSubPlugin) Healthy() bool  { return g.healthy }

func (g *gcpPubSubPlugin) Init(cfg map[string]string) error {
	g.projectID = cfg["projectID"]
	if g.projectID == "" {
		return fmt.Errorf("gcp-pubsub plugin requires 'projectID' config")
	}
	g.subscriptionID = cfg["subscriptionID"]
	if g.subscriptionID == "" {
		return fmt.Errorf("gcp-pubsub plugin requires 'subscriptionID' config")
	}
	g.targetDepth = 10
	if v, ok := cfg["targetDepth"]; ok {
		if d, err := strconv.ParseFloat(v, 64); err == nil && d > 0 {
			g.targetDepth = d
		}
	}
	g.timeout = 10 * time.Second
	g.healthy = true
	return nil
}

func (g *gcpPubSubPlugin) Collect(ctx context.Context, _ *aiscalerv1.AIScaler, bundle *plugin.Bundle) error {
	// Use Cloud Monitoring API to get num_undelivered_messages
	// This works with Workload Identity (GKE) or Application Default Credentials
	client, err := newGCPMonitoringClient(ctx, g.timeout)
	if err != nil {
		g.healthy = false
		return err
	}

	filterExpr := fmt.Sprintf(
		`metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages" AND resource.labels.subscription_id="%s"`,
		g.subscriptionID,
	)
	end := time.Now()
	start := end.Add(-2 * time.Minute)
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
					Int64Value string `json:"int64Value"`
				} `json:"value"`
			} `json:"points"`
		} `json:"timeSeries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		g.healthy = false
		return fmt.Errorf("failed to decode gcp monitoring response: %w", err)
	}

	if len(result.TimeSeries) > 0 && len(result.TimeSeries[0].Points) > 0 {
		val, _ := strconv.ParseFloat(result.TimeSeries[0].Points[0].Value.Int64Value, 64)
		bundle.QueueDepth = val
		if bundle.CustomSignals == nil {
			bundle.CustomSignals = make(map[string]float64)
		}
		bundle.CustomSignals["pubsub_unacked_messages"] = val
	}

	g.healthy = true
	return nil
}
