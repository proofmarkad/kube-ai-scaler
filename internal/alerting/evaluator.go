package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// FiredAlert represents a triggered alert.
type FiredAlert struct {
	Name      string    `json:"name"`
	Severity  string    `json:"severity"`
	Condition string    `json:"condition"`
	Value     float64   `json:"value"`
	FiredAt   time.Time `json:"firedAt"`
}

// Evaluator evaluates CRD-defined alert rules against the signal bundle.
type Evaluator struct {
	webhookURL   string
	webhookToken string
	activeAlerts map[string]time.Time // name → first-seen
}

// NewEvaluator creates an alert evaluator.
func NewEvaluator(webhookURL, webhookToken string) *Evaluator {
	return &Evaluator{
		webhookURL:   webhookURL,
		webhookToken: webhookToken,
		activeAlerts: make(map[string]time.Time),
	}
}

// Evaluate checks all rules against the bundle and returns fired alerts.
func (e *Evaluator) Evaluate(bundle *plugin.Bundle, rules []aiscalerv1.AlertRule) []FiredAlert {
	var fired []FiredAlert

	for _, rule := range rules {
		val, breached := e.evaluateCondition(rule.Condition, bundle)
		if breached {
			if _, seen := e.activeAlerts[rule.Name]; !seen {
				e.activeAlerts[rule.Name] = time.Now()
			}
			// Check if it's been active long enough (for duration)
			elapsed := time.Since(e.activeAlerts[rule.Name])
			if elapsed >= rule.For.Duration {
				fired = append(fired, FiredAlert{
					Name:      rule.Name,
					Severity:  rule.Severity,
					Condition: rule.Condition,
					Value:     val,
					FiredAt:   time.Now(),
				})
			}
		} else {
			delete(e.activeAlerts, rule.Name)
		}
	}

	return fired
}

// evaluateCondition parses simple conditions like "error_rate > 5".
func (e *Evaluator) evaluateCondition(condition string, bundle *plugin.Bundle) (float64, bool) {
	parts := strings.Fields(condition)
	if len(parts) != 3 {
		return 0, false
	}

	metric := parts[0]
	operator := parts[1]
	threshold, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return 0, false
	}

	var val float64
	switch metric {
	case "error_rate":
		val = bundle.ErrorRate * 100 // convert to percentage
	case "p95_latency":
		val = bundle.P95LatencyMs
	case "cpu":
		val = bundle.CPUUtilization
	case "memory":
		val = bundle.MemoryUtilization
	case "queue_depth":
		val = bundle.QueueDepth
	default:
		if v, ok := bundle.CustomSignals[metric]; ok {
			val = v
		}
	}

	switch operator {
	case ">":
		return val, val > threshold
	case ">=":
		return val, val >= threshold
	case "<":
		return val, val < threshold
	case "<=":
		return val, val <= threshold
	case "==":
		return val, val == threshold
	default:
		return val, false
	}
}

// Notify sends fired alerts to the configured webhook.
func (e *Evaluator) Notify(ctx context.Context, alerts []FiredAlert) error {
	if e.webhookURL == "" || len(alerts) == 0 {
		return nil
	}

	log := logf.FromContext(ctx)

	payload, err := json.Marshal(map[string]interface{}{
		"alerts": alerts,
		"source": "aiscaler",
	})
	if err != nil {
		return fmt.Errorf("failed to marshal alert payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.webhookURL, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.webhookToken != "" {
		req.Header.Set("Authorization", "Bearer "+e.webhookToken)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("alert webhook failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		log.Info("alert webhook returned error", "status", resp.StatusCode)
	}

	return nil
}
