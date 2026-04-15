package alerting

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEvaluate_NoRules(t *testing.T) {
	e := NewEvaluator("", "")
	fired := e.Evaluate(&plugin.Bundle{}, nil)
	if len(fired) != 0 {
		t.Errorf("expected 0 fired alerts, got %d", len(fired))
	}
}

func TestEvaluate_RuleFires(t *testing.T) {
	e := NewEvaluator("", "")
	rules := []aiscalerv1.AlertRule{
		{
			Name:      "high-cpu",
			Condition: "cpu > 80",
			Severity:  "warning",
			For:       metav1.Duration{Duration: 0}, // fire immediately
		},
	}
	bundle := &plugin.Bundle{
		CPUUtilization: 90,
	}

	fired := e.Evaluate(bundle, rules)
	if len(fired) != 1 {
		t.Fatalf("expected 1 fired alert, got %d", len(fired))
	}
	if fired[0].Name != "high-cpu" {
		t.Errorf("expected high-cpu, got %q", fired[0].Name)
	}
}

func TestEvaluate_RuleDoesNotFire(t *testing.T) {
	e := NewEvaluator("", "")
	rules := []aiscalerv1.AlertRule{
		{
			Name:      "high-cpu",
			Condition: "cpu > 80",
			Severity:  "warning",
			For:       metav1.Duration{Duration: 0},
		},
	}
	bundle := &plugin.Bundle{
		CPUUtilization: 50,
	}

	fired := e.Evaluate(bundle, rules)
	if len(fired) != 0 {
		t.Errorf("expected 0 fired alerts, got %d", len(fired))
	}
}

func TestEvaluate_DurationGating(t *testing.T) {
	e := NewEvaluator("", "")
	rules := []aiscalerv1.AlertRule{
		{
			Name:      "sustained-cpu",
			Condition: "cpu > 80",
			Severity:  "critical",
			For:       metav1.Duration{Duration: 5 * time.Minute},
		},
	}
	bundle := &plugin.Bundle{
		CPUUtilization: 90,
	}

	// First evaluation — alert starts tracking but hasn't been active long enough
	fired := e.Evaluate(bundle, rules)
	if len(fired) != 0 {
		t.Errorf("expected 0 fired (duration not met), got %d", len(fired))
	}

	// Verify it's being tracked
	if _, ok := e.activeAlerts["sustained-cpu"]; !ok {
		t.Error("expected alert to be tracked")
	}
}

func TestEvaluate_AlertClears(t *testing.T) {
	e := NewEvaluator("", "")
	rules := []aiscalerv1.AlertRule{
		{
			Name:      "high-cpu",
			Condition: "cpu > 80",
			Severity:  "warning",
			For:       metav1.Duration{Duration: 0},
		},
	}

	// First: breach
	e.Evaluate(&plugin.Bundle{CPUUtilization: 90}, rules)
	if _, ok := e.activeAlerts["high-cpu"]; !ok {
		t.Error("expected alert to be tracked after breach")
	}

	// Second: no breach → should clear
	e.Evaluate(&plugin.Bundle{CPUUtilization: 50}, rules)
	if _, ok := e.activeAlerts["high-cpu"]; ok {
		t.Error("expected alert to be cleared after recovery")
	}
}

func TestEvaluate_InvalidCondition(t *testing.T) {
	e := NewEvaluator("", "")
	rules := []aiscalerv1.AlertRule{
		{
			Name:      "bad-rule",
			Condition: "invalid",
			Severity:  "warning",
			For:       metav1.Duration{Duration: 0},
		},
	}

	fired := e.Evaluate(&plugin.Bundle{}, rules)
	if len(fired) != 0 {
		t.Errorf("expected 0 for invalid condition, got %d", len(fired))
	}
}

func TestNotify_Webhook(t *testing.T) {
	var receivedAlerts []FiredAlert
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("expected bearer token")
		}
		var payload struct {
			Alerts []FiredAlert `json:"alerts"`
			Source string       `json:"source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("failed to decode body: %v", err)
		}
		receivedAlerts = payload.Alerts
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	e := NewEvaluator(server.URL, "test-token")
	alerts := []FiredAlert{
		{Name: "test-alert", Severity: "warning"},
	}

	if err := e.Notify(context.Background(), alerts); err != nil {
		t.Fatalf("Notify failed: %v", err)
	}

	if len(receivedAlerts) != 1 {
		t.Fatalf("expected 1 alert received, got %d", len(receivedAlerts))
	}
	if receivedAlerts[0].Name != "test-alert" {
		t.Errorf("expected test-alert, got %q", receivedAlerts[0].Name)
	}
}
