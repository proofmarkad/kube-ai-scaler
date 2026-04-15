package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad_ValidConfig(t *testing.T) {
	content := `
llm:
  providers:
    - name: openai
      base_url: https://api.openai.com
      api_key: test-key
      model: gpt-4
  response_cache_ttl_seconds: 120
  circuit_breaker_threshold: 3
  circuit_breaker_timeout_seconds: 30
prometheus:
  baseURL: http://prometheus:9090
operator:
  leaderElection: true
  metricsBindAddress: ":8080"
  healthProbeBindAddress: ":8081"
  alertWebhookURL: https://hooks.example.com
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(cfg.LLM.Providers) != 1 {
		t.Errorf("expected 1 provider, got %d", len(cfg.LLM.Providers))
	}
	if cfg.LLM.Providers[0].Name != "openai" {
		t.Errorf("expected provider name openai, got %q", cfg.LLM.Providers[0].Name)
	}
	if cfg.Operator.AlertWebhookURL != "https://hooks.example.com" {
		t.Errorf("unexpected alertWebhookURL: %q", cfg.Operator.AlertWebhookURL)
	}
}

func TestLoad_MissingProviders(t *testing.T) {
	content := `
llm:
  providers: []
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing providers")
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("/nonexistent/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLLMConfig_Defaults(t *testing.T) {
	cfg := &LLMConfig{}
	if cfg.CacheTTL() != 5*time.Minute {
		t.Errorf("expected 5m default TTL, got %v", cfg.CacheTTL())
	}
	if cfg.CircuitBreakerThreshold() != 5 {
		t.Errorf("expected 5 default threshold, got %d", cfg.CircuitBreakerThreshold())
	}
	if cfg.CircuitBreakerTimeout() != 60*time.Second {
		t.Errorf("expected 60s default timeout, got %v", cfg.CircuitBreakerTimeout())
	}
}

func TestLLMConfig_CustomValues(t *testing.T) {
	cfg := &LLMConfig{
		ResponseCacheTTLSeconds: 120,
		CBThreshold:             3,
		CBTimeoutSec:            30,
	}
	if cfg.CacheTTL() != 120*time.Second {
		t.Errorf("expected 2m TTL, got %v", cfg.CacheTTL())
	}
	if cfg.CircuitBreakerThreshold() != 3 {
		t.Errorf("expected 3, got %d", cfg.CircuitBreakerThreshold())
	}
	if cfg.CircuitBreakerTimeout() != 30*time.Second {
		t.Errorf("expected 30s, got %v", cfg.CircuitBreakerTimeout())
	}
}

func TestLLMProvider_Found(t *testing.T) {
	cfg := &Config{
		LLM: LLMConfig{
			Providers: []ProviderConfig{
				{Name: "openai", BaseURL: "https://api.openai.com", Model: "gpt-4"},
				{Name: "anthropic", BaseURL: "https://api.anthropic.com", Model: "claude"},
			},
		},
	}
	p, err := cfg.LLMProvider("anthropic")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if p.Name != "anthropic" {
		t.Errorf("expected anthropic, got %q", p.Name)
	}
}

func TestLLMProvider_NotFound(t *testing.T) {
	cfg := &Config{
		LLM: LLMConfig{
			Providers: []ProviderConfig{
				{Name: "openai", BaseURL: "https://api.openai.com", Model: "gpt-4"},
			},
		},
	}
	_, err := cfg.LLMProvider("nonexistent")
	if err == nil {
		t.Fatal("expected error for missing provider")
	}
}
