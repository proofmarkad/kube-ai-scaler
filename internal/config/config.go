package config

import (
	"fmt"
	"os"
	"time"

	"go.yaml.in/yaml/v2"
)

// Config is the central configuration for the entire operator.
type Config struct {
	LLM        LLMConfig        `yaml:"llm"`
	Prometheus PrometheusConfig `yaml:"prometheus"`
	Operator   OperatorConfig   `yaml:"operator"`
}

// LLMConfig holds configuration for all LLM providers as a list.
type LLMConfig struct {
	Providers               []ProviderConfig `yaml:"providers"`
	ResponseCacheTTLSeconds int              `yaml:"response_cache_ttl_seconds"`
	CBThreshold             int              `yaml:"circuit_breaker_threshold"`
	CBTimeoutSec            int              `yaml:"circuit_breaker_timeout_seconds"`
}

// CacheTTL returns the response cache TTL duration (defaults to 5m).
func (c *LLMConfig) CacheTTL() time.Duration {
	if c.ResponseCacheTTLSeconds > 0 {
		return time.Duration(c.ResponseCacheTTLSeconds) * time.Second
	}
	return 5 * time.Minute
}

// CircuitBreakerThreshold returns the failure count threshold (defaults to 5).
func (c *LLMConfig) CircuitBreakerThreshold() int {
	if c.CBThreshold > 0 {
		return c.CBThreshold
	}
	return 5
}

// CircuitBreakerTimeout returns the circuit breaker timeout (defaults to 60s).
func (c *LLMConfig) CircuitBreakerTimeout() time.Duration {
	if c.CBTimeoutSec > 0 {
		return time.Duration(c.CBTimeoutSec) * time.Second
	}
	return 60 * time.Second
}

// ProviderConfig holds settings for a single LLM provider entry.
type ProviderConfig struct {
	Name    string `yaml:"name"`
	BaseURL string `yaml:"base_url"`
	APIKey  string `yaml:"api_key"`
	Model   string `yaml:"model"`
}

// PrometheusConfig holds the default Prometheus endpoint.
// Can be overridden per AIScaler in the CRD spec.
type PrometheusConfig struct {
	BaseURL string `yaml:"baseURL"`
}

// OperatorConfig holds general operator settings.
type OperatorConfig struct {
	LeaderElection                   bool    `yaml:"leaderElection"`
	MetricsBindAddress               string  `yaml:"metricsBindAddress"`
	HealthProbeBindAddress           string  `yaml:"healthProbeBindAddress"`
	AlertWebhookURL                  string  `yaml:"alertWebhookURL"`
	AlertWebhookToken                string  `yaml:"alertWebhookToken"`
	CoordinationMaxConcurrentScaling int     `yaml:"coordinationMaxConcurrentScaling"`
	CoordinationRequeueSeconds       int     `yaml:"coordinationRequeueSeconds"`
	ClusterMaxHourlyCost             float64 `yaml:"clusterMaxHourlyCost"`
}

// CoordinationRequeue returns how long to wait before retrying a deferred coordinated scaling operation.
func (c *OperatorConfig) CoordinationRequeue() time.Duration {
	if c.CoordinationRequeueSeconds > 0 {
		return time.Duration(c.CoordinationRequeueSeconds) * time.Second
	}
	return 15 * time.Second
}

// CoordinationMaxConcurrentScalingLimit returns the cluster concurrency limit for live scaling operations.
func (c *OperatorConfig) CoordinationMaxConcurrentScalingLimit() int {
	if c.CoordinationMaxConcurrentScaling > 0 {
		return c.CoordinationMaxConcurrentScaling
	}
	return 0
}

// Load reads the operator config from the given file path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	data = []byte(os.ExpandEnv(string(data)))

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config file: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil

}

// validate checks that all required fields are present.
func (c *Config) validate() error {
	if len(c.LLM.Providers) == 0 {
		return fmt.Errorf("at least one LLM provider must be configured")
	}
	for _, p := range c.LLM.Providers {
		if p.Name == "" {
			return fmt.Errorf("a provider entry is missing name")
		}
		if p.BaseURL == "" {
			return fmt.Errorf("provider %q is missing baseURL", p.Name)
		}
		if p.Model == "" {
			return fmt.Errorf("provider %q is missing model", p.Name)
		}
	}
	return nil
}

// LLMProvider searches the provider list by name and returns its config.
func (c *Config) LLMProvider(name string) (ProviderConfig, error) {
	for _, p := range c.LLM.Providers {
		if p.Name == name {
			return p, nil
		}
	}
	return ProviderConfig{}, fmt.Errorf("provider %q not found", name)
}
