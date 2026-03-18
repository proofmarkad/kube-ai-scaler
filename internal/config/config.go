package config

import (
	"fmt"
	"os"

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
	Providers []ProviderConfig `yaml:"providers"`
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
	LeaderElection         bool   `yaml:"leaderElection"`
	MetricsBindAddress     string `yaml:"metricsBindAddress"`
	HealthProbeBindAddress string `yaml:"healthProbeBindAddress"`
}

// Load reads the operator config from the given file path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

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
