package llm

import (
	"testing"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

func TestEffectiveModelOverrideUsesOverrideOnlyForPrimaryProvider(t *testing.T) {
	if got := effectiveModelOverride(aiscalerv1.ProviderAnthropic, aiscalerv1.ProviderAnthropic, "claude-sonnet-4-5"); got != "claude-sonnet-4-5" {
		t.Fatalf("expected primary provider to keep override, got %q", got)
	}

	if got := effectiveModelOverride(aiscalerv1.ProviderAnthropic, aiscalerv1.ProviderOllama, "claude-sonnet-4-5"); got != "" {
		t.Fatalf("expected fallback provider to use its configured default model, got %q", got)
	}
}

func TestEffectiveModelOverrideHandlesEmptyOverride(t *testing.T) {
	if got := effectiveModelOverride(aiscalerv1.ProviderGemini, aiscalerv1.ProviderGemini, ""); got != "" {
		t.Fatalf("expected empty override to stay empty, got %q", got)
	}
}