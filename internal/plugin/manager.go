package plugin

import (
	"context"
	"fmt"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	aiscalermetrics "github.com/sanjbh/kube-scaling-agent/internal/metrics"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Manager orchestrates all active signal plugins.
type Manager struct {
	plugins []pluginInstance
}

type pluginInstance struct {
	plugin   SignalPlugin
	required bool
}

// NewManager creates plugins from the CRD's signal config list.
// If spec.signals is empty, it creates default plugins (metrics-server + annotations).
func NewManager(ctx context.Context, signalConfigs []aiscalerv1.SignalSourceConfig, k8sDeps K8sPluginDeps) (*Manager, error) {
	m := &Manager{}
	if ctx == nil {
		ctx = context.Background()
	}

	if len(signalConfigs) == 0 {
		signalConfigs = []aiscalerv1.SignalSourceConfig{
			{Name: "metrics-server", Required: true},
			{Name: "annotations", Required: false},
		}
	}

	for _, sc := range signalConfigs {
		p, err := Get(sc.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get plugin %q: %w", sc.Name, err)
		}

		// Merge config
		cfg := make(map[string]string)
		for k, v := range sc.Config {
			cfg[k] = v
		}

		// Pass K8s dependencies if the plugin needs them
		if kp, ok := p.(K8sAwarePlugin); ok {
			kp.SetK8sDeps(k8sDeps)
		}

		if sc.SecretRef != nil {
			sr, ok := p.(SecretReader)
			if !ok {
				return nil, fmt.Errorf("plugin %q does not support secretRef", sc.Name)
			}

			k8sClient, ok := k8sDeps.Client.(ctrlclient.Client)
			if !ok || k8sClient == nil {
				return nil, fmt.Errorf("plugin %q secretRef requires a Kubernetes client", sc.Name)
			}

			secretData, err := readSecretData(ctx, k8sClient, sc.SecretRef)
			if err != nil {
				return nil, fmt.Errorf("failed to load secret for plugin %q: %w", sc.Name, err)
			}
			sr.SetSecretData(secretData)
		}

		if err := p.Init(cfg); err != nil {
			return nil, fmt.Errorf("failed to init plugin %q: %w", sc.Name, err)
		}

		m.plugins = append(m.plugins, pluginInstance{
			plugin:   p,
			required: sc.Required,
		})
	}

	return m, nil
}

func readSecretData(ctx context.Context, k8sClient ctrlclient.Client, ref *aiscalerv1.SecretRef) (map[string][]byte, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}
	if err := k8sClient.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("failed to read secret %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	if _, ok := secret.Data[ref.Key]; !ok {
		return nil, fmt.Errorf("key %q not found in secret %s/%s", ref.Key, ref.Namespace, ref.Name)
	}
	return secret.Data, nil
}

// Collect runs all plugins and returns the aggregated bundle.
func (m *Manager) Collect(ctx context.Context, policy *aiscalerv1.AIScaler) (*Bundle, error) {
	log := logf.FromContext(ctx)
	bundle := &Bundle{
		CollectedAt:   time.Now(),
		CustomSignals: make(map[string]float64),
		SourceHealth:  make(map[string]bool),
	}

	for _, pi := range m.plugins {
		start := time.Now()
		err := pi.plugin.Collect(ctx, policy, bundle)
		aiscalermetrics.SignalCollectionLatency.WithLabelValues(pi.plugin.Name()).Observe(time.Since(start).Seconds())
		bundle.SourceHealth[pi.plugin.Name()] = err == nil

		if err != nil {
			if pi.required {
				return nil, fmt.Errorf("required signal plugin %q failed: %w", pi.plugin.Name(), err)
			}
			log.Info("optional signal plugin failed (non-fatal)",
				"plugin", pi.plugin.Name(), "error", err)
		}
	}

	return bundle, nil
}

// HealthStatus returns the health of each active plugin.
func (m *Manager) HealthStatus() map[string]bool {
	status := make(map[string]bool)
	for _, pi := range m.plugins {
		status[pi.plugin.Name()] = pi.plugin.Healthy()
	}
	return status
}
