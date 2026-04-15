package plugin

import (
	"context"
	"testing"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type testSecretPlugin struct {
	data map[string][]byte
}

func (p *testSecretPlugin) Name() string                                                 { return "test-secret" }
func (p *testSecretPlugin) Init(map[string]string) error                                 { return nil }
func (p *testSecretPlugin) Collect(context.Context, *aiscalerv1.AIScaler, *Bundle) error { return nil }
func (p *testSecretPlugin) Required() bool                                               { return false }
func (p *testSecretPlugin) Healthy() bool                                                { return true }
func (p *testSecretPlugin) SetSecretData(data map[string][]byte)                         { p.data = data }

func TestNewManager_LoadsPluginSecretData(t *testing.T) {
	ResetForTesting()
	t.Cleanup(ResetForTesting)

	pl := &testSecretPlugin{}
	Register("test-secret", func() SignalPlugin { return pl })

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "plugin-creds",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"token": []byte("super-secret"),
			"extra": []byte("secondary-value"),
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	_, err := NewManager(context.Background(), []aiscalerv1.SignalSourceConfig{
		{
			Name: "test-secret",
			SecretRef: &aiscalerv1.SecretRef{
				Name:      "plugin-creds",
				Namespace: "default",
				Key:       "token",
			},
		},
	}, K8sPluginDeps{Client: k8sClient})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}

	if got := string(pl.data["token"]); got != "super-secret" {
		t.Fatalf("expected token secret data, got %q", got)
	}
	if got := string(pl.data["extra"]); got != "secondary-value" {
		t.Fatalf("expected full secret payload, got %q", got)
	}
}
