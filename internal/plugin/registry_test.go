package plugin

import (
	"context"
	"testing"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

// fakePlugin implements SignalPlugin for testing.
type fakePlugin struct {
	name     string
	required bool
}

func (f *fakePlugin) Name() string                                                       { return f.name }
func (f *fakePlugin) Init(cfg map[string]string) error                                   { return nil }
func (f *fakePlugin) Collect(_ context.Context, _ *aiscalerv1.AIScaler, _ *Bundle) error { return nil }
func (f *fakePlugin) Required() bool                                                     { return f.required }
func (f *fakePlugin) Healthy() bool                                                      { return true }

func TestRegisterAndGet(t *testing.T) {
	ResetForTesting()

	Register("test-plugin", func() SignalPlugin {
		return &fakePlugin{name: "test-plugin"}
	})

	got, err := Get("test-plugin")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got.Name() != "test-plugin" {
		t.Errorf("got name %q, want %q", got.Name(), "test-plugin")
	}
}

func TestGetMissing(t *testing.T) {
	ResetForTesting()

	_, err := Get("nonexistent")
	if err == nil {
		t.Fatal("expected error for missing plugin")
	}
}

func TestList(t *testing.T) {
	ResetForTesting()

	Register("a", func() SignalPlugin { return &fakePlugin{name: "a"} })
	Register("b", func() SignalPlugin { return &fakePlugin{name: "b"} })

	list := List()
	if len(list) != 2 {
		t.Fatalf("expected 2 plugins, got %d", len(list))
	}

	names := map[string]bool{}
	for _, n := range list {
		names[n] = true
	}
	if !names["a"] || !names["b"] {
		t.Errorf("expected plugins a and b, got %v", names)
	}
}

func TestRegisterDuplicate_Panics(t *testing.T) {
	ResetForTesting()

	Register("dup", func() SignalPlugin { return &fakePlugin{name: "dup"} })

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on duplicate registration")
		}
	}()
	Register("dup", func() SignalPlugin { return &fakePlugin{name: "dup"} })
}

func TestResetForTesting(t *testing.T) {
	ResetForTesting()
	Register("x", func() SignalPlugin { return &fakePlugin{name: "x"} })

	ResetForTesting()
	list := List()
	if len(list) != 0 {
		t.Errorf("expected empty registry after reset, got %d", len(list))
	}
}
