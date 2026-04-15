package coordinator

import (
	"testing"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
)

func TestDependencyGraphUsesTargetWorkloadName(t *testing.T) {
	graph := NewDependencyGraph()
	graph.BuildFromPolicies([]aiscalerv1.AIScaler{
		{
			Spec: aiscalerv1.AIScalerSpec{
				TargetRef: aiscalerv1.TargetRef{Name: "api", Namespace: "default"},
				Dependencies: &aiscalerv1.DependencyConfig{
					UpstreamOf: []aiscalerv1.TargetRef{{Name: "db", Namespace: "default"}},
					CoscalesWith: []aiscalerv1.CoscaleRef{{
						TargetRef: aiscalerv1.TargetRef{Name: "worker", Namespace: "default"},
						Ratio:     2,
					}},
				},
			},
		},
	})

	if !graph.ShouldDefer("api", map[string]bool{"db": true}) {
		t.Fatal("expected dependency graph lookup by target workload name to defer scaling")
	}

	coscaleTargets := graph.GetCoscaleTargets("api")
	if len(coscaleTargets) != 1 {
		t.Fatalf("expected 1 coscale target, got %d", len(coscaleTargets))
	}
	if coscaleTargets[0].Workload != "worker" {
		t.Fatalf("expected coscale target worker, got %q", coscaleTargets[0].Workload)
	}
}
