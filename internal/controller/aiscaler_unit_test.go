package controller

import (
	"context"
	"testing"
	"time"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/alerting"
	"github.com/sanjbh/kube-scaling-agent/internal/audit"
	"github.com/sanjbh/kube-scaling-agent/internal/coordinator"
	"github.com/sanjbh/kube-scaling-agent/internal/cost"
	"github.com/sanjbh/kube-scaling-agent/internal/decision"
	"github.com/sanjbh/kube-scaling-agent/internal/llm"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeAuditStore struct {
	records []*audit.DecisionRecord
}

func (f *fakeAuditStore) Store(ctx context.Context, record *audit.DecisionRecord) error {
	f.records = append([]*audit.DecisionRecord{record}, f.records...)
	return nil
}

func (f *fakeAuditStore) List(ctx context.Context, workload string, limit int) ([]*audit.DecisionRecord, error) {
	var filtered []*audit.DecisionRecord
	for _, record := range f.records {
		if record != nil && record.Workload == workload {
			filtered = append(filtered, record)
		}
	}
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered, nil
}

func (f *fakeAuditStore) Get(ctx context.Context, id string) (*audit.DecisionRecord, error) {
	for _, record := range f.records {
		if record != nil && record.ID == id {
			return record, nil
		}
	}
	return nil, nil
}

func TestEnsureFinalizerInvalidSpecShortCircuits(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := aiscalerv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add AIScaler scheme: %v", err)
	}

	obj := &aiscalerv1.AIScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid-policy"},
		Spec: aiscalerv1.AIScalerSpec{
			Constraints: aiscalerv1.ScalingConstraints{
				MinReplicas:  5,
				MaxReplicas:  3,
				MaxScaleStep: 1,
			},
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).WithStatusSubresource(obj).Build()
	reconciler := &AIScalerReconciler{Client: k8sClient, Scheme: scheme}

	result, err := reconciler.ensureFinalizer(context.Background(), &reconcileState{obj: obj})
	if err != nil {
		t.Fatalf("ensureFinalizer returned error: %v", err)
	}
	if result == nil {
		t.Fatal("expected ensureFinalizer to short-circuit on invalid spec")
	}
	if *result != (ctrl.Result{}) {
		t.Fatalf("expected empty reconcile result, got %#v", *result)
	}
	if len(obj.Status.Conditions) == 0 {
		t.Fatal("expected invalid spec condition to be recorded")
	}
}

func TestPersistStatusHandlesPartialCooldownState(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := aiscalerv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add AIScaler scheme: %v", err)
	}

	obj := &aiscalerv1.AIScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "cooldown-policy", Namespace: "default"},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).WithStatusSubresource(obj).Build()
	reconciler := &AIScalerReconciler{Client: k8sClient, Scheme: scheme}

	state := &reconcileState{
		obj:        obj,
		phase:      aiscalerv1.PhaseCoolingDown,
		decisionID: "decision-1",
	}

	if _, err := reconciler.persistStatus(context.Background(), state, false); err != nil {
		t.Fatalf("persistStatus returned error: %v", err)
	}

	updated := &aiscalerv1.AIScaler{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(obj), updated); err != nil {
		t.Fatalf("get updated object: %v", err)
	}
	if updated.Status.Phase != aiscalerv1.PhaseCoolingDown {
		t.Fatalf("expected phase %q, got %q", aiscalerv1.PhaseCoolingDown, updated.Status.Phase)
	}
	if updated.Status.LastDecisionID != "decision-1" {
		t.Fatalf("expected decision id to persist, got %q", updated.Status.LastDecisionID)
	}
	if updated.Status.DesiredReplicas != 0 {
		t.Fatalf("expected desired replicas to remain unset, got %d", updated.Status.DesiredReplicas)
	}
}

func TestBuildVerticalDecisionParsesProposal(t *testing.T) {
	decision, err := buildVerticalDecision(&aiscalerv1.VerticalScalingConfig{
		Enabled:      true,
		ResizePolicy: "InPlace",
	}, &llm.VerticalProposal{
		CPURequest:     "500m",
		MemoryRequest:  "512Mi",
		CPULimit:       "1",
		MemoryLimit:    "1Gi",
		ResizeStrategy: "Recreate",
	})
	if err != nil {
		t.Fatalf("buildVerticalDecision returned error: %v", err)
	}
	if decision == nil {
		t.Fatal("expected vertical decision to be created")
	}
	if got := decision.CPURequest.String(); got != "500m" {
		t.Fatalf("expected cpu request 500m, got %s", got)
	}
	if got := decision.MemoryRequest.String(); got != "512Mi" {
		t.Fatalf("expected memory request 512Mi, got %s", got)
	}
	if got := decision.CPULimit.String(); got != "1" {
		t.Fatalf("expected cpu limit 1, got %s", got)
	}
	if got := decision.MemoryLimit.String(); got != "1Gi" {
		t.Fatalf("expected memory limit 1Gi, got %s", got)
	}
	if decision.ResizeStrategy != "Recreate" {
		t.Fatalf("expected resize strategy Recreate, got %q", decision.ResizeStrategy)
	}
}

func TestBuildVerticalDecisionRejectsIncompleteProposal(t *testing.T) {
	decision, err := buildVerticalDecision(&aiscalerv1.VerticalScalingConfig{
		Enabled:      true,
		ResizePolicy: "InPlace",
	}, &llm.VerticalProposal{CPURequest: "500m"})
	if err == nil {
		t.Fatal("expected incomplete proposal to return an error")
	}
	if decision != nil {
		t.Fatal("expected no vertical decision for incomplete proposal")
	}
}

func TestResolveDecisionStateHonorsReactivePrecedenceConfig(t *testing.T) {
	reconciler := &AIScalerReconciler{PrecedenceResolver: decision.NewPrecedenceResolver()}
	state := &reconcileState{
		obj: &aiscalerv1.AIScaler{
			Spec: aiscalerv1.AIScalerSpec{
				Constraints: aiscalerv1.ScalingConstraints{MinReplicas: 1, MaxReplicas: 20},
				Precedence:  &aiscalerv1.PrecedenceConfig{ReactiveRulesOverrideLLM: false},
			},
		},
		bundle:           &plugin.Bundle{CurrentReplicas: 5},
		reactiveDecision: &llm.ScalingDecision{TargetReplicas: 10, Reasoning: "reactive rule", Confidence: 1.0},
		llmDecision:      &llm.ScalingDecision{TargetReplicas: 6, Reasoning: "llm target", Confidence: 0.8},
		llmProvider:      aiscalerv1.ProviderAnthropic,
	}

	if err := reconciler.resolveDecisionState(context.Background(), state, false, false); err != nil {
		t.Fatalf("resolveDecisionState returned error: %v", err)
	}
	if state.decision == nil || state.decision.TargetReplicas != 6 {
		t.Fatalf("expected llm target 6, got %#v", state.decision)
	}
	if state.provider != aiscalerv1.ProviderAnthropic {
		t.Fatalf("expected provider %q, got %q", aiscalerv1.ProviderAnthropic, state.provider)
	}
	if state.precedence == nil || state.precedence.TargetReplicas != 6 {
		t.Fatalf("expected precedence target 6, got %#v", state.precedence)
	}
}

func TestResolveDecisionStateAppliesCostCeiling(t *testing.T) {
	reconciler := &AIScalerReconciler{
		PrecedenceResolver: decision.NewPrecedenceResolver(),
		CostEstimator:      cost.NewEstimator(),
	}
	state := &reconcileState{
		obj: &aiscalerv1.AIScaler{
			Spec: aiscalerv1.AIScalerSpec{
				Constraints:     aiscalerv1.ScalingConstraints{MinReplicas: 1, MaxReplicas: 20},
				CostConstraints: &aiscalerv1.CostConstraints{MaxHourlyCost: 6, Enforcement: "hard"},
			},
		},
		bundle:              &plugin.Bundle{CurrentReplicas: 4},
		llmDecision:         &llm.ScalingDecision{TargetReplicas: 10, Reasoning: "llm target", Confidence: 0.9},
		llmProvider:         aiscalerv1.ProviderAnthropic,
		costCeiling:         int32Ptr(6),
		costReason:          "cost budget caps replicas at 6",
		currentWorkloadCost: &cost.WorkloadCost{TotalCost: 4, TotalEfficiency: 0.8},
	}

	if err := reconciler.resolveDecisionState(context.Background(), state, false, true); err != nil {
		t.Fatalf("resolveDecisionState returned error: %v", err)
	}
	if state.decision == nil || state.decision.TargetReplicas != 6 {
		t.Fatalf("expected capped target 6, got %#v", state.decision)
	}
	if state.provider != aiscalerv1.LLMProvider("cost-override") {
		t.Fatalf("expected cost-override provider, got %q", state.provider)
	}
	if state.costEstimate == nil || state.costEstimate.ProposedHourlyCost != 6 {
		t.Fatalf("expected proposed hourly cost 6, got %#v", state.costEstimate)
	}
}

func TestCheckRollbackCreatesSafetyDecision(t *testing.T) {
	reconciler := &AIScalerReconciler{
		RollbackManager: decision.NewRollbackManager(),
		AuditStore: &fakeAuditStore{records: []*audit.DecisionRecord{
			{
				ID:               "decision-1",
				Workload:         "api",
				Applied:          true,
				PreviousReplicas: 3,
				NewReplicas:      6,
				Signals: &plugin.Bundle{
					ErrorRate:    0.001,
					P95LatencyMs: 20,
				},
			},
		}},
	}
	state := &reconcileState{
		obj: &aiscalerv1.AIScaler{
			Spec: aiscalerv1.AIScalerSpec{
				TargetRef: aiscalerv1.TargetRef{Name: "api", Namespace: "default"},
				Safety: &aiscalerv1.SafetyConfig{
					AutoRollback: &aiscalerv1.AutoRollbackConfig{
						Enabled:    true,
						Conditions: []string{"errorRateIncrease"},
					},
				},
			},
		},
		bundle: &plugin.Bundle{CurrentReplicas: 6, ErrorRate: 0.02, P95LatencyMs: 120},
	}

	if _, err := reconciler.checkRollback(context.Background(), state); err != nil {
		t.Fatalf("checkRollback returned error: %v", err)
	}
	if state.safetyDecision == nil || state.safetyDecision.TargetReplicas != 3 {
		t.Fatalf("expected rollback to target 3 replicas, got %#v", state.safetyDecision)
	}
	if state.safetySource != aiscalerv1.LLMProvider("rollback") {
		t.Fatalf("expected rollback safety source, got %q", state.safetySource)
	}
}

func TestEvaluateAlertsHandlesNilAlertingSpec(t *testing.T) {
	reconciler := &AIScalerReconciler{Alerting: alerting.NewEvaluator("", "")}
	state := &reconcileState{
		obj:    &aiscalerv1.AIScaler{},
		bundle: &plugin.Bundle{},
	}

	if _, err := reconciler.evaluateAlerts(context.Background(), state); err != nil {
		t.Fatalf("evaluateAlerts returned error: %v", err)
	}
}

func TestValidateDecisionUsesAsymmetricPolicy(t *testing.T) {
	reconciler := &AIScalerReconciler{Validator: decision.NewValidator()}
	state := &reconcileState{
		obj: &aiscalerv1.AIScaler{
			Spec: aiscalerv1.AIScalerSpec{
				Constraints: aiscalerv1.ScalingConstraints{MinReplicas: 1, MaxReplicas: 20, MaxScaleStep: 10},
				Safety: &aiscalerv1.SafetyConfig{
					ScaleUp: &aiscalerv1.DirectionPolicy{MaxStep: 2},
				},
			},
		},
		bundle:   &plugin.Bundle{CurrentReplicas: 5},
		decision: &llm.ScalingDecision{TargetReplicas: 10, Confidence: 1.0},
	}

	if _, err := reconciler.validateDecision(context.Background(), state); err != nil {
		t.Fatalf("validateDecision returned error: %v", err)
	}
	if state.validation == nil || state.validation.ValidatedReplicas != 7 {
		t.Fatalf("expected asymmetric validation to clamp to 7, got %#v", state.validation)
	}
}

func TestCheckCoordinationDefersWhenMaxConcurrentReached(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := aiscalerv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add AIScaler scheme: %v", err)
	}

	obj := &aiscalerv1.AIScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "api-policy", Namespace: "default"},
		Spec: aiscalerv1.AIScalerSpec{
			TargetRef: aiscalerv1.TargetRef{Name: "api", Namespace: "default"},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).Build()
	coord := coordinator.NewClusterCoordinator(1, 0)
	if err := coord.AcquireScalingSlot("worker"); err != nil {
		t.Fatalf("seed coordinator slot: %v", err)
	}

	reconciler := &AIScalerReconciler{
		Client:              k8sClient,
		Scheme:              scheme,
		Coordinator:         coord,
		CoordinationRequeue: 20 * time.Second,
	}
	state := &reconcileState{
		obj:        obj,
		bundle:     &plugin.Bundle{CurrentReplicas: 2},
		validation: &decision.ValidationResult{ValidatedReplicas: 4},
	}

	result, err := reconciler.checkCoordination(context.Background(), state)
	if err != nil {
		t.Fatalf("checkCoordination returned error: %v", err)
	}
	if result == nil || result.RequeueAfter != 20*time.Second {
		t.Fatalf("expected coordination requeue, got %#v", result)
	}
	if state.coordinationSlotHeld {
		t.Fatal("did not expect slot to be acquired when concurrency limit is exceeded")
	}
}

func TestCheckCoordinationDefersWhenDependencyActive(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := aiscalerv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add AIScaler scheme: %v", err)
	}

	obj := &aiscalerv1.AIScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "api-policy", Namespace: "default"},
		Spec: aiscalerv1.AIScalerSpec{
			TargetRef: aiscalerv1.TargetRef{Name: "api", Namespace: "default"},
			Dependencies: &aiscalerv1.DependencyConfig{
				UpstreamOf: []aiscalerv1.TargetRef{{Name: "db", Namespace: "default"}},
			},
		},
	}
	dep := &aiscalerv1.AIScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "db-policy", Namespace: "default"},
		Spec:       aiscalerv1.AIScalerSpec{TargetRef: aiscalerv1.TargetRef{Name: "db", Namespace: "default"}},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj, dep).Build()
	coord := coordinator.NewClusterCoordinator(0, 0)
	if err := coord.AcquireScalingSlot("db"); err != nil {
		t.Fatalf("seed dependency slot: %v", err)
	}

	reconciler := &AIScalerReconciler{
		Client:              k8sClient,
		Scheme:              scheme,
		Coordinator:         coord,
		CoordinationRequeue: 15 * time.Second,
	}
	state := &reconcileState{
		obj:        obj,
		bundle:     &plugin.Bundle{CurrentReplicas: 3},
		validation: &decision.ValidationResult{ValidatedReplicas: 5},
	}

	result, err := reconciler.checkCoordination(context.Background(), state)
	if err != nil {
		t.Fatalf("checkCoordination returned error: %v", err)
	}
	if result == nil || result.RequeueAfter != 15*time.Second {
		t.Fatalf("expected dependency deferral requeue, got %#v", result)
	}
	if state.coordinationSlotHeld {
		t.Fatal("did not expect slot to be acquired when dependency is active")
	}
}

func TestCheckCoordinationDefersWhenClusterBudgetExceeded(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := aiscalerv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add AIScaler scheme: %v", err)
	}

	obj := &aiscalerv1.AIScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "api-policy", Namespace: "default"},
		Spec:       aiscalerv1.AIScalerSpec{TargetRef: aiscalerv1.TargetRef{Name: "api", Namespace: "default"}},
		Status:     aiscalerv1.AIScalerStatus{Cost: &aiscalerv1.CostStatus{CurrentHourlyCost: 2}},
	}
	other := &aiscalerv1.AIScaler{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-policy", Namespace: "default"},
		Spec:       aiscalerv1.AIScalerSpec{TargetRef: aiscalerv1.TargetRef{Name: "worker", Namespace: "default"}},
		Status:     aiscalerv1.AIScalerStatus{Cost: &aiscalerv1.CostStatus{CurrentHourlyCost: 4}},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj, other).Build()

	reconciler := &AIScalerReconciler{
		Client:               k8sClient,
		Scheme:               scheme,
		Coordinator:          coordinator.NewClusterCoordinator(0, 0),
		CoordinationRequeue:  10 * time.Second,
		ClusterMaxHourlyCost: 6.5,
	}
	state := &reconcileState{
		obj:          obj,
		bundle:       &plugin.Bundle{CurrentReplicas: 2},
		validation:   &decision.ValidationResult{ValidatedReplicas: 4},
		costEstimate: &cost.CostEstimate{CurrentHourlyCost: 2, DeltaHourlyCost: 1, ProposedHourlyCost: 3},
	}

	result, err := reconciler.checkCoordination(context.Background(), state)
	if err != nil {
		t.Fatalf("checkCoordination returned error: %v", err)
	}
	if result == nil || result.RequeueAfter != 10*time.Second {
		t.Fatalf("expected cluster budget deferral requeue, got %#v", result)
	}
	if state.coordinationSlotHeld {
		t.Fatal("did not expect slot to be acquired when cluster budget is exceeded")
	}
}

func TestReleaseCoordinationReleasesHeldSlot(t *testing.T) {
	coord := coordinator.NewClusterCoordinator(1, 0)
	state := &reconcileState{
		obj:                  &aiscalerv1.AIScaler{Spec: aiscalerv1.AIScalerSpec{TargetRef: aiscalerv1.TargetRef{Name: "api"}}},
		coordinationSlotHeld: true,
		coordinationWorkload: "api",
	}
	if err := coord.AcquireScalingSlot("api"); err != nil {
		t.Fatalf("seed slot: %v", err)
	}

	reconciler := &AIScalerReconciler{Coordinator: coord}
	reconciler.releaseCoordination(state)

	if coord.ActiveOperations() != 0 {
		t.Fatalf("expected active operations to be released, got %d", coord.ActiveOperations())
	}
	if state.coordinationSlotHeld {
		t.Fatal("expected coordination slot flag to be cleared")
	}
}
