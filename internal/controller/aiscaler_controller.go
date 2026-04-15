/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/google/uuid"
	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/actuator"
	"github.com/sanjbh/kube-scaling-agent/internal/alerting"
	"github.com/sanjbh/kube-scaling-agent/internal/approval"
	"github.com/sanjbh/kube-scaling-agent/internal/audit"
	"github.com/sanjbh/kube-scaling-agent/internal/coordinator"
	"github.com/sanjbh/kube-scaling-agent/internal/cost"
	"github.com/sanjbh/kube-scaling-agent/internal/decision"
	"github.com/sanjbh/kube-scaling-agent/internal/feedback"
	"github.com/sanjbh/kube-scaling-agent/internal/llm"
	aiscalermetrics "github.com/sanjbh/kube-scaling-agent/internal/metrics"
	"github.com/sanjbh/kube-scaling-agent/internal/node"
	"github.com/sanjbh/kube-scaling-agent/internal/notification"
	"github.com/sanjbh/kube-scaling-agent/internal/plugin"
	"github.com/sanjbh/kube-scaling-agent/internal/prediction"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

const finalizerName = "aiscaler.io/finalizer"

// AIScalerReconciler reconciles a AIScaler object
type AIScalerReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	Router           *llm.Router
	Validator        *decision.Validator
	Actuator         *actuator.Actuator
	VerticalActuator *actuator.VerticalActuator
	MetricsClient    metricsclient.Interface
	Alerting         *alerting.Evaluator

	// Extended subsystems
	OscillationDetector  *decision.OscillationDetector
	PrecedenceResolver   *decision.PrecedenceResolver
	RollbackManager      *decision.RollbackManager
	Coordinator          *coordinator.ClusterCoordinator
	CoordinationRequeue  time.Duration
	ClusterMaxHourlyCost float64
	CostClient           *cost.Client
	CostEstimator        *cost.Estimator
	BudgetEnforcer       *cost.BudgetEnforcer
	AuditStore           audit.Store
	ApprovalManager      *approval.Manager
	Dispatcher           *notification.Dispatcher
	NodeCollector        *node.Collector
	Predictor            *prediction.SeasonalPredictor
	FeedbackEvaluator    *feedback.OutcomeEvaluator
}

// reconcileState carries data between steps within a single reconcile cycle.
type reconcileState struct {
	obj                   *aiscalerv1.AIScaler
	bundle                *plugin.Bundle
	decision              *llm.ScalingDecision
	llmDecision           *llm.ScalingDecision
	reactiveDecision      *llm.ScalingDecision
	deterministicDecision *llm.ScalingDecision
	sloDecision           *llm.ScalingDecision
	safetyDecision        *llm.ScalingDecision
	humanDecision         *llm.ScalingDecision
	provider              aiscalerv1.LLMProvider
	llmProvider           aiscalerv1.LLMProvider
	safetySource          aiscalerv1.LLMProvider
	validation            *decision.ValidationResult
	phase                 aiscalerv1.ScalingPhase
	precedence            *decision.ResolvedDecision
	rollbackAction        *decision.RollbackAction
	currentSLOs           []decision.SLOStatus
	coordinationSlotHeld  bool
	coordinationWorkload  string

	// Extended context
	currentWorkloadCost *cost.WorkloadCost
	costEstimate        *cost.CostEstimate
	costCeiling         *int32
	costReason          string
	nodeCtx             *node.NodeContext
	currentResources    *aiscalerv1.ResourceStatus
	verticalDecision    *aiscalerv1.VerticalDecision
	verticalContext     string
	feedbackContext     string
	feedbackStatus      *aiscalerv1.FeedbackStatus
	verticalApplyResult *actuator.VerticalApplyResult
	decisionID          string
	predictedCPU        float64
	cpuConfidence       float64
	predictedQueueDepth float64
	queueConfidence     float64
}

// StepFunc is a single reconciliation step.
// A non-nil Result or error short-circuits the chain.
// type StepFunc func(ctx context.Context, state *reconcileState) (*ctrl.Result, error)
type StepFunc struct {
	Name string
	Run  func(ctx context.Context, state *reconcileState) (*ctrl.Result, error)
}

// +kubebuilder:rbac:groups=aiscaler.io,resources=aiscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=aiscaler.io,resources=aiscalers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=aiscaler.io,resources=aiscalers/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=metrics.k8s.io,resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the AIScaler object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.4/pkg/reconcile
func (r *AIScalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	reconcileStart := time.Now()

	obj := &aiscalerv1.AIScaler{}
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("Reconciling", "name", obj.Name, "phase", obj.Status.Phase)

	state := &reconcileState{
		obj:        obj,
		phase:      aiscalerv1.PhaseObserving,
		decisionID: uuid.New().String(),
	}
	defer r.releaseCoordination(state)

	steps := []StepFunc{
		{Name: "ensureFinalizer", Run: r.ensureFinalizer},
		{Name: "checkCooldown", Run: r.checkCooldown},
		{Name: "collectSignals", Run: r.collectSignals},
		{Name: "enrichContext", Run: r.enrichContext},
		{Name: "fetchDecision", Run: r.fetchDecision},
		{Name: "checkOscillation", Run: r.checkOscillation},
		{Name: "checkRollback", Run: r.checkRollback},
		{Name: "resolveTentativeDecision", Run: r.resolveTentativeDecision},
		{Name: "estimateCost", Run: r.estimateCost},
		{Name: "checkApproval", Run: r.checkApproval},
		{Name: "resolveFinalDecision", Run: r.resolveFinalDecision},
		{Name: "validateDecision", Run: r.validateDecision},
		{Name: "checkCoordination", Run: r.checkCoordination},
		{Name: "actuate", Run: r.actuate},
		{Name: "evaluateAlerts", Run: r.evaluateAlerts},
		{Name: "recordAudit", Run: r.recordAudit},
		{Name: "updateStatus", Run: r.updateStatus},
	}

	for _, step := range steps {
		log.Info("step started", "step", step.Name)
		result, err := step.Run(ctx, state)
		if err != nil {
			log.Error(err, "step failed", "step", step.Name)
			aiscalermetrics.ReconcileErrors.WithLabelValues(obj.Name, step.Name).Inc()
			r.setCondition(ctx, obj, aiscalerv1.ConditionReady,
				metav1.ConditionFalse, "ReconcileFailed", fmt.Sprintf("%s failed: %v", step.Name, err))
			state.phase = aiscalerv1.PhaseFailed
			if _, statusErr := r.persistStatus(ctx, state, false); statusErr != nil {
				log.Error(statusErr, "failed to persist status after reconcile error", "step", step.Name)
			}
			return ctrl.Result{}, err
		}
		if result != nil {
			if step.Name != "ensureFinalizer" && step.Name != "updateStatus" {
				if _, statusErr := r.persistStatus(ctx, state, false); statusErr != nil {
					log.Error(statusErr, "failed to persist partial status", "step", step.Name)
					return ctrl.Result{}, statusErr
				}
			}
			log.Info("step short-circuited", "step", step.Name)
			aiscalermetrics.ReconcileDuration.WithLabelValues(obj.Name).Observe(time.Since(reconcileStart).Seconds())
			return *result, nil
		}
		log.Info("step completed", "step", step.Name)
	}

	aiscalermetrics.ReconcileDuration.WithLabelValues(obj.Name).Observe(time.Since(reconcileStart).Seconds())

	return ctrl.Result{
		RequeueAfter: obj.Spec.EvaluationInterval.Duration,
	}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *AIScalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiscalerv1.AIScaler{}).
		Owns(&appsv1.Deployment{}).
		Named("aiscaler").
		Complete(r)
}

func (r *AIScalerReconciler) ensureFinalizer(ctx context.Context, state *reconcileState) (*ctrl.Result, error) {
	obj := state.obj
	// handle deletion
	if !obj.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(obj, finalizerName) {
			controllerutil.RemoveFinalizer(obj, finalizerName)
			if err := r.Update(ctx, obj); err != nil {
				return nil, err
			}
		}

		result := &ctrl.Result{}
		return result, nil
	}

	// Validate spec before doing any real work
	if err := obj.Spec.ValidateSpec(); err != nil {
		logf.FromContext(ctx).Error(err, "invalid AIScaler spec")
		r.setCondition(
			ctx,
			obj,
			aiscalerv1.ConditionReady,
			metav1.ConditionFalse,
			"InvalidSpec",
			err.Error(),
		)
		if err := r.Status().Update(ctx, obj); err != nil {
			return nil, err
		}
		// Don't requeue — spec won't fix itself without a user edit
		return &ctrl.Result{}, nil
	}

	// Add finalizer if missing
	if !controllerutil.ContainsFinalizer(obj, finalizerName) {
		controllerutil.AddFinalizer(obj, finalizerName)
		if err := r.Update(ctx, obj); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (r *AIScalerReconciler) checkCooldown(ctx context.Context, state *reconcileState) (*ctrl.Result, error) {
	obj := state.obj

	if obj.Status.LastScaleTime == nil {
		return nil, nil
	}

	elapsed := time.Since(obj.Status.LastScaleTime.Time)
	if elapsed < obj.Spec.CooldownPeriod.Duration {
		remaining := obj.Spec.CooldownPeriod.Duration - elapsed
		state.phase = aiscalerv1.PhaseCoolingDown
		r.setCondition(ctx, obj, aiscalerv1.ConditionScaling,
			metav1.ConditionFalse, "Cooldown", fmt.Sprintf("waiting %s before next scaling action", remaining.Round(time.Second)))
		logf.FromContext(ctx).Info("in cooldown", "remaining", remaining)
		result := &ctrl.Result{
			RequeueAfter: remaining,
		}
		return result, nil
	}
	return nil, nil
}

func (r *AIScalerReconciler) collectSignals(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {

	log := logf.FromContext(ctx)

	// Create a signal manager from the CRD's signal config on each reconcile.
	// This allows dynamic reconfiguration without operator restart.
	k8sDeps := plugin.K8sPluginDeps{
		Client:        r.Client,
		MetricsClient: r.MetricsClient,
	}
	mgr, err := plugin.NewManager(ctx, state.obj.Spec.Signals, k8sDeps)
	if err != nil {
		log.Error(err, "failed to create signal manager")
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionSignalsReady,
			metav1.ConditionFalse, "ManagerCreateFailed", err.Error())
		return nil, err
	}

	bundle, err := mgr.Collect(ctx, state.obj)
	if err != nil {
		log.Error(err, "failed to collect signals")
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionSignalsReady,
			metav1.ConditionFalse, "CollectionFailed", err.Error())
		return nil, err
	}

	// Update plugin health metrics
	for name, healthy := range bundle.SourceHealth {
		val := 0.0
		if healthy {
			val = 1.0
		}
		aiscalermetrics.SignalPluginHealth.WithLabelValues(state.obj.Name, name).Set(val)
	}

	state.bundle = bundle

	// Check freeze annotation — if active, skip scaling entirely
	if bundle.Annotations.FreezeUntil != nil && time.Now().Before(*bundle.Annotations.FreezeUntil) {
		log.Info("scaling frozen", "until", *bundle.Annotations.FreezeUntil)
		remaining := time.Until(*bundle.Annotations.FreezeUntil)
		state.phase = aiscalerv1.PhaseObserving
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionScaling,
			metav1.ConditionFalse, "Frozen", fmt.Sprintf("scaling frozen until %s", bundle.Annotations.FreezeUntil.Format(time.RFC3339)))
		result := &ctrl.Result{RequeueAfter: remaining}
		return result, nil
	}

	r.setCondition(ctx, state.obj, aiscalerv1.ConditionSignalsReady,
		metav1.ConditionTrue, "Collected", "all signals collected successfully")

	// Update seasonal predictor baselines with current observations
	if r.Predictor != nil {
		if bundle.CPUUtilization > 0 {
			r.Predictor.UpdateBaseline("cpu", bundle.CPUUtilization)
		}
		if bundle.QueueDepth > 0 {
			r.Predictor.UpdateBaseline("queue_depth", bundle.QueueDepth)
		}
	}

	return nil, nil
}

// enrichContext gathers node, cost, and predictive context to embed in the LLM prompt.
func (r *AIScalerReconciler) enrichContext(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Node context
	if r.NodeCollector != nil {
		nc, err := r.NodeCollector.Collect(ctx)
		if err != nil {
			log.Error(err, "failed to collect node context, continuing without it")
		} else {
			state.nodeCtx = nc
		}
	}

	// Predictive context
	if r.Predictor != nil {
		state.predictedCPU, state.cpuConfidence = r.Predictor.Predict("cpu", 1*time.Hour)
		if state.cpuConfidence > 0 {
			aiscalermetrics.PredictionConfidence.WithLabelValues(state.obj.Name, "cpu").Set(state.cpuConfidence)
		}
		state.predictedQueueDepth, state.queueConfidence = r.Predictor.Predict("queue_depth", 1*time.Hour)
		if state.queueConfidence > 0 {
			aiscalermetrics.PredictionConfidence.WithLabelValues(state.obj.Name, "queue_depth").Set(state.queueConfidence)
		}
		if state.predictedCPU > 0 || state.predictedQueueDepth > 0 {
			log.Info("predictive forecast",
				"cpuPred", state.predictedCPU, "cpuConf", state.cpuConfidence,
				"queuePred", state.predictedQueueDepth, "queueConf", state.queueConfidence)
		}
	}

	if state.obj.Spec.VerticalScaling != nil && state.obj.Spec.VerticalScaling.Enabled {
		resources, err := r.readCurrentResources(ctx, state.obj)
		if err != nil {
			log.Error(err, "failed to read current resources, continuing without vertical context")
		} else {
			state.currentResources = resources
			state.verticalContext = buildVerticalContext(resources, state.obj.Spec.VerticalScaling)
		}
	}

	if state.obj.Spec.Feedback != nil && state.obj.Spec.Feedback.Enabled && r.FeedbackEvaluator != nil {
		historyDepth := int(state.obj.Spec.Feedback.HistoryDepth)
		if historyDepth <= 0 {
			historyDepth = 10
		}
		outcomes, err := r.FeedbackEvaluator.Evaluate(
			ctx,
			state.obj.Spec.TargetRef.Name,
			state.bundle,
			state.obj.Spec.Feedback.EvaluationDelay.Duration,
			historyDepth,
		)
		if err != nil {
			log.Error(err, "failed to evaluate feedback, continuing without it")
		} else {
			state.feedbackStatus = summarizeFeedback(outcomes)
			state.feedbackContext = buildFeedbackContext(outcomes, state.obj.Spec.Feedback.IncludeInPrompt)
		}
	}

	return nil, nil
}

func (r *AIScalerReconciler) fetchDecision(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {

	log := logf.FromContext(ctx)
	obj := state.obj
	b := state.bundle

	log.Info("signal bundle",
		"cpu", b.CPUUtilization,
		"memory", b.MemoryUtilization,
		"currentReplicas", b.CurrentReplicas,
		"readyReplicas", b.ReadyReplicas,
		"deploymentReady", b.DeploymentReady,
	)

	if obj.Spec.KEDAIntegration != nil && obj.Spec.KEDAIntegration.Enabled && b.KEDADesiredReplicas > 0 {
		state.deterministicDecision = &llm.ScalingDecision{
			TargetReplicas: b.KEDADesiredReplicas,
			Reasoning:      fmt.Sprintf("KEDA recommends %d replicas", b.KEDADesiredReplicas),
			Confidence:     1.0,
			ActionType:     actionTypeForTarget(b.KEDADesiredReplicas, b.CurrentReplicas),
			Urgency:        "medium",
		}
	}

	// --- Reactive rules: evaluate before LLM call ---
	if len(b.Annotations.ReactiveRules) > 0 {
		if result := decision.EvaluateReactiveRules(b.Annotations.ReactiveRules, b); result != nil {
			log.Info("reactive rule fired",
				"metric", result.Rule.Metric, "action", result.Rule.Action,
				"replicas", result.Replicas)
			state.reactiveDecision = &llm.ScalingDecision{
				TargetReplicas: result.Replicas,
				Reasoning:      fmt.Sprintf("reactive rule: %s %s %.2f (actual=%.2f)", result.Rule.Metric, result.Rule.Operator, result.Rule.Threshold, result.Actual),
				Confidence:     1.0,
				ActionType:     actionTypeForTarget(result.Replicas, b.CurrentReplicas),
			}
			if shouldSkipLLMForReactiveRules(obj.Spec.Precedence) {
				r.setCondition(ctx, state.obj, aiscalerv1.ConditionLLMReady,
					metav1.ConditionTrue, "ReactiveRule", state.reactiveDecision.Reasoning)
				return nil, nil
			}
		}
	}

	// --- SLO evaluation: build context for LLM prompt ---
	var sloContext string
	if len(obj.Spec.SLOs) > 0 {
		sloStatuses := decision.EvaluateSLOs(obj.Spec.SLOs, b)
		state.currentSLOs = sloStatuses
		sloContext = decision.FormatSLOContext(sloStatuses)

		// Track SLO breaches in metrics
		for _, s := range sloStatuses {
			val := 0.0
			if s.Breached {
				val = 1.0
			}
			aiscalermetrics.SLOBreach.WithLabelValues(obj.Name, s.Name, s.Metric).Set(val)
		}
	}

	scalingRequest := llm.ScalingRequest{
		PolicyName:          obj.Name,
		CurrentReplicas:     b.CurrentReplicas,
		Namespace:           obj.Spec.TargetRef.Namespace,
		MinReplicas:         obj.Spec.Constraints.MinReplicas,
		MaxReplicas:         obj.Spec.Constraints.MaxReplicas,
		MaxScaleStep:        obj.Spec.Constraints.MaxScaleStep,
		CPUUtilization:      b.CPUUtilization,
		MemoryUtilization:   b.MemoryUtilization,
		P95LatencyMs:        b.P95LatencyMs,
		ErrorRate:           b.ErrorRate,
		QueueDepth:          b.QueueDepth,
		DeploymentReady:     b.DeploymentReady,
		ExpectedTraffic:     b.Annotations.ExpectedTraffic,
		ScaleConservatively: b.Annotations.ScaleConservatively,
		Note:                b.Annotations.Note,
		PeakHours:           b.Annotations.PeakHours,
		CustomSignals:       b.CustomSignals,
		SLOContext:          sloContext,
		PredictiveContext:   buildPredictiveContext(state.predictedCPU, state.cpuConfidence, state.predictedQueueDepth, state.queueConfidence),
		NodeContext:         buildNodeContext(state.nodeCtx),
		VerticalContext:     state.verticalContext,
		FeedbackContext:     state.feedbackContext,
	}

	llmStart := time.Now()
	scalingDecision, provider, err := r.Router.Decide(ctx, state.obj, scalingRequest)
	llmDuration := time.Since(llmStart)

	model := obj.Spec.LLM.Model
	if model == "" {
		model = "default"
	}
	aiscalermetrics.LLMLatency.WithLabelValues(string(provider), model).Observe(llmDuration.Seconds())

	if err != nil {
		log.Error(err, "failed to decide")
		aiscalermetrics.LLMErrors.WithLabelValues(string(obj.Spec.LLM.Primary), aiscalermetrics.ClassifyError(err)).Inc()
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionLLMReady,
			metav1.ConditionFalse, "DecisionFailed", err.Error())
		if r.Dispatcher != nil {
			r.Dispatcher.Dispatch(notification.Event{
				Type:      notification.EventLLMFailure,
				Severity:  notification.SeverityWarning,
				Workload:  state.obj.Spec.TargetRef.Name,
				Namespace: state.obj.Spec.TargetRef.Namespace,
				Message:   err.Error(),
				Timestamp: time.Now(),
			})
		}
		if state.reactiveDecision != nil || state.deterministicDecision != nil {
			return nil, nil
		}
		return nil, err
	}

	aiscalermetrics.LLMConfidence.WithLabelValues(string(provider)).Observe(scalingDecision.Confidence)
	state.llmProvider = provider

	// Confidence gating
	minConf := obj.Spec.Constraints.MinConfidence
	if minConf > 0 && scalingDecision.Confidence < minConf {
		log.Info("LLM confidence below threshold, skipping scaling",
			"confidence", scalingDecision.Confidence, "threshold", minConf)
		if state.reactiveDecision == nil && state.deterministicDecision == nil {
			state.llmDecision = &llm.ScalingDecision{
				TargetReplicas: b.CurrentReplicas,
				Reasoning:      fmt.Sprintf("confidence %.2f below threshold %.2f", scalingDecision.Confidence, minConf),
				Confidence:     scalingDecision.Confidence,
				ActionType:     "no_change",
			}
		}
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionLLMReady,
			metav1.ConditionTrue, "LowConfidence", "confidence below threshold, maintaining current replicas")
		return nil, nil
	}

	log.Info("LLM decision",
		"provider", provider,
		"target", scalingDecision.TargetReplicas,
		"confidence", scalingDecision.Confidence,
	)
	if obj.Spec.VerticalScaling != nil && obj.Spec.VerticalScaling.Enabled {
		verticalDecision, vErr := buildVerticalDecision(obj.Spec.VerticalScaling, scalingDecision.VerticalChanges)
		if vErr != nil {
			log.Error(vErr, "ignoring invalid vertical recommendation")
		} else {
			state.verticalDecision = verticalDecision
		}
	}

	state.llmDecision = scalingDecision

	r.setCondition(ctx, state.obj, aiscalerv1.ConditionLLMReady,
		metav1.ConditionTrue, "DecisionMade", scalingDecision.Reasoning)
	return nil, nil
}

// checkOscillation detects rapid up-down scaling patterns and blocks if oscillating.
func (r *AIScalerReconciler) checkOscillation(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	if r.OscillationDetector == nil {
		return nil, nil
	}

	if r.OscillationDetector.IsOscillating() {
		logf.FromContext(ctx).Info("oscillation detected, holding current replicas")
		aiscalermetrics.OscillationDetected.WithLabelValues(state.obj.Name).Set(1)

		if r.Dispatcher != nil {
			r.Dispatcher.Dispatch(notification.Event{
				Type:      notification.EventOscillationDetected,
				Severity:  notification.SeverityWarning,
				Workload:  state.obj.Spec.TargetRef.Name,
				Namespace: state.obj.Spec.TargetRef.Namespace,
				Message:   "Oscillation detected — holding current replicas",
				Timestamp: time.Now(),
			})
		}

		state.safetyDecision = &llm.ScalingDecision{
			TargetReplicas: state.bundle.CurrentReplicas,
			Reasoning:      "oscillation detected, maintaining current replicas",
			Confidence:     1.0,
			ActionType:     "no_change",
		}
		state.safetySource = aiscalerv1.LLMProvider("safety")
		return nil, nil
	}

	aiscalermetrics.OscillationDetected.WithLabelValues(state.obj.Name).Set(0)
	return nil, nil
}

func (r *AIScalerReconciler) checkRollback(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	if r.RollbackManager == nil || r.AuditStore == nil || state.bundle == nil {
		return nil, nil
	}

	safety := state.obj.Spec.Safety
	if safety == nil || safety.AutoRollback == nil || !safety.AutoRollback.Enabled || len(safety.AutoRollback.Conditions) == 0 {
		return nil, nil
	}

	records, err := r.AuditStore.List(ctx, state.obj.Spec.TargetRef.Name, 5)
	if err != nil {
		logf.FromContext(ctx).Error(err, "failed to load audit history for rollback evaluation")
		return nil, nil
	}

	record := selectRollbackRecord(records, state.bundle.CurrentReplicas)
	if record == nil || record.Signals == nil {
		return nil, nil
	}

	beforeSLOsMet := true
	afterSLOsMet := true
	if len(state.obj.Spec.SLOs) > 0 {
		beforeSLOsMet = !decision.AnyBreached(decision.EvaluateSLOs(state.obj.Spec.SLOs, record.Signals))
		if len(state.currentSLOs) > 0 {
			afterSLOsMet = !decision.AnyBreached(state.currentSLOs)
		} else {
			afterSLOsMet = !decision.AnyBreached(decision.EvaluateSLOs(state.obj.Spec.SLOs, state.bundle))
		}
	}

	action := r.RollbackManager.CheckForRollback(&decision.RollbackInput{
		PreviousReplicas:    record.PreviousReplicas,
		CurrentReplicas:     state.bundle.CurrentReplicas,
		SLOsMetBefore:       beforeSLOsMet,
		SLOsMetAfter:        afterSLOsMet,
		ErrorRateBefore:     record.Signals.ErrorRate,
		ErrorRateAfter:      state.bundle.ErrorRate,
		LatencyBefore:       record.Signals.P95LatencyMs,
		LatencyAfter:        state.bundle.P95LatencyMs,
		CrashLoopDetected:   false,
		AutoRollbackEnabled: true,
		RollbackConditions:  safety.AutoRollback.Conditions,
	})
	if !action.Recommended {
		return nil, nil
	}

	state.rollbackAction = action
	state.safetyDecision = &llm.ScalingDecision{
		TargetReplicas: action.TargetReplicas,
		Reasoning:      action.Reason,
		Confidence:     1.0,
		ActionType:     actionTypeForTarget(action.TargetReplicas, state.bundle.CurrentReplicas),
	}
	state.safetySource = aiscalerv1.LLMProvider("rollback")

	if r.Dispatcher != nil {
		r.Dispatcher.Dispatch(notification.Event{
			Type:      notification.EventRollbackTriggered,
			Severity:  notification.SeverityWarning,
			Workload:  state.obj.Spec.TargetRef.Name,
			Namespace: state.obj.Spec.TargetRef.Namespace,
			Message:   action.Reason,
			Timestamp: time.Now(),
		})
	}

	return nil, nil
}

func (r *AIScalerReconciler) resolveTentativeDecision(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	return nil, r.resolveDecisionState(ctx, state, false, false)
}

func (r *AIScalerReconciler) resolveFinalDecision(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	return nil, r.resolveDecisionState(ctx, state, true, true)
}

// estimateCost computes the cost delta for the proposed scaling decision.
func (r *AIScalerReconciler) estimateCost(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	if r.CostClient == nil || r.CostEstimator == nil {
		return nil, nil
	}
	if state.bundle == nil || state.decision == nil {
		return nil, nil
	}

	log := logf.FromContext(ctx)

	wc, err := r.CostClient.GetWorkloadCost(ctx, state.obj.Spec.TargetRef.Namespace)
	if err != nil {
		log.Error(err, "failed to get workload cost, continuing without cost estimate")
		return nil, nil
	}
	state.currentWorkloadCost = wc

	estimate, err := r.CostEstimator.Estimate(wc, state.bundle.CurrentReplicas, state.decision.TargetReplicas)
	if err != nil {
		log.Error(err, "failed to estimate cost")
		return nil, nil
	}

	state.costEstimate = estimate

	aiscalermetrics.CostSavingsHourly.WithLabelValues(state.obj.Name).Set(-estimate.DeltaHourlyCost)
	aiscalermetrics.WorkloadWaste.WithLabelValues(state.obj.Name).Set(estimate.WasteReduction)

	// Budget enforcement
	if r.BudgetEnforcer != nil && state.obj.Spec.CostConstraints != nil {
		cc := state.obj.Spec.CostConstraints
		result := r.BudgetEnforcer.Check(cc.MaxHourlyCost, cc.MaxMonthlyCost, cc.Enforcement, estimate)
		if !result.Allowed {
			log.Info("budget exceeded, capping decision", "reason", result.Reason)

			if r.Dispatcher != nil {
				r.Dispatcher.Dispatch(notification.Event{
					Type:      notification.EventBudgetExceeded,
					Severity:  notification.SeverityWarning,
					Workload:  state.obj.Spec.TargetRef.Name,
					Namespace: state.obj.Spec.TargetRef.Namespace,
					Message:   result.Reason,
					Timestamp: time.Now(),
				})
			}
			state.costCeiling, state.costReason = computeCostCeiling(state.obj.Spec.CostConstraints, estimate, state.obj.Spec.Constraints.MinReplicas)
			if state.costCeiling == nil {
				current := state.bundle.CurrentReplicas
				state.costCeiling = &current
			}
			if state.costReason == "" {
				state.costReason = result.Reason
			}
		}
	}

	return nil, nil
}

// checkApproval determines if human approval is needed before actuation.
func (r *AIScalerReconciler) checkApproval(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	if r.ApprovalManager == nil {
		return nil, nil
	}
	if state.bundle == nil || state.decision == nil {
		return nil, nil
	}

	costDelta := 0.0
	if state.costEstimate != nil {
		costDelta = state.costEstimate.DeltaHourlyCost
	}

	if r.ApprovalManager.NeedsApproval(state.obj, state.decision, state.bundle.CurrentReplicas, costDelta) {
		logf.FromContext(ctx).Info("approval required, holding decision")
		aiscalermetrics.ApprovalPending.WithLabelValues(state.obj.Name).Set(1)

		if r.Dispatcher != nil {
			req := r.ApprovalManager.CreateRequest(state.obj, state.decision, state.bundle.CurrentReplicas, costDelta)
			r.Dispatcher.Dispatch(notification.Event{
				Type:      notification.EventApprovalRequired,
				Severity:  notification.SeverityWarning,
				Workload:  state.obj.Spec.TargetRef.Name,
				Namespace: state.obj.Spec.TargetRef.Namespace,
				Message:   fmt.Sprintf("Approval required: scale %s from %d to %d", req.Workload, req.CurrentReplicas, req.TargetReplicas),
				Timestamp: time.Now(),
			})
		}

		state.humanDecision = &llm.ScalingDecision{
			TargetReplicas: state.bundle.CurrentReplicas,
			Reasoning:      "awaiting human approval",
			Confidence:     state.decision.Confidence,
			ActionType:     "no_change",
		}
		return nil, nil
	}

	state.humanDecision = nil
	aiscalermetrics.ApprovalPending.WithLabelValues(state.obj.Name).Set(0)
	return nil, nil
}

// validateDecision never short-circuits the chain — it always passes through.
func (r *AIScalerReconciler) validateDecision(ctx context.Context, state *reconcileState) (*ctrl.Result, error) {
	if state.decision == nil || state.bundle == nil {
		return nil, nil
	}
	var validationResult *decision.ValidationResult
	if state.obj.Spec.Safety != nil && (state.obj.Spec.Safety.ScaleUp != nil || state.obj.Spec.Safety.ScaleDown != nil) {
		validationResult = &decision.ValidateAsymmetric(state.decision, state.bundle.CurrentReplicas, state.obj).ValidationResult
	} else {
		validationResult = r.Validator.Validate(state.decision, state.bundle.CurrentReplicas, state.obj)
	}
	if validationResult.Clamped {
		logf.FromContext(ctx).Info(
			"decision clamped by validator",
			"original", validationResult.OriginalReplicas,
			"validated", validationResult.ValidatedReplicas,
			"reason", validationResult.Reason,
		)
		aiscalermetrics.ValidationClamps.WithLabelValues(state.obj.Name, validationResult.Reason).Inc()
	}
	state.validation = validationResult
	return nil, nil
}

func (r *AIScalerReconciler) checkCoordination(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	if r.Coordinator == nil || state.obj == nil || state.bundle == nil || !needsLiveCoordination(state) {
		return nil, nil
	}

	policies := &aiscalerv1.AIScalerList{}
	if err := r.List(ctx, policies); err != nil {
		return nil, fmt.Errorf("list AIScaler policies for coordination: %w", err)
	}

	workload := coordinationWorkloadName(state.obj)
	active := r.Coordinator.ActiveWorkloads()
	delete(active, workload)

	graph := coordinator.NewDependencyGraph()
	graph.BuildFromPolicies(policies.Items)
	if graph.ShouldDefer(workload, active) {
		msg := "dependent workload is currently scaling; deferring coordinated change"
		logf.FromContext(ctx).Info(msg, "workload", workload)
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionScaling,
			metav1.ConditionFalse, "DependencyDeferred", msg)
		state.phase = aiscalerv1.PhaseObserving
		return &ctrl.Result{RequeueAfter: r.coordinationDelay()}, nil
	}

	if r.ClusterMaxHourlyCost > 0 && exceedsClusterBudget(policies.Items, state, r.ClusterMaxHourlyCost) {
		msg := fmt.Sprintf("projected cluster hourly cost would exceed limit $%.2f", r.ClusterMaxHourlyCost)
		logf.FromContext(ctx).Info(msg, "workload", workload)
		if r.Dispatcher != nil {
			r.Dispatcher.Dispatch(notification.Event{
				Type:      notification.EventBudgetExceeded,
				Severity:  notification.SeverityWarning,
				Workload:  state.obj.Spec.TargetRef.Name,
				Namespace: state.obj.Spec.TargetRef.Namespace,
				Message:   msg,
				Timestamp: time.Now(),
			})
		}
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionScaling,
			metav1.ConditionFalse, "ClusterBudgetDeferred", msg)
		state.phase = aiscalerv1.PhaseObserving
		return &ctrl.Result{RequeueAfter: r.coordinationDelay()}, nil
	}

	if err := r.Coordinator.AcquireScalingSlot(workload); err != nil {
		logf.FromContext(ctx).Info("cluster coordinator deferred scaling", "workload", workload, "reason", err.Error())
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionScaling,
			metav1.ConditionFalse, "CoordinatorDeferred", err.Error())
		state.phase = aiscalerv1.PhaseObserving
		return &ctrl.Result{RequeueAfter: r.coordinationDelay()}, nil
	}

	state.coordinationSlotHeld = true
	state.coordinationWorkload = workload
	return nil, nil
}

func (r *AIScalerReconciler) actuate(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if state.validation == nil && state.verticalDecision == nil {
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionScaling,
			metav1.ConditionFalse, "NoDecision", "no scaling changes to apply")
		return nil, nil
	}

	var (
		applyResult *actuator.ApplyResult
		err         error
	)
	if state.validation != nil {
		applyResult, err = r.Actuator.Apply(ctx, state.obj, state.validation)
		if err != nil {
			log.Error(err, "failed to apply scaling decision")
			return nil, err
		}
	}

	if state.verticalDecision != nil {
		if r.VerticalActuator == nil {
			log.Info("vertical recommendation skipped because vertical actuator is not configured")
		} else {
			state.verticalApplyResult, err = r.VerticalActuator.Apply(ctx, state.obj, state.verticalDecision)
			if err != nil {
				log.Error(err, "failed to apply vertical scaling decision")
				return nil, err
			}
		}
	}

	// Determine direction for metrics
	direction := "none"
	if applyResult != nil {
		if applyResult.AppliedReplicas > applyResult.PreviousReplicas {
			direction = "up"
		} else if applyResult.AppliedReplicas < applyResult.PreviousReplicas {
			direction = "down"
		}
	}

	dryRun := false
	if applyResult != nil && applyResult.DryRun {
		dryRun = true
		log.Info("dry run — decision computed but not applied",
			"replicas", applyResult.AppliedReplicas)
		aiscalermetrics.DryRunDecisions.WithLabelValues(state.obj.Name, direction).Inc()
	}
	if state.verticalApplyResult != nil && state.verticalApplyResult.DryRun {
		dryRun = true
	}

	if applyResult != nil {
		aiscalermetrics.ScalingDecisions.WithLabelValues(state.obj.Name, string(state.provider), direction).Inc()
		aiscalermetrics.CurrentReplicas.WithLabelValues(state.obj.Name,
			state.obj.Spec.TargetRef.Namespace, state.obj.Spec.TargetRef.Name).Set(float64(applyResult.AppliedReplicas))
		aiscalermetrics.DesiredReplicas.WithLabelValues(state.obj.Name,
			state.obj.Spec.TargetRef.Namespace, state.obj.Spec.TargetRef.Name).Set(float64(state.validation.ValidatedReplicas))

		log.Info("Scaling applied",
			"previous", applyResult.PreviousReplicas,
			"applied", applyResult.AppliedReplicas,
		)

		// Record scaling event for oscillation detection
		if r.OscillationDetector != nil && direction != "none" {
			r.OscillationDetector.Record(decision.ScalingEvent{
				Timestamp: time.Now(),
				Direction: direction,
				From:      applyResult.PreviousReplicas,
				To:        applyResult.AppliedReplicas,
			})
		}

		// Dispatch scaling-applied notification
		if r.Dispatcher != nil && direction != "none" {
			r.Dispatcher.Dispatch(notification.Event{
				Type:      notification.EventScalingApplied,
				Severity:  notification.SeverityInfo,
				Workload:  state.obj.Spec.TargetRef.Name,
				Namespace: state.obj.Spec.TargetRef.Namespace,
				Message:   fmt.Sprintf("Scaled %s→%d (was %d)", direction, applyResult.AppliedReplicas, applyResult.PreviousReplicas),
				Timestamp: time.Now(),
			})
		}
	}

	verticalApplied := state.verticalApplyResult != nil && state.verticalApplyResult.Applied
	verticalRecommended := state.verticalApplyResult != nil && state.verticalApplyResult.RecommendOnly

	switch {
	case dryRun:
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionScaling,
			metav1.ConditionFalse, "DryRun", "scaling decision computed in dry-run mode")
	case verticalRecommended && direction == "none":
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionScaling,
			metav1.ConditionFalse, "RecommendOnly", "vertical recommendation generated but not applied")
	case direction != "none" || verticalApplied:
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionScaling,
			metav1.ConditionTrue, "Applied", "scaling changes applied successfully")
	default:
		r.setCondition(ctx, state.obj, aiscalerv1.ConditionScaling,
			metav1.ConditionFalse, "NoChange", "workload already at desired scale")
	}

	return nil, nil
}

func (r *AIScalerReconciler) releaseCoordination(state *reconcileState) {
	if r.Coordinator == nil || state == nil || !state.coordinationSlotHeld {
		return
	}
	workload := state.coordinationWorkload
	if workload == "" && state.obj != nil {
		workload = coordinationWorkloadName(state.obj)
	}
	if workload == "" {
		return
	}
	r.Coordinator.ReleaseScalingSlot(workload)
	state.coordinationSlotHeld = false
}

func (r *AIScalerReconciler) evaluateAlerts(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	obj := state.obj

	if r.Alerting == nil || obj.Spec.Alerting == nil || !obj.Spec.Alerting.Enabled || len(obj.Spec.Alerting.Rules) == 0 {
		return nil, nil
	}

	fired := r.Alerting.Evaluate(state.bundle, obj.Spec.Alerting.Rules)
	if len(fired) > 0 {
		log := logf.FromContext(ctx)
		log.Info("alerts fired", "count", len(fired))
		if err := r.Alerting.Notify(ctx, fired); err != nil {
			log.Error(err, "failed to send alert notifications")
			// Don't fail the reconcile for notification errors
		}
	}

	return nil, nil
}

// recordAudit persists a decision audit record.
func (r *AIScalerReconciler) recordAudit(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	if r.AuditStore == nil || state.bundle == nil || state.decision == nil || state.validation == nil {
		return nil, nil
	}

	record := &audit.DecisionRecord{
		ID:             state.decisionID,
		Timestamp:      time.Now(),
		Workload:       state.obj.Spec.TargetRef.Name,
		Namespace:      state.obj.Spec.TargetRef.Namespace,
		Signals:        state.bundle,
		Provider:       string(state.provider),
		ParsedDecision: state.decision,
		PreValidation: &decision.ValidationResult{
			OriginalReplicas:  state.decision.TargetReplicas,
			ValidatedReplicas: state.decision.TargetReplicas,
		},
		PostValidation:   state.validation,
		WasClamped:       state.validation.Clamped,
		PreviousReplicas: state.bundle.CurrentReplicas,
		NewReplicas:      state.validation.ValidatedReplicas,
		Applied:          state.validation.ValidatedReplicas != state.bundle.CurrentReplicas,
		DryRun:           state.obj.Spec.DryRun,
	}

	if state.costEstimate != nil {
		record.CostDeltaHourly = state.costEstimate.DeltaHourlyCost
		record.CostDeltaMonthly = state.costEstimate.DeltaMonthlyCost
	}

	if err := r.AuditStore.Store(ctx, record); err != nil {
		logf.FromContext(ctx).Error(err, "failed to persist audit record")
		// Don't fail reconcile for audit errors
	}

	return nil, nil
}

func (r *AIScalerReconciler) updateStatus(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	return r.persistStatus(ctx, state, true)
}

func (r *AIScalerReconciler) setCondition(
	ctx context.Context,
	obj *aiscalerv1.AIScaler,
	condType string,
	status metav1.ConditionStatus,
	reason, message string,
) {
	apimeta.SetStatusCondition(&obj.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: obj.Generation,
	})
}

func (r *AIScalerReconciler) persistStatus(
	ctx context.Context,
	state *reconcileState,
	markReady bool,
) (*ctrl.Result, error) {
	obj := state.obj
	now := metav1.Now()

	if state.phase != "" {
		obj.Status.Phase = state.phase
	} else if obj.Status.Phase == "" {
		obj.Status.Phase = aiscalerv1.PhaseObserving
	}

	if state.bundle != nil {
		obj.Status.CurrentReplicas = state.bundle.CurrentReplicas
		if state.decision == nil && state.validation == nil {
			obj.Status.DesiredReplicas = state.bundle.CurrentReplicas
		}
	}
	if state.validation != nil {
		obj.Status.DesiredReplicas = state.validation.ValidatedReplicas
	} else if state.decision != nil {
		obj.Status.DesiredReplicas = state.decision.TargetReplicas
	}
	if state.provider != "" {
		obj.Status.LastProvider = state.provider
	}
	if state.decision != nil && state.decision.Reasoning != "" {
		obj.Status.LastDecisionReason = state.decision.Reasoning
	}
	if state.decisionID != "" {
		obj.Status.LastDecisionID = state.decisionID
	}
	if state.currentResources != nil {
		resources := *state.currentResources
		obj.Status.CurrentResources = &resources
	}
	if state.verticalApplyResult != nil {
		if obj.Status.CurrentResources == nil {
			obj.Status.CurrentResources = &aiscalerv1.ResourceStatus{}
		}
		if state.verticalApplyResult.Applied && !state.verticalApplyResult.DryRun {
			obj.Status.CurrentResources.CPURequest = state.verticalApplyResult.AppliedCPU
			obj.Status.CurrentResources.MemoryRequest = state.verticalApplyResult.AppliedMemory
			if state.verticalDecision != nil {
				if !state.verticalDecision.CPULimit.IsZero() {
					obj.Status.CurrentResources.CPULimit = state.verticalDecision.CPULimit
				}
				if !state.verticalDecision.MemoryLimit.IsZero() {
					obj.Status.CurrentResources.MemoryLimit = state.verticalDecision.MemoryLimit
				}
			}
			obj.Status.CurrentResources.LastResizeStrategy = state.verticalApplyResult.Strategy
			obj.Status.CurrentResources.LastResizeTime = &now
		} else if state.verticalApplyResult.RecommendOnly || state.verticalApplyResult.DryRun {
			obj.Status.CurrentResources.LastResizeStrategy = state.verticalApplyResult.Strategy
		}
	}
	if state.costEstimate != nil {
		costStatus := &aiscalerv1.CostStatus{
			CurrentHourlyCost:  state.costEstimate.CurrentHourlyCost,
			CurrentMonthlyCost: state.costEstimate.CurrentHourlyCost * 24 * 30,
			WastePercent:       state.costEstimate.WasteReduction,
		}
		if obj.Spec.CostConstraints != nil {
			costStatus.BudgetUtilization = computeBudgetUtilization(obj.Spec.CostConstraints, state.costEstimate)
		}
		obj.Status.Cost = costStatus
	}
	if state.feedbackStatus != nil {
		feedbackStatus := *state.feedbackStatus
		obj.Status.Feedback = &feedbackStatus
	}
	if state.precedence != nil {
		obj.Status.Precedence = buildPrecedenceStatus(state.precedence)
	} else {
		obj.Status.Precedence = nil
	}

	if state.bundle != nil && state.validation != nil && state.validation.ValidatedReplicas != state.bundle.CurrentReplicas && !obj.Spec.DryRun {
		obj.Status.Phase = aiscalerv1.PhaseScaling
		obj.Status.LastScaleTime = &now
	}
	if state.verticalApplyResult != nil && state.verticalApplyResult.Applied && !state.verticalApplyResult.DryRun {
		obj.Status.Phase = aiscalerv1.PhaseScaling
	}
	if obj.Status.Phase == "" {
		obj.Status.Phase = aiscalerv1.PhaseObserving
	}

	if markReady {
		r.setCondition(
			ctx,
			obj,
			aiscalerv1.ConditionReady,
			metav1.ConditionTrue,
			"Reconciled",
			"reconcile cycle completed successfully",
		)
	}

	if err := r.Status().Update(ctx, obj); err != nil {
		if apierrors.IsConflict(err) {
			return &ctrl.Result{Requeue: true}, nil
		}
		return nil, fmt.Errorf("failed to update status: %w", err)
	}
	return nil, nil
}

func (r *AIScalerReconciler) resolveDecisionState(
	ctx context.Context,
	state *reconcileState,
	includeHuman bool,
	includeCost bool,
) error {
	if state.bundle == nil {
		return nil
	}

	state.sloDecision = buildSLODecision(state)

	if state.safetyDecision == nil && state.humanDecision == nil && state.reactiveDecision == nil && state.llmDecision == nil && state.deterministicDecision == nil && state.sloDecision == nil {
		state.decision = &llm.ScalingDecision{
			TargetReplicas: state.bundle.CurrentReplicas,
			Reasoning:      "no applicable scaling inputs; maintaining current replicas",
			Confidence:     1.0,
			ActionType:     "no_change",
		}
		state.provider = aiscalerv1.LLMProvider("fallback")
		state.precedence = nil
		return nil
	}

	resolver := r.PrecedenceResolver
	if resolver == nil {
		resolver = decision.NewPrecedenceResolver()
	}

	resolved := resolver.Resolve(buildPrecedenceInputs(state, includeHuman, includeCost))
	state.precedence = resolved
	state.decision, state.provider = materializeResolvedDecision(state, resolved)

	if includeCost && state.currentWorkloadCost != nil && r.CostEstimator != nil {
		estimate, err := r.CostEstimator.Estimate(state.currentWorkloadCost, state.bundle.CurrentReplicas, state.decision.TargetReplicas)
		if err != nil {
			return fmt.Errorf("recalculate final cost estimate: %w", err)
		}
		state.costEstimate = estimate
	}

	if prec := state.obj.Spec.Precedence; prec != nil && prec.LogConflicts && len(resolved.Conflicts) > 0 {
		logf.FromContext(ctx).Info("precedence conflicts resolved", "conflicts", strings.Join(resolved.Conflicts, "; "))
	}

	return nil
}

func buildPrecedenceInputs(
	state *reconcileState,
	includeHuman bool,
	includeCost bool,
) *decision.PrecedenceInputs {
	inputs := &decision.PrecedenceInputs{
		MinReplicas: state.obj.Spec.Constraints.MinReplicas,
		MaxReplicas: state.obj.Spec.Constraints.MaxReplicas,
	}

	if prec := state.obj.Spec.Precedence; prec != nil {
		inputs.ScheduleOverridesLLM = boolPtr(prec.ScheduleOverridesLLM)
		inputs.ReactiveRulesOverrideLLM = boolPtr(prec.ReactiveRulesOverrideLLM)
		inputs.SLOAlwaysWins = boolPtr(prec.SLOAlwaysWins)
	}
	if state.safetyDecision != nil {
		inputs.SafetyOverride = int32Ptr(state.safetyDecision.TargetReplicas)
		inputs.SafetyReason = state.safetyDecision.Reasoning
	}
	if includeHuman && state.humanDecision != nil {
		inputs.HumanOverride = int32Ptr(state.humanDecision.TargetReplicas)
		inputs.HumanReason = state.humanDecision.Reasoning
	}
	if state.sloDecision != nil {
		inputs.SLOTarget = int32Ptr(state.sloDecision.TargetReplicas)
		inputs.SLOReason = state.sloDecision.Reasoning
	}
	if state.reactiveDecision != nil {
		inputs.ReactiveTarget = int32Ptr(state.reactiveDecision.TargetReplicas)
		inputs.ReactiveReason = state.reactiveDecision.Reasoning
	}
	if state.llmDecision != nil {
		inputs.LLMTarget = int32Ptr(state.llmDecision.TargetReplicas)
		inputs.LLMReason = state.llmDecision.Reasoning
		inputs.LLMConfidence = state.llmDecision.Confidence
	}
	if state.deterministicDecision != nil {
		inputs.DeterministicTarget = int32Ptr(state.deterministicDecision.TargetReplicas)
		inputs.DeterministicReason = state.deterministicDecision.Reasoning
	}
	if includeCost && state.costCeiling != nil {
		inputs.CostConstrainedMax = state.costCeiling
		inputs.CostReason = state.costReason
	}

	return inputs
}

func materializeResolvedDecision(
	state *reconcileState,
	resolved *decision.ResolvedDecision,
) (*llm.ScalingDecision, aiscalerv1.LLMProvider) {
	if resolved == nil {
		return &llm.ScalingDecision{
			TargetReplicas: state.bundle.CurrentReplicas,
			Reasoning:      "no resolved decision; maintaining current replicas",
			Confidence:     1.0,
			ActionType:     "no_change",
		}, aiscalerv1.LLMProvider("fallback")
	}

	baseLayer := ""
	costApplied := false
	costReason := ""
	for _, layer := range resolved.Layers {
		if !layer.Applied {
			continue
		}
		if layer.Layer == "cost" {
			costApplied = true
			costReason = layer.Reason
			continue
		}
		if baseLayer == "" {
			baseLayer = layer.Layer
		}
	}

	baseDecision, provider := decisionForLayer(state, baseLayer)
	if baseDecision == nil {
		baseDecision = &llm.ScalingDecision{
			TargetReplicas: resolved.TargetReplicas,
			Reasoning:      "resolved scaling decision",
			Confidence:     1.0,
		}
	}

	finalDecision := cloneScalingDecision(baseDecision)
	finalDecision.TargetReplicas = resolved.TargetReplicas
	finalDecision.ActionType = actionTypeForTarget(resolved.TargetReplicas, state.bundle.CurrentReplicas)
	if finalDecision.Reasoning == "" {
		finalDecision.Reasoning = appliedLayerReason(resolved, baseLayer)
	}
	if costApplied {
		if finalDecision.Reasoning == "" {
			finalDecision.Reasoning = costReason
		} else if costReason != "" {
			finalDecision.Reasoning = finalDecision.Reasoning + "; " + costReason
		}
		provider = aiscalerv1.LLMProvider("cost-override")
	}
	if provider == "" {
		provider = aiscalerv1.LLMProvider("precedence")
	}

	return finalDecision, provider
}

func decisionForLayer(
	state *reconcileState,
	layer string,
) (*llm.ScalingDecision, aiscalerv1.LLMProvider) {
	switch layer {
	case "safety":
		if state.safetyDecision != nil {
			provider := state.safetySource
			if provider == "" {
				provider = aiscalerv1.LLMProvider("safety")
			}
			return state.safetyDecision, provider
		}
	case "human":
		return state.humanDecision, aiscalerv1.LLMProvider("approval-hold")
	case "slo":
		return state.sloDecision, aiscalerv1.LLMProvider("slo")
	case "reactive":
		return state.reactiveDecision, aiscalerv1.LLMProvider("reactive-rule")
	case "llm":
		return state.llmDecision, state.llmProvider
	case "deterministic":
		return state.deterministicDecision, aiscalerv1.LLMProvider("keda")
	}
	return nil, ""
}

func buildSLODecision(state *reconcileState) *llm.ScalingDecision {
	if state.bundle == nil || len(state.currentSLOs) == 0 || !decision.AnyBreached(state.currentSLOs) {
		return nil
	}

	target := state.bundle.CurrentReplicas
	for _, candidate := range []*llm.ScalingDecision{state.reactiveDecision, state.llmDecision, state.deterministicDecision} {
		if candidate != nil && candidate.TargetReplicas > target {
			target = candidate.TargetReplicas
		}
	}

	return &llm.ScalingDecision{
		TargetReplicas: target,
		Reasoning:      "SLO breach prevents scale-down while objectives are violated",
		Confidence:     1.0,
		ActionType:     actionTypeForTarget(target, state.bundle.CurrentReplicas),
	}
}

func buildPrecedenceStatus(resolved *decision.ResolvedDecision) *aiscalerv1.PrecedenceStatus {
	if resolved == nil {
		return nil
	}

	status := &aiscalerv1.PrecedenceStatus{
		ResolvedReplicas: resolved.TargetReplicas,
	}
	for _, conflict := range resolved.Conflicts {
		layer := conflict
		message := conflict
		if parts := strings.SplitN(conflict, ": ", 2); len(parts) == 2 {
			layer = parts[0]
			message = parts[1]
		}
		status.Conflicts = append(status.Conflicts, aiscalerv1.PrecedenceConflict{
			Layer:   layer,
			Message: message,
		})
	}
	for _, layer := range resolved.Layers {
		if layer.Applied && layer.Layer != "llm" {
			status.ActiveOverrides = append(status.ActiveOverrides, aiscalerv1.ActiveOverride{Layer: layer.Layer})
		}
	}

	return status
}

func computeCostCeiling(
	constraints *aiscalerv1.CostConstraints,
	estimate *cost.CostEstimate,
	minReplicas int32,
) (*int32, string) {
	if constraints == nil || estimate == nil || estimate.CostPerReplica <= 0 {
		return nil, ""
	}

	maxAffordable := int32(0)
	hasLimit := false
	if constraints.MaxHourlyCost > 0 {
		maxAffordable = int32(constraints.MaxHourlyCost / estimate.CostPerReplica)
		hasLimit = true
	}
	if constraints.MaxMonthlyCost > 0 {
		monthlyCap := int32((constraints.MaxMonthlyCost / (24 * 30)) / estimate.CostPerReplica)
		if !hasLimit || monthlyCap < maxAffordable {
			maxAffordable = monthlyCap
		}
		hasLimit = true
	}
	if !hasLimit {
		return nil, ""
	}
	if maxAffordable < minReplicas {
		maxAffordable = minReplicas
	}

	return &maxAffordable, fmt.Sprintf("cost budget caps replicas at %d", maxAffordable)
}

func computeBudgetUtilization(constraints *aiscalerv1.CostConstraints, estimate *cost.CostEstimate) float64 {
	if constraints == nil || estimate == nil {
		return 0
	}
	utilization := 0.0
	if constraints.MaxHourlyCost > 0 {
		utilization = estimate.ProposedHourlyCost / constraints.MaxHourlyCost * 100
	}
	if constraints.MaxMonthlyCost > 0 {
		monthlyUtilization := estimate.ProposedHourlyCost * 24 * 30 / constraints.MaxMonthlyCost * 100
		if monthlyUtilization > utilization {
			utilization = monthlyUtilization
		}
	}
	return utilization
}

func (r *AIScalerReconciler) coordinationDelay() time.Duration {
	if r.CoordinationRequeue > 0 {
		return r.CoordinationRequeue
	}
	return 15 * time.Second
}

func coordinationWorkloadName(obj *aiscalerv1.AIScaler) string {
	if obj == nil {
		return ""
	}
	if obj.Spec.TargetRef.Name != "" {
		return obj.Spec.TargetRef.Name
	}
	return obj.Name
}

func needsLiveCoordination(state *reconcileState) bool {
	if state == nil || state.obj == nil || state.bundle == nil || state.obj.Spec.DryRun {
		return false
	}
	if state.validation != nil && state.validation.ValidatedReplicas != state.bundle.CurrentReplicas {
		return true
	}
	return hasVerticalMutation(state)
}

func hasVerticalMutation(state *reconcileState) bool {
	if state == nil || state.obj == nil || state.verticalDecision == nil {
		return false
	}
	if state.obj.Spec.VerticalScaling == nil || !state.obj.Spec.VerticalScaling.Enabled {
		return false
	}
	if state.obj.Spec.VerticalScaling.ResizePolicy == "RecommendOnly" || state.obj.Spec.DryRun {
		return false
	}
	if state.currentResources == nil {
		return true
	}
	if state.verticalDecision.CPURequest.Cmp(state.currentResources.CPURequest) != 0 {
		return true
	}
	if state.verticalDecision.MemoryRequest.Cmp(state.currentResources.MemoryRequest) != 0 {
		return true
	}
	if !state.verticalDecision.CPULimit.IsZero() && state.verticalDecision.CPULimit.Cmp(state.currentResources.CPULimit) != 0 {
		return true
	}
	if !state.verticalDecision.MemoryLimit.IsZero() && state.verticalDecision.MemoryLimit.Cmp(state.currentResources.MemoryLimit) != 0 {
		return true
	}
	return false
}

func exceedsClusterBudget(policies []aiscalerv1.AIScaler, state *reconcileState, clusterMaxHourlyCost float64) bool {
	if clusterMaxHourlyCost <= 0 || state == nil || state.costEstimate == nil || state.costEstimate.DeltaHourlyCost <= 0 {
		return false
	}

	currentTotal := 0.0
	currentWorkload := coordinationWorkloadName(state.obj)
	for _, policy := range policies {
		if coordinationWorkloadName(&policy) == currentWorkload {
			continue
		}
		if policy.Status.Cost != nil {
			currentTotal += policy.Status.Cost.CurrentHourlyCost
		}
	}
	currentTotal += state.costEstimate.CurrentHourlyCost

	return currentTotal+state.costEstimate.DeltaHourlyCost > clusterMaxHourlyCost
}

func selectRollbackRecord(records []*audit.DecisionRecord, currentReplicas int32) *audit.DecisionRecord {
	for _, record := range records {
		if record == nil || !record.Applied || record.DryRun || record.NewReplicas == record.PreviousReplicas {
			continue
		}
		if record.NewReplicas != currentReplicas {
			continue
		}
		return record
	}
	return nil
}

func shouldSkipLLMForReactiveRules(config *aiscalerv1.PrecedenceConfig) bool {
	if config == nil {
		return true
	}
	return config.ReactiveRulesOverrideLLM
}

func actionTypeForTarget(target, current int32) string {
	switch {
	case target > current:
		return "scale_up"
	case target < current:
		return "scale_down"
	default:
		return "no_change"
	}
}

func appliedLayerReason(resolved *decision.ResolvedDecision, layerName string) string {
	if resolved == nil {
		return ""
	}
	for _, layer := range resolved.Layers {
		if layer.Layer == layerName {
			return layer.Reason
		}
	}
	return ""
}

func cloneScalingDecision(decision *llm.ScalingDecision) *llm.ScalingDecision {
	if decision == nil {
		return nil
	}
	clone := *decision
	if decision.ReasonCodes != nil {
		clone.ReasonCodes = append([]string(nil), decision.ReasonCodes...)
	}
	if decision.VerticalChanges != nil {
		verticalClone := *decision.VerticalChanges
		clone.VerticalChanges = &verticalClone
	}
	return &clone
}

func int32Ptr(value int32) *int32 {
	return &value
}

func boolPtr(value bool) *bool {
	return &value
}

func (r *AIScalerReconciler) readCurrentResources(
	ctx context.Context,
	obj *aiscalerv1.AIScaler,
) (*aiscalerv1.ResourceStatus, error) {
	deployment := &appsv1.Deployment{}
	key := client.ObjectKey{Namespace: obj.Spec.TargetRef.Namespace, Name: obj.Spec.TargetRef.Name}
	if err := r.Get(ctx, key, deployment); err != nil {
		return nil, fmt.Errorf("get deployment resources: %w", err)
	}
	if len(deployment.Spec.Template.Spec.Containers) == 0 {
		return nil, fmt.Errorf("deployment %s/%s has no containers", obj.Spec.TargetRef.Namespace, obj.Spec.TargetRef.Name)
	}

	container := deployment.Spec.Template.Spec.Containers[0]
	status := &aiscalerv1.ResourceStatus{}
	if cpu, ok := container.Resources.Requests[corev1.ResourceCPU]; ok {
		status.CPURequest = cpu
	}
	if memory, ok := container.Resources.Requests[corev1.ResourceMemory]; ok {
		status.MemoryRequest = memory
	}
	if cpuLimit, ok := container.Resources.Limits[corev1.ResourceCPU]; ok {
		status.CPULimit = cpuLimit
	}
	if memoryLimit, ok := container.Resources.Limits[corev1.ResourceMemory]; ok {
		status.MemoryLimit = memoryLimit
	}
	return status, nil
}

func buildVerticalDecision(
	config *aiscalerv1.VerticalScalingConfig,
	proposal *llm.VerticalProposal,
) (*aiscalerv1.VerticalDecision, error) {
	if config == nil || !config.Enabled || proposal == nil {
		return nil, nil
	}
	if proposal.CPURequest == "" && proposal.MemoryRequest == "" && proposal.CPULimit == "" && proposal.MemoryLimit == "" {
		return nil, nil
	}
	if proposal.CPURequest == "" || proposal.MemoryRequest == "" {
		return nil, fmt.Errorf("vertical recommendation requires both cpu_request and memory_request")
	}

	cpuRequest, err := resource.ParseQuantity(proposal.CPURequest)
	if err != nil {
		return nil, fmt.Errorf("parse cpu_request: %w", err)
	}
	memoryRequest, err := resource.ParseQuantity(proposal.MemoryRequest)
	if err != nil {
		return nil, fmt.Errorf("parse memory_request: %w", err)
	}

	decision := &aiscalerv1.VerticalDecision{
		CPURequest:     cpuRequest,
		MemoryRequest:  memoryRequest,
		ResizeStrategy: proposal.ResizeStrategy,
	}
	if decision.ResizeStrategy == "" {
		decision.ResizeStrategy = config.ResizePolicy
	}
	if proposal.CPULimit != "" {
		cpuLimit, err := resource.ParseQuantity(proposal.CPULimit)
		if err != nil {
			return nil, fmt.Errorf("parse cpu_limit: %w", err)
		}
		decision.CPULimit = cpuLimit
	}
	if proposal.MemoryLimit != "" {
		memoryLimit, err := resource.ParseQuantity(proposal.MemoryLimit)
		if err != nil {
			return nil, fmt.Errorf("parse memory_limit: %w", err)
		}
		decision.MemoryLimit = memoryLimit
	}
	return decision, nil
}

func buildVerticalContext(resources *aiscalerv1.ResourceStatus, config *aiscalerv1.VerticalScalingConfig) string {
	if resources == nil || config == nil || !config.Enabled {
		return ""
	}

	lines := []string{
		fmt.Sprintf("- Resize policy: %s", config.ResizePolicy),
		fmt.Sprintf("- Current CPU request: %s", quantityString(resources.CPURequest)),
		fmt.Sprintf("- Current memory request: %s", quantityString(resources.MemoryRequest)),
		fmt.Sprintf("- Current CPU limit: %s", quantityString(resources.CPULimit)),
		fmt.Sprintf("- Current memory limit: %s", quantityString(resources.MemoryLimit)),
	}
	constraints := config.Constraints
	if !constraints.MinCPURequest.IsZero() || !constraints.MaxCPURequest.IsZero() {
		lines = append(lines, fmt.Sprintf("- CPU request bounds: %s to %s",
			quantityString(constraints.MinCPURequest), quantityString(constraints.MaxCPURequest)))
	}
	if !constraints.MinMemoryRequest.IsZero() || !constraints.MaxMemoryRequest.IsZero() {
		lines = append(lines, fmt.Sprintf("- Memory request bounds: %s to %s",
			quantityString(constraints.MinMemoryRequest), quantityString(constraints.MaxMemoryRequest)))
	}

	return "Vertical scaling context:\n" + strings.Join(lines, "\n")
}

func summarizeFeedback(outcomes []feedback.DecisionOutcome) *aiscalerv1.FeedbackStatus {
	if len(outcomes) == 0 {
		return nil
	}

	effective := 0
	for _, outcome := range outcomes {
		if outcome.Effective {
			effective++
		}
	}

	return &aiscalerv1.FeedbackStatus{
		TotalDecisions: int32(len(outcomes)),
		EffectiveRate:  float64(effective) / float64(len(outcomes)),
	}
}

func buildFeedbackContext(outcomes []feedback.DecisionOutcome, includeInPrompt int32) string {
	if len(outcomes) == 0 || includeInPrompt <= 0 {
		return ""
	}
	if includeInPrompt > int32(len(outcomes)) {
		includeInPrompt = int32(len(outcomes))
	}

	effective := 0
	for _, outcome := range outcomes {
		if outcome.Effective {
			effective++
		}
	}

	lines := []string{
		fmt.Sprintf("Feedback summary: %d evaluated decisions, %.0f%% effective.", len(outcomes), float64(effective)/float64(len(outcomes))*100),
	}
	for idx := 0; idx < int(includeInPrompt); idx++ {
		outcome := outcomes[idx]
		verdict := "effective"
		if !outcome.Effective {
			verdict = "ineffective"
		}
		lines = append(lines, fmt.Sprintf("- Decision %s was %s (CPU %.1f%%→%.1f%%, p95 %.1fms→%.1fms)",
			shortDecisionID(outcome.RecordID),
			verdict,
			outcome.CPUBefore,
			outcome.CPUAfter,
			outcome.LatencyBefore,
			outcome.LatencyAfter,
		))
	}

	return strings.Join(lines, "\n")
}

func shortDecisionID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func quantityString(q resource.Quantity) string {
	if q.IsZero() {
		return "0"
	}
	return q.String()
}

func buildPredictiveContext(cpuPred, cpuConf, queuePred, queueConf float64) string {
	lines := make([]string, 0, 2)
	if cpuPred > 0 || cpuConf > 0 {
		lines = append(lines, fmt.Sprintf("- CPU in 1h: %.1f%% (confidence %.2f)", cpuPred, cpuConf))
	}
	if queuePred > 0 || queueConf > 0 {
		lines = append(lines, fmt.Sprintf("- Queue depth in 1h: %.0f (confidence %.2f)", queuePred, queueConf))
	}
	if len(lines) == 0 {
		return ""
	}
	return "Predictions:\n" + strings.Join(lines, "\n")
}

func buildNodeContext(nodeCtx *node.NodeContext) string {
	if nodeCtx == nil {
		return ""
	}

	cpuRequestedPct := 0.0
	if nodeCtx.ClusterCPUCapacity > 0 {
		cpuRequestedPct = nodeCtx.ClusterCPURequested / nodeCtx.ClusterCPUCapacity * 100
	}
	memRequestedPct := 0.0
	if nodeCtx.ClusterMemCapacity > 0 {
		memRequestedPct = nodeCtx.ClusterMemRequested / nodeCtx.ClusterMemCapacity * 100
	}

	return fmt.Sprintf(
		"Cluster state:\n- Ready nodes: %d/%d\n- Pending pods: %d\n- Node pools: %d\n- CPU requested: %.2f/%.2f cores (%.1f%%)\n- Memory requested: %.0f/%.0f bytes (%.1f%%)",
		nodeCtx.ReadyNodes,
		nodeCtx.TotalNodes,
		nodeCtx.PendingPods,
		len(nodeCtx.NodePools),
		nodeCtx.ClusterCPURequested,
		nodeCtx.ClusterCPUCapacity,
		cpuRequestedPct,
		nodeCtx.ClusterMemRequested,
		nodeCtx.ClusterMemCapacity,
		memRequestedPct,
	)
}
