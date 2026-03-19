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
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiscalerv1 "github.com/sanjbh/kube-scaling-agent/api/v1"
	"github.com/sanjbh/kube-scaling-agent/internal/actuator"
	"github.com/sanjbh/kube-scaling-agent/internal/decision"
	"github.com/sanjbh/kube-scaling-agent/internal/llm"
	"github.com/sanjbh/kube-scaling-agent/internal/signals"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const finalizerName = "aiscaler.io/finalizer"

// AIScalerReconciler reconciles a AIScaler object
type AIScalerReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Collector *signals.Collector
	Router    *llm.Router
	Validator *decision.Validator
	Actuator  *actuator.Actuator
}

// reconcileState carries data between steps within a single reconcile cycle.
type reconcileState struct {
	obj        *aiscalerv1.AIScaler
	bundle     *signals.Bundle
	decision   *llm.ScalingDecision
	provider   aiscalerv1.LLMProvider
	validation *decision.ValidationResult
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

	obj := &aiscalerv1.AIScaler{}
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("Reconciling", "name", obj.Name, "phase", obj.Status.Phase)

	state := &reconcileState{
		obj: obj,
	}

	steps := []StepFunc{
		{Name: "ensureFinalizer", Run: r.ensureFinalizer},
		{Name: "checkCooldown", Run: r.checkCooldown},
		{Name: "collectSignals", Run: r.collectSignals},
		{Name: "fetchDecision", Run: r.fetchDecision},
		{Name: "validateDecision", Run: r.validateDecision},
		{Name: "actuate", Run: r.actuate},
		{Name: "updateStatus", Run: r.updateStatus},
	}

	for _, step := range steps {
		log.Info("step started", "step", step.Name)
		result, err := step.Run(ctx, state)
		if err != nil {
			log.Error(err, "step failed", "step", step.Name)
			return ctrl.Result{}, err
		}
		if result != nil {
			log.Info("step short-circuited", "step", step.Name)
			return *result, nil
		}
		log.Info("step completed", "step", step.Name)
	}

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
		return nil, nil
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

	// obj := state.obj

	log := logf.FromContext(ctx)

	bundle, err := r.Collector.Collect(ctx, state.obj)
	if err != nil {
		log.Error(err, "failed to collect signals")
		r.setCondition(
			ctx,
			state.obj,
			aiscalerv1.ConditionSignalsReady,
			metav1.ConditionFalse,
			"CollectionFailed",
			err.Error(),
		)
		return nil, err
	}

	// Check freeze annotation — if active, skip scaling entirely
	if bundle.Annotations.FreezeUntil != nil && time.Now().Before(*bundle.Annotations.FreezeUntil) {
		log.Info("scaling frozen", "until", *bundle.Annotations.FreezeUntil)
		remaining := time.Until(*bundle.Annotations.FreezeUntil)
		result := &ctrl.Result{
			RequeueAfter: remaining,
		}
		return result, nil
	}

	r.setCondition(
		ctx,
		state.obj,
		aiscalerv1.ConditionSignalsReady,
		metav1.ConditionTrue,
		"Collected",
		"all signals collected successfully",
	)

	state.bundle = bundle

	return nil, nil
}

func (r *AIScalerReconciler) fetchDecision(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {

	log := logf.FromContext(ctx)
	obj := state.obj
	b := state.bundle

	// Temporary debug — remove after testing
	log.Info("signal bundle",
		"cpu", b.CPUUtilization,
		"memory", b.MemoryUtilization,
		"currentReplicas", b.CurrentReplicas,
		"readyReplicas", b.ReadyReplicas,
		"deploymentReady", b.DeploymentReady,
	)

	scalingRequest := llm.ScalingRequest{
		PolicyName:          obj.Name,
		CurrentReplicas:     obj.Status.CurrentReplicas,
		Namespace:           obj.Namespace,
		MinReplicas:         obj.Spec.Constraints.MinReplicas,
		MaxReplicas:         obj.Spec.Constraints.MaxReplicas,
		MaxScaleStep:        obj.Spec.Constraints.MaxScaleStep,
		CPUUtilization:      b.CPUUtilization,
		MemoryUtilization:   b.MemoryUtilization,
		P95LatencyMs:        b.P95LatencyMs,
		ErrorRate:           b.ErrorRate,
		DeploymentReady:     b.DeploymentReady,
		ExpectedTraffic:     b.Annotations.ExpectedTraffic,
		ScaleConservatively: b.Annotations.ScaleConservatively,
		Note:                b.Annotations.Note,
		PeakHours:           b.Annotations.PeakHours,
	}

	scalingDecision, provider, err := r.Router.Decide(ctx, state.obj, scalingRequest)

	if err != nil {
		log.Error(err, "failed to decide")
		r.setCondition(
			ctx,
			state.obj,
			aiscalerv1.ConditionLLMReady,
			metav1.ConditionFalse,
			"DecisionFailed",
			err.Error(),
		)
		return nil, err
	}

	log.Info(
		"LLM decision",
		"provider", provider,
		"target", scalingDecision.TargetReplicas,
		"confidence", scalingDecision.Confidence,
	)

	state.decision = scalingDecision
	state.provider = provider

	r.setCondition(
		ctx,
		state.obj,
		aiscalerv1.ConditionLLMReady,
		metav1.ConditionTrue,
		"DecisionMade",
		scalingDecision.Reasoning,
	)
	return nil, nil
}

// validateDecision never short-circuits the chain — it always passes through.
// Validation is pure arithmetic so it cannot fail.
func (r *AIScalerReconciler) validateDecision(ctx context.Context, state *reconcileState) (*ctrl.Result, error) {
	validationResult := r.Validator.Validate(state.decision, state.bundle.CurrentReplicas, state.obj)
	if validationResult.Clamped {
		logf.FromContext(ctx).Info(
			"decision clamped by validator",
			"original", validationResult.OriginalReplicas,
			"validated", validationResult.ValidatedReplicas,
			"reason", validationResult.Reason,
		)
	}
	state.validation = validationResult
	return nil, nil
}

func (r *AIScalerReconciler) actuate(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {
	log := logf.FromContext(ctx)

	applyResult, err := r.Actuator.Apply(ctx, state.obj, state.validation)
	if err != nil {
		log.Error(err, "failed to apply scaling decision")
		return nil, err
	}

	if applyResult.DryRun {
		log.Info("dry run — decision computed but not applied",
			"replicas", applyResult.AppliedReplicas)
		return nil, nil
	}

	log.Info(
		"Scaling applied",
		"previous", applyResult.PreviousReplicas,
		"applied", applyResult.AppliedReplicas,
	)
	return nil, nil
}

func (r *AIScalerReconciler) updateStatus(
	ctx context.Context,
	state *reconcileState,
) (*ctrl.Result, error) {

	obj := state.obj
	now := metav1.Now()

	obj.Status.Phase = aiscalerv1.PhaseObserving
	obj.Status.CurrentReplicas = state.bundle.CurrentReplicas
	obj.Status.DesiredReplicas = state.validation.ValidatedReplicas
	obj.Status.LastProvider = state.provider
	obj.Status.LastDecisionReason = state.decision.Reasoning

	// Only update LastScaleTime if replicas actually changed
	if state.validation.ValidatedReplicas != state.bundle.CurrentReplicas {
		obj.Status.LastScaleTime = &now
		obj.Status.Phase = aiscalerv1.PhaseScaling
	}

	r.setCondition(
		ctx,
		obj,
		aiscalerv1.ConditionReady,
		metav1.ConditionTrue,
		"Reconciled",
		"reconcile cycle completed successfully",
	)

	if err := r.Status().Update(ctx, obj); err != nil {
		if apierrors.IsConflict(err) {
			return &ctrl.Result{Requeue: true}, nil
		}
		return nil, fmt.Errorf("failed to update status: %w", err)
	}
	return nil, nil
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
