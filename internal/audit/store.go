package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Store is the interface for persisting decision records.
type Store interface {
	Store(ctx context.Context, record *DecisionRecord) error
	List(ctx context.Context, workload string, limit int) ([]*DecisionRecord, error)
	Get(ctx context.Context, id string) (*DecisionRecord, error)
}

// ConfigMapStore persists audit records in Kubernetes ConfigMaps.
// Each workload gets a ConfigMap holding the last N decisions.
type ConfigMapStore struct {
	client    client.Client
	namespace string
	maxPerCM  int
	mu        sync.Mutex
}

// NewConfigMapStore creates a ConfigMap-backed audit store.
func NewConfigMapStore(c client.Client, namespace string, maxPerCM int) *ConfigMapStore {
	if maxPerCM <= 0 {
		maxPerCM = 50
	}
	return &ConfigMapStore{
		client:    c,
		namespace: namespace,
		maxPerCM:  maxPerCM,
	}
}

func cmName(workload string) string {
	return fmt.Sprintf("aiscaler-audit-%s", workload)
}

// Store persists a decision record into the workload's ConfigMap.
func (s *ConfigMapStore) Store(ctx context.Context, record *DecisionRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	name := cmName(record.Workload)
	key := types.NamespacedName{Namespace: s.namespace, Name: name}

	cm := &corev1.ConfigMap{}
	err := s.client.Get(ctx, key, cm)
	if apierrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: s.namespace,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "aiscaler",
					"aiscaler.io/component":        "audit",
				},
			},
			Data: make(map[string]string),
		}
		if err := s.client.Create(ctx, cm); err != nil {
			return fmt.Errorf("create audit configmap: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("get audit configmap: %w", err)
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[record.ID] = string(data)

	// Prune old records if over limit
	if len(cm.Data) > s.maxPerCM {
		s.pruneOldest(cm)
	}

	return s.client.Update(ctx, cm)
}

// List retrieves recent decision records for a workload.
func (s *ConfigMapStore) List(ctx context.Context, workload string, limit int) ([]*DecisionRecord, error) {
	name := cmName(workload)
	key := types.NamespacedName{Namespace: s.namespace, Name: name}

	cm := &corev1.ConfigMap{}
	if err := s.client.Get(ctx, key, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	records := make([]*DecisionRecord, 0, len(cm.Data))
	for _, v := range cm.Data {
		var rec DecisionRecord
		if err := json.Unmarshal([]byte(v), &rec); err != nil {
			continue
		}
		records = append(records, &rec)
	}

	// Sort newest first
	sort.Slice(records, func(i, j int) bool {
		return records[i].Timestamp.After(records[j].Timestamp)
	})

	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}

	return records, nil
}

// Get retrieves a single decision record by ID.
func (s *ConfigMapStore) Get(ctx context.Context, id string) (*DecisionRecord, error) {
	// Search across all audit ConfigMaps
	cmList := &corev1.ConfigMapList{}
	if err := s.client.List(ctx, cmList, client.InNamespace(s.namespace),
		client.MatchingLabels{"aiscaler.io/component": "audit"}); err != nil {
		return nil, err
	}

	for _, cm := range cmList.Items {
		if data, ok := cm.Data[id]; ok {
			var rec DecisionRecord
			if err := json.Unmarshal([]byte(data), &rec); err != nil {
				return nil, err
			}
			return &rec, nil
		}
	}

	return nil, fmt.Errorf("record %s not found", id)
}

// pruneOldest removes the oldest entries to stay within maxPerCM.
func (s *ConfigMapStore) pruneOldest(cm *corev1.ConfigMap) {
	type entry struct {
		key string
		rec DecisionRecord
	}

	var entries []entry
	for k, v := range cm.Data {
		var rec DecisionRecord
		if err := json.Unmarshal([]byte(v), &rec); err != nil {
			delete(cm.Data, k) // remove corrupt entries
			continue
		}
		entries = append(entries, entry{key: k, rec: rec})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].rec.Timestamp.Before(entries[j].rec.Timestamp)
	})

	// Remove oldest entries exceeding limit
	toRemove := len(entries) - s.maxPerCM
	for i := 0; i < toRemove; i++ {
		delete(cm.Data, entries[i].key)
	}
}
