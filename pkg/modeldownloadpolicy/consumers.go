// Package modeldownloadpolicy validates deployment-owned consumer inventory.
// Keep this contract in sync with the manager's model-download inventory patch.
package modeldownloadpolicy

import (
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	InventoryName       = "model-download-consumers"
	PolicyAnnotation    = "models.ome.io/download-policy"
	LifecycleAnnotation = "models.ome.io/policy-lifecycle"
	ManagedLabel        = "models.ome.io/policy-managed"
)

type Agent struct {
	Name           string `json:"name"`
	ServiceAccount string `json:"serviceAccount"`
}
type Inventory struct {
	Policy     string  `json:"policy"`
	DaemonSets []Agent `json:"daemonSets"`
	// Includes disabled consumers so an unmarked old CPU Pod cannot escape
	// retirement detection when its DaemonSet has already been removed.
	ServiceAccounts []string `json:"serviceAccounts"`
	// Includes disabled DaemonSets, including Pods with an old service account.
	KnownDaemonSets []string `json:"knownDaemonSets"`
}

func Decode(record *corev1.ConfigMap) (Inventory, error) {
	var inventory Inventory
	if record == nil || record.DeletionTimestamp != nil || record.Data["contract"] != "v1" {
		return inventory, fmt.Errorf("missing or unsupported model download consumer inventory")
	}
	if err := json.Unmarshal([]byte(record.Data["inventory"]), &inventory); err != nil {
		return inventory, fmt.Errorf("invalid model download consumer inventory: %w", err)
	}
	if len(inventory.DaemonSets) == 0 || len(inventory.ServiceAccounts) == 0 {
		return inventory, fmt.Errorf("model download consumer inventory is empty")
	}
	if inventory.Policy != "eager" && inventory.Policy != "endpoint" && inventory.Policy != "byor_pool" {
		return inventory, fmt.Errorf("unsupported consumer policy %q", inventory.Policy)
	}
	accounts, names := map[string]bool{}, map[string]bool{}
	for _, account := range inventory.ServiceAccounts {
		if len(validation.IsDNS1123Subdomain(account)) != 0 {
			return inventory, fmt.Errorf("invalid consumer service account %q", account)
		}
		accounts[account] = true
	}
	for _, agent := range inventory.DaemonSets {
		if len(validation.IsDNS1123Subdomain(agent.Name)) != 0 || names[agent.Name] || !accounts[agent.ServiceAccount] {
			return inventory, fmt.Errorf("invalid or duplicate model download consumer %q", agent.Name)
		}
		names[agent.Name] = true
	}
	return inventory, nil
}

// Validate is deliberately independent of Kubernetes caches and policy writers.
// Callers provide complete, namespace-scoped observations. No new-only label
// selector is used to discover consumers.
func Validate(inventory Inventory, daemonSets []appsv1.DaemonSet, pods []corev1.Pod, policy string) error {
	if inventory.Policy != policy {
		return fmt.Errorf("waiting for consumer inventory policy %s to become %s", inventory.Policy, policy)
	}
	expected, accounts := map[string]string{}, map[string]bool{}
	for _, agent := range inventory.DaemonSets {
		expected[agent.Name] = agent.ServiceAccount
	}
	for _, account := range inventory.ServiceAccounts {
		accounts[account] = true
	}
	owners := map[types.UID]bool{}
	for _, ds := range daemonSets {
		account, required := expected[ds.Name]
		consumer := required || accounts[ds.Spec.Template.Spec.ServiceAccountName] || ds.Labels[ManagedLabel] == "true" || ds.Spec.Template.Annotations[PolicyAnnotation] != ""
		if !consumer {
			continue
		}
		if !required || ds.UID == "" || ds.DeletionTimestamp != nil ||
			ds.Spec.Template.Spec.ServiceAccountName != account ||
			ds.Spec.Template.Annotations[PolicyAnnotation] != policy ||
			ds.Spec.Template.Annotations[LifecycleAnnotation] != "v2" ||
			ds.Status.ObservedGeneration < ds.Generation {
			return fmt.Errorf("waiting for agent DaemonSet %s rollout or retirement", ds.Name)
		}
		// Compatibility is independent of rollout availability: an unavailable or
		// misscheduled compatible agent cannot reinterpret model policy. Inspect
		// every nonterminal consumer Pod below instead of relying on fleet counts
		// to prove that old consumers have retired.
		owners[ds.UID] = true
		delete(expected, ds.Name)
	}
	if len(expected) != 0 {
		return fmt.Errorf("waiting for configured agent DaemonSets: %v", expected)
	}
	for _, pod := range pods {
		owner := metav1.GetControllerOf(&pod)
		managed := owner != nil && owner.Kind == "DaemonSet" && owners[owner.UID]
		known := false
		for _, name := range inventory.KnownDaemonSets {
			known = known || (owner != nil && owner.Kind == "DaemonSet" && owner.Name == name) || pod.Labels["app.kubernetes.io/component"] == name
		}
		if !managed && !known && !accounts[pod.Spec.ServiceAccountName] && pod.Annotations[PolicyAnnotation] == "" {
			continue
		}
		// A terminal Pod cannot consume models; all running/pending/terminating old
		// consumers must retire. NoSchedule or deletionTimestamp alone is not proof.
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if !managed || pod.Annotations[PolicyAnnotation] != policy ||
			pod.Annotations[LifecycleAnnotation] != "v2" {
			return fmt.Errorf("waiting for old agent Pod %s to retire", pod.Name)
		}
	}
	return nil
}
