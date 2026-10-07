package basemodel

import (
	"context"
	"fmt"
	"os"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/modeldownloadpolicy"
)

// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list

// This is retirement-only lifecycle handling, independent of demand production.
// The inventory includes disabled consumers and pre-marker CPU/surge Pods.
func demandConsumersRetired(ctx context.Context, c client.Reader) (bool, error) {
	if c == nil {
		return false, fmt.Errorf("uncached consumer reader is required for demand retirement")
	}
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		return false, fmt.Errorf("POD_NAMESPACE is required to retire model download demand")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	record := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: modeldownloadpolicy.InventoryName}, record); err != nil {
		return false, err
	}
	inventory, err := modeldownloadpolicy.Decode(record)
	if err != nil {
		return false, err
	}
	if inventory.Policy == "endpoint" {
		return false, nil
	}
	var agents []appsv1.DaemonSet
	options := &client.ListOptions{Namespace: namespace, Limit: 250}
	for {
		page := &appsv1.DaemonSetList{}
		if err := c.List(ctx, page, options); err != nil {
			return false, err
		}
		agents = append(agents, page.Items...)
		if page.Continue == "" {
			break
		}
		options.Continue = page.Continue
	}
	var pods []corev1.Pod
	options = &client.ListOptions{Namespace: namespace, Limit: 250}
	for {
		page := &corev1.PodList{}
		if err := c.List(ctx, page, options); err != nil {
			return false, err
		}
		pods = append(pods, page.Items...)
		if page.Continue == "" {
			break
		}
		options.Continue = page.Continue
	}
	if err := modeldownloadpolicy.Validate(inventory, agents, pods, inventory.Policy); err != nil {
		return false, nil
	}
	current := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(record), current); err != nil {
		return false, err
	}
	return current.UID == record.UID && current.ResourceVersion == record.ResourceVersion && current.DeletionTimestamp == nil, nil
}
