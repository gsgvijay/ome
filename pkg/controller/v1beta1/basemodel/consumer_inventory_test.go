package basemodel

import (
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/modeldownloadpolicy"
)

func testConsumerInventory(policy string) *corev1.ConfigMap {
	body, _ := json.Marshal(modeldownloadpolicy.Inventory{Policy: policy, DaemonSets: []modeldownloadpolicy.Agent{{Name: "agent", ServiceAccount: "model-agent"}}, ServiceAccounts: []string{"model-agent", "cpu-agent"}, KnownDaemonSets: []string{"agent", "cpu"}})
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: modeldownloadpolicy.InventoryName, Namespace: "ome", UID: "inventory"}, Data: map[string]string{"contract": "v1", "inventory": string(body)}}
}
