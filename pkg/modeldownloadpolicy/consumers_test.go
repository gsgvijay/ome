package modeldownloadpolicy

import (
	"encoding/json"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConsumerCompatibilityDoesNotRequireFleetHealth(t *testing.T) {
	inventory := Inventory{Policy: "endpoint", DaemonSets: []Agent{{Name: "gpu", ServiceAccount: "agent"}}, ServiceAccounts: []string{"agent"}, KnownDaemonSets: []string{"gpu"}}
	agent := appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu", UID: "gpu-uid", Generation: 2},
		Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{PolicyAnnotation: "endpoint", LifecycleAnnotation: "v2"}},
			Spec:       corev1.PodSpec{ServiceAccountName: "agent"},
		}},
		Status: appsv1.DaemonSetStatus{ObservedGeneration: 2, DesiredNumberScheduled: 2, UpdatedNumberScheduled: 1, NumberAvailable: 0, NumberMisscheduled: 1},
	}
	controller := true
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "compatible", Annotations: agent.Spec.Template.Annotations,
			OwnerReferences: []metav1.OwnerReference{{Kind: "DaemonSet", Name: agent.Name, UID: agent.UID, Controller: &controller}}},
		Spec: corev1.PodSpec{ServiceAccountName: "agent"}, Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, terminating := range []bool{false, true} {
		current := pod.DeepCopy()
		if terminating {
			when := metav1.NewTime(time.Now())
			current.DeletionTimestamp = &when
		}
		if err := Validate(inventory, []appsv1.DaemonSet{agent}, []corev1.Pod{*current}, "endpoint"); err != nil {
			t.Fatalf("compatible consumer must not block on health/termination: %v", err)
		}
		current.Annotations[PolicyAnnotation] = "eager"
		if err := Validate(inventory, []appsv1.DaemonSet{agent}, []corev1.Pod{*current}, "endpoint"); err == nil {
			t.Fatal("an incompatible consumer must still block, even while terminating")
		}
	}
}

func TestConsumerInventoryCoversUnmarkedCPUAndSurge(t *testing.T) {
	inventory := Inventory{Policy: "eager", DaemonSets: []Agent{{Name: "gpu", ServiceAccount: "agent"}}, ServiceAccounts: []string{"agent", "cpu-agent"}, KnownDaemonSets: []string{"gpu", "cpu"}}
	gpu := appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "gpu", UID: "gpu-uid", Generation: 2}, Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{PolicyAnnotation: "eager", LifecycleAnnotation: "v2"}}, Spec: corev1.PodSpec{ServiceAccountName: "agent"}}}, Status: appsv1.DaemonSetStatus{ObservedGeneration: 2}}
	control := true
	cases := []struct {
		name   string
		agents []appsv1.DaemonSet
		pods   []corev1.Pod
		fail   bool
	}{
		{"zero nodes", []appsv1.DaemonSet{gpu}, nil, false},
		{"missing GPU", nil, nil, true},
		{"ordinary pod", []appsv1.DaemonSet{gpu}, []corev1.Pod{{Spec: corev1.PodSpec{ServiceAccountName: "unrelated"}}}, false},
		{"unmarked CPU pod", []appsv1.DaemonSet{gpu}, []corev1.Pod{{Spec: corev1.PodSpec{ServiceAccountName: "cpu-agent"}}}, true},
		{"retired CPU account", []appsv1.DaemonSet{gpu}, []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{Kind: "DaemonSet", Name: "cpu", UID: "old", Controller: &control}}}, Spec: corev1.PodSpec{ServiceAccountName: "retired-account"}}}, true},
		{"old surge pod", []appsv1.DaemonSet{gpu}, []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{PolicyAnnotation: "endpoint"}, OwnerReferences: []metav1.OwnerReference{{Kind: "DaemonSet", Name: "gpu", UID: "gpu-uid", Controller: &control}}}}}, true},
		{"terminal CPU", []appsv1.DaemonSet{gpu}, []corev1.Pod{{Spec: corev1.PodSpec{ServiceAccountName: "cpu-agent"}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}}, false},
	}
	cpu := gpu.DeepCopy()
	cpu.Name = "cpu"
	cpu.UID = "cpu"
	cpu.Spec.Template.Spec.ServiceAccountName = "cpu-agent"
	cpu.Spec.Template.Annotations = nil
	cases = append(cases, struct {
		name   string
		agents []appsv1.DaemonSet
		pods   []corev1.Pod
		fail   bool
	}{"unmarked CPU controller", []appsv1.DaemonSet{gpu, *cpu}, nil, true})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(inventory, tc.agents, tc.pods, "eager")
			if (err != nil) != tc.fail {
				t.Fatalf("Validate = %v; fail = %v", err, tc.fail)
			}
		})
	}
	body, _ := json.Marshal(inventory)
	cm := &corev1.ConfigMap{Data: map[string]string{"contract": "v1", "inventory": string(body)}}
	if _, err := Decode(cm); err != nil {
		t.Fatal(err)
	}
	cm.Data["contract"] = "v2"
	if _, err := Decode(cm); err == nil {
		t.Fatal("unknown inventory version accepted")
	}
	inventory.Policy = "endpoint"
	if err := Validate(inventory, []appsv1.DaemonSet{gpu}, nil, "eager"); err == nil {
		t.Fatal("stale inventory policy accepted")
	}
}
