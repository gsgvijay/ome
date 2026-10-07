package basemodel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDemandRetirementAcceptsPoolScopeButNotOldConsumers(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "ome")
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	agent := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "ome", UID: "uid", Labels: map[string]string{"models.ome.io/policy-managed": "true"}},
		Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"models.ome.io/download-policy": "byor_pool", "models.ome.io/policy-lifecycle": "v2"}}}}}
	agent.Spec.Template.Spec.ServiceAccountName = "model-agent"
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent, testConsumerInventory("byor_pool")).Build()
	ready, err := demandConsumersRetired(context.Background(), c)
	require.NoError(t, err)
	require.True(t, ready)
	old := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "surge", Namespace: "ome", Annotations: map[string]string{"models.ome.io/download-policy": "endpoint"}}}
	require.NoError(t, c.Create(context.Background(), old))
	ready, err = demandConsumersRetired(context.Background(), c)
	require.NoError(t, err)
	require.False(t, ready, "old or orphaned consumers must retire first")
	t.Setenv("POD_NAMESPACE", "another-deployment")
	ready, err = demandConsumersRetired(context.Background(), c)
	require.Error(t, err)
	require.False(t, ready, "another deployment's inventory is not evidence")
}
