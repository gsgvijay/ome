package basemodel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestRetirementUsesUnfilteredConsumerReader(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "ome")
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns", UID: "model-uid"}}
	model.Status.EndpointDownloadDemand = &v1beta1.EndpointDownloadDemandStatus{ReferenceCount: 1}
	agent := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "ome", UID: "agent-uid", Labels: map[string]string{"models.ome.io/policy-managed": "true"}},
		Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"models.ome.io/download-policy": "eager", "models.ome.io/policy-lifecycle": "v2"}}}}}
	// The manager cache excludes agent Pods but can still contain the DaemonSet.
	agent.Spec.Template.Spec.ServiceAccountName = "model-agent"
	cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(model, agent).WithStatusSubresource(&v1beta1.BaseModel{}).Build()
	old := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "old-agent", Namespace: "ome", Annotations: map[string]string{"models.ome.io/download-policy": "endpoint"}}}
	live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(agent, old, testConsumerInventory("eager")).Build()
	for _, reader := range []client.Reader{nil, live} {
		require.Error(t, retireEndpointDemand(context.Background(), cached, reader, model))
		require.NotNil(t, model.Status.EndpointDownloadDemand, "do not clear based on the filtered serving-Pod cache")
	}
	require.NoError(t, live.Delete(context.Background(), old))
	var reader client.Reader = live
	require.NoError(t, retireEndpointDemand(context.Background(), cached, reader, model))
	require.Nil(t, model.Status.EndpointDownloadDemand)
}
