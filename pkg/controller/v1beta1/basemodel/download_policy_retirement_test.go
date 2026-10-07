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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

func TestRetirementPreservesReadinessAndRequiresConsumerProof(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "ome")
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	for _, cluster := range []bool{false, true} {
		var model client.Object = &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"}}
		if cluster {
			model = &v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "m"}}
		}
		_, status, err := shared.ModelSpecAndStatus(model)
		require.NoError(t, err)
		status.NodesReady = []string{"node"}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(model).WithStatusSubresource(&v1beta1.BaseModel{}, &v1beta1.ClusterBaseModel{}).Build()
		require.NoError(t, retireEndpointDemand(context.Background(), c, c, model), "ordinary model has no rollout dependency")
		status.EndpointDownloadDemand = &v1beta1.EndpointDownloadDemandStatus{ReferenceCount: 1}
		require.Error(t, retireEndpointDemand(context.Background(), c, c, model))
		require.NotNil(t, status.EndpointDownloadDemand)
		agent := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "agent", Namespace: "ome", UID: "uid", Labels: map[string]string{"models.ome.io/policy-managed": "true"}},
			Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"models.ome.io/download-policy": "byor_pool", "models.ome.io/policy-lifecycle": "v2"}}}}}
		require.NoError(t, c.Create(context.Background(), agent))
		agent.Spec.Template.Spec.ServiceAccountName = "model-agent"
		require.NoError(t, c.Update(context.Background(), agent))
		require.NoError(t, c.Create(context.Background(), testConsumerInventory("byor_pool")))
		require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(model), model))
		_, status, _ = shared.ModelSpecAndStatus(model)
		status.EndpointDownloadDemand = &v1beta1.EndpointDownloadDemandStatus{ReferenceCount: 1}
		require.NoError(t, c.Status().Update(context.Background(), model))
		require.NoError(t, retireEndpointDemand(context.Background(), c, c, model))
		require.Nil(t, status.EndpointDownloadDemand)
		require.Equal(t, []string{"node"}, status.NodesReady)
	}
}
