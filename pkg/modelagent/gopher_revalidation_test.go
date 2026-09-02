package modelagent

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	v1beta1lister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
)

func TestDeleteTaskRevalidationDistinguishesSameNameRecreation(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	replacement := clusterModelWithIdentity("model", "new-uid", "/models/shared")
	require.NoError(t, indexer.Add(replacement))
	gopher := &Gopher{clusterBaseModelLister: v1beta1lister.NewClusterBaseModelLister(indexer)}
	old := clusterModelWithIdentity("model", "old-uid", "/models/shared")

	valid, err := gopher.deleteTaskIsCurrent(&GopherTask{
		TaskType: Delete, DeleteReason: DeleteReasonResourceDeleted, ClusterBaseModel: old,
	})
	require.NoError(t, err)
	require.True(t, valid)

	valid, err = gopher.deleteTaskIsCurrent(&GopherTask{
		TaskType: Delete, DeleteReason: DeleteReasonNodeIneligible, ClusterBaseModel: old,
	})
	require.NoError(t, err)
	require.False(t, valid)

	valid, err = gopher.deleteTaskIsCurrent(&GopherTask{
		TaskType: Delete, DeleteReason: DeleteReasonResourceDeleted, ClusterBaseModel: replacement,
	})
	require.NoError(t, err)
	require.False(t, valid)
}

func TestPathReferenceExcludesOnlyDeletedUID(t *testing.T) {
	baseIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	clusterIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	replacement := clusterModelWithIdentity("model", "new-uid", "/models/shared")
	require.NoError(t, clusterIndexer.Add(replacement))
	gopher := &Gopher{
		baseModelLister:        v1beta1lister.NewBaseModelLister(baseIndexer),
		clusterBaseModelLister: v1beta1lister.NewClusterBaseModelLister(clusterIndexer),
		logger:                 zaptest.NewLogger(t).Sugar(),
	}
	old := clusterModelWithIdentity("model", "old-uid", "/models/shared")

	referenced, err := gopher.isPathReferencedByOtherModels("/models/shared", nil, old)
	require.NoError(t, err)
	require.True(t, referenced)
}

func TestNodeIneligibilityDeleteRevalidatesLatestModelAndNode(t *testing.T) {
	const nodeName = "node-a"
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	model := clusterModelWithIdentity("model", "model-uid", "/models/shared")
	model.Spec.Storage.NodeSelector = map[string]string{"gpu": "h100"}
	require.NoError(t, indexer.Add(model))
	gopher := &Gopher{
		clusterBaseModelLister: v1beta1lister.NewClusterBaseModelLister(indexer),
		kubeClient: fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: nodeName, Labels: map[string]string{"gpu": "a10"},
		}}),
		nodeLabelReconciler: &NodeLabelReconciler{nodeName: nodeName},
	}
	task := &GopherTask{
		TaskType: Delete, DeleteReason: DeleteReasonNodeIneligible, ClusterBaseModel: model.DeepCopy(),
	}

	valid, err := gopher.deleteTaskIsCurrent(task)
	require.NoError(t, err)
	require.True(t, valid)

	model.Spec.Storage.NodeSelector = map[string]string{"gpu": "a10"}
	require.NoError(t, indexer.Update(model))
	valid, err = gopher.deleteTaskIsCurrent(task)
	require.NoError(t, err)
	require.False(t, valid)
}

func clusterModelWithIdentity(name string, uid types.UID, path string) *v1beta1.ClusterBaseModel {
	return &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid},
		Spec: v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{
			StorageUri: stringPtr("oci://n/ns/b/bucket/o/model"), Path: &path,
		}},
	}
}
