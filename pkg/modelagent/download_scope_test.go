package modelagent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	listers "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
)

type scopeFixture struct {
	*Scout
	tasks chan *GopherTask
}

func scopedScout(t *testing.T) (*scopeFixture, *v1beta1.BaseModel, cache.Indexer) {
	t.Helper()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{
		"test/class": "true", "test/pool": "pool-a", "test/owner": "a", "gpu": "yes",
	}, Annotations: map[string]string{"test/verified": "true"}}}
	m := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "models", UID: "uid", Labels: map[string]string{"owner": "a"}}}
	uri := "hf://example/weights"
	m.Spec.Storage = &v1beta1.StorageSpec{StorageUri: &uri}
	projectScope(t, m, true, map[string]string{"test/owner": "a"})
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, index.Add(m))
	tasks := make(chan *GopherTask, 20)
	w := &Scout{ctx: context.Background(), nodeInfo: node, scopeNode: node, scopeReplayNode: node, nodeName: node.Name,
		logger: zap.NewNop().Sugar(), kubeClient: fake.NewSimpleClientset(node), gopherChan: tasks,
		baseModelLister:        listers.NewBaseModelLister(index),
		clusterBaseModelLister: listers.NewClusterBaseModelLister(cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})),
		DownloadScope:          DownloadScope{NodeLabel: "test/class", PoolLabel: "test/pool", VerifiedAnnotation: "test/verified", QuarantineTaint: "test/quarantine"}}
	return &scopeFixture{Scout: w, tasks: tasks}, m, index
}

func projectScope(t *testing.T, m metav1.Object, allowed bool, match map[string]string) {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{"version": "v1", "uid": string(m.GetUID()),
		"sourceLabels": map[string]string{"owner": m.GetLabels()["owner"]}, "allowed": allowed,
		"selector": &metav1.LabelSelector{MatchLabels: match}})
	require.NoError(t, err)
	m.SetAnnotations(map[string]string{downloadScopeAnnotation: string(body)})
}

func TestScopePreservesOrdinaryEligibility(t *testing.T) {
	w, m, _ := scopedScout(t)
	w.nodeInfo.Labels = map[string]string{"gpu": "yes"}
	w.nodeInfo.Annotations = nil
	m.Annotations = nil
	m.Spec.Storage = nil // Projection must not force an ordinary model through download override.
	require.True(t, w.modelEligible(m, nil), "ordinary model needs no projection")
	require.True(t, w.CurrentTaskAllowed(&GopherTask{TaskType: Download, BaseModel: m}))
	updated := m.DeepCopy()
	projectScope(t, updated, false, nil)
	w.updateBaseModel(m, updated)
	require.Empty(t, w.gopherChan, "metadata-only projection must not restart ordinary downloads")
	storage := &v1beta1.StorageSpec{NodeSelector: map[string]string{"gpu": "no"}}
	require.False(t, w.modelEligible(m, storage), "existing storage filter remains authoritative")
}

func TestScopePrewarmsWithoutEndpointsAndIsolatesOwners(t *testing.T) {
	w, m, _ := scopedScout(t)
	require.True(t, w.modelEligible(m, nil))
	w.nodeInfo.Labels["test/pool"] = "second-pool"
	require.True(t, w.modelEligible(m, nil), "same customer needs no per-pool projection")
	w.nodeInfo.Labels["test/owner"] = "b"
	require.False(t, w.modelEligible(m, nil))
	projectScope(t, m, true, nil)
	require.True(t, w.modelEligible(m, nil), "shared catalog model eligible on every verified pool")
	require.False(t, w.modelEligible(m, &v1beta1.StorageSpec{NodeSelector: map[string]string{"gpu": "other"}}))
}

func TestScopeFailsClosedBeforeVerificationWithoutDeletingCache(t *testing.T) {
	w, m, _ := scopedScout(t)
	for _, change := range []func(){
		func() { delete(w.nodeInfo.Annotations, "test/verified") },
		func() { delete(w.nodeInfo.Labels, "test/pool") },
		func() { w.nodeInfo.Spec.Taints = []corev1.Taint{{Key: "test/quarantine"}} },
		func() { m.Annotations = nil },
	} {
		change()
		require.False(t, w.modelEligible(m, nil))
		require.False(t, w.mayRemoveIneligibleModel(m, nil))
	}
}

func TestScopeOwnershipAndUIDChangesFenceQueuedWork(t *testing.T) {
	w, m, index := scopedScout(t)
	task := &GopherTask{TaskType: Download, BaseModel: m}
	require.True(t, w.CurrentTaskAllowed(task))
	updated := m.DeepCopy()
	updated.Labels["owner"] = "b"
	require.NoError(t, index.Update(updated))
	require.False(t, w.CurrentTaskAllowed(task), "stale projection cannot authorize new owner")
	w.updateBaseModel(m, updated)
	require.Empty(t, w.gopherChan, "unknown scope is not deletion evidence")
	projected := updated.DeepCopy()
	projectScope(t, projected, true, map[string]string{"test/owner": "b"})
	w.updateBaseModel(updated, projected)
	require.Equal(t, Delete, (<-w.tasks).TaskType, "confirmed exclusion removes old cache after uncertainty")
	recreated := m.DeepCopy()
	recreated.UID = "new-uid"
	require.NoError(t, index.Update(recreated))
	require.False(t, w.CurrentTaskAllowed(task))
	task.TaskType = Delete
	require.False(t, w.CurrentTaskAllowed(task), "old delete must not remove replacement")
}

func TestScopeVerificationRefreshReplaysWithoutModelEvent(t *testing.T) {
	w, _, _ := scopedScout(t)
	w.nodeInfo = w.nodeInfo.DeepCopy()
	w.nodeInfo.Annotations["test/verified"] = "false"
	w.scopeReplayNode = w.nodeInfo
	w.refreshDownloadScopeNode()
	require.Len(t, w.gopherChan, 1)
	require.Equal(t, Download, (<-w.tasks).TaskType)
	w.refreshDownloadScopeNode()
	require.Empty(t, w.gopherChan, "unchanged node must not replay downloads")
}

func TestDownloadScopeConfiguration(t *testing.T) {
	require.NoError(t, (DownloadScope{}).Validate())
	require.Error(t, (DownloadScope{NodeLabel: "test/class"}).Validate())
	w, _, _ := scopedScout(t)
	require.NoError(t, w.DownloadScope.Validate())
}

func TestClusterModelProjectionAndStaleDelete(t *testing.T) {
	w, m, _ := scopedScout(t)
	before := &v1beta1.ClusterBaseModel{ObjectMeta: m.ObjectMeta, Spec: m.Spec}
	before.Annotations = nil
	after := before.DeepCopy()
	projectScope(t, after, true, nil)
	w.updateClusterBaseModel(before, after)
	require.Len(t, w.tasks, 1)
	require.Equal(t, Download, (<-w.tasks).TaskType)
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	after.Namespace = ""
	require.NoError(t, index.Add(after))
	w.clusterBaseModelLister = listers.NewClusterBaseModelLister(index)
	require.False(t, w.CurrentTaskAllowed(&GopherTask{TaskType: Delete, ClusterBaseModel: after}),
		"queued exclusion cannot delete a model eligible again")
}
