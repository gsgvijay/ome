package modelagent

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const downloadScopeAnnotation = "models.ome.io/eager-download-scope"

// Called under eventMu. Preserve the existing add-event refresh behavior and
// recheck eligibility against the refreshed node before dispatching work.
func (w *Scout) refreshNodeForAdd() bool {
	node, err := w.kubeClient.CoreV1().Nodes().Get(w.ctx, w.nodeName, metav1.GetOptions{})
	if err != nil {
		w.logger.Warnf("Cannot refresh node before model download: %v", err)
		return false
	}
	w.nodeInfo = node
	w.nodeMu.Lock()
	w.scopeNode = node
	w.nodeMu.Unlock()
	return true
}

// DownloadScope is a generic controller-projected storage selection contract.
// It does not watch endpoints or know how a controller chooses model demand.
type DownloadScope struct {
	NodeLabel          string
	PoolLabel          string
	VerifiedAnnotation string
	QuarantineTaint    string
}

func (s DownloadScope) Validate() error {
	if s.NodeLabel == "" && s.PoolLabel == "" && s.VerifiedAnnotation == "" && s.QuarantineTaint == "" {
		return nil
	}
	if s.NodeLabel == s.PoolLabel {
		return fmt.Errorf("download scope labels must differ")
	}
	for _, key := range []string{s.NodeLabel, s.PoolLabel, s.VerifiedAnnotation, s.QuarantineTaint} {
		if problems := validation.IsQualifiedName(key); len(problems) != 0 {
			return fmt.Errorf("invalid download scope label %q: %v", key, problems)
		}
	}
	return nil
}

func (s DownloadScope) scopedNode(node *corev1.Node) bool {
	if s.NodeLabel == "" || node == nil {
		return false
	}
	_, classified := node.Labels[s.NodeLabel]
	_, pooled := node.Labels[s.PoolLabel]
	_, verified := node.Annotations[s.VerifiedAnnotation]
	if classified || pooled || verified {
		return true
	}
	for _, taint := range node.Spec.Taints {
		if taint.Key == s.QuarantineTaint {
			return true
		}
	}
	return false
}

func (w *Scout) modelEligible(meta metav1.Object, storage *v1beta1.StorageSpec) bool {
	if !w.shouldDownloadModel(storage) {
		return false
	}
	allowed, _ := w.scopeAllowsModel(meta, storage)
	return allowed
}

// The second result distinguishes explicit exclusion from an incomplete
// projection. Unknown selection blocks NEW downloads, never destroys a cache.
func (w *Scout) scopeAllowsModel(meta metav1.Object, storage *v1beta1.StorageSpec) (bool, bool) {
	if w.DownloadScope.NodeLabel == "" {
		return true, true
	}
	if !w.DownloadScope.scopedNode(w.nodeInfo) {
		return true, true
	} // ordinary nodes keep eager semantics
	if w.nodeInfo.Labels[w.DownloadScope.NodeLabel] != "true" || w.nodeInfo.Labels[w.DownloadScope.PoolLabel] == "" ||
		w.nodeInfo.Annotations[w.DownloadScope.VerifiedAnnotation] != "true" {
		return false, false
	}
	for _, taint := range w.nodeInfo.Spec.Taints {
		if taint.Key == w.DownloadScope.QuarantineTaint {
			return false, false
		}
	}
	var proof struct {
		Version      string                `json:"version"`
		UID          string                `json:"uid"`
		SourceLabels map[string]string     `json:"sourceLabels"`
		Allowed      bool                  `json:"allowed"`
		Selector     *metav1.LabelSelector `json:"selector"`
	}
	if json.Unmarshal([]byte(meta.GetAnnotations()[downloadScopeAnnotation]), &proof) != nil ||
		proof.Version != "v1" || proof.UID == "" || proof.UID != string(meta.GetUID()) ||
		len(proof.SourceLabels) == 0 {
		return false, false // new/unprojected or concurrently edited model: wait for its controller
	}
	for key, value := range proof.SourceLabels {
		if meta.GetLabels()[key] != value {
			return false, false
		}
	}
	if !proof.Allowed {
		return false, true
	}
	if proof.Selector == nil {
		return false, false
	}
	selector, err := metav1.LabelSelectorAsSelector(proof.Selector)
	if err != nil {
		return false, false
	}
	return selector.Matches(labels.Set(w.nodeInfo.Labels)), true
}

func (w *Scout) mayRemoveIneligibleModel(meta metav1.Object, storage *v1beta1.StorageSpec) bool {
	_, known := w.scopeAllowsModel(meta, storage)
	return known && !w.modelEligible(meta, storage)
}

func downloadAnnotations(annotations map[string]string) map[string]string {
	result := maps.Clone(annotations)
	delete(result, downloadScopeAnnotation)
	if len(result) == 0 {
		return nil
	}
	return result
}

// Publishing an inert scope must not exercise download overrides on ordinary
// nodes (including models whose storage has not yet been populated).
func scopeOnlyUpdate(old, next metav1.Object, oldSpec, newSpec v1beta1.BaseModelSpec) bool {
	return old.GetUID() == next.GetUID() &&
		old.GetAnnotations()[downloadScopeAnnotation] != next.GetAnnotations()[downloadScopeAnnotation] &&
		reflect.DeepEqual(oldSpec, newSpec) && reflect.DeepEqual(old.GetLabels(), next.GetLabels()) &&
		reflect.DeepEqual(downloadAnnotations(old.GetAnnotations()), downloadAnnotations(next.GetAnnotations()))
}

// CurrentTaskAllowed revalidates queued work against current model identity and
// eligibility. In particular, a queued eligibility-loss Delete must not remove a
// same-UID model which has become eligible again, or a same-name replacement.
func (w *Scout) CurrentTaskAllowed(task *GopherTask) bool {
	w.nodeMu.RLock()
	node := w.scopeNode
	w.nodeMu.RUnlock()
	if node == nil {
		return false
	}
	// Preserve the baseline task path on ordinary nodes, including UID handling.
	if !w.DownloadScope.scopedNode(node) {
		return true
	}
	evaluator := &Scout{nodeInfo: node, DownloadScope: w.DownloadScope, logger: w.logger}
	var current metav1.Object
	var storage *v1beta1.StorageSpec
	var queued metav1.Object
	var err error
	if task.BaseModel != nil {
		queued = task.BaseModel
		model, e := w.baseModelLister.BaseModels(task.BaseModel.Namespace).Get(task.BaseModel.Name)
		err = e
		if e == nil {
			current, storage = model, model.Spec.Storage
		}
	} else if task.ClusterBaseModel != nil {
		queued = task.ClusterBaseModel
		model, e := w.clusterBaseModelLister.Get(task.ClusterBaseModel.Name)
		err = e
		if e == nil {
			current, storage = model, model.Spec.Storage
		}
	} else {
		return false
	}
	if apierrors.IsNotFound(err) {
		return task.TaskType == Delete
	}
	if err != nil || current.GetUID() != queued.GetUID() {
		return false
	}
	if task.TaskType == Delete {
		return current.GetDeletionTimestamp() != nil || evaluator.mayRemoveIneligibleModel(current, storage)
	}
	return current.GetDeletionTimestamp() == nil && evaluator.modelEligible(current, storage)
}

// Refresh only this agent's node; there is no cluster-wide node or endpoint
// informer per agent. Serialize refresh/replay with the two model informers.
func (w *Scout) refreshDownloadScopeNode() {
	node, err := w.kubeClient.CoreV1().Nodes().Get(w.ctx, w.nodeName, metav1.GetOptions{})
	if err != nil {
		w.logger.Warnf("Cannot refresh download scope node: %v", err)
		return
	}
	w.eventMu.Lock()
	defer w.eventMu.Unlock()
	old := w.nodeInfo
	if w.scopeReplayNode != nil {
		old = w.scopeReplayNode
	}
	w.nodeInfo = node
	w.nodeMu.Lock()
	w.scopeNode = node
	w.nodeMu.Unlock()
	// Eligibility depends on node identity and labels, not heartbeat/status or
	// resourceVersion. Model changes are handled by the existing model watches.
	// Only skip after a successful replay; a failed list must be retried even
	// when the next node GET has identical selection inputs.
	if w.scopeReplayNode != nil && old.UID == node.UID && old.Name == node.Name && reflect.DeepEqual(old.Labels, node.Labels) &&
		old.Annotations[w.DownloadScope.VerifiedAnnotation] == node.Annotations[w.DownloadScope.VerifiedAnnotation] && reflect.DeepEqual(old.Spec.Taints, node.Spec.Taints) {
		return
	}
	baseModels, err := w.baseModelLister.List(labels.Everything())
	if err != nil {
		w.logger.Warnf("Cannot replay model scope: %v", err)
		return
	}
	clusterModels, err := w.clusterBaseModelLister.List(labels.Everything())
	if err != nil {
		w.logger.Warnf("Cannot replay model scope: %v", err)
		return
	}
	w.scopeReplayNode = node
	changed := func(meta metav1.Object, storage *v1beta1.StorageSpec) (bool, bool, bool) {
		w.nodeInfo = old
		before := w.modelEligible(meta, storage)
		_, known := w.scopeAllowsModel(meta, storage)
		w.nodeInfo = node
		return before, w.modelEligible(meta, storage), known
	}
	for _, model := range baseModels {
		if model.DeletionTimestamp != nil {
			continue
		}
		before, after, known := changed(model, model.Spec.Storage)
		if !before && after {
			w.enqueueBaseModelDownload(model)
		}
		if (before || !known) && !after && w.mayRemoveIneligibleModel(model, model.Spec.Storage) {
			w.sendTask(&GopherTask{TaskType: Delete, BaseModel: model, NodeIneligible: true})
		}
	}
	for _, model := range clusterModels {
		if model.DeletionTimestamp != nil {
			continue
		}
		before, after, known := changed(model, model.Spec.Storage)
		if !before && after {
			w.enqueueClusterBaseModelDownload(model)
		}
		if (before || !known) && !after && w.mayRemoveIneligibleModel(model, model.Spec.Storage) {
			w.sendTask(&GopherTask{TaskType: Delete, ClusterBaseModel: model, NodeIneligible: true})
		}
	}
}

func (w *Scout) sendTask(task *GopherTask) {
	if w.ctx == nil {
		w.gopherChan <- task
		return
	} // unit-test fixtures
	select {
	case w.gopherChan <- task:
	case <-w.ctx.Done():
	}
}
