package basemodel

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

// Retire only this controller's old demand status. This implementation neither
// watches endpoints nor creates demand, and ordinary models are a strict no-op.
func retireEndpointDemand(ctx context.Context, c client.Client, consumerReader client.Reader, obj client.Object) error {
	if !obj.GetDeletionTimestamp().IsZero() {
		return nil
	}
	_, status, err := shared.ModelSpecAndStatus(obj)
	if err != nil || status.EndpointDownloadDemand == nil {
		return err
	}
	ready, err := demandConsumersRetired(ctx, consumerReader)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("endpoint demand retirement waiting for non-demand agent rollout")
	}
	before := obj.DeepCopyObject().(client.Object)
	status.EndpointDownloadDemand = nil
	if err := c.Status().Patch(ctx, obj, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		_, prior, _ := shared.ModelSpecAndStatus(before)
		status.EndpointDownloadDemand = prior.EndpointDownloadDemand
		return err
	}
	return nil
}
