package fixture

import (
	"context"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// fulfillmentTaskQueue is the task queue of FulfillmentWorkflow.
const fulfillmentTaskQueue = "fulfillment"

func fulfillmentV1(ctx workflow.Context, shippingBranch bool) error {
	if shippingBranch {
		workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Hour}), "PrepareShippingLabel")
	}
	return workflow.Await(ctx, func() bool { return false })
}

func fulfillmentV2(ctx workflow.Context, _ bool) error {
	return workflow.Await(ctx, func() bool { return false })
}

// NewFulfillmentWorker returns an unstarted worker that runs version 1 or 2 of FulfillmentWorkflow on
// fulfillmentTaskQueue, and no activities. Version 1 starts a PrepareShippingLabel activity on the shipping branch
// and then waits forever. Version 2 only waits, so once it runs, the next workflow task of a run that took the
// shipping branch, such as one for a deployment_probe signal, fails as nondeterministic.
func NewFulfillmentWorker(c client.Client, version int) worker.Worker {
	w := worker.New(c, fulfillmentTaskQueue, worker.Options{LocalActivityWorkerOnly: true})
	run := fulfillmentV1
	if version == 2 {
		run = fulfillmentV2
	}
	w.RegisterWorkflowWithOptions(run, workflow.RegisterOptions{Name: "FulfillmentWorkflow"})
	return w
}

// StartFulfillment starts FulfillmentWorkflow as workflow id.
func StartFulfillment(ctx context.Context, c client.Client, id string, shippingBranch bool) (client.WorkflowRun, error) {
	return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: fulfillmentTaskQueue}, "FulfillmentWorkflow", shippingBranch)
}
