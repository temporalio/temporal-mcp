package fixture

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	fulfillmentTaskQueue = "fulfillment"
	shippingTaskQueue    = "shipping"
)

func fulfillmentV1(ctx workflow.Context, shippingBranch bool) error {
	if shippingBranch {
		workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: shippingTaskQueue, StartToCloseTimeout: time.Hour}), "PrepareShippingLabel")
	}
	return workflow.Await(ctx, func() bool { return false })
}

func fulfillmentV2(ctx workflow.Context, _ bool) error {
	return workflow.Await(ctx, func() bool { return false })
}

func newFulfillmentWorker(c client.Client, version int) worker.Worker {
	w := worker.New(c, fulfillmentTaskQueue, worker.Options{
		Identity:                "fulfillment-worker",
		DeploymentOptions:       worker.DeploymentOptions{Version: worker.WorkerDeploymentVersion{BuildID: fmt.Sprintf("fulfillment-v%d", version)}},
		LocalActivityWorkerOnly: true,
	})
	run := fulfillmentV1
	if version == 2 {
		run = fulfillmentV2
	}
	w.RegisterWorkflowWithOptions(run, workflow.RegisterOptions{Name: "FulfillmentWorkflow"})
	return w
}

// SeedFulfillment starts 230 FulfillmentWorkflow runs, fulfillment-0001 to fulfillment-0230, on a worker with build
// ID fulfillment-v1. 43 of the first 190 take the shipping branch, which starts a PrepareShippingLabel activity on a
// task queue that no worker polls, and then every run waits. It then deploys version 2, which only waits, on a worker
// with build ID fulfillment-v2, and signals the first 190 with address_updated. It returns once the signal's
// workflow task has failed as nondeterministic for those 43 and completed on version 2 for the other 147, with the
// time version 2 was deployed, a whole second that follows every version 1 event and precedes every signal, and a
// function that stops its worker.
func SeedFulfillment(ctx context.Context, c client.Client) (deployed time.Time, stop func(), err error) {
	const runs, signaled, nondeterministic = 230, 190, 43
	shippingBranch := map[int]bool{}
	for _, i := range rand.New(rand.NewPCG(1, 1)).Perm(signaled)[:nondeterministic] {
		shippingBranch[i] = true
	}

	v1 := newFulfillmentWorker(c, 1)
	if err := v1.Start(); err != nil {
		return time.Time{}, nil, err
	}
	defer v1.Stop()
	for i := range runs {
		if _, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: fulfillmentID(i), TaskQueue: fulfillmentTaskQueue}, "FulfillmentWorkflow", shippingBranch[i]); err != nil {
			return time.Time{}, nil, err
		}
	}
	for i := range runs {
		if _, err := WaitForDescribe(ctx, c, fulfillmentID(i), func(desc *workflowservice.DescribeWorkflowExecutionResponse) bool {
			return desc.GetPendingWorkflowTask() == nil && desc.GetWorkflowExecutionInfo().GetHistoryLength() >= 4
		}); err != nil {
			return time.Time{}, nil, waitingFor(fulfillmentID(i)+"'s first workflow task to complete on version 1", err)
		}
	}
	v1.Stop()

	deployed = waitForNextSecond()
	v2 := newFulfillmentWorker(c, 2)
	if err := v2.Start(); err != nil {
		return time.Time{}, nil, err
	}
	defer func() {
		if err != nil {
			v2.Stop()
		}
	}()
	for i := range signaled {
		if err := c.SignalWorkflow(ctx, fulfillmentID(i), "", "address_updated", nil); err != nil {
			return time.Time{}, nil, err
		}
	}
	for i := range signaled {
		if _, err := WaitForDescribe(ctx, c, fulfillmentID(i), func(desc *workflowservice.DescribeWorkflowExecutionResponse) bool {
			if shippingBranch[i] {
				return desc.GetPendingWorkflowTask().GetAttempt() >= 2
			}
			return desc.GetPendingWorkflowTask() == nil && desc.GetWorkflowExecutionInfo().GetHistoryLength() >= 8
		}); err != nil {
			return time.Time{}, nil, waitingFor(fulfillmentID(i)+"'s address_updated workflow task to run on version 2", err)
		}
	}
	return deployed, v2.Stop, waitingFor("the fulfillment runs to show in Visibility", waitForCount(ctx, c, fmt.Sprintf("TaskQueue = '%s'", fulfillmentTaskQueue), runs))
}

func fulfillmentID(i int) string {
	return fmt.Sprintf("fulfillment-%04d", i+1)
}
