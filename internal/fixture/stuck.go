package fixture

import (
	"context"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	// LedgerTaskQueue is the task queue of the stuck ledger workflows and their activities.
	LedgerTaskQueue = "ledger"
	// StuckWorkerIdentity is the identity of the worker that NewStuckWorker returns.
	StuckWorkerIdentity = "stuck-worker"
	// StuckLedgerWorkflowID is the ID of the stuck ledger workflow that starts three activities.
	StuckLedgerWorkflowID = "ledger-reconciliation-main"
	// StuckLedgerActivities is the number of activities that the stuck ledger workflows start together.
	StuckLedgerActivities = 7
)

// ReconcileLedgerWorkflow starts batches FetchLedgerBatch activities and waits for them.
func ReconcileLedgerWorkflow(ctx workflow.Context, batches int) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 12 * time.Hour})
	var fetches []workflow.Future
	for range batches {
		fetches = append(fetches, workflow.ExecuteActivity(ctx, "FetchLedgerBatch"))
	}
	for _, f := range fetches {
		if err := f.Get(ctx, nil); err != nil {
			return err
		}
	}
	return nil
}

// NewStuckWorker returns an unstarted worker that polls LedgerTaskQueue for workflow tasks only, so no
// FetchLedgerBatch activity ever starts.
func NewStuckWorker(c client.Client) worker.Worker {
	w := worker.New(c, LedgerTaskQueue, worker.Options{Identity: StuckWorkerIdentity, LocalActivityWorkerOnly: true})
	w.RegisterWorkflow(ReconcileLedgerWorkflow)
	return w
}

// StartStuckLedger starts ReconcileLedgerWorkflow as StuckLedgerWorkflowID with three batches and as two more
// workflows with two batches each.
func StartStuckLedger(ctx context.Context, c client.Client) error {
	for id, batches := range map[string]int{StuckLedgerWorkflowID: 3, "ledger-reconciliation-2": 2, "ledger-reconciliation-3": 2} {
		if _, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: LedgerTaskQueue}, ReconcileLedgerWorkflow, batches); err != nil {
			return err
		}
	}
	return nil
}
