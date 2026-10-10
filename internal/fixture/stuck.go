package fixture

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	// LedgerTaskQueue is the task queue of the stuck ledger workflows and their activities.
	LedgerTaskQueue = "ledger"
	// StuckWorkerIdentity is the identity of the worker that polls LedgerTaskQueue for workflow tasks.
	StuckWorkerIdentity = "stuck-worker"
	// StuckLedgerWorkflowID is the ID of a stuck ledger workflow that starts three activities.
	StuckLedgerWorkflowID = "session2-payment-reconciliation-main"
)

var stuckLedgerPopulations = []struct {
	workflowType, idPrefix string
	count                  int
}{
	{"PaymentReconciliationWorkflow", "payment-reconciliation", 800},
	{"PaymentSettlementWorkflow", "payment-settlement", 500},
	{"LedgerCompactionWorkflow", "ledger-compaction", 300},
	{"BalanceSnapshotWorkflow", "balance-snapshot", 230},
}

func reconcileLedgerWorkflow(ctx workflow.Context, batches int) error {
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

// SeedStuckLedger starts a worker, with identity StuckWorkerIdentity, that polls LedgerTaskQueue for workflow tasks
// only, and the 1,830 workflows of stuckLedgerPopulations on LedgerTaskQueue. StuckLedgerWorkflowID and the next 539
// each start three FetchLedgerBatch activities and the rest two, so 4,200 activities wait with no worker to run them.
// namespace must be c's namespace. It returns once they all wait, with a function that stops the worker.
func SeedStuckLedger(ctx context.Context, c client.Client, namespace string) (stop func(), err error) {
	w := worker.New(c, LedgerTaskQueue, worker.Options{Identity: StuckWorkerIdentity, LocalActivityWorkerOnly: true})
	for _, p := range stuckLedgerPopulations {
		w.RegisterWorkflowWithOptions(reconcileLedgerWorkflow, workflow.RegisterOptions{Name: p.workflowType})
	}
	if err := w.Start(); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			w.Stop()
		}
	}()
	var runs, activities int
	for _, p := range stuckLedgerPopulations {
		for i := range p.count {
			id, batches := fmt.Sprintf("session2-%s-%04d", p.idPrefix, i+1), 2
			if runs == 0 {
				id = StuckLedgerWorkflowID
			}
			if runs < 540 {
				batches = 3
			}
			if _, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: LedgerTaskQueue}, p.workflowType, batches); err != nil {
				return nil, err
			}
			runs, activities = runs+1, activities+batches
		}
	}
	if err := WaitForBacklog(ctx, c, namespace, LedgerTaskQueue, activities); err != nil {
		return nil, waitingFor(fmt.Sprintf("the %d ledger activities to queue", activities), err)
	}
	return w.Stop, waitingFor("the stuck ledger workflows to show as running in Visibility", waitForCount(ctx, c, fmt.Sprintf("TaskQueue = '%s' AND ExecutionStatus = 'Running'", LedgerTaskQueue), runs))
}
