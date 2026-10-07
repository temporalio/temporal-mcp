package fixture

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	// IncidentTaskQueue is the task queue of LedgerEntryWorkflow.
	IncidentTaskQueue = "ledger-incident"
	// PaymentsTaskQueue is the task queue of the ProcessLedgerEntry activities for the payments domain.
	PaymentsTaskQueue = "ledger-payments"
	// CoreTaskQueue is the task queue of the ProcessLedgerEntry activities for every other domain.
	CoreTaskQueue = "ledger-core"
	// RecoveryWorkerIdentity is the identity of the worker that NewRecoveryWorker returns.
	RecoveryWorkerIdentity = "recovery-worker"
)

var domain = temporal.NewSearchAttributeKeyKeyword("Domain")

// LedgerEntry is the input of LedgerEntryWorkflow.
type LedgerEntry struct {
	Domain   string `json:"domain"`
	Terminal bool   `json:"terminal"`
}

// LedgerEntryWorkflow processes entry with a ProcessLedgerEntry activity on its domain's task queue, retried every
// second.
func LedgerEntryWorkflow(ctx workflow.Context, entry LedgerEntry) error {
	queue := CoreTaskQueue
	if entry.Domain == "payments" {
		queue = PaymentsTaskQueue
	}
	return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           queue,
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 1, MaximumAttempts: 25},
	}), "ProcessLedgerEntry", entry).Get(ctx, nil)
}

func processLedgerEntry(ctx context.Context, entry LedgerEntry) error {
	if entry.Terminal {
		return temporal.NewNonRetryableApplicationError("LedgerWriteRejected: the ledger rejected the entry", "LedgerWriteRejected", nil)
	}
	if activity.GetInfo(ctx).Attempt == 1 {
		return temporal.NewApplicationError("LedgerServiceUnavailable: recovery still in progress", "LedgerServiceUnavailable")
	}
	return nil
}

// NewIncidentWorker returns an unstarted worker that runs LedgerEntryWorkflow on IncidentTaskQueue, and no
// activities: while no recovery worker runs, the incident goes on and no ProcessLedgerEntry activity starts.
func NewIncidentWorker(c client.Client) worker.Worker {
	w := worker.New(c, IncidentTaskQueue, worker.Options{LocalActivityWorkerOnly: true})
	w.RegisterWorkflow(LedgerEntryWorkflow)
	return w
}

// NewRecoveryWorker returns an unstarted worker, with identity RecoveryWorkerIdentity, that runs the
// ProcessLedgerEntry activities on PaymentsTaskQueue, which ends the incident for the payments domain. The first
// attempt of each fails with LedgerServiceUnavailable and the next succeeds; a terminal entry's fails with the
// non-retryable LedgerWriteRejected.
func NewRecoveryWorker(c client.Client) worker.Worker {
	w := worker.New(c, PaymentsTaskQueue, worker.Options{Identity: RecoveryWorkerIdentity, DisableWorkflowWorker: true})
	w.RegisterActivityWithOptions(processLedgerEntry, activity.RegisterOptions{Name: "ProcessLedgerEntry"})
	return w
}

// StartLedgerEntry starts LedgerEntryWorkflow for entry as workflow id, with search attribute Domain set to its
// domain.
func StartLedgerEntry(ctx context.Context, c client.Client, id string, entry LedgerEntry) (client.WorkflowRun, error) {
	return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    id,
		TaskQueue:             IncidentTaskQueue,
		TypedSearchAttributes: temporal.NewSearchAttributes(domain.ValueSet(entry.Domain)),
	}, LedgerEntryWorkflow, entry)
}
