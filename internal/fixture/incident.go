package fixture

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	incidentTaskQueue = "ledger-entries"
	// PaymentsTaskQueue is the task queue of the ProcessLedgerEntry activities for the payments domain.
	PaymentsTaskQueue = "ledger-payments"
	coreTaskQueue     = "ledger-core"
	// RecoveryWorkerIdentity is the identity of the workers that run the ProcessLedgerEntry
	// activities once the incident ends.
	RecoveryWorkerIdentity = "ledger-entries-worker"
)

var (
	team   = temporal.NewSearchAttributeKeyKeyword("Team")
	domain = temporal.NewSearchAttributeKeyKeyword("Domain")
)

var incidentPopulations = []struct {
	workflowType, idPrefix, team, domain string
	count                                int
}{
	{"SettlementWorkflow", "settlement", "treasury", "payments", 50},
	{"RefundWorkflow", "refund", "refunds", "payments", 25},
	{"PayoutWorkflow", "payout", "payouts", "payments", 15},
	{"LedgerIngestionWorkflow", "ingestion", "ledger-platform", "ledger", 30},
	{"BalanceSnapshotWorkflow", "balance", "accounting", "ledger", 20},
	{"AuditExportWorkflow", "audit", "compliance", "compliance", 20},
}

type ledgerEntry struct {
	Domain string `json:"domain"`
}

func ledgerEntryWorkflow(ctx workflow.Context, entry ledgerEntry) error {
	queue := coreTaskQueue
	if entry.Domain == "payments" {
		queue = PaymentsTaskQueue
	}
	return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           queue,
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 1, MaximumAttempts: 25},
	}), "ProcessLedgerEntry", entry).Get(ctx, nil)
}

// SeedIncident runs the 160 workflows of incidentPopulations, across 6 teams and 3 domains, with search attributes
// Team and Domain, which must be registered, through a ledger incident and its recovery. The 90 in domain payments
// run their ProcessLedgerEntry activity on PaymentsTaskQueue and the rest on another queue. During the incident,
// which starts 40 minutes before seeding, rounded down to the second, no worker polls either queue; it ends at the
// next whole second once every activity waits. Then workers with identity RecoveryWorkerIdentity poll both queues:
// each activity fails failedAttempts times with LedgerServiceUnavailable and then succeeds, except 10 that fail on
// their first attempt with the non-retryable LedgerWriteRejected. failedAttempts must be below 25, the activities'
// attempt limit. It returns once every run has closed, with the incident's start and end and a function that stops
// the workers.
func SeedIncident(ctx context.Context, c client.Client, failedAttempts int) (start, end time.Time, stop func(), err error) {
	start = time.Now().UTC().Add(-40 * time.Minute).Truncate(time.Second)
	var workers []worker.Worker
	stopWorkers := func() {
		for _, w := range workers {
			w.Stop()
		}
	}
	defer func() {
		if err != nil {
			stopWorkers()
		}
	}()
	startWorker := func(w worker.Worker) error {
		if err := w.Start(); err != nil {
			return err
		}
		workers = append(workers, w)
		return nil
	}

	w := worker.New(c, incidentTaskQueue, worker.Options{Identity: "ledger-entries-workflow-worker", LocalActivityWorkerOnly: true})
	for _, p := range incidentPopulations {
		w.RegisterWorkflowWithOptions(ledgerEntryWorkflow, workflow.RegisterOptions{Name: p.workflowType})
	}
	if err := startWorker(w); err != nil {
		return start, end, nil, err
	}
	var runs []client.WorkflowRun
	rejected := map[string]bool{}
	for _, p := range incidentPopulations {
		for i := range p.count {
			id := fmt.Sprintf("%s-%04d", p.idPrefix, i+1)
			rejected[id] = len(runs)%16 == 0
			r, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
				ID:                    id,
				TaskQueue:             incidentTaskQueue,
				TypedSearchAttributes: temporal.NewSearchAttributes(team.ValueSet(p.team), domain.ValueSet(p.domain)),
			}, p.workflowType, ledgerEntry{Domain: p.domain})
			if err != nil {
				return start, end, nil, err
			}
			runs = append(runs, r)
		}
	}
	for _, r := range runs {
		if _, err := WaitForDescribe(ctx, c, r.GetID(), func(desc *workflowservice.DescribeWorkflowExecutionResponse) bool {
			return len(desc.GetPendingActivities()) == 1
		}); err != nil {
			return start, end, nil, waitingFor(r.GetID()+"'s ProcessLedgerEntry activity to queue", err)
		}
	}
	end = waitForNextSecond()

	process := func(ctx context.Context, _ ledgerEntry) error {
		if rejected[activity.GetInfo(ctx).WorkflowExecution.ID] {
			return temporal.NewNonRetryableApplicationError("LedgerWriteRejected: the ledger rejected the entry", "LedgerWriteRejected", nil)
		}
		if activity.GetInfo(ctx).Attempt <= int32(failedAttempts) {
			return temporal.NewApplicationError("LedgerServiceUnavailable: recovery still in progress", "LedgerServiceUnavailable")
		}
		return nil
	}
	for _, queue := range []string{PaymentsTaskQueue, coreTaskQueue} {
		w := worker.New(c, queue, worker.Options{Identity: RecoveryWorkerIdentity, DisableWorkflowWorker: true})
		w.RegisterActivityWithOptions(process, activity.RegisterOptions{Name: "ProcessLedgerEntry"})
		if err := startWorker(w); err != nil {
			return start, end, nil, err
		}
	}
	var failure *temporal.WorkflowExecutionError
	for _, r := range runs {
		if err := r.Get(ctx, nil); err != nil && !errors.As(err, &failure) {
			return start, end, nil, waitingFor(r.GetID()+" to close after the incident", err)
		}
	}
	return start, end, stopWorkers, waitingFor("the incident runs to show as closed in Visibility", waitForCount(ctx, c, fmt.Sprintf("TaskQueue = '%s' AND ExecutionStatus != 'Running'", incidentTaskQueue), len(runs)))
}
