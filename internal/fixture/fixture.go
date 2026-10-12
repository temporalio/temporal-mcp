// Package fixture provides the seeded Temporal workflows that the tests and the acceptance sessions run against a
// dev server.
package fixture

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is the task queue the order workflow runs on.
const TaskQueue = "orders"

var orderID = temporal.NewSearchAttributeKeyKeyword("OrderId")

// CatalogDown, while true, makes OrderWorkflow's catalog activity, enrich_product, fail with a retryable 503 Service
// Unavailable from the catalog service.
var CatalogDown atomic.Bool

// OrderInput is the input of OrderWorkflow.
type OrderInput struct {
	OrderID              string `json:"order_id"`
	RetryIntervalSeconds int    `json:"retry_interval_seconds"`
}

// OrderWorkflow authorizes payment for an order, then enriches its product
// from the catalog, making up to three attempts RetryIntervalSeconds apart.
func OrderWorkflow(ctx workflow.Context, order OrderInput) (map[string]string, error) {
	var payment string
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}), "authorize_payment", order).Get(ctx, &payment)
	if err != nil {
		return nil, err
	}
	var enrichment string
	err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Duration(order.RetryIntervalSeconds) * time.Second,
			BackoffCoefficient: 1,
			MaximumAttempts:    3,
		},
	}), "enrich_product", order).Get(ctx, &enrichment)
	if err != nil {
		return nil, err
	}
	return map[string]string{"payment": payment, "enrichment": enrichment}, nil
}

func authorizePayment(_ context.Context, order OrderInput) (string, error) {
	return "payment-authorized:" + order.OrderID, nil
}

func enrichProduct(_ context.Context, order OrderInput) (string, error) {
	if CatalogDown.Load() {
		return "", temporal.NewApplicationError("503 Service Unavailable from catalog service", "CatalogServiceUnavailable")
	}
	return "product-enriched:" + order.OrderID, nil
}

// NewWorker returns an unstarted worker for TaskQueue with OrderWorkflow and
// its activities registered.
func NewWorker(c client.Client) worker.Worker {
	w := worker.New(c, TaskQueue, worker.Options{Identity: "orders-worker"})
	w.RegisterWorkflow(OrderWorkflow)
	w.RegisterActivityWithOptions(authorizePayment, activity.RegisterOptions{Name: "authorize_payment"})
	w.RegisterActivityWithOptions(enrichProduct, activity.RegisterOptions{Name: "enrich_product"})
	return w
}

// SeedOrders sets CatalogDown and runs order 88231, as workflow order-88231, until it fails, its catalog activity
// failing all three attempts retryIntervalSeconds apart. It then clears CatalogDown and runs orders 88232 to 88243,
// as workflows order-88232 to order-88243 with the same retry interval, until they complete. Each run's OrderId
// search attribute is its order's ID, such as 88231, not its workflow ID. A worker from NewWorker must be running
// and search attribute OrderId registered. It returns order-88231's run.
func SeedOrders(ctx context.Context, c client.Client, retryIntervalSeconds int) (client.WorkflowRun, error) {
	CatalogDown.Store(true)
	failed, err := startOrder(ctx, c, OrderInput{OrderID: "88231", RetryIntervalSeconds: retryIntervalSeconds})
	if err != nil {
		return nil, err
	}
	var failure *temporal.WorkflowExecutionError
	if err := failed.Get(ctx, nil); !errors.As(err, &failure) {
		return nil, fmt.Errorf("order-88231 ended with %v, want a workflow failure", err)
	}
	CatalogDown.Store(false)
	var recovered []client.WorkflowRun
	for i := range 12 {
		run, err := startOrder(ctx, c, OrderInput{OrderID: strconv.Itoa(88232 + i), RetryIntervalSeconds: retryIntervalSeconds})
		if err != nil {
			return nil, err
		}
		recovered = append(recovered, run)
	}
	for _, run := range recovered {
		if err := run.Get(ctx, nil); err != nil {
			return nil, waitingFor(run.GetID()+" to complete", err)
		}
	}
	return failed, waitingFor("orders 88232 to 88243 to show as completed in Visibility", waitForCount(ctx, c, "WorkflowId STARTS_WITH 'order-882' AND ExecutionStatus = 'Completed'", len(recovered)))
}

func startOrder(ctx context.Context, c client.Client, order OrderInput) (client.WorkflowRun, error) {
	return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    "order-" + order.OrderID,
		TaskQueue:             TaskQueue,
		TypedSearchAttributes: temporal.NewSearchAttributes(orderID.ValueSet(order.OrderID)),
	}, OrderWorkflow, order)
}

func waitUntil(ctx context.Context, done func() (bool, error)) error {
	for {
		if ok, err := done(); ok || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// WaitForDescribe describes the latest run of workflowID every 100 milliseconds until done returns true for the
// description, and returns that description. It returns an error if a describe fails or ctx ends first.
func WaitForDescribe(ctx context.Context, c client.Client, workflowID string, done func(*workflowservice.DescribeWorkflowExecutionResponse) bool) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	var desc *workflowservice.DescribeWorkflowExecutionResponse
	err := waitUntil(ctx, func() (bool, error) {
		var err error
		desc, err = c.DescribeWorkflowExecution(ctx, workflowID, "")
		return err == nil && done(desc), err
	})
	return desc, err
}

// WaitForBacklog returns once the activity backlog of taskQueue in namespace, as DescribeTaskQueue reports it, is n
// tasks. It returns an error if a describe fails or ctx ends first.
func WaitForBacklog(ctx context.Context, c client.Client, namespace, taskQueue string, n int) error {
	return waitUntil(ctx, func() (bool, error) {
		resp, err := c.WorkflowService().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{Namespace: namespace, TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue}, TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY, ReportStats: true})
		return resp.GetStats().GetApproximateBacklogCount() == int64(n), err
	})
}

func waitForCount(ctx context.Context, c client.Client, query string, n int) error {
	return waitUntil(ctx, func() (bool, error) {
		resp, err := c.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: query})
		return resp.GetCount() == int64(n), err
	})
}

func waitingFor(what string, err error) error {
	if err != nil {
		return fmt.Errorf("waiting for %s: %w", what, err)
	}
	return nil
}

func waitForNextSecond() time.Time {
	next := time.Now().UTC().Truncate(time.Second).Add(time.Second)
	time.Sleep(time.Until(next))
	return next
}
