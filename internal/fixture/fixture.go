// Package fixture provides the seeded Temporal workflows that tests run
// against a dev server.
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

// TaskQueue is the task queue the order workflow runs on.
const TaskQueue = "orders-demo"

var orderID = temporal.NewSearchAttributeKeyKeyword("OrderId")

// OrderInput is the input of OrderWorkflow.
type OrderInput struct {
	OrderID              string `json:"order_id"`
	CatalogShouldFail    bool   `json:"catalog_should_fail"`
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
	if order.CatalogShouldFail {
		return "", temporal.NewApplicationError("503 Service Unavailable from catalog service", "CatalogServiceUnavailable")
	}
	return "product-enriched:" + order.OrderID, nil
}

// NewWorker returns an unstarted worker for TaskQueue with OrderWorkflow and
// its activities registered.
func NewWorker(c client.Client) worker.Worker {
	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflow(OrderWorkflow)
	w.RegisterActivityWithOptions(authorizePayment, activity.RegisterOptions{Name: "authorize_payment"})
	w.RegisterActivityWithOptions(enrichProduct, activity.RegisterOptions{Name: "enrich_product"})
	return w
}

// StartFailedOrder starts order 88231 as workflow order-88231 with OrderId
// 88231. Its catalog activity fails on all three attempts, so the workflow
// fails.
func StartFailedOrder(ctx context.Context, c client.Client) (client.WorkflowRun, error) {
	return c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    "order-88231",
		TaskQueue:             TaskQueue,
		TypedSearchAttributes: temporal.NewSearchAttributes(orderID.ValueSet("88231")),
	}, OrderWorkflow, OrderInput{OrderID: "88231", CatalogShouldFail: true, RetryIntervalSeconds: 1})
}
