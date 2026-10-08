// The get_task_queue tool, which returns the pollers and backlog of a task queue for each
// task queue type asked for.

package server

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

const maxPollers = 100

type getTaskQueueInput struct {
	Target    string   `json:"target" jsonschema:"the name or an alias of a target"`
	TaskQueue string   `json:"task_queue" jsonschema:"the task queue's name"`
	Types     []string `json:"types,omitempty" jsonschema:"the task queue types to return a section for; every type if omitted"`
}

type taskQueue struct {
	TaskQueue string            `json:"task_queue"`
	Workflow  *taskQueueSection `json:"workflow"`
	Activity  *taskQueueSection `json:"activity"`
	Nexus     *taskQueueSection `json:"nexus"`
}

type taskQueueSection struct {
	Pollers      []poller  `json:"pollers"`
	PollerCount  int       `json:"poller_count"`
	BacklogCount *int64    `json:"backlog_count"`
	BacklogAge   *string   `json:"backlog_age"`
	AddRate      *float32  `json:"add_rate"`
	DispatchRate *float32  `json:"dispatch_rate"`
	ObservedAt   time.Time `json:"observed_at"`
}

type poller struct {
	Identity       string     `json:"identity"`
	LastAccessTime *time.Time `json:"last_access_time"`
}

func getTaskQueueInputSchema() *jsonschema.Schema {
	schema, err := jsonschema.For[getTaskQueueInput](nil)
	if err != nil {
		panic(fmt.Errorf("get_task_queue: input schema: %w", err))
	}
	schema.Properties["types"].Items.Enum = []any{"workflow", "activity", "nexus"}
	return schema
}

func getTaskQueue(cfg *config.Config, clients *temporal.Clients) mcp.ToolHandlerFor[getTaskQueueInput, envelope[taskQueue]] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in getTaskQueueInput) (*mcp.CallToolResult, envelope[taskQueue], error) {
		t, err := lookupTarget(ctx, cfg, in.Target)
		if err != nil {
			return nil, envelope[taskQueue]{}, err
		}
		if in.TaskQueue == "" {
			return nil, envelope[taskQueue]{}, &toolError{Code: "invalid_argument", Message: "task_queue is required"}
		}
		out := envelope[taskQueue]{Target: &t.Name, Result: taskQueue{TaskQueue: in.TaskQueue}, Complete: true}
		for _, s := range []struct {
			name    string
			typ     enumspb.TaskQueueType
			section **taskQueueSection
		}{
			{"workflow", enumspb.TASK_QUEUE_TYPE_WORKFLOW, &out.Result.Workflow},
			{"activity", enumspb.TASK_QUEUE_TYPE_ACTIVITY, &out.Result.Activity},
			{"nexus", enumspb.TASK_QUEUE_TYPE_NEXUS, &out.Result.Nexus},
		} {
			if len(in.Types) > 0 && !slices.Contains(in.Types, s.name) {
				continue
			}
			resp, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.DescribeTaskQueueResponse, error) {
				return clients.Client(t.Name).WorkflowService().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
					Namespace:     t.Namespace,
					TaskQueue:     &taskqueuepb.TaskQueue{Name: in.TaskQueue},
					TaskQueueType: s.typ,
					ReportStats:   true,
				}, opts...)
			})
			if err != nil {
				return nil, envelope[taskQueue]{}, err
			}
			section := &taskQueueSection{Pollers: []poller{}, PollerCount: len(resp.GetPollers()), ObservedAt: time.Now().UTC()}
			pollers := slices.SortedFunc(slices.Values(resp.GetPollers()), func(a, b *taskqueuepb.PollerInfo) int {
				return b.GetLastAccessTime().AsTime().Compare(a.GetLastAccessTime().AsTime())
			})
			for _, p := range pollers[:min(len(pollers), maxPollers)] {
				section.Pollers = append(section.Pollers, poller{Identity: p.GetIdentity(), LastAccessTime: timeOf(p.GetLastAccessTime())})
			}
			if stats := resp.GetStats(); stats != nil {
				section.BacklogCount = new(stats.GetApproximateBacklogCount())
				section.BacklogAge = new(durationString(stats.GetApproximateBacklogAge()))
				section.AddRate = new(stats.GetTasksAddRate())
				section.DispatchRate = new(stats.GetTasksDispatchRate())
			}
			*s.section = section
			out.Complete = out.Complete && len(section.Pollers) == section.PollerCount
		}
		return nil, out, nil
	}
}
