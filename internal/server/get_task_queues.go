// The get_task_queues tool, which returns the pollers and backlog of each task queue in a
// list, for each task queue type asked for.

package server

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
	"github.com/temporalio/temporal-operator-mcp/internal/temporal"
)

const maxPollers = 100

type getTaskQueuesInput struct {
	Target     string   `json:"target" jsonschema:"the name or an alias of a target"`
	TaskQueues []string `json:"task_queues" jsonschema:"the names of the task queues to get"`
	Types      []string `json:"types,omitempty" jsonschema:"the task queue types to return a section for in each task queue; every type if omitted"`
}

type taskQueue struct {
	TaskQueue string            `json:"task_queue"`
	Workflow  *taskQueueSection `json:"workflow"`
	Activity  *taskQueueSection `json:"activity"`
	Nexus     *taskQueueSection `json:"nexus"`
	Error     *toolError        `json:"error"`
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

func getTaskQueuesInputSchema() *jsonschema.Schema {
	schema := schemaFor[getTaskQueuesInput]("get_task_queues", "input")
	queues := schema.Properties["task_queues"]
	boundList(queues, queues.Items)
	schema.Properties["types"].Items.Enum = []any{"workflow", "activity", "nexus"}
	return schema
}

func getTaskQueuesOutputSchema() *jsonschema.Schema {
	def, err := jsonschema.For[taskQueueSection](nil)
	if err != nil {
		panic(fmt.Errorf("get_task_queues: section schema: %w", err))
	}
	def.Type, def.Types = "", []string{"null", "object"}
	schema, err := jsonschema.For[envelope[[]taskQueue]](&jsonschema.ForOptions{TypeSchemas: map[reflect.Type]*jsonschema.Schema{
		reflect.TypeFor[taskQueueSection](): {Types: []string{"null", "object"}, Ref: "#/$defs/section"},
	}})
	if err != nil {
		panic(fmt.Errorf("get_task_queues: output schema: %w", err))
	}
	schema.Defs = map[string]*jsonschema.Schema{"section": def}
	return schema
}

func getTaskQueues(cfg *config.Config, clients *temporal.Clients) mcp.ToolHandlerFor[getTaskQueuesInput, envelope[[]taskQueue]] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in getTaskQueuesInput) (*mcp.CallToolResult, envelope[[]taskQueue], error) {
		var none envelope[[]taskQueue]
		t, err := lookupTarget(ctx, cfg, in.Target)
		if err != nil {
			return nil, none, err
		}
		svc := clients.Client(t.Name).WorkflowService()
		out := envelope[[]taskQueue]{Target: &t.Name, Result: []taskQueue{}, Complete: true}
		for i, name := range in.TaskQueues {
			q, cut, err := readTaskQueue(ctx, t, svc, name, in.Types)
			if q.Error, err = itemError(err, "task_queues", i); err != nil {
				return nil, none, err
			}
			out.Result = append(out.Result, q)
			out.Complete = out.Complete && !cut
		}
		return nil, out, nil
	}
}

// readTaskQueue returns the named task queue with a section for each of types, every type if types is empty, and whether it cut any section's list of pollers.
func readTaskQueue(ctx context.Context, t *config.Target, svc workflowservice.WorkflowServiceClient, name string, types []string) (taskQueue, bool, error) {
	q, cut := taskQueue{TaskQueue: name}, false
	for _, s := range []struct {
		name    string
		typ     enumspb.TaskQueueType
		section **taskQueueSection
	}{
		{"workflow", enumspb.TASK_QUEUE_TYPE_WORKFLOW, &q.Workflow},
		{"activity", enumspb.TASK_QUEUE_TYPE_ACTIVITY, &q.Activity},
		{"nexus", enumspb.TASK_QUEUE_TYPE_NEXUS, &q.Nexus},
	} {
		if len(types) > 0 && !slices.Contains(types, s.name) {
			continue
		}
		resp, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.DescribeTaskQueueResponse, error) {
			return svc.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
				Namespace:     t.Namespace,
				TaskQueue:     &taskqueuepb.TaskQueue{Name: name},
				TaskQueueType: s.typ,
				ReportStats:   true,
			}, opts...)
		})
		if err != nil {
			return taskQueue{TaskQueue: name}, false, err
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
		cut = cut || len(section.Pollers) < section.PollerCount
	}
	return q, cut, nil
}
