// The start_workflow tool, which starts a workflow run under workflow ID policies that
// never terminate a run.

package server

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"maps"
	"slices"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/payload"
	"github.com/temporalio/temporal-mcp/internal/temporal"
)

type startWorkflowInput struct {
	Target                   string `json:"target" jsonschema:"the name or an alias of a target"`
	WorkflowID               string `json:"workflow_id" jsonschema:"the workflow ID of the run to start"`
	WorkflowType             string `json:"workflow_type" jsonschema:"the workflow type, such as OrderWorkflow"`
	TaskQueue                string `json:"task_queue" jsonschema:"the task queue whose workers run the workflow"`
	Input                    []any  `json:"input,omitempty" jsonschema:"the workflow's arguments, in order, as JSON values"`
	WorkflowIDConflictPolicy string `json:"workflow_id_conflict_policy,omitempty" jsonschema:"fail or use_existing; fail if omitted"`
	WorkflowIDReusePolicy    string `json:"workflow_id_reuse_policy,omitempty" jsonschema:"allow_duplicate, allow_duplicate_failed_only or reject_duplicate; allow_duplicate if omitted"`
}

type startWorkflowResult struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	Started    bool   `json:"started"`
}

// The policies an agent may choose, none of which terminates a run. A policy left out is UNSPECIFIED, which the
// Temporal server replaces with its default.
var (
	conflictPolicies = map[string]enumspb.WorkflowIdConflictPolicy{
		"fail":         enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		"use_existing": enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}
	reusePolicies = map[string]enumspb.WorkflowIdReusePolicy{
		"allow_duplicate":             enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		"allow_duplicate_failed_only": enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		"reject_duplicate":            enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}
)

// enumOf returns m's keys, sorted, as a schema enum.
func enumOf[V any](m map[string]V) []any {
	var enum []any
	for _, k := range slices.Sorted(maps.Keys(m)) {
		enum = append(enum, k)
	}
	return enum
}

func startWorkflowInputSchema() *jsonschema.Schema {
	schema := schemaFor[startWorkflowInput]("start_workflow", "input", nil)
	for _, name := range []string{"workflow_id", "workflow_type", "task_queue"} {
		schema.Properties[name].MinLength = new(1)
	}
	schema.Properties["workflow_id_conflict_policy"].Enum = enumOf(conflictPolicies)
	schema.Properties["workflow_id_reuse_policy"].Enum = enumOf(reusePolicies)
	return schema
}

func startWorkflow(cfg *config.Config, clients *temporal.Clients, payloads *payload.Renderer) mcp.ToolHandlerFor[startWorkflowInput, envelope[startWorkflowResult]] {
	return func(ctx context.Context, req *mcp.CallToolRequest, in startWorkflowInput) (*mcp.CallToolResult, envelope[startWorkflowResult], error) {
		var none envelope[startWorkflowResult]
		t, err := lookupTarget(ctx, cfg, in.Target)
		if err != nil {
			return nil, none, err
		}
		// The MCP SDK decodes arguments through float64, which rounds integers above 2^53, so input comes from the raw arguments.
		var args struct {
			Input []json.RawMessage `json:"input"`
		}
		_ = json.Unmarshal(req.Params.Arguments, &args)
		input, err := payloads.Encode(ctx, t.Name, args.Input)
		if err != nil {
			return nil, none, &toolError{Code: "encode_failed", Message: "input was not encoded, so no run was started: " + err.Error(), cause: err}
		}
		// One request ID across retries, so that Temporal starts one run however many attempts reach it.
		start := &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                t.Namespace,
			WorkflowId:               in.WorkflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: in.WorkflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: in.TaskQueue},
			Input:                    &commonpb.Payloads{Payloads: input},
			Identity:                 clients.Identity(ctx),
			RequestId:                rand.Text(),
			WorkflowIdConflictPolicy: conflictPolicies[in.WorkflowIDConflictPolicy],
			WorkflowIdReusePolicy:    reusePolicies[in.WorkflowIDReusePolicy],
		}
		resp, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			return clients.Client(t.Name).WorkflowService().StartWorkflowExecution(ctx, start, opts...)
		})
		if err != nil {
			return nil, none, err
		}
		return nil, envelope[startWorkflowResult]{Target: &t.Name, Result: startWorkflowResult{WorkflowID: in.WorkflowID, RunID: resp.GetRunId(), Started: resp.GetStarted()}, Complete: true}, nil
	}
}
