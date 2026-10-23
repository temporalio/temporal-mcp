package server

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-mcp/internal/config"
)

func TestShutdownCancellationResult(t *testing.T) {
	temporalCancelled := temporalError(status.Error(codes.Canceled, "context canceled"), &config.Target{Address: "a:7233"}, &peer.Peer{}, temporalServer)
	tests := []struct {
		name          string
		shutdown      bool
		clientCancels bool
		handlerError  error
		code          string
		retryable     bool
	}{
		{"shutdown during raw cancellation", true, false, context.Canceled, "server_shutting_down", true},
		{"shutdown during Temporal cancellation", true, false, temporalCancelled, "server_shutting_down", true},
		{"client cancellation", false, true, temporalCancelled, "temporal_unavailable", false},
		{"client cancels before shutdown", true, true, temporalCancelled, "temporal_unavailable", false},
		{"unrelated failure during shutdown", true, false, errors.New("unrelated failure"), "internal", false},
		{"permission denied during shutdown", true, false, temporalError(status.Error(codes.PermissionDenied, "denied"), &config.Target{Address: "a:7233"}, &peer.Peer{}, temporalServer), "temporal_unavailable", false},
		{"success during shutdown", true, false, nil, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stop, shutdown := context.WithCancel(t.Context())
			defer shutdown()
			client, cancelClient := context.WithCancel(t.Context())
			defer cancelClient()
			success := &mcp.CallToolResult{}
			handler := cancelWith(stop, stop)(normalizeToolCalls(nil)(func(ctx context.Context, _ string, _ mcp.Request) (mcp.Result, error) {
				if tt.clientCancels {
					cancelClient()
				}
				if tt.shutdown {
					shutdown()
				}
				<-ctx.Done()
				if tt.handlerError != nil {
					return errorResult(tt.handlerError), nil
				}
				return success, nil
			}))
			res, err := handler(client, "tools/call", &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "test"}})
			if err != nil {
				t.Fatal(err)
			}
			if tt.handlerError == nil {
				if res != success {
					t.Errorf("result = %+v, want the successful result", res)
				}
				return
			}
			result, ok := res.(*mcp.CallToolResult)
			if !ok || !result.IsError {
				t.Fatalf("result = %+v, want a tool error", res)
			}
			var te *toolError
			if !errors.As(result.GetError(), &te) {
				t.Fatalf("error = %v, want a toolError", result.GetError())
			}
			if te.Code != tt.code || te.Retryable != tt.retryable {
				t.Errorf("error = %s, want code %s, retryable %v", te, tt.code, tt.retryable)
			}
		})
	}
}

func TestInvalidArgumentMessages(t *testing.T) {
	// A tool with a rule that argumentError does not know, whose message it keeps.
	patternTool := func(s *toolServer) {
		schema := &jsonschema.Schema{Type: "object", Properties: map[string]*jsonschema.Schema{"code": {Type: "string", Pattern: "^[a-z]+$"}}}
		addTool(s, &mcp.Tool{Name: "pattern", InputSchema: schema}, nil, func(context.Context, *mcp.CallToolRequest, map[string]any) (*mcp.CallToolResult, struct{}, error) {
			return nil, struct{}{}, nil
		})
	}
	session := connect(t, filepath.Join("..", "..", "testdata", "config.yaml"), t.Output(), patternTool)
	tooMany := make([]any, maxListItems+1)
	for i := range tooMany {
		tooMany[i] = map[string]any{"workflow_id": "wf"}
	}
	tests := []struct {
		rule, tool string
		args       map[string]any
		message    string
	}{
		{"required", "get_task_queues", map[string]any{}, "target is required; task_queues is required"},
		{"required in an item", "get_workflows", map[string]any{"target": "prod", "workflows": []any{map[string]any{"workflow_id": "wf"}, map[string]any{"run_id": "r"}}}, "workflows[1].workflow_id is required"},
		{"additionalProperties", "check_connection", map[string]any{"target": "prod", "TARGET": "staging"}, "TARGET is not a known argument"},
		{"additionalProperties beside a list", "get_workflows", map[string]any{"target": "prod", "workflows": []any{map[string]any{"workflow_id": "wf"}}, "extra": "x"}, "extra is not a known argument"},
		{"type", "get_workflow_history", map[string]any{"target": "prod", "workflow_id": "wf", "limit": "10"}, "limit must be an integer"},
		{"type in an item", "get_workflows", map[string]any{"target": "prod", "workflows": []any{map[string]any{"workflow_id": "wf", "run_id": 5}}}, "workflows[0].run_id must be a string"},
		{"enum", "get_task_queues", map[string]any{"target": "prod", "task_queues": []any{"tq"}, "types": []any{"workflow", "workflows"}}, "types[1] must be one of workflow, activity, nexus"},
		{"minLength", "get_task_queues", map[string]any{"target": "prod", "task_queues": []any{"tq", ""}}, "task_queues[1] must not be empty"},
		{"minItems", "get_task_queues", map[string]any{"target": "prod", "task_queues": []any{}}, "task_queues must not be empty"},
		{"maxItems", "get_workflows", map[string]any{"target": "prod", "workflows": tooMany}, "workflows must have at most 50 items"},
		{"minimum", "get_workflow_history", map[string]any{"target": "prod", "workflow_id": "wf", "event_range": map[string]any{"first": 0}}, "event_range.first must be at least 1"},
		{"maximum", "search_workflows", map[string]any{"target": "prod", "page_size": 101}, "page_size must be at most 100"},
		{"maximum beyond int64", "get_workflow_history", map[string]any{"target": "prod", "workflow_id": "wf", "event_range": map[string]any{"first": 1e300}}, "event_range.first must be at most 9007199254740991"},
		{"unknown rule", "pattern", map[string]any{"code": "A"}, `validating root: validating /properties/code: pattern: "A" does not match regular expression "^[a-z]+$"`},
	}
	for _, tt := range tests {
		t.Run(tt.rule, func(t *testing.T) {
			res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: tt.tool, Arguments: tt.args})
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]any{"code": "invalid_argument", "message": tt.message, "retryable": false}
			if got := errorContent(t, res); !reflect.DeepEqual(got, want) {
				t.Errorf("error = %#v, want %#v", got, want)
			}
		})
	}
}
