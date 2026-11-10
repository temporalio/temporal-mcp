package server

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/temporal"
)

func TestTemporalError(t *testing.T) {
	tests := []struct {
		code      codes.Code
		want      string
		retryable bool
	}{
		{codes.NotFound, "not_found", false},
		{codes.InvalidArgument, "invalid_argument", false},
		{codes.DeadlineExceeded, "timeout", true},
		{codes.ResourceExhausted, "rate_limited", true},
		{codes.Unavailable, "temporal_unavailable", true},
		{codes.PermissionDenied, "permission_denied", false},
	}
	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			var got *toolError
			if !errors.As(temporalError(status.Error(tt.code, "x"), &config.Target{Address: "a:7233"}, &peer.Peer{}, temporalServer), &got) {
				t.Fatal("not a tool error")
			}
			if got.Code != tt.want || got.Retryable != tt.retryable {
				t.Errorf("code = %s, retryable = %v, want %s, %v", got.Code, got.Retryable, tt.want, tt.retryable)
			}
		})
	}
}

type flakyWorkflowService struct {
	workflowservice.UnimplementedWorkflowServiceServer
	failures   int32
	calls      atomic.Int32
	mu         sync.Mutex
	requestIDs []string
}

// unavailable returns Unavailable for the first f.failures calls, and nil after.
func (f *flakyWorkflowService) unavailable() error {
	if f.calls.Add(1) <= f.failures {
		return status.Error(codes.Unavailable, "x")
	}
	return nil
}

func (f *flakyWorkflowService) CountWorkflowExecutions(context.Context, *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	if err := f.unavailable(); err != nil {
		return nil, err
	}
	return &workflowservice.CountWorkflowExecutionsResponse{Count: 7}, nil
}

func (f *flakyWorkflowService) StartWorkflowExecution(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest) (*workflowservice.StartWorkflowExecutionResponse, error) {
	f.mu.Lock()
	f.requestIDs = append(f.requestIDs, req.GetRequestId())
	f.mu.Unlock()
	if err := f.unavailable(); err != nil {
		return nil, err
	}
	return &workflowservice.StartWorkflowExecutionResponse{RunId: "r", Started: true}, nil
}

func TestCallServerRetriesUnavailable(t *testing.T) {
	tests := []struct {
		name      string
		failures  int32
		wantCalls int32
		wantError string
	}{
		{"blip", 1, 2, ""},
		{"down", 1 << 30, 4, "temporal_unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &flakyWorkflowService{failures: tt.failures}
			ln := listen(t)
			serve(t, ln, fake)
			cfg := &config.Config{Targets: []config.Target{{Name: "a", Description: "x", Address: ln.Addr().String(), Namespace: "default"}}}
			clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			ctx := temporal.WithTool(context.WithValue(t.Context(), targetsKey{}, []string{"a"}), "count_workflows", []string{workflowservice.WorkflowService_CountWorkflowExecutions_FullMethodName}, []string{"a"})

			_, env, err := countWorkflows(cfg, clients)(ctx, nil, countWorkflowsInput{Target: "a"})

			var te *toolError
			switch {
			case tt.wantError == "" && (err != nil || env.Result.Count != 7):
				t.Errorf("count = %d, error = %v; want 7 and no error", env.Result.Count, err)
			case tt.wantError != "" && (!errors.As(err, &te) || te.Code != tt.wantError):
				t.Errorf("error = %v, want %s", err, tt.wantError)
			}
			if got := fake.calls.Load(); got != tt.wantCalls {
				t.Errorf("calls = %d, want %d", got, tt.wantCalls)
			}
		})
	}
}

func TestStartWorkflowRetriesWithOneRequestID(t *testing.T) {
	fake := &flakyWorkflowService{failures: 1}
	ln := listen(t)
	serve(t, ln, fake)
	cfg := &config.Config{Targets: []config.Target{{Name: "a", Description: "x", Address: ln.Addr().String(), Namespace: "default"}}}
	clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx := temporal.WithTool(context.WithValue(t.Context(), targetsKey{}, []string{"a"}), "start_workflow", []string{workflowservice.WorkflowService_StartWorkflowExecution_FullMethodName}, []string{"a"})

	_, env, err := startWorkflow(cfg, clients, newRenderer(t, cfg, slog.New(slog.DiscardHandler)))(ctx, &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{}}, startWorkflowInput{Target: "a", WorkflowID: "w", WorkflowType: "T", TaskQueue: "q"})

	if err != nil || !env.Result.Started {
		t.Fatalf("result = %+v, error = %v; want a started run", env.Result, err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if ids := fake.requestIDs; len(ids) != 2 || ids[0] == "" || ids[0] != ids[1] {
		t.Errorf("request IDs = %q, want one request ID on both attempts", ids)
	}
}
