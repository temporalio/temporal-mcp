package server

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
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
		{codes.PermissionDenied, "temporal_unavailable", false},
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
	failures int32
	calls    atomic.Int32
}

func (f *flakyWorkflowService) CountWorkflowExecutions(context.Context, *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	if f.calls.Add(1) <= f.failures {
		return nil, status.Error(codes.Unavailable, "x")
	}
	return &workflowservice.CountWorkflowExecutionsResponse{Count: 7}, nil
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
