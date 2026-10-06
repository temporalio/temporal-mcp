package server

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

type pagingWorkflowService struct {
	workflowservice.UnimplementedWorkflowServiceServer
	pageToken []byte
	mu        sync.Mutex
	gotTokens [][]byte
}

func (*pagingWorkflowService) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (f *pagingWorkflowService) ListWorkflowExecutions(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotTokens = append(f.gotTokens, req.GetNextPageToken())
	resp := &workflowservice.ListWorkflowExecutionsResponse{Executions: []*workflowpb.WorkflowExecutionInfo{{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: "run"}}}}
	if len(req.GetNextPageToken()) == 0 {
		resp.NextPageToken = f.pageToken
	}
	return resp, nil
}

func TestSearchWorkflowsPageTokenSize(t *testing.T) {
	tests := []struct {
		name      string
		tokenSize int
		wantNext  bool
	}{
		{"token that fits", 3000, true},
		{"token too long to hand out", 3100, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &pagingWorkflowService{pageToken: bytes.Repeat([]byte{0xff}, tt.tokenSize)}
			ln := listen(t)
			serve(t, ln, fake)
			cfg := &config.Config{Targets: []config.Target{{Name: "a", Description: "x", Address: ln.Addr().String(), Namespace: "default"}}}
			clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			ctx := temporal.WithTool(context.WithValue(t.Context(), targetsKey{}, []string{"a"}), "search_workflows", []string{workflowservice.WorkflowService_ListWorkflowExecutions_FullMethodName}, []string{"a"})
			search := searchWorkflows(cfg, clients)
			_, env, err := search(ctx, nil, searchWorkflowsInput{Target: "a", PageSize: 25})
			if err != nil {
				t.Fatal(err)
			}
			if env.Complete || len(env.Result.Workflows) != 1 || (env.NextCall != nil) != tt.wantNext {
				t.Fatalf("complete = %v, %d workflows, next_call = %v; want false, 1 and a next_call: %v", env.Complete, len(env.Result.Workflows), env.NextCall, tt.wantNext)
			}
			if !tt.wantNext {
				return
			}
			_, env, err = search(ctx, nil, searchWorkflowsInput{Target: "a", PageSize: 25, Continuation: env.NextCall.Arguments["continuation"].(string)})
			if err != nil {
				t.Fatal(err)
			}
			if !env.Complete {
				t.Errorf("second page complete = false, want true")
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.gotTokens) != 2 || !bytes.Equal(fake.gotTokens[1], fake.pageToken) {
				t.Errorf("Temporal got page tokens of lengths %d, want none and then the one it returned", len(fake.gotTokens))
			}
		})
	}
}
