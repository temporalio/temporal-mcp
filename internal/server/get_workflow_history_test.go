package server

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

type failingHistoryService struct {
	workflowservice.UnimplementedWorkflowServiceServer
	mu        sync.Mutex
	failed    bool
	gotTokens [][]byte
}

func (*failingHistoryService) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (f *failingHistoryService) GetWorkflowExecutionHistory(_ context.Context, req *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotTokens = append(f.gotTokens, req.GetNextPageToken())
	event := func(id int64, typ enumspb.EventType) *historypb.HistoryEvent {
		return &historypb.HistoryEvent{EventId: id, EventType: typ}
	}
	if len(req.GetNextPageToken()) == 0 {
		return &workflowservice.GetWorkflowExecutionHistoryResponse{
			History:       &historypb.History{Events: []*historypb.HistoryEvent{event(1, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED), event(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}},
			NextPageToken: []byte("p2"),
		}, nil
	}
	if !f.failed {
		f.failed = true
		return nil, status.Error(codes.Unavailable, "unavailable")
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{event(3, enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED)}}}, nil
}

func TestGetWorkflowHistoryFailedPage(t *testing.T) {
	eventIDs := func(events []historyEvent) []int64 {
		ids := []int64{}
		for _, e := range events {
			ids = append(ids, e.EventID)
		}
		return ids
	}
	ln := listen(t)
	fake := &failingHistoryService{}
	serve(t, ln, fake)
	cfg := &config.Config{Targets: []config.Target{{Name: "a", Description: "x", Address: ln.Addr().String(), Namespace: "default"}}}
	clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ctx := temporal.WithTool(context.WithValue(t.Context(), targetsKey{}, []string{"a"}), "get_workflow_history", []string{workflowservice.WorkflowService_GetWorkflowExecutionHistory_FullMethodName}, []string{"a"})
	history := getWorkflowHistory(cfg, clients)
	call := func(continuation string) (envelope[workflowHistory], error) {
		_, env, err := history(ctx, nil, getWorkflowHistoryInput{getWorkflowInput: getWorkflowInput{Target: "a", WorkflowID: "wf", RunID: "run"}, Order: "oldest_first", Limit: 25, Continuation: continuation})
		return env, err
	}

	env, err := call("")
	if err != nil {
		t.Fatal(err)
	}
	if got := eventIDs(env.Result.Events); !slices.Equal(got, []int64{1, 2}) || env.Complete || env.NextCall == nil {
		t.Fatalf("event IDs = %v, complete = %v, next_call = %v; want [1 2], incomplete and a next_call", got, env.Complete, env.NextCall)
	}
	env, err = call(env.NextCall.Arguments["continuation"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if got := eventIDs(env.Result.Events); !slices.Equal(got, []int64{3}) || !env.Complete {
		t.Errorf("event IDs = %v, complete = %v; want [3] and complete", got, env.Complete)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if last := fake.gotTokens[len(fake.gotTokens)-1]; !bytes.Equal(last, []byte("p2")) {
		t.Errorf("Temporal got page token %q on the continued call, want p2", last)
	}
}
