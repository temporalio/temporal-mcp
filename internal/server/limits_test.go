package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"golang.org/x/time/rate"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
	"github.com/temporalio/temporal-operator-mcp/internal/temporal"
)

// slowHistoryService serves a history of two pages, events 1 and 2 and then 3 and 4, and holds a request for the
// second page while hang is set.
type slowHistoryService struct {
	workflowservice.UnimplementedWorkflowServiceServer
	hang atomic.Bool
}

func (f *slowHistoryService) GetWorkflowExecutionHistory(ctx context.Context, req *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	page := func(ids ...int64) *historypb.History {
		h := &historypb.History{}
		for _, id := range ids {
			h.Events = append(h.Events, &historypb.HistoryEvent{EventId: id, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED})
		}
		return h
	}
	if len(req.GetNextPageToken()) == 0 {
		return &workflowservice.GetWorkflowExecutionHistoryResponse{History: page(1, 2), NextPageToken: []byte("2")}, nil
	}
	if f.hang.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: page(3, 4)}, nil
}

func TestCallDeadline(t *testing.T) {
	defer func(d, r time.Duration) { toolCallTimeout, historyReserve = d, r }(toolCallTimeout, historyReserve)
	toolCallTimeout, historyReserve = 600*time.Millisecond, 300*time.Millisecond
	fake := &slowHistoryService{}
	fake.hang.Store(true)
	ln := listen(t)
	serve(t, ln, fake)
	cfg := &config.Config{Targets: []config.Target{{Name: "a", Description: "x", Address: ln.Addr().String(), Namespace: "default"}}}
	clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	session := serveTools(t, cfg, clients, t.Output(), func(s *toolServer) {
		addTool(s, &mcp.Tool{Name: "wait"}, nil, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, struct{}, error) {
			<-ctx.Done()
			return nil, struct{}{}, ctx.Err()
		})
	})
	call := func(name string, args map[string]any) *mcp.CallToolResult {
		t.Helper()
		res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	read := func(args map[string]any) (ids []float64, env map[string]any) {
		t.Helper()
		res := call("get_workflow_history", args)
		if res.IsError {
			t.Fatalf("get_workflow_history returned an error: %s", res.Content[0].(*mcp.TextContent).Text)
		}
		env = res.StructuredContent.(map[string]any)
		for _, e := range env["result"].(map[string]any)["events"].([]any) {
			ids = append(ids, e.(map[string]any)["event_id"].(float64))
		}
		return ids, env
	}

	ids, env := read(map[string]any{"target": "a", "workflow_id": "wf", "run_id": "run"})
	if !reflect.DeepEqual(ids, []float64{1, 2}) || env["complete"] != false || env["next_call"] == nil {
		t.Fatalf("events %v, complete = %v, next_call = %v; want events 1 and 2 and a next_call", ids, env["complete"], env["next_call"])
	}
	next := env["next_call"].(map[string]any)["arguments"].(map[string]any)
	timeout := map[string]any{"code": "timeout", "message": "the call did not finish within 600ms", "retryable": true}
	if got := errorContent(t, call("get_workflow_history", next)); !reflect.DeepEqual(got, timeout) {
		t.Errorf("next_call that reads no page: error = %v, want %v", got, timeout)
	}
	fake.hang.Store(false)
	ids, env = read(next)
	if !reflect.DeepEqual(ids, []float64{3, 4}) || env["complete"] != true {
		t.Errorf("after next_call: events %v, complete = %v; want events 3 and 4 and complete", ids, env["complete"])
	}

	if got := errorContent(t, call("wait", map[string]any{})); !reflect.DeepEqual(got, timeout) {
		t.Errorf("a call that outlives its deadline: error = %v, want %v", got, timeout)
	}
}

// serveBlocking serves a tool, block, whose calls on any target but "free" wait until the test ends. call calls block
// on target, or on no target if it is "". queue calls block in the background and returns the channel its result comes
// on. hold calls block in the background and returns once that call is waiting.
func serveBlocking(t *testing.T) (call func(target string) *mcp.CallToolResult, queue func(target string) <-chan *mcp.CallToolResult, hold func(target string)) {
	t.Helper()
	started, release := make(chan struct{}, 10), make(chan struct{})
	cfg := &config.Config{Targets: []config.Target{
		{Name: "hung", Aliases: []string{"h"}, Description: "x", Address: "127.0.0.1:1", Namespace: "a"},
		{Name: "free", Description: "x", Address: "127.0.0.1:1", Namespace: "b"},
	}}
	session := serveTools(t, cfg, nil, t.Output(), func(s *toolServer) {
		addTool(s, &mcp.Tool{Name: "block"}, nil, func(_ context.Context, _ *mcp.CallToolRequest, in struct {
			Target string `json:"target,omitempty"`
		}) (*mcp.CallToolResult, struct{}, error) {
			if in.Target != "free" {
				started <- struct{}{}
				<-release
			}
			return nil, struct{}{}, nil
		})
	})
	var held sync.WaitGroup
	t.Cleanup(func() {
		close(release)
		held.Wait()
	})
	params := func(target string) *mcp.CallToolParams {
		args := map[string]any{}
		if target != "" {
			args["target"] = target
		}
		return &mcp.CallToolParams{Name: "block", Arguments: args}
	}
	call = func(target string) *mcp.CallToolResult {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		res, err := session.CallTool(ctx, params(target))
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	queue = func(target string) <-chan *mcp.CallToolResult {
		result := make(chan *mcp.CallToolResult, 1)
		held.Go(func() {
			res, _ := session.CallTool(context.Background(), params(target))
			result <- res
		})
		return result
	}
	hold = func(target string) {
		t.Helper()
		select {
		case <-started:
		case <-queue(target):
			t.Fatalf("call on %q ended instead of waiting", target)
		}
	}
	return call, queue, hold
}

var busy = map[string]any{"code": "rate_limited", "message": "this server is busy with other calls; retry later", "retryable": true}

func TestConcurrentCallsPerTarget(t *testing.T) {
	defer func(d time.Duration, n, m int) {
		toolCallTimeout, maxToolCallsPerTarget, maxToolCallsPerReplica = d, n, m
	}(toolCallTimeout, maxToolCallsPerTarget, maxToolCallsPerReplica)
	toolCallTimeout, maxToolCallsPerTarget, maxToolCallsPerReplica = 200*time.Millisecond, 2, 3
	call, queue, hold := serveBlocking(t)
	hold("hung")
	hold("h")
	start := time.Now()
	waiting := queue("hung")
	time.Sleep(toolCallTimeout / 4)
	// The call waiting for the hung target holds no slot on the replica, so the replica's last slot is free.
	if res := call("free"); res.IsError || time.Since(start) >= toolCallTimeout {
		t.Errorf("call on another target: isError = %v after %v, want success before the waiting call's deadline", res.IsError, time.Since(start))
	}
	if got := errorContent(t, <-waiting); !reflect.DeepEqual(got, busy) || time.Since(start) < toolCallTimeout {
		t.Errorf("call on the hung target: error = %v after %v, want %v at its deadline", got, time.Since(start), busy)
	}
}

func TestConcurrentCallsPerReplica(t *testing.T) {
	defer func(d time.Duration, n int) { toolCallTimeout, maxToolCallsPerReplica = d, n }(toolCallTimeout, maxToolCallsPerReplica)
	toolCallTimeout, maxToolCallsPerReplica = 200*time.Millisecond, 2
	call, _, hold := serveBlocking(t)
	hold("")
	hold("hung")
	start := time.Now()
	if got := errorContent(t, call("free")); !reflect.DeepEqual(got, busy) || time.Since(start) < toolCallTimeout {
		t.Errorf("call over the cap: error = %v after %v, want %v at its deadline", got, time.Since(start), busy)
	}
}

func TestShutdownWhileWaitingForSlot(t *testing.T) {
	defer func(n int) { maxToolCallsPerReplica = n }(maxToolCallsPerReplica)
	maxToolCallsPerReplica = 1
	l := newLimits(&config.Config{})
	call := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "test"}}
	release, err := l.acquire(t.Context(), call)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	stop, shutdown := context.WithCancel(t.Context())
	handler := cancelWith(stop)(normalizeToolCalls(func(ctx context.Context, _ string, _ mcp.Request) (mcp.Result, error) {
		shutdown()
		_, err := l.acquire(ctx, call)
		return errorResult(err), nil
	}))
	res, err := handler(t.Context(), "tools/call", call)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"code": "server_shutting_down", "message": "the server is shutting down", "retryable": true}
	if got := errorContent(t, res.(*mcp.CallToolResult)); !reflect.DeepEqual(got, want) {
		t.Errorf("call waiting for a slot at shutdown: error = %v, want %v", got, want)
	}
}

func TestCallerRate(t *testing.T) {
	defer func(r rate.Limit, b int) { callerRate, callerBurst = r, b }(callerRate, callerBurst)
	callerRate, callerBurst = rate.Every(time.Minute), 2
	issuer, sign := newTestIssuer(t)
	otherIssuer, signOther := newTestIssuer(t)
	cfg := &config.Config{
		Targets: []config.Target{{Name: "a", Description: "x", Address: "127.0.0.1:1", Namespace: "a"}},
		Auth:    &config.Auth{Resource: "https://mcp.example.com/mcp", Issuers: []config.Issuer{{Issuer: issuer, Audience: "operator"}, {Issuer: otherIssuer, Audience: "operator"}}},
		Authorization: &config.Authorization{
			Profiles: map[string]config.Profile{"p": {Tools: []string{"list_available_targets"}, Targets: []string{"a"}}},
			Rules:    []config.Rule{{ClaimName: "sub", ClaimValue: "alice", Profile: "p"}},
		},
	}
	logger := slog.New(slog.DiscardHandler)
	s, _ := newServer(t.Context(), "temporal-operator", "0", cfg, nil, newRenderer(t, cfg, logger), io.Discard, logger)
	srv := httptest.NewServer(NewMux(s.Server, cfg, logger))
	t.Cleanup(srv.Close)
	connect := func(token string) *mcp.ClientSession {
		t.Helper()
		session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: srv.URL + "/mcp", HTTPClient: &http.Client{Transport: bearer(token)}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { session.Close() })
		return session
	}
	call := func(session *mcp.ClientSession) *mcp.CallToolResult {
		t.Helper()
		res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_available_targets"})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	session := connect(sign(map[string]any{}))
	for i := range callerBurst {
		if res := call(session); res.IsError {
			t.Fatalf("call %d: error %s, want success", i+1, res.Content[0].(*mcp.TextContent).Text)
		}
	}
	got := errorContent(t, call(session))
	if msg, _ := got["message"].(string); got["code"] != "rate_limited" || got["retryable"] != true || !strings.HasPrefix(msg, "too many calls from this caller; retry in ") {
		t.Errorf("call over the burst: error = %v, want rate_limited, retryable, saying when to retry", got)
	}
	if res := call(connect(signOther(map[string]any{}))); res.IsError {
		t.Errorf("another caller's call: error %s, want success", res.Content[0].(*mcp.TextContent).Text)
	}
}
