package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/payload"
	"github.com/temporalio/temporal-mcp/internal/temporal"
)

// A blockingWorkflowService holds each CountWorkflowExecutions call, after sending on started, until release is closed.
type blockingWorkflowService struct {
	workflowservice.UnimplementedWorkflowServiceServer
	started chan struct{}
	release chan struct{}
}

func (*blockingWorkflowService) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (s *blockingWorkflowService) CountWorkflowExecutions(ctx context.Context, _ *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	s.started <- struct{}{}
	select {
	case <-s.release:
		return &workflowservice.CountWorkflowExecutionsResponse{Count: 1}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ping returns the code of a GetSystemInfo call through the client for target in clients.
func ping(ctx context.Context, clients *temporal.Clients, target string) codes.Code {
	ctx = temporal.WithTool(ctx, "check_connection", []string{workflowservice.WorkflowService_GetSystemInfo_FullMethodName}, []string{target})
	_, err := clients.Client(target).WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
	return serviceerror.ToStatus(err).Code()
}

func TestReload(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	fake := &blockingWorkflowService{started: make(chan struct{}, 1), release: make(chan struct{})}
	ln := listen(t)
	serve(t, ln, fake)
	logger := slog.New(slog.DiscardHandler)
	newConfig := func(targets ...config.Target) (*config.Config, *temporal.Clients, *payload.Renderer) {
		t.Helper()
		cfg := &config.Config{Targets: targets}
		for i := range cfg.Targets {
			cfg.Targets[i].Description, cfg.Targets[i].Address, cfg.Targets[i].Namespace = "x", ln.Addr().String(), "default"
		}
		clients, err := temporal.NewClients(cfg.Targets, "test", "0", logger)
		if err != nil {
			t.Fatal(err)
		}
		return cfg, clients, newRenderer(t, cfg, logger)
	}
	cfg, clients, payloads := newConfig(config.Target{Name: "a"})
	s, err := NewServer(ctx, ctx, "temporal", "0", cfg, clients, payloads, io.Discard, logger)
	if err != nil {
		t.Fatal(err)
	}
	session := connectTo(t, NewMux(s.Server, cfg, logger), http.DefaultTransport)

	type callResult struct {
		res *mcp.CallToolResult
		err error
	}
	called := make(chan callResult, 1)
	go func() {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "count_workflows", Arguments: map[string]any{"target": "a"}})
		called <- callResult{res, err}
	}()
	<-fake.started

	next, nextClients, nextPayloads := newConfig(config.Target{Name: "b"})
	if err := s.Reload(next, nextClients, nextPayloads); err != nil {
		t.Fatal(err)
	}
	if got := availableTargets(t, session); !slices.Equal(got, []string{"b"}) {
		t.Errorf("list_available_targets after the reload = %v, want [b]", got)
	}
	if code := ping(ctx, clients, "a"); code != codes.OK {
		t.Errorf("old clients while a call holds them: %v, want OK", code)
	}
	close(fake.release)
	got := <-called
	if got.err != nil || got.res.IsError {
		t.Fatalf("count_workflows in progress during the reload = %+v, %v; want a result", got.res, got.err)
	}
	waitUntil(ctx, t, "the old clients close", func() bool { return ping(ctx, clients, "a") == codes.Canceled })
	if code := ping(ctx, nextClients, "b"); code != codes.OK {
		t.Errorf("new clients: %v, want OK", code)
	}

	bad, badClients, badPayloads := newConfig(config.Target{Name: "c"})
	bad.Authorization = &config.Authorization{Profiles: map[string]config.Profile{"p": {Tools: []string{"terminate_workflow"}, Targets: []string{"c"}}}}
	if err := s.Reload(bad, badClients, badPayloads); err == nil || !strings.Contains(err.Error(), `unknown tool "terminate_workflow"`) {
		t.Errorf("Reload with an unknown tool: err = %v, want an unknown tool error", err)
	}
	if code := ping(ctx, badClients, "c"); code != codes.Canceled {
		t.Errorf("clients of the failed reload: %v, want Canceled", code)
	}
	if got := availableTargets(t, session); !slices.Equal(got, []string{"b"}) {
		t.Errorf("list_available_targets after the failed reload = %v, want [b]", got)
	}
}

func TestCallKeepsItsSnapshot(t *testing.T) {
	clients, err := temporal.NewClients(nil, "test", "0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	var records bytes.Buffer
	logger := slog.New(slog.DiscardHandler)
	s, _ := newServer(t.Context(), t.Context(), "temporal", "0", &config.Config{Targets: []config.Target{{Name: "a", Aliases: []string{"alpha"}}}}, clients, nil, &records, logger)
	next := newSnapshot(&config.Config{Targets: []config.Target{{Name: "b", Aliases: []string{"alpha"}}}}, clients, nil)
	// A reload lands once the call is pinned, before audit and authorize run.
	reload := func(h mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			s.replace(next)
			return h(ctx, method, req)
		}
	}
	var targets []string
	handler := func(ctx context.Context, _ string, _ mcp.Request) (mcp.Result, error) {
		targets = toolTargets(ctx)
		return &mcp.CallToolResult{}, nil
	}
	call := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "count_workflows", Arguments: json.RawMessage(`{"target":"alpha"}`)}}
	if _, err := pin(s)(reload(audit(&records, logger, s.access)(authorize()(handler))))(t.Context(), "tools/call", call); err != nil {
		t.Fatal(err)
	}
	var rec struct{ Target string }
	if err := json.Unmarshal(records.Bytes(), &rec); err != nil || rec.Target != "a" || !slices.Equal(targets, []string{"a"}) {
		t.Errorf("audit record %s (%v) and the call's targets %v after a reload mid-call, want target a and [a]", records.Bytes(), err, targets)
	}
}

func TestReloadAuthorization(t *testing.T) {
	idp, sign := newTestIssuer(t)
	logger := slog.New(slog.DiscardHandler)
	// load returns the config with targets a and b and the given authorization section, with its clients.
	load := func(authorization string) (*config.Config, *temporal.Clients, *payload.Renderer) {
		t.Helper()
		cfg, err := config.Parse([]byte(fmt.Sprintf(`listen: 127.0.0.1:0
targets:
  - {name: a, description: x, address: "a:7233", namespace: default}
  - {name: b, description: x, address: "b:7233", namespace: default}
auth: {resource: https://mcp.example.com/mcp, issuer: %q, audience: operator, required_scopes: [temporal.read]}
authorization:
%s`, idp, authorization)))
		if err != nil {
			t.Fatal(err)
		}
		clients, err := temporal.NewClients(cfg.Targets, "test", "0", logger)
		if err != nil {
			t.Fatal(err)
		}
		return cfg, clients, newRenderer(t, cfg, logger)
	}
	cfg, clients, payloads := load(`  profiles:
    dev: {tools_deny: [count_workflows], targets: [a]}
  rules:
    - {claim_name: groups, claim_value: dev, profile: dev}
`)
	s, err := NewServer(t.Context(), t.Context(), "temporal", "0", cfg, clients, payloads, io.Discard, logger)
	if err != nil {
		t.Fatal(err)
	}
	mux := NewMux(s.Server, cfg, logger)
	dev := connectTo(t, mux, bearer(sign(map[string]any{"groups": []any{"dev"}})))
	ops := connectTo(t, mux, bearer(sign(map[string]any{"groups": []any{"ops"}})))
	hasTool := func(session *mcp.ClientSession, name string) bool {
		t.Helper()
		list, err := session.ListTools(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		return slices.ContainsFunc(list.Tools, func(tool *mcp.Tool) bool { return tool.Name == name })
	}
	if got := availableTargets(t, dev); !slices.Equal(got, []string{"a"}) || hasTool(dev, "count_workflows") {
		t.Errorf("before the reload, dev gets targets %v and count_workflows %v; want [a] without count_workflows", got, hasTool(dev, "count_workflows"))
	}
	if got := availableTargets(t, ops); got != nil {
		t.Errorf("before the reload, ops gets targets %v, want none", got)
	}

	// The reload changes dev's profile, and adds a profile and a rule for ops.
	if err := s.Reload(load(`  profiles:
    dev: {targets: [b]}
    ops: {targets: [a]}
  rules:
    - {claim_name: groups, claim_value: dev, profile: dev}
    - {claim_name: groups, claim_value: ops, profile: ops}
`)); err != nil {
		t.Fatal(err)
	}
	if got := availableTargets(t, dev); !slices.Equal(got, []string{"b"}) || !hasTool(dev, "count_workflows") {
		t.Errorf("after the reload, dev gets targets %v and count_workflows %v; want [b] with count_workflows", got, hasTool(dev, "count_workflows"))
	}
	if got := availableTargets(t, ops); !slices.Equal(got, []string{"a"}) {
		t.Errorf("after the reload, ops gets targets %v, want [a]", got)
	}
}

func TestReloadStartableWorkflows(t *testing.T) {
	idp, sign := newTestIssuer(t)
	logger := slog.New(slog.DiscardHandler)
	// load returns the config whose target a declares declared and whose profile p starts the types in workflows, with
	// its clients.
	load := func(declared, workflows string) (*config.Config, *temporal.Clients, *payload.Renderer) {
		t.Helper()
		cfg, err := config.Parse([]byte(fmt.Sprintf(`listen: 127.0.0.1:0
targets:
  - {name: a, description: x, address: "a:7233", namespace: default, workflows: [%s]}
auth: {resource: https://mcp.example.com/mcp, issuer: %q, audience: operator, required_scopes: [temporal.read]}
authorization:
  profiles: {p: {tools: [start_workflow], workflows: [%s], targets: [a]}}
  rules: [{claim_name: groups, claim_value: p, profile: p}]
`, declared, idp, workflows)))
		if err != nil {
			t.Fatal(err)
		}
		clients, err := temporal.NewClients(cfg.Targets, "test", "0", logger)
		if err != nil {
			t.Fatal(err)
		}
		return cfg, clients, newRenderer(t, cfg, logger)
	}
	cfg, clients, payloads := load("{type: A, task_queue: q}, {type: B, task_queue: q}", "A")
	s, err := NewServer(t.Context(), t.Context(), "temporal", "0", cfg, clients, payloads, io.Discard, logger)
	if err != nil {
		t.Fatal(err)
	}
	session := connectTo(t, NewMux(s.Server, cfg, logger), bearer(sign(map[string]any{"groups": []any{"p"}})))
	startable := func() []string {
		t.Helper()
		res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_startable_workflows", Arguments: map[string]any{"target": "a"}})
		if err != nil || res.IsError {
			t.Fatalf("list_startable_workflows = %+v, %v; want a result", res, err)
		}
		var out envelope[startableWorkflowList]
		if err := json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &out); err != nil {
			t.Fatal(err)
		}
		var workflows []string
		for _, w := range out.Result.Workflows {
			workflows = append(workflows, w.Type+" on "+w.TaskQueue)
		}
		return workflows
	}
	if got := startable(); !slices.Equal(got, []string{"A on q"}) {
		t.Errorf("before the reload, list_startable_workflows = %v, want [A on q]", got)
	}

	if err := s.Reload(load("{type: A, task_queue: r}, {type: B, task_queue: q}, {type: C, task_queue: q}", "A, C")); err != nil {
		t.Fatal(err)
	}
	if got := startable(); !slices.Equal(got, []string{"A on r", "C on q"}) {
		t.Errorf("after the reload, list_startable_workflows = %v, want [A on r, C on q]", got)
	}
}

// availableTargets returns the names of the targets that list_available_targets gives session, or nil if the call is
// denied.
func availableTargets(t *testing.T, session *mcp.ClientSession) []string {
	t.Helper()
	res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_available_targets"})
	var wire *jsonrpc.Error
	if errors.As(err, &wire) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out envelope[targetList]
	if err := json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &out); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, target := range out.Result.Targets {
		names = append(names, target.Name)
	}
	return names
}
