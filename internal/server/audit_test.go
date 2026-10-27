package server

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/temporal"
)

type recordWriter struct {
	writes   chan string
	failNext bool
}

func (w *recordWriter) Write(p []byte) (int, error) {
	if w.failNext {
		w.failNext = false
		w.writes <- string(p[:len(p)/2])
		return len(p) / 2, errors.New("disk full")
	}
	w.writes <- string(p)
	return len(p), nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAudit(t *testing.T) {
	issuer, sign := newTestIssuer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	ln.Close()
	var cancelCall context.CancelFunc
	hanging := listen(t)
	hang := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		cancelCall()
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	workflowservice.RegisterWorkflowServiceServer(hang, workflowservice.UnimplementedWorkflowServiceServer{})
	go hang.Serve(hanging)
	t.Cleanup(hang.Stop)
	targets := []config.Target{
		{Name: "granted", Aliases: []string{"g"}, Description: "x", Address: closed, Namespace: "a"},
		{Name: "ungranted", Aliases: []string{"u"}, Description: "x", Address: closed, Namespace: "b"},
		{Name: "hanging", Description: "x", Address: hanging.Addr().String(), Namespace: "c"},
	}
	clients, err := temporal.NewClients(targets, "test", "0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	authCfg := &config.Config{
		Targets: targets,
		Auth:    &config.Auth{Resource: "https://mcp.example.com/mcp", Issuer: issuer, Audience: "operator"},
		Authorization: &config.Authorization{
			Profiles: map[string]config.Profile{"p": {Tools: []string{"check_connection", "fail", "boom", "block", "cancel_and_panic", "deadline"}, Targets: []string{"granted"}}},
			Rules:    []config.Rule{{ClaimName: "sub", ClaimValue: "alice", Profile: "p"}},
		},
	}
	records := &recordWriter{writes: make(chan string, 10)}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	connectTo := func(h http.Handler, transport http.RoundTripper) *mcp.ClientSession {
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: srv.URL + "/mcp", HTTPClient: &http.Client{Transport: transport}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { session.Close() })
		return session
	}

	s, _ := newServer(t.Context(), t.Context(), "temporal", "0", authCfg, clients, newRenderer(t, authCfg, logger), records, logger)
	addTool(s, &mcp.Tool{Name: "fail"}, nil, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, struct{}, error) {
		return nil, struct{}{}, errors.New("dial tcp 10.0.0.1:7233: connection refused")
	})
	addTool(s, &mcp.Tool{Name: "boom"}, nil, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, struct{}, error) {
		panic("boom-value")
	})
	addTool(s, &mcp.Tool{Name: "deadline"}, nil, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, struct{}, error) {
		return nil, struct{}{}, context.DeadlineExceeded
	})
	addTool(s, &mcp.Tool{Name: "block"}, nil, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, struct{}, error) {
		cancelCall()
		<-ctx.Done()
		return nil, struct{}{}, ctx.Err()
	})
	addTool(s, &mcp.Tool{Name: "cancel_and_panic"}, nil, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, struct{}, error) {
		cancelCall()
		<-ctx.Done()
		panic("boom-value")
	})
	token := sign(map[string]any{})
	var callToken atomic.Pointer[string]
	callToken.Store(&token)
	authed := connectTo(NewMux(s.Server, authCfg, logger), roundTripFunc(func(r *http.Request) (*http.Response, error) { return bearer(*callToken.Load()).RoundTrip(r) }))
	openCfg := &config.Config{Targets: targets}
	open, _ := newServer(t.Context(), t.Context(), "temporal", "0", openCfg, clients, newRenderer(t, openCfg, logger), records, logger)
	unauthed := connectTo(NewMux(open.Server, openCfg, logger), http.DefaultTransport)
	if _, err := authed.ListTools(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case w := <-records.writes:
		t.Errorf("tools/list wrote audit record %q", w)
	default:
	}
	alice := func(record map[string]any) map[string]any {
		record["issuer"], record["subject"] = issuer, "alice"
		return record
	}

	tests := []struct {
		name      string
		session   *mcp.ClientSession
		token     string
		tool      string
		args      string
		failWrite bool
		callErr   string
		record    map[string]any
		log       string
	}{
		{name: "ok", session: authed, tool: "check_connection", args: `{"target":"g","unused":null}`,
			record: alice(map[string]any{"tool": "check_connection", "access": "read", "target": "granted", "arguments": map[string]any{"target": "g", "unused": nil}, "outcome": "ok", "temporal_calls": 1.0})},
		{name: "denied tool", session: authed, tool: "get_workflows", args: `{"target":"g","workflows":[{"workflow_id":"w"}]}`, callErr: `unknown tool "get_workflows"`,
			record: alice(map[string]any{"tool": "get_workflows", "access": "read", "target": "granted", "arguments": map[string]any{"target": "g", "workflows": []any{map[string]any{"workflow_id": "w"}}}, "outcome": "denied", "temporal_calls": 0.0})},
		{name: "denied target", session: authed, tool: "check_connection", args: `{"target":"u"}`,
			record: alice(map[string]any{"tool": "check_connection", "access": "read", "target": "ungranted", "arguments": map[string]any{"target": "u"}, "outcome": "denied", "temporal_calls": 0.0})},
		{name: "unknown target", session: authed, tool: "check_connection", args: `{"target":"nope"}`,
			record: alice(map[string]any{"tool": "check_connection", "access": "read", "target": "nope", "arguments": map[string]any{"target": "nope"}, "outcome": "not_found", "temporal_calls": 0.0})},
		{name: "unknown tool", session: authed, tool: "no_such_tool", args: `{"target":"g"}`, callErr: `unknown tool "no_such_tool"`,
			record: alice(map[string]any{"tool": "no_such_tool", "access": nil, "target": "granted", "arguments": map[string]any{"target": "g"}, "outcome": "unknown_tool", "temporal_calls": 0.0})},
		{name: "invalid arguments", session: authed, tool: "check_connection", args: `{"target":"g","extra":1}`,
			record: alice(map[string]any{"tool": "check_connection", "access": "read", "target": "granted", "arguments": map[string]any{"target": "g", "extra": 1.0}, "outcome": "invalid_argument", "temporal_calls": 0.0})},
		{name: "internal error", session: authed, tool: "fail", args: `{"target":null}`,
			record: alice(map[string]any{"tool": "fail", "access": "write", "target": nil, "arguments": map[string]any{"target": nil}, "outcome": "internal", "temporal_calls": 0.0}),
			log:    `level=ERROR msg="tool call failed" tool=fail error="dial tcp 10.0.0.1:7233: connection refused"`},
		{name: "panicking tool", session: authed, tool: "boom", args: `{}`,
			record: alice(map[string]any{"tool": "boom", "access": "write", "target": nil, "arguments": map[string]any{}, "outcome": "internal", "temporal_calls": 0.0}),
			log:    `level=ERROR msg="tool call failed" tool=boom error="panic: boom-value\ngoroutine `},
		{name: "cancelled", session: authed, tool: "block", args: `{}`, callErr: "context canceled",
			record: alice(map[string]any{"tool": "block", "access": "write", "target": nil, "arguments": map[string]any{}, "outcome": "cancelled", "temporal_calls": 0.0})},
		{name: "deadline exceeded in a live call", session: authed, tool: "deadline", args: `{}`,
			record: alice(map[string]any{"tool": "deadline", "access": "write", "target": nil, "arguments": map[string]any{}, "outcome": "internal", "temporal_calls": 0.0}),
			log:    `level=ERROR msg="tool call failed" tool=deadline error="context deadline exceeded"`},
		{name: "cancelled check_connection", session: unauthed, tool: "check_connection", args: `{"target":"hanging"}`, callErr: "context canceled",
			record: map[string]any{"issuer": nil, "subject": nil, "tool": "check_connection", "access": "read", "target": "hanging", "arguments": map[string]any{"target": "hanging"}, "outcome": "cancelled", "temporal_calls": 1.0}},
		{name: "cancelled get_workflows", session: unauthed, tool: "get_workflows", args: `{"target":"hanging","workflows":[{"workflow_id":"w"}]}`, callErr: "context canceled",
			record: map[string]any{"issuer": nil, "subject": nil, "tool": "get_workflows", "access": "read", "target": "hanging", "arguments": map[string]any{"target": "hanging", "workflows": []any{map[string]any{"workflow_id": "w"}}}, "outcome": "cancelled", "temporal_calls": 1.0}},
		{name: "cancelled call that panics", session: authed, tool: "cancel_and_panic", args: `{}`, callErr: "context canceled",
			record: alice(map[string]any{"tool": "cancel_and_panic", "access": "write", "target": nil, "arguments": map[string]any{}, "outcome": "internal", "temporal_calls": 0.0}),
			log:    `level=ERROR msg="tool call failed" tool=cancel_and_panic error="panic: boom-value\ngoroutine `},
		{name: "rejected token", session: authed, token: "not-a-token", tool: "check_connection", args: `{"target":"g"}`, callErr: "Unauthorized",
			log: `level=WARN msg="rejected access token" reason="malformed token"`},
		{name: "write failure", session: unauthed, tool: "list_available_targets", failWrite: true, callErr: "internal error",
			log: `level=ERROR msg="cannot write audit record" tool=list_available_targets error="disk full"`},
		{name: "no auth, after a write failure", session: unauthed, tool: "list_available_targets",
			record: map[string]any{"issuer": nil, "subject": nil, "tool": "list_available_targets", "access": "read", "target": nil, "arguments": map[string]any{}, "outcome": "ok", "temporal_calls": 0.0}},
	}
	var stream string
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs.Reset()
			callToken.Store(new(cmp.Or(tt.token, token)))
			records.failNext = tt.failWrite
			params := &mcp.CallToolParams{Name: tt.tool}
			if tt.args != "" {
				params.Arguments = json.RawMessage(tt.args)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cancelCall = cancel
			before := time.Now()
			if _, err := tt.session.CallTool(ctx, params); (err == nil) != (tt.callErr == "") || err != nil && !strings.Contains(err.Error(), tt.callErr) {
				t.Errorf("CallTool error = %v, want %q", err, tt.callErr)
			}
			if tt.record != nil || tt.failWrite {
				select {
				case w := <-records.writes:
					stream += w
				case <-time.After(10 * time.Second):
					t.Fatal("no audit write")
				}
			}
			after := time.Now()
			select {
			case w := <-records.writes:
				t.Errorf("unexpected audit write %q", w)
			default:
			}
			if got := logs.String(); tt.log == "" && got != "" || !strings.Contains(got, tt.log) {
				t.Errorf("log = %q, want %q", got, tt.log)
			}
			if tt.record == nil {
				return
			}
			rest, ok := strings.CutSuffix(stream, "\n")
			line := rest[strings.LastIndex(rest, "\n")+1:]
			var got map[string]any
			if !ok || json.Unmarshal([]byte(line), &got) != nil {
				t.Fatalf("last audit line %q is not a line of JSON", line)
			}
			ts, _ := got["time"].(string)
			at, err := time.Parse(time.RFC3339Nano, ts)
			if err != nil || at.Location() != time.UTC || at.Before(before) || at.After(after) {
				t.Errorf("time = %v, want a UTC time between %v and %v", got["time"], before, after)
			}
			if ms, ok := got["duration_ms"].(float64); !ok || ms < 0 || ms > float64(after.Sub(before).Milliseconds()) {
				t.Errorf("duration_ms = %v, want at most %v", got["duration_ms"], after.Sub(before).Milliseconds())
			}
			delete(got, "time")
			delete(got, "duration_ms")
			if !reflect.DeepEqual(got, tt.record) {
				t.Errorf("record = %#v\nwant %#v", got, tt.record)
			}
		})
	}
}
