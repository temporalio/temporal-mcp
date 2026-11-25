package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc/oidctest"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/temporal"
)

func newTestIssuer(t *testing.T) (string, func(claims map[string]any) string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	idp := &oidctest.Server{PublicKeys: []oidctest.PublicKey{{PublicKey: &key.PublicKey, KeyID: "k1", Algorithm: "RS256"}}}
	srv := httptest.NewUnstartedServer(idp)
	idp.SetIssuer("http://" + srv.Listener.Addr().String())
	srv.Start()
	t.Cleanup(srv.Close)
	return srv.URL, func(claims map[string]any) string {
		claims = maps.Clone(claims)
		if _, ok := claims["scope"]; !ok {
			claims["scope"] = "temporal.read"
		}
		claims["iss"], claims["sub"], claims["aud"], claims["exp"] = srv.URL, "alice", "operator", time.Now().Add(time.Hour).Unix()
		b, err := json.Marshal(claims)
		if err != nil {
			t.Fatal(err)
		}
		return oidctest.SignIDToken(key, "k1", "RS256", string(b))
	}
}

func TestAuthorization(t *testing.T) {
	idp, sign := newTestIssuer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	ln.Close()
	staging := listen(t)
	gs := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(gs, fakeWorkflowService{})
	operatorservice.RegisterOperatorServiceServer(gs, fakeOperatorService{})
	go gs.Serve(staging)
	t.Cleanup(gs.Stop)

	yaml := fmt.Sprintf(`listen: 127.0.0.1:0
allow_writes_in_profiles: true
targets:
  - {name: payments-prod, aliases: [pp], description: x, address: %[1]q, namespace: a}
  - {name: payments-staging, description: x, address: %[3]q, namespace: b}
  - {name: orders-prod, description: x, address: %[1]q, namespace: c}
auth:
  resource: https://mcp.example.com/mcp
  issuer: %[2]q
  audience: operator
  required_scopes: [temporal.read]
authorization:
  profiles:
    oncall: {tools: [list_available_targets, check_connection], targets: [payments-prod]}
    dev: {tools: [list_available_targets, get_workflows, search_workflows, count_workflows, list_search_attributes], targets: [payments-staging]}
    orders: {tools: [get_workflows], targets: [orders-prod]}
    search: {tools: [search_workflows], targets: [payments-staging]}
    all: {targets: [payments-staging], allow_writes: true}
    no-history: {tools_deny: [get_workflow_history], targets: [payments-staging]}
  rules:
    - {claim_name: "cognito:groups", claim_value: temporal-oncall, profile: oncall}
    - {claim_name: "https://acme.example/roles", claim_value: Temporal.Dev, profile: dev}
    - {claim_name: "https://acme.example/roles", claim_value: Temporal.Orders, profile: orders}
    - {claim_name: "https://acme.example/roles", claim_value: Temporal.Search, profile: search}
    - {claim_name: email, claim_value: alice@acme.example, profile: oncall}
    - {claim_name: scope, claim_value: temporal.orders, profile: orders}
    - {claim_name: scp, claim_value: temporal.orders, profile: orders}
    - {claim_name: "https://acme.example/roles", claim_value: Temporal.All, profile: all}
    - {claim_name: "https://acme.example/roles", claim_value: Temporal.NoHistory, profile: no-history}
`, closed, idp, staging.Addr().String())
	cfg, err := config.Parse([]byte(yaml), "")
	if err != nil {
		t.Fatal(err)
	}
	clients, err := temporal.NewClients(cfg, "http", "test", "0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	s, err := NewServer(t.Context(), t.Context(), "temporal", "0", cfg, clients, newRenderer(t, cfg, logger), t.Output(), logger)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewMux(s.Server, cfg, logger))
	t.Cleanup(srv.Close)
	connectAs := func(t *testing.T, token string) *mcp.ClientSession {
		t.Helper()
		session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: srv.URL + "/mcp", HTTPClient: &http.Client{Transport: bearer(token)}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { session.Close() })
		return session
	}
	call := func(t *testing.T, session *mcp.ClientSession, name string, args map[string]any) (*mcp.CallToolResult, *jsonrpc.Error) {
		t.Helper()
		res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args})
		if err == nil {
			return res, nil
		}
		var wire *jsonrpc.Error
		if !errors.As(err, &wire) {
			t.Fatalf("%s: error %v is not a JSON-RPC error", name, err)
		}
		return nil, &jsonrpc.Error{Code: wire.Code, Message: strings.ReplaceAll(wire.Message, name, "TOOL"), Data: wire.Data}
	}
	unknownTool := func(t *testing.T, session *mcp.ClientSession, name string, args map[string]any) {
		t.Helper()
		_, got := call(t, session, name, args)
		_, want := call(t, session, "no_such_tool", args)
		if got == nil || want == nil || !reflect.DeepEqual(got, want) {
			t.Errorf("calling %s = %+v, want %+v as for a nonexistent tool", name, got, want)
		}
	}
	notFound := func(t *testing.T, session *mcp.ClientSession, tool string, args map[string]any) {
		t.Helper()
		target := args["target"].(string)
		res, _ := call(t, session, tool, args)
		args = maps.Clone(args)
		args["target"] = "nope"
		nonexistent, _ := call(t, session, tool, args)
		if res == nil || nonexistent == nil {
			t.Fatalf("%s on %s: want tool errors", tool, target)
		}
		got, want := errorContent(t, res), errorContent(t, nonexistent)
		got["message"] = strings.ReplaceAll(got["message"].(string), target, "nope")
		if want["code"] != "not_found" || !reflect.DeepEqual(got, want) {
			t.Errorf("%s on %s = %#v, want %#v as for a nonexistent target", tool, target, got, want)
		}
	}

	tests := []struct {
		name    string
		token   string
		tools   []string
		targets []string
	}{
		{
			name:    "two profiles",
			token:   sign(map[string]any{"cognito:groups": []any{"other", "temporal-oncall"}, "https://acme.example/roles": []any{"Temporal.Dev"}}),
			tools:   []string{"check_connection", "count_workflows", "get_workflows", "list_available_targets", "list_search_attributes", "search_workflows"},
			targets: []string{"payments-prod", "payments-staging"},
		},
		{
			name:    "one profile",
			token:   sign(map[string]any{"https://acme.example/roles": []any{"Temporal.Dev"}}),
			tools:   []string{"count_workflows", "get_workflows", "list_available_targets", "list_search_attributes", "search_workflows"},
			targets: []string{"payments-staging"},
		},
		{
			name:    "target granted only through another tool",
			token:   sign(map[string]any{"https://acme.example/roles": []any{"Temporal.Dev", "Temporal.Orders"}}),
			tools:   []string{"count_workflows", "get_workflows", "list_available_targets", "list_search_attributes", "search_workflows"},
			targets: []string{"payments-staging", "orders-prod"},
		},
		{
			name:    "string claim",
			token:   sign(map[string]any{"email": "alice@acme.example"}),
			tools:   []string{"check_connection", "list_available_targets"},
			targets: []string{"payments-prod"},
		},
		{
			name: "no matching rule, including claims of the wrong type",
			token: sign(map[string]any{
				"cognito:groups":             map[string]any{"temporal-oncall": true},
				"https://acme.example/roles": []any{[]any{"Temporal.Dev"}, map[string]any{"role": "Temporal.Dev"}},
				"email":                      true,
				"groups":                     []any{"temporal-oncall"},
			}),
			tools: []string{},
		},
		{
			name:  "tools without list_available_targets",
			token: sign(map[string]any{"https://acme.example/roles": []any{"Temporal.Orders"}}),
			tools: []string{"get_workflows"},
		},
		{
			name:  "value differing only in case",
			token: sign(map[string]any{"cognito:groups": []any{"Temporal-Oncall"}, "https://acme.example/roles": "temporal.dev", "scope": "openid temporal.read Temporal.Orders"}),
			tools: []string{},
		},
		{
			name:  "substring or superstring of a value",
			token: sign(map[string]any{"cognito:groups": []any{"temporal-oncall-2", "oncall"}, "https://acme.example/roles": "Temporal.Dev.Admin", "scope": "openid temporal.read temporal.orders.admin temporal"}),
			tools: []string{},
		},
		{
			name:  "scope string holding several scopes",
			token: sign(map[string]any{"scope": "openid temporal.read temporal.orders email"}),
			tools: []string{"get_workflows"},
		},
		{
			name:  "scp string holding several scopes",
			token: sign(map[string]any{"scp": "openid temporal.orders email"}),
			tools: []string{"get_workflows"},
		},
		{
			name:  "space-separated string in a claim other than scope",
			token: sign(map[string]any{"https://acme.example/roles": "Temporal.Admin Temporal.Dev"}),
			tools: []string{},
		},
		{
			name:    "profile without tools or tools_deny, even with allow_writes",
			token:   sign(map[string]any{"https://acme.example/roles": []any{"Temporal.All"}}),
			tools:   []string{"check_connection", "count_workflows", "get_task_queues", "get_workflow_history", "get_workflow_history_summary", "get_workflows", "list_available_targets", "list_search_attributes", "search_workflows"},
			targets: []string{"payments-staging"},
		},
		{
			name:    "profile with tools_deny",
			token:   sign(map[string]any{"https://acme.example/roles": []any{"Temporal.NoHistory"}}),
			tools:   []string{"check_connection", "count_workflows", "get_task_queues", "get_workflow_history_summary", "get_workflows", "list_available_targets", "list_search_attributes", "search_workflows"},
			targets: []string{"payments-staging"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := connectAs(t, tt.token)
			list, err := session.ListTools(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			tools := []string{}
			for _, tool := range list.Tools {
				tools = append(tools, tool.Name)
			}
			if !slices.Equal(tools, tt.tools) || list.CacheScope != "private" {
				t.Errorf("tools/list = %v with cacheScope %q, want %v with cacheScope private", tools, list.CacheScope, tt.tools)
			}
			if tt.targets == nil {
				unknownTool(t, session, "list_available_targets", nil)
				return
			}
			res, _ := call(t, session, "list_available_targets", nil)
			var out envelope[targetList]
			if err := json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &out); err != nil {
				t.Fatal(err)
			}
			var targets []string
			for _, target := range out.Result.Targets {
				targets = append(targets, target.Name)
			}
			if !slices.Equal(targets, tt.targets) {
				t.Errorf("list_available_targets = %v, want %v", targets, tt.targets)
			}
		})
	}

	t.Run("profiles combine per target", func(t *testing.T) {
		session := connectAs(t, tests[0].token)
		res, _ := call(t, session, "check_connection", map[string]any{"target": "pp"})
		if res == nil || res.IsError {
			t.Errorf("check_connection on pp = %+v, want a connection check", res)
		}
		notFound(t, session, "check_connection", map[string]any{"target": "payments-staging"})
		notFound(t, session, "get_workflows", map[string]any{"target": "payments-prod", "workflows": []any{map[string]any{"workflow_id": "wf"}}})
		notFound(t, session, "get_workflows", map[string]any{"target": "orders-prod", "workflows": []any{map[string]any{"workflow_id": "wf"}}})
		notFound(t, session, "search_workflows", map[string]any{"target": "payments-prod", "query": ""})
		notFound(t, session, "count_workflows", map[string]any{"target": "payments-prod", "query": ""})
		notFound(t, session, "list_search_attributes", map[string]any{"target": "payments-prod"})
	})

	t.Run("unknown field", func(t *testing.T) {
		for role, mayList := range map[string]bool{"Temporal.Dev": true, "Temporal.Search": false} {
			session := connectAs(t, sign(map[string]any{"https://acme.example/roles": []any{role}}))
			res, _ := call(t, session, "search_workflows", map[string]any{"target": "payments-staging", "fields": []any{"status", "customerId"}})
			got := errorContent(t, res)
			if msg, _ := got["message"].(string); got["code"] != "invalid_argument" || !strings.Contains(msg, `"status", "customerId"`) || strings.Contains(msg, "list_search_attributes") != mayList {
				t.Errorf("%s: error = %#v, want invalid_argument naming status and customerId, pointing to list_search_attributes: %v", role, got, mayList)
			}
		}
	})

	t.Run("tool the caller lacks", func(t *testing.T) {
		session := connectAs(t, tests[1].token)
		unknownTool(t, session, "check_connection", map[string]any{"target": "payments-staging"})
		unknownTool(t, session, "check_connection", map[string]any{"target": "payments-staging", "extra": true})
		unknownTool(t, connectAs(t, sign(map[string]any{"https://acme.example/roles": []any{"Temporal.NoHistory"}})), "get_workflow_history", map[string]any{"target": "payments-staging", "workflow_id": "wf"})
	})

	t.Run("invalid cursor", func(t *testing.T) {
		session := connectAs(t, tests[4].token)
		_, err := session.ListTools(t.Context(), &mcp.ListToolsParams{Cursor: "x"})
		var wire *jsonrpc.Error
		if !errors.As(err, &wire) || wire.Code != jsonrpc.CodeInvalidParams {
			t.Errorf("tools/list with an invalid cursor: err = %v, want invalid params", err)
		}
	})

	t.Run("reload", func(t *testing.T) {
		session := connectAs(t, tests[1].token)
		// The reload gives dev start_workflow for A on orders-prod in place of its read-only tools on payments-staging.
		next, err := config.Parse([]byte(strings.NewReplacer(
			"namespace: c}", "namespace: c, workflows: [{type: A, task_queue: q}, {type: B, task_queue: q}]}",
			"dev: {tools: [list_available_targets, get_workflows, search_workflows, count_workflows, list_search_attributes], targets: [payments-staging]}", "dev: {tools: [start_workflow], workflows: [A], targets: [orders-prod], allow_writes: true}",
		).Replace(yaml)), "")
		if err != nil {
			t.Fatal(err)
		}
		nextClients, err := temporal.NewClients(next, "http", "test", "0", slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Reload(next, nextClients, newRenderer(t, next, logger)); err != nil {
			t.Fatal(err)
		}
		list, err := session.ListTools(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		var tools []string
		for _, tool := range list.Tools {
			tools = append(tools, tool.Name)
		}
		res, _ := call(t, session, "list_startable_workflows", map[string]any{"target": "orders-prod"})
		var out envelope[startableWorkflowList]
		if err := json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &out); err != nil {
			t.Fatal(err)
		}
		if want := []string{"list_startable_workflows", "start_workflow"}; !slices.Equal(tools, want) || len(out.Result.Workflows) != 1 || out.Result.Workflows[0].Type != "A" {
			t.Errorf("after the reload, dev gets tools %v and startable workflows %+v; want %v and A", tools, out.Result.Workflows, want)
		}

		for _, tt := range []struct {
			name, optIn         string
			profile, wantWrites bool
			wantWarnings        int
		}{
			{"omitted", "", true, false, 2},
			{"false", "allow_writes_in_profiles: false\n", true, false, 2},
			{"disabled profiles", "", false, false, 1},
			{"enabled", "allow_writes_in_profiles: true\n", true, true, 0},
			{"profile false", "allow_writes_in_profiles: true\n", false, false, 0},
			{"disabled again", "allow_writes_in_profiles: false\n", true, false, 2},
		} {
			t.Run(tt.name, func(t *testing.T) {
				data := strings.NewReplacer(
					"allow_writes_in_profiles: true\n", tt.optIn,
					"namespace: c}", "namespace: c, workflows: [{type: A, task_queue: q}]}",
					"dev: {tools: [list_available_targets, get_workflows, search_workflows, count_workflows, list_search_attributes], targets: [payments-staging]}", fmt.Sprintf("dev: {tools: [get_workflows, start_workflow], workflows: [A], targets: [orders-prod], allow_writes: %t}", tt.profile),
				).Replace(yaml)
				cfg, err := config.Parse([]byte(data), "")
				if err != nil {
					t.Fatal(err)
				}
				clients, err := temporal.NewClients(cfg, "http", "test", "0", slog.New(slog.DiscardHandler))
				if err != nil {
					t.Fatal(err)
				}
				var warnings bytes.Buffer
				s.logger = slog.New(slog.NewJSONHandler(&warnings, nil))
				if err := s.Reload(cfg, clients, newRenderer(t, cfg, logger)); err != nil {
					t.Fatal(err)
				}
				lines := strings.Split(strings.TrimSpace(warnings.String()), "\n")
				if warnings.Len() == 0 {
					lines = nil
				}
				if len(lines) != tt.wantWarnings {
					t.Fatalf("warnings = %s, want %d warnings", warnings.String(), tt.wantWarnings)
				}
				for i, line := range lines {
					var record struct{ Level, Msg, Profile string }
					if err := json.Unmarshal([]byte(line), &record); err != nil {
						t.Fatal(err)
					}
					wantProfile := "all"
					if i == 1 {
						wantProfile = "dev"
					}
					if record.Level != "WARN" || record.Profile != wantProfile || !strings.Contains(record.Msg, "ignoring profile allow_writes because allow_writes_in_profiles is not true") {
						t.Errorf("warning = %s, want WARN naming profile %q and the ignored setting", line, wantProfile)
					}
				}
				list, err := session.ListTools(t.Context(), nil)
				if err != nil {
					t.Fatal(err)
				}
				var tools []string
				for _, tool := range list.Tools {
					tools = append(tools, tool.Name)
				}
				want := []string{"get_workflows"}
				if tt.wantWrites {
					want = append(want, "list_startable_workflows", "start_workflow")
				}
				if !slices.Equal(tools, want) {
					t.Errorf("tools/list = %v, want %v", tools, want)
				}
				for name, args := range map[string]map[string]any{
					"list_startable_workflows": {"target": "orders-prod"},
					"start_workflow":           {"target": "orders-prod", "workflow_id": "w", "workflow_type": "V"},
				} {
					_, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args})
					if tt.wantWrites != (err == nil) || err != nil && !strings.Contains(err.Error(), fmt.Sprintf("unknown tool %q", name)) {
						t.Errorf("calling %s: err = %v, want an unknown tool error: %t", name, err, !tt.wantWrites)
					}
				}
			})
		}
	})
}

type fakeOperatorService struct {
	operatorservice.UnimplementedOperatorServiceServer
}

func (fakeOperatorService) ListSearchAttributes(context.Context, *operatorservice.ListSearchAttributesRequest) (*operatorservice.ListSearchAttributesResponse, error) {
	return &operatorservice.ListSearchAttributesResponse{CustomAttributes: map[string]enums.IndexedValueType{"CustomerId": enums.INDEXED_VALUE_TYPE_KEYWORD}}, nil
}

func TestNoAuthAllowsWritesWithoutProfiles(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	load := func(allowWritesInProfiles bool) (*config.Config, *temporal.Clients) {
		t.Helper()
		cfg, err := config.Parse(fmt.Appendf(nil, "listen: 127.0.0.1:0\nallow_writes_in_profiles: %t\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default, workflows: [{type: W, task_queue: q}]}\n", allowWritesInProfiles), "")
		if err != nil {
			t.Fatal(err)
		}
		clients, err := temporal.NewClients(cfg, "http", "test", "0", slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		return cfg, clients
	}
	cfg, clients := load(false)
	s, err := NewServer(t.Context(), t.Context(), "temporal", "0", cfg, clients, newRenderer(t, cfg, logger), t.Output(), logger)
	if err != nil {
		t.Fatal(err)
	}
	session := connectTo(t, NewMux(s.Server, cfg, logger), http.DefaultTransport)
	readTools := []string{"check_connection", "count_workflows", "get_task_queues", "get_workflow_history", "get_workflow_history_summary", "get_workflows", "list_available_targets", "list_search_attributes", "search_workflows"}
	for i, allowWritesInProfiles := range []bool{false, true, false} {
		if i > 0 {
			next, nextClients := load(allowWritesInProfiles)
			if err := s.Reload(next, nextClients, newRenderer(t, next, logger)); err != nil {
				t.Fatal(err)
			}
		}
		list, err := session.ListTools(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		var tools []string
		for _, tool := range list.Tools {
			tools = append(tools, tool.Name)
		}
		want := slices.Sorted(slices.Values(append(slices.Clone(readTools), "list_startable_workflows", "start_workflow")))
		if !slices.Equal(tools, want) || list.CacheScope != "public" {
			t.Errorf("allow_writes_in_profiles %t: tools/list = %v with cacheScope %q, want %v with cacheScope public", allowWritesInProfiles, tools, list.CacheScope, want)
		}
		for name, args := range map[string]map[string]any{
			"list_startable_workflows": {"target": "a"},
			"start_workflow":           {"target": "a", "workflow_id": "w", "workflow_type": "V"},
		} {
			if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args}); err != nil {
				t.Errorf("allow_writes_in_profiles %t: calling %s: err = %v, want no protocol error", allowWritesInProfiles, name, err)
			}
		}
	}
}

func TestNewServerRequiresAuthorizationWithAuth(t *testing.T) {
	cfg := &config.Config{Auth: &config.Auth{Resource: "https://mcp.example.com/mcp", Issuer: "https://idp.example.com", Audience: "operator"}}
	if _, err := NewServer(t.Context(), t.Context(), "temporal", "0", cfg, nil, nil, io.Discard, slog.New(slog.DiscardHandler)); err == nil {
		t.Error("NewServer with auth and no authorization succeeded, want an error")
	}
}

func TestToolCannotReachUngrantedTarget(t *testing.T) {
	idp, sign := newTestIssuer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	ln.Close()
	cfg := &config.Config{
		AllowWritesInProfiles: true,
		Targets: []config.Target{
			{Name: "granted", Description: "x", Address: closed, Namespace: "a"},
			{Name: "ungranted", Description: "x", Address: closed, Namespace: "b"},
		},
		Auth: &config.Auth{Resource: "https://mcp.example.com/mcp", Issuer: idp, Audience: "operator"},
		Authorization: &config.Authorization{
			Profiles: map[string]config.Profile{"p": {AllowWrites: true, Tools: []string{"skips_lookup", "crosses_namespace", "empty_namespace"}, Targets: []string{"granted"}}},
			Rules:    []config.Rule{{ClaimName: "sub", ClaimValue: "alice", Profile: "p"}},
		},
	}
	clients, err := temporal.NewClients(cfg, "http", "test", "0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	s, _ := newServer(t.Context(), t.Context(), "temporal", "0", cfg, clients, newRenderer(t, cfg, logger), t.Output(), logger)
	calls := map[string]struct {
		rpc  string
		call func(context.Context) error
	}{
		"skips_lookup": {workflowservice.WorkflowService_GetSystemInfo_FullMethodName, func(ctx context.Context) error {
			_, err := clients.Client("ungranted").WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
			return err
		}},
		"crosses_namespace": {workflowservice.WorkflowService_DescribeWorkflowExecution_FullMethodName, func(ctx context.Context) error {
			_, err := clients.Client("granted").WorkflowService().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{Namespace: "b", Execution: &common.WorkflowExecution{WorkflowId: "w"}})
			return err
		}},
		"empty_namespace": {workflowservice.WorkflowService_DescribeNamespace_FullMethodName, func(ctx context.Context) error {
			_, err := clients.Client("granted").WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Id: "b-id"})
			return err
		}},
	}
	errs := make(chan error, 1)
	for name, c := range calls {
		addTool(s, &mcp.Tool{Name: name}, []string{c.rpc}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, struct{}, error) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			errs <- c.call(ctx)
			return nil, struct{}{}, nil
		})
	}
	srv := httptest.NewServer(NewMux(s.Server, cfg, logger))
	t.Cleanup(srv.Close)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: srv.URL + "/mcp", HTTPClient: &http.Client{Transport: bearer(sign(map[string]any{}))}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	for name := range calls {
		if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: name}); err != nil {
			t.Fatal(err)
		}
		if err := <-errs; serviceerror.ToStatus(err).Code() != codes.PermissionDenied {
			t.Errorf("%s: err = %v, want PermissionDenied", name, err)
		}
	}
}

func TestToolsListFailsClosedOnUnexpectedResult(t *testing.T) {
	cfg := &config.Config{
		Auth:          &config.Auth{Resource: "https://mcp.example.com/mcp", Issuer: "https://idp.example.com", Audience: "operator"},
		Authorization: &config.Authorization{},
	}
	for _, result := range []mcp.Result{&mcp.ListPromptsResult{}, (*mcp.ListToolsResult)(nil)} {
		next := func(context.Context, string, mcp.Request) (mcp.Result, error) {
			return result, nil
		}
		ctx := context.WithValue(t.Context(), snapshotKey{}, newSnapshot(cfg, nil, nil))
		res, err := authorize(nil)(next)(ctx, "tools/list", &mcp.ListToolsRequest{Params: &mcp.ListToolsParams{}})
		var wire *jsonrpc.Error
		if res != nil || !errors.As(err, &wire) || wire.Code != jsonrpc.CodeInternalError {
			t.Errorf("tools/list with next returning %#v = %#v, %v; want no result and an internal error", result, res, err)
		}
	}
}
