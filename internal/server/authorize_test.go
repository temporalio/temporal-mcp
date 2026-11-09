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
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc/oidctest"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identitypb "go.temporal.io/cloud-sdk/api/identity/v1"
	namespacepb "go.temporal.io/cloud-sdk/api/namespace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	yaml := fmt.Sprintf(`listen: 127.0.0.1:0
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
    all: {targets: [payments-staging]}
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
	if err := os.WriteFile(configPath, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(configPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	s, err := NewServer(t.Context(), t.Context(), "temporal", "0", cfg, clients, newRenderer(t, cfg, logger), t.Output(), logger, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewMux(s, cfg, logger))
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
			name:    "profile without tools or tools_deny",
			token:   sign(map[string]any{"https://acme.example/roles": []any{"Temporal.All"}}),
			tools:   []string{"check_connection", "count_workflows", "get_task_queues", "get_workflow_history", "get_workflows", "list_available_targets", "list_search_attributes", "search_workflows"},
			targets: []string{"payments-staging"},
		},
		{
			name:    "profile with tools_deny",
			token:   sign(map[string]any{"https://acme.example/roles": []any{"Temporal.NoHistory"}}),
			tools:   []string{"check_connection", "count_workflows", "get_task_queues", "get_workflows", "list_available_targets", "list_search_attributes", "search_workflows"},
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
}

type fakeOperatorService struct {
	operatorservice.UnimplementedOperatorServiceServer
}

func (fakeOperatorService) ListSearchAttributes(context.Context, *operatorservice.ListSearchAttributesRequest) (*operatorservice.ListSearchAttributesResponse, error) {
	return &operatorservice.ListSearchAttributesResponse{CustomAttributes: map[string]enums.IndexedValueType{"CustomerId": enums.INDEXED_VALUE_TYPE_KEYWORD}}, nil
}

func TestNoAuthAllowsEveryTool(t *testing.T) {
	session := connect(t, filepath.Join("..", "..", "testdata", "config.yaml"), t.Output())
	list, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var tools []string
	for _, tool := range list.Tools {
		tools = append(tools, tool.Name)
	}
	if want := []string{"check_connection", "count_workflows", "get_task_queues", "get_workflow_history", "get_workflows", "list_available_targets", "list_search_attributes", "search_workflows"}; !slices.Equal(tools, want) {
		t.Errorf("tools/list = %v, want %v", tools, want)
	}
}

func TestNewServerRequiresAuthorizationWithAuth(t *testing.T) {
	cfg := &config.Config{Auth: &config.Auth{Resource: "https://mcp.example.com/mcp", Issuer: "https://idp.example.com", Audience: "operator"}}
	if _, err := NewServer(t.Context(), t.Context(), "temporal", "0", cfg, nil, nil, io.Discard, slog.New(slog.DiscardHandler), nil); err == nil {
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
		Targets: []config.Target{
			{Name: "granted", Description: "x", Address: closed, Namespace: "a"},
			{Name: "ungranted", Description: "x", Address: closed, Namespace: "b"},
		},
		Auth: &config.Auth{Resource: "https://mcp.example.com/mcp", Issuer: idp, Audience: "operator"},
		Authorization: &config.Authorization{
			Profiles: map[string]config.Profile{"p": {Tools: []string{"skips_lookup", "crosses_namespace", "empty_namespace"}, Targets: []string{"granted"}}},
			Rules:    []config.Rule{{ClaimName: "sub", ClaimValue: "alice", Profile: "p"}},
		},
	}
	clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler))
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

func TestProfileWithoutToolsGetsReadOnlyToolsOnly(t *testing.T) {
	a := &config.Authorization{Profiles: map[string]config.Profile{
		"all":  {Targets: []string{"t"}},
		"deny": {ToolsDeny: []string{"get_workflows"}, Targets: []string{"t"}},
	}}
	s, tools := newServer(t.Context(), t.Context(), "temporal", "0", &config.Config{Authorization: a}, nil, nil, io.Discard, slog.New(slog.DiscardHandler))
	noop := func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, struct{}, error) {
		return nil, struct{}{}, nil
	}
	addTool(s, &mcp.Tool{Name: "read_tool", Annotations: readOnlyAnnotations}, nil, noop)
	addTool(s, &mcp.Tool{Name: "write_tool"}, nil, noop)
	if err := resolveProfileTools(a, append(tools, "read_tool", "write_tool"), s.access); err != nil {
		t.Fatal(err)
	}
	for name, excluded := range map[string][]string{"all": {"write_tool"}, "deny": {"write_tool", "get_workflows"}} {
		got := a.Profiles[name].Tools
		if !slices.Contains(got, "read_tool") || slices.ContainsFunc(excluded, func(tool string) bool { return slices.Contains(got, tool) }) {
			t.Errorf("profile %q: tools = %v, want read_tool and none of %v", name, got, excluded)
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
		res, err := authorize(cfg, new(atomic.Pointer[config.Authorization]))(next)(t.Context(), "tools/list", &mcp.ListToolsRequest{Params: &mcp.ListToolsParams{}})
		var wire *jsonrpc.Error
		if res != nil || !errors.As(err, &wire) || wire.Code != jsonrpc.CodeInternalError {
			t.Errorf("tools/list with next returning %#v = %#v, %v; want no result and an internal error", result, res, err)
		}
	}
}

func TestCallKeepsItsGrants(t *testing.T) {
	cfg := &config.Config{Auth: &config.Auth{}, Targets: []config.Target{{Name: "a"}, {Name: "b"}}}
	var policy atomic.Pointer[config.Authorization]
	policy.Store(&config.Authorization{
		Profiles: map[string]config.Profile{
			"search": {Tools: []string{"search_workflows"}, Targets: []string{"a", "b"}},
			"list":   {Tools: []string{"list_search_attributes"}, Targets: []string{"a"}},
		},
		Rules: []config.Rule{{ClaimName: "groups", ClaimValue: "search", Profile: "search"}, {ClaimName: "groups", ClaimValue: "list", Profile: "list"}},
	})
	var mayList []bool
	var targets []string
	next := func(ctx context.Context, _ string, _ mcp.Request) (mcp.Result, error) {
		// A refresh that gives nothing to anyone lands mid-call.
		policy.Store(&config.Authorization{})
		mayList = []bool{mayUse(ctx, cfg, "list_search_attributes", "a"), mayUse(ctx, cfg, "list_search_attributes", "b")}
		targets = toolTargets(ctx)
		return nil, nil
	}
	req := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: "search_workflows"}, Extra: &mcp.RequestExtra{TokenInfo: &auth.TokenInfo{Extra: map[string]any{"groups": []any{"search", "list"}}}}}
	if _, err := authorize(cfg, &policy)(next)(t.Context(), "tools/call", req); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(mayList, []bool{true, false}) || !slices.Equal(targets, []string{"a", "b"}) {
		t.Errorf("may use list_search_attributes on a, b = %v and targets %v after a refresh mid-call, want [true false] and [a b]", mayList, targets)
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) messages(t *testing.T) []string {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var msgs []string
	for line := range strings.Lines(b.buf.String()) {
		var rec struct{ Msg string }
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		msgs = append(msgs, rec.Msg)
	}
	return msgs
}

// cloudNamespace returns the Temporal Cloud namespace name.acct1, which accepts API keys at address.
func cloudNamespace(name, address string) *namespacepb.Namespace {
	return &namespacepb.Namespace{
		Namespace: name + ".acct1",
		Spec:      &namespacepb.NamespaceSpec{Name: name, ApiKeyAuth: &namespacepb.ApiKeyAuthSpec{Enabled: true}},
		Endpoints: &namespacepb.Endpoints{GrpcAddress: address},
	}
}

// scimGroup returns the SCIM user group id, whose display name and idp_id are idpID, with role on the account and
// the given permissions on namespaces.
func scimGroup(id, idpID string, role identitypb.AccountAccess_Role, namespaces map[string]identitypb.NamespaceAccess_Permission) *identitypb.UserGroup {
	spec := &identitypb.UserGroupSpec{
		DisplayName: idpID,
		Access:      &identitypb.Access{AccountAccess: &identitypb.AccountAccess{Role: role}, NamespaceAccesses: map[string]*identitypb.NamespaceAccess{}},
		GroupType:   &identitypb.UserGroupSpec_ScimGroup{ScimGroup: &identitypb.SCIMGroupSpec{IdpId: idpID}},
	}
	for ns, p := range namespaces {
		spec.Access.NamespaceAccesses[ns] = &identitypb.NamespaceAccess{Permission: p}
	}
	return &identitypb.UserGroup{Id: id, Spec: spec}
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

// serveCloud serves fake as the Cloud Ops API and returns the config that config.Load reads from yaml with the
// namespaces fake lists, its clients, and a function that lists fake's user groups. It sets
// TEMPORAL_MCP_TEST_CLOUD_KEY, which yaml can read the API key from, to cloud-key.
func serveCloud(t *testing.T, fake *fakeCloudService, yaml string) (*config.Config, *temporal.Clients, config.ListUserGroupsFunc) {
	t.Helper()
	ln := listen(t)
	gs := grpc.NewServer()
	cloudservice.RegisterCloudServiceServer(gs, fake)
	go gs.Serve(ln)
	t.Cleanup(gs.Stop)
	t.Setenv("TEMPORAL_MCP_TEST_CLOUD_KEY", "cloud-key")
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cloudOps := temporal.WithPlaintextCloudOpsAPI(ln.Addr().String())
	cfg, err := config.Load(configPath, func(apiKey string) ([]*namespacepb.Namespace, error) {
		return temporal.ListNamespaces(t.Context(), apiKey, "test", "0", cloudOps)
	})
	if err != nil {
		t.Fatal(err)
	}
	clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return cfg, clients, func(apiKey string) ([]*identitypb.UserGroup, error) {
		return temporal.ListUserGroups(t.Context(), apiKey, "test", "0", cloudOps)
	}
}

func TestTemporalCloudGroups(t *testing.T) {
	idp, sign := newTestIssuer(t)
	closed := listen(t)
	closed.Close()
	address := closed.Addr().String()
	const (
		read  = identitypb.NamespaceAccess_PERMISSION_READ
		write = identitypb.NamespaceAccess_PERMISSION_WRITE
		admin = identitypb.NamespaceAccess_PERMISSION_ADMIN
	)
	developer := identitypb.AccountAccess_ROLE_DEVELOPER
	cloudGroup := scimGroup("g6", "Cloud Readers", identitypb.AccountAccess_ROLE_READ, map[string]identitypb.NamespaceAccess_Permission{"payments.acct1": read})
	cloudGroup.Spec.GroupType = &identitypb.UserGroupSpec_CloudGroup{CloudGroup: &identitypb.CloudGroupSpec{}}
	fake := &fakeCloudService{
		pages: [][]*namespacepb.Namespace{{cloudNamespace("payments", address), cloudNamespace("orders", address), cloudNamespace("billing", address), cloudNamespace("shipping", address), cloudNamespace("archive", address)}},
		// Listed out of ID order, which the report must not follow.
		groups: [][]*identitypb.UserGroup{{
			scimGroup("g4", "Temporal Admins", identitypb.AccountAccess_ROLE_ADMIN, nil),
			scimGroup("g1", "Payments Dev", developer, map[string]identitypb.NamespaceAccess_Permission{"payments.acct1": read, "orders.acct1": write, "legacy.acct1": read, "archive.acct1": read, "sandbox.acct1": admin}),
			scimGroup("g7", "Finance", identitypb.AccountAccess_ROLE_FINANCE_ADMIN, nil),
		}, {
			scimGroup("g3", "Temporal Owners", identitypb.AccountAccess_ROLE_OWNER, nil),
			cloudGroup,
			scimGroup("g2", "Shipping", developer, map[string]identitypb.NamespaceAccess_Permission{"shipping.acct1": admin}),
			scimGroup("g5", "Developers", developer, map[string]identitypb.NamespaceAccess_Permission{"legacy.acct1": read}),
		}},
	}
	yaml := fmt.Sprintf(`listen: 127.0.0.1:0
targets:
  - {name: ship, description: x, address: %[1]q, namespace: shipping.acct1}
  - {name: local, description: x, address: %[1]q, namespace: default}
temporal_cloud:
  api_key: {env: TEMPORAL_MCP_TEST_CLOUD_KEY}
  namespaces: {exclude: [archive]}
  groups: {claim_name: groups, tools_deny: [get_workflow_history]}
auth:
  resource: https://mcp.example.com/mcp
  issuer: %[2]q
  audience: operator
  required_scopes: [temporal.read]
authorization:
  profiles:
    oncall: {tools: [list_available_targets, check_connection], targets: [local]}
  rules:
    - {claim_name: roles, claim_value: oncall, profile: oncall}
`, closed.Addr().String(), idp)
	cfg, clients, listGroups := serveCloud(t, fake, yaml)
	setGroups := func(groups ...*identitypb.UserGroup) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		fake.groups = [][]*identitypb.UserGroup{groups}
	}
	setErr := func(err error) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		fake.err = err
	}
	groupCalls := func() int {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return fake.groupCalls
	}
	interval := groupRefreshInterval
	groupRefreshInterval = 10 * time.Millisecond
	t.Cleanup(func() { groupRefreshInterval = interval })

	setErr(status.Error(codes.Unavailable, "down"))
	if _, err := NewServer(t.Context(), t.Context(), "temporal", "0", cfg, clients, nil, io.Discard, slog.New(slog.DiscardHandler), listGroups); err == nil || !strings.Contains(err.Error(), "temporal_cloud: listing the account's user groups: ") {
		t.Errorf("NewServer with a failing listing: err = %v, want the listing's error", err)
	}
	setErr(nil)

	var logs lockedBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	s, err := NewServer(t.Context(), t.Context(), "temporal", "0", cfg, clients, newRenderer(t, cfg, logger), io.Discard, logger, listGroups)
	if err != nil {
		t.Fatal(err)
	}
	report := []string{
		"temporal_cloud: user groups listed by the API key: 7",
		`temporal_cloud: group g1 "Payments Dev": gives targets ["payments" "orders"] to callers whose groups claim has "Payments Dev"; its namespaces ["archive.acct1" "legacy.acct1" "sandbox.acct1"] have no target`,
		`temporal_cloud: group g2 "Shipping": gives targets ["ship"] to callers whose groups claim has "Shipping"`,
		`temporal_cloud: group g3 "Temporal Owners": gives targets ["ship" "payments" "orders" "billing"] to callers whose groups claim has "Temporal Owners"`,
		`temporal_cloud: group g4 "Temporal Admins": gives targets ["ship" "payments" "orders" "billing"] to callers whose groups claim has "Temporal Admins"`,
		`temporal_cloud: group g5 "Developers": gives no targets, because its namespaces ["legacy.acct1"] have no target`,
		`temporal_cloud: group g6 "Cloud Readers": gives no targets, because it is not a SCIM group`,
		`temporal_cloud: group g7 "Finance": gives no targets, because it has no access to any namespace`,
	}
	if got := logs.messages(t); !slices.Equal(got, report) {
		t.Errorf("log = %q\nwant %q", got, report)
	}
	srv := httptest.NewServer(NewMux(s, cfg, logger))
	t.Cleanup(srv.Close)
	sessions := map[string]*mcp.ClientSession{}
	session := func(t *testing.T, claims map[string]any) *mcp.ClientSession {
		t.Helper()
		token := sign(claims)
		if sessions[token] == nil {
			var err error
			sessions[token], err = mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: srv.URL + "/mcp", HTTPClient: &http.Client{Transport: bearer(token)}}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return sessions[token]
	}
	t.Cleanup(func() {
		for _, s := range sessions {
			s.Close()
		}
	})
	targets := func(t *testing.T, claims map[string]any) []string {
		t.Helper()
		return availableTargets(t, session(t, claims))
	}
	groups := func(names ...any) map[string]any { return map[string]any{"groups": names} }
	waitFor := func(t *testing.T, what string, done func() bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); !done(); time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting until %s", what)
			}
		}
	}

	for _, tt := range []struct {
		name    string
		claims  map[string]any
		targets []string
	}{
		{"one group", groups("Payments Dev"), []string{"payments", "orders"}},
		{"several groups", groups("Payments Dev", "Shipping"), []string{"ship", "payments", "orders"}},
		{"Owner role", groups("Temporal Owners"), []string{"ship", "payments", "orders", "billing"}},
		{"Global Admin role", groups("Temporal Admins"), []string{"ship", "payments", "orders", "billing"}},
		{"group whose namespaces have no target", groups("Developers"), nil},
		{"group that is not a SCIM group", groups("Cloud Readers"), nil},
		{"group name in another case", groups("payments dev"), nil},
		{"group and static rule", map[string]any{"groups": []any{"Payments Dev"}, "roles": []any{"oncall"}}, []string{"local", "payments", "orders"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := targets(t, tt.claims); !slices.Equal(got, tt.targets) {
				t.Errorf("list_available_targets = %v, want %v", got, tt.targets)
			}
		})
	}
	list, err := session(t, groups("Payments Dev")).ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var tools []string
	for _, tool := range list.Tools {
		tools = append(tools, tool.Name)
	}
	if want := []string{"check_connection", "count_workflows", "get_task_queues", "get_workflows", "list_available_targets", "list_search_attributes", "search_workflows"}; !slices.Equal(tools, want) {
		t.Errorf("tools/list = %v, want %v", tools, want)
	}

	calls := groupCalls()
	waitFor(t, "two more refreshes", func() bool { return groupCalls() >= calls+4 })
	if got := logs.messages(t); !slices.Equal(got, report) {
		t.Errorf("log after unchanged refreshes = %q\nwant %q", got, report)
	}

	setGroups(scimGroup("g1", "Payments Dev", developer, map[string]identitypb.NamespaceAccess_Permission{"orders.acct1": read}))
	waitFor(t, "a refresh revokes payments", func() bool { return slices.Equal(targets(t, groups("Payments Dev")), []string{"orders"}) })
	if got := targets(t, groups("Temporal Owners")); got != nil {
		t.Errorf("list_available_targets for a group no longer listed = %v, want none", got)
	}
	if got, want := logs.messages(t)[len(report):], []string{
		"temporal_cloud: user groups listed by the API key: 1",
		`temporal_cloud: group g1 "Payments Dev": gives targets ["orders"] to callers whose groups claim has "Payments Dev"`,
	}; !slices.Equal(got, want) {
		t.Errorf("log after the refresh = %q, want %q", got, want)
	}

	setGroups(scimGroup("g1", "Payments Dev", developer, map[string]identitypb.NamespaceAccess_Permission{"orders.acct1": read, "legacy.acct1": read}))
	gained := `temporal_cloud: group g1 "Payments Dev": gives targets ["orders"] to callers whose groups claim has "Payments Dev"; its namespaces ["legacy.acct1"] have no target`
	waitFor(t, "a refresh logs access to a namespace with no target", func() bool { return slices.Contains(logs.messages(t), gained) })

	setErr(status.Error(codes.Unavailable, "down"))
	calls = groupCalls()
	waitFor(t, "two failed refreshes", func() bool { return groupCalls() >= calls+2 })
	if got := targets(t, groups("Payments Dev")); !slices.Equal(got, []string{"orders"}) {
		t.Errorf("list_available_targets after a failed refresh = %v, want [orders]", got)
	}
	if got := logs.messages(t); !slices.Contains(got, "cannot refresh the access of temporal_cloud groups, so callers keep the access they have") {
		t.Errorf("log = %q, want the failed refresh", got)
	}
	setErr(nil)

	setGroups(scimGroup("g1", "Payments Engineering", developer, map[string]identitypb.NamespaceAccess_Permission{"orders.acct1": read}))
	waitFor(t, "a refresh renames the group", func() bool { return slices.Equal(targets(t, groups("Payments Engineering")), []string{"orders"}) })
	if got := targets(t, groups("Payments Dev")); got != nil {
		t.Errorf("list_available_targets for the old name = %v, want none", got)
	}
}

func TestTemporalCloudGroupsAlone(t *testing.T) {
	idp, sign := newTestIssuer(t)
	closed := listen(t)
	closed.Close()
	address := closed.Addr().String()
	fake := &fakeCloudService{
		pages: [][]*namespacepb.Namespace{{cloudNamespace("payments", address), cloudNamespace("orders", address), cloudNamespace("billing", address)}},
		groups: [][]*identitypb.UserGroup{{scimGroup("g1", "Payments Dev", identitypb.AccountAccess_ROLE_DEVELOPER, map[string]identitypb.NamespaceAccess_Permission{
			"payments.acct1": identitypb.NamespaceAccess_PERMISSION_READ,
			"orders.acct1":   identitypb.NamespaceAccess_PERMISSION_WRITE,
		})}},
	}
	yaml := fmt.Sprintf(`listen: 127.0.0.1:0
temporal_cloud:
  api_key: {env: TEMPORAL_MCP_TEST_CLOUD_KEY}
  groups: {claim_name: groups}
auth: {resource: https://mcp.example.com/mcp, issuer: %q, audience: operator, required_scopes: [temporal.read]}
`, idp)
	cfg, clients, listGroups := serveCloud(t, fake, yaml)
	logger := slog.New(slog.DiscardHandler)
	s, err := NewServer(t.Context(), t.Context(), "temporal", "0", cfg, clients, newRenderer(t, cfg, logger), io.Discard, logger, listGroups)
	if err != nil {
		t.Fatal(err)
	}
	mux := NewMux(s, cfg, logger)
	for _, tt := range []struct {
		name    string
		groups  []any
		targets []string
	}{
		{"member", []any{"Payments Dev"}, []string{"payments", "orders"}},
		{"non-member", []any{"Billing"}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := availableTargets(t, connectTo(t, mux, bearer(sign(map[string]any{"groups": tt.groups})))); !slices.Equal(got, tt.targets) {
				t.Errorf("list_available_targets = %v, want %v", got, tt.targets)
			}
		})
	}
}

func TestNewServerRejectsUnknownGroupTools(t *testing.T) {
	cfg := &config.Config{
		Auth:          &config.Auth{Resource: "https://mcp.example.com/mcp", Issuer: "https://idp.example.com", Audience: "operator"},
		TemporalCloud: &config.TemporalCloud{Groups: &config.CloudGroups{ClaimName: "groups", ToolsDeny: []string{"terminate_workflow"}}},
	}
	_, err := NewServer(t.Context(), t.Context(), "temporal", "0", cfg, nil, nil, io.Discard, slog.New(slog.DiscardHandler), nil)
	if want := `temporal_cloud: groups: tools_deny[0]: unknown tool "terminate_workflow"; the tools are list_available_targets`; err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err = %v, want %q", err, want)
	}
}
