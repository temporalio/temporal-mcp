package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
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
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc/oidctest"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
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
		claims["iss"], claims["sub"], claims["aud"], claims["exp"] = srv.URL, "alice", "operator", time.Now().Add(time.Hour).Unix()
		b, err := json.Marshal(claims)
		if err != nil {
			t.Fatal(err)
		}
		return oidctest.SignIDToken(key, "k1", "RS256", string(b))
	}
}

func TestAuthorization(t *testing.T) {
	idpA, signA := newTestIssuer(t)
	idpB, signB := newTestIssuer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	ln.Close()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	yaml := fmt.Sprintf(`listen: 127.0.0.1:0
targets:
  - {name: payments-prod, aliases: [pp], description: x, address: %[1]q, namespace: a}
  - {name: payments-staging, description: x, address: %[1]q, namespace: b}
  - {name: orders-prod, description: x, address: %[1]q, namespace: c}
auth:
  resource: https://mcp.example.com/mcp
  issuers: [{issuer: %[2]q, audience: operator}, {issuer: %[3]q, audience: operator}]
authorization:
  profiles:
    oncall: {tools: [list_available_targets, check_connection], targets: [payments-prod]}
    dev: {tools: [list_available_targets, get_workflow], targets: [payments-staging]}
    orders: {tools: [get_workflow], targets: [orders-prod]}
  rules:
    - {claim_name: "cognito:groups", claim_value: temporal-oncall, profile: oncall}
    - {claim_name: "https://acme.example/roles", claim_value: Temporal.Dev, profile: dev}
    - {claim_name: "https://acme.example/roles", claim_value: Temporal.Orders, profile: orders}
    - {claim_name: email, claim_value: alice@acme.example, issuer: %[2]q, profile: oncall}
    - {claim_name: scope, claim_value: temporal.orders, profile: orders}
    - {claim_name: scp, claim_value: temporal.orders, profile: orders}
`, closed, idpA, idpB)
	if err := os.WriteFile(configPath, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	clients, err := temporal.NewClients(cfg.Targets, "test", "0")
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewMux("temporal-operator", "0", cfg, clients, slog.New(slog.NewTextHandler(t.Output(), nil)))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
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
			token:   signA(map[string]any{"cognito:groups": []any{"other", "temporal-oncall"}, "https://acme.example/roles": []any{"Temporal.Dev"}}),
			tools:   []string{"check_connection", "get_workflow", "list_available_targets"},
			targets: []string{"payments-prod", "payments-staging"},
		},
		{
			name:    "one profile",
			token:   signB(map[string]any{"https://acme.example/roles": []any{"Temporal.Dev"}}),
			tools:   []string{"get_workflow", "list_available_targets"},
			targets: []string{"payments-staging"},
		},
		{
			name:    "target granted only through another tool",
			token:   signA(map[string]any{"https://acme.example/roles": []any{"Temporal.Dev", "Temporal.Orders"}}),
			tools:   []string{"get_workflow", "list_available_targets"},
			targets: []string{"payments-staging", "orders-prod"},
		},
		{
			name:    "issuer-scoped rule, matching issuer",
			token:   signA(map[string]any{"email": "alice@acme.example"}),
			tools:   []string{"check_connection", "list_available_targets"},
			targets: []string{"payments-prod"},
		},
		{
			name:  "issuer-scoped rule, other issuer",
			token: signB(map[string]any{"email": "alice@acme.example"}),
			tools: []string{},
		},
		{
			name: "no matching rule, including claims of the wrong type",
			token: signA(map[string]any{
				"cognito:groups":             map[string]any{"temporal-oncall": true},
				"https://acme.example/roles": []any{[]any{"Temporal.Dev"}, map[string]any{"role": "Temporal.Dev"}},
				"email":                      true,
				"groups":                     []any{"temporal-oncall"},
			}),
			tools: []string{},
		},
		{
			name:  "tools without list_available_targets",
			token: signA(map[string]any{"https://acme.example/roles": []any{"Temporal.Orders"}}),
			tools: []string{"get_workflow"},
		},
		{
			name:  "value differing only in case",
			token: signA(map[string]any{"cognito:groups": []any{"Temporal-Oncall"}, "https://acme.example/roles": "temporal.dev", "scope": "openid Temporal.Orders"}),
			tools: []string{},
		},
		{
			name:  "substring or superstring of a value",
			token: signA(map[string]any{"cognito:groups": []any{"temporal-oncall-2", "oncall"}, "https://acme.example/roles": "Temporal.Dev.Admin", "scope": "openid temporal.orders.admin temporal"}),
			tools: []string{},
		},
		{
			name:  "scope string holding several scopes",
			token: signA(map[string]any{"scope": "openid temporal.orders email"}),
			tools: []string{"get_workflow"},
		},
		{
			name:  "scp string holding several scopes",
			token: signA(map[string]any{"scp": "openid temporal.orders email"}),
			tools: []string{"get_workflow"},
		},
		{
			name:  "space-separated string in a claim other than scope",
			token: signA(map[string]any{"https://acme.example/roles": "Temporal.Admin Temporal.Dev"}),
			tools: []string{},
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
		notFound(t, session, "get_workflow", map[string]any{"target": "payments-prod", "workflow_id": "wf"})
		notFound(t, session, "get_workflow", map[string]any{"target": "orders-prod", "workflow_id": "wf"})
	})

	t.Run("tool the caller lacks", func(t *testing.T) {
		session := connectAs(t, tests[1].token)
		unknownTool(t, session, "check_connection", map[string]any{"target": "payments-staging"})
		unknownTool(t, session, "check_connection", map[string]any{"target": "payments-staging", "extra": true})
	})

	t.Run("invalid cursor", func(t *testing.T) {
		session := connectAs(t, tests[5].token)
		_, err := session.ListTools(t.Context(), &mcp.ListToolsParams{Cursor: "x"})
		var wire *jsonrpc.Error
		if !errors.As(err, &wire) || wire.Code != jsonrpc.CodeInvalidParams {
			t.Errorf("tools/list with an invalid cursor: err = %v, want invalid params", err)
		}
	})
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
	if want := []string{"check_connection", "get_workflow", "list_available_targets"}; !slices.Equal(tools, want) {
		t.Errorf("tools/list = %v, want %v", tools, want)
	}
}

func TestNewMuxRequiresAuthorizationWithAuth(t *testing.T) {
	cfg := &config.Config{Auth: &config.Auth{Resource: "https://mcp.example.com/mcp", Issuers: []config.Issuer{{Issuer: "https://idp.example.com", Audience: "operator"}}}}
	if _, err := NewMux("temporal-operator", "0", cfg, nil, slog.New(slog.DiscardHandler)); err == nil {
		t.Error("NewMux with auth and no authorization succeeded, want an error")
	}
}

func TestRuleWithoutIssuerWarnsWithSeveralIssuers(t *testing.T) {
	one := []config.Issuer{{Issuer: "https://a.example.com", Audience: "operator"}}
	two := []config.Issuer{one[0], {Issuer: "https://b.example.com", Audience: "operator"}}
	const warning = `level=WARN msg="rule has no issuer, so it matches tokens from every issuer in auth: issuers; set issuer to limit it" rule="authorization: rules[%d]" profile=p` + "\n"
	noTime := &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return a
	}}
	for _, tc := range []struct {
		name    string
		issuers []config.Issuer
		issuer  string
		want    string
	}{
		{"two issuers, rules without issuer", two, "", fmt.Sprintf(warning, 1) + fmt.Sprintf(warning, 2)},
		{"one issuer, rules without issuer", one, "", ""},
		{"two issuers, rules with issuer", two, "https://b.example.com", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				Auth: &config.Auth{Resource: "https://mcp.example.com/mcp", Issuers: tc.issuers},
				Authorization: &config.Authorization{
					Profiles: map[string]config.Profile{"p": {Tools: []string{"get_workflow"}, Targets: []string{"t"}}},
					Rules: []config.Rule{
						{ClaimName: "sub", ClaimValue: "alice", Issuer: "https://a.example.com", Profile: "p"},
						{ClaimName: "sub", ClaimValue: "bob", Issuer: tc.issuer, Profile: "p"},
						{ClaimName: "sub", ClaimValue: "carol", Issuer: tc.issuer, Profile: "p"},
					},
				},
			}
			var logs strings.Builder
			if _, err := NewMux("temporal-operator", "0", cfg, nil, slog.New(slog.NewTextHandler(&logs, noTime))); err != nil {
				t.Fatal(err)
			}
			if got := logs.String(); got != tc.want {
				t.Errorf("startup log = %q, want %q", got, tc.want)
			}
		})
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
		Auth: &config.Auth{Resource: "https://mcp.example.com/mcp", Issuers: []config.Issuer{{Issuer: idp, Audience: "operator"}}},
		Authorization: &config.Authorization{
			Profiles: map[string]config.Profile{"p": {Tools: []string{"skips_lookup"}, Targets: []string{"granted"}}},
			Rules:    []config.Rule{{ClaimName: "sub", ClaimValue: "alice", Profile: "p"}},
		},
	}
	clients, err := temporal.NewClients(cfg.Targets, "test", "0")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	s, _ := newServer("temporal-operator", "0", cfg, clients, logger)
	errs := make(chan error, 1)
	addTool(s, &mcp.Tool{Name: "skips_lookup"}, []string{workflowservice.WorkflowService_GetSystemInfo_FullMethodName}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, struct{}, error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := clients.Client("ungranted").WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
		errs <- err
		return nil, struct{}{}, nil
	})
	srv := httptest.NewServer(newMux(s, cfg, logger))
	t.Cleanup(srv.Close)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: srv.URL + "/mcp", HTTPClient: &http.Client{Transport: bearer(sign(map[string]any{}))}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "skips_lookup"}); err != nil {
		t.Fatal(err)
	}
	if err := <-errs; serviceerror.ToStatus(err).Code() != codes.PermissionDenied {
		t.Errorf("calling a target the tool was not granted: err = %v, want PermissionDenied", err)
	}
}
