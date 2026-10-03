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
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc/oidctest"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/identity"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

func connect(t *testing.T, configPath string, logs io.Writer, extra ...func(*mcp.Server)) *mcp.ClientSession {
	t.Helper()
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	clients, err := temporal.NewClients(cfg.Targets, "test", "0")
	if err != nil {
		t.Fatal(err)
	}
	s := newServer("temporal-operator", "0", cfg, clients, slog.New(slog.NewJSONHandler(logs, nil)))
	for _, add := range extra {
		add(s)
	}
	srv := httptest.NewServer(newHandler(s, cfg))
	t.Cleanup(srv.Close)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: srv.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func outputValidator(t *testing.T, session *mcp.ClientSession) func(*testing.T, string, *mcp.CallToolResult) {
	t.Helper()
	tools, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	schemas := map[string]*jsonschema.Resolved{}
	for _, tool := range tools.Tools {
		b, err := json.Marshal(tool.OutputSchema)
		if err != nil {
			t.Fatal(err)
		}
		var s jsonschema.Schema
		if err := json.Unmarshal(b, &s); err != nil {
			t.Fatal(err)
		}
		if schemas[tool.Name], err = s.Resolve(nil); err != nil {
			t.Fatalf("%s output schema: %v", tool.Name, err)
		}
	}
	return func(t *testing.T, name string, res *mcp.CallToolResult) {
		t.Helper()
		if err := schemas[name].Validate(res.StructuredContent); err != nil {
			t.Errorf("%s structuredContent does not match its output schema: %v", name, err)
		}
	}
}

func errorContent(t *testing.T, res *mcp.CallToolResult) map[string]any {
	t.Helper()
	if !res.IsError || res.StructuredContent != nil {
		t.Fatalf("isError = %v, structuredContent = %#v; want a tool error without structuredContent", res.IsError, res.StructuredContent)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &got); err != nil {
		t.Fatalf("error content is not JSON: %v", err)
	}
	return got
}

func TestListAvailableTargets(t *testing.T) {
	session := connect(t, filepath.Join("..", "..", "testdata", "config.yaml"), t.Output())
	res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_available_targets"})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("tool returned an error: %+v", res.Content)
	}
	outputValidator(t, session)(t, "list_available_targets", res)
	want := map[string]any{
		"target": nil,
		"result": map[string]any{
			"targets": []any{
				map[string]any{
					"name":        "prod",
					"aliases":     []any{"production", "prod-us"},
					"namespace":   "payments.abc12",
					"description": "Production payments namespace.",
				},
				map[string]any{
					"name":        "staging",
					"aliases":     []any{},
					"namespace":   "default",
					"description": "Staging namespace on a local dev server.",
				},
			},
		},
		"complete":  true,
		"next_call": nil,
	}
	if !reflect.DeepEqual(res.StructuredContent, want) {
		t.Errorf("structuredContent = %#v\nwant %#v", res.StructuredContent, want)
	}
}

func TestToolCalls(t *testing.T) {
	var logs bytes.Buffer
	testTool := func(name string, err error) func(*mcp.Server) {
		return func(s *mcp.Server) {
			addTool(s, &mcp.Tool{Name: name}, nil, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, struct{}, error) {
				if err == nil {
					panic("boom-value")
				}
				return nil, struct{}{}, err
			})
		}
	}
	session := connect(t, filepath.Join("..", "..", "testdata", "config.yaml"), &logs,
		testTool("fail", errors.New("dial tcp 10.0.0.1:7233: connection refused")),
		testTool("wrapped", fmt.Errorf("lookup: %w", &toolError{Code: "not_found", Message: "no such thing"})),
		testTool("boom", nil))

	calls := []struct {
		name, args    string
		level, target string
		errorCode     string
		message       string
		logError      string
		protocolError bool
	}{
		{"check_connection", `{"target":"nope"}`, "INFO", "nope", "not_found", `unknown target "nope"; list_available_targets lists the targets`, "", false},
		{"get_workflow", `{"target":"production","workflow_id":"wf-secret-1","extra":"arg-secret-2"}`, "INFO", "production", "invalid_argument", "", "", false},
		{"check_connection", `{"target":"nope","TARGET":"tgt-secret-4"}`, "INFO", "nope", "invalid_argument", "", "", false},
		{"get_workflow", `["dev"]`, "INFO", "", "invalid_argument", "arguments must be a JSON object", "", false},
		{"fail", ``, "ERROR", "", "internal", "internal error", "dial tcp 10.0.0.1:7233: connection refused", false},
		{"wrapped", ``, "INFO", "", "not_found", "no such thing", "", false},
		{"boom", ``, "ERROR", "", "internal", "internal error", "panic: boom-value\ngoroutine ", false},
		{"no_such_tool", `{"target":"nope"}`, "WARN", "nope", "", "", "", true},
		{"no_such_tool", `"dev"`, "WARN", "", "", "", "", true},
	}
	for _, c := range calls {
		params := &mcp.CallToolParams{Name: c.name}
		if c.args != "" {
			params.Arguments = json.RawMessage(c.args)
		}
		res, err := session.CallTool(t.Context(), params)
		if c.protocolError {
			if err == nil {
				t.Errorf("%s %s: result %+v; want a protocol error", c.name, c.args, res)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s %s: CallTool error = %v", c.name, c.args, err)
		}
		got := errorContent(t, res)
		want := map[string]any{"code": c.errorCode, "message": c.message, "retryable": false}
		if c.message == "" {
			want["message"] = got["message"]
		}
		if msg, _ := got["message"].(string); msg == "" || !reflect.DeepEqual(got, want) {
			t.Errorf("%s %s: error = %#v, want %#v", c.name, c.args, got, want)
		}
	}

	if strings.Contains(logs.String(), "secret") {
		t.Errorf("log contains a tool argument value other than the target:\n%s", logs.String())
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != len(calls) {
		t.Fatalf("got %d log lines, want one per tool call:\n%s", len(lines), logs.String())
	}
	for i, c := range calls {
		var got map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &got); err != nil {
			t.Fatal(err)
		}
		if _, ok := got["duration"].(float64); !ok {
			t.Errorf("line %d duration = %#v, want a number", i, got["duration"])
		}
		if e, _ := got["error"].(string); !strings.HasPrefix(e, c.logError) || (c.logError == "") != (got["error"] == nil) {
			t.Errorf("line %d error = %#v, want prefix %q", i, got["error"], c.logError)
		}
		for _, k := range []string{"time", "duration", "error"} {
			delete(got, k)
		}
		want := map[string]any{"level": c.level, "msg": "tool call", "tool": c.name, "target": c.target, "error_code": c.errorCode}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("line %d = %#v, want %#v", i, got, want)
		}
	}
}

func TestNewMux(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	idp := &oidctest.Server{PublicKeys: []oidctest.PublicKey{{PublicKey: &key.PublicKey, KeyID: "k1", Algorithm: "RS256"}}}
	issuer := httptest.NewUnstartedServer(idp)
	idp.SetIssuer("http://" + issuer.Listener.Addr().String())
	issuer.Start()
	t.Cleanup(issuer.Close)
	claims := fmt.Sprintf(`{"iss":%q,"sub":"alice","aud":"operator","exp":%d}`, issuer.URL, time.Now().Add(time.Hour).Unix())
	token := oidctest.SignIDToken(key, "k1", "RS256", claims)

	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	authCfg := &config.Config{Auth: &config.Auth{Resource: "https://mcp.example.com:8443/team/mcp/", Issuers: []config.Issuer{{Issuer: issuer.URL, Audience: "operator"}}}}
	s := newServer("temporal-operator", "0", authCfg, nil, logger)
	callers := make(chan *identity.Caller, 1)
	addTool(s, &mcp.Tool{Name: "whoami"}, nil, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, struct{}, error) {
		callers <- identity.CallerFrom(ctx)
		return nil, struct{}{}, nil
	})
	withAuth := httptest.NewServer(newMux(s, authCfg, logger))
	t.Cleanup(withAuth.Close)
	withoutAuth := httptest.NewServer(NewMux("temporal-operator", "0", &config.Config{}, nil, logger))
	t.Cleanup(withoutAuth.Close)

	tests := []struct {
		name                      string
		srv                       *httptest.Server
		method, path, host, token string
		want                      int
		challenge                 string
	}{
		{"no token", withAuth, http.MethodPost, "/mcp", "", "", http.StatusUnauthorized, `Bearer resource_metadata="https://mcp.example.com:8443/.well-known/oauth-protected-resource/team/mcp"`},
		{"metadata at the challenged path", withAuth, http.MethodGet, "/.well-known/oauth-protected-resource/team/mcp", "", "", http.StatusOK, ""},
		{"token under a public host", withAuth, http.MethodPost, "/mcp", "mcp.example.com", token, http.StatusOK, ""},
		{"no auth under a public host", withoutAuth, http.MethodPost, "/mcp", "mcp.example.com", "", http.StatusForbidden, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, tt.srv.URL+tt.path, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Host = tt.host
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if got := resp.Header.Get("WWW-Authenticate"); resp.StatusCode != tt.want || got != tt.challenge {
				t.Errorf("status = %d, WWW-Authenticate = %q; want %d and %q", resp.StatusCode, got, tt.want, tt.challenge)
			}
		})
	}

	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: withAuth.URL + "/mcp", HTTPClient: &http.Client{Transport: bearer(token)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "whoami"}); err != nil {
		t.Fatal(err)
	}
	want := &identity.Caller{Issuer: issuer.URL, Subject: "alice"}
	if err := json.Unmarshal([]byte(claims), &want.Claims); err != nil {
		t.Fatal(err)
	}
	if got := <-callers; !reflect.DeepEqual(got, want) {
		t.Errorf("caller = %+v, want %+v", got, want)
	}
}

type bearer string

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+string(b))
	return http.DefaultTransport.RoundTrip(r)
}
