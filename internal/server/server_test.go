package server

import (
	"bytes"
	"context"
	"encoding/base64"
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

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/identity"
	"github.com/temporalio/operator-mcp/internal/payload"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

// connect writes the server's logs and audit records to out.
func connect(t *testing.T, configPath string, out io.Writer, extra ...func(*toolServer)) *mcp.ClientSession {
	t.Helper()
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return serveTools(t, cfg, clients, out, extra...)
}

// serveTools serves cfg's tools over clients, writing the server's logs and audit records to out, and returns a session
// connected to it.
func serveTools(t *testing.T, cfg *config.Config, clients *temporal.Clients, out io.Writer, extra ...func(*toolServer)) *mcp.ClientSession {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(out, nil))
	s, _ := newServer("temporal-operator", "0", cfg, clients, newRenderer(t, cfg, logger), out, logger)
	for _, add := range extra {
		add(s)
	}
	srv := httptest.NewServer(newHandler(s.Server, cfg))
	t.Cleanup(srv.Close)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: srv.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func newRenderer(t *testing.T, cfg *config.Config, logger *slog.Logger) *payload.Renderer {
	t.Helper()
	r, err := payload.NewRenderer(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	return r
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
					"name":             "prod",
					"aliases":          []any{"production", "prod-us"},
					"namespace":        "payments.abc12",
					"description":      "Production payments namespace.",
					"payload_handling": "omit",
				},
				map[string]any{
					"name":             "staging",
					"aliases":          []any{},
					"namespace":        "default",
					"description":      "Staging namespace on a local dev server.",
					"payload_handling": "passthrough",
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
	testTool := func(name string, err error) func(*toolServer) {
		return func(s *toolServer) {
			addTool(s, &mcp.Tool{Name: name}, nil, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, struct{}, error) {
				if err == nil {
					panic("boom-value")
				}
				return nil, struct{}{}, err
			})
		}
	}
	session := connect(t, filepath.Join("..", "..", "testdata", "config.yaml"), t.Output(),
		testTool("fail", errors.New("dial tcp 10.0.0.1:7233: connection refused")),
		testTool("wrapped", fmt.Errorf("lookup: %w", &toolError{Code: "not_found", Message: "no such thing"})),
		testTool("boom", nil))

	calls := []struct {
		name, args    string
		errorCode     string
		message       string
		protocolError bool
	}{
		{"check_connection", `{"target":"nope"}`, "not_found", `unknown target "nope"`, false},
		{"get_workflow", `{"target":"production","workflow_id":"wf","extra":"x"}`, "invalid_argument", "", false},
		{"check_connection", `{"target":"nope","TARGET":"other"}`, "invalid_argument", "", false},
		{"get_workflow", `["dev"]`, "invalid_argument", "arguments must be a JSON object", false},
		{"fail", ``, "internal", "internal error", false},
		{"wrapped", ``, "not_found", "no such thing", false},
		{"boom", ``, "internal", "internal error", false},
		{"no_such_tool", `{"target":"nope"}`, "", "", true},
		{"no_such_tool", `"dev"`, "", "", true},
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
}

func TestToolErrorHasResultType(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	s, err := NewServer("temporal-operator", "0", &config.Config{}, nil, nil, t.Output(), logger)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewMux(s, &config.Config{}, logger))
	t.Cleanup(srv.Close)
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"check_connection","arguments":{"target":"nope"},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Protocol-Version", "2026-07-28")
	req.Header.Set("Mcp-Method", "tools/call")
	req.Header.Set("Mcp-Name", "check_connection")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	_, data, _ := strings.Cut(strings.TrimSpace(string(body)), "data: ")
	var msg struct {
		Result struct {
			IsError    bool   `json:"isError"`
			ResultType string `json:"resultType"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		t.Fatalf("response %q: %v", body, err)
	}
	if !msg.Result.IsError || msg.Result.ResultType != "complete" {
		t.Errorf("response = %s; want a tool error with resultType \"complete\"", data)
	}
}

func TestNewMux(t *testing.T) {
	issuerURL, sign := newTestIssuer(t)
	token := sign(map[string]any{})
	claims, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	authCfg := &config.Config{
		Auth: &config.Auth{Resource: "https://mcp.example.com:8443/team/mcp/", Issuers: []config.Issuer{{Issuer: issuerURL, Audience: "operator"}}},
		Authorization: &config.Authorization{
			Profiles: map[string]config.Profile{"p": {Tools: []string{"whoami"}, Targets: []string{"prod"}}},
			Rules:    []config.Rule{{ClaimName: "sub", ClaimValue: "alice", Profile: "p"}},
		},
	}
	s, _ := newServer("temporal-operator", "0", authCfg, nil, nil, t.Output(), logger)
	callers := make(chan *identity.Caller, 1)
	addTool(s, &mcp.Tool{Name: "whoami"}, nil, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, struct{}, error) {
		callers <- identity.CallerFrom(ctx)
		return nil, struct{}{}, nil
	})
	withAuth := httptest.NewServer(NewMux(s.Server, authCfg, logger))
	t.Cleanup(withAuth.Close)
	noSlashCfg := *authCfg
	noSlashCfg.Auth = &config.Auth{Resource: "https://mcp.example.com:8443/team/mcp", Issuers: authCfg.Auth.Issuers}
	noSlash := httptest.NewServer(NewMux(s.Server, &noSlashCfg, logger))
	t.Cleanup(noSlash.Close)
	plain, err := NewServer("temporal-operator", "0", &config.Config{}, nil, nil, t.Output(), logger)
	if err != nil {
		t.Fatal(err)
	}
	withoutAuth := httptest.NewServer(NewMux(plain, &config.Config{}, logger))
	t.Cleanup(withoutAuth.Close)

	tests := []struct {
		name                      string
		srv                       *httptest.Server
		method, path, host, token string
		want                      int
		challenge                 string
	}{
		{"no token", withAuth, http.MethodPost, "/mcp", "", "", http.StatusUnauthorized, `Bearer resource_metadata="https://mcp.example.com:8443/.well-known/oauth-protected-resource/team/mcp/"`},
		{"metadata at the challenged path", withAuth, http.MethodGet, "/.well-known/oauth-protected-resource/team/mcp/", "", "", http.StatusOK, ""},
		{"metadata without the trailing slash", withAuth, http.MethodGet, "/.well-known/oauth-protected-resource/team/mcp", "", "", http.StatusOK, ""},
		{"no metadata with a slash the resource lacks", noSlash, http.MethodGet, "/.well-known/oauth-protected-resource/team/mcp/", "", "", http.StatusNotFound, ""},
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
	want := &identity.Caller{Issuer: issuerURL, Subject: "alice"}
	if err := json.Unmarshal(claims, &want.Claims); err != nil {
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

func TestPanicGetsInternalError(t *testing.T) {
	var logs bytes.Buffer
	session := connect(t, filepath.Join("..", "..", "testdata", "config.yaml"), &logs, func(s *toolServer) {
		s.AddPrompt(&mcp.Prompt{Name: "boom"}, func(context.Context, *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
			panic("boom-value")
		})
	})
	_, err := session.GetPrompt(t.Context(), &mcp.GetPromptParams{Name: "boom"})
	var wire *jsonrpc.Error
	if !errors.As(err, &wire) || wire.Code != jsonrpc.CodeInternalError {
		t.Errorf("prompts/get that panics: err = %v, want an internal error", err)
	}
	if _, err := session.ListTools(t.Context(), nil); err != nil {
		t.Errorf("tools/list after a panic: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(logs.Bytes(), &got); err != nil {
		t.Fatalf("log %q: %v", logs.String(), err)
	}
	if e, _ := got["error"].(string); !strings.HasPrefix(e, "panic: boom-value\ngoroutine ") {
		t.Errorf("log error = %q, want the panic and its stack", e)
	}
	delete(got, "time")
	delete(got, "error")
	if want := map[string]any{"level": "ERROR", "msg": "request panicked", "method": "prompts/get"}; !reflect.DeepEqual(got, want) {
		t.Errorf("log = %#v, want %#v", got, want)
	}
}
