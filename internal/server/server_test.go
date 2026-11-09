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
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	namespacepb "go.temporal.io/cloud-sdk/api/namespace/v1"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/identity"
	"github.com/temporalio/temporal-mcp/internal/payload"
	"github.com/temporalio/temporal-mcp/internal/temporal"
)

// connect writes the server's logs and audit records to out.
func connect(t *testing.T, configPath string, out io.Writer, extra ...func(*toolServer)) *mcp.ClientSession {
	t.Helper()
	cfg, err := config.Load(configPath, nil)
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
	s, _ := newServer(t.Context(), t.Context(), "temporal", "0", cfg, clients, newRenderer(t, cfg, logger), out, logger)
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

// connectTo serves h and returns a session connected to its /mcp endpoint, sending requests through transport.
func connectTo(t *testing.T, h http.Handler, transport http.RoundTripper) *mcp.ClientSession {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: srv.URL + "/mcp", HTTPClient: &http.Client{Transport: transport}}, nil)
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

func TestTemporalCloudTargets(t *testing.T) {
	closed := listen(t)
	closed.Close()
	address := closed.Addr().String()
	payments, legacy := cloudNamespace("payments", address), cloudNamespace("legacy", address)
	payments.Spec.Description = "Payments."
	legacy.Spec.ApiKeyAuth.Enabled = false
	fake := &fakeCloudService{pages: [][]*namespacepb.Namespace{{payments}, {cloudNamespace("orders", address), legacy}}}
	cfg, clients, _ := serveCloud(t, fake, "listen: 127.0.0.1:0\ntemporal_cloud:\n  api_key: {env: TEMPORAL_MCP_TEST_CLOUD_KEY}\n")
	fake.mu.Lock()
	if want := []string{" Bearer cloud-key", "1 Bearer cloud-key"}; !reflect.DeepEqual(fake.calls, want) {
		t.Errorf("GetNamespaces calls = %q, want %q", fake.calls, want)
	}
	if want := []bool{true, true}; !reflect.DeepEqual(fake.deadlines, want) {
		t.Errorf("GetNamespaces calls with a deadline within temporal.CallTimeout = %v, want %v", fake.deadlines, want)
	}
	fake.mu.Unlock()

	session := serveTools(t, cfg, clients, t.Output())
	res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_available_targets"})
	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		map[string]any{"name": "payments", "aliases": []any{}, "namespace": "payments.acct1", "description": "Payments.", "payload_handling": "omit"},
		map[string]any{"name": "orders", "aliases": []any{}, "namespace": "orders.acct1", "description": "Temporal Cloud namespace orders.acct1", "payload_handling": "omit"},
	}
	if got := res.StructuredContent.(map[string]any)["result"].(map[string]any)["targets"]; !reflect.DeepEqual(got, want) {
		t.Errorf("targets = %#v\nwant %#v", got, want)
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	CheckConnections(t.Context(), cfg, clients, newRenderer(t, cfg, logger), cfg.TemporalCloud.Added, logger)
	var failed []string
	for line := range strings.Lines(logs.String()) {
		var rec struct{ Msg, Target, Stage, Reason string }
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		failed = append(failed, rec.Msg+": "+rec.Target+" "+rec.Stage+": "+rec.Reason)
	}
	if want := []string{"connection check failed: payments server: Unavailable: could not connect to the Temporal server", "connection check failed: orders server: Unavailable: could not connect to the Temporal server"}; !reflect.DeepEqual(failed, want) {
		t.Errorf("log = %q, want %q", failed, want)
	}
	if strings.Contains(logs.String(), "cloud-key") {
		t.Errorf("log contains the API key:\n%s", &logs)
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
		{"get_workflows", `["dev"]`, "invalid_argument", "arguments must be a JSON object", false},
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
	s, err := NewServer(t.Context(), t.Context(), "temporal", "0", &config.Config{}, nil, nil, t.Output(), logger, nil)
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
		Auth: &config.Auth{Resource: "https://mcp.example.com:8443/team/mcp/", Issuer: issuerURL, Audience: "operator", RequiredScopes: []string{"temporal.read"}},
		Authorization: &config.Authorization{
			Profiles: map[string]config.Profile{"p": {Tools: []string{"whoami"}, Targets: []string{"prod"}}},
			Rules:    []config.Rule{{ClaimName: "sub", ClaimValue: "alice", Profile: "p"}},
		},
	}
	s, _ := newServer(t.Context(), t.Context(), "temporal", "0", authCfg, nil, nil, t.Output(), logger)
	callers := make(chan *identity.Caller, 1)
	addTool(s, &mcp.Tool{Name: "whoami"}, nil, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, struct{}, error) {
		callers <- identity.CallerFrom(ctx)
		return nil, struct{}{}, nil
	})
	withAuth := httptest.NewServer(NewMux(s.Server, authCfg, logger))
	t.Cleanup(withAuth.Close)
	noSlashCfg := *authCfg
	noSlashCfg.Auth = &config.Auth{Resource: "https://mcp.example.com:8443/team/mcp", Issuer: issuerURL, Audience: "operator"}
	noSlash := httptest.NewServer(NewMux(s.Server, &noSlashCfg, logger))
	t.Cleanup(noSlash.Close)
	plain, err := NewServer(t.Context(), t.Context(), "temporal", "0", &config.Config{}, nil, nil, t.Output(), logger, nil)
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
		{"no token", withAuth, http.MethodPost, "/mcp", "", "", http.StatusUnauthorized, `Bearer resource_metadata="https://mcp.example.com:8443/.well-known/oauth-protected-resource/team/mcp/", scope="temporal.read"`},
		{"token matching a rule without a required scope", withAuth, http.MethodPost, "/mcp", "", sign(map[string]any{"scope": "openid"}), http.StatusForbidden, `Bearer resource_metadata="https://mcp.example.com:8443/.well-known/oauth-protected-resource/team/mcp/", scope="temporal.read", error="insufficient_scope"`},
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

// memoWorkflowService describes each workflow as a running run with a JSON memo.
type memoWorkflowService struct{ fakeWorkflowService }

func (memoWorkflowService) DescribeWorkflowExecution(_ context.Context, req *workflowservice.DescribeWorkflowExecutionRequest) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	memo := map[string]*commonpb.Payload{"note": {Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(`"gift"`)}}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Execution: req.GetExecution(),
		Memo:      &commonpb.Memo{Fields: memo},
	}}, nil
}

func TestCodecPassthroughHeaders(t *testing.T) {
	received := make(chan http.Header, 10)
	codecServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Clone()
		body, _ := io.ReadAll(r.Body)
		// check_connection's check sends no payloads; failing it puts a codec failure in the log.
		if string(body) == `{"payloads":[]}` {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Write(body)
	}))
	t.Cleanup(codecServer.Close)
	temporalServer := listen(t)
	serve(t, temporalServer, memoWorkflowService{})
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, fmt.Appendf(nil, "listen: 127.0.0.1:0\n"+
		"targets:\n  - {name: orders, description: x, address: %q, namespace: orders, payload_handling: decode, codec: orders}\n"+
		"codecs:\n  orders:\n    endpoint: %q\n    passthrough_headers:\n"+
		"      - {name: x-tenant-id}\n"+
		"      - {name: X-Codec-Authorization, send_as: Authorization}\n"+
		"      - {name: X-Roles}\n"+
		"      - {name: X-Region}\n", temporalServer.Addr().String(), codecServer.URL), 0o600); err != nil {
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
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	s, _ := newServer(t.Context(), t.Context(), "temporal", "0", cfg, clients, newRenderer(t, cfg, logger), &out, logger)
	sent := http.Header{
		"X-Tenant-Id":           {"tenant-7f3a"},
		"X-Codec-Authorization": {"Bearer caller-codec-token"},
		"X-Roles":               {"role-a", "role-b"},
		"X-Unlisted":            {"unlisted-value"},
	}
	session := connectTo(t, NewMux(s.Server, cfg, logger), roundTripFunc(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		for name, values := range sent {
			r.Header[name] = values
		}
		return http.DefaultTransport.RoundTrip(r)
	}))

	var results []string
	for _, tt := range []struct {
		params *mcp.CallToolParams
		want   string
	}{
		{&mcp.CallToolParams{Name: "check_connection", Arguments: map[string]any{"target": "orders"}}, `{"name":"codec","status":"failed","latency_ms":`},
		{&mcp.CallToolParams{Name: "get_workflows", Arguments: map[string]any{"target": "orders", "workflows": []any{map[string]any{"workflow_id": "order-1"}}}}, `"memo":{"note":{"handling":"decoded","value":"gift"}}`},
	} {
		t.Run(tt.params.Name, func(t *testing.T) {
			res, err := session.CallTool(t.Context(), tt.params)
			if err != nil {
				t.Fatal(err)
			}
			text := res.Content[0].(*mcp.TextContent).Text
			results = append(results, text)
			if res.IsError || !strings.Contains(text, tt.want) {
				t.Fatalf("result %s, want one containing %s", text, tt.want)
			}
			h := <-received
			for name, want := range map[string][]string{
				"X-Tenant-Id":   {"tenant-7f3a"},
				"Authorization": {"Bearer caller-codec-token"},
				"X-Roles":       {"role-a", "role-b"},
				"X-Namespace":   {"orders"},
			} {
				if got := h.Values(name); !slices.Equal(got, want) {
					t.Errorf("codec request header %s = %q, want %q", name, got, want)
				}
			}
			for _, name := range []string{"X-Codec-Authorization", "X-Unlisted", "X-Region"} {
				if got := h.Values(name); got != nil {
					t.Errorf("codec request header %s = %q, want none", name, got)
				}
			}
		})
	}
	if !strings.Contains(out.String(), `"msg":"codec check failed"`) {
		t.Errorf("log and audit records %s, want the codec check failure", &out)
	}
	for _, values := range sent {
		for _, v := range values {
			if leaked := slices.ContainsFunc(append(results, out.String()), func(s string) bool { return strings.Contains(s, v) }); leaked {
				t.Errorf("a result, log line or audit record contains the header value %q", v)
			}
		}
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
