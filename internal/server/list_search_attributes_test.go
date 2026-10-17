package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/cloud-sdk/api/cloudservice/v1"
	namespacepb "go.temporal.io/cloud-sdk/api/namespace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

type fakeCloudService struct {
	cloudservice.UnimplementedCloudServiceServer
	mu    sync.Mutex
	calls []string
	err   error
}

func (f *fakeCloudService) GetNamespace(ctx context.Context, req *cloudservice.GetNamespaceRequest) (*cloudservice.GetNamespaceResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req.GetNamespace()+" "+strings.Join(md.Get("authorization"), ","))
	if f.err != nil {
		return nil, f.err
	}
	return &cloudservice.GetNamespaceResponse{Namespace: &namespacepb.Namespace{Namespace: req.GetNamespace(), Spec: &namespacepb.NamespaceSpec{
		SearchAttributes: map[string]namespacepb.NamespaceSpec_SearchAttributeType{
			"OrderId":  namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD,
			"Note":     namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_TEXT,
			"Quantity": namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_INT,
			"Amount":   namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DOUBLE,
			"Gift":     namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_BOOL,
			"DueAt":    namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DATETIME,
			"Tags":     namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD_LIST,
		},
	}}}, nil
}

func TestListSearchAttributesOnCloud(t *testing.T) {
	fake := &fakeCloudService{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	cloudservice.RegisterCloudServiceServer(gs, fake)
	go gs.Serve(ln)
	t.Cleanup(gs.Stop)

	keyPath := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(keyPath, []byte("key-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certificate := &config.Certificate{CertFile: filepath.Join(dir, "tls.crt"), KeyFile: filepath.Join(dir, "tls.key")}
	_, cert, key := newCertificate(t, "client")
	writeCertificate(t, certificate, cert, key)
	cfg := &config.Config{Targets: []config.Target{
		{Name: "cloud", Description: "x", Address: "us-east-1.aws.api.temporal.io:7233", Namespace: "orders.acct1", APIKey: &config.Secret{File: keyPath}},
		{Name: "cloud-mtls", Description: "x", Address: "orders.acct1.tmprl.cloud:7233", Namespace: "orders.acct1", TLS: certificate},
	}}
	clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler), temporal.WithPlaintextCloudOpsAPI(ln.Addr().String()))
	if err != nil {
		t.Fatal(err)
	}
	var records bytes.Buffer
	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	s, _ := newServer("temporal-operator", "0", cfg, clients, newRenderer(t, cfg, logger), &records, logger)
	srv := httptest.NewServer(newHandler(s.Server, cfg))
	t.Cleanup(srv.Close)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: srv.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	validateOutput := outputValidator(t, session)

	list := func(t *testing.T, target string) (env map[string]any, custom, system map[string]any, temporalCalls float64) {
		t.Helper()
		records.Reset()
		res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_search_attributes", Arguments: map[string]any{"target": target}})
		if err != nil {
			t.Fatal(err)
		}
		if res.IsError {
			t.Fatalf("tool returned an error: %s", res.Content[0].(*mcp.TextContent).Text)
		}
		validateOutput(t, "list_search_attributes", res)
		env = res.StructuredContent.(map[string]any)
		custom, system = map[string]any{}, map[string]any{}
		for _, a := range env["result"].(map[string]any)["search_attributes"].([]any) {
			attr := a.(map[string]any)
			map[string]map[string]any{"custom": custom, "system": system}[attr["category"].(string)][attr["name"].(string)] = attr["type"]
		}
		var rec map[string]any
		if err := json.Unmarshal(records.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		return env, custom, system, rec["temporal_calls"].(float64)
	}
	wantSystem := map[string]any{}
	for name, typ := range cloudSystemSearchAttributes {
		wantSystem[name] = typ.String()
	}

	t.Run("API key", func(t *testing.T) {
		wantCustom := map[string]any{"OrderId": "Keyword", "Note": "Text", "Quantity": "Int", "Amount": "Double", "Gift": "Bool", "DueAt": "Datetime", "Tags": "KeywordList"}
		for _, key := range []string{"key-1", "key-2"} {
			if err := os.WriteFile(keyPath, []byte(key+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			env, custom, system, calls := list(t, "cloud")
			if env["complete"] != true || env["next_call"] != nil || calls != 1 {
				t.Errorf("complete = %v, next_call = %v, temporal_calls = %v; want true, null and 1", env["complete"], env["next_call"], calls)
			}
			if !reflect.DeepEqual(custom, wantCustom) || !reflect.DeepEqual(system, wantSystem) {
				t.Errorf("custom = %v, system = %v; want %v and %v", custom, system, wantCustom, wantSystem)
			}
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if want := []string{"orders.acct1 Bearer key-1", "orders.acct1 Bearer key-2"}; !reflect.DeepEqual(fake.calls, want) {
			t.Errorf("Cloud Ops API calls = %q, want %q", fake.calls, want)
		}
	})

	t.Run("without API key", func(t *testing.T) {
		fake.mu.Lock()
		fake.calls = nil
		fake.mu.Unlock()
		env, custom, system, calls := list(t, "cloud-mtls")
		if env["complete"] != false || len(custom) != 0 || !reflect.DeepEqual(system, wantSystem) || calls != 0 {
			t.Errorf("complete = %v, custom = %v, system = %v, temporal_calls = %v; want false, none, %v and 0", env["complete"], custom, system, calls, wantSystem)
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if len(fake.calls) != 0 {
			t.Errorf("Cloud Ops API calls = %q, want none", fake.calls)
		}
		if err := checkFields(t.Context(), nil, cfg, clients, &searchAttributeNames{}, &cfg.Targets[1], []string{"OrderId"}); err != nil {
			t.Errorf("checkFields = %v, want a field it cannot check to pass", err)
		}
	})

	t.Run("rate limited", func(t *testing.T) {
		fake.mu.Lock()
		fake.calls, fake.err = nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		fake.mu.Unlock()
		res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_search_attributes", Arguments: map[string]any{"target": "cloud"}})
		if err != nil {
			t.Fatal(err)
		}
		if got := errorContent(t, res); got["code"] != "rate_limited" || got["retryable"] != true {
			t.Errorf("error = %#v, want code rate_limited, retryable", got)
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if len(fake.calls) != 1 {
			t.Errorf("Cloud Ops API calls = %q, want one", fake.calls)
		}
	})

	t.Run("Cloud Ops API unreachable", func(t *testing.T) {
		closed := listen(t)
		closed.Close()
		unreachable, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler), temporal.WithPlaintextCloudOpsAPI(closed.Addr().String()))
		if err != nil {
			t.Fatal(err)
		}
		ctx := temporal.WithTool(context.WithValue(t.Context(), targetsKey{}, []string{"cloud"}), "list_search_attributes", []string{cloudservice.CloudService_GetNamespace_FullMethodName}, []string{"cloud"})
		_, _, err = listSearchAttributes(cfg, unreachable, &searchAttributeNames{})(ctx, nil, listSearchAttributesInput{Target: "cloud"})
		var got *toolError
		if !errors.As(err, &got) || got.Code != "temporal_unavailable" || !got.Retryable || got.Message != "could not connect to the Temporal Cloud Ops API at saas-api.tmprl.cloud" {
			t.Errorf("error = %v, want a retryable temporal_unavailable error naming the Cloud Ops API", err)
		}
	})
	t.Run("search_workflows fields", func(t *testing.T) {
		fake.mu.Lock()
		fake.calls, fake.err = nil, nil
		fake.mu.Unlock()
		records.Reset()
		// The call is rejected before the search, which would otherwise go to the target's Temporal Cloud address.
		res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "search_workflows", Arguments: map[string]any{"target": "cloud", "fields": []any{"OrderId", "TemporalNewBuiltIn", "orderId"}}})
		if err != nil {
			t.Fatal(err)
		}
		want := `these fields are not search attributes on target cloud: "orderId"; call list_search_attributes for the target's search attributes`
		if got := errorContent(t, res); got["code"] != "invalid_argument" || got["message"] != want {
			t.Errorf("error = %#v, want invalid_argument %q", got, want)
		}
		var rec map[string]any
		if err := json.Unmarshal(records.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if len(fake.calls) != 1 || !strings.HasPrefix(fake.calls[0], "orders.acct1 Bearer ") || rec["temporal_calls"] != 1.0 {
			t.Errorf("Cloud Ops API calls = %q, temporal_calls = %v; want one GetNamespace for orders.acct1 and no search", fake.calls, rec["temporal_calls"])
		}
	})
}
