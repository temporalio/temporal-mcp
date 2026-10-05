package temporal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/identity"
)

func closedAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestToolCallsAreLimitedToDeclaredRPCsAndGrantedTargets(t *testing.T) {
	clients, err := NewClients([]config.Target{{Name: "a", Address: closedAddress(t), Namespace: "default"}}, "test", "0")
	if err != nil {
		t.Fatal(err)
	}
	svc := clients.Client("a").WorkflowService()
	declared := []string{workflowservice.WorkflowService_GetSystemInfo_FullMethodName}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tests := []struct {
		name string
		ctx  context.Context
		want codes.Code
	}{
		{"declared", WithTool(ctx, "t", declared, []string{"a"}), codes.Unavailable},
		{"undeclared", WithTool(ctx, "t", []string{workflowservice.WorkflowService_DescribeNamespace_FullMethodName}, []string{"a"}), codes.PermissionDenied},
		{"another target", WithTool(ctx, "t", declared, []string{"b"}), codes.PermissionDenied},
		{"no tool", ctx, codes.PermissionDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.GetSystemInfo(tt.ctx, &workflowservice.GetSystemInfoRequest{})
			if got := serviceerror.ToStatus(err).Code(); got != tt.want {
				t.Errorf("code = %v, want %v (err %v)", got, tt.want, err)
			}
		})
	}
}

func TestNewClientsAPIKeyFromEnv(t *testing.T) {
	targets := []config.Target{{Name: "a", Address: "a:7233", Namespace: "a", APIKey: &config.APIKey{Env: "OPERATOR_MCP_TEST_API_KEY"}}}

	for _, value := range []string{"", "  \n"} {
		t.Run(fmt.Sprintf("%q", value), func(t *testing.T) {
			t.Setenv("OPERATOR_MCP_TEST_API_KEY", value)
			_, err := NewClients(targets, "test", "0")
			if want := "targets[0]: api_key: environment variable OPERATOR_MCP_TEST_API_KEY is unset or empty"; err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("err = %v, want it to contain %q", err, want)
			}
		})
	}

	t.Setenv("OPERATOR_MCP_TEST_API_KEY", "key")
	if _, err := NewClients(targets, "test", "0"); err != nil {
		t.Error(err)
	}

	t.Run("trimmed", func(t *testing.T) {
		t.Setenv("OPERATOR_MCP_TEST_API_KEY", "key\n")
		targets[0].Address = closedAddress(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ctx = WithTool(ctx, "t", []string{workflowservice.WorkflowService_GetSystemInfo_FullMethodName}, []string{"a"})
		_, err := newClient(t, targets[0]).WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
		if got := serviceerror.ToStatus(err).Code(); got != codes.Unavailable {
			t.Errorf("code = %v, want %v (err %v)", got, codes.Unavailable, err)
		}
	})
}

func TestAPIKeyCredential(t *testing.T) {
	t.Setenv("OPERATOR_MCP_TEST_API_KEY", " env-key\n")
	path := filepath.Join(t.TempDir(), "key")
	cred := func(k *config.APIKey) credential {
		c, err := apiKeyCredential(k)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	env, file := cred(&config.APIKey{Env: "OPERATOR_MCP_TEST_API_KEY"}), cred(&config.APIKey{File: path})
	for _, tt := range []struct {
		cred        credential
		write, want string
	}{
		{env, "", "env-key"},
		{file, "file-key-1\n", "file-key-1"},
		{file, "file-key-2\n", "file-key-2"},
	} {
		if tt.write != "" {
			if err := os.WriteFile(path, []byte(tt.write), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if got, err := tt.cred(t.Context(), nil, nil); err != nil || got != tt.want {
			t.Errorf("credential = %q, %v; want %q", got, err, tt.want)
		}
	}
}

func TestNewClientsRejectsUnparseableAddress(t *testing.T) {
	_, err := NewClients([]config.Target{{Name: "a", Address: "%zz.example:7233", Namespace: "default"}}, "test", "0")
	if want := "targets[0]: address:"; err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err = %v, want it to contain %q", err, want)
	}
}

func TestAPIKeyTargetsUseTLS(t *testing.T) {
	t.Setenv("OPERATOR_MCP_TEST_API_KEY", "key")
	tests := []struct {
		name   string
		apiKey *config.APIKey
		want   byte
	}{
		{"api key", &config.APIKey{Env: "OPERATOR_MCP_TEST_API_KEY"}, 0x16},
		{"no api key", nil, 'P'},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { ln.Close() })
			first := make(chan byte, 1)
			go func() {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				b := make([]byte, 1)
				if _, err := io.ReadFull(conn, b); err == nil {
					first <- b[0]
				}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ctx = WithTool(ctx, "t", []string{workflowservice.WorkflowService_GetSystemInfo_FullMethodName}, []string{"a"})
			target := config.Target{Name: "a", Address: ln.Addr().String(), Namespace: "default", APIKey: tt.apiKey}
			_, _ = newClient(t, target).WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})

			select {
			case got := <-first:
				if got != tt.want {
					t.Errorf("first byte = %#x, want %#x", got, tt.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("no bytes received")
			}
		})
	}
}

type call struct {
	method string
	md     metadata.MD
}

type fakeWorkflowService struct {
	workflowservice.UnimplementedWorkflowServiceServer
	mu    sync.Mutex
	calls []call
}

func (f *fakeWorkflowService) record(ctx context.Context, method string) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call{method, md})
}

func (f *fakeWorkflowService) GetSystemInfo(ctx context.Context, _ *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	f.record(ctx, "GetSystemInfo")
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (f *fakeWorkflowService) DescribeNamespace(ctx context.Context, _ *workflowservice.DescribeNamespaceRequest) (*workflowservice.DescribeNamespaceResponse, error) {
	f.record(ctx, "DescribeNamespace")
	return &workflowservice.DescribeNamespaceResponse{}, nil
}

func (f *fakeWorkflowService) DescribeWorkflowExecution(ctx context.Context, _ *workflowservice.DescribeWorkflowExecutionRequest) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.record(ctx, "DescribeWorkflowExecution")
	return &workflowservice.DescribeWorkflowExecutionResponse{}, nil
}

func serve(t *testing.T, svc workflowservice.WorkflowServiceServer) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(s, svc)
	go s.Serve(ln)
	t.Cleanup(s.Stop)
	return ln.Addr().String()
}

func newClient(t *testing.T, target config.Target) client.Client {
	t.Helper()
	clients, err := NewClients([]config.Target{target}, "operator-mcp", "1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	c := clients.Client(target.Name)
	t.Cleanup(c.Close)
	return c
}

func TestClientIdentityHeaders(t *testing.T) {
	fake := &fakeWorkflowService{}
	target := config.Target{Name: "a", Address: serve(t, fake), Namespace: "default"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = WithTool(ctx, "t", []string{
		workflowservice.WorkflowService_GetSystemInfo_FullMethodName,
		workflowservice.WorkflowService_DescribeNamespace_FullMethodName,
		workflowservice.WorkflowService_DescribeWorkflowExecution_FullMethodName,
	}, []string{"a"})

	raw := newClient(t, target).WorkflowService()
	if _, err := raw.GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: "default"}); err != nil {
		t.Fatal(err)
	}
	if _, err := newClient(t, target).DescribeWorkflowExecution(ctx, "wf", ""); err != nil {
		t.Fatal(err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	var methods []string
	for _, c := range fake.calls {
		methods = append(methods, c.method)
		if got, want := c.md.Get("client-name"), []string{"operator-mcp"}; !slices.Equal(got, want) {
			t.Errorf("%s client-name = %v, want %v", c.method, got, want)
		}
		if got, want := c.md.Get("client-version"), []string{"1.2.3"}; !slices.Equal(got, want) {
			t.Errorf("%s client-version = %v, want %v", c.method, got, want)
		}
		if ua := c.md.Get("user-agent"); len(ua) == 0 || !strings.HasPrefix(ua[0], "operator-mcp/1.2.3") {
			t.Errorf("%s user-agent = %v, want prefix operator-mcp/1.2.3", c.method, ua)
		}
	}
	slices.Sort(methods)
	if want := []string{"DescribeNamespace", "DescribeWorkflowExecution", "GetSystemInfo", "GetSystemInfo"}; !slices.Equal(methods, want) {
		t.Errorf("methods = %v, want %v", methods, want)
	}
}

func TestCredentialReceivesCallContext(t *testing.T) {
	target := config.Target{Name: "a", Address: closedAddress(t), Namespace: "default"}
	type seen struct {
		caller *identity.Caller
		target *config.Target
	}
	calls := make(chan seen, 1)
	c, err := lazyClient(target, "test", "0", func(_ context.Context, caller *identity.Caller, target *config.Target) (string, error) {
		calls <- seen{caller, target}
		return "", errors.New("no credential")
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	caller := &identity.Caller{Issuer: "https://idp.example", Subject: "alice"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = WithTool(identity.WithCaller(ctx, caller), "t", []string{workflowservice.WorkflowService_GetSystemInfo_FullMethodName}, []string{"a"})
	if _, err := c.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}); err == nil || !strings.Contains(err.Error(), "no credential") {
		t.Errorf("err = %v, want the credential's error", err)
	}
	select {
	case got := <-calls:
		if got.caller != caller || got.target.Name != "a" {
			t.Errorf("credential got caller %+v and target %+v, want caller %+v and target a", got.caller, got.target, caller)
		}
	default:
		t.Fatal("credential was not called")
	}
}
