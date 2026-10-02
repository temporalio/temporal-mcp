package server

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

type fakeWorkflowService struct {
	workflowservice.UnimplementedWorkflowServiceServer
	describeNamespaceErr error
}

func (fakeWorkflowService) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (f fakeWorkflowService) DescribeNamespace(context.Context, *workflowservice.DescribeNamespaceRequest) (*workflowservice.DescribeNamespaceResponse, error) {
	return nil, f.describeNamespaceErr
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return ln
}

func serve(t *testing.T, ln net.Listener, svc workflowservice.WorkflowServiceServer) {
	t.Helper()
	s := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(s, svc)
	go s.Serve(ln)
	t.Cleanup(s.Stop)
}

func reason(s stage) string {
	if s.Reason == nil {
		return ""
	}
	return *s.Reason
}

func TestCheckConnectionFailureReasons(t *testing.T) {
	silent := listen(t)
	go func() {
		for {
			conn, err := silent.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
		}
	}()

	tlsMismatch := listen(t)
	serve(t, tlsMismatch, fakeWorkflowService{})

	unavailable := listen(t)
	serve(t, unavailable, fakeWorkflowService{describeNamespaceErr: status.Error(codes.Unavailable, "namespace ns1 is unavailable on "+unavailable.Addr().String())})

	t.Setenv("OPERATOR_MCP_TEST_API_KEY", "key")
	tests := []struct {
		name                  string
		target                config.Target
		timeout               time.Duration
		wantServer            string
		wantNamespacePrefix   string
		wantNamespaceContains string
	}{
		{
			name:       "TLS handshake fails",
			target:     config.Target{Address: tlsMismatch.Addr().String(), APIKey: &config.APIKey{Env: "OPERATOR_MCP_TEST_API_KEY"}},
			timeout:    time.Minute,
			wantServer: "Unavailable: the TLS handshake or certificate check with the Temporal server failed",
		},
		{
			name:                  "server reports unavailable",
			target:                config.Target{Address: unavailable.Addr().String()},
			timeout:               time.Minute,
			wantNamespacePrefix:   "Unavailable: the Temporal server reported: namespace ns1 is unavailable on ",
			wantNamespaceContains: "<address>",
		},
		{
			name:       "no response",
			target:     config.Target{Address: silent.Addr().String()},
			timeout:    time.Second,
			wantServer: "DeadlineExceeded: timed out waiting for the Temporal server",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.target.Name, tt.target.Description, tt.target.Namespace = "a", "x", "default"
			cfg := &config.Config{Targets: []config.Target{tt.target}}
			clients, err := temporal.NewClients(cfg.Targets, "test", "0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()
			ctx = temporal.WithTool(ctx, "check_connection", []string{
				workflowservice.WorkflowService_GetSystemInfo_FullMethodName,
				workflowservice.WorkflowService_DescribeNamespace_FullMethodName,
			})

			_, env, err := checkConnection(cfg, clients)(ctx, nil, checkConnectionInput{Target: "a"})
			if err != nil {
				t.Fatal(err)
			}

			server, namespace := env.Result.Stages[0], env.Result.Stages[1]
			if tt.wantServer != "" {
				if got := reason(server); server.Status != "failed" || got != tt.wantServer {
					t.Errorf("server stage = %s %q, want failed %q", server.Status, got, tt.wantServer)
				}
			} else if server.Status != "ok" {
				t.Errorf("server stage = %s %q, want ok", server.Status, reason(server))
			}
			if tt.wantNamespacePrefix != "" {
				if got := reason(namespace); namespace.Status != "failed" || !strings.HasPrefix(got, tt.wantNamespacePrefix) || !strings.Contains(got, tt.wantNamespaceContains) {
					t.Errorf("namespace stage = %s %q, want failed with reason starting %q and containing %q", namespace.Status, got, tt.wantNamespacePrefix, tt.wantNamespaceContains)
				}
			}

			out, err := json.Marshal(env)
			if err != nil {
				t.Fatal(err)
			}
			for _, leak := range []string{tt.target.Address, "127.0.0.1"} {
				if strings.Contains(string(out), leak) {
					t.Errorf("result %s contains %q", out, leak)
				}
			}
		})
	}
}

func TestRPCErrorMessageScrub(t *testing.T) {
	tests := []struct {
		name    string
		address string
		message string
		want    string
	}{
		{
			name:    "single-label host is not replaced inside other words",
			address: "temporal:7233",
			message: "namespace temporal-system is unavailable",
			want:    "the Temporal server reported: namespace temporal-system is unavailable",
		},
		{
			name:    "FQDN, IP and address are replaced",
			address: "temporal.example.com:7233",
			message: "temporal.example.com at 10.1.2.3 refused temporal.example.com:7233",
			want:    "the Temporal server reported: <address> at <address> refused <address>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 1, 2, 3), Port: 7233}}
			got := rpcErrorMessage(status.Error(codes.Unavailable, tt.message), &config.Target{Address: tt.address}, p)
			if got != tt.want {
				t.Errorf("message = %q, want %q", got, tt.want)
			}
		})
	}
}
