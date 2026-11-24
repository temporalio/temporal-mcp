package temporal

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/identity"
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
	clients, err := NewClients(&config.Config{Targets: []config.Target{{Name: "a", Address: closedAddress(t), Namespace: "default"}}}, "http", "test", "0", slog.New(slog.DiscardHandler))
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

func TestNewClientsRejectsUnparseableAddress(t *testing.T) {
	_, err := NewClients(&config.Config{Targets: []config.Target{{Name: "a", Address: "%zz.example:7233", Namespace: "default"}}}, "http", "test", "0", slog.New(slog.DiscardHandler))
	if want := "targets[0]: address:"; err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("err = %v, want it to contain %q", err, want)
	}
}

func TestIsCloud(t *testing.T) {
	for _, tt := range []struct {
		target config.Target
		want   bool
	}{
		{config.Target{Address: "dns:///orders.acct1.tmprl.cloud:7233"}, true},
		{config.Target{Address: "vpce-0a1b2c.vpce-svc-3d4e5f.us-east-1.vpce.amazonaws.com:7233", TLSServerName: "orders.acct1.tmprl.cloud"}, true},
		{config.Target{Address: "orders.acct1.tmprl.cloud:7233", TLSServerName: "temporal.example.com"}, false},
	} {
		if got := IsCloud(&tt.target); got != tt.want {
			t.Errorf("IsCloud of address %q and TLS server name %q = %v, want %v", tt.target.Address, tt.target.TLSServerName, got, tt.want)
		}
	}
}

func TestAPIKeyTargetsUseTLS(t *testing.T) {
	t.Setenv("TEMPORAL_MCP_TEST_API_KEY", "key")
	tests := []struct {
		name   string
		apiKey *config.Secret
		want   byte
	}{
		{"api key", &config.Secret{Env: "TEMPORAL_MCP_TEST_API_KEY"}, 0x16},
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

// serve answers every gRPC call with an empty message and sends the call's method and metadata on calls.
func serve(t *testing.T, calls chan<- call, opts ...grpc.ServerOption) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(append(opts, grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		md, _ := metadata.FromIncomingContext(stream.Context())
		calls <- call{path.Base(method), md}
		return stream.SendMsg(&emptypb.Empty{})
	}))...)
	go s.Serve(ln)
	t.Cleanup(s.Stop)
	return ln.Addr().String()
}

func newClient(t *testing.T, target config.Target) client.Client {
	t.Helper()
	clients, err := NewClients(&config.Config{Targets: []config.Target{target}}, "http", "temporal-mcp", "1.2.3", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	c := clients.Client(target.Name)
	t.Cleanup(c.Close)
	return c
}

func newCertificate(t *testing.T, dir, host string) (*config.Certificate, *tls.Certificate, *x509.CertPool) {
	t.Helper()
	cert := &config.Certificate{CertFile: filepath.Join(dir, "tls.crt"), KeyFile: filepath.Join(dir, "tls.key")}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotAfter: time.Now().Add(time.Hour)}
	if ip := net.ParseIP(host); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{host}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	for file, block := range map[string]*pem.Block{cert.CertFile: {Type: "CERTIFICATE", Bytes: der}, cert.KeyFile: {Type: "PRIVATE KEY", Bytes: keyDER}} {
		if err := os.WriteFile(file, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	pair, err := cert.Load()
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(pair.Leaf)
	return cert, pair, roots
}

func TestClientIdentityHeaders(t *testing.T) {
	const apiKey = "test-api-key-value"
	t.Setenv("TEMPORAL_MCP_TEST_API_KEY", apiKey)
	dir := t.TempDir()
	cert, pair, roots := newCertificate(t, dir, "127.0.0.1")

	calls := make(chan call, 10)
	plain := serve(t, calls)
	secure := serve(t, calls, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{*pair}})))
	tests := []struct {
		name          string
		transport     string
		cfg           config.Config
		clientVersion string
	}{
		{"stdio without credentials", "stdio", config.Config{Targets: []config.Target{{Name: "a", Address: plain, Namespace: "default"}}},
			"1.2.3 (transport=stdio; sso=false; credential=none)"},
		{"http with an API key", "http", config.Config{Targets: []config.Target{{Name: "a", Address: "orders.acct1.tmprl.cloud:7233", Namespace: "orders.acct1", APIKey: &config.Secret{Env: "TEMPORAL_MCP_TEST_API_KEY"}}}},
			"1.2.3 (transport=http; sso=false; credential=api_key)"},
		{"https with auth, an issuer to escape and a client certificate", "https", config.Config{Auth: &config.Auth{Issuer: "https://ïd.example.com:8443/tenant-1/v2.0"}, Targets: []config.Target{{Name: "a", Address: secure, Namespace: "default", TLS: cert}}},
			"1.2.3 (transport=https; sso=true; issuer=%C3%AFd.example.com; credential=mtls)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clients, err := NewClients(&tt.cfg, tt.transport, "temporal-mcp", "1.2.3", slog.New(slog.DiscardHandler), WithRootCAs(roots), WithPlaintextCloudOpsAPI(plain))
			if err != nil {
				t.Fatal(err)
			}
			c := clients.Client("a")
			t.Cleanup(c.Close)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			ctx = WithTool(ctx, "t", []string{
				workflowservice.WorkflowService_GetSystemInfo_FullMethodName,
				workflowservice.WorkflowService_DescribeNamespace_FullMethodName,
				workflowservice.WorkflowService_DescribeWorkflowExecution_FullMethodName,
				cloudservice.CloudService_GetNamespace_FullMethodName,
			}, []string{"a"})

			wantMethods := []string{"GetNamespace"}
			if ops := clients.CloudOps("a"); ops != nil {
				if _, err := ops.GetNamespace(ctx, &cloudservice.GetNamespaceRequest{Namespace: "orders.acct1"}); err != nil {
					t.Fatal(err)
				}
			} else {
				wantMethods = []string{"DescribeNamespace", "DescribeWorkflowExecution", "GetSystemInfo", "GetSystemInfo"}
				if _, err := c.WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{}); err != nil {
					t.Fatal(err)
				}
				if _, err := c.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: "default"}); err != nil {
					t.Fatal(err)
				}
				if _, err := c.DescribeWorkflowExecution(ctx, "wf", ""); err != nil {
					t.Fatal(err)
				}
			}

			var methods []string
			for len(calls) > 0 {
				rpc := <-calls
				methods = append(methods, rpc.method)
				if got, want := rpc.md.Get("client-name"), []string{"temporal-mcp"}; !slices.Equal(got, want) {
					t.Errorf("%s client-name = %v, want %v", rpc.method, got, want)
				}
				if got, want := rpc.md.Get("client-version"), []string{tt.clientVersion}; !slices.Equal(got, want) {
					t.Errorf("%s client-version = %v, want %v", rpc.method, got, want)
				}
				if ua := rpc.md.Get("user-agent"); len(ua) == 0 || !strings.HasPrefix(ua[0], "temporal-mcp/"+tt.clientVersion) {
					t.Errorf("%s user-agent = %v, want prefix temporal-mcp/%s", rpc.method, ua, tt.clientVersion)
				}
				for k, v := range rpc.md {
					if s := strings.Join(v, ","); k != "authorization" && (strings.Contains(s, apiKey) || strings.Contains(s, dir)) {
						t.Errorf("%s header %s = %q, want no API key or certificate path", rpc.method, k, s)
					}
				}
			}
			slices.Sort(methods)
			if !slices.Equal(methods, wantMethods) {
				t.Errorf("methods = %v, want %v", methods, wantMethods)
			}
		})
	}
}

func TestTLSServerName(t *testing.T) {
	t.Setenv("TEMPORAL_MCP_TEST_API_KEY", "key")
	cert, pair, roots := newCertificate(t, t.TempDir(), "orders.acct1.tmprl.cloud")
	address := serve(t, make(chan call, 10), grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{*pair}})))
	apiKey := &config.Secret{Env: "TEMPORAL_MCP_TEST_API_KEY"}
	tests := []struct {
		name       string
		apiKey     *config.Secret
		cert       *config.Certificate
		serverName string
		cloudOps   bool
		wantErr    string
	}{
		{"API key and the certificate's name", apiKey, nil, "orders.acct1.tmprl.cloud", true, ""},
		{"API key and no server name", apiKey, nil, "", false, "x509: cannot validate certificate for 127.0.0.1"},
		{"API key and a name the certificate does not name", apiKey, nil, "other.example.com", false, "x509: certificate is valid for orders.acct1.tmprl.cloud, not other.example.com"},
		{"client certificate and the certificate's name", nil, cert, "orders.acct1.tmprl.cloud", false, ""},
		{"client certificate and no server name", nil, cert, "", false, "x509: cannot validate certificate for 127.0.0.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := config.Target{Name: "a", Address: address, TLSServerName: tt.serverName, Namespace: "orders.acct1", APIKey: tt.apiKey, TLS: tt.cert}
			clients, err := NewClients(&config.Config{Targets: []config.Target{target}}, "http", "temporal-mcp", "1.2.3", slog.New(slog.DiscardHandler), WithRootCAs(roots))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(clients.Close)
			if got := clients.CloudOps("a") != nil; got != tt.cloudOps {
				t.Errorf("has a Cloud Ops API client = %v, want %v", got, tt.cloudOps)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			ctx = WithTool(ctx, "t", []string{workflowservice.WorkflowService_GetSystemInfo_FullMethodName}, []string{"a"})
			_, err = clients.Client("a").WorkflowService().GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("GetSystemInfo = %v, want success", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("GetSystemInfo = %v, want an error containing %q", err, tt.wantErr)
			}
		})
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
	}, nil, slog.New(slog.DiscardHandler))
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

func TestCloseClosesCloudOpsClients(t *testing.T) {
	t.Setenv("TEMPORAL_MCP_TEST_API_KEY", "key")
	clients, err := NewClients(&config.Config{Targets: []config.Target{{Name: "cloud", Address: "payments.a1b2c.tmprl.cloud:7233", Namespace: "payments.a1b2c", APIKey: &config.Secret{Env: "TEMPORAL_MCP_TEST_API_KEY"}}}}, "http", "test", "0", slog.New(slog.DiscardHandler), WithPlaintextCloudOpsAPI(closedAddress(t)))
	if err != nil {
		t.Fatal(err)
	}
	clients.Close()
	ctx := WithTool(t.Context(), "t", []string{cloudservice.CloudService_GetNamespace_FullMethodName}, []string{"cloud"})
	_, err = clients.CloudOps("cloud").GetNamespace(ctx, &cloudservice.GetNamespaceRequest{Namespace: "payments.a1b2c"})
	if code := serviceerror.ToStatus(err).Code(); code != codes.Canceled {
		t.Errorf("a Cloud Ops API call after Close = %v, want Canceled", code)
	}
}

func TestIdentity(t *testing.T) {
	clients, err := NewClients(&config.Config{}, "http", "temporal-mcp", "0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if got := clients.Identity(identity.WithCaller(t.Context(), &identity.Caller{Issuer: "https://idp.example", Subject: "alice"})); got != "temporal-mcp:alice" {
		t.Errorf("identity of caller alice = %q, want temporal-mcp:alice", got)
	}
}
