package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
	"github.com/temporalio/temporal-operator-mcp/internal/temporal"
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

func serve(t *testing.T, ln net.Listener, svc workflowservice.WorkflowServiceServer, opts ...grpc.ServerOption) {
	t.Helper()
	s := grpc.NewServer(opts...)
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

	t.Setenv("TEMPORAL_OPERATOR_MCP_TEST_API_KEY", "key")
	tests := []struct {
		name                  string
		target                config.Target
		wantServer            string
		wantNamespacePrefix   string
		wantNamespaceContains string
	}{
		{
			name:       "TLS handshake fails",
			target:     config.Target{Address: tlsMismatch.Addr().String(), APIKey: &config.Secret{Env: "TEMPORAL_OPERATOR_MCP_TEST_API_KEY"}},
			wantServer: "Unavailable: the TLS handshake or certificate check with the Temporal server failed",
		},
		{
			name:                  "server reports unavailable",
			target:                config.Target{Address: unavailable.Addr().String()},
			wantNamespacePrefix:   "Unavailable: the Temporal server reported: namespace ns1 is unavailable on ",
			wantNamespaceContains: "<address>",
		},
		{
			name:       "no response",
			target:     config.Target{Address: silent.Addr().String()},
			wantServer: "DeadlineExceeded: timed out waiting for the Temporal server",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.target.Name, tt.target.Description, tt.target.Namespace = "a", "x", "default"
			cfg := &config.Config{Targets: []config.Target{tt.target}}
			clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			ctx = temporal.WithTool(context.WithValue(ctx, targetsKey{}, []string{"a"}), "check_connection", []string{
				workflowservice.WorkflowService_GetSystemInfo_FullMethodName,
				workflowservice.WorkflowService_DescribeNamespace_FullMethodName,
			}, []string{"a"})

			_, env, err := checkConnection(cfg, clients, newRenderer(t, cfg, slog.New(slog.DiscardHandler)))(ctx, nil, checkConnectionInput{Target: "a"})
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
		{
			name:    "host of a dns:/// address is replaced",
			address: "dns:///temporal.example.com:7233",
			message: "temporal.example.com at 10.1.2.3 refused dns:///temporal.example.com:7233",
			want:    "the Temporal server reported: <address> at <address> refused <address>",
		},
		{
			name:    "unprefixed address of a dns:/// address is replaced",
			address: "dns:///temporal.example.com:7233",
			message: "temporal.example.com:7233 refused the call",
			want:    "the Temporal server reported: <address> refused the call",
		},
		{
			name:    "host of a port-less dns:/// address is replaced",
			address: "dns:///temporal.example.com",
			message: "no route to temporal.example.com",
			want:    "the Temporal server reported: no route to <address>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 1, 2, 3), Port: 7233}}
			got := rpcErrorMessage(status.Error(codes.Unavailable, tt.message), &config.Target{Address: tt.address}, p, temporalServer)
			if got != tt.want {
				t.Errorf("message = %q, want %q", got, tt.want)
			}
		})
	}
}

// newCertificate returns a self-signed certificate for 127.0.0.1 named name, and its PEM certificate and key.
func newCertificate(t *testing.T, name string) (cert *x509.Certificate, certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if cert, err = x509.ParseCertificate(der); err != nil {
		t.Fatal(err)
	}
	return cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func writeCertificate(t *testing.T, c *config.Certificate, cert, key []byte) {
	t.Helper()
	for file, data := range map[string][]byte{c.CertFile: cert, c.KeyFile: key} {
		if err := os.WriteFile(file, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// certificateRecorder records rejected certificates and the common name of the client certificate on each GetSystemInfo call,
// or "" if there is none.
type certificateRecorder struct {
	workflowservice.UnimplementedWorkflowServiceServer
	credentials.TransportCredentials
	rejected  chan *x509.Certificate
	mu        sync.Mutex
	presented []string
}

func (r *certificateRecorder) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := r.TransportCredentials.ServerHandshake(conn)
	var untrusted x509.UnknownAuthorityError
	if errors.As(err, &untrusted) {
		select {
		case r.rejected <- untrusted.Cert:
		default:
		}
	}
	return conn, info, err
}

func (r *certificateRecorder) GetSystemInfo(ctx context.Context, _ *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	p, _ := peer.FromContext(ctx)
	var name string
	if certs := p.AuthInfo.(credentials.TLSInfo).State.PeerCertificates; len(certs) > 0 {
		name = certs[0].Subject.CommonName
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.presented = append(r.presented, name)
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (r *certificateRecorder) last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.presented[len(r.presented)-1]
}

func TestClientCertificate(t *testing.T) {
	serverRoot, serverCert, serverKey := newCertificate(t, "server")
	serverPair, err := tls.X509KeyPair(serverCert, serverKey)
	if err != nil {
		t.Fatal(err)
	}
	clientCAs := x509.NewCertPool()
	first, firstCert, firstKey := newCertificate(t, "first")
	renewed, renewedCert, renewedKey := newCertificate(t, "renewed")
	clientCAs.AddCert(first)
	clientCAs.AddCert(renewed)
	recorder := &certificateRecorder{
		// A client with no certificate connects, so a client that falls back to none when it cannot load its own fails the test.
		TransportCredentials: credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{serverPair}, ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: clientCAs, MinVersion: tls.VersionTLS13}),
		rejected:             make(chan *x509.Certificate, 1),
	}
	ln := listen(t)
	serve(t, ln, recorder,
		grpc.Creds(recorder),
		// Like a Temporal server, which by default replaces each connection after 5 minutes.
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: 100 * time.Millisecond}))

	dir := t.TempDir()
	files := &config.Certificate{CertFile: filepath.Join(dir, "tls.crt"), KeyFile: filepath.Join(dir, "tls.key")}
	writeCertificate(t, files, firstCert, firstKey)
	untrusted := &config.Certificate{CertFile: filepath.Join(dir, "untrusted.crt"), KeyFile: filepath.Join(dir, "untrusted.key")}
	untrustedRoot, untrustedCert, untrustedKey := newCertificate(t, "untrusted")
	writeCertificate(t, untrusted, untrustedCert, untrustedKey)
	cfg := &config.Config{Targets: []config.Target{
		{Name: "a", Description: "x", Address: ln.Addr().String(), Namespace: "default", TLS: files},
		{Name: "untrusted", Description: "x", Address: ln.Addr().String(), Namespace: "default", TLS: untrusted},
	}}
	roots := x509.NewCertPool()
	roots.AddCert(serverRoot)
	clients, err := temporal.NewClients(cfg.Targets, "test", "0", slog.New(slog.DiscardHandler), temporal.WithRootCAs(roots))
	if err != nil {
		t.Fatal(err)
	}
	session := serveTools(t, cfg, clients, t.Output())

	// waitForServerStage calls check_connection on target until its server stage fails with one of reasons, or succeeds with
	// presented as the client certificate if presented is not "".
	waitForServerStage := func(t *testing.T, target, presented string, reasons ...string) {
		t.Helper()
		wantStatus := "failed"
		if presented != "" {
			wantStatus = "ok"
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "check_connection", Arguments: map[string]any{"target": target}})
			if err != nil {
				t.Fatal(err)
			}
			serverStage := res.StructuredContent.(map[string]any)["result"].(map[string]any)["stages"].([]any)[0].(map[string]any)
			got, _ := serverStage["reason"].(string)
			if serverStage["status"] == wantStatus && slices.Contains(reasons, got) && (presented == "" || recorder.last() == presented) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("server stage = %s %q; want %s with reason in %q and certificate %q", serverStage["status"], got, wantStatus, reasons, presented)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitForServerStage(t, "a", "first", "")
	writeCertificate(t, files, renewedCert, renewedKey)
	waitForServerStage(t, "a", "renewed", "")
	writeCertificate(t, files, []byte("not a certificate\n"), renewedKey)
	waitForServerStage(t, "a", "", "Unavailable: the TLS handshake or certificate check with the Temporal server failed")
	// Under TLS 1.3, writing the HTTP/2 preface can fail before gRPC reads the TLS rejection alert.
	waitForServerStage(t, "untrusted", "",
		"Unavailable: the TLS handshake or certificate check with the Temporal server failed",
		"Unavailable: could not connect to the Temporal server")
	select {
	case rejected := <-recorder.rejected:
		if !rejected.Equal(untrustedRoot) {
			t.Fatalf("rejected certificate = %q, want untrusted", rejected.Subject.CommonName)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not reject the untrusted client certificate")
	}
}
