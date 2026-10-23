package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc/oidctest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/grpc"
)

func buildBinary(t *testing.T, buildArgs ...string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), name)
	out, err := exec.Command("go", append([]string{"build", "-o", bin}, append(buildArgs, ".")...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

func startServer(t *testing.T, bin, configPath string, stdout io.Writer) string {
	t.Helper()
	cmd := exec.Command(bin, "--config", configPath)
	cmd.Stdout = stdout
	stderr, _ := start(t, cmd)
	url := listening(t, stderr)
	go func() {
		for range stderr {
		}
	}()
	return url
}

// start runs cmd and returns its stderr lines and pipe. Read the lines to the end, or close the pipe.
func start(t *testing.T, cmd *exec.Cmd) (<-chan string, io.ReadCloser) {
	t.Helper()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	lines := make(chan string)
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	return lines, stderr
}

func listening(t *testing.T, lines <-chan string) string {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("server exited before listening")
			}
			t.Log(line)
			if url, found := strings.CutPrefix(line, name+": listening on "); found {
				return url
			}
		case <-timeout:
			t.Fatal("timed out waiting for server to listen")
		}
	}
}

func writeCertificate(t *testing.T, certFile, keyFile string) *x509.CertPool {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	for file, block := range map[string]*pem.Block{certFile: {Type: "CERTIFICATE", Bytes: der}, keyFile: {Type: "PRIVATE KEY", Bytes: keyDER}} {
		if err := os.WriteFile(file, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return roots
}

func TestServerHandshake(t *testing.T) {
	stdout, err := os.Create(filepath.Join(t.TempDir(), "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	defer stdout.Close()
	url := startServer(t, buildBinary(t, "-ldflags", "-X main.version=9.9.9-test"), filepath.Join("testdata", "config.yaml"), stdout)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "e2e-test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: url}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if got := session.InitializeResult().ServerInfo; got.Name != "temporal-operator" || got.Version != "9.9.9-test" {
		t.Errorf("server info = %+v, want name temporal-operator and version 9.9.9-test", got)
	}
	if got := session.InitializeResult().ProtocolVersion; got != "2026-07-28" {
		t.Errorf("protocol version = %s, want 2026-07-28: clients fell back to the 2025 session protocol, so check that the HTTP handler is still stateless", got)
	}
	if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_available_targets"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(stdout.Name())
	if err != nil {
		t.Fatal(err)
	}
	var record struct{ Tool string }
	if err := json.Unmarshal(data, &record); err != nil || record.Tool != "list_available_targets" {
		t.Errorf("stdout = %q (%v), want one list_available_targets audit record", data, err)
	}
}

func TestHTTPS(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, configPath := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"), filepath.Join(dir, "config.yaml")
	roots := writeCertificate(t, certFile, keyFile)
	config := fmt.Sprintf("listen: 127.0.0.1:0\nlisten_tls: {cert_file: %q, key_file: %q}\n"+
		"targets:\n  - {name: prod, description: x, address: prod.example.tmprl.cloud:7233, namespace: payments.abc12}\n", certFile, keyFile)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	url := startServer(t, buildBinary(t), configPath, nil)
	if !strings.HasPrefix(url, "https://") {
		t.Fatalf("listening on %s, want an https URL", url)
	}
	connect := func(rootCAs *x509.CertPool) error {
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: rootCAs}}}
		session, err := mcp.NewClient(&mcp.Implementation{Name: "e2e-test", Version: "0"}, nil).Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: url, HTTPClient: client}, nil)
		if err != nil {
			return err
		}
		return session.Close()
	}
	if err := connect(roots); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, []byte("invalid PEM\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := connect(roots); err == nil {
		t.Error("connection succeeded with an invalid certificate file")
	}
	if err := connect(writeCertificate(t, certFile, keyFile)); err != nil {
		t.Errorf("connecting after replacing the certificate: %v", err)
	}
}

func TestLogFiles(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddr := ln.Addr().String()
	ln.Close()
	bin := buildBinary(t)
	for _, stdio := range []bool{false, true} {
		t.Run(fmt.Sprintf("stdio=%t", stdio), func(t *testing.T) {
			dir := t.TempDir()
			auditLog, errorLog, configPath := filepath.Join(dir, "audit.log"), filepath.Join(dir, "error.log"), filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(errorLog, []byte("earlier line\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(errorLog, 0o640); err != nil {
				t.Fatal(err)
			}
			config := fmt.Sprintf("audit_log_file: %q\nerror_log_file: %q\n"+
				"targets:\n  - {name: prod, description: x, address: %q, namespace: default, payload_handling: decode, codec: c}\n"+
				"codecs:\n  c: {endpoint: \"http://%s\"}\n", auditLog, errorLog, closedAddr, closedAddr)
			if !stdio {
				config = "listen: 127.0.0.1:0\n" + config
			}
			if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			var transport mcp.Transport = &mcp.CommandTransport{Command: exec.Command(bin, "--stdio", "--config", configPath)}
			if !stdio {
				transport = &mcp.StreamableClientTransport{Endpoint: startServer(t, bin, configPath, nil)}
			}
			session, err := mcp.NewClient(&mcp.Implementation{Name: "e2e-test", Version: "0"}, nil).Connect(t.Context(), transport, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "check_connection", Arguments: map[string]any{"target": "prod"}}); err != nil {
				t.Fatal(err)
			}

			data, err := os.ReadFile(auditLog)
			if err != nil {
				t.Fatal(err)
			}
			var record struct{ Tool, Target string }
			if err := json.Unmarshal(data, &record); err != nil || record.Tool != "check_connection" || record.Target != "prod" {
				t.Errorf("audit log = %q (%v), want one check_connection record for prod", data, err)
			}
			if data, err = os.ReadFile(errorLog); err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(string(data), "earlier line\n") || !strings.Contains(string(data), `msg="codec check failed"`) {
				t.Errorf("error log = %q, want the earlier line followed by the codec check failure", data)
			}
			for path, want := range map[string]os.FileMode{auditLog: 0o600, errorLog: 0o640} {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if got := info.Mode().Perm(); got != want {
					t.Errorf("%s mode = %v, want %v", filepath.Base(path), got, want)
				}
			}
		})
	}
}

func TestStdio(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	dev, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Stop() })
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	config := fmt.Sprintf("targets:\n  - {name: dev, description: x, address: %q, namespace: default}\n", dev.FrontendHostPort())
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := exec.CommandContext(ctx, buildBinary(t), "--stdio", "--config", configPath)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	// The client fails on any line of stdout that is not a JSON-RPC message.
	session, err := mcp.NewClient(&mcp.Implementation{Name: "e2e-test", Version: "0"}, nil).Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatalf("%v\nstderr:\n%s", err, &stderr)
	}
	t.Cleanup(func() { _ = session.Close() })
	if got := session.InitializeResult().ProtocolVersion; got != "2026-07-28" {
		t.Errorf("protocol version = %s, want 2026-07-28", got)
	}
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "check_connection", Arguments: map[string]any{"target": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	var check struct {
		Result struct {
			Stages []struct{ Name, Status string }
		}
	}
	b, _ := json.Marshal(res.StructuredContent)
	if res.IsError || json.Unmarshal(b, &check) != nil {
		t.Fatalf("check_connection = %s", b)
	}
	statuses := map[string]string{}
	for _, s := range check.Result.Stages {
		statuses[s.Name] = s.Status
	}
	if want := map[string]string{"server": "ok", "namespace": "ok", "codec": "skipped"}; !maps.Equal(statuses, want) {
		t.Errorf("check_connection = %s, want stage statuses %v", b, want)
	}

	// Closing the session closes the server's stdin and returns the error of the server's exit.
	if err := session.Close(); err != nil {
		t.Fatalf("server did not exit cleanly when stdin closed: %v\nstderr:\n%s", err, &stderr)
	}
	if !strings.Contains(stderr.String(), `"tool":"check_connection"`) {
		t.Errorf("stderr has no audit record of the call:\n%s", &stderr)
	}
}

func TestBinaryDoesNotImportFixture(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	deps := strings.Fields(string(out))
	if !slices.Contains(deps, "github.com/temporalio/temporal-mcp/internal/server") {
		t.Fatalf("go list -deps did not list internal/server:\n%s", out)
	}
	if slices.Contains(deps, "github.com/temporalio/temporal-mcp/internal/fixture") {
		t.Error("the binary imports internal/fixture, which must stay test-only")
	}
}

func TestExitsWithoutServing(t *testing.T) {
	bin := buildBinary(t, "-ldflags", "-X main.version=9.9.9-test")
	writeConfig := func(yaml string) string {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	noTargets := writeConfig("listen: 0.0.0.0:0\ntargets: []\n")
	unknownTool := writeConfig("listen: 127.0.0.1:0\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default}\n" +
		"auth:\n  resource: https://mcp.example.com/mcp\n  issuers: [{issuer: https://idp.example.com, audience: operator}]\n" +
		"authorization:\n  profiles: {p: {tools: [get_workflows, terminate_workflow], targets: [a]}}\n  rules: [{claim_name: sub, claim_value: alice, profile: p}]\n")
	unsetKey := writeConfig("listen: 127.0.0.1:0\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default, api_key: {env: TEMPORAL_MCP_TEST_UNSET_KEY}}\n")
	unsetCodecAuthorization := writeConfig("listen: 127.0.0.1:0\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default, payload_handling: decode, codec: c}\n" +
		"codecs:\n  c: {endpoint: https://codec.example.com, authorization_header: {env: TEMPORAL_MCP_TEST_UNSET_KEY}}\n")
	dir := t.TempDir()
	writeCertificate(t, filepath.Join(dir, "a.crt"), filepath.Join(dir, "a.key"))
	writeCertificate(t, filepath.Join(dir, "b.crt"), filepath.Join(dir, "b.key"))
	writeTLSConfig := func(certFile, keyFile string) string {
		return writeConfig(fmt.Sprintf("listen: 127.0.0.1:0\nlisten_tls: {cert_file: %q, key_file: %q}\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default}\n", certFile, keyFile))
	}
	missingLog := filepath.Join(dir, "missing", "server.log")
	writeLogConfig := func(field string) string {
		return writeConfig(fmt.Sprintf("listen: 127.0.0.1:0\n%s: %q\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default}\n", field, missingLog))
	}
	unopenableAuditLog, unopenableErrorLog := writeLogConfig("audit_log_file"), writeLogConfig("error_log_file")
	missingCertificate := writeTLSConfig(filepath.Join(dir, "missing.crt"), filepath.Join(dir, "a.key"))
	mismatchedKey := writeTLSConfig(filepath.Join(dir, "a.crt"), filepath.Join(dir, "b.key"))
	mismatchedClientKey := writeConfig(fmt.Sprintf("listen: 127.0.0.1:0\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default, tls: {cert_file: %q, key_file: %q}}\n", filepath.Join(dir, "a.crt"), filepath.Join(dir, "b.key")))
	missingKeyPath := filepath.Join(dir, "missing-api-key")
	missingKey := writeConfig(fmt.Sprintf("listen: 127.0.0.1:0\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default, api_key: {file: %q}}\n", missingKeyPath))
	missingKeyAndCertificate := writeConfig(fmt.Sprintf("listen: 127.0.0.1:0\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default, api_key: {file: %q}}\n  - {name: b, description: x, address: \"b:7233\", namespace: default, tls: {cert_file: %q, key_file: %q}}\n", missingKeyPath, filepath.Join(dir, "missing.crt"), filepath.Join(dir, "a.key")))
	emptyAuthorizationPath := filepath.Join(dir, "empty-authorization")
	if err := os.WriteFile(emptyAuthorizationPath, []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	emptyCodecAuthorization := writeConfig(fmt.Sprintf("listen: 127.0.0.1:0\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default, payload_handling: decode, codec: c}\n"+
		"codecs:\n  c: {endpoint: https://codec.example.com, authorization_header: {file: %q}}\n", emptyAuthorizationPath))
	tests := []struct {
		name     string
		args     []string
		exitCode int
		want     string
	}{
		{"-h", []string{"-h"}, 0, "Usage of temporal-mcp"},
		{"--version", []string{"--version"}, 0, "temporal-mcp 9.9.9-test"},
		{"unexpected arguments", []string{"--config", filepath.Join("testdata", "config.yaml"), "extra"}, 1, `unexpected arguments ["extra"]`},
		{"invalid config", []string{"--config", noTargets}, 1, "invalid config " + noTargets + ": "},
		{"unusable target", []string{"--config", unsetKey}, 1, "invalid config " + unsetKey + ": "},
		{"unusable codec", []string{"--config", unsetCodecAuthorization}, 1, "invalid config " + unsetCodecAuthorization + `: codecs: "c": authorization_header: environment variable TEMPORAL_MCP_TEST_UNSET_KEY is unset or empty`},
		{"missing API key file", []string{"--config", missingKey}, 1, "invalid config " + missingKey + ": targets[0]: api_key: open " + missingKeyPath + ": no such file or directory"},
		{"missing API key file and client certificate", []string{"--config", missingKeyAndCertificate}, 1, "invalid config " + missingKeyAndCertificate + ": targets[0]: api_key: open " + missingKeyPath + ": no such file or directory\ntargets[1]: tls: open " + filepath.Join(dir, "missing.crt")},
		{"empty codec authorization header file", []string{"--config", emptyCodecAuthorization}, 1, "invalid config " + emptyCodecAuthorization + `: codecs: "c": authorization_header: ` + emptyAuthorizationPath + " is empty"},
		{"unreadable certificate", []string{"--config", missingCertificate}, 1, "invalid config " + missingCertificate + ": listen_tls: open " + filepath.Join(dir, "missing.crt")},
		{"key does not match certificate", []string{"--config", mismatchedKey}, 1, "invalid config " + mismatchedKey + ": listen_tls: tls: private key does not match public key"},
		{"client key does not match certificate", []string{"--config", mismatchedClientKey}, 1, "invalid config " + mismatchedClientKey + ": targets[0]: tls: tls: private key does not match public key"},
		{"unopenable audit log", []string{"--config", unopenableAuditLog}, 1, "invalid config " + unopenableAuditLog + ": audit_log_file: open " + missingLog},
		{"unopenable error log", []string{"--config", unopenableErrorLog}, 1, "invalid config " + unopenableErrorLog + ": error_log_file: open " + missingLog},
		{"unknown tool in a profile", []string{"--config", unknownTool}, 1, "invalid config " + unknownTool + `: authorization: profiles: "p": tools[1]: unknown tool "terminate_workflow"; the tools are list_available_targets, check_connection, get_workflows`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, tt.args...)
			cmd.Env = slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "TEMPORAL_MCP_TEST_UNSET_KEY=") })
			out, err := cmd.CombinedOutput()
			if code := cmd.ProcessState.ExitCode(); code != tt.exitCode {
				t.Fatalf("exit code = %d (err %v), want %d\n%s", code, err, tt.exitCode, out)
			}
			if !strings.Contains(string(out), tt.want) {
				t.Errorf("output %q does not contain %q", out, tt.want)
			}
		})
	}
}

func TestAuthIssuerDownAtStartup(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	issuer := "http://" + ln.Addr().String()
	ln.Close()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	token := oidctest.SignIDToken(key, "k1", "RS256", fmt.Sprintf(`{"iss":%q,"sub":"alice","aud":"operator","exp":%d}`, issuer, time.Now().Add(time.Hour).Unix()))
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	config := fmt.Sprintf("listen: 127.0.0.1:0\n"+
		"targets:\n  - {name: prod, description: x, address: prod.example.tmprl.cloud:7233, namespace: payments.abc12}\n"+
		"auth:\n  resource: https://mcp.example.com/mcp\n  issuers: [{issuer: %q, audience: operator}]\n"+
		"authorization:\n  profiles: {p: {tools: [get_workflows], targets: [prod]}}\n  rules: [{claim_name: sub, claim_value: alice, profile: p}]\n", issuer)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, startServer(t, buildBinary(t), configPath, nil), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"e2e-test","version":"0"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

// A blockingWorkflowService holds each DescribeNamespace or CountWorkflowExecutions call, after sending on
// started, until release is closed or the call is cancelled.
type blockingWorkflowService struct {
	workflowservice.UnimplementedWorkflowServiceServer
	started chan struct{}
	release chan struct{}
}

func (*blockingWorkflowService) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (s *blockingWorkflowService) DescribeNamespace(ctx context.Context, _ *workflowservice.DescribeNamespaceRequest) (*workflowservice.DescribeNamespaceResponse, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return &workflowservice.DescribeNamespaceResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *blockingWorkflowService) CountWorkflowExecutions(ctx context.Context, _ *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	_, err := s.DescribeNamespace(ctx, nil)
	return &workflowservice.CountWorkflowExecutionsResponse{Count: 1}, err
}

// callShutdownTool calls tool on target a over MCP protocol
// version protocol and returns its result or a transport error. With version
// 2026-07-28 it calls through the SDK's client. With an older version, which the
// SDK's client cannot speak, it posts the call itself, with protocol in the
// Mcp-Protocol-Version header, or with no header if protocol is "".
func callShutdownTool(ctx context.Context, url, protocol, tool string) (*mcp.CallToolResult, error) {
	if protocol == "2026-07-28" {
		session, err := mcp.NewClient(&mcp.Implementation{Name: "e2e-test", Version: "0"}, nil).Connect(ctx, &mcp.StreamableClientTransport{Endpoint: url}, nil)
		if err != nil {
			return nil, err
		}
		defer session.Close()
		if v := session.InitializeResult().ProtocolVersion; v != protocol {
			return nil, fmt.Errorf("the SDK's client negotiated protocol version %s, want %s", v, protocol)
		}
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: map[string]any{"target": "a"}})
		return res, err
	}
	request, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": map[string]any{"name": tool, "arguments": map[string]any{"target": "a"}},
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(request))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if protocol != "" {
		req.Header.Set("Mcp-Protocol-Version", protocol)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// An event stream carries the JSON-RPC response in its last data line.
	msg := body
	for line := range strings.Lines(string(body)) {
		if data, found := strings.CutPrefix(line, "data:"); found {
			msg = []byte(data)
		}
	}
	var rpc struct {
		Result *mcp.CallToolResult `json:"result"`
	}
	if err := json.Unmarshal(msg, &rpc); err != nil || rpc.Result == nil {
		return nil, fmt.Errorf("%s with response %q", resp.Status, body)
	}
	return rpc.Result, nil
}

func TestShutdownDuringToolCall(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals cannot be sent on Windows")
	}
	bin := buildBinary(t)
	tests := []struct {
		name string
		// protocol is the MCP protocol version of the call; see callShutdownTool.
		protocol        string
		signal          os.Signal
		shutdownTimeout string
		// release says whether Temporal answers the call once the server is shutting down.
		release bool
		outcome string
		// tool defaults to check_connection, whose cancellation returns a raw context error.
		tool       string
		stdio      bool
		calls      int
		disconnect bool
	}{
		{"call finishes", "2026-07-28", syscall.SIGTERM, "", true, "ok", "", false, 1, false},
		{"call finishes after Ctrl-C", "2026-07-28", os.Interrupt, "", true, "ok", "", false, 1, false},
		{"call outlasts the shutdown timeout", "2026-07-28", syscall.SIGTERM, "500ms", false, "cancelled", "", false, 1, false},
		{"2025-11-25 call finishes", "2025-11-25", syscall.SIGTERM, "", true, "ok", "", false, 1, false},
		{"2025-11-25 call outlasts the shutdown timeout", "2025-11-25", syscall.SIGTERM, "500ms", false, "cancelled", "", false, 1, false},
		{"call without a protocol version finishes", "", syscall.SIGTERM, "", true, "ok", "", false, 1, false},
		{"call without a protocol version outlasts the shutdown timeout", "", syscall.SIGTERM, "500ms", false, "cancelled", "", false, 1, false},
		{"count call outlasts the shutdown timeout", "2026-07-28", syscall.SIGTERM, "500ms", false, "cancelled", "count_workflows", false, 1, false},
		{"2025-11-25 count call outlasts the shutdown timeout", "2025-11-25", syscall.SIGTERM, "500ms", false, "cancelled", "count_workflows", false, 1, false},
		{"count call without a protocol version outlasts the shutdown timeout", "", syscall.SIGTERM, "500ms", false, "cancelled", "count_workflows", false, 1, false},
		{"stdio call finishes", "2026-07-28", syscall.SIGTERM, "", true, "ok", "", true, 1, false},
		{"stdio call finishes after Ctrl-C", "2026-07-28", os.Interrupt, "", true, "ok", "", true, 1, false},
		{"stdio call outlasts the shutdown timeout", "2026-07-28", syscall.SIGTERM, "500ms", false, "cancelled", "", true, 1, false},
		{"stdio count call outlasts the shutdown timeout", "2026-07-28", syscall.SIGTERM, "500ms", false, "cancelled", "count_workflows", true, 1, false},
		{"stdio call finishes after EOF", "2026-07-28", nil, "", true, "ok", "", true, 1, false},
		{"stdio call outlasts the shutdown timeout after EOF", "2026-07-28", nil, "500ms", false, "cancelled", "", true, 1, false},
		{"stdio count call outlasts the shutdown timeout after EOF", "2026-07-28", nil, "500ms", false, "cancelled", "count_workflows", true, 1, false},
		{"stdio concurrent calls finish", "2026-07-28", syscall.SIGTERM, "", true, "ok", "", true, 2, false},
		{"stdio concurrent calls time out after EOF", "2026-07-28", nil, "500ms", false, "cancelled", "", true, 2, false},
		{"stdio client closes all pipes", "2026-07-28", nil, "500ms", false, "cancelled", "", true, 2, true},
		{"idle HTTP subscription", "2026-07-28", syscall.SIGTERM, "1s", false, "", "", false, 0, false},
		{"idle stdio subscription", "2026-07-28", syscall.SIGTERM, "1s", false, "", "", true, 0, false},
		{"idle stdio subscription after EOF", "2026-07-28", nil, "1s", false, "", "", true, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			temporal := &blockingWorkflowService{started: make(chan struct{}, tt.calls), release: make(chan struct{})}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			gs := grpc.NewServer()
			workflowservice.RegisterWorkflowServiceServer(gs, temporal)
			go gs.Serve(ln)
			t.Cleanup(gs.Stop)
			dir := t.TempDir()
			configPath, auditPath := filepath.Join(dir, "config.yaml"), filepath.Join(dir, "audit.log")
			config := fmt.Sprintf("targets:\n  - {name: a, description: x, address: %q, namespace: default}\n", ln.Addr())
			if !tt.stdio {
				config = "listen: 127.0.0.1:0\n" + config
			}
			if tt.shutdownTimeout != "" {
				config += "shutdown_timeout: " + tt.shutdownTimeout + "\n"
			}
			if tt.disconnect {
				config += fmt.Sprintf("audit_log_file: %q\n", auditPath)
			}
			if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(ctx, bin, "--config", configPath)
			var records bytes.Buffer
			var stdin io.WriteCloser
			var stdout io.ReadCloser
			if tt.stdio {
				cmd.Args = append(cmd.Args, "--stdio")
				stdin, err = cmd.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				stdout, err = cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
			} else {
				cmd.Stdout = &records
			}
			stderr, stderrPipe := start(t, cmd)
			var url string
			if !tt.stdio {
				url = listening(t, stderr)
				// A connection that never sends a request, like one a client's pool dials and leaves
				// unused, must not hold the shutdown open. The server accepts it before the client's.
				unused, err := net.Dial("tcp", strings.TrimSuffix(strings.TrimPrefix(url, "http://"), "/mcp"))
				if err != nil {
					t.Fatal(err)
				}
				defer unused.Close()
			}
			// logUntil logs stderr until a line equal to want, or to its end if want is "", and reports whether it saw want.
			logUntil := func(want string) bool {
				for {
					select {
					case line, ok := <-stderr:
						if !ok {
							return false
						}
						t.Log(line)
						if tt.calls == 0 && strings.Contains(line, "cancelling the requests still in progress") {
							t.Error("the shutdown waited for the timeout with no call in progress")
						}
						if tt.stdio && strings.HasPrefix(line, "{") {
							records.WriteString(line + "\n")
						}
						if want != "" && line == want {
							return true
						}
					case <-ctx.Done():
						t.Fatal("timed out reading the server's stderr")
					}
				}
			}
			tool := tt.tool
			if tool == "" {
				tool = "check_connection"
			}
			type callResponse struct {
				result *mcp.CallToolResult
				err    error
			}
			var session *mcp.ClientSession
			if tt.stdio || tt.protocol == "2026-07-28" {
				var transport mcp.Transport = &mcp.StreamableClientTransport{Endpoint: url}
				if tt.stdio {
					transport = &mcp.IOTransport{Reader: stdout, Writer: stdin}
				}
				client := mcp.NewClient(&mcp.Implementation{Name: "e2e-test", Version: "0"}, &mcp.ClientOptions{
					ToolListChangedHandler: func(context.Context, *mcp.ToolListChangedRequest) {},
				})
				acked := make(chan struct{})
				client.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
					return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
						if method == "notifications/subscriptions/acknowledged" {
							close(acked)
						}
						return next(ctx, method, req)
					}
				})
				session, err = client.Connect(ctx, transport, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				select {
				case <-acked:
				case <-ctx.Done():
					t.Fatal("subscription was not acknowledged")
				}
			}
			called := make(chan callResponse, tt.calls)
			for range tt.calls {
				go func() {
					if session != nil {
						res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: map[string]any{"target": "a"}})
						called <- callResponse{res, err}
						return
					}
					res, err := callShutdownTool(ctx, url, tt.protocol, tool)
					called <- callResponse{res, err}
				}()
			}
			for range tt.calls {
				select {
				case <-temporal.started:
				case response := <-called:
					t.Fatalf("%s returned before it reached Temporal: %v", tool, response.err)
				case <-ctx.Done():
					t.Fatalf("%s did not reach Temporal", tool)
				}
			}

			if tt.disconnect {
				if err := stderrPipe.Close(); err != nil {
					t.Fatal(err)
				}
				if err := stdout.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if tt.signal == nil {
				if err := stdin.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Fatal(err)
				}
			} else if err := cmd.Process.Signal(tt.signal); err != nil {
				t.Fatal(err)
			}
			if !tt.disconnect && !logUntil(name+": shutting down") {
				t.Errorf("the server exited on %v without shutting down", tt.signal)
			}
			if tt.release {
				close(temporal.release)
			}
			for range tt.calls {
				select {
				case response := <-called:
					if tt.disconnect {
						if response.err == nil {
							t.Error("call succeeded after the client closed its pipes")
						}
						continue
					}
					if response.err != nil {
						t.Fatalf("%s: %v", tool, response.err)
					}
					res := response.result
					if tt.release {
						if res.IsError {
							t.Errorf("%s returned a tool error: %+v", tool, res.Content)
						}
					} else {
						if !res.IsError || len(res.Content) != 1 {
							t.Fatalf("%s result = %+v, want a shutdown tool error", tool, res)
						}
						text, ok := res.Content[0].(*mcp.TextContent)
						if !ok {
							t.Fatalf("error content = %T, want text", res.Content[0])
						}
						var got struct {
							Code      string `json:"code"`
							Message   string `json:"message"`
							Retryable bool   `json:"retryable"`
						}
						if err := json.Unmarshal([]byte(text.Text), &got); err != nil {
							t.Fatal(err)
						}
						if got.Code != "server_shutting_down" || got.Message != "the server is shutting down" || !got.Retryable {
							t.Errorf("%s error = %s, want a retryable server_shutting_down error", tool, text.Text)
						}
					}
				case <-ctx.Done():
					t.Fatal("check_connection did not return")
				}
			}
			logUntil("")
			if err := cmd.Wait(); err != nil {
				// A broken pipe may return an error, but must not kill the server with SIGPIPE.
				if !tt.disconnect || cmd.ProcessState.ExitCode() != 1 {
					t.Errorf("server exited with %v", err)
				}
			}
			if tt.disconnect {
				data, err := os.ReadFile(auditPath)
				if err != nil {
					t.Fatal(err)
				}
				records.Write(data)
			}

			var got []string
			for line := range strings.Lines(records.String()) {
				var rec struct{ Tool, Outcome string }
				if err := json.Unmarshal([]byte(line), &rec); err != nil {
					t.Fatalf("audit record %q: %v", line, err)
				}
				got = append(got, rec.Tool+" "+rec.Outcome)
			}
			if want := slices.Repeat([]string{tool + " " + tt.outcome}, tt.calls); !slices.Equal(got, want) {
				t.Errorf("audit records (tool outcome) = %q, want %q", got, want)
			}
		})
	}
}
