package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"maps"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc/oidctest"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/testsuite"
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

func startServer(t *testing.T, bin, configPath string) string {
	t.Helper()
	cmd := exec.Command(bin, "--config", configPath)
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
	var url string
	timeout := time.After(10 * time.Second)
	for url == "" {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("server exited before listening")
			}
			t.Log(line)
			if u, found := strings.CutPrefix(line, name+": listening on "); found {
				url = u
			}
		case <-timeout:
			t.Fatal("timed out waiting for server to listen")
		}
	}
	go func() {
		for range lines {
		}
	}()
	return url
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
	url := startServer(t, buildBinary(t, "-ldflags", "-X main.version=9.9.9-test"), filepath.Join("testdata", "config.yaml"))
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
	url := startServer(t, buildBinary(t), configPath)
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

func TestStdio(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	exe, _ := exec.LookPath("temporal")
	dev, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{ExistingPath: exe})
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
	if !slices.Contains(deps, "github.com/temporalio/operator-mcp/internal/server") {
		t.Fatalf("go list -deps did not list internal/server:\n%s", out)
	}
	if slices.Contains(deps, "github.com/temporalio/operator-mcp/internal/fixture") {
		t.Error("the binary imports internal/fixture, which must stay test-only")
	}
}

func TestExitsWithoutServing(t *testing.T) {
	bin := buildBinary(t)
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
		"authorization:\n  profiles: {p: {tools: [get_workflow, terminate_workflow], targets: [a]}}\n  rules: [{claim_name: sub, claim_value: alice, profile: p}]\n")
	unsetKey := writeConfig("listen: 127.0.0.1:0\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default, api_key: {env: OPERATOR_MCP_TEST_UNSET_KEY}}\n")
	unsetCodecAuthorization := writeConfig("listen: 127.0.0.1:0\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default, payload_handling: decode, codec: c}\n" +
		"codecs:\n  c: {endpoint: https://codec.example.com, authorization_header: {env: OPERATOR_MCP_TEST_UNSET_KEY}}\n")
	dir := t.TempDir()
	writeCertificate(t, filepath.Join(dir, "a.crt"), filepath.Join(dir, "a.key"))
	writeCertificate(t, filepath.Join(dir, "b.crt"), filepath.Join(dir, "b.key"))
	writeTLSConfig := func(certFile, keyFile string) string {
		return writeConfig(fmt.Sprintf("listen: 127.0.0.1:0\nlisten_tls: {cert_file: %q, key_file: %q}\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default}\n", certFile, keyFile))
	}
	missingCertificate := writeTLSConfig(filepath.Join(dir, "missing.crt"), filepath.Join(dir, "a.key"))
	mismatchedKey := writeTLSConfig(filepath.Join(dir, "a.crt"), filepath.Join(dir, "b.key"))
	tests := []struct {
		name     string
		args     []string
		exitCode int
		want     string
	}{
		{"-h", []string{"-h"}, 0, "Usage of operator-mcp"},
		{"unexpected arguments", []string{"--config", filepath.Join("testdata", "config.yaml"), "extra"}, 1, `unexpected arguments ["extra"]`},
		{"invalid config", []string{"--config", noTargets}, 1, "invalid config " + noTargets + ": "},
		{"unusable target", []string{"--config", unsetKey}, 1, "invalid config " + unsetKey + ": "},
		{"unusable codec", []string{"--config", unsetCodecAuthorization}, 1, "invalid config " + unsetCodecAuthorization + `: codecs: "c": authorization_header: environment variable OPERATOR_MCP_TEST_UNSET_KEY is unset or empty`},
		{"unreadable certificate", []string{"--config", missingCertificate}, 1, "invalid config " + missingCertificate + ": listen_tls: open " + filepath.Join(dir, "missing.crt")},
		{"key does not match certificate", []string{"--config", mismatchedKey}, 1, "invalid config " + mismatchedKey + ": listen_tls: tls: private key does not match public key"},
		{"unknown tool in a profile", []string{"--config", unknownTool}, 1, "invalid config " + unknownTool + `: authorization: profiles: "p": tools[1]: unknown tool "terminate_workflow"; the tools are list_available_targets, check_connection, get_workflow`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, tt.args...)
			cmd.Env = slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "OPERATOR_MCP_TEST_UNSET_KEY=") })
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
		"authorization:\n  profiles: {p: {tools: [get_workflow], targets: [prod]}}\n  rules: [{claim_name: sub, claim_value: alice, profile: p}]\n", issuer)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, startServer(t, buildBinary(t), configPath), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"e2e-test","version":"0"}}}`))
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
