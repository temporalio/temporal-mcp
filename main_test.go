package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
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
