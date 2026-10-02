package main

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

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

func TestServerHandshake(t *testing.T) {
	cmd := exec.Command(buildBinary(t, "-ldflags", "-X main.version=9.9.9-test"), "--config", filepath.Join("testdata", "config.yaml"))
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
	unsetKey := writeConfig("listen: 127.0.0.1:0\ntargets:\n  - {name: a, description: x, address: \"a:7233\", namespace: default, api_key: {env: OPERATOR_MCP_TEST_UNSET_KEY}}\n")
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
