package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

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
	prefix := name + ": listening on "
	timeout := time.After(10 * time.Second)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("server exited before listening")
			}
			t.Log(line)
			if url, found := strings.CutPrefix(line, prefix); found {
				go func() {
					for range lines {
					}
				}()
				return url
			}
		case <-timeout:
			t.Fatal("timed out waiting for server to listen")
		}
	}
}

func TestVersionIsSetAtLinkTime(t *testing.T) {
	url := startServer(t, buildBinary(t, "-ldflags", "-X main.version=9.9.9-test"), filepath.Join("testdata", "config.yaml"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "e2e-test", Version: "0"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: url}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	if got := session.InitializeResult().ServerInfo.Version; got != "9.9.9-test" {
		t.Errorf("server version = %q, want %q", got, "9.9.9-test")
	}
}

func TestListAvailableTargets(t *testing.T) {
	url := startServer(t, buildBinary(t), filepath.Join("testdata", "config.yaml"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "e2e-test", Version: "0"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: url}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tool := range tools.Tools {
		names = append(names, tool.Name)
	}
	if want := []string{"check_connection", "list_available_targets"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("tools = %v, want %v", names, want)
	}

	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "list_available_targets"})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("tool returned an error: %+v", res.Content)
	}

	want := map[string]any{
		"target": nil,
		"result": map[string]any{
			"targets": []any{
				map[string]any{
					"name":        "prod",
					"aliases":     []any{"production", "prod-us"},
					"namespace":   "payments.abc12",
					"description": "Production payments namespace.",
				},
				map[string]any{
					"name":        "staging",
					"aliases":     []any{},
					"namespace":   "default",
					"description": "Staging namespace on a local dev server.",
				},
			},
		},
		"complete":  true,
		"next_call": nil,
	}
	if !reflect.DeepEqual(res.StructuredContent, want) {
		t.Errorf("structuredContent = %#v\nwant %#v", res.StructuredContent, want)
	}

	if len(res.Content) != 1 {
		t.Fatalf("got %d content items, want 1", len(res.Content))
	}
	text, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content is %T, want *mcp.TextContent", res.Content[0])
	}
	var fromText any
	if err := json.Unmarshal([]byte(text.Text), &fromText); err != nil {
		t.Fatalf("text content is not JSON: %v", err)
	}
	if !reflect.DeepEqual(fromText, res.StructuredContent) {
		t.Errorf("text content %s does not match structuredContent %#v", text.Text, res.StructuredContent)
	}
}

func TestCheckConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	exe, _ := exec.LookPath("temporal")
	dev, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{ExistingPath: exe})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Stop() })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unreachable := ln.Addr().String()
	ln.Close()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	config := fmt.Sprintf("listen: 127.0.0.1:0\ntargets:\n"+
		"  - {name: dev, aliases: [local], description: x, address: %q, namespace: default}\n"+
		"  - {name: down, description: x, address: %q, namespace: default}\n"+
		"  - {name: nons, description: x, address: %q, namespace: does-not-exist}\n", dev.FrontendHostPort(), unreachable, dev.FrontendHostPort())
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	url := startServer(t, buildBinary(t), configPath)

	client := mcp.NewClient(&mcp.Implementation{Name: "e2e-test", Version: "0"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: url}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	tests := []struct {
		target string
		want   map[string]any
	}{
		{
			target: "LOCAL",
			want: map[string]any{
				"target": "dev",
				"result": map[string]any{"stages": []any{
					map[string]any{"name": "server", "status": "ok", "reason": nil},
					map[string]any{"name": "namespace", "status": "ok", "reason": nil},
				}},
				"complete":  true,
				"next_call": nil,
			},
		},
		{
			target: "down",
			want: map[string]any{
				"target": "down",
				"result": map[string]any{"stages": []any{
					map[string]any{"name": "server", "status": "failed", "reason": "Unavailable: could not connect to the Temporal server"},
					map[string]any{"name": "namespace", "status": "skipped", "reason": "not run because the server stage failed"},
				}},
				"complete":  true,
				"next_call": nil,
			},
		},
		{
			target: "nons",
			want: map[string]any{
				"target": "nons",
				"result": map[string]any{"stages": []any{
					map[string]any{"name": "server", "status": "ok", "reason": nil},
					map[string]any{"name": "namespace", "status": "failed", "reason": "NotFound: Namespace does-not-exist is not found."},
				}},
				"complete":  true,
				"next_call": nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "check_connection", Arguments: map[string]any{"target": tt.target}})
			if err != nil {
				t.Fatal(err)
			}
			if res.IsError {
				t.Fatalf("tool returned an error: %+v", res.Content)
			}
			text := res.Content[0].(*mcp.TextContent).Text
			for _, addr := range []string{dev.FrontendHostPort(), unreachable} {
				if strings.Contains(text, addr) {
					t.Errorf("result %s contains the address %s", text, addr)
				}
			}
			got := res.StructuredContent.(map[string]any)
			for _, s := range got["result"].(map[string]any)["stages"].([]any) {
				stage := s.(map[string]any)
				latency, ran := stage["latency_ms"].(float64)
				if wantRan := stage["status"] != "skipped"; ran != wantRan || latency < 0 {
					t.Errorf("stage %v latency_ms = %v", stage["name"], stage["latency_ms"])
				}
				delete(stage, "latency_ms")
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("structuredContent = %#v\nwant %#v", got, tt.want)
			}
		})
	}

	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "check_connection", Arguments: map[string]any{"target": "nope"}})
	if err != nil {
		t.Fatal(err)
	}
	if text := res.Content[0].(*mcp.TextContent).Text; !res.IsError || !strings.Contains(text, `unknown target "nope"`) {
		t.Errorf("isError = %v, content %q, want an unknown target error", res.IsError, text)
	}
}

func TestHelpExitsZero(t *testing.T) {
	bin := buildBinary(t)
	for _, flag := range []string{"-h", "--help"} {
		t.Run(flag, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, bin, flag).CombinedOutput()
			if err != nil {
				t.Fatalf("err = %v, want exit code 0\n%s", err, out)
			}
			if !strings.Contains(string(out), "Usage of operator-mcp") {
				t.Errorf("output %q does not contain the usage text", out)
			}
			if strings.Contains(string(out), "help requested") {
				t.Errorf("output %q contains %q", out, "help requested")
			}
		})
	}
}

func TestUnexpectedArgumentsRefusesToStart(t *testing.T) {
	bin := buildBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "--config", filepath.Join("testdata", "config.yaml"), "extra").CombinedOutput()
	if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 1 {
		t.Fatalf("err = %v, want exit code 1\n%s", err, out)
	}
	if want := `unexpected arguments ["extra"]`; !strings.Contains(string(out), want) {
		t.Errorf("output %q does not contain %q", out, want)
	}
}

func TestInvalidConfigRefusesToStart(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("listen: 0.0.0.0:0\ntargets: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(buildBinary(t), "--config", configPath).CombinedOutput()
	if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() == 0 {
		t.Fatalf("err = %v, want a non-zero exit", err)
	}
	for _, want := range []string{"invalid config", "loopback", "at least one target"} {
		if !strings.Contains(string(out), want) {
			t.Errorf("output %q does not contain %q", out, want)
		}
	}
}

func TestStartupConfigErrors(t *testing.T) {
	tests := []struct {
		name, target, want string
	}{
		{
			name:   "unparseable address",
			target: `{name: a, description: x, address: "%zz.example:7233", namespace: default}`,
			want:   "targets[0]: address:",
		},
		{
			name:   "unset API key variable",
			target: `{name: a, description: x, address: "a:7233", namespace: default, api_key: {env: OPERATOR_MCP_TEST_UNSET_KEY}}`,
			want:   "targets[0]: api_key: environment variable OPERATOR_MCP_TEST_UNSET_KEY is unset or empty",
		},
	}
	bin := buildBinary(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(configPath, []byte("listen: 127.0.0.1:0\ntargets:\n  - "+tt.target+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, bin, "--config", configPath)
			cmd.Env = slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "OPERATOR_MCP_TEST_UNSET_KEY=") })
			out, err := cmd.CombinedOutput()
			if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() == 0 {
				t.Fatalf("err = %v, want a non-zero exit\n%s", err, out)
			}
			if want := "invalid config " + configPath + ": " + tt.want; !strings.Contains(string(out), want) {
				t.Errorf("output %q does not contain %q", out, want)
			}
		})
	}
}
