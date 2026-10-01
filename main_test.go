package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), name)
	out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput()
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
	if want := []string{"list_available_targets"}; !reflect.DeepEqual(names, want) {
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
