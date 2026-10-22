package main

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.yaml.in/yaml/v3"
)

func TestScenarioTargetMcpStartsServer(t *testing.T) {
	dir := t.TempDir()
	root, server, err := buildServer(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	config, err := writeConfig(dir, "127.0.0.1:7233")
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{"Server": server, "Config": config, "Root": root, "Deployed": "2026-01-02T03:04:05Z", "IncidentStart": "2026-01-02T03:04:05Z", "IncidentEnd": "2026-01-02T03:44:05Z"}
	entries, err := scenarios.ReadDir("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Run(e.Name(), func(t *testing.T) {
			tmpl, err := parseScenario(e.Name(), "")
			if err != nil {
				t.Fatal(err)
			}
			var rendered bytes.Buffer
			if err := tmpl.Execute(&rendered, values); err != nil {
				t.Fatal(err)
			}
			var scenario struct {
				Session struct {
					TargetMcp string `yaml:"targetMcp"`
				}
			}
			if err := yaml.Unmarshal(rendered.Bytes(), &scenario); err != nil {
				t.Fatal(err)
			}
			var target struct {
				Command string
				Args    []string
				Cwd     string
			}
			if err := json.Unmarshal([]byte(scenario.Session.TargetMcp), &target); err != nil {
				t.Fatalf("targetMcp: %v\n%s", err, scenario.Session.TargetMcp)
			}
			cmd := exec.CommandContext(t.Context(), target.Command, target.Args...)
			cmd.Dir = target.Cwd
			session, err := mcp.NewClient(&mcp.Implementation{Name: "acceptance-test", Version: "0"}, nil).Connect(t.Context(), &mcp.CommandTransport{Command: cmd}, nil)
			if err != nil {
				t.Fatal(err)
			}
			session.Close()
		})
	}
}

func TestAgentCommandGetsScenarioWithoutTemporalEnv(t *testing.T) {
	t.Setenv("TEMPORAL_ADDRESS", "127.0.0.1:7233")
	t.Setenv("TEMPORAL_API_KEY", "key")
	cmd := agentCommand(t.Context(), []string{"agent", "run", "{scenario}", "--out", "runs"}, "/tmp/session-1.yaml")
	if want := []string{"agent", "run", "/tmp/session-1.yaml", "--out", "runs"}; !slices.Equal(cmd.Args, want) {
		t.Errorf("args = %q, want %q", cmd.Args, want)
	}
	for _, kv := range cmd.Environ() {
		if strings.HasPrefix(kv, "TEMPORAL_") {
			t.Errorf("env has %s", kv)
		}
	}
}
