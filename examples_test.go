package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/server"
)

// TestExamplesLoad loads each config in examples/, those named stdio*.yaml as
// for --stdio and the others as for HTTP, and builds its MCP server, which checks
// the tool names in authorization profiles.
func TestExamplesLoad(t *testing.T) {
	paths, err := filepath.Glob("examples/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no examples found")
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			load := config.Load
			switch base := filepath.Base(path); {
			case strings.HasPrefix(base, "stdio"):
				load = config.LoadStdio
			case base == "helm-values.yaml":
				path = helmConfig(t, path)
			}
			cfg, err := load(path)
			if err != nil {
				t.Fatal(err)
			}
			// The tools use the clients and payload renderer only when called.
			if _, err := server.NewServer(t.Context(), t.Context(), serverName, version, cfg, nil, nil, io.Discard, slog.New(slog.DiscardHandler)); err != nil {
				t.Error(err)
			}
		})
	}
}

func helmConfig(t *testing.T, path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var values struct{ Config string }
	if err := yaml.Unmarshal(data, &values); err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(values.Config), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
