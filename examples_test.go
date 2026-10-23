package main

import (
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

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
			if strings.HasPrefix(filepath.Base(path), "stdio") {
				load = config.LoadStdio
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
