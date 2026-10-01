// Package server exposes the MCP tools over streamable HTTP.
package server

import (
	"context"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/temporalio/operator-mcp/internal/config"
)

type envelope[T any] struct {
	Target   *string `json:"target"`
	Result   T       `json:"result"`
	Complete bool    `json:"complete"`
	NextCall any     `json:"next_call"`
}

type targetList struct {
	Targets []targetInfo `json:"targets"`
}

type targetInfo struct {
	Name        string   `json:"name"`
	Aliases     []string `json:"aliases"`
	Namespace   string   `json:"namespace"`
	Description string   `json:"description"`
}

// NewHandler returns an HTTP handler that serves the MCP streamable HTTP transport
// for the given configuration.
func NewHandler(name, version string, cfg *config.Config) http.Handler {
	s := mcp.NewServer(&mcp.Implementation{Name: name, Version: version}, nil)

	targets := targetList{Targets: make([]targetInfo, 0, len(cfg.Targets))}
	for _, t := range cfg.Targets {
		aliases := t.Aliases
		if aliases == nil {
			aliases = []string{}
		}
		targets.Targets = append(targets.Targets, targetInfo{
			Name:        t.Name,
			Aliases:     aliases,
			Namespace:   t.Namespace,
			Description: t.Description,
		})
	}
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_available_targets",
		Description: "List the Temporal targets this server can read from.",
	}, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, envelope[targetList], error) {
		return nil, envelope[targetList]{Result: targets, Complete: true}, nil
	})

	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, &mcp.StreamableHTTPOptions{SessionTimeout: time.Hour})
}
