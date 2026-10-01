// Package server exposes the MCP tools over streamable HTTP.
package server

import (
	"context"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

type envelope[T any] struct {
	Target   *string `json:"target"`
	Result   T       `json:"result"`
	Complete bool    `json:"complete"`
	NextCall any     `json:"next_call"`
}

type tool struct {
	name        string
	description string
	readOnly    bool
	rpcs        []string
	add         func(*mcp.Server)
}

func newTool[In, Out any](t tool, h mcp.ToolHandlerFor[In, Out]) tool {
	t.add = func(s *mcp.Server) {
		mcp.AddTool(s, &mcp.Tool{
			Name:        t.name,
			Description: t.description,
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: t.readOnly},
		}, func(ctx context.Context, req *mcp.CallToolRequest, in In) (*mcp.CallToolResult, Out, error) {
			return h(temporal.WithTool(ctx, t.name, t.rpcs), req, in)
		})
	}
	return t
}

func tools(cfg *config.Config, clients *temporal.Clients) []tool {
	return []tool{
		newTool(tool{
			name:        "list_available_targets",
			description: "List the Temporal targets this server can read from.",
			readOnly:    true,
		}, listAvailableTargets(cfg)),
		newTool(tool{
			name:        "check_connection",
			description: "Check that this server can reach a target's Temporal server and namespace.",
			readOnly:    true,
			rpcs: []string{
				workflowservice.WorkflowService_GetSystemInfo_FullMethodName,
				workflowservice.WorkflowService_DescribeNamespace_FullMethodName,
			},
		}, checkConnection(cfg, clients)),
	}
}

// NewHandler returns an HTTP handler that serves the MCP streamable HTTP transport
// for the given configuration, making Temporal calls through clients.
func NewHandler(name, version string, cfg *config.Config, clients *temporal.Clients) http.Handler {
	s := mcp.NewServer(&mcp.Implementation{Name: name, Version: version}, nil)
	for _, t := range tools(cfg, clients) {
		t.add(s)
	}
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, &mcp.StreamableHTTPOptions{SessionTimeout: time.Hour})
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

func listAvailableTargets(cfg *config.Config) mcp.ToolHandlerFor[struct{}, envelope[targetList]] {
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
	return func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, envelope[targetList], error) {
		return nil, envelope[targetList]{Result: targets, Complete: true}, nil
	}
}
