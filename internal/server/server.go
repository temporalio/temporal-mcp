// Package server exposes the MCP tools over streamable HTTP. With auth configured, NewMux
// puts the token check in front of /mcp and serves the protected resource metadata.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/identity"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

type envelope[T any] struct {
	Target   *string   `json:"target"`
	Result   T         `json:"result"`
	Complete bool      `json:"complete"`
	NextCall *nextCall `json:"next_call"`
}

type nextCall struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

var readOnlyAnnotations = &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true}

func addTool[In, Out any](s *mcp.Server, def *mcp.Tool, rpcs []string, h mcp.ToolHandlerFor[In, Out]) {
	if def.OutputSchema == nil {
		schema, err := jsonschema.For[Out](&jsonschema.ForOptions{})
		if err != nil {
			panic(fmt.Errorf("tool %q: output schema: %w", def.Name, err))
		}
		def.OutputSchema = schema
	}
	// The handler encodes the output itself: the SDK would re-encode it through float64, corrupting integers above 2^53.
	mcp.AddTool(s, def, func(ctx context.Context, req *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
		ctx = temporal.WithTool(ctx, def.Name, rpcs)
		if req.Extra != nil && req.Extra.TokenInfo != nil {
			ctx = identity.WithCaller(ctx, identity.CallerFromTokenInfo(req.Extra.TokenInfo))
		}
		_, out, err := h(ctx, req, in)
		if err != nil {
			return nil, nil, err
		}
		b, err := json.Marshal(out)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{StructuredContent: json.RawMessage(b), Content: []mcp.Content{&mcp.TextContent{Text: string(b)}}}, nil, nil
	})
}

func addTools(s *mcp.Server, cfg *config.Config, clients *temporal.Clients) {
	addTool(s, &mcp.Tool{
		Name:        "list_available_targets",
		Description: "List the Temporal targets this server can read from.",
		Annotations: readOnlyAnnotations,
	}, nil, listAvailableTargets(cfg))
	addTool(s, &mcp.Tool{
		Name:        "check_connection",
		Description: "Check that this server can reach a target's Temporal server and namespace.",
		Annotations: readOnlyAnnotations,
	}, []string{
		workflowservice.WorkflowService_GetSystemInfo_FullMethodName,
		workflowservice.WorkflowService_DescribeNamespace_FullMethodName,
	}, checkConnection(cfg, clients))
	addTool(s, &mcp.Tool{
		Name:         "get_workflow",
		Description:  "Get the current state of a workflow run. For a closed run, also its result if it completed, or the failure its close event records; a timed-out run has no failure, only status TimedOut.",
		Annotations:  readOnlyAnnotations,
		OutputSchema: getWorkflowOutputSchema(),
	}, []string{
		workflowservice.WorkflowService_DescribeWorkflowExecution_FullMethodName,
		workflowservice.WorkflowService_GetWorkflowExecutionHistory_FullMethodName,
	}, getWorkflow(cfg, clients))
}

// NewMux returns an HTTP handler that serves the MCP streamable HTTP transport
// at /mcp for the given configuration, making Temporal calls through clients.
// With auth configured, /mcp requires a valid access token and the protected
// resource metadata is served at the path the token challenge points to.
// logger gets the per-call log and the reason for each rejected token or
// verification failure.
func NewMux(name, version string, cfg *config.Config, clients *temporal.Clients, logger *slog.Logger) http.Handler {
	return newMux(newServer(name, version, cfg, clients, logger), cfg, logger)
}

func newMux(s *mcp.Server, cfg *config.Config, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	handler := newHandler(s, cfg)
	if cfg.Auth != nil {
		metadataPath, metadata := identity.MetadataPath(cfg.Auth.Resource), identity.MetadataHandler(cfg.Auth)
		// Matched exactly rather than as a ServeMux pattern, which would read wildcards or a method in the configured path.
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != metadataPath {
				http.NotFound(w, r)
				return
			}
			metadata.ServeHTTP(w, r)
		})
		handler = identity.RequireToken(cfg.Auth, logger, handler)
	}
	mux.Handle("/mcp", handler)
	return mux
}

// newHandler serves s statelessly: go-sdk serves MCP 2026-07-28 only on a stateless handler.
func newHandler(s *mcp.Server, cfg *config.Config) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, &mcp.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true, DisableLocalhostProtection: cfg.Auth != nil})
}

func newServer(name, version string, cfg *config.Config, clients *temporal.Clients, logger *slog.Logger) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: name, Version: version}, nil)
	s.AddReceivingMiddleware(logToolCalls(logger), normalizeToolCalls)
	addTools(s, cfg, clients)
	return s
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
