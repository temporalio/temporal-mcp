// Package server exposes the MCP tools over streamable HTTP.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
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
		inSchema, inResolved := resolvedSchema[In](t.name)
		outSchema, outResolved := resolvedSchema[Out](t.name)
		s.AddTool(&mcp.Tool{
			Name:         t.name,
			Description:  t.description,
			Annotations:  &mcp.ToolAnnotations{ReadOnlyHint: t.readOnly},
			InputSchema:  inSchema,
			OutputSchema: outSchema,
		}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			in, err := decodeArguments[In](req.Params.Arguments, inResolved)
			var res *mcp.CallToolResult
			var out Out
			if err == nil {
				res, out, err = h(temporal.WithTool(ctx, t.name, t.rpcs), req, in)
			}
			var te *toolError
			if errors.As(err, &te) {
				return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: te.Error()}}}, nil
			}
			if err != nil {
				return nil, err
			}
			b, err := json.Marshal(out)
			if err != nil {
				return nil, fmt.Errorf("marshaling output: %w", err)
			}
			var instance any
			if err := json.Unmarshal(b, &instance); err != nil {
				return nil, fmt.Errorf("unmarshaling output: %w", err)
			}
			if err := outResolved.Validate(instance); err != nil {
				return nil, fmt.Errorf("validating tool output: %w", err)
			}
			if res == nil {
				res = &mcp.CallToolResult{}
			}
			res.StructuredContent = json.RawMessage(b)
			res.Content = []mcp.Content{&mcp.TextContent{Text: string(b)}}
			return res, nil
		})
	}
	return t
}

func resolvedSchema[T any](name string) (*jsonschema.Schema, *jsonschema.Resolved) {
	schema, err := jsonschema.For[T](&jsonschema.ForOptions{})
	if err != nil {
		panic(fmt.Errorf("AddTool %q: schema: %w", name, err))
	}
	resolved, err := schema.Resolve(&jsonschema.ResolveOptions{ValidateDefaults: true})
	if err != nil {
		panic(fmt.Errorf("AddTool %q: resolving schema: %w", name, err))
	}
	return schema, resolved
}

func decodeArguments[In any](raw json.RawMessage, resolved *jsonschema.Resolved) (In, error) {
	var in In
	if raw = bytes.TrimSpace(raw); len(raw) == 0 || string(raw) == "null" {
		raw = json.RawMessage("{}")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return in, &toolError{Code: "invalid_argument", Message: "arguments must be a JSON object"}
	}
	maps.DeleteFunc(fields, func(_ string, v json.RawMessage) bool { return string(v) == "null" })
	cleaned, err := json.Marshal(fields)
	if err != nil {
		return in, &toolError{Code: "invalid_argument", Message: err.Error()}
	}
	var instance map[string]any
	if err := json.Unmarshal(cleaned, &instance); err != nil {
		return in, &toolError{Code: "invalid_argument", Message: err.Error()}
	}
	if err := resolved.Validate(instance); err != nil {
		return in, &toolError{Code: "invalid_argument", Message: err.Error()}
	}
	if err := json.Unmarshal(cleaned, &in); err != nil {
		return in, &toolError{Code: "invalid_argument", Message: err.Error()}
	}
	return in, nil
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
		newTool(tool{
			name:        "get_workflow",
			description: "Get the current state of a workflow run. For a closed run, also its result if it completed, or the failure its close event records; a timed-out run has no failure, only status TimedOut.",
			readOnly:    true,
			rpcs: []string{
				workflowservice.WorkflowService_DescribeWorkflowExecution_FullMethodName,
				workflowservice.WorkflowService_GetWorkflowExecutionHistory_FullMethodName,
			},
		}, getWorkflow(cfg, clients)),
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
