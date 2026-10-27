// Package server exposes the MCP tools as an MCP server, which runs over stdio or, through
// NewMux, over streamable HTTP. With auth configured, NewMux puts the token check in front
// of /mcp, limits each caller to the tools and targets that the authorization section
// gives it, and serves the protected resource metadata.
package server

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/cloud-sdk/api/cloudservice/v1"

	"github.com/temporalio/temporal-mcp/internal/config"
	"github.com/temporalio/temporal-mcp/internal/identity"
	"github.com/temporalio/temporal-mcp/internal/payload"
	"github.com/temporalio/temporal-mcp/internal/temporal"
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

const maxPageSize = 100

// boundPageSize gives p, a property that caps how many results a call returns, a range of 1 to maxPageSize and a
// default of 25, and appends the range to p's description.
func boundPageSize(p *jsonschema.Schema) {
	p.Minimum, p.Maximum, p.Default = new(1.0), new(float64(maxPageSize)), json.RawMessage("25")
	p.Description += fmt.Sprintf(", from 1 to %d", maxPageSize)
}

// maxJSONInteger, 2^53-1, is the largest integer a JSON number holds exactly, and the maximum of an int64 argument:
// the SDK validates arguments as float64, in which math.MaxInt64 rounds up to 2^63 and overflows an int64.
const maxJSONInteger = 1<<53 - 1

// maxListItems is the most items a list argument, such as get_workflows' workflows, may have.
const maxListItems = 50

// boundList limits p, a list property, to 1 to maxListItems items, and key, the schema of each item's key, to a
// nonempty string, and appends the range of items to p's description.
func boundList(p, key *jsonschema.Schema) {
	p.MinItems, p.MaxItems, key.MinLength = new(1), new(maxListItems), new(1)
	p.Description += fmt.Sprintf(", from 1 to %d", maxListItems)
}

var readOnlyAnnotations = &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true}

// A toolServer is an MCP server that knows the access, read or write, and the input schema of each tool addTool adds
// to it.
type toolServer struct {
	*mcp.Server
	access map[string]string
	inputs map[string]*jsonschema.Schema
}

// schemaFor returns the schema inferred from T with opts, which may be nil, and panics naming tool and which schema, such as "input", if it cannot.
func schemaFor[T any](tool, which string, opts *jsonschema.ForOptions) *jsonschema.Schema {
	schema, err := jsonschema.For[T](opts)
	if err != nil {
		panic(fmt.Errorf("tool %q: %s schema: %w", tool, which, err))
	}
	return schema
}

func addTool[In, Out any](s *toolServer, def *mcp.Tool, rpcs []string, h mcp.ToolHandlerFor[In, Out]) string {
	if def.InputSchema == nil {
		def.InputSchema = schemaFor[In](def.Name, "input", nil)
	}
	if def.OutputSchema == nil {
		def.OutputSchema = schemaFor[Out](def.Name, "output", nil)
	}
	s.inputs[def.Name], _ = def.InputSchema.(*jsonschema.Schema)
	s.access[def.Name] = "write"
	if def.Annotations != nil && def.Annotations.ReadOnlyHint {
		s.access[def.Name] = "read"
	}
	// The handler encodes the output itself: the SDK would re-encode it through float64, corrupting integers above 2^53.
	mcp.AddTool(s.Server, def, func(ctx context.Context, req *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
		ctx = temporal.WithTool(ctx, def.Name, rpcs, toolTargets(ctx))
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
	return def.Name
}

// addTools registers the server's tools on s and returns their names.
func addTools(s *toolServer, cfg *config.Config, clients *temporal.Clients, payloads *payload.Renderer) []string {
	names := &searchAttributeNames{}
	return []string{addTool(s, &mcp.Tool{
		Name:        "list_available_targets",
		Description: "List the Temporal targets you can read from, and how tool responses show each one's payloads.",
		Annotations: readOnlyAnnotations,
	}, nil, listAvailableTargets(cfg)), addTool(s, &mcp.Tool{
		Name:        "check_connection",
		Description: "Check that this server can reach a target's Temporal server and namespace, and its codec server if it decodes payloads with one.",
		Annotations: readOnlyAnnotations,
	}, []string{
		workflowservice.WorkflowService_GetSystemInfo_FullMethodName,
		workflowservice.WorkflowService_DescribeNamespace_FullMethodName,
	}, checkConnection(cfg, clients, payloads)), addTool(s, &mcp.Tool{
		Name:         "get_workflows",
		Description:  fmt.Sprintf("Get the current state of up to %d workflow runs: for each item of workflows, the run its run_id names, or the workflow's latest run if run_id is omitted. For a closed run, also its result if it completed, or the failure its close event records; a timed-out run has no failure, only status TimedOut. result has an entry for each item of workflows, in the order given. An item that the Temporal server reports not found or invalid, such as an unknown workflow or a malformed run_id, gets an error in its entry and no run, while the other entries are unaffected. Any other error reading an item fails the whole call, and its message starts with the item's path, such as workflows[3].", maxListItems),
		Annotations:  readOnlyAnnotations,
		InputSchema:  getWorkflowsInputSchema(),
		OutputSchema: getWorkflowsOutputSchema(),
	}, []string{
		workflowservice.WorkflowService_DescribeWorkflowExecution_FullMethodName,
		workflowservice.WorkflowService_GetWorkflowExecutionHistory_FullMethodName,
	}, getWorkflows(cfg, clients, payloads)), addTool(s, &mcp.Tool{
		Name:        "get_workflow_history",
		Description: "Get the events of a workflow run's history, oldest first unless order is newest_first, each with its event_id, event_time, event_type, attributes and links; run_id in the result is the run read. links point to related executions, such as the workflow run a Nexus operation started; read a linked run in another namespace with get_workflows on the target for that namespace (see list_available_targets). event_types (event types, or the category failures: every event type that ends in Failed or TimedOut) and event_range filter the events, and combine. A call can return fewer than limit events, even none, while more events still match: until complete is true, call next_call's tool with its arguments unchanged for the rest.",
		Annotations: readOnlyAnnotations,
		InputSchema: getWorkflowHistoryInputSchema(),
	}, []string{
		workflowservice.WorkflowService_DescribeWorkflowExecution_FullMethodName,
		workflowservice.WorkflowService_GetWorkflowExecutionHistory_FullMethodName,
		workflowservice.WorkflowService_GetWorkflowExecutionHistoryReverse_FullMethodName,
	}, getWorkflowHistory(cfg, clients, payloads)), addTool(s, &mcp.Tool{
		Name:         "get_task_queues",
		Description:  fmt.Sprintf("Get the pollers and backlog of up to %d task queues, with a section in each for every task queue type asked for: up to %d of the type's pollers, most recently polled first, and poller_count, the number of pollers (complete is false when any section's list of pollers is cut); the approximate backlog count and the age of its oldest task; and the tasks added and dispatched per second, averaged over about the last 30 seconds. These stats are null if the Temporal server does not report them. A task queue that no worker polls and no task was added to gets sections with no pollers and no backlog, not an error. result has an entry for each name in task_queues, in the order given. A name that the Temporal server rejects as invalid, such as one that is too long, gets an error in its entry and no sections, while the other entries are unaffected. Any other error reading a task queue fails the whole call, and its message starts with the task queue's path, such as task_queues[3].", maxListItems, maxPollers),
		Annotations:  readOnlyAnnotations,
		InputSchema:  getTaskQueuesInputSchema(),
		OutputSchema: getTaskQueuesOutputSchema(),
	}, []string{
		workflowservice.WorkflowService_DescribeTaskQueue_FullMethodName,
	}, getTaskQueues(cfg, clients)), addTool(s, &mcp.Tool{
		Name:        "search_workflows",
		Description: "Search a target's workflow runs with a Visibility query (every run without one) and return a page of them. Each run has workflow_id, run_id and the search attributes asked for in fields, which uses the same names as query (see list_search_attributes); one the run lacks is null (HistoryLength, HistorySizeBytes, StateTransitionCount and ExecutionDuration are null until the run closes). For the next page, call next_call's tool with its arguments unchanged.",
		Annotations: readOnlyAnnotations,
		InputSchema: searchWorkflowsInputSchema(),
	}, []string{
		workflowservice.WorkflowService_ListWorkflowExecutions_FullMethodName,
		operatorservice.OperatorService_ListSearchAttributes_FullMethodName,
		cloudservice.CloudService_GetNamespace_FullMethodName,
	}, searchWorkflows(cfg, clients, names)), addTool(s, &mcp.Tool{
		Name:        "count_workflows",
		Description: "Count a target's workflow runs that match a Visibility query (every run without one), and with group_by, how many have each value of that search attribute. Counts are approximate.",
		Annotations: readOnlyAnnotations,
	}, []string{
		workflowservice.WorkflowService_CountWorkflowExecutions_FullMethodName,
	}, countWorkflows(cfg, clients)), addTool(s, &mcp.Tool{
		Name:        "list_search_attributes",
		Description: "List the search attributes that a target's Visibility queries can use, with their types. complete is false when the target's custom search attributes cannot be listed.",
		Annotations: readOnlyAnnotations,
	}, []string{
		operatorservice.OperatorService_ListSearchAttributes_FullMethodName,
		cloudservice.CloudService_GetNamespace_FullMethodName,
	}, listSearchAttributes(cfg, clients, names))}
}

// NewServer returns the MCP server for the given configuration, making
// Temporal calls through clients and rendering the payloads in tool responses
// with payloads. Without auth, every caller may use every tool on every target;
// with it, each caller may use only what cfg's authorization gives it.
// records gets an audit record of each tool call, as a line of JSON; a call
// whose record cannot be written fails. logger gets the cause of each internal
// error.
// Cancelling subscriptions ends subscription listeners without cancelling tool calls.
// Once ctx is done, the server cancels each request in progress and each
// request it gets, whatever its MCP protocol version, so tool calls end with
// outcome cancelled.
// It fails if cfg has an auth section but no authorization, if an
// authorization profile names a tool the server does not have, or if a
// profile's ToolsDeny leaves it no tools. It sets the Tools of each
// authorization profile without them to the server's read-only tools other
// than those in the profile's ToolsDeny.
func NewServer(ctx, subscriptions context.Context, name, version string, cfg *config.Config, clients *temporal.Clients, payloads *payload.Renderer, records io.Writer, logger *slog.Logger) (*mcp.Server, error) {
	if cfg.Auth != nil && cfg.Authorization == nil {
		return nil, errors.New("authorization: required with an auth section")
	}
	s, tools := newServer(ctx, subscriptions, name, version, cfg, clients, payloads, records, logger)
	if err := resolveProfileTools(cfg.Authorization, tools, s.access); err != nil {
		return nil, err
	}
	return s.Server, nil
}

// NewMux returns an HTTP handler that serves s, a [NewServer] server for cfg,
// over the MCP streamable HTTP transport at /mcp. With auth configured, /mcp
// requires a valid access token, logger gets the reason for each rejected token
// or verification failure, and the protected resource metadata is served at the
// path the token challenge points to.
func NewMux(s *mcp.Server, cfg *config.Config, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	handler := newHandler(s, cfg)
	if cfg.Auth != nil {
		metadataPath, metadata := identity.MetadataPath(cfg.Auth.Resource), identity.MetadataHandler(cfg.Auth)
		// The TypeScript SDK drops the trailing slash when it builds the metadata path itself, so that path is served too.
		trimmedPath := strings.TrimSuffix(metadataPath, "/")
		// Matched exactly rather than as a ServeMux pattern, which would read wildcards or a method in the configured path.
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != metadataPath && r.URL.Path != trimmedPath {
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

// newServer returns the MCP server, which cancels its requests once ctx is done, and the names of its tools.
func newServer(ctx, subscriptions context.Context, name, version string, cfg *config.Config, clients *temporal.Clients, payloads *payload.Renderer, records io.Writer, logger *slog.Logger) (*toolServer, []string) {
	s := &toolServer{mcp.NewServer(&mcp.Implementation{Name: name, Version: version}, nil), map[string]string{}, map[string]*jsonschema.Schema{}}
	s.AddReceivingMiddleware(recoverPanics(logger), cancelWith(ctx, subscriptions), audit(records, logger, cfg, s.access), normalizeToolCalls(s.inputs), authorize(cfg))
	return s, addTools(s, cfg, clients, payloads)
}

type targetList struct {
	Targets []targetInfo `json:"targets"`
}

type targetInfo struct {
	Name            string   `json:"name"`
	Aliases         []string `json:"aliases"`
	Namespace       string   `json:"namespace"`
	Description     string   `json:"description"`
	PayloadHandling string   `json:"payload_handling" jsonschema:"how tool responses show this target's payloads, such as workflow inputs and results: omit leaves their contents out; passthrough shows them as Temporal stores them, which may be encrypted or compressed; decode shows them as the target's codec server decodes them. Whoever runs this server sets it for each target"`
}

func listAvailableTargets(cfg *config.Config) mcp.ToolHandlerFor[struct{}, envelope[targetList]] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, envelope[targetList], error) {
		targets := targetList{Targets: []targetInfo{}}
		for _, t := range cfg.Targets {
			if !slices.Contains(toolTargets(ctx), t.Name) {
				continue
			}
			aliases := t.Aliases
			if aliases == nil {
				aliases = []string{}
			}
			targets.Targets = append(targets.Targets, targetInfo{
				Name:            t.Name,
				Aliases:         aliases,
				Namespace:       t.Namespace,
				Description:     t.Description,
				PayloadHandling: cmp.Or(t.PayloadHandling, config.PayloadOmit),
			})
		}
		return nil, envelope[targetList]{Result: targets, Complete: true}, nil
	}
}
