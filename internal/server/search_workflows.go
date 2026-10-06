// The search_workflows tool, which returns one page of the workflow runs that match a
// Visibility query, and the continuation that asks for the next page.

package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/grpc"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

const maxContinuation = 4096

type searchWorkflowsInput struct {
	Target       string   `json:"target" jsonschema:"the name or an alias of a target"`
	Query        string   `json:"query,omitempty" jsonschema:"a Visibility query, such as WorkflowType = 'OrderWorkflow' AND ExecutionStatus = 'Running'; omit it for every run"`
	Fields       []string `json:"fields,omitempty" jsonschema:"the search attributes to return besides workflow_id and run_id, such as ExecutionStatus or CustomerId; ExecutionDuration is in nanoseconds; WorkflowType, ExecutionStatus, StartTime and CloseTime if omitted"`
	PageSize     int      `json:"page_size,omitempty" jsonschema:"the most runs to return"`
	Continuation string   `json:"continuation,omitempty" jsonschema:"the continuation from the next_call of the previous page"`
}

type workflowSearch struct {
	Workflows []map[string]any `json:"workflows"`
}

var defaultFields = []string{"WorkflowType", "ExecutionStatus", "StartTime", "CloseTime"}

// coreSearchAttributes reads the search attributes that Temporal keeps as fields of a run rather than in its
// search attribute map, in the form Visibility queries use. Visibility reports the Int ones only for a closed run.
var coreSearchAttributes = map[string]func(*workflowpb.WorkflowExecutionInfo) any{
	"WorkflowId":      func(i *workflowpb.WorkflowExecutionInfo) any { return i.GetExecution().GetWorkflowId() },
	"RunId":           func(i *workflowpb.WorkflowExecutionInfo) any { return i.GetExecution().GetRunId() },
	"WorkflowType":    func(i *workflowpb.WorkflowExecutionInfo) any { return i.GetType().GetName() },
	"StartTime":       func(i *workflowpb.WorkflowExecutionInfo) any { return timeOf(i.GetStartTime()) },
	"ExecutionTime":   func(i *workflowpb.WorkflowExecutionInfo) any { return timeOf(i.GetExecutionTime()) },
	"CloseTime":       func(i *workflowpb.WorkflowExecutionInfo) any { return timeOf(i.GetCloseTime()) },
	"ExecutionStatus": func(i *workflowpb.WorkflowExecutionInfo) any { return i.GetStatus().String() },
	"TaskQueue":       func(i *workflowpb.WorkflowExecutionInfo) any { return i.GetTaskQueue() },
	"HistoryLength":   func(i *workflowpb.WorkflowExecutionInfo) any { return ifClosed(i, i.GetHistoryLength()) },
	"ExecutionDuration": func(i *workflowpb.WorkflowExecutionInfo) any {
		return ifClosed(i, int64(i.GetExecutionDuration().AsDuration()))
	},
	"StateTransitionCount": func(i *workflowpb.WorkflowExecutionInfo) any { return ifClosed(i, i.GetStateTransitionCount()) },
	"HistorySizeBytes":     func(i *workflowpb.WorkflowExecutionInfo) any { return ifClosed(i, i.GetHistorySizeBytes()) },
	"ParentWorkflowId":     func(i *workflowpb.WorkflowExecutionInfo) any { return ifSet(i.GetParentExecution().GetWorkflowId()) },
	"ParentRunId":          func(i *workflowpb.WorkflowExecutionInfo) any { return ifSet(i.GetParentExecution().GetRunId()) },
	"RootWorkflowId":       func(i *workflowpb.WorkflowExecutionInfo) any { return ifSet(i.GetRootExecution().GetWorkflowId()) },
	"RootRunId":            func(i *workflowpb.WorkflowExecutionInfo) any { return ifSet(i.GetRootExecution().GetRunId()) },
}

// getWorkflowNames gives the search attribute for each of get_workflow's names that has one.
var getWorkflowNames = map[string]string{
	"workflow_id":    "WorkflowId",
	"run_id":         "RunId",
	"type":           "WorkflowType",
	"status":         "ExecutionStatus",
	"task_queue":     "TaskQueue",
	"start_time":     "StartTime",
	"execution_time": "ExecutionTime",
	"close_time":     "CloseTime",
	"history_length": "HistoryLength",
	"parent":         "ParentWorkflowId and ParentRunId",
}

func ifClosed(i *workflowpb.WorkflowExecutionInfo, v int64) any {
	if i.GetCloseTime() == nil {
		return nil
	}
	return v
}

func ifSet(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func searchWorkflowsInputSchema() *jsonschema.Schema {
	schema, err := jsonschema.For[searchWorkflowsInput](nil)
	if err != nil {
		panic(fmt.Errorf("search_workflows: input schema: %w", err))
	}
	p := schema.Properties["page_size"]
	p.Minimum, p.Maximum, p.Default = new(1.0), new(100.0), json.RawMessage("25")
	return schema
}

func searchWorkflows(cfg *config.Config, clients *temporal.Clients, names *searchAttributeNames) mcp.ToolHandlerFor[searchWorkflowsInput, envelope[workflowSearch]] {
	return func(ctx context.Context, req *mcp.CallToolRequest, in searchWorkflowsInput) (*mcp.CallToolResult, envelope[workflowSearch], error) {
		t, err := lookupTarget(ctx, cfg, in.Target)
		if err != nil {
			return nil, envelope[workflowSearch]{}, err
		}
		fields := in.Fields
		if fields == nil {
			fields = defaultFields
		}
		for _, f := range fields {
			if name, ok := getWorkflowNames[f]; ok {
				return nil, envelope[workflowSearch]{}, &toolError{Code: "invalid_argument", Message: fmt.Sprintf("%q is not a search attribute; use %s", f, name)}
			}
		}
		var pageToken []byte
		if in.Continuation != "" {
			if pageToken, err = decodeContinuation(in.Continuation, t.Name, in.Query); err != nil {
				return nil, envelope[workflowSearch]{}, err
			}
		}
		if err := checkFields(ctx, req, cfg, clients, names, t, fields); err != nil {
			return nil, envelope[workflowSearch]{}, err
		}
		resp, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.ListWorkflowExecutionsResponse, error) {
			return clients.Client(t.Name).WorkflowService().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     t.Namespace,
				PageSize:      int32(in.PageSize),
				NextPageToken: pageToken,
				Query:         in.Query,
			}, opts...)
		})
		if err != nil {
			return nil, envelope[workflowSearch]{}, queryError(err)
		}
		out := envelope[workflowSearch]{Target: &t.Name, Result: workflowSearch{Workflows: []map[string]any{}}, Complete: len(resp.GetNextPageToken()) == 0}
		for _, info := range resp.GetExecutions() {
			row, err := workflowRow(info, fields)
			if err != nil {
				return nil, envelope[workflowSearch]{}, err
			}
			out.Result.Workflows = append(out.Result.Workflows, row)
		}
		if !out.Complete {
			continuation, ok := encodeContinuation(resp.GetNextPageToken(), t.Name, in.Query)
			if !ok {
				// Temporal's page token is too long to hand out, so the page is returned without a next_call and stays incomplete.
				return nil, out, nil
			}
			args := map[string]any{"target": t.Name, "query": in.Query, "page_size": in.PageSize, "continuation": continuation}
			if in.Fields != nil {
				args["fields"] = in.Fields
			}
			out.NextCall = &nextCall{Name: "search_workflows", Arguments: args}
		}
		return nil, out, nil
	}
}

// checkFields returns an invalid_argument error if a name in fields is not a search attribute on t. Core attributes,
// names already listed for t and, on Temporal Cloud, names with Temporal's reserved prefix (which may be built-ins newer
// than this server's sadefs) pass without a call; otherwise t's search attributes are fetched once, and if that fails,
// fields pass unchecked.
func checkFields(ctx context.Context, req *mcp.CallToolRequest, cfg *config.Config, clients *temporal.Clients, names *searchAttributeNames, t *config.Target, fields []string) error {
	cloud := clients.CloudOps(t.Name) != nil
	unchecked := slices.DeleteFunc(slices.Clone(fields), func(f string) bool {
		_, core := coreSearchAttributes[f]
		return core || names.has(t.Name, f) || cloud && strings.HasPrefix(f, sadefs.ReservedPrefix)
	})
	if len(unchecked) == 0 {
		return nil
	}
	custom, system, err := fetchSearchAttributes(ctx, clients, names, t)
	if err != nil {
		return nil
	}
	known := slices.Concat(slices.Collect(maps.Keys(custom)), slices.Collect(maps.Keys(system)))
	slices.Sort(known)
	var unknown []string
	for _, f := range unchecked {
		if _, found := slices.BinarySearch(known, f); found {
			continue
		}
		u := strconv.Quote(f)
		if i := slices.IndexFunc(known, func(k string) bool { return strings.EqualFold(k, f) }); i >= 0 {
			u += fmt.Sprintf(" (did you mean %s?)", known[i])
		}
		unknown = append(unknown, u)
	}
	if len(unknown) == 0 {
		return nil
	}
	msg := fmt.Sprintf("these fields are not search attributes on target %s: %s", t.Name, strings.Join(unknown, ", "))
	if mayUse(cfg, req, "list_search_attributes", t.Name) {
		msg += "; call list_search_attributes for the target's search attributes"
	} else {
		msg += "; fix or remove them"
	}
	return &toolError{Code: "invalid_argument", Message: msg}
}

func workflowRow(info *workflowpb.WorkflowExecutionInfo, fields []string) (map[string]any, error) {
	row := map[string]any{"workflow_id": info.GetExecution().GetWorkflowId(), "run_id": info.GetExecution().GetRunId()}
	for _, name := range fields {
		if value, ok := coreSearchAttributes[name]; ok {
			row[name] = value(info)
			continue
		}
		row[name] = nil
		if p, ok := info.GetSearchAttributes().GetIndexedFields()[name]; ok {
			v, err := searchAttribute(p)
			if err != nil {
				return nil, &toolError{Code: "temporal_unavailable", Message: fmt.Sprintf("the Temporal server returned search attribute %s in an unreadable form: %v", name, err)}
			}
			row[name] = v
		}
	}
	return row, nil
}

// queryError reports an invalid argument, which the Temporal server gives for a query it cannot run, as invalid_query.
func queryError(err error) error {
	if te := (*toolError)(nil); errors.As(err, &te) && te.Code == "invalid_argument" {
		te.Code = "invalid_query"
	}
	return err
}

// A continuation is the base64url of a version byte, queryDigest(target, query) and Temporal's page token. It is unsigned: a changed page token only pages through the same query on the same namespace.
const continuationVersion = 1

func queryDigest(target, query string) []byte {
	sum := sha256.Sum256([]byte(target + "\x00" + query))
	return sum[:]
}

// encodeContinuation returns the continuation for pageToken, and false if it is longer than decodeContinuation accepts.
func encodeContinuation(pageToken []byte, target, query string) (string, bool) {
	s := base64.RawURLEncoding.EncodeToString(slices.Concat([]byte{continuationVersion}, queryDigest(target, query), pageToken))
	return s, len(s) <= maxContinuation
}

func decodeContinuation(s, target, query string) ([]byte, error) {
	invalid := &toolError{Code: "continuation_invalid", Message: "continuation is not one that search_workflows returned for this target and query; search again without it"}
	if len(s) > maxContinuation {
		return nil, invalid
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	digest := queryDigest(target, query)
	if err != nil || len(b) <= 1+len(digest) || b[0] != continuationVersion || !bytes.Equal(b[1:1+len(digest)], digest) {
		return nil, invalid
	}
	return b[1+len(digest):], nil
}
