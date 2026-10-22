// The search_workflows tool, which returns one page of the workflow runs that match a
// Visibility query, and the continuation that asks for the next page.

package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
	"github.com/temporalio/temporal-operator-mcp/internal/temporal"
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

// coreSearchAttributes reads workflow_id and run_id, which every row has, and the search attributes that Temporal
// keeps as fields of a run rather than in its search attribute map, in the form Visibility queries use. Visibility
// reports the Int ones only for a closed run.
var coreSearchAttributes = map[string]func(*workflowpb.WorkflowExecutionInfo) any{
	"workflow_id":     func(i *workflowpb.WorkflowExecutionInfo) any { return i.GetExecution().GetWorkflowId() },
	"run_id":          func(i *workflowpb.WorkflowExecutionInfo) any { return i.GetExecution().GetRunId() },
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
	schema := schemaFor[searchWorkflowsInput]("search_workflows", "input", nil)
	boundPageSize(schema.Properties["page_size"])
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
		var pageToken []byte
		if in.Continuation != "" {
			if pageToken, err = decodeContinuation(in.Continuation, "search_workflows", t.Name, in.Query); err != nil {
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
			if in.Continuation != "" {
				return nil, envelope[workflowSearch]{}, continuationError(err, "search_workflows")
			}
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
			continuation, ok := encodeContinuation(resp.GetNextPageToken(), "search_workflows", t.Name, in.Query)
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

// checkFields returns an invalid_argument error naming, in the order given, each name in fields that is not
// workflow_id, run_id or a search attribute on t. Names in coreSearchAttributes, names already listed for t and, on a
// Temporal Cloud target with an API key, names with Temporal's reserved prefix (which may be built-ins newer than this
// server's sadefs) need no call; otherwise t's search attributes are listed once, and if the listing fails or is
// incomplete, every name passes unchecked.
func checkFields(ctx context.Context, req *mcp.CallToolRequest, cfg *config.Config, clients *temporal.Clients, names *searchAttributeNames, t *config.Target, fields []string) error {
	hasCloudOps := clients.CloudOps(t.Name) != nil
	passes := func(f string) bool {
		_, core := coreSearchAttributes[f]
		return core || names.has(t.Name, f) || hasCloudOps && strings.HasPrefix(f, sadefs.ReservedPrefix)
	}
	if !slices.ContainsFunc(fields, func(f string) bool { return !passes(f) }) {
		return nil
	}
	// fetchSearchAttributes stores t's names, so passes now checks fields against this listing.
	if _, _, complete, err := fetchSearchAttributes(ctx, clients, names, t); err != nil || !complete {
		return nil
	}
	var bad []string
	for _, f := range fields {
		if passes(f) {
			continue
		}
		bad = append(bad, strconv.Quote(f))
	}
	if len(bad) == 0 {
		return nil
	}
	msg := fmt.Sprintf("these fields are not search attributes on target %s: %s", t.Name, strings.Join(bad, ", "))
	if mayUse(cfg, req, "list_search_attributes", t.Name) {
		msg += "; call list_search_attributes for the target's search attributes"
	} else {
		msg += "; fix or remove them"
	}
	return &toolError{Code: "invalid_argument", Message: msg}
}

func workflowRow(info *workflowpb.WorkflowExecutionInfo, fields []string) (map[string]any, error) {
	row := map[string]any{}
	for _, name := range slices.Concat([]string{"workflow_id", "run_id"}, fields) {
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

// A continuation is the base64url of a version byte, the digest of the tool and the arguments it is bound to, and
// the position to resume from, such as Temporal's page token. It is unsigned: a changed position only resumes the
// same tool with the same arguments on the same namespace.
const continuationVersion = 1

func continuationDigest(tool string, binding []string) []byte {
	sum := sha256.Sum256([]byte(strings.Join(slices.Concat([]string{tool}, binding), "\x00")))
	return sum[:]
}

// encodeContinuation returns tool's continuation for position, and false if it is longer than decodeContinuation accepts.
func encodeContinuation(position []byte, tool string, binding ...string) (string, bool) {
	s := base64.RawURLEncoding.EncodeToString(slices.Concat([]byte{continuationVersion}, continuationDigest(tool, binding), position))
	return s, len(s) <= maxContinuation
}

func decodeContinuation(s, tool string, binding ...string) ([]byte, error) {
	if len(s) > maxContinuation {
		return nil, invalidContinuation(tool)
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	digest := continuationDigest(tool, binding)
	if err != nil || len(b) <= 1+len(digest) || b[0] != continuationVersion || !bytes.Equal(b[1:1+len(digest)], digest) {
		return nil, invalidContinuation(tool)
	}
	return b[1+len(digest):], nil
}

func invalidContinuation(tool string) *toolError {
	return &toolError{Code: "continuation_invalid", Message: fmt.Sprintf("continuation is not one that %s returned for these arguments; call %s again without it", tool, tool)}
}

// continuationError reports an invalid argument, which the Temporal server gives for a page token it cannot read, as
// tool's continuation_invalid.
func continuationError(err error, tool string) error {
	if te := (*toolError)(nil); errors.As(err, &te) && te.Code == "invalid_argument" {
		return invalidContinuation(tool)
	}
	return err
}
