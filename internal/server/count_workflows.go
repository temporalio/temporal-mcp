// The count_workflows tool, which counts the workflow runs that match a Visibility query,
// optionally by the values of a search attribute.

package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
	"github.com/temporalio/temporal-operator-mcp/internal/temporal"
)

type countWorkflowsInput struct {
	Target  string `json:"target" jsonschema:"the name or an alias of a target"`
	Query   string `json:"query,omitempty" jsonschema:"a Visibility query, such as WorkflowType = 'OrderWorkflow'; omit it for every run"`
	GroupBy string `json:"group_by,omitempty" jsonschema:"a search attribute whose values to group the count by; Temporal currently groups only by ExecutionStatus"`
}

type workflowCount struct {
	Count  int64        `json:"count"`
	Groups []countGroup `json:"groups"`
}

type countGroup struct {
	Value any   `json:"value"`
	Count int64 `json:"count"`
}

func countWorkflows(cfg *config.Config, clients *temporal.Clients) mcp.ToolHandlerFor[countWorkflowsInput, envelope[workflowCount]] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in countWorkflowsInput) (*mcp.CallToolResult, envelope[workflowCount], error) {
		t, err := lookupTarget(ctx, cfg, in.Target)
		if err != nil {
			return nil, envelope[workflowCount]{}, err
		}
		query := in.Query
		if in.GroupBy != "" {
			query = strings.TrimSpace(query + " GROUP BY `" + in.GroupBy + "`")
		}
		resp, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.CountWorkflowExecutionsResponse, error) {
			return clients.Client(t.Name).WorkflowService().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: t.Namespace, Query: query}, opts...)
		})
		if err != nil {
			return nil, envelope[workflowCount]{}, queryError(err)
		}
		count := workflowCount{Count: resp.GetCount(), Groups: []countGroup{}}
		for _, g := range resp.GetGroups() {
			group := countGroup{Count: g.GetCount()}
			if values := g.GetGroupValues(); len(values) > 0 {
				if group.Value, err = searchAttribute(values[0]); err != nil {
					return nil, envelope[workflowCount]{}, &toolError{Code: "temporal_unavailable", Message: fmt.Sprintf("the Temporal server returned a group value in an unreadable form: %v", err)}
				}
			}
			count.Groups = append(count.Groups, group)
		}
		return nil, envelope[workflowCount]{Target: &t.Name, Result: count, Complete: true}, nil
	}
}
