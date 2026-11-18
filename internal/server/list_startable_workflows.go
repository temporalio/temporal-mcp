// The list_startable_workflows tool, which lists the workflows that the caller may start on
// a target with start_workflow.

package server

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/temporalio/temporal-mcp/internal/config"
)

type listStartableWorkflowsInput struct {
	Target string `json:"target" jsonschema:"the name or an alias of a target"`
}

type startableWorkflowList struct {
	Workflows []startableWorkflow `json:"workflows"`
}

type startableWorkflow struct {
	Type        string  `json:"type"`
	TaskQueue   string  `json:"task_queue"`
	Description *string `json:"description"`
	InputSchema []any   `json:"input_schema"`
}

func listStartableWorkflows(cfg *config.Config) mcp.ToolHandlerFor[listStartableWorkflowsInput, envelope[startableWorkflowList]] {
	return func(ctx context.Context, req *mcp.CallToolRequest, in listStartableWorkflowsInput) (*mcp.CallToolResult, envelope[startableWorkflowList], error) {
		var none envelope[startableWorkflowList]
		t, err := lookupTarget(ctx, cfg, in.Target)
		if err != nil {
			return nil, none, err
		}
		list := startableWorkflowList{Workflows: []startableWorkflow{}}
		for _, w := range startableWorkflows(cfg, req, t) {
			var description *string
			if w.Description != "" {
				description = &w.Description
			}
			list.Workflows = append(list.Workflows, startableWorkflow{Type: w.Type, TaskQueue: w.TaskQueue, Description: description, InputSchema: w.InputSchema})
		}
		return nil, envelope[startableWorkflowList]{Target: &t.Name, Result: list, Complete: true}, nil
	}
}
