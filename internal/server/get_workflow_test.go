package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/operatorservice/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/searchattribute/sadefs"

	"github.com/temporalio/operator-mcp/internal/fixture"
)

func describeWithSearchAttributes(fields map[string]*commonpb.Payload) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: fields},
		},
	}
}

func typedPayload(t *testing.T, value any, typ string) *commonpb.Payload {
	t.Helper()
	p, err := converter.GetDefaultDataConverter().ToPayload(value)
	if err != nil {
		t.Fatal(err)
	}
	if typ != "" {
		p.Metadata["type"] = []byte(typ)
	}
	return p
}

func TestDescribedRunDecodesSearchAttributesByType(t *testing.T) {
	tests := []struct {
		typ   string
		value any
		want  string
	}{
		{"", "untyped", `"untyped"`},
		{"Unknown", 7, `7`},
	}
	fields := map[string]*commonpb.Payload{}
	for _, tt := range tests {
		fields["attr"+tt.typ] = typedPayload(t, tt.value, tt.typ)
	}
	run, err := describedRun(describeWithSearchAttributes(fields))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		got, err := json.Marshal(run.SearchAttributes["attr"+tt.typ])
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tt.want {
			t.Errorf("type %q: search attribute = %s, want %s", tt.typ, got, tt.want)
		}
	}
}

func TestDescribedRunRejectsUndecodableSearchAttribute(t *testing.T) {
	p := typedPayload(t, "not a number", "Int")
	_, err := describedRun(describeWithSearchAttributes(map[string]*commonpb.Payload{"Count": p}))
	var got *toolError
	if !errors.As(err, &got) || got.Code != "temporal_unavailable" {
		t.Errorf("err = %v, want a temporal_unavailable tool error", err)
	}
}

func TestToFailurePassesThroughEncodedAttributesAndLastHeartbeatDetails(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	encoded, err := dc.ToPayload("secret")
	if err != nil {
		t.Fatal(err)
	}
	heartbeat, err := dc.ToPayloads("beat")
	if err != nil {
		t.Fatal(err)
	}
	marker := func(data string) payload {
		return payload{Handling: "passthrough", Encoding: "json/plain", Data: base64.StdEncoding.EncodeToString([]byte(data))}
	}
	secret := marker(`"secret"`)
	beat := []payload{marker(`"beat"`)}

	tests := []struct {
		name          string
		in            *failurepb.Failure
		wantEncoded   *payload
		wantHeartbeat []payload
	}{
		{
			name: "encoded attributes",
			in: &failurepb.Failure{
				EncodedAttributes: encoded,
				FailureInfo:       &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "T"}},
			},
			wantEncoded:   &secret,
			wantHeartbeat: []payload{},
		},
		{
			name: "timeout",
			in: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{LastHeartbeatDetails: heartbeat}},
			},
			wantHeartbeat: beat,
		},
		{
			name: "reset workflow",
			in: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ResetWorkflowFailureInfo{ResetWorkflowFailureInfo: &failurepb.ResetWorkflowFailureInfo{LastHeartbeatDetails: heartbeat}},
			},
			wantHeartbeat: beat,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toFailure(tt.in)
			if !reflect.DeepEqual(got.EncodedAttributes, tt.wantEncoded) {
				t.Errorf("EncodedAttributes = %#v, want %#v", got.EncodedAttributes, tt.wantEncoded)
			}
			if !reflect.DeepEqual(got.LastHeartbeatDetails, tt.wantHeartbeat) {
				t.Errorf("LastHeartbeatDetails = %#v, want %#v", got.LastHeartbeatDetails, tt.wantHeartbeat)
			}
		})
	}
}

func continueOnce(ctx workflow.Context, continued bool) (bool, error) {
	if !continued {
		return false, workflow.NewContinueAsNewError(ctx, continueOnce, true)
	}
	return true, nil
}

func waitForSignal(ctx workflow.Context) error {
	workflow.GetSignalChannel(ctx, "release").Receive(ctx, nil)
	return nil
}

func parentOfWaiting(ctx workflow.Context) error {
	return workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: "order-child"}), waitForSignal).Get(ctx, nil)
}

func heartbeatThenWait(ctx context.Context) error {
	activity.RecordHeartbeat(ctx, "progress-42")
	<-ctx.Done()
	return ctx.Err()
}

func runHeartbeating(ctx workflow.Context) error {
	return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Hour}), heartbeatThenWait).Get(ctx, nil)
}

func rejectWithDetails(workflow.Context) error {
	return temporal.NewApplicationError("order rejected", "OrderRejected", "detail-1")
}

func waitForDescribe(ctx context.Context, t *testing.T, c client.Client, workflowID, what string, done func(*workflowservice.DescribeWorkflowExecutionResponse) bool) *workflowservice.DescribeWorkflowExecutionResponse {
	t.Helper()
	for {
		desc, err := c.DescribeWorkflowExecution(ctx, workflowID, "")
		if err != nil {
			t.Fatal(err)
		}
		if done(desc) {
			return desc
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func passthrough(data string) map[string]any {
	return map[string]any{"handling": "passthrough", "encoding": "json/plain", "data": base64.StdEncoding.EncodeToString([]byte(data))}
}

func TestToolsOnDevServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	exe, _ := exec.LookPath("temporal")
	dev, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{ExistingPath: exe, ExtraArgs: []string{
		"--search-attribute", "OrderId=Keyword",
		"--search-attribute", "SaInt=Int",
		"--search-attribute", "SaDouble=Double",
		"--search-attribute", "SaBool=Bool",
		"--search-attribute", "SaKeyword=Keyword",
		"--search-attribute", "SaText=Text",
		"--search-attribute", "SaDatetime=Datetime",
		"--search-attribute", "SaKeywordList=KeywordList",
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Stop() })
	c := dev.Client()

	w := fixture.NewWorker(c)
	w.RegisterWorkflow(continueOnce)
	w.RegisterWorkflow(waitForSignal)
	w.RegisterWorkflow(parentOfWaiting)
	w.RegisterWorkflow(runHeartbeating)
	w.RegisterWorkflow(rejectWithDetails)
	w.RegisterActivity(heartbeatThenWait)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)

	failed, err := fixture.StartFailedOrder(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if err := failed.Get(ctx, nil); err == nil {
		t.Fatal("order-88231 completed, want it to fail")
	}

	start := func(id string, memo map[string]any, wf any, args ...any) client.WorkflowRun {
		t.Helper()
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: fixture.TaskQueue, Memo: memo}, wf, args...)
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	completed := start("order-completed", map[string]any{"note": "gift"}, fixture.OrderWorkflow, fixture.OrderInput{OrderID: "1"})
	if err := completed.Get(ctx, nil); err != nil {
		t.Fatal(err)
	}
	savedAt := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	typed, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "order-typed",
		TaskQueue: fixture.TaskQueue,
		TypedSearchAttributes: temporal.NewSearchAttributes(
			temporal.NewSearchAttributeKeyInt64("SaInt").ValueSet(9007199254740993),
			temporal.NewSearchAttributeKeyFloat64("SaDouble").ValueSet(1.5),
			temporal.NewSearchAttributeKeyBool("SaBool").ValueSet(true),
			temporal.NewSearchAttributeKeyKeyword("SaKeyword").ValueSet("k"),
			temporal.NewSearchAttributeKeyString("SaText").ValueSet("some text"),
			temporal.NewSearchAttributeKeyTime("SaDatetime").ValueSet(savedAt),
			temporal.NewSearchAttributeKeyKeywordList("SaKeywordList").ValueSet([]string{"a", "b"}),
		),
	}, fixture.OrderWorkflow, fixture.OrderInput{OrderID: "3"})
	if err != nil {
		t.Fatal(err)
	}
	if err := typed.Get(ctx, nil); err != nil {
		t.Fatal(err)
	}
	firstRunID := start("continue-once", nil, continueOnce, false).GetRunID()
	if err := c.GetWorkflow(ctx, "continue-once", "").Get(ctx, nil); err != nil {
		t.Fatal(err)
	}
	latest, err := c.DescribeWorkflowExecution(ctx, "continue-once", "")
	if err != nil {
		t.Fatal(err)
	}
	latestRunID := latest.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	stuck := fixture.OrderInput{OrderID: "2", CatalogShouldFail: true, RetryIntervalSeconds: 3600}
	running := start("order-running", nil, fixture.OrderWorkflow, stuck)
	terminated := start("order-terminated", nil, fixture.OrderWorkflow, stuck)
	if err := c.TerminateWorkflow(ctx, "order-terminated", "", "stopped by test"); err != nil {
		t.Fatal(err)
	}
	canceled := start("order-canceled", nil, fixture.OrderWorkflow, stuck)
	if err := c.CancelWorkflow(ctx, "order-canceled", ""); err != nil {
		t.Fatal(err)
	}
	for _, run := range []client.WorkflowRun{terminated, canceled} {
		if err := run.Get(ctx, nil); err == nil {
			t.Fatalf("%s completed, want it to close unsuccessfully", run.GetID())
		}
	}
	waitForDescribe(ctx, t, c, "order-running", "the second catalog attempt to be scheduled", func(desc *workflowservice.DescribeWorkflowExecutionResponse) bool {
		pa := desc.GetPendingActivities()
		return len(pa) == 1 && pa[0].GetAttempt() == 2
	})
	parent := start("order-parent", nil, parentOfWaiting)
	child := waitForDescribe(ctx, t, c, "order-parent", "the child workflow to start", func(desc *workflowservice.DescribeWorkflowExecutionResponse) bool {
		pc := desc.GetPendingChildren()
		return len(pc) == 1 && pc[0].GetRunId() != ""
	}).GetPendingChildren()[0]
	heartbeating := start("order-heartbeating", nil, runHeartbeating)
	waitForDescribe(ctx, t, c, "order-heartbeating", "the activity heartbeat to be recorded", func(desc *workflowservice.DescribeWorkflowExecutionResponse) bool {
		pa := desc.GetPendingActivities()
		return len(pa) == 1 && len(pa[0].GetHeartbeatDetails().GetPayloads()) > 0
	})
	unpolled, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-unpolled", TaskQueue: "unpolled"}, fixture.OrderWorkflow, fixture.OrderInput{OrderID: "4"})
	if err != nil {
		t.Fatal(err)
	}
	delayed, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-delayed", TaskQueue: fixture.TaskQueue, StartDelay: time.Hour}, fixture.OrderWorkflow, fixture.OrderInput{OrderID: "5"})
	if err != nil {
		t.Fatal(err)
	}
	rejected := start("order-rejected", nil, rejectWithDetails)
	if err := rejected.Get(ctx, nil); err == nil {
		t.Fatal("order-rejected completed, want it to fail")
	}

	pages := []string{"page-1", "page-2", "page-3", "page-4", "page-5"}
	for _, id := range pages {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:                    id,
			TaskQueue:             fixture.TaskQueue,
			TypedSearchAttributes: temporal.NewSearchAttributes(temporal.NewSearchAttributeKeyKeyword("OrderId").ValueSet(id)),
		}, fixture.OrderWorkflow, fixture.OrderInput{OrderID: id})
		if err != nil {
			t.Fatal(err)
		}
		if err := run.Get(ctx, nil); err != nil {
			t.Fatal(err)
		}
	}
	const pagesQuery = "OrderId IN ('page-1', 'page-2', 'page-3', 'page-4', 'page-5') AND ExecutionStatus = 'Completed'"
	for {
		n, err := c.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: pagesQuery})
		if err != nil {
			t.Fatal(err)
		}
		if n.GetCount() == int64(len(pages)) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for Visibility to show the page workflows")
		case <-time.After(100 * time.Millisecond):
		}
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unreachable := ln.Addr().String()
	ln.Close()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf("listen: 127.0.0.1:0\ntargets:\n"+
		"  - {name: dev, aliases: [local], description: x, address: %q, namespace: default}\n"+
		"  - {name: down, description: x, address: %q, namespace: default}\n"+
		"  - {name: misconfigured, description: x, address: %q, namespace: nosuchns}\n", dev.FrontendHostPort(), unreachable, dev.FrontendHostPort())), 0o600); err != nil {
		t.Fatal(err)
	}
	var records bytes.Buffer
	session := connect(t, configPath, io.MultiWriter(t.Output(), &records))
	validateOutput := outputValidator(t, session)

	t.Run("check_connection", func(t *testing.T) {
		tests := []struct {
			target string
			want   map[string]any
		}{
			{
				target: "LOCAL",
				want: map[string]any{
					"target": "dev",
					"result": map[string]any{"stages": []any{
						map[string]any{"name": "server", "status": "ok", "reason": nil},
						map[string]any{"name": "namespace", "status": "ok", "reason": nil},
					}},
					"complete":  true,
					"next_call": nil,
				},
			},
			{
				target: "down",
				want: map[string]any{
					"target": "down",
					"result": map[string]any{"stages": []any{
						map[string]any{"name": "server", "status": "failed", "reason": "Unavailable: could not connect to the Temporal server"},
						map[string]any{"name": "namespace", "status": "skipped", "reason": "not run because the server stage failed"},
					}},
					"complete":  true,
					"next_call": nil,
				},
			},
			{
				target: "misconfigured",
				want: map[string]any{
					"target": "misconfigured",
					"result": map[string]any{"stages": []any{
						map[string]any{"name": "server", "status": "ok", "reason": nil},
						map[string]any{"name": "namespace", "status": "failed", "reason": "NotFound: Namespace nosuchns is not found."},
					}},
					"complete":  true,
					"next_call": nil,
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.target, func(t *testing.T) {
				res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "check_connection", Arguments: map[string]any{"target": tt.target}})
				if err != nil {
					t.Fatal(err)
				}
				if res.IsError {
					t.Fatalf("tool returned an error: %+v", res.Content)
				}
				validateOutput(t, "check_connection", res)
				text := res.Content[0].(*mcp.TextContent).Text
				for _, addr := range []string{dev.FrontendHostPort(), unreachable} {
					if strings.Contains(text, addr) {
						t.Errorf("result %s contains the address %s", text, addr)
					}
				}
				got := res.StructuredContent.(map[string]any)
				for _, s := range got["result"].(map[string]any)["stages"].([]any) {
					stage := s.(map[string]any)
					latency, ran := stage["latency_ms"].(float64)
					if wantRan := stage["status"] != "skipped"; ran != wantRan || latency < 0 {
						t.Errorf("stage %v latency_ms = %v", stage["name"], stage["latency_ms"])
					}
					delete(stage, "latency_ms")
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("structuredContent = %#v\nwant %#v", got, tt.want)
				}
			})
		}
	})

	t.Run("get_workflow", func(t *testing.T) {
		callGetWorkflow := func(t *testing.T, args any) *mcp.CallToolResult {
			t.Helper()
			res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "get_workflow", Arguments: args})
			if err != nil {
				t.Fatal(err)
			}
			text := res.Content[0].(*mcp.TextContent).Text
			for _, addr := range []string{dev.FrontendHostPort(), unreachable} {
				if strings.Contains(text, addr) {
					t.Errorf("result %s contains the address %s", text, addr)
				}
			}
			return res
		}
		getWorkflow := func(t *testing.T, args any) map[string]any {
			t.Helper()
			res := callGetWorkflow(t, args)
			if res.IsError {
				t.Fatalf("tool returned an error: %s", res.Content[0].(*mcp.TextContent).Text)
			}
			validateOutput(t, "get_workflow", res)
			var fromText any
			if err := json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &fromText); err != nil || !reflect.DeepEqual(fromText, res.StructuredContent) {
				t.Errorf("text content does not match structuredContent (%v)", err)
			}
			env := res.StructuredContent.(map[string]any)
			if env["target"] != "dev" || env["complete"] != true || env["next_call"] != nil {
				t.Errorf("envelope = %#v, want target dev, complete true and next_call null", env)
			}
			run := env["result"].(map[string]any)
			if _, err := time.Parse(time.RFC3339, run["observed_at"].(string)); err != nil || !strings.HasSuffix(run["observed_at"].(string), "Z") {
				t.Errorf("observed_at = %v, want a UTC RFC 3339 time", run["observed_at"])
			}
			return run
		}
		check := func(t *testing.T, run map[string]any, want map[string]any) {
			t.Helper()
			for k, v := range want {
				if !reflect.DeepEqual(run[k], v) {
					t.Errorf("%s = %#v, want %#v", k, run[k], v)
				}
			}
		}
		empty := []any{}

		t.Run("failed", func(t *testing.T) {
			for _, args := range []map[string]any{
				{"target": "local", "workflow_id": "order-88231"},
				{"target": "dev", "workflow_id": "order-88231", "run_id": failed.GetRunID()},
			} {
				run := getWorkflow(t, args)
				check(t, run, map[string]any{
					"workflow_id":           "order-88231",
					"run_id":                failed.GetRunID(),
					"type":                  "OrderWorkflow",
					"status":                "Failed",
					"task_queue":            fixture.TaskQueue,
					"parent":                nil,
					"pending_activities":    empty,
					"pending_children":      empty,
					"pending_workflow_task": nil,
					"memo":                  map[string]any{},
					"result":                nil,
					"new_execution_run_id":  nil,
					"failure": map[string]any{
						"message":                "activity error",
						"kind":                   "ActivityFailure",
						"stack_trace":            "",
						"application":            nil,
						"details":                empty,
						"encoded_attributes":     nil,
						"last_heartbeat_details": empty,
						"cause": map[string]any{
							"message":                "503 Service Unavailable from catalog service",
							"kind":                   "ApplicationFailure",
							"stack_trace":            "",
							"application":            map[string]any{"type": "CatalogServiceUnavailable", "non_retryable": false},
							"details":                empty,
							"encoded_attributes":     nil,
							"last_heartbeat_details": empty,
							"cause":                  nil,
						},
					},
				})
				if run["close_time"] == nil || run["start_time"] == nil {
					t.Errorf("start_time = %v, close_time = %v, want both set", run["start_time"], run["close_time"])
				}
				if got := run["search_attributes"].(map[string]any)["OrderId"]; got != "88231" {
					t.Errorf("search attribute OrderId = %#v, want %q", got, "88231")
				}
			}
		})

		t.Run("null run_id is an omitted run_id", func(t *testing.T) {
			check(t, getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-88231", "run_id": nil}), map[string]any{
				"run_id": failed.GetRunID(),
				"status": "Failed",
			})
		})

		t.Run("typed search attributes", func(t *testing.T) {
			res := callGetWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-typed"})
			if res.IsError {
				t.Fatalf("tool returned an error: %s", res.Content[0].(*mcp.TextContent).Text)
			}
			validateOutput(t, "get_workflow", res)
			dec := json.NewDecoder(strings.NewReader(res.Content[0].(*mcp.TextContent).Text))
			dec.UseNumber()
			var env map[string]any
			if err := dec.Decode(&env); err != nil {
				t.Fatal(err)
			}
			got := env["result"].(map[string]any)["search_attributes"].(map[string]any)
			want := map[string]any{
				"SaInt":         json.Number("9007199254740993"),
				"SaDouble":      json.Number("1.5"),
				"SaBool":        true,
				"SaKeyword":     "k",
				"SaText":        "some text",
				"SaDatetime":    "2026-03-04T05:06:07Z",
				"SaKeywordList": []any{"a", "b"},
			}
			for k, v := range want {
				if !reflect.DeepEqual(got[k], v) {
					t.Errorf("search attribute %s = %#v, want %#v", k, got[k], v)
				}
			}
		})

		t.Run("completed", func(t *testing.T) {
			check(t, getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-completed"}), map[string]any{
				"run_id":  completed.GetRunID(),
				"status":  "Completed",
				"result":  []any{passthrough(`{"enrichment":"product-enriched:1","payment":"payment-authorized:1"}`)},
				"failure": nil,
				"memo":    map[string]any{"note": passthrough(`"gift"`)},
			})
		})

		t.Run("continued as new", func(t *testing.T) {
			check(t, getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "continue-once", "run_id": firstRunID}), map[string]any{
				"run_id":               firstRunID,
				"status":               "ContinuedAsNew",
				"result":               nil,
				"new_execution_run_id": latestRunID,
			})
			check(t, getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "continue-once"}), map[string]any{
				"run_id":               latestRunID,
				"status":               "Completed",
				"result":               []any{passthrough("true")},
				"new_execution_run_id": nil,
			})
		})

		t.Run("running", func(t *testing.T) {
			run := getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-running"})
			check(t, run, map[string]any{
				"run_id":     running.GetRunID(),
				"status":     "Running",
				"close_time": nil,
				"result":     nil,
				"failure":    nil,
			})
			activities := run["pending_activities"].([]any)
			if len(activities) != 1 {
				t.Fatalf("pending_activities = %#v, want one", activities)
			}
			pending := activities[0].(map[string]any)
			check(t, pending, map[string]any{
				"type":                "enrich_product",
				"state":               "Scheduled",
				"attempt":             float64(2),
				"last_heartbeat_time": nil,
				"heartbeat_details":   empty,
			})
			if pending["scheduled_time"] == nil {
				t.Error("scheduled_time = nil, want it set")
			}
			if f, _ := pending["last_failure"].(map[string]any); f["message"] != "503 Service Unavailable from catalog service" {
				t.Errorf("last_failure = %#v, want the catalog failure", pending["last_failure"])
			}
		})

		t.Run("parent and pending child", func(t *testing.T) {
			check(t, getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-parent"}), map[string]any{
				"run_id": parent.GetRunID(),
				"status": "Running",
				"parent": nil,
				"pending_children": []any{map[string]any{
					"workflow_id":         "order-child",
					"run_id":              child.GetRunId(),
					"type":                "waitForSignal",
					"initiated_id":        float64(child.GetInitiatedId()),
					"parent_close_policy": "Terminate",
				}},
			})
			check(t, getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-child"}), map[string]any{
				"run_id":           child.GetRunId(),
				"type":             "waitForSignal",
				"parent":           map[string]any{"workflow_id": "order-parent", "run_id": parent.GetRunID()},
				"pending_children": empty,
			})
		})

		t.Run("heartbeating activity", func(t *testing.T) {
			run := getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-heartbeating"})
			check(t, run, map[string]any{"run_id": heartbeating.GetRunID(), "status": "Running"})
			activities := run["pending_activities"].([]any)
			if len(activities) != 1 {
				t.Fatalf("pending_activities = %#v, want one", activities)
			}
			pending := activities[0].(map[string]any)
			check(t, pending, map[string]any{
				"type":              "heartbeatThenWait",
				"state":             "Started",
				"attempt":           float64(1),
				"last_failure":      nil,
				"heartbeat_details": []any{passthrough(`"progress-42"`)},
			})
			if pending["last_heartbeat_time"] == nil || pending["last_started_time"] == nil {
				t.Errorf("last_heartbeat_time = %v, last_started_time = %v, want both set", pending["last_heartbeat_time"], pending["last_started_time"])
			}
		})

		t.Run("pending workflow task", func(t *testing.T) {
			run := getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-unpolled"})
			check(t, run, map[string]any{
				"run_id":         unpolled.GetRunID(),
				"status":         "Running",
				"task_queue":     "unpolled",
				"history_length": float64(2),
			})
			task, _ := run["pending_workflow_task"].(map[string]any)
			check(t, task, map[string]any{"state": "Scheduled", "attempt": float64(1), "started_time": nil})
			if task["scheduled_time"] == nil || task["original_scheduled_time"] == nil {
				t.Errorf("pending_workflow_task = %#v, want scheduled_time and original_scheduled_time set", task)
			}
		})

		t.Run("delayed start", func(t *testing.T) {
			run := getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-delayed"})
			check(t, run, map[string]any{"run_id": delayed.GetRunID(), "status": "Running"})
			startTime, startErr := time.Parse(time.RFC3339Nano, fmt.Sprint(run["start_time"]))
			executionTime, executionErr := time.Parse(time.RFC3339Nano, fmt.Sprint(run["execution_time"]))
			if startErr != nil || executionErr != nil || executionTime.Sub(startTime) != time.Hour {
				t.Errorf("start_time = %v, execution_time = %v, want execution_time one hour after start_time", run["start_time"], run["execution_time"])
			}
		})

		t.Run("application failure details", func(t *testing.T) {
			check(t, getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-rejected"}), map[string]any{
				"run_id": rejected.GetRunID(),
				"status": "Failed",
				"failure": map[string]any{
					"message":                "order rejected",
					"kind":                   "ApplicationFailure",
					"stack_trace":            "",
					"application":            map[string]any{"type": "OrderRejected", "non_retryable": false},
					"details":                []any{passthrough(`"detail-1"`)},
					"encoded_attributes":     nil,
					"last_heartbeat_details": empty,
					"cause":                  nil,
				},
			})
		})

		t.Run("terminated", func(t *testing.T) {
			check(t, getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-terminated"}), map[string]any{
				"status": "Terminated",
				"failure": map[string]any{
					"message":                "stopped by test",
					"kind":                   "",
					"stack_trace":            "",
					"application":            nil,
					"details":                empty,
					"encoded_attributes":     nil,
					"last_heartbeat_details": empty,
					"cause":                  nil,
				},
			})
		})

		t.Run("canceled", func(t *testing.T) {
			check(t, getWorkflow(t, map[string]any{"target": "dev", "workflow_id": "order-canceled"}), map[string]any{
				"status": "Canceled",
				"failure": map[string]any{
					"message":                "",
					"kind":                   "",
					"stack_trace":            "",
					"application":            nil,
					"details":                empty,
					"encoded_attributes":     nil,
					"last_heartbeat_details": empty,
					"cause":                  nil,
				},
			})
		})

		errorTests := []struct {
			name          string
			args          any
			code          string
			messagePrefix string
			retryable     bool
		}{
			{"unknown target", map[string]any{"target": "nope", "workflow_id": "order-88231"}, "not_found", `unknown target "nope"`, false},
			{"unknown workflow", map[string]any{"target": "dev", "workflow_id": "order-0"}, "not_found", "workflow not found", false},
			{"unknown run", map[string]any{"target": "dev", "workflow_id": "order-88231", "run_id": latestRunID}, "not_found", "workflow execution not found for workflow ID ", false},
			{"array arguments", []any{"dev", "order-88231"}, "invalid_argument", "", false},
			{"empty workflow_id", map[string]any{"target": "dev", "workflow_id": ""}, "invalid_argument", "workflow_id is required", false},
			{"unknown namespace", map[string]any{"target": "misconfigured", "workflow_id": "order-88231"}, "temporal_unavailable", "Namespace nosuchns is not found.", false},
			{"unreachable target", map[string]any{"target": "down", "workflow_id": "order-88231"}, "temporal_unavailable", "could not connect to the Temporal server", true},
		}
		for _, tt := range errorTests {
			t.Run(tt.name, func(t *testing.T) {
				res := callGetWorkflow(t, tt.args)
				got := errorContent(t, res)
				if msg, _ := got["message"].(string); got["code"] != tt.code || got["retryable"] != tt.retryable || !strings.HasPrefix(msg, tt.messagePrefix) || len(got) != 3 {
					t.Errorf("error = %#v, want code %s, retryable %v and a message starting %q", got, tt.code, tt.retryable, tt.messagePrefix)
				}
			})
		}
	})

	callTool := func(t *testing.T, name string, args map[string]any) map[string]any {
		t.Helper()
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		if res.IsError {
			t.Fatalf("%s returned an error: %s", name, res.Content[0].(*mcp.TextContent).Text)
		}
		validateOutput(t, name, res)
		return res.StructuredContent.(map[string]any)
	}
	callToolError := func(t *testing.T, name string, args map[string]any) map[string]any {
		t.Helper()
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		return errorContent(t, res)
	}

	t.Run("search_workflows", func(t *testing.T) {
		t.Run("pages through next_call", func(t *testing.T) {
			name, args := "search_workflows", map[string]any{"target": "local", "query": pagesQuery, "fields": []any{"workflow_id", "ExecutionStatus", "run_id", "OrderId"}, "page_size": 2}
			seen := map[string]bool{}
			for calls := 1; ; calls++ {
				env := callTool(t, name, args)
				for _, w := range env["result"].(map[string]any)["workflows"].([]any) {
					row := w.(map[string]any)
					id := row["workflow_id"].(string)
					want := map[string]any{"workflow_id": id, "run_id": row["run_id"], "ExecutionStatus": "Completed", "OrderId": id}
					if seen[id] || row["run_id"] == "" || !reflect.DeepEqual(row, want) {
						t.Errorf("row %#v: want %#v, once", row, want)
					}
					seen[id] = true
				}
				if env["complete"] == true {
					if env["next_call"] != nil {
						t.Errorf("next_call = %#v on the last page, want null", env["next_call"])
					}
					break
				}
				if calls == len(pages) {
					t.Fatalf("still not complete after %d pages of 2", calls)
				}
				next := env["next_call"].(map[string]any)
				name, args = next["name"].(string), next["arguments"].(map[string]any)
				if args["target"] != "dev" {
					t.Errorf("next_call target = %v, want the target's name, dev", args["target"])
				}
			}
			if len(seen) != len(pages) {
				t.Errorf("workflows = %v, want %v", seen, pages)
			}
		})

		t.Run("typed search attributes", func(t *testing.T) {
			res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "search_workflows", Arguments: map[string]any{
				"target": "dev",
				"query":  "WorkflowId = 'order-typed'",
				"fields": []any{"SaInt", "SaDouble", "SaBool", "SaKeyword", "SaText", "SaDatetime", "SaKeywordList"},
			}})
			if err != nil || res.IsError {
				t.Fatalf("search_workflows: %v %+v", err, res)
			}
			dec := json.NewDecoder(strings.NewReader(res.Content[0].(*mcp.TextContent).Text))
			dec.UseNumber()
			var env map[string]any
			if err := dec.Decode(&env); err != nil {
				t.Fatal(err)
			}
			got := env["result"].(map[string]any)["workflows"].([]any)[0].(map[string]any)
			delete(got, "workflow_id")
			delete(got, "run_id")
			// SQLite Visibility rounds SaInt's 9007199254740993 to 2^53, so expect what Visibility returns; get_workflow's test covers full precision.
			listed, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: "WorkflowId = 'order-typed'"})
			if err != nil || len(listed.GetExecutions()) != 1 {
				t.Fatalf("ListWorkflow: %v, %d executions", err, len(listed.GetExecutions()))
			}
			var saInt int64
			if err := converter.GetDefaultDataConverter().FromPayload(listed.GetExecutions()[0].GetSearchAttributes().GetIndexedFields()["SaInt"], &saInt); err != nil {
				t.Fatal(err)
			}
			want := map[string]any{
				"SaInt":         json.Number(fmt.Sprint(saInt)),
				"SaDouble":      json.Number("1.5"),
				"SaBool":        true,
				"SaKeyword":     "k",
				"SaText":        "some text",
				"SaDatetime":    "2026-03-04T05:06:07Z",
				"SaKeywordList": []any{"a", "b"},
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("search attributes = %#v, want %#v", got, want)
			}
		})

		t.Run("every core search attribute", func(t *testing.T) {
			// Temporal keeps these out of a run's search attribute map, so search_workflows must read each from the run or it comes back null.
			core := sadefs.System()
			env := callTool(t, "search_workflows", map[string]any{"target": "dev", "query": "WorkflowId IN ('order-completed', 'order-child')", "fields": slices.Collect(maps.Keys(core))})
			rows := map[string]map[string]any{}
			for _, w := range env["result"].(map[string]any)["workflows"].([]any) {
				rows[w.(map[string]any)["workflow_id"].(string)] = w.(map[string]any)
			}
			for name, typ := range core {
				set := false
				for id, row := range rows {
					if row[name] == nil {
						continue
					}
					set = true
					var ok bool
					switch typ {
					case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
						_, ok = row[name].(string)
					case enumspb.INDEXED_VALUE_TYPE_INT:
						_, ok = row[name].(float64)
					case enumspb.INDEXED_VALUE_TYPE_DATETIME:
						s, _ := row[name].(string)
						_, err := time.Parse(time.RFC3339Nano, s)
						ok = err == nil
					}
					if !ok {
						t.Errorf("%s of %s = %#v, want a %s", name, id, row[name], typ)
					}
				}
				if !set {
					t.Errorf("%s is null on every run, want it read from the run", name)
				}
			}
			want := map[string]map[string]any{
				"order-completed": {"WorkflowId": "order-completed", "RunId": completed.GetRunID(), "WorkflowType": "OrderWorkflow", "ExecutionStatus": "Completed", "TaskQueue": fixture.TaskQueue,
					"ParentWorkflowId": nil, "ParentRunId": nil, "RootWorkflowId": "order-completed", "RootRunId": completed.GetRunID()},
				"order-child": {"WorkflowId": "order-child", "RunId": child.GetRunId(), "WorkflowType": "waitForSignal", "ExecutionStatus": "Running", "TaskQueue": fixture.TaskQueue,
					"ParentWorkflowId": "order-parent", "ParentRunId": parent.GetRunID(), "RootWorkflowId": "order-parent", "RootRunId": parent.GetRunID(),
					"CloseTime": nil, "ExecutionDuration": nil, "HistoryLength": nil, "HistorySizeBytes": nil, "StateTransitionCount": nil},
			}
			for id, attrs := range want {
				for name, v := range attrs {
					if got := rows[id][name]; got != v {
						t.Errorf("%s of %s = %#v, want %#v", name, id, got, v)
					}
				}
			}
		})

		t.Run("next_call when query is omitted", func(t *testing.T) {
			env := callTool(t, "search_workflows", map[string]any{"target": "local", "page_size": 2})
			next := env["next_call"].(map[string]any)
			callTool(t, next["name"].(string), next["arguments"].(map[string]any))
		})

		first := callTool(t, "search_workflows", map[string]any{"target": "dev", "query": pagesQuery, "page_size": 2})
		continuation := first["next_call"].(map[string]any)["arguments"].(map[string]any)["continuation"]
		errorTests := []struct {
			name          string
			tool          string
			args          map[string]any
			code          string
			messagePrefix string
		}{
			{"continuation for another query", "search_workflows", map[string]any{"target": "dev", "query": "WorkflowType = 'OrderWorkflow'", "continuation": continuation}, "continuation_invalid", "continuation is not one"},
			{"continuation for another target", "search_workflows", map[string]any{"target": "misconfigured", "query": pagesQuery, "continuation": continuation}, "continuation_invalid", "continuation is not one"},
			{"garbage continuation", "search_workflows", map[string]any{"target": "dev", "query": pagesQuery, "continuation": "not-a-continuation"}, "continuation_invalid", "continuation is not one"},
			{"oversized continuation", "search_workflows", map[string]any{"target": "dev", "query": pagesQuery, "continuation": strings.Repeat("A", 4097)}, "continuation_invalid", "continuation is not one"},
			{"fields the target lacks", "search_workflows", map[string]any{"target": "dev", "fields": []any{"NoSuchAttribute", "ExecutionStatus", "status", "SaInt", "TemporalNoSuchAttribute", "saInt"}}, "invalid_argument",
				`these fields are not search attributes on target dev: "NoSuchAttribute", "status", "TemporalNoSuchAttribute", "saInt"; call list_search_attributes for the target's search attributes`},
			{"unknown attribute in the query", "search_workflows", map[string]any{"target": "dev", "query": "NoSuchAttribute = 'x'"}, "invalid_query", "invalid query"},
			{"unknown attribute in a count", "count_workflows", map[string]any{"target": "dev", "query": "NoSuchAttribute = 'x'"}, "invalid_query", "invalid query"},
		}
		for _, tt := range errorTests {
			t.Run(tt.name, func(t *testing.T) {
				got := callToolError(t, tt.tool, tt.args)
				if msg, _ := got["message"].(string); got["code"] != tt.code || got["retryable"] != false || !strings.HasPrefix(msg, tt.messagePrefix) || msg == "" {
					t.Errorf("error = %#v, want code %s and a message starting %q", got, tt.code, tt.messagePrefix)
				}
			})
		}
	})

	t.Run("count_workflows", func(t *testing.T) {
		env := callTool(t, "count_workflows", map[string]any{"target": "dev", "query": pagesQuery})
		if want := map[string]any{"count": float64(len(pages)), "groups": []any{}}; !reflect.DeepEqual(env["result"], want) || env["complete"] != true {
			t.Errorf("envelope = %#v, want result %#v and complete true", env, want)
		}
		env = callTool(t, "count_workflows", map[string]any{"target": "dev", "query": "WorkflowId IN ('order-88231', 'order-completed', 'order-running', 'order-terminated', 'order-canceled')", "group_by": "ExecutionStatus"})
		result := env["result"].(map[string]any)
		groups := map[any]any{}
		for _, g := range result["groups"].([]any) {
			groups[g.(map[string]any)["value"]] = g.(map[string]any)["count"]
		}
		want := map[any]any{"Failed": 1.0, "Completed": 1.0, "Running": 1.0, "Terminated": 1.0, "Canceled": 1.0}
		if result["count"] != 5.0 || !reflect.DeepEqual(groups, want) {
			t.Errorf("result = %#v, want count 5 and groups %v", result, want)
		}
		all := callTool(t, "count_workflows", map[string]any{"target": "local"})["result"].(map[string]any)
		byStatus := callTool(t, "count_workflows", map[string]any{"target": "local", "group_by": "ExecutionStatus"})["result"].(map[string]any)
		sum := 0.0
		for _, g := range byStatus["groups"].([]any) {
			sum += g.(map[string]any)["count"].(float64)
		}
		if all["count"].(float64) < float64(len(pages)) || byStatus["count"] != all["count"] || sum != all["count"] {
			t.Errorf("without a query: count = %v, grouped count = %v with groups summing to %v; want every run, in both", all["count"], byStatus["count"], sum)
		}
	})

	t.Run("list_search_attributes", func(t *testing.T) {
		env := callTool(t, "list_search_attributes", map[string]any{"target": "dev"})
		if env["target"] != "dev" || env["complete"] != true {
			t.Errorf("envelope = %#v, want target dev and complete true", env)
		}
		custom := map[string]any{}
		for _, a := range env["result"].(map[string]any)["search_attributes"].([]any) {
			attr := a.(map[string]any)
			switch attr["category"] {
			case "custom":
				custom[attr["name"].(string)] = attr["type"]
			case "system":
			default:
				t.Errorf("attribute %#v has an unknown category", attr)
			}
		}
		wantCustom := map[string]any{"OrderId": "Keyword", "SaInt": "Int", "SaDouble": "Double", "SaBool": "Bool", "SaKeyword": "Keyword", "SaText": "Text", "SaDatetime": "Datetime", "SaKeywordList": "KeywordList"}
		if !reflect.DeepEqual(custom, wantCustom) {
			t.Errorf("custom attributes = %v, want %v", custom, wantCustom)
		}
	})

	t.Run("fields checked against the target's search attributes", func(t *testing.T) {
		search := func(t *testing.T, target string, fields ...any) (*mcp.CallToolResult, float64) {
			t.Helper()
			records.Reset()
			res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "search_workflows", Arguments: map[string]any{"target": target, "fields": fields, "page_size": 1}})
			if err != nil {
				t.Fatal(err)
			}
			var rec map[string]any
			if err := json.Unmarshal(records.Bytes(), &rec); err != nil {
				t.Fatal(err)
			}
			return res, rec["temporal_calls"].(float64)
		}
		// misconfigured's search attributes cannot be listed, so a core attribute passes without trying, and another
		// attribute runs the search unchecked after the failed listing.
		if _, calls := search(t, "misconfigured", "ExecutionStatus"); calls != 1 {
			t.Errorf("core attribute: temporal_calls = %v, want 1, the search alone", calls)
		}
		if _, calls := search(t, "misconfigured", "SaInt"); calls != 2 {
			t.Errorf("attribute on a target that cannot be listed: temporal_calls = %v, want 2, the failed list and the search", calls)
		}
		addSearchAttribute := func(t *testing.T, name string) {
			t.Helper()
			if _, err := c.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
				Namespace:        "default",
				SearchAttributes: map[string]enumspb.IndexedValueType{name: enumspb.INDEXED_VALUE_TYPE_KEYWORD},
			}); err != nil {
				t.Fatal(err)
			}
			for {
				listed, err := c.OperatorService().ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: "default"})
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := listed.GetCustomAttributes()[name]; ok {
					return
				}
				select {
				case <-ctx.Done():
					t.Fatalf("timed out waiting for %s to be listed", name)
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
		if res, _ := search(t, "dev", "status"); !res.IsError || errorContent(t, res)["code"] != "invalid_argument" {
			t.Errorf("attribute not added yet: isError = %v, want invalid_argument", res.IsError)
		}
		addSearchAttribute(t, "status")
		if res, calls := search(t, "dev", "status"); res.IsError || calls != 2 {
			t.Errorf("attribute added since the list: isError = %v, temporal_calls = %v; want the list and the search", res.IsError, calls)
		}
		if res, calls := search(t, "dev", "status"); res.IsError || calls != 1 {
			t.Errorf("attribute added since the list, again: isError = %v, temporal_calls = %v; want a search alone", res.IsError, calls)
		}
		addSearchAttribute(t, "type")
		callTool(t, "list_search_attributes", map[string]any{"target": "dev"})
		if res, calls := search(t, "dev", "type"); res.IsError || calls != 1 {
			t.Errorf("attribute added and then listed: isError = %v, temporal_calls = %v; want a search alone", res.IsError, calls)
		}
	})
}
