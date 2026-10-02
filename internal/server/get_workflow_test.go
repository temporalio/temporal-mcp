package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

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
	session := connect(t, configPath, t.Output())
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
}
