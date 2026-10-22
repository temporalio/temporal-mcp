package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/searchattribute/sadefs"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
	"github.com/temporalio/temporal-operator-mcp/internal/fixture"
	"github.com/temporalio/temporal-operator-mcp/internal/payload"
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
		{"Int", []int64{5}, `5`},
		{"Keyword", []string{"a", "b"}, `["a","b"]`},
		{"Int", []int64{}, `null`},
		{"Keyword", nil, `null`},
	}
	fields := map[string]*commonpb.Payload{}
	for i, tt := range tests {
		fields["attr"+strconv.Itoa(i)] = typedPayload(t, tt.value, tt.typ)
	}
	run, err := describedRun(describeWithSearchAttributes(fields), new(payload.Renderer).Batch("t"))
	if err != nil {
		t.Fatal(err)
	}
	for i, tt := range tests {
		got, err := json.Marshal(run.SearchAttributes["attr"+strconv.Itoa(i)])
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tt.want {
			t.Errorf("type %q value %v: search attribute = %s, want %s", tt.typ, tt.value, got, tt.want)
		}
	}
}

func TestDescribedRunRejectsUndecodableSearchAttribute(t *testing.T) {
	p := typedPayload(t, "not a number", "Int")
	_, err := describedRun(describeWithSearchAttributes(map[string]*commonpb.Payload{"Count": p}), new(payload.Renderer).Batch("t"))
	var got *toolError
	if !errors.As(err, &got) || got.Code != "temporal_unavailable" {
		t.Errorf("err = %v, want a temporal_unavailable tool error", err)
	}
}

func TestToFailureEncodedAttributesAndLastHeartbeatDetails(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	encoded, err := dc.ToPayload("secret")
	if err != nil {
		t.Fatal(err)
	}
	heartbeat, err := dc.ToPayloads("beat")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Targets: []config.Target{{Name: "t", PayloadHandling: config.PayloadPassthrough}}}
	payloads := newRenderer(t, cfg, slog.New(slog.DiscardHandler))
	beat := `[{"handling":"passthrough","value":"beat"}]`

	// want is [encoded_attributes, last_heartbeat_details].
	tests := []struct {
		name string
		in   *failurepb.Failure
		want string
	}{
		{
			name: "encoded attributes",
			in: &failurepb.Failure{
				EncodedAttributes: encoded,
				FailureInfo:       &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "T"}},
			},
			want: `[{"handling":"passthrough","value":"secret"},[]]`,
		},
		{
			name: "timeout",
			in: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{LastHeartbeatDetails: heartbeat}},
			},
			want: `[null,` + beat + `]`,
		},
		{
			name: "reset workflow",
			in: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ResetWorkflowFailureInfo{ResetWorkflowFailureInfo: &failurepb.ResetWorkflowFailureInfo{LastHeartbeatDetails: heartbeat}},
			},
			want: `[null,` + beat + `]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := payloads.Batch("t")
			got := toFailure(tt.in, batch)
			if err := batch.Render(t.Context()); err != nil {
				t.Fatal(err)
			}
			if b, err := json.Marshal([]any{got.EncodedAttributes, got.LastHeartbeatDetails}); err != nil || string(b) != tt.want {
				t.Errorf("[encoded_attributes, last_heartbeat_details] = %s (%v), want %s", b, err, tt.want)
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

// handlerNamespace is the namespace of the workflow that greetOperation starts, apart from the caller's.
const handlerNamespace = "nexus-handler"

func greet(_ workflow.Context, name string) (string, error) {
	return "hello " + name, nil
}

var greetOperation = temporalnexus.NewWorkflowRunOperation("greet", greet, func(_ context.Context, name string, _ nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
	return client.StartWorkflowOptions{ID: "greet-" + name}, nil
})

func callGreet(ctx workflow.Context, name string) (string, error) {
	var greeting string
	err := workflow.NewNexusClient("greeter", "greeter").ExecuteOperation(ctx, greetOperation, name, workflow.NexusOperationOptions{}).Get(ctx, &greeting)
	return greeting, err
}

func waitUntil(ctx context.Context, t *testing.T, what string, done func() bool) {
	t.Helper()
	for !done() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitForDescribe(ctx context.Context, t *testing.T, c client.Client, workflowID, what string, done func(*workflowservice.DescribeWorkflowExecutionResponse) bool) *workflowservice.DescribeWorkflowExecutionResponse {
	t.Helper()
	var desc *workflowservice.DescribeWorkflowExecutionResponse
	waitUntil(ctx, t, what, func() bool {
		var err error
		if desc, err = c.DescribeWorkflowExecution(ctx, workflowID, ""); err != nil {
			t.Fatal(err)
		}
		return done(desc)
	})
	return desc
}

func decoded(value any) map[string]any {
	return map[string]any{"handling": "decoded", "value": value}
}

func TestToolsOnDevServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	zlib := converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true})
	codecHandler := converter.NewPayloadCodecHTTPHandler(zlib)
	codecServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer codec-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		codecHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(codecServer.Close)
	codecAuthorization := filepath.Join(t.TempDir(), "codec-authorization")
	if err := os.WriteFile(codecAuthorization, []byte("Bearer codec-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	encoded := &client.Options{DataConverter: converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), zlib)}
	// encode returns value as the test's clients and workers store it.
	encode := func(t *testing.T, value any) *commonpb.Payload {
		t.Helper()
		p, err := encoded.DataConverter.ToPayload(value)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	dev, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{ClientOptions: encoded, ExtraArgs: []string{
		"--search-attribute", "OrderId=Keyword",
		"--search-attribute", "SaInt=Int",
		"--search-attribute", "SaDouble=Double",
		"--search-attribute", "SaBool=Bool",
		"--search-attribute", "SaKeyword=Keyword",
		"--search-attribute", "SaText=Text",
		"--search-attribute", "SaDatetime=Datetime",
		"--search-attribute", "SaKeywordList=KeywordList",
		"--search-attribute", "Domain=Keyword",
		"--namespace", handlerNamespace,
		// Small history pages let a filtered get_workflow_history read reach its page limit on a short history.
		"--dynamic-config-value", "frontend.historyMaxPageSize=5",
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
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf("listen: 127.0.0.1:0\n"+
		"codecs:\n"+
		"  test: {endpoint: %[1]q, authorization_header: {file: %[2]q}}\n"+
		"  unauthorized: {endpoint: %[1]q}\n"+
		"targets:\n"+
		"  - {name: dev, aliases: [local], description: x, address: %[3]q, namespace: default, payload_handling: decode, codec: test}\n"+
		"  - {name: dev-passthrough, description: x, address: %[3]q, namespace: default, payload_handling: passthrough}\n"+
		"  - {name: dev-omit, description: x, address: %[3]q, namespace: default}\n"+
		"  - {name: dev-unauthorized, description: x, address: %[3]q, namespace: default, payload_handling: decode, codec: unauthorized}\n"+
		"  - {name: down, description: x, address: %[4]q, namespace: default}\n"+
		"  - {name: misconfigured, description: x, address: %[3]q, namespace: nosuchns}\n"+
		"  - {name: dev-handler, description: x, address: %[3]q, namespace: %[5]s}\n", codecServer.URL, codecAuthorization, dev.FrontendHostPort(), unreachable, handlerNamespace)), 0o600); err != nil {
		t.Fatal(err)
	}
	addresses := []string{dev.FrontendHostPort(), unreachable, strings.TrimPrefix(codecServer.URL, "http://")}
	var records bytes.Buffer
	session := connect(t, configPath, io.MultiWriter(t.Output(), &records))
	validateOutput := outputValidator(t, session)
	temporalCalls := func(t *testing.T) float64 {
		t.Helper()
		var rec map[string]any
		if err := json.Unmarshal(records.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		return rec["temporal_calls"].(float64)
	}

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
						map[string]any{"name": "codec", "status": "ok", "reason": nil},
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
						map[string]any{"name": "codec", "status": "skipped", "reason": "not run because the target does not decode payloads"},
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
						map[string]any{"name": "codec", "status": "skipped", "reason": "not run because the target does not decode payloads"},
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
				for _, addr := range addresses {
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

	t.Run("get_workflows", func(t *testing.T) {
		callGetWorkflows := func(t *testing.T, args any) *mcp.CallToolResult {
			t.Helper()
			res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "get_workflows", Arguments: args})
			if err != nil {
				t.Fatal(err)
			}
			text := res.Content[0].(*mcp.TextContent).Text
			for _, addr := range addresses {
				if strings.Contains(text, addr) {
					t.Errorf("result %s contains the address %s", text, addr)
				}
			}
			return res
		}
		getWorkflows := func(t *testing.T, target string, workflows ...map[string]any) []map[string]any {
			t.Helper()
			res := callGetWorkflows(t, map[string]any{"target": target, "workflows": workflows})
			if res.IsError {
				t.Fatalf("tool returned an error: %s", res.Content[0].(*mcp.TextContent).Text)
			}
			validateOutput(t, "get_workflows", res)
			var fromText any
			if err := json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &fromText); err != nil || !reflect.DeepEqual(fromText, res.StructuredContent) {
				t.Errorf("text content does not match structuredContent (%v)", err)
			}
			env := res.StructuredContent.(map[string]any)
			wantTarget := target
			if wantTarget == "local" {
				wantTarget = "dev"
			}
			if env["target"] != wantTarget || env["complete"] != true || env["next_call"] != nil {
				t.Errorf("envelope = %#v, want target %v, complete true and next_call null", env, wantTarget)
			}
			result := env["result"].([]any)
			if len(result) != len(workflows) {
				t.Fatalf("result = %#v, want an entry for each of %v", result, workflows)
			}
			entries := []map[string]any{}
			for i, e := range result {
				entry := e.(map[string]any)
				if entry["workflow_id"] != workflows[i]["workflow_id"] || entry["run_id"] != workflows[i]["run_id"] {
					t.Errorf("entry %d = %#v, want the workflow_id and run_id of %v", i, entry, workflows[i])
				}
				entries = append(entries, entry)
			}
			return entries
		}
		getRuns := func(t *testing.T, target string, workflows ...map[string]any) []map[string]any {
			t.Helper()
			runs := []map[string]any{}
			for _, entry := range getWorkflows(t, target, workflows...) {
				run, _ := entry["run"].(map[string]any)
				if run == nil || entry["error"] != nil {
					t.Fatalf("entry = %#v, want a run and no error", entry)
				}
				if _, err := time.Parse(time.RFC3339, run["observed_at"].(string)); err != nil || !strings.HasSuffix(run["observed_at"].(string), "Z") {
					t.Errorf("observed_at = %v, want a UTC RFC 3339 time", run["observed_at"])
				}
				runs = append(runs, run)
			}
			return runs
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
			for _, run := range getRuns(t, "local",
				map[string]any{"workflow_id": "order-88231"},
				map[string]any{"workflow_id": "order-88231", "run_id": failed.GetRunID()},
				// A null run_id is an omitted run_id.
				map[string]any{"workflow_id": "order-88231", "run_id": nil},
			) {
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

		t.Run("found, missing and invalid workflows, in the order given", func(t *testing.T) {
			entries := getWorkflows(t, "dev",
				map[string]any{"workflow_id": "order-completed"},
				map[string]any{"workflow_id": "order-0"},
				map[string]any{"workflow_id": "order-88231", "run_id": latestRunID},
				map[string]any{"workflow_id": "order-88231", "run_id": "not-a-run-id"},
				map[string]any{"workflow_id": "continue-once"},
			)
			for i, want := range []struct {
				result        []any
				code, message string
			}{
				{result: []any{decoded(map[string]any{"enrichment": "product-enriched:1", "payment": "payment-authorized:1"})}},
				{code: "not_found", message: "workflow not found"},
				{code: "not_found", message: "workflow execution not found for workflow ID "},
				{code: "invalid_argument", message: "Invalid RunId."},
				{result: []any{decoded(true)}},
			} {
				run, _ := entries[i]["run"].(map[string]any)
				got, _ := entries[i]["error"].(map[string]any)
				if want.code == "" {
					if got != nil || run == nil || !reflect.DeepEqual(run["result"], want.result) {
						t.Errorf("entry %d = %#v, want a run with result %#v", i, entries[i], want.result)
					}
					continue
				}
				if msg, _ := got["message"].(string); run != nil || got["code"] != want.code || got["retryable"] != false || !strings.HasPrefix(msg, want.message) || len(got) != 3 {
					t.Errorf("entry %d = %#v, want no run and error code %s with a message starting %q", i, entries[i], want.code, want.message)
				}
			}
		})

		t.Run("typed search attributes", func(t *testing.T) {
			res := callGetWorkflows(t, map[string]any{"target": "dev", "workflows": []any{map[string]any{"workflow_id": "order-typed"}}})
			if res.IsError {
				t.Fatalf("tool returned an error: %s", res.Content[0].(*mcp.TextContent).Text)
			}
			validateOutput(t, "get_workflows", res)
			dec := json.NewDecoder(strings.NewReader(res.Content[0].(*mcp.TextContent).Text))
			dec.UseNumber()
			var env map[string]any
			if err := dec.Decode(&env); err != nil {
				t.Fatal(err)
			}
			got := env["result"].([]any)[0].(map[string]any)["run"].(map[string]any)["search_attributes"].(map[string]any)
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
			check(t, getRuns(t, "dev", map[string]any{"workflow_id": "order-completed"})[0], map[string]any{
				"run_id":  completed.GetRunID(),
				"status":  "Completed",
				"result":  []any{decoded(map[string]any{"enrichment": "product-enriched:1", "payment": "payment-authorized:1"})},
				"failure": nil,
				"memo":    map[string]any{"note": decoded("gift")},
			})
		})

		t.Run("payload handling", func(t *testing.T) {
			stored := func(value any) map[string]any {
				return map[string]any{"handling": "passthrough", "encoding": "binary/zlib", "data": base64.StdEncoding.EncodeToString(encode(t, value).GetData())}
			}
			omitted := func(reason, why string, value any) map[string]any {
				f := map[string]any{"handling": "omitted", "reason": reason, "encoding": "binary/zlib", "size_bytes": float64(len(encode(t, value).GetData()))}
				if why != "" {
					f["error"] = why
				}
				return f
			}
			const unauthorized = "the codec server responded HTTP 401 Unauthorized"
			result := map[string]string{"enrichment": "product-enriched:1", "payment": "payment-authorized:1"}
			for _, tt := range []struct {
				target       string
				result, memo map[string]any
			}{
				{"dev-passthrough", stored(result), stored("gift")},
				{"dev-omit", omitted("payload_handling", "", result), omitted("payload_handling", "", "gift")},
				{"dev-unauthorized", omitted("decode_failed", unauthorized, result), omitted("decode_failed", unauthorized, "gift")},
			} {
				t.Run(tt.target, func(t *testing.T) {
					check(t, getRuns(t, tt.target, map[string]any{"workflow_id": "order-completed"})[0], map[string]any{
						"run_id":  completed.GetRunID(),
						"status":  "Completed",
						"result":  []any{tt.result},
						"failure": nil,
						"memo":    map[string]any{"note": tt.memo},
					})
				})
			}
		})

		t.Run("continued as new", func(t *testing.T) {
			runs := getRuns(t, "dev", map[string]any{"workflow_id": "continue-once", "run_id": firstRunID}, map[string]any{"workflow_id": "continue-once"})
			check(t, runs[0], map[string]any{
				"run_id":               firstRunID,
				"status":               "ContinuedAsNew",
				"result":               nil,
				"new_execution_run_id": latestRunID,
			})
			check(t, runs[1], map[string]any{
				"run_id":               latestRunID,
				"status":               "Completed",
				"result":               []any{decoded(true)},
				"new_execution_run_id": nil,
			})
		})

		t.Run("running", func(t *testing.T) {
			run := getRuns(t, "dev", map[string]any{"workflow_id": "order-running"})[0]
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
			runs := getRuns(t, "dev", map[string]any{"workflow_id": "order-parent"}, map[string]any{"workflow_id": "order-child"})
			check(t, runs[0], map[string]any{
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
			check(t, runs[1], map[string]any{
				"run_id":           child.GetRunId(),
				"type":             "waitForSignal",
				"parent":           map[string]any{"workflow_id": "order-parent", "run_id": parent.GetRunID()},
				"pending_children": empty,
			})
		})

		t.Run("heartbeating activity", func(t *testing.T) {
			run := getRuns(t, "dev", map[string]any{"workflow_id": "order-heartbeating"})[0]
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
				"heartbeat_details": []any{decoded("progress-42")},
			})
			if pending["last_heartbeat_time"] == nil || pending["last_started_time"] == nil {
				t.Errorf("last_heartbeat_time = %v, last_started_time = %v, want both set", pending["last_heartbeat_time"], pending["last_started_time"])
			}
		})

		t.Run("pending workflow task", func(t *testing.T) {
			run := getRuns(t, "dev", map[string]any{"workflow_id": "order-unpolled"})[0]
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
			run := getRuns(t, "dev", map[string]any{"workflow_id": "order-delayed"})[0]
			check(t, run, map[string]any{"run_id": delayed.GetRunID(), "status": "Running"})
			startTime, startErr := time.Parse(time.RFC3339Nano, fmt.Sprint(run["start_time"]))
			executionTime, executionErr := time.Parse(time.RFC3339Nano, fmt.Sprint(run["execution_time"]))
			if startErr != nil || executionErr != nil || executionTime.Sub(startTime) != time.Hour {
				t.Errorf("start_time = %v, execution_time = %v, want execution_time one hour after start_time", run["start_time"], run["execution_time"])
			}
		})

		t.Run("application failure details", func(t *testing.T) {
			check(t, getRuns(t, "dev", map[string]any{"workflow_id": "order-rejected"})[0], map[string]any{
				"run_id": rejected.GetRunID(),
				"status": "Failed",
				"failure": map[string]any{
					"message":                "order rejected",
					"kind":                   "ApplicationFailure",
					"stack_trace":            "",
					"application":            map[string]any{"type": "OrderRejected", "non_retryable": false},
					"details":                []any{decoded("detail-1")},
					"encoded_attributes":     nil,
					"last_heartbeat_details": empty,
					"cause":                  nil,
				},
			})
		})

		t.Run("terminated", func(t *testing.T) {
			check(t, getRuns(t, "dev", map[string]any{"workflow_id": "order-terminated"})[0], map[string]any{
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
			check(t, getRuns(t, "dev", map[string]any{"workflow_id": "order-canceled"})[0], map[string]any{
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

		found := map[string]any{"workflow_id": "order-88231"}
		errorTests := []struct {
			name          string
			args          any
			code          string
			messagePrefix string
			retryable     bool
			calls         float64
		}{
			{"unknown target", map[string]any{"target": "nope", "workflows": []any{found}}, "not_found", `unknown target "nope"`, false, 0},
			{"array arguments", []any{"dev", "order-88231"}, "invalid_argument", "", false, 0},
			// A server error stops the reading at the first workflow, so temporal_calls counts only that call's attempts.
			{"unknown namespace", map[string]any{"target": "misconfigured", "workflows": []any{found, found}}, "temporal_unavailable", "workflows[0]: Namespace nosuchns is not found.", false, 1},
			{"unreachable target", map[string]any{"target": "down", "workflows": []any{found, found}}, "temporal_unavailable", "workflows[0]: could not connect to the Temporal server", true, float64(1 + len(retryDelays))},
		}
		for _, tt := range errorTests {
			t.Run(tt.name, func(t *testing.T) {
				records.Reset()
				got := errorContent(t, callGetWorkflows(t, tt.args))
				if msg, _ := got["message"].(string); got["code"] != tt.code || got["retryable"] != tt.retryable || !strings.HasPrefix(msg, tt.messagePrefix) || len(got) != 3 {
					t.Errorf("error = %#v, want code %s, retryable %v and a message starting %q", got, tt.code, tt.retryable, tt.messagePrefix)
				}
				if calls := temporalCalls(t); calls != tt.calls {
					t.Errorf("temporal_calls = %v, want %v", calls, tt.calls)
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
			// SQLite Visibility rounds SaInt's 9007199254740993 to 2^53, so expect what Visibility returns; get_workflows' test covers full precision.
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
		wantCustom := map[string]any{"OrderId": "Keyword", "SaInt": "Int", "SaDouble": "Double", "SaBool": "Bool", "SaKeyword": "Keyword", "SaText": "Text", "SaDatetime": "Datetime", "SaKeywordList": "KeywordList", "Domain": "Keyword"}
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
			return res, temporalCalls(t)
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

	callCounted := func(t *testing.T, name string, args map[string]any) (map[string]any, float64) {
		t.Helper()
		records.Reset()
		env := callTool(t, name, args)
		return env, temporalCalls(t)
	}
	historyEvents := func(env map[string]any) []map[string]any {
		events := []map[string]any{}
		for _, e := range env["result"].(map[string]any)["events"].([]any) {
			events = append(events, e.(map[string]any))
		}
		return events
	}
	typesOf := func(events []map[string]any) []string {
		types := []string{}
		for _, e := range events {
			types = append(types, e["event_type"].(string))
		}
		return types
	}
	attributes := func(e map[string]any) map[string]any { return e["attributes"].(map[string]any) }
	pageThrough := func(t *testing.T, name string, args map[string]any) ([]map[string]any, []float64) {
		t.Helper()
		var events []map[string]any
		var calls []float64
		for {
			env, n := callCounted(t, name, args)
			events, calls = append(events, historyEvents(env)...), append(calls, n)
			if env["complete"] == true {
				if env["next_call"] != nil {
					t.Errorf("next_call = %#v on a complete call, want null", env["next_call"])
				}
				return events, calls
			}
			if len(calls) == 100 {
				t.Fatal("still not complete after 100 calls")
			}
			next := env["next_call"].(map[string]any)
			name, args = next["name"].(string), next["arguments"].(map[string]any)
		}
	}
	waitForBacklog := func(t *testing.T, queue string, typ enumspb.TaskQueueType, n int64) {
		t.Helper()
		waitUntil(ctx, t, fmt.Sprintf("a backlog of %d on %s", n, queue), func() bool {
			resp, err := c.WorkflowService().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{Namespace: "default", TaskQueue: &taskqueuepb.TaskQueue{Name: queue}, TaskQueueType: typ, ReportStats: true})
			if err != nil {
				t.Fatal(err)
			}
			return resp.GetStats().GetApproximateBacklogCount() == n
		})
	}
	taskQueues := func(env map[string]any) []map[string]any {
		queues := []map[string]any{}
		for _, q := range env["result"].([]any) {
			queues = append(queues, q.(map[string]any))
		}
		return queues
	}
	section := func(q map[string]any, typ string) map[string]any {
		s, _ := q[typ].(map[string]any)
		return s
	}
	identities := func(s map[string]any) []string {
		ids := []string{}
		for _, p := range s["pollers"].([]any) {
			ids = append(ids, p.(map[string]any)["identity"].(string))
		}
		return ids
	}

	t.Run("get_workflow_history", func(t *testing.T) {
		desc, err := c.DescribeWorkflowExecution(ctx, "order-88231", "")
		if err != nil {
			t.Fatal(err)
		}
		length := desc.GetWorkflowExecutionInfo().GetHistoryLength()
		ids := func(events []map[string]any) []float64 {
			out := []float64{}
			for _, e := range events {
				out = append(out, e["event_id"].(float64))
			}
			return out
		}
		input := decoded(map[string]any{"order_id": "88231", "catalog_should_fail": true, "retry_interval_seconds": float64(1)})

		var scheduledID, failedID int64
		t.Run("failures", func(t *testing.T) {
			env := callTool(t, "get_workflow_history", map[string]any{"target": "local", "workflow_id": "order-88231", "event_types": []any{"failures"}})
			events := historyEvents(env)
			if env["target"] != "dev" || env["complete"] != true || env["next_call"] != nil {
				t.Errorf("envelope = %#v, want target dev, complete and no next_call", env)
			}
			if got, want := typesOf(events), []string{"ActivityTaskFailed", "WorkflowExecutionFailed"}; !slices.Equal(got, want) {
				t.Fatalf("event types = %v, want %v", got, want)
			}
			failedID = int64(events[0]["event_id"].(float64))
			scheduledID = int64(attributes(events[0])["scheduled_event_id"].(float64))
		})

		t.Run("latest run", func(t *testing.T) {
			env := callTool(t, "get_workflow_history", map[string]any{"target": "dev", "workflow_id": "continue-once", "order": "newest_first", "limit": 1})
			if run, got := env["result"].(map[string]any)["run_id"], typesOf(historyEvents(env)); run != latestRunID || !slices.Equal(got, []string{"WorkflowExecutionCompleted"}) {
				t.Errorf("run_id = %v, event types = %v; want run %s and its WorkflowExecutionCompleted", run, got, latestRunID)
			}
		})

		t.Run("attributes", func(t *testing.T) {
			events := historyEvents(callTool(t, "get_workflow_history", map[string]any{"target": "dev", "workflow_id": "order-88231", "event_range": map[string]any{"last": scheduledID}}))
			started, scheduled := events[0], events[len(events)-1]
			a := attributes(started)
			if started["event_type"] != "WorkflowExecutionStarted" || !reflect.DeepEqual(a["input"], []any{input}) || !reflect.DeepEqual(a["workflow_type"], map[string]any{"name": "OrderWorkflow"}) ||
				!reflect.DeepEqual(a["search_attributes"], map[string]any{"OrderId": "88231"}) {
				t.Errorf("first event = %#v, want WorkflowExecutionStarted with the order's input, type and search attributes", started)
			}
			if scheduled["event_type"] != "ActivityTaskScheduled" {
				t.Fatalf("event %v = %#v, want ActivityTaskScheduled", scheduledID, scheduled)
			}
			a = attributes(scheduled)
			want := map[string]any{
				"activity_type":          map[string]any{"name": "enrich_product"},
				"input":                  []any{input},
				"start_to_close_timeout": "10s",
			}
			for k, v := range want {
				if !reflect.DeepEqual(a[k], v) {
					t.Errorf("%s = %#v, want %#v", k, a[k], v)
				}
			}
			if a["task_queue"].(map[string]any)["name"] != fixture.TaskQueue || a["retry_policy"].(map[string]any)["initial_interval"] != "1s" {
				t.Errorf("task_queue = %#v, retry_policy = %#v; want task queue %s and an initial interval of 1s", a["task_queue"], a["retry_policy"], fixture.TaskQueue)
			}
		})

		t.Run("payload handling", func(t *testing.T) {
			a := attributes(historyEvents(callTool(t, "get_workflow_history", map[string]any{"target": "dev-omit", "workflow_id": "order-88231", "limit": 1}))[0])
			stored := encode(t, fixture.OrderInput{OrderID: "88231", CatalogShouldFail: true, RetryIntervalSeconds: 1})
			omitted := map[string]any{"handling": "omitted", "reason": "payload_handling", "encoding": "binary/zlib", "size_bytes": float64(len(stored.GetData()))}
			if !reflect.DeepEqual(a["input"], []any{omitted}) || !reflect.DeepEqual(a["search_attributes"], map[string]any{"OrderId": "88231"}) {
				t.Errorf("first event's input = %#v, search_attributes = %#v; want the input omitted with its encoding and size, and the search attributes decoded", a["input"], a["search_attributes"])
			}
		})

		t.Run("nexus links", func(t *testing.T) {
			if _, err := c.OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{Spec: &nexuspb.EndpointSpec{
				Name:   "greeter",
				Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{Namespace: handlerNamespace, TaskQueue: "greeter"}}},
			}}); err != nil {
				t.Fatal(err)
			}
			hc, err := client.NewClientFromExisting(c, client.Options{Namespace: handlerNamespace, DataConverter: encoded.DataConverter})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(hc.Close)
			service := nexus.NewService("greeter")
			if err := service.Register(greetOperation); err != nil {
				t.Fatal(err)
			}
			handler := worker.New(hc, "greeter", worker.Options{})
			handler.RegisterWorkflow(greet)
			handler.RegisterNexusService(service)
			caller := worker.New(c, "greeter-caller", worker.Options{})
			caller.RegisterWorkflow(callGreet)
			for _, w := range []worker.Worker{handler, caller} {
				if err := w.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(w.Stop)
			}
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "greet-caller", TaskQueue: "greeter-caller"}, callGreet, "ada")
			if err != nil {
				t.Fatal(err)
			}
			if err := run.Get(ctx, nil); err != nil {
				t.Fatal(err)
			}
			link := func(namespace, workflowID, runID string, eventID any, eventType string) []any {
				return []any{map[string]any{"workflow_event": map[string]any{"namespace": namespace, "workflow_id": workflowID, "run_id": runID, "event_ref": map[string]any{"event_id": eventID, "event_type": eventType}}}}
			}
			handled := callTool(t, "get_workflow_history", map[string]any{"target": "dev-handler", "workflow_id": "greet-ada", "event_types": []any{"WorkflowExecutionStarted"}})
			events := historyEvents(callTool(t, "get_workflow_history", map[string]any{"target": "dev", "workflow_id": "greet-caller", "event_types": []any{"WorkflowExecutionStarted", "NexusOperationStarted"}}))
			if got := events[0]["links"]; !reflect.DeepEqual(got, []any{}) {
				t.Errorf("caller's WorkflowExecutionStarted links = %#v, want none", got)
			}
			started := events[1]
			if got, want := started["links"], link(handlerNamespace, "greet-ada", handled["result"].(map[string]any)["run_id"].(string), 1.0, "WorkflowExecutionStarted"); !reflect.DeepEqual(got, want) {
				t.Errorf("NexusOperationStarted links = %#v, want the handler's run in %s and its WorkflowExecutionStarted %#v", got, handlerNamespace, want)
			}
			// The server keeps the handler's link back to the caller on the completion callback, not in the event's links.
			callback := attributes(historyEvents(handled)[0])["completion_callbacks"].([]any)[0].(map[string]any)
			if got, want := callback["links"], link("default", "greet-caller", run.GetRunID(), attributes(started)["scheduled_event_id"], "NexusOperationScheduled"); !reflect.DeepEqual(got, want) {
				t.Errorf("handler's completion callback links = %#v, want the caller's NexusOperationScheduled %#v", got, want)
			}
		})

		t.Run("filters and order", func(t *testing.T) {
			all := []int64{}
			for id := length; id > 0; id-- {
				all = append(all, id)
			}
			tests := []struct {
				order       string
				eventTypes  []any
				first, last int64
				want        []int64
			}{
				{"newest_first", nil, 0, 0, all},
				{"newest_first", nil, 2, 4, []int64{4, 3, 2}},
				{"newest_first", nil, length + 10, 0, nil},
				{"newest_first", nil, length - 2, 0, []int64{length, length - 1, length - 2}},
				{"newest_first", nil, 0, 3, []int64{3, 2, 1}},
				{"oldest_first", nil, length - 2, 0, []int64{length - 2, length - 1, length}},
				{"oldest_first", nil, 0, 3, []int64{1, 2, 3}},
				{"oldest_first", []any{"failures"}, 0, length - 1, []int64{failedID}},
				{"oldest_first", []any{"failures"}, scheduledID, scheduledID, nil},
			}
			for _, tt := range tests {
				bounds := map[string]any{}
				if tt.first != 0 {
					bounds["first"] = tt.first
				}
				if tt.last != 0 {
					bounds["last"] = tt.last
				}
				args := map[string]any{"target": "dev", "workflow_id": "order-88231", "run_id": failed.GetRunID(), "order": tt.order, "limit": 100}
				if len(bounds) > 0 {
					args["event_range"] = bounds
				}
				if tt.eventTypes != nil {
					args["event_types"] = tt.eventTypes
				}
				env := callTool(t, "get_workflow_history", args)
				want := []float64{}
				for _, id := range tt.want {
					want = append(want, float64(id))
				}
				if got := ids(historyEvents(env)); !slices.Equal(got, want) || env["complete"] != true {
					t.Errorf("%s %v first %d last %d: event IDs = %v, complete = %v; want %v, complete", tt.order, tt.eventTypes, tt.first, tt.last, got, env["complete"], want)
				}
			}
		})

		t.Run("pages one event at a time", func(t *testing.T) {
			events, calls := pageThrough(t, "get_workflow_history", map[string]any{"target": "dev", "workflow_id": "order-88231", "limit": 1})
			want := []float64{}
			for id := range length {
				want = append(want, float64(id+1))
			}
			if got := ids(events); !slices.Equal(got, want) {
				t.Errorf("event IDs = %v, want %v", got, want)
			}
			if calls[0] != 2 || slices.ContainsFunc(calls[1:], func(n float64) bool { return n != 1 }) {
				t.Errorf("temporal_calls = %v, want 2 for the first call, which finds the run, and 1 for each after", calls)
			}
		})

		t.Run("stops a filtered read after a fixed number of pages", func(t *testing.T) {
			if _, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-signaled", TaskQueue: "unpolled-signals"}, waitForSignal); err != nil {
				t.Fatal(err)
			}
			want := []float64{}
			for i := range 60 {
				if err := c.SignalWorkflow(ctx, "order-signaled", "", "note", i); err != nil {
					t.Fatal(err)
				}
				want = append(want, float64(i+3))
			}
			events, calls := pageThrough(t, "get_workflow_history", map[string]any{"target": "dev", "workflow_id": "order-signaled", "event_types": []any{"WorkflowExecutionSignaled"}, "limit": 100})
			if got := ids(events); !slices.Equal(got, want) {
				t.Errorf("event IDs = %v, want %v", got, want)
			}
			if calls[0] != 1+maxHistoryPages || len(calls) < 2 {
				t.Errorf("temporal_calls = %v, want the run and %d pages for the first call, and more calls", calls, maxHistoryPages)
			}
		})

		t.Run("newest first continues from the last event it returned", func(t *testing.T) {
			if _, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-signaled-newest", TaskQueue: "unpolled-signals"}, waitForSignal); err != nil {
				t.Fatal(err)
			}
			for range 4 {
				if err := c.SignalWorkflow(ctx, "order-signaled-newest", "", "note", nil); err != nil {
					t.Fatal(err)
				}
			}
			args := map[string]any{"target": "dev", "workflow_id": "order-signaled-newest", "event_types": []any{"WorkflowExecutionSignaled"}, "order": "newest_first", "limit": 1}
			env := callTool(t, "get_workflow_history", args)
			next, _ := env["next_call"].(map[string]any)
			if got := ids(historyEvents(env)); !slices.Equal(got, []float64{6}) || next == nil {
				t.Fatalf("first call: event IDs = %v, next_call = %v; want [6] and a next_call", got, env["next_call"])
			}
			for range 10 {
				if err := c.SignalWorkflow(ctx, "order-signaled-newest", "", "note", nil); err != nil {
					t.Fatal(err)
				}
			}
			events, _ := pageThrough(t, next["name"].(string), next["arguments"].(map[string]any))
			if got := ids(events); !slices.Equal(got, []float64{5, 4, 3}) {
				t.Errorf("event IDs after the first call = %v, want [5 4 3]: each event once, newest first, none that arrived later", got)
			}
		})

		first := callTool(t, "get_workflow_history", map[string]any{"target": "dev", "workflow_id": "order-88231", "limit": 1})
		continuation := first["next_call"].(map[string]any)["arguments"].(map[string]any)["continuation"]
		second := callTool(t, "get_workflow_history", first["next_call"].(map[string]any)["arguments"].(map[string]any))
		secondContinuation := second["next_call"].(map[string]any)["arguments"].(map[string]any)["continuation"].(string)
		damaged := secondContinuation[:len(secondContinuation)-4]
		search := callTool(t, "search_workflows", map[string]any{"target": "dev", "query": pagesQuery, "page_size": 2})
		const invalid = "continuation is not one that get_workflow_history returned for these arguments; call get_workflow_history again without it"
		errorTests := []struct {
			name    string
			args    map[string]any
			code    string
			message string
		}{
			{"unknown event types", map[string]any{"event_types": []any{"Nope", "ActivityTaskFailed", "failure", "Unspecified"}}, "invalid_argument",
				`these event_types are neither event types nor categories: "Nope", "failure", "Unspecified"; give event types as get_workflow_history returns them in event_type, such as ActivityTaskFailed, or the category failures`},
			{"range that ends before it starts", map[string]any{"event_range": map[string]any{"first": 5, "last": 4}}, "invalid_argument", "event_range: last must not be less than first"},
			{"continuation for another workflow", map[string]any{"workflow_id": "order-completed", "continuation": continuation}, "continuation_invalid", invalid},
			{"continuation for another filter", map[string]any{"run_id": failed.GetRunID(), "event_types": []any{"failures"}, "continuation": continuation}, "continuation_invalid", invalid},
			{"continuation from search_workflows", map[string]any{"continuation": search["next_call"].(map[string]any)["arguments"].(map[string]any)["continuation"]}, "continuation_invalid", invalid},
			{"damaged page token", map[string]any{"continuation": damaged}, "continuation_invalid", invalid},
			{"unknown workflow", map[string]any{"workflow_id": "order-0"}, "not_found", ""},
			{"empty workflow_id", map[string]any{"workflow_id": ""}, "invalid_argument", "workflow_id is required"},
		}
		for _, tt := range errorTests {
			t.Run(tt.name, func(t *testing.T) {
				args := map[string]any{"target": "dev", "workflow_id": "order-88231"}
				maps.Copy(args, tt.args)
				got := callToolError(t, "get_workflow_history", args)
				if msg, _ := got["message"].(string); got["code"] != tt.code || msg == "" || tt.message != "" && msg != tt.message {
					t.Errorf("error = %#v, want code %s and message %q", got, tt.code, tt.message)
				}
			})
		}
	})

	t.Run("get_task_queues", func(t *testing.T) {
		tooLong := strings.Repeat("q", 1001)
		env, calls := callCounted(t, "get_task_queues", map[string]any{"target": "local", "task_queues": []any{fixture.TaskQueue, tooLong, "never-used"}})
		queues := taskQueues(env)
		if env["target"] != "dev" || env["complete"] != true || env["next_call"] != nil || len(queues) != 3 || calls != 7 {
			t.Fatalf("envelope = %#v, temporal_calls = %v; want target dev, complete, no next_call, three task queues and seven calls", env, calls)
		}
		polled, invalid, unused := queues[0], queues[1], queues[2]
		for _, typ := range []string{"workflow", "activity", "nexus"} {
			s := section(polled, typ)
			if observed, _ := s["observed_at"].(string); !strings.HasSuffix(observed, "Z") || s["backlog_count"] == nil || s["add_rate"] == nil || s["dispatch_rate"] == nil || s["poller_count"] != float64(len(s["pollers"].([]any))) {
				t.Errorf("%s section = %#v, want observed_at in UTC, a backlog_count, an add_rate, a dispatch_rate and every poller", typ, s)
			}
			if wantPollers := typ != "nexus"; (len(identities(s)) > 0) != wantPollers {
				t.Errorf("%s pollers = %v, want some: %v", typ, identities(s), wantPollers)
			}
			if s := section(unused, typ); len(identities(s)) != 0 || s["backlog_count"] != 0.0 {
				t.Errorf("%s section of an unused task queue = %#v, want no pollers and no backlog", typ, s)
			}
		}
		if polled["task_queue"] != fixture.TaskQueue || polled["error"] != nil || unused["task_queue"] != "never-used" || unused["error"] != nil {
			t.Errorf("task queues = %v and %v, want %s and never-used with no error", polled["task_queue"], unused["task_queue"], fixture.TaskQueue)
		}
		got, _ := invalid["error"].(map[string]any)
		if invalid["task_queue"] != tooLong || invalid["workflow"] != nil || invalid["activity"] != nil || invalid["nexus"] != nil || got["code"] != "invalid_argument" || got["retryable"] != false || got["message"] == "" {
			t.Errorf("entry for a name that is too long = %#v, want no sections and an invalid_argument error", invalid)
		}

		env, calls = callCounted(t, "get_task_queues", map[string]any{"target": "dev", "task_queues": []any{fixture.TaskQueue, fixture.TaskQueue}, "types": []any{"activity"}})
		queues = taskQueues(env)
		for _, q := range queues {
			if q["task_queue"] != fixture.TaskQueue || section(q, "activity") == nil || section(q, "workflow") != nil || section(q, "nexus") != nil {
				t.Errorf("entry = %#v, want only the activity section of %s", q, fixture.TaskQueue)
			}
		}
		if len(queues) != 2 || calls != 2 {
			t.Errorf("%d entries, temporal_calls = %v; want an entry for each name given, from a call each", len(queues), calls)
		}

		t.Run("unreachable target", func(t *testing.T) {
			records.Reset()
			if got := callToolError(t, "get_task_queues", map[string]any{"target": "down", "task_queues": []any{fixture.TaskQueue, fixture.TaskQueue}}); got["code"] != "temporal_unavailable" || got["retryable"] != true || got["message"] != "task_queues[0]: could not connect to the Temporal server" {
				t.Errorf("error = %#v, want a retryable temporal_unavailable error naming task_queues[0]", got)
			}
			// Reading stops at the first task queue's first type, so temporal_calls counts only that call's attempts.
			if calls := temporalCalls(t); calls != float64(1+len(retryDelays)) {
				t.Errorf("temporal_calls = %v, want %d", calls, 1+len(retryDelays))
			}
		})
	})

	t.Run("non-determinism population", func(t *testing.T) {
		v1 := fixture.NewFulfillmentWorker(c, 1)
		if err := v1.Start(); err != nil {
			t.Fatal(err)
		}
		shippingBranch := map[string]bool{"nde-affected": true, "nde-clean": false, "nde-unexposed": true}
		for id, branch := range shippingBranch {
			if _, err := fixture.StartFulfillment(ctx, c, id, branch); err != nil {
				t.Fatal(err)
			}
		}
		for id := range shippingBranch {
			waitForDescribe(ctx, t, c, id, id+"'s first workflow task", func(desc *workflowservice.DescribeWorkflowExecutionResponse) bool {
				return desc.GetPendingWorkflowTask() == nil && desc.GetWorkflowExecutionInfo().GetHistoryLength() >= 4
			})
		}
		v1.Stop()
		worker.PurgeStickyWorkflowCache()
		v2 := fixture.NewFulfillmentWorker(c, 2)
		if err := v2.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(v2.Stop)
		for _, id := range []string{"nde-affected", "nde-clean"} {
			if err := c.SignalWorkflow(ctx, id, "", "deployment_probe", nil); err != nil {
				t.Fatal(err)
			}
		}
		waitForDescribe(ctx, t, c, "nde-affected", "a retry of the failed workflow task", func(desc *workflowservice.DescribeWorkflowExecutionResponse) bool {
			return desc.GetPendingWorkflowTask().GetAttempt() >= 2
		})
		waitForDescribe(ctx, t, c, "nde-clean", "the probe's workflow task", func(desc *workflowservice.DescribeWorkflowExecutionResponse) bool {
			return desc.GetPendingWorkflowTask() == nil && desc.GetWorkflowExecutionInfo().GetHistoryLength() >= 8
		})

		failures := func(t *testing.T, id string) []map[string]any {
			t.Helper()
			env := callTool(t, "get_workflow_history", map[string]any{"target": "dev", "workflow_id": id, "event_types": []any{"failures"}})
			if env["complete"] != true {
				t.Errorf("%s: complete = false, want true", id)
			}
			return historyEvents(env)
		}
		affected := failures(t, "nde-affected")
		if got := typesOf(affected); !slices.Equal(got, []string{"WorkflowTaskFailed"}) {
			t.Fatalf("nde-affected failures = %v, want one WorkflowTaskFailed: later attempts are not recorded", got)
		}
		a := attributes(affected[0])
		if a["cause"] != "NonDeterministicError" {
			t.Errorf("WorkflowTaskFailed attributes = %#v, want cause NonDeterministicError", a)
		}
		newest := historyEvents(callTool(t, "get_workflow_history", map[string]any{"target": "dev", "workflow_id": "nde-affected", "event_types": []any{"failures"}, "order": "newest_first", "limit": 1}))
		if len(newest) != 1 || newest[0]["event_id"] != affected[0]["event_id"] {
			t.Errorf("newest failure = %#v, want event %v", newest, affected[0]["event_id"])
		}
		for _, id := range []string{"nde-clean", "nde-unexposed"} {
			if got := failures(t, id); len(got) != 0 {
				t.Errorf("%s failures = %v, want none", id, typesOf(got))
			}
		}
	})

	t.Run("stuck workflow", func(t *testing.T) {
		w := fixture.NewStuckWorker(c)
		if err := w.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(w.Stop)
		if err := fixture.StartStuckLedger(ctx, c); err != nil {
			t.Fatal(err)
		}
		waitForBacklog(t, fixture.LedgerTaskQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY, fixture.StuckLedgerActivities)
		ledger := taskQueues(callTool(t, "get_task_queues", map[string]any{"target": "dev", "task_queues": []any{fixture.LedgerTaskQueue}}))[0]
		if got := identities(section(ledger, "workflow")); !slices.Contains(got, fixture.StuckWorkerIdentity) {
			t.Errorf("workflow pollers = %v, want %s among them", got, fixture.StuckWorkerIdentity)
		}
		activities := section(ledger, "activity")
		age, err := strconv.ParseFloat(strings.TrimSuffix(fmt.Sprint(activities["backlog_age"]), "s"), 64)
		if len(identities(activities)) != 0 || activities["backlog_count"] != float64(fixture.StuckLedgerActivities) || err != nil || age <= 0 {
			t.Errorf("activity section = %#v, want no pollers and a backlog of %d with an age", activities, fixture.StuckLedgerActivities)
		}
		if got := identities(section(ledger, "nexus")); len(got) != 0 {
			t.Errorf("nexus pollers = %v, want none", got)
		}

		env := callTool(t, "get_workflow_history", map[string]any{"target": "dev", "workflow_id": fixture.StuckLedgerWorkflowID, "order": "newest_first", "limit": 3})
		events := historyEvents(env)
		if got := typesOf(events); !slices.Equal(got, []string{"ActivityTaskScheduled", "ActivityTaskScheduled", "ActivityTaskScheduled"}) || env["next_call"] == nil {
			t.Fatalf("newest events = %v, next_call = %v; want three ActivityTaskScheduled and a next_call", got, env["next_call"])
		}
		for _, e := range events {
			if a := attributes(e); a["activity_type"].(map[string]any)["name"] != "FetchLedgerBatch" || a["task_queue"].(map[string]any)["name"] != fixture.LedgerTaskQueue {
				t.Errorf("event %v attributes = %#v, want a FetchLedgerBatch activity on %s", e["event_id"], a, fixture.LedgerTaskQueue)
			}
		}
	})

	t.Run("incident window", func(t *testing.T) {
		w := fixture.NewIncidentWorker(c)
		if err := w.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(w.Stop)
		start := func(id string, entry fixture.LedgerEntry) client.WorkflowRun {
			t.Helper()
			run, err := fixture.StartLedgerEntry(ctx, c, id, entry)
			if err != nil {
				t.Fatal(err)
			}
			return run
		}
		start("ledger-payments-before", fixture.LedgerEntry{Domain: "payments"})
		windowStart := time.Now().UTC()
		recovered := start("ledger-payments-recovered", fixture.LedgerEntry{Domain: "payments"})
		rejected := start("ledger-payments-rejected", fixture.LedgerEntry{Domain: "payments", Terminal: true})
		start("ledger-core", fixture.LedgerEntry{Domain: "core"})
		windowEnd := time.Now().UTC()
		waitForBacklog(t, fixture.PaymentsTaskQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 3)

		payments := func() map[string]any {
			return section(taskQueues(callTool(t, "get_task_queues", map[string]any{"target": "dev", "task_queues": []any{fixture.PaymentsTaskQueue}, "types": []any{"activity"}}))[0], "activity")
		}
		if s := payments(); len(identities(s)) != 0 || s["backlog_count"] != 3.0 {
			t.Errorf("payments activity section during the incident = %#v, want no pollers and a backlog of 3", s)
		}

		query := fmt.Sprintf("StartTime BETWEEN '%s' AND '%s' AND Domain = 'payments'", windowStart.Format(time.RFC3339Nano), windowEnd.Format(time.RFC3339Nano))
		waitUntil(ctx, t, "Visibility to show the incident's runs", func() bool {
			n, err := c.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: query})
			if err != nil {
				t.Fatal(err)
			}
			return n.GetCount() == 2
		})
		found := []string{}
		for _, row := range callTool(t, "search_workflows", map[string]any{"target": "dev", "query": query})["result"].(map[string]any)["workflows"].([]any) {
			found = append(found, row.(map[string]any)["workflow_id"].(string))
		}
		if slices.Sort(found); !slices.Equal(found, []string{"ledger-payments-recovered", "ledger-payments-rejected"}) {
			t.Errorf("payments runs started in the window = %v, want the recovered and rejected ones", found)
		}

		recoveryStart := time.Now().UTC()
		recovery := fixture.NewRecoveryWorker(c)
		if err := recovery.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(recovery.Stop)
		if err := recovered.Get(ctx, nil); err != nil {
			t.Fatal(err)
		}
		if err := rejected.Get(ctx, nil); err == nil {
			t.Fatal("ledger-payments-rejected completed, want it to fail")
		}
		waitForBacklog(t, fixture.PaymentsTaskQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 0)
		if s := payments(); !slices.Contains(identities(s), fixture.RecoveryWorkerIdentity) || s["backlog_count"] != 0.0 {
			t.Errorf("payments activity section after recovery = %#v, want %s polling and no backlog", s, fixture.RecoveryWorkerIdentity)
		}

		events := historyEvents(callTool(t, "get_workflow_history", map[string]any{"target": "dev", "workflow_id": "ledger-payments-recovered", "event_types": []any{"ActivityTaskScheduled", "ActivityTaskStarted"}}))
		if got := typesOf(events); !slices.Equal(got, []string{"ActivityTaskScheduled", "ActivityTaskStarted"}) {
			t.Fatalf("activity events = %v, want ActivityTaskScheduled and ActivityTaskStarted", got)
		}
		scheduledAt, _ := time.Parse(time.RFC3339Nano, events[0]["event_time"].(string))
		startedAt, _ := time.Parse(time.RFC3339Nano, events[1]["event_time"].(string))
		started := attributes(events[1])
		if msg, _ := started["last_failure"].(map[string]any)["message"].(string); started["attempt"] != 2.0 || !strings.Contains(msg, "LedgerServiceUnavailable") {
			t.Errorf("ActivityTaskStarted attributes = %#v, want attempt 2 after a LedgerServiceUnavailable failure", started)
		}
		if !scheduledAt.Before(recoveryStart) || !startedAt.After(recoveryStart) {
			t.Errorf("activity scheduled at %v and started at %v, want scheduled during the incident and started after recovery began at %v", scheduledAt, startedAt, recoveryStart)
		}

		events = historyEvents(callTool(t, "get_workflow_history", map[string]any{"target": "dev", "workflow_id": "ledger-payments-rejected", "event_types": []any{"failures"}}))
		if got := typesOf(events); !slices.Equal(got, []string{"ActivityTaskFailed", "WorkflowExecutionFailed"}) {
			t.Fatalf("failures = %v, want ActivityTaskFailed and WorkflowExecutionFailed", got)
		}
		if got := attributes(events[0])["failure"].(map[string]any)["application"]; !reflect.DeepEqual(got, map[string]any{"type": "LedgerWriteRejected", "non_retryable": true}) {
			t.Errorf("ActivityTaskFailed application failure = %#v, want the non-retryable LedgerWriteRejected", got)
		}
	})
}
