// The get_workflows tool, which returns the current state of each workflow run in a list
// and, for a closed run, its result or failure.

package server

import (
	"cmp"
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
	"github.com/temporalio/temporal-operator-mcp/internal/payload"
	"github.com/temporalio/temporal-operator-mcp/internal/temporal"
)

const callTimeout = 10 * time.Second

// The waits before each retry of an unavailable server: 1.4s in all, which spans the first reconnect attempts of the Temporal and Cloud Ops clients and still reports a server that stays down well inside callTimeout.
var retryDelays = []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond}

type getWorkflowsInput struct {
	Target    string        `json:"target" jsonschema:"the name or an alias of a target"`
	Workflows []workflowRef `json:"workflows" jsonschema:"the workflow runs to get"`
}

type workflowRef struct {
	WorkflowID string `json:"workflow_id" jsonschema:"the workflow ID"`
	RunID      string `json:"run_id,omitempty" jsonschema:"the run ID; the latest run of the workflow if omitted"`
}

type workflowEntry struct {
	workflowRef
	Run   *workflowRun `json:"run"`
	Error *toolError   `json:"error"`
}

type workflowRun struct {
	WorkflowID          string                    `json:"workflow_id"`
	RunID               string                    `json:"run_id"`
	Type                string                    `json:"type"`
	Status              string                    `json:"status"`
	TaskQueue           string                    `json:"task_queue"`
	StartTime           *time.Time                `json:"start_time"`
	ExecutionTime       *time.Time                `json:"execution_time"`
	CloseTime           *time.Time                `json:"close_time"`
	HistoryLength       int64                     `json:"history_length"`
	Parent              *execution                `json:"parent"`
	PendingActivities   []pendingActivity         `json:"pending_activities"`
	PendingChildren     []pendingChild            `json:"pending_children"`
	PendingWorkflowTask *pendingWorkflowTask      `json:"pending_workflow_task"`
	Memo                map[string]*payload.Field `json:"memo"`
	SearchAttributes    map[string]any            `json:"search_attributes"`
	Result              []*payload.Field          `json:"result"`
	Failure             *failure                  `json:"failure"`
	NewExecutionRunID   *string                   `json:"new_execution_run_id"`
	ObservedAt          time.Time                 `json:"observed_at"`
}

type execution struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

type typeName struct {
	Name string `json:"name"`
}

type pendingActivity struct {
	ActivityID        string           `json:"activity_id"`
	Type              string           `json:"type"`
	State             string           `json:"state"`
	Attempt           int32            `json:"attempt"`
	ScheduledTime     *time.Time       `json:"scheduled_time"`
	LastStartedTime   *time.Time       `json:"last_started_time"`
	LastHeartbeatTime *time.Time       `json:"last_heartbeat_time"`
	LastFailure       *failure         `json:"last_failure"`
	HeartbeatDetails  []*payload.Field `json:"heartbeat_details"`
}

type pendingChild struct {
	WorkflowID        string `json:"workflow_id"`
	RunID             string `json:"run_id"`
	Type              string `json:"type"`
	InitiatedID       int64  `json:"initiated_id"`
	ParentClosePolicy string `json:"parent_close_policy"`
}

type pendingWorkflowTask struct {
	State                 string     `json:"state"`
	ScheduledTime         *time.Time `json:"scheduled_time"`
	OriginalScheduledTime *time.Time `json:"original_scheduled_time"`
	StartedTime           *time.Time `json:"started_time"`
	Attempt               int32      `json:"attempt"`
}

type failure struct {
	Message                string                         `json:"message"`
	Kind                   string                         `json:"kind"`
	StackTrace             string                         `json:"stack_trace"`
	Application            *applicationFailure            `json:"application"`
	Timeout                *timeoutFailure                `json:"timeout"`
	Canceled               *canceledFailure               `json:"canceled"`
	Terminated             *terminatedFailure             `json:"terminated"`
	Server                 *serverFailure                 `json:"server"`
	Activity               *activityFailure               `json:"activity"`
	ChildWorkflowExecution *childWorkflowExecutionFailure `json:"child_workflow_execution"`
	NexusOperation         *nexusOperationFailure         `json:"nexus_operation"`
	NexusHandler           *nexusHandlerFailure           `json:"nexus_handler"`
	Details                []*payload.Field               `json:"details"`
	EncodedAttributes      *payload.Field                 `json:"encoded_attributes"`
	LastHeartbeatDetails   []*payload.Field               `json:"last_heartbeat_details"`
	Cause                  *failure                       `json:"cause"`
}

// failureFields builds failure's $defs entry; jsonschema.For[failure] would return the $ref override.
type failureFields failure

func getWorkflowsInputSchema() *jsonschema.Schema {
	schema := schemaFor[getWorkflowsInput]("get_workflows", "input", nil)
	workflows := schema.Properties["workflows"]
	boundList(workflows, workflows.Items.Properties["workflow_id"])
	// Null-valued arguments are dropped only at the top level, so a null run_id in an item is allowed here.
	runID := workflows.Items.Properties["run_id"]
	runID.Type, runID.Types = "", []string{"null", "string"}
	return schema
}

func getWorkflowsOutputSchema() *jsonschema.Schema {
	payloadDef := schemaFor[payload.Field]("get_workflows", "payload", nil)
	payloadDef.Type, payloadDef.Types = "", []string{"null", "object"}
	opts := &jsonschema.ForOptions{TypeSchemas: map[reflect.Type]*jsonschema.Schema{
		reflect.TypeFor[failure]():       {Types: []string{"null", "object"}, Ref: "#/$defs/failure"},
		reflect.TypeFor[payload.Field](): {Types: []string{"null", "object"}, Ref: "#/$defs/payload"},
	}}
	failureDef := schemaFor[failureFields]("get_workflows", "failure", opts)
	failureDef.Type, failureDef.Types = "", []string{"null", "object"}
	schema := schemaFor[envelope[[]workflowEntry]]("get_workflows", "output", opts)
	schema.Defs = map[string]*jsonschema.Schema{"failure": failureDef, "payload": payloadDef}
	return schema
}

type applicationFailure struct {
	Type           string  `json:"type"`
	NonRetryable   bool    `json:"non_retryable"`
	NextRetryDelay *string `json:"next_retry_delay"`
	Category       string  `json:"category"`
}

type timeoutFailure struct {
	TimeoutType string `json:"timeout_type"`
}

type canceledFailure struct {
	Identity string `json:"identity"`
}

type terminatedFailure struct {
	Identity string `json:"identity"`
}

type serverFailure struct {
	NonRetryable bool `json:"non_retryable"`
}

type activityFailure struct {
	ScheduledEventID int64    `json:"scheduled_event_id"`
	StartedEventID   int64    `json:"started_event_id"`
	Identity         string   `json:"identity"`
	ActivityType     typeName `json:"activity_type"`
	ActivityID       string   `json:"activity_id"`
	RetryState       string   `json:"retry_state"`
}

type childWorkflowExecutionFailure struct {
	Namespace         string    `json:"namespace"`
	WorkflowExecution execution `json:"workflow_execution"`
	WorkflowType      typeName  `json:"workflow_type"`
	InitiatedEventID  int64     `json:"initiated_event_id"`
	StartedEventID    int64     `json:"started_event_id"`
	RetryState        string    `json:"retry_state"`
}

type nexusOperationFailure struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	Endpoint         string `json:"endpoint"`
	Service          string `json:"service"`
	Operation        string `json:"operation"`
	OperationToken   string `json:"operation_token"`
}

type nexusHandlerFailure struct {
	Type          string `json:"type"`
	RetryBehavior string `json:"retry_behavior"`
}

func getWorkflows(cfg *config.Config, clients *temporal.Clients, payloads *payload.Renderer) mcp.ToolHandlerFor[getWorkflowsInput, envelope[[]workflowEntry]] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in getWorkflowsInput) (*mcp.CallToolResult, envelope[[]workflowEntry], error) {
		var none envelope[[]workflowEntry]
		t, err := lookupTarget(ctx, cfg, in.Target)
		if err != nil {
			return nil, none, err
		}
		svc := clients.Client(t.Name).WorkflowService()
		batch := payloads.Batch(t.Name)
		entries := []workflowEntry{}
		for i, w := range in.Workflows {
			e := workflowEntry{workflowRef: w}
			e.Run, err = readRun(ctx, t, svc, w, batch)
			if e.Error, err = itemError(err, "workflows", i); err != nil {
				return nil, none, err
			}
			entries = append(entries, e)
		}
		if err := batch.Render(ctx); err != nil {
			return nil, none, err
		}
		return nil, envelope[[]workflowEntry]{Target: &t.Name, Result: entries, Complete: true}, nil
	}
}

func readRun(ctx context.Context, t *config.Target, svc workflowservice.WorkflowServiceClient, w workflowRef, batch *payload.Batch) (*workflowRun, error) {
	desc, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return svc.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: t.Namespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: w.WorkflowID, RunId: w.RunID},
		}, opts...)
	})
	if err != nil {
		return nil, err
	}
	run, err := describedRun(desc, batch)
	if err != nil {
		return nil, err
	}
	// Check the close time, not the status: a Paused run, for one, is open.
	if desc.GetWorkflowExecutionInfo().GetCloseTime() != nil {
		hist, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
			return svc.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:              t.Namespace,
				Execution:              desc.GetWorkflowExecutionInfo().GetExecution(),
				HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
			}, opts...)
		})
		if err != nil {
			return nil, err
		}
		events := hist.GetHistory().GetEvents()
		if len(events) == 0 {
			return nil, &toolError{Code: "temporal_unavailable", Message: "the Temporal server returned no close event for the closed run"}
		}
		addCloseEvent(&run, events[0], batch)
	}
	return &run, nil
}

func callTemporal[Resp any](ctx context.Context, t *config.Target, rpc func(context.Context, ...grpc.CallOption) (Resp, error)) (Resp, error) {
	return callServer(ctx, t, temporalServer, rpc)
}

// callServer calls server for target t, retrying a few times while server is unavailable, all within the timeout of a Temporal call, and returns its failure as a tool error that names server.
func callServer[Resp any](ctx context.Context, t *config.Target, server string, rpc func(context.Context, ...grpc.CallOption) (Resp, error)) (Resp, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	for attempt := 0; ; attempt++ {
		var p peer.Peer
		resp, err := rpc(ctx, grpc.Peer(&p))
		if err == nil {
			return resp, nil
		}
		if attempt < len(retryDelays) && serviceerror.ToStatus(err).Code() == codes.Unavailable {
			select {
			case <-time.After(retryDelays[attempt]):
				continue
			case <-ctx.Done():
			}
		}
		return resp, temporalError(err, t, &p, server)
	}
}

func describedRun(desc *workflowservice.DescribeWorkflowExecutionResponse, batch *payload.Batch) (workflowRun, error) {
	observedAt := time.Now().UTC()
	info := desc.GetWorkflowExecutionInfo()
	run := workflowRun{
		WorkflowID:        info.GetExecution().GetWorkflowId(),
		RunID:             info.GetExecution().GetRunId(),
		Type:              info.GetType().GetName(),
		Status:            info.GetStatus().String(),
		TaskQueue:         info.GetTaskQueue(),
		StartTime:         timeOf(info.GetStartTime()),
		ExecutionTime:     timeOf(info.GetExecutionTime()),
		CloseTime:         timeOf(info.GetCloseTime()),
		HistoryLength:     info.GetHistoryLength(),
		PendingActivities: []pendingActivity{},
		PendingChildren:   []pendingChild{},
		Memo:              payloadMap(info.GetMemo(), batch),
		ObservedAt:        observedAt,
	}
	if p := info.GetParentExecution(); p != nil {
		run.Parent = &execution{WorkflowID: p.GetWorkflowId(), RunID: p.GetRunId()}
	}
	for _, a := range desc.GetPendingActivities() {
		run.PendingActivities = append(run.PendingActivities, pendingActivity{
			ActivityID:        a.GetActivityId(),
			Type:              a.GetActivityType().GetName(),
			State:             a.GetState().String(),
			Attempt:           a.GetAttempt(),
			ScheduledTime:     timeOf(a.GetScheduledTime()),
			LastStartedTime:   timeOf(a.GetLastStartedTime()),
			LastHeartbeatTime: timeOf(a.GetLastHeartbeatTime()),
			LastFailure:       toFailure(a.GetLastFailure(), batch),
			HeartbeatDetails:  batch.AddAll(a.GetHeartbeatDetails()),
		})
	}
	for _, c := range desc.GetPendingChildren() {
		run.PendingChildren = append(run.PendingChildren, pendingChild{
			WorkflowID:        c.GetWorkflowId(),
			RunID:             c.GetRunId(),
			Type:              c.GetWorkflowTypeName(),
			InitiatedID:       c.GetInitiatedId(),
			ParentClosePolicy: c.GetParentClosePolicy().String(),
		})
	}
	if wt := desc.GetPendingWorkflowTask(); wt != nil {
		run.PendingWorkflowTask = &pendingWorkflowTask{
			State:                 wt.GetState().String(),
			ScheduledTime:         timeOf(wt.GetScheduledTime()),
			OriginalScheduledTime: timeOf(wt.GetOriginalScheduledTime()),
			StartedTime:           timeOf(wt.GetStartedTime()),
			Attempt:               wt.GetAttempt(),
		}
	}
	sa, err := searchAttributes(info.GetSearchAttributes())
	if err != nil {
		return workflowRun{}, err
	}
	run.SearchAttributes = sa
	return run, nil
}

func searchAttributes(sa *commonpb.SearchAttributes) (map[string]any, error) {
	out := map[string]any{}
	for k, p := range sa.GetIndexedFields() {
		v, err := searchAttribute(p)
		if err != nil {
			return nil, &toolError{Code: "temporal_unavailable", Message: fmt.Sprintf("the Temporal server returned search attribute %s in an unreadable form: %v", k, err)}
		}
		out[k] = v
	}
	return out, nil
}

func searchAttribute(p *commonpb.Payload) (any, error) {
	typ, _ := enumspb.IndexedValueTypeFromString(string(p.GetMetadata()["type"]))
	switch typ {
	case enumspb.INDEXED_VALUE_TYPE_INT:
		return decode[int64](p)
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		return decode[float64](p)
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		return decode[bool](p)
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD, enumspb.INDEXED_VALUE_TYPE_TEXT:
		return decode[string](p)
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		return decode[time.Time](p)
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
		return decode[[]string](p)
	}
	return decode[any](p)
}

// decode returns p as a T, or nil if p is null. Like the Temporal server, it also accepts a list of T: nil if it is empty, its one value, or the list.
func decode[T any](p *commonpb.Payload) (any, error) {
	dc := converter.GetDefaultDataConverter()
	var v *T
	if err := dc.FromPayload(p, &v); err == nil {
		if v == nil {
			return nil, nil
		}
		return *v, nil
	}
	var list []T
	if err := dc.FromPayload(p, &list); err != nil {
		return nil, err
	}
	switch len(list) {
	case 0:
		return nil, nil
	case 1:
		return list[0], nil
	}
	return list, nil
}

func addCloseEvent(run *workflowRun, e *historypb.HistoryEvent, batch *payload.Batch) {
	var next string
	switch e.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		a := e.GetWorkflowExecutionCompletedEventAttributes()
		run.Result, next = batch.AddAll(a.GetResult()), a.GetNewExecutionRunId()
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		a := e.GetWorkflowExecutionFailedEventAttributes()
		run.Failure, next = toFailure(a.GetFailure(), batch), a.GetNewExecutionRunId()
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		next = e.GetWorkflowExecutionTimedOutEventAttributes().GetNewExecutionRunId()
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		run.Failure = &failure{Kind: "CanceledFailure", Details: batch.AddAll(e.GetWorkflowExecutionCanceledEventAttributes().GetDetails()), LastHeartbeatDetails: []*payload.Field{}}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		a := e.GetWorkflowExecutionTerminatedEventAttributes()
		run.Failure = &failure{Message: a.GetReason(), Kind: "TerminatedFailure", Terminated: &terminatedFailure{Identity: a.GetIdentity()}, Details: batch.AddAll(a.GetDetails()), LastHeartbeatDetails: []*payload.Field{}}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		next = e.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
	}
	if next != "" {
		run.NewExecutionRunID = &next
	}
}

func toFailure(f *failurepb.Failure, batch *payload.Batch) *failure {
	if f == nil {
		return nil
	}
	out := &failure{
		Message:    f.GetMessage(),
		StackTrace: f.GetStackTrace(),
		Details:    batch.AddAll(f.GetApplicationFailureInfo().GetDetails()),
		Cause:      toFailure(f.GetCause(), batch),
	}
	if ea := f.GetEncodedAttributes(); ea != nil {
		out.EncodedAttributes = batch.Add(ea)
	}
	out.LastHeartbeatDetails = batch.AddAll(cmp.Or(f.GetTimeoutFailureInfo().GetLastHeartbeatDetails(), f.GetResetWorkflowFailureInfo().GetLastHeartbeatDetails()))
	m := f.ProtoReflect()
	if info := m.WhichOneof(m.Descriptor().Oneofs().ByName("failure_info")); info != nil {
		out.Kind = strings.TrimSuffix(string(info.Message().Name()), "Info")
	}
	if a := f.GetApplicationFailureInfo(); a != nil {
		out.Application = &applicationFailure{Type: a.GetType(), NonRetryable: a.GetNonRetryable(), Category: a.GetCategory().String()}
		if d := a.GetNextRetryDelay(); d != nil {
			out.Application.NextRetryDelay = new(durationString(d))
		}
	}
	if t := f.GetTimeoutFailureInfo(); t != nil {
		out.Timeout = &timeoutFailure{TimeoutType: t.GetTimeoutType().String()}
	}
	if c := f.GetCanceledFailureInfo(); c != nil {
		out.Details = batch.AddAll(c.GetDetails())
		out.Canceled = &canceledFailure{Identity: c.GetIdentity()}
	}
	if t := f.GetTerminatedFailureInfo(); t != nil {
		out.Terminated = &terminatedFailure{Identity: t.GetIdentity()}
	}
	if s := f.GetServerFailureInfo(); s != nil {
		out.Server = &serverFailure{NonRetryable: s.GetNonRetryable()}
	}
	if a := f.GetActivityFailureInfo(); a != nil {
		out.Activity = &activityFailure{
			ScheduledEventID: a.GetScheduledEventId(),
			StartedEventID:   a.GetStartedEventId(),
			Identity:         a.GetIdentity(),
			ActivityType:     typeName{Name: a.GetActivityType().GetName()},
			ActivityID:       a.GetActivityId(),
			RetryState:       a.GetRetryState().String(),
		}
	}
	if c := f.GetChildWorkflowExecutionFailureInfo(); c != nil {
		out.ChildWorkflowExecution = &childWorkflowExecutionFailure{
			Namespace:         c.GetNamespace(),
			WorkflowExecution: execution{WorkflowID: c.GetWorkflowExecution().GetWorkflowId(), RunID: c.GetWorkflowExecution().GetRunId()},
			WorkflowType:      typeName{Name: c.GetWorkflowType().GetName()},
			InitiatedEventID:  c.GetInitiatedEventId(),
			StartedEventID:    c.GetStartedEventId(),
			RetryState:        c.GetRetryState().String(),
		}
	}
	if n := f.GetNexusOperationExecutionFailureInfo(); n != nil {
		out.NexusOperation = &nexusOperationFailure{
			ScheduledEventID: n.GetScheduledEventId(),
			Endpoint:         n.GetEndpoint(),
			Service:          n.GetService(),
			Operation:        n.GetOperation(),
			// A failure from before operation_token has the token in the deprecated operation_id.
			OperationToken: cmp.Or(n.GetOperationToken(), n.GetOperationId()),
		}
	}
	if n := f.GetNexusHandlerFailureInfo(); n != nil {
		out.NexusHandler = &nexusHandlerFailure{Type: n.GetType(), RetryBehavior: n.GetRetryBehavior().String()}
	}
	return out
}

// payloadFields is a Memo or a Header.
type payloadFields interface {
	GetFields() map[string]*commonpb.Payload
}

func payloadMap(m payloadFields, batch *payload.Batch) map[string]*payload.Field {
	out := map[string]*payload.Field{}
	for k, p := range m.GetFields() {
		out[k] = batch.Add(p)
	}
	return out
}

func timeOf(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	return &t
}
