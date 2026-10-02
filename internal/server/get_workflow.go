package server

import (
	"cmp"
	"context"
	"encoding/base64"
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
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/temporalio/operator-mcp/internal/config"
	"github.com/temporalio/operator-mcp/internal/temporal"
)

const callTimeout = 10 * time.Second

type getWorkflowInput struct {
	Target     string `json:"target" jsonschema:"the name or an alias of a target from list_available_targets"`
	WorkflowID string `json:"workflow_id" jsonschema:"the workflow ID"`
	RunID      string `json:"run_id,omitempty" jsonschema:"the run ID; the latest run of the workflow if omitted"`
}

type workflowRun struct {
	WorkflowID          string               `json:"workflow_id"`
	RunID               string               `json:"run_id"`
	Type                string               `json:"type"`
	Status              string               `json:"status"`
	TaskQueue           string               `json:"task_queue"`
	StartTime           *time.Time           `json:"start_time"`
	ExecutionTime       *time.Time           `json:"execution_time"`
	CloseTime           *time.Time           `json:"close_time"`
	HistoryLength       int64                `json:"history_length"`
	Parent              *execution           `json:"parent"`
	PendingActivities   []pendingActivity    `json:"pending_activities"`
	PendingChildren     []pendingChild       `json:"pending_children"`
	PendingWorkflowTask *pendingWorkflowTask `json:"pending_workflow_task"`
	Memo                map[string]payload   `json:"memo"`
	SearchAttributes    map[string]any       `json:"search_attributes"`
	Result              []payload            `json:"result"`
	Failure             *failure             `json:"failure"`
	NewExecutionRunID   *string              `json:"new_execution_run_id"`
	ObservedAt          time.Time            `json:"observed_at"`
}

type execution struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

type pendingActivity struct {
	ActivityID        string     `json:"activity_id"`
	Type              string     `json:"type"`
	State             string     `json:"state"`
	Attempt           int32      `json:"attempt"`
	ScheduledTime     *time.Time `json:"scheduled_time"`
	LastStartedTime   *time.Time `json:"last_started_time"`
	LastHeartbeatTime *time.Time `json:"last_heartbeat_time"`
	LastFailure       *failure   `json:"last_failure"`
	HeartbeatDetails  []payload  `json:"heartbeat_details"`
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

type payload struct {
	Handling string `json:"handling"`
	Encoding string `json:"encoding"`
	Data     string `json:"data"`
}

type failure struct {
	Message              string              `json:"message"`
	Kind                 string              `json:"kind"`
	StackTrace           string              `json:"stack_trace"`
	Application          *applicationFailure `json:"application"`
	Details              []payload           `json:"details"`
	EncodedAttributes    *payload            `json:"encoded_attributes"`
	LastHeartbeatDetails []payload           `json:"last_heartbeat_details"`
	Cause                *failure            `json:"cause"`
}

// failureFields builds failure's $defs entry; jsonschema.For[failure] would return the $ref override.
type failureFields failure

func getWorkflowOutputSchema() *jsonschema.Schema {
	opts := &jsonschema.ForOptions{TypeSchemas: map[reflect.Type]*jsonschema.Schema{
		reflect.TypeFor[failure](): {Types: []string{"null", "object"}, Ref: "#/$defs/failure"},
	}}
	def, err := jsonschema.For[failureFields](opts)
	if err != nil {
		panic(fmt.Errorf("get_workflow: failure schema: %w", err))
	}
	def.Type, def.Types = "", []string{"null", "object"}
	schema, err := jsonschema.For[envelope[workflowRun]](opts)
	if err != nil {
		panic(fmt.Errorf("get_workflow: output schema: %w", err))
	}
	schema.Defs = map[string]*jsonschema.Schema{"failure": def}
	return schema
}

type applicationFailure struct {
	Type         string `json:"type"`
	NonRetryable bool   `json:"non_retryable"`
}

func getWorkflow(cfg *config.Config, clients *temporal.Clients) mcp.ToolHandlerFor[getWorkflowInput, envelope[workflowRun]] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in getWorkflowInput) (*mcp.CallToolResult, envelope[workflowRun], error) {
		t, err := lookupTarget(cfg, in.Target)
		if err != nil {
			return nil, envelope[workflowRun]{}, err
		}
		if in.WorkflowID == "" {
			return nil, envelope[workflowRun]{}, &toolError{Code: "invalid_argument", Message: "workflow_id is required"}
		}
		svc := clients.Client(t.Name).WorkflowService()
		desc, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
			return svc.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: t.Namespace,
				Execution: &commonpb.WorkflowExecution{WorkflowId: in.WorkflowID, RunId: in.RunID},
			}, opts...)
		})
		if err != nil {
			return nil, envelope[workflowRun]{}, err
		}
		run, err := describedRun(desc)
		if err != nil {
			return nil, envelope[workflowRun]{}, err
		}
		if desc.GetWorkflowExecutionInfo().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			hist, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
				return svc.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
					Namespace:              t.Namespace,
					Execution:              desc.GetWorkflowExecutionInfo().GetExecution(),
					HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
				}, opts...)
			})
			if err != nil {
				return nil, envelope[workflowRun]{}, err
			}
			events := hist.GetHistory().GetEvents()
			if len(events) == 0 {
				return nil, envelope[workflowRun]{}, &toolError{Code: "temporal_unavailable", Message: "the Temporal server returned no close event for the closed run"}
			}
			addCloseEvent(&run, events[0])
		}
		return nil, envelope[workflowRun]{Target: &t.Name, Result: run, Complete: true}, nil
	}
}

func callTemporal[Resp any](ctx context.Context, t *config.Target, rpc func(context.Context, ...grpc.CallOption) (Resp, error)) (Resp, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	var p peer.Peer
	resp, err := rpc(ctx, grpc.Peer(&p))
	if err != nil {
		return resp, temporalError(err, t, &p)
	}
	return resp, nil
}

func describedRun(desc *workflowservice.DescribeWorkflowExecutionResponse) (workflowRun, error) {
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
		Memo:              map[string]payload{},
		SearchAttributes:  map[string]any{},
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
			LastFailure:       toFailure(a.GetLastFailure()),
			HeartbeatDetails:  payloads(a.GetHeartbeatDetails()),
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
	for k, p := range info.GetMemo().GetFields() {
		run.Memo[k] = toPayload(p)
	}
	for k, p := range info.GetSearchAttributes().GetIndexedFields() {
		v, err := searchAttribute(p)
		if err != nil {
			return workflowRun{}, &toolError{Code: "temporal_unavailable", Message: fmt.Sprintf("the Temporal server returned search attribute %s in an unreadable form: %v", k, err)}
		}
		run.SearchAttributes[k] = v
	}
	return run, nil
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

func decode[T any](p *commonpb.Payload) (any, error) {
	var v T
	if err := converter.GetDefaultDataConverter().FromPayload(p, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func addCloseEvent(run *workflowRun, e *historypb.HistoryEvent) {
	var next string
	switch e.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		a := e.GetWorkflowExecutionCompletedEventAttributes()
		run.Result, next = payloads(a.GetResult()), a.GetNewExecutionRunId()
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		a := e.GetWorkflowExecutionFailedEventAttributes()
		run.Failure, next = toFailure(a.GetFailure()), a.GetNewExecutionRunId()
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		next = e.GetWorkflowExecutionTimedOutEventAttributes().GetNewExecutionRunId()
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		run.Failure = &failure{Details: payloads(e.GetWorkflowExecutionCanceledEventAttributes().GetDetails()), LastHeartbeatDetails: []payload{}}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		a := e.GetWorkflowExecutionTerminatedEventAttributes()
		run.Failure = &failure{Message: a.GetReason(), Details: payloads(a.GetDetails()), LastHeartbeatDetails: []payload{}}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		next = e.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
	}
	if next != "" {
		run.NewExecutionRunID = &next
	}
}

func toFailure(f *failurepb.Failure) *failure {
	if f == nil {
		return nil
	}
	out := &failure{
		Message:    f.GetMessage(),
		StackTrace: f.GetStackTrace(),
		Details:    payloads(f.GetApplicationFailureInfo().GetDetails()),
		Cause:      toFailure(f.GetCause()),
	}
	if ea := f.GetEncodedAttributes(); ea != nil {
		out.EncodedAttributes = new(toPayload(ea))
	}
	out.LastHeartbeatDetails = payloads(cmp.Or(f.GetTimeoutFailureInfo().GetLastHeartbeatDetails(), f.GetResetWorkflowFailureInfo().GetLastHeartbeatDetails()))
	m := f.ProtoReflect()
	if info := m.WhichOneof(m.Descriptor().Oneofs().ByName("failure_info")); info != nil {
		out.Kind = strings.TrimSuffix(string(info.Message().Name()), "Info")
	}
	if a := f.GetApplicationFailureInfo(); a != nil {
		out.Application = &applicationFailure{Type: a.GetType(), NonRetryable: a.GetNonRetryable()}
	}
	if c := f.GetCanceledFailureInfo(); c != nil {
		out.Details = payloads(c.GetDetails())
	}
	return out
}

func payloads(ps *commonpb.Payloads) []payload {
	out := []payload{}
	for _, p := range ps.GetPayloads() {
		out = append(out, toPayload(p))
	}
	return out
}

func toPayload(p *commonpb.Payload) payload {
	return payload{Handling: "passthrough", Encoding: string(p.GetMetadata()["encoding"]), Data: base64.StdEncoding.EncodeToString(p.GetData())}
}

func timeOf(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	return &t
}
