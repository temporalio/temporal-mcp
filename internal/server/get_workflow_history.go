// The get_workflow_history tool, which returns the events of a workflow run's history that
// match its filters, a bounded number at a time, and the continuation that asks for the rest.

package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/temporalio/temporal-operator-mcp/internal/config"
	"github.com/temporalio/temporal-operator-mcp/internal/payload"
	"github.com/temporalio/temporal-operator-mcp/internal/temporal"
)

const maxHistoryPages = 10

type getWorkflowHistoryInput struct {
	Target string `json:"target" jsonschema:"the name or an alias of a target"`
	workflowRef
	EventTypes   []string    `json:"event_types,omitempty" jsonschema:"the event types to return, named as in event_type (such as ActivityTaskFailed), or the category failures (every event type ending in Failed or TimedOut); every event type if omitted"`
	EventRange   *eventRange `json:"event_range,omitempty" jsonschema:"the event IDs to return, inclusive"`
	Order        string      `json:"order,omitempty" jsonschema:"oldest_first or newest_first"`
	Limit        int         `json:"limit,omitempty" jsonschema:"the most events to return"`
	Continuation string      `json:"continuation,omitempty" jsonschema:"the continuation from the next_call of the previous call"`
}

type eventRange struct {
	First int64 `json:"first,omitempty" jsonschema:"the lowest event ID to return, in either order; the run's first event if omitted"`
	Last  int64 `json:"last,omitempty" jsonschema:"the highest event ID to return, in either order; the run's last event if omitted"`
}

type workflowHistory struct {
	WorkflowID string         `json:"workflow_id"`
	RunID      string         `json:"run_id"`
	Events     []historyEvent `json:"events"`
}

type historyEvent struct {
	EventID    int64            `json:"event_id"`
	EventTime  *time.Time       `json:"event_time"`
	EventType  string           `json:"event_type"`
	Attributes map[string]any   `json:"attributes"`
	Links      []map[string]any `json:"links"`
}

func getWorkflowHistoryInputSchema() *jsonschema.Schema {
	schema := schemaFor[getWorkflowHistoryInput]("get_workflow_history", "input", nil)
	order := schema.Properties["order"]
	order.Enum, order.Default = []any{"oldest_first", "newest_first"}, json.RawMessage(`"oldest_first"`)
	boundPageSize(schema.Properties["limit"])
	for _, id := range schema.Properties["event_range"].Properties {
		id.Minimum, id.Maximum = new(1.0), new(float64(maxJSONInteger))
	}
	return schema
}

func getWorkflowHistory(cfg *config.Config, clients *temporal.Clients, payloads *payload.Renderer) mcp.ToolHandlerFor[getWorkflowHistoryInput, envelope[workflowHistory]] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in getWorkflowHistoryInput) (*mcp.CallToolResult, envelope[workflowHistory], error) {
		var none envelope[workflowHistory]
		t, err := lookupTarget(ctx, cfg, in.Target)
		if err != nil {
			return nil, none, err
		}
		if in.WorkflowID == "" {
			return nil, none, &toolError{Code: "invalid_argument", Message: "workflow_id is required"}
		}
		types, err := eventTypes(in.EventTypes)
		if err != nil {
			return nil, none, err
		}
		r := eventRange{}
		if in.EventRange != nil {
			r = *in.EventRange
		}
		if r.Last != 0 && r.Last < r.First {
			return nil, none, &toolError{Code: "invalid_argument", Message: "event_range: last must not be less than first"}
		}
		svc := clients.Client(t.Name).WorkflowService()
		runID := in.RunID
		if runID == "" {
			desc, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
				return svc.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: t.Namespace,
					Execution: &commonpb.WorkflowExecution{WorkflowId: in.WorkflowID},
				}, opts...)
			})
			if err != nil {
				return nil, none, err
			}
			runID = desc.GetWorkflowExecutionInfo().GetExecution().GetRunId()
		}
		binding := []string{t.Name, in.WorkflowID, runID, in.Order, fmt.Sprint(slices.Sorted(maps.Keys(types))), strconv.FormatInt(r.First, 10), strconv.FormatInt(r.Last, 10)}
		// A position is the ID of the last event returned, or 0 if none was, and then the token of the page to read. Every
		// page skips events up to that ID: a newest_first read without a token starts again at the run's latest event.
		var after int64
		var token []byte
		if in.Continuation != "" {
			position, err := decodeContinuation(in.Continuation, "get_workflow_history", binding...)
			if err != nil {
				return nil, none, err
			}
			id, n := binary.Uvarint(position)
			if n <= 0 {
				return nil, none, invalidContinuation("get_workflow_history")
			}
			after, token = int64(id), position[n:]
		}
		execution := &commonpb.WorkflowExecution{WorkflowId: in.WorkflowID, RunId: runID}
		forward := in.Order != "newest_first"
		pageSize := int32(in.Limit)
		if len(types) > 0 || in.EventRange != nil {
			// 0 asks for the server's largest page.
			pageSize = 0
		}
		read := func(token []byte) ([]*historypb.HistoryEvent, []byte, error) {
			if forward {
				resp, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
					return svc.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: t.Namespace, Execution: execution, MaximumPageSize: pageSize, NextPageToken: token}, opts...)
				})
				return resp.GetHistory().GetEvents(), resp.GetNextPageToken(), err
			}
			resp, err := callTemporal(ctx, t, func(ctx context.Context, opts ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryReverseResponse, error) {
				return svc.GetWorkflowExecutionHistoryReverse(ctx, &workflowservice.GetWorkflowExecutionHistoryReverseRequest{Namespace: t.Namespace, Execution: execution, MaximumPageSize: pageSize, NextPageToken: token}, opts...)
			})
			return resp.GetHistory().GetEvents(), resp.GetNextPageToken(), err
		}
		earlier := func(a, b int64) bool { return forward && a < b || !forward && a > b }
		batch := payloads.Batch(t.Name)
		out := envelope[workflowHistory]{Target: &t.Name, Result: workflowHistory{WorkflowID: in.WorkflowID, RunID: runID, Events: []historyEvent{}}}
		var more bool
	pages:
		for pages := 1; ; pages++ {
			events, next, err := read(token)
			if err != nil {
				if pages == 1 && in.Continuation != "" {
					err = continuationError(err, "get_workflow_history")
				}
				return nil, none, err
			}
			for _, e := range events {
				id := e.GetEventId()
				if after != 0 && !earlier(after, id) {
					continue
				}
				if id >= r.First && (r.Last == 0 || id <= r.Last) && (len(types) == 0 || types[e.GetEventType()]) {
					if len(out.Result.Events) == in.Limit {
						more = true
						break pages
					}
					event, err := toHistoryEvent(e, batch)
					if err != nil {
						return nil, none, err
					}
					out.Result.Events = append(out.Result.Events, event)
					after = id
				}
				if forward && r.Last != 0 && id >= r.Last || !forward && id <= r.First {
					break pages
				}
			}
			if len(next) == 0 {
				break
			}
			token = next
			if len(out.Result.Events) == in.Limit || pages == maxHistoryPages {
				more = true
				break
			}
		}
		if err := batch.Render(ctx); err != nil {
			return nil, none, err
		}
		out.Complete = !more
		if out.Complete {
			return nil, out, nil
		}
		continuation, ok := encodeContinuation(append(binary.AppendUvarint(nil, uint64(after)), token...), "get_workflow_history", binding...)
		if !ok {
			return nil, out, nil
		}
		args := map[string]any{"target": t.Name, "workflow_id": in.WorkflowID, "run_id": runID, "order": in.Order, "limit": in.Limit, "continuation": continuation}
		if in.EventTypes != nil {
			args["event_types"] = in.EventTypes
		}
		if in.EventRange != nil {
			args["event_range"] = in.EventRange
		}
		out.NextCall = &nextCall{Name: "get_workflow_history", Arguments: args}
		return nil, out, nil
	}
}

func eventTypes(names []string) (map[enumspb.EventType]bool, error) {
	types := map[enumspb.EventType]bool{}
	var bad []string
	for _, name := range names {
		if name == "failures" {
			for n := range enumspb.EventType_name {
				if s := enumspb.EventType(n).String(); strings.HasSuffix(s, "Failed") || strings.HasSuffix(s, "TimedOut") {
					types[enumspb.EventType(n)] = true
				}
			}
			continue
		}
		typ, err := enumspb.EventTypeFromString(name)
		if err != nil || typ == enumspb.EVENT_TYPE_UNSPECIFIED {
			bad = append(bad, strconv.Quote(name))
			continue
		}
		types[typ] = true
	}
	if len(bad) > 0 {
		return nil, &toolError{Code: "invalid_argument", Message: fmt.Sprintf("these event_types are neither event types nor categories: %s; give event types as get_workflow_history returns them in event_type, such as ActivityTaskFailed, or the category failures", strings.Join(bad, ", "))}
	}
	return types, nil
}

func toHistoryEvent(e *historypb.HistoryEvent, batch *payload.Batch) (historyEvent, error) {
	event := historyEvent{EventID: e.GetEventId(), EventTime: timeOf(e.GetEventTime()), EventType: e.GetEventType().String(), Attributes: map[string]any{}, Links: []map[string]any{}}
	m := e.ProtoReflect()
	if fd := m.WhichOneof(m.Descriptor().Oneofs().ByName("attributes")); fd != nil {
		var err error
		if event.Attributes, err = protoFields(m.Get(fd).Message(), batch); err != nil {
			return historyEvent{}, err
		}
	}
	for _, l := range e.GetLinks() {
		link, err := protoFields(l.ProtoReflect(), batch)
		if err != nil {
			return historyEvent{}, err
		}
		event.Links = append(event.Links, link)
	}
	return event, nil
}

// protoFields returns the fields set in m by their proto names, with payloads, failures and search attributes as get_workflows gives them.
func protoFields(m protoreflect.Message, batch *payload.Batch) (map[string]any, error) {
	out := map[string]any{}
	var err error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		out[string(fd.Name())], err = protoField(fd, v, batch)
		return err == nil
	})
	return out, err
}

func protoField(fd protoreflect.FieldDescriptor, v protoreflect.Value, batch *payload.Batch) (any, error) {
	var err error
	switch {
	case fd.IsList():
		out := make([]any, v.List().Len())
		for i := range out {
			if out[i], err = protoValue(fd, v.List().Get(i), batch); err != nil {
				return nil, err
			}
		}
		return out, nil
	case fd.IsMap():
		out := map[string]any{}
		v.Map().Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			out[k.String()], err = protoValue(fd.MapValue(), v, batch)
			return err == nil
		})
		return out, err
	}
	return protoValue(fd, v, batch)
}

func protoValue(fd protoreflect.FieldDescriptor, v protoreflect.Value, batch *payload.Batch) (any, error) {
	switch fd.Kind() {
	case protoreflect.EnumKind:
		// The registered Go enum type gives the short name that get_workflows uses, such as Failed.
		if typ, err := protoregistry.GlobalTypes.FindEnumByName(fd.Enum().FullName()); err == nil {
			return fmt.Sprint(typ.New(v.Enum())), nil
		}
		return v.Enum(), nil
	case protoreflect.MessageKind:
		switch m := v.Message().Interface().(type) {
		case *commonpb.Payload:
			return batch.Add(m), nil
		case *commonpb.Payloads:
			return batch.AddAll(m), nil
		case *failurepb.Failure:
			return toFailure(m, batch), nil
		case *commonpb.SearchAttributes:
			return searchAttributes(m)
		case payloadFields:
			return payloadMap(m, batch), nil
		case *timestamppb.Timestamp:
			return m.AsTime(), nil
		case *durationpb.Duration:
			return durationString(m), nil
		}
		return protoFields(v.Message(), batch)
	}
	return v.Interface(), nil
}

// durationString returns d in seconds with the fewest digits that give it, such as 1.5s.
func durationString(d *durationpb.Duration) string {
	return strconv.FormatFloat(d.AsDuration().Seconds(), 'f', -1, 64) + "s"
}
